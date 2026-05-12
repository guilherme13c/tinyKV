package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	src "github.com/guilherme13c/tinyKV/src"
	"github.com/guilherme13c/tinyKV/src/memtable"
)

type indexEntry struct {
	lastKey []byte
	handle  BlockHandle
}

type Reader struct {
	file      *os.File
	index     []indexEntry
	bloom     *BloomFilter
	blockPool sync.Pool
}

func (r *Reader) Path() string { return r.file.Name() }

// EstimatedKeyCount returns an approximation of the number of keys in the SSTable,
// derived from the bloom filter bit-array size: n ≈ ⌈bloomBytes × 8 / bitsPerKey⌉.
// Using ceiling division ensures the pre-allocated capacity is never under-sized.
func (r *Reader) EstimatedKeyCount() int {
	if r.bloom == nil || len(r.bloom.bits) == 0 {
		return 0
	}
	return (len(r.bloom.bits)*8 + bitsPerKey - 1) / bitsPerKey
}

func NewReader(path string) (*Reader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if info.Size() < FooterSize {
		_ = f.Close()
		return nil, fmt.Errorf("sstable: file too small")
	}

	var footerBuf [FooterSize]byte
	if _, err := f.ReadAt(footerBuf[:], info.Size()-FooterSize); err != nil {
		_ = f.Close()
		return nil, err
	}

	footer := Footer{
		IndexHandle: BlockHandle{
			Offset: binary.LittleEndian.Uint64(footerBuf[0:]),
			Length: binary.LittleEndian.Uint64(footerBuf[8:]),
		},
		BloomHandle: BlockHandle{
			Offset: binary.LittleEndian.Uint64(footerBuf[16:]),
			Length: binary.LittleEndian.Uint64(footerBuf[24:]),
		},
	}

	bloomData := make([]byte, footer.BloomHandle.Length)
	if _, err := f.ReadAt(bloomData, int64(footer.BloomHandle.Offset)); err != nil {
		_ = f.Close()
		return nil, err
	}
	bloom := DecodeBloom(bloomData)

	indexData := make([]byte, footer.IndexHandle.Length)
	if _, err := f.ReadAt(indexData, int64(footer.IndexHandle.Offset)); err != nil {
		_ = f.Close()
		return nil, err
	}

	index, err := parseIndexBlock(indexData)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return &Reader{
		file:  f,
		index: index,
		bloom: bloom,
		blockPool: sync.Pool{
			New: func() any { b := make([]byte, 4096); return &b },
		},
	}, nil
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	if !r.bloom.MayContain(key) {
		return nil, &src.KeyNotFoundError{Key: key}
	}

	// Binary search: first block whose lastKey >= key.
	lo, hi := 0, len(r.index)-1
	blockIdx := -1
	for lo <= hi {
		mid := (lo + hi) / 2
		if bytes.Compare(r.index[mid].lastKey, key) < 0 {
			lo = mid + 1
		} else {
			blockIdx = mid
			hi = mid - 1
		}
	}
	if blockIdx == -1 {
		return nil, &src.KeyNotFoundError{Key: key}
	}

	bp := r.blockPool.Get().(*[]byte)
	data, err := r.readBlockInto(r.index[blockIdx].handle, bp)
	if err != nil {
		r.blockPool.Put(bp)
		return nil, err
	}
	result, scanErr := scanBlock(data, key)
	r.blockPool.Put(bp)
	return result, scanErr
}

func (r *Reader) Iterator() memtable.MemTableIteratorI {
	it := &sstableIterator{r: r, blockIdx: -1}
	if len(r.index) > 0 {
		it.loadBlock(0)
	}
	it.advance()
	return it
}

func (r *Reader) Close() error {
	return r.file.Close()
}

// readBlockInto reads the block described by handle into the pooled buffer *bp,
// growing it if necessary, and returns the populated slice.
func (r *Reader) readBlockInto(handle BlockHandle, bp *[]byte) ([]byte, error) {
	if uint64(cap(*bp)) < handle.Length {
		*bp = make([]byte, handle.Length)
	}
	data := (*bp)[:handle.Length]
	_, err := r.file.ReadAt(data, int64(handle.Offset))
	return data, err
}

func parseIndexBlock(data []byte) ([]indexEntry, error) {
	var entries []indexEntry
	pos := 0
	for pos < len(data) {
		keyLen, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("sstable: malformed index block")
		}
		pos += n

		if pos+int(keyLen)+16 > len(data) {
			return nil, fmt.Errorf("sstable: index block truncated")
		}

		key := make([]byte, keyLen)
		copy(key, data[pos:])
		pos += int(keyLen)

		offset := binary.LittleEndian.Uint64(data[pos:])
		length := binary.LittleEndian.Uint64(data[pos+8:])
		pos += 16

		entries = append(entries, indexEntry{
			lastKey: key,
			handle:  BlockHandle{Offset: offset, Length: length},
		})
	}
	return entries, nil
}

func scanBlock(data, key []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, &src.KeyNotFoundError{Key: key}
	}

	// Parse restart table from the end of the block.
	// Format: [entries...][restart_0 uint32]...[restart_N uint32][num_restarts uint32]
	numRestarts := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	dataEnd := len(data) - 4*(1+numRestarts)
	if dataEnd < 0 || numRestarts < 0 {
		return nil, &src.KeyNotFoundError{Key: key}
	}
	restartBase := dataEnd

	// Binary search: find the largest restart point whose first key <= target key.
	startPos := 0
	if numRestarts > 1 {
		lo, hi := 0, numRestarts-1
		best := 0
		for lo <= hi {
			mid := (lo + hi) / 2
			rpOff := int(binary.LittleEndian.Uint32(data[restartBase+mid*4:]))
			// Decode the key at this restart point (skip valueMeta uvarint).
			rpKeyLen, n := binary.Uvarint(data[rpOff:])
			if n <= 0 {
				break
			}
			_, n2 := binary.Uvarint(data[rpOff+n:])
			if n2 <= 0 {
				break
			}
			rpKeyStart := rpOff + n + n2
			rpKey := data[rpKeyStart : rpKeyStart+int(rpKeyLen)]
			if bytes.Compare(rpKey, key) <= 0 {
				best = mid
				lo = mid + 1
			} else {
				hi = mid - 1
			}
		}
		startPos = int(binary.LittleEndian.Uint32(data[restartBase+best*4:]))
	}

	pos := startPos
	for pos < dataEnd {
		keyLen, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			break
		}
		pos += n

		valueMeta, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			break
		}
		pos += n

		valueLen := int(valueMeta >> 1)
		isTombstone := valueMeta&1 == 1

		k := data[pos : pos+int(keyLen)]
		pos += int(keyLen)

		var v []byte
		if !isTombstone {
			v = data[pos : pos+valueLen]
			pos += valueLen
		}

		cmp := bytes.Compare(k, key)
		if cmp == 0 {
			if isTombstone {
				return nil, src.ErrTombstone
			}
			result := make([]byte, len(v))
			copy(result, v)
			return result, nil
		}
		if cmp > 0 {
			break
		}
	}
	return nil, &src.KeyNotFoundError{
		Key: key,
	}
}

// sstableIterator walks data blocks sequentially.
type sstableIterator struct {
	r             *Reader
	blockIdx      int
	blockData     []byte
	blockBuf      *[]byte // pooled buffer backing blockData; nil when no block is loaded
	blockPos      int
	blockDataEnd  int    // index past the last entry (excludes restart-point trailer)
	currKey       []byte
	currValue     []byte
	currTombstone bool
	valid         bool
	// Double-buffer for key and value: we alternate between the two slots so
	// that the mergeIterator can safely hold slice references from the previous
	// advance while we write the new entry into the inactive slot.
	keyBufs [2][]byte
	valBufs [2][]byte
	bufIdx  uint8
}

func (it *sstableIterator) loadBlock(idx int) bool {
	if idx >= len(it.r.index) {
		return false
	}
	// Return the old buffer to the pool before fetching a new one.
	// currKey/currValue live in the double-buffers (keyBufs/valBufs), not in
	// blockData, so they are safe after this Put.
	if it.blockBuf != nil {
		it.r.blockPool.Put(it.blockBuf)
		it.blockBuf = nil
		it.blockData = nil
	}
	bp := it.r.blockPool.Get().(*[]byte)
	data, err := it.r.readBlockInto(it.r.index[idx].handle, bp)
	if err != nil {
		it.r.blockPool.Put(bp)
		return false
	}

	// Parse the restart-point trailer to find where entries end.
	blockDataEnd := len(data)
	if len(data) >= 4 {
		numRestarts := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
		end := len(data) - 4*(1+numRestarts)
		if end >= 0 {
			blockDataEnd = end
		}
	}

	it.blockIdx = idx
	it.blockData = data
	it.blockBuf = bp
	it.blockPos = 0
	it.blockDataEnd = blockDataEnd
	return true
}

// readEntry decodes the next raw entry from the current position,
// loading the next block automatically when the current one is exhausted.
// key and value are zero-copy subslices of blockData; callers must copy
// them (via the double-buffer in advance) before calling loadBlock again.
func (it *sstableIterator) readEntry() (key, value []byte, isTombstone bool, ok bool) {
	for it.blockPos >= it.blockDataEnd {
		if !it.loadBlock(it.blockIdx + 1) {
			return nil, nil, false, false
		}
	}

	pos := it.blockPos

	keyLen, n := binary.Uvarint(it.blockData[pos:])
	if n <= 0 {
		return nil, nil, false, false
	}
	pos += n

	valueMeta, n := binary.Uvarint(it.blockData[pos:])
	if n <= 0 {
		return nil, nil, false, false
	}
	pos += n

	valueLen := int(valueMeta >> 1)
	isTombstone = valueMeta&1 == 1

	if pos+int(keyLen) > len(it.blockData) {
		return nil, nil, false, false
	}
	key = it.blockData[pos : pos+int(keyLen)]
	pos += int(keyLen)

	if !isTombstone {
		if pos+valueLen > len(it.blockData) {
			return nil, nil, false, false
		}
		value = it.blockData[pos : pos+valueLen]
		pos += valueLen
	}

	it.blockPos = pos
	return key, value, isTombstone, true
}

func (it *sstableIterator) advance() {
	key, value, isTombstone, ok := it.readEntry()
	if !ok {
		it.valid = false
		return
	}
	// Write into the inactive slot, then flip the index.
	// The active slot (old bufIdx) may still be referenced by the mergeIterator
	// heap as the key/value from the previous advance — it is safe because each
	// iterator has at most one outstanding heap entry and we only overwrite the
	// OTHER slot here.
	next := it.bufIdx ^ 1
	it.keyBufs[next] = append(it.keyBufs[next][:0], key...)
	it.valBufs[next] = append(it.valBufs[next][:0], value...)
	it.currKey = it.keyBufs[next]
	it.currValue = it.valBufs[next]
	it.bufIdx = next
	it.currTombstone = isTombstone
	it.valid = true
}

func (it *sstableIterator) Valid() bool        { return it.valid }
func (it *sstableIterator) Key() []byte        { return it.currKey }
func (it *sstableIterator) Value() []byte      { return it.currValue }
func (it *sstableIterator) IsTombstone() bool  { return it.currTombstone }
func (it *sstableIterator) Next()              { it.advance() }

func (it *sstableIterator) Seek(key []byte) {
	lo, hi := 0, len(it.r.index)-1
	blockIdx := len(it.r.index)
	for lo <= hi {
		mid := (lo + hi) / 2
		if bytes.Compare(it.r.index[mid].lastKey, key) < 0 {
			lo = mid + 1
		} else {
			blockIdx = mid
			hi = mid - 1
		}
	}
	if blockIdx >= len(it.r.index) || !it.loadBlock(blockIdx) {
		it.valid = false
		return
	}
	it.advance()
	for it.valid && bytes.Compare(it.currKey, key) < 0 {
		it.advance()
	}
}

func (it *sstableIterator) Close() error {
	if it.blockBuf != nil {
		it.r.blockPool.Put(it.blockBuf)
		it.blockBuf = nil
	}
	it.valid = false
	return nil
}
