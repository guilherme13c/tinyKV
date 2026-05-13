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

// BlockCache is the interface satisfied by the store's block cache.
// Pass nil to NewReader to disable block caching.
type BlockCache interface {
	GetBlock(path string, offset uint64) ([]byte, bool)
	PutBlock(path string, offset uint64, data []byte)
}

type indexEntry struct {
	lastKey []byte
	handle  BlockHandle
}

type Reader struct {
	file      *os.File
	index     []indexEntry
	bloom     *BloomFilter
	blockPool sync.Pool
	cache     BlockCache // nil when caching is disabled
	minKey    []byte     // first key in the SSTable; nil for an empty file
}

func (r *Reader) Path() string { return r.file.Name() }

// MinKey returns the first (smallest) key in the SSTable.
func (r *Reader) MinKey() []byte { return r.minKey }

// MaxKey returns the last (largest) key in the SSTable.
func (r *Reader) MaxKey() []byte {
	if len(r.index) == 0 {
		return nil
	}
	return r.index[len(r.index)-1].lastKey
}

// FileSize returns the size of the SSTable file in bytes.
func (r *Reader) FileSize() (int64, error) {
	info, err := r.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// EstimatedKeyCount returns an approximation of the number of keys in the SSTable,
// derived from the bloom filter bit-array size: n ≈ ⌈bloomBytes × 8 / bitsPerKey⌉.
// Using ceiling division ensures the pre-allocated capacity is never under-sized.
func (r *Reader) EstimatedKeyCount() int {
	if r.bloom == nil || len(r.bloom.bits) == 0 {
		return 0
	}
	return (len(r.bloom.bits)*8 + bitsPerKey - 1) / bitsPerKey
}

func NewReader(path string, cache BlockCache) (*Reader, error) {
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

	if footerBuf[32] != FormatVersion {
		_ = f.Close()
		return nil, fmt.Errorf("sstable: unsupported format version 0x%02x (want 0x%02x); delete old SSTable files and restart", footerBuf[32], FormatVersion)
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

	// Read the first key (minKey) from the first block.
	var minKey []byte
	if len(index) > 0 {
		minKey, err = readFirstKey(f, index[0].handle)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	return &Reader{
		file:   f,
		index:  index,
		bloom:  bloom,
		cache:  cache,
		minKey: minKey,
		blockPool: sync.Pool{
			New: func() any { b := make([]byte, 4096); return &b },
		},
	}, nil
}

// readFirstKey reads and returns the first key stored in the block described by handle.
func readFirstKey(f *os.File, handle BlockHandle) ([]byte, error) {
	buf := make([]byte, handle.Length)
	if _, err := f.ReadAt(buf, int64(handle.Offset)); err != nil {
		return nil, err
	}
	// Decode: keyLen (uvarint), valueMeta (uvarint), key bytes.
	keyLen, n := binary.Uvarint(buf)
	if n <= 0 {
		return nil, fmt.Errorf("sstable: malformed first block")
	}
	_, n2 := binary.Uvarint(buf[n:])
	if n2 <= 0 {
		return nil, fmt.Errorf("sstable: malformed first block")
	}
	keyStart := n + n2
	if keyStart+int(keyLen) > len(buf) {
		return nil, fmt.Errorf("sstable: first block truncated")
	}
	key := make([]byte, keyLen)
	copy(key, buf[keyStart:keyStart+int(keyLen)])
	return key, nil
}

// readCachedBlock fetches the block described by handle from the cache when
// available, falling back to a disk read on a miss. Returns:
//   - data:    the block bytes
//   - poolBuf: non-nil when the backing buffer came from blockPool; the caller
//     must return it with r.blockPool.Put(poolBuf) after use. nil when the
//     data was served from the cache (no pool buffer is involved).
//   - err: non-nil on I/O failure
func (r *Reader) readCachedBlock(handle BlockHandle) (data []byte, poolBuf *[]byte, err error) {
	if r.cache != nil {
		if cached, ok := r.cache.GetBlock(r.file.Name(), handle.Offset); ok {
			return cached, nil, nil
		}
	}
	bp := r.blockPool.Get().(*[]byte)
	data, err = r.readBlockInto(handle, bp)
	if err != nil {
		r.blockPool.Put(bp)
		return nil, nil, err
	}
	if r.cache != nil {
		// Store a copy in the cache; the pool buffer will be reused by the caller.
		cached := make([]byte, len(data))
		copy(cached, data)
		r.cache.PutBlock(r.file.Name(), handle.Offset, cached)
	}
	return data, bp, nil
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

	data, bp, err := r.readCachedBlock(r.index[blockIdx].handle)
	if err != nil {
		return nil, err
	}
	result, scanErr := scanBlock(data, key)
	if bp != nil {
		r.blockPool.Put(bp)
	}
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
	data, bp, err := it.r.readCachedBlock(it.r.index[idx].handle)
	if err != nil {
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
	it.blockBuf = bp // nil when the block was served from cache
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
