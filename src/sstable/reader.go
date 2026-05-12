package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	src "github.com/guilherme13c/tinyKV/src"
	"github.com/guilherme13c/tinyKV/src/memtable"
)

type indexEntry struct {
	lastKey []byte
	handle  BlockHandle
}

type Reader struct {
	file  *os.File
	index []indexEntry
	bloom *BloomFilter
}

func (r *Reader) Path() string { return r.file.Name() }

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

	data, err := r.readBlock(r.index[blockIdx].handle)
	if err != nil {
		return nil, err
	}
	return scanBlock(data, key)
}

func (r *Reader) Iterator() memtable.MemTableIteratorI {
	it := &sstableIterator{r: r, blockIdx: 0}
	if len(r.index) > 0 {
		if data, err := r.readBlock(r.index[0].handle); err == nil {
			it.blockData = data
		}
	}
	it.advance()
	return it
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) readBlock(handle BlockHandle) ([]byte, error) {
	data := make([]byte, handle.Length)
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
	pos := 0
	for pos < len(data) {
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
	blockPos      int
	currKey       []byte
	currValue     []byte
	currTombstone bool
	valid         bool
}

func (it *sstableIterator) loadBlock(idx int) bool {
	if idx >= len(it.r.index) {
		return false
	}
	data, err := it.r.readBlock(it.r.index[idx].handle)
	if err != nil {
		return false
	}
	it.blockIdx = idx
	it.blockData = data
	it.blockPos = 0
	return true
}

// readEntry decodes the next raw entry from the current position,
// loading the next block automatically when the current one is exhausted.
func (it *sstableIterator) readEntry() (key, value []byte, isTombstone bool, ok bool) {
	for it.blockPos >= len(it.blockData) {
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
	key = make([]byte, keyLen)
	copy(key, it.blockData[pos:])
	pos += int(keyLen)

	if !isTombstone {
		if pos+valueLen > len(it.blockData) {
			return nil, nil, false, false
		}
		value = make([]byte, valueLen)
		copy(value, it.blockData[pos:])
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
	it.currKey = key
	it.currValue = value
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
	it.valid = false
	return nil
}
