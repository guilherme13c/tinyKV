package sstable

import (
	"encoding/binary"
	"math"
	"os"
)

type Writer struct {
	file         *os.File
	offset       uint64
	dataBuf      []byte
	lastKey      []byte
	indexKeys    [][]byte
	indexHandles []BlockHandle
	blockStart   uint64

	// Bloom filter built incrementally — no per-key heap copy.
	bloomBuf    []byte    // all keys concatenated
	bloomLens   []int     // length of each key
	pooledBufs  *bloomBufs // non-nil when buffers came from bloomBufPool

	// Restart points for the current (not-yet-flushed) block.
	restartPoints []uint32 // byte offsets within dataBuf of every RestartInterval-th entry
	entryCount    int      // entries appended since last flushBlock
}

// NewWriter opens path for writing a new SSTable.
// keyHint is the expected number of keys; pass 0 if unknown.
// A positive hint ensures bloom-filter accumulation buffers are large enough;
// buffers are pooled across calls so most invocations incur zero allocation.
func NewWriter(path string, keyHint int) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}

	// Acquire bloom buffers from the pool; fall back to fresh allocation if empty.
	var bb *bloomBufs
	select {
	case bb = <-bloomBufPool:
	default:
		bb = &bloomBufs{}
	}

	// Ensure the pooled (or fresh) backing arrays are large enough for this writer.
	if keyHint > 0 {
		if cap(bb.lens) < keyHint {
			bb.lens = make([]int, 0, keyHint)
		}
		if cap(bb.buf) < keyHint*32 {
			bb.buf = make([]byte, 0, keyHint*32)
		}
	} else {
		if cap(bb.lens) < defaultBloomLensCap {
			bb.lens = make([]int, 0, defaultBloomLensCap)
		}
		if cap(bb.buf) < defaultBloomBufCap {
			bb.buf = make([]byte, 0, defaultBloomBufCap)
		}
	}

	return &Writer{
		file:       f,
		dataBuf:    make([]byte, 0, BlockSize),
		bloomLens:  bb.lens[:0],
		bloomBuf:   bb.buf[:0],
		pooledBufs: bb,
	}, nil
}

// returnBloomBufs resets and returns bloom accumulation buffers to the pool.
// Must be called exactly once per Writer, after writeBloomBlock has consumed the data.
func (w *Writer) returnBloomBufs() {
	if w.pooledBufs == nil {
		return
	}
	// Preserve backing arrays; store current (possibly grown) slices back.
	w.pooledBufs.lens = w.bloomLens[:0]
	w.pooledBufs.buf = w.bloomBuf[:0]
	select {
	case bloomBufPool <- w.pooledBufs:
	default: // pool full; let GC collect
	}
	w.pooledBufs = nil
	w.bloomLens = nil
	w.bloomBuf = nil
}

// BytesWritten returns the number of bytes written to the SSTable so far.
func (w *Writer) BytesWritten() uint64 { return w.offset }

func (w *Writer) Append(key, value []byte, isTombstone bool) error {
	valueMeta := uint64(len(value)) << 1
	if isTombstone {
		valueMeta |= 1
	}

	// Record restart point before encoding this entry.
	if w.entryCount%RestartInterval == 0 {
		w.restartPoints = append(w.restartPoints, uint32(len(w.dataBuf)))
	}

	var header [20]byte
	n1 := binary.PutUvarint(header[:], uint64(len(key)))
	n2 := binary.PutUvarint(header[n1:], valueMeta)

	w.dataBuf = append(w.dataBuf, header[:n1+n2]...)
	w.dataBuf = append(w.dataBuf, key...)
	if !isTombstone {
		w.dataBuf = append(w.dataBuf, value...)
	}

	w.lastKey = append(w.lastKey[:0], key...)
	w.entryCount++

	// Accumulate key bytes for bloom filter — zero per-key allocs.
	w.bloomLens = append(w.bloomLens, len(key))
	w.bloomBuf = append(w.bloomBuf, key...)

	if len(w.dataBuf) >= BlockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *Writer) Close() error {
	if err := w.flushBlock(); err != nil {
		w.returnBloomBufs()
		return err
	}

	indexHandle, err := w.writeIndexBlock()
	if err != nil {
		w.returnBloomBufs()
		return err
	}

	bloomHandle, err := w.writeBloomBlock()
	w.returnBloomBufs() // bloom data consumed; return buffers regardless of error
	if err != nil {
		return err
	}

	if err := w.writeFooter(indexHandle, bloomHandle); err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

func (w *Writer) flushBlock() error {
	if len(w.dataBuf) == 0 {
		return nil
	}

	// Append restart table: [offset_0 uint32]...[offset_N uint32][num_restarts uint32].
	var tmp [4]byte
	for _, rp := range w.restartPoints {
		binary.LittleEndian.PutUint32(tmp[:], rp)
		w.dataBuf = append(w.dataBuf, tmp[:]...)
	}
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(w.restartPoints)))
	w.dataBuf = append(w.dataBuf, tmp[:]...)

	n, err := w.file.Write(w.dataBuf)
	if err != nil {
		return err
	}

	w.indexKeys = append(w.indexKeys, append([]byte(nil), w.lastKey...))
	w.indexHandles = append(w.indexHandles, BlockHandle{Offset: w.blockStart, Length: uint64(n)})

	w.offset += uint64(n)
	w.blockStart = w.offset
	w.dataBuf = w.dataBuf[:0]
	w.restartPoints = w.restartPoints[:0]
	w.entryCount = 0
	return nil
}

func (w *Writer) writeIndexBlock() (BlockHandle, error) {
	start := w.offset
	var headerBuf [10]byte
	var handleBuf [16]byte

	for i, key := range w.indexKeys {
		n := binary.PutUvarint(headerBuf[:], uint64(len(key)))
		if _, err := w.file.Write(headerBuf[:n]); err != nil {
			return BlockHandle{}, err
		}
		if _, err := w.file.Write(key); err != nil {
			return BlockHandle{}, err
		}
		binary.LittleEndian.PutUint64(handleBuf[:8], w.indexHandles[i].Offset)
		binary.LittleEndian.PutUint64(handleBuf[8:], w.indexHandles[i].Length)
		if _, err := w.file.Write(handleBuf[:]); err != nil {
			return BlockHandle{}, err
		}
		w.offset += uint64(n) + uint64(len(key)) + 16
	}

	return BlockHandle{Offset: start, Length: w.offset - start}, nil
}

// writeBloomBlock builds the bloom filter directly from bloomBuf/bloomLens
// — no per-key heap allocation beyond the filter bits themselves.
func (w *Writer) writeBloomBlock() (BlockHandle, error) {
	n := len(w.bloomLens)
	var data []byte
	if n == 0 {
		data = (&BloomFilter{bits: []byte{}, k: 0}).Encode()
	} else {
		byteLen := int(math.Ceil(float64(n)*bitsPerKey)) / 8
		if byteLen < 1 {
			byteLen = 1
		}
		k := uint32(math.Round(bitsPerKey * math.Log(2)))
		if k < 1 {
			k = 1
		}
		bloom := &BloomFilter{bits: make([]byte, byteLen), k: k}
		pos := 0
		for _, l := range w.bloomLens {
			bloom.Add(w.bloomBuf[pos : pos+l])
			pos += l
		}
		data = bloom.Encode()
	}

	start := w.offset
	if _, err := w.file.Write(data); err != nil {
		return BlockHandle{}, err
	}
	w.offset += uint64(len(data))
	return BlockHandle{Offset: start, Length: uint64(len(data))}, nil
}

func (w *Writer) writeFooter(indexHandle, bloomHandle BlockHandle) error {
	var buf [FooterSize]byte
	binary.LittleEndian.PutUint64(buf[0:], indexHandle.Offset)
	binary.LittleEndian.PutUint64(buf[8:], indexHandle.Length)
	binary.LittleEndian.PutUint64(buf[16:], bloomHandle.Offset)
	binary.LittleEndian.PutUint64(buf[24:], bloomHandle.Length)
	buf[32] = FormatVersion
	_, err := w.file.Write(buf[:])
	return err
}
