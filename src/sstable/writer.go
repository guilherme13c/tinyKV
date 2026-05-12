package sstable

import (
	"encoding/binary"
	"os"
)

type Writer struct {
	file         *os.File
	offset       uint64
	dataBuf      []byte
	lastKey      []byte
	indexKeys    [][]byte
	indexHandles []BlockHandle
	bloomKeys    [][]byte
	blockStart   uint64
}

func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file:         f,
		dataBuf:      make([]byte, 0, BlockSize),
		indexKeys:    make([][]byte, 0),
		indexHandles: make([]BlockHandle, 0),
		bloomKeys:    make([][]byte, 0),
	}, nil
}

func (w *Writer) Append(key, value []byte, isTombstone bool) error {
	valueMeta := uint64(len(value)) << 1
	if isTombstone {
		valueMeta |= 1
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
	w.bloomKeys = append(w.bloomKeys, append([]byte(nil), key...))

	if len(w.dataBuf) >= BlockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *Writer) Close() error {
	if err := w.flushBlock(); err != nil {
		return err
	}

	indexHandle, err := w.writeIndexBlock()
	if err != nil {
		return err
	}

	bloomHandle, err := w.writeBloomBlock()
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

	n, err := w.file.Write(w.dataBuf)
	if err != nil {
		return err
	}

	w.indexKeys = append(w.indexKeys, append([]byte(nil), w.lastKey...))

	w.indexHandles = append(w.indexHandles, BlockHandle{Offset: w.blockStart, Length: uint64(n)})

	w.offset += uint64(n)
	w.blockStart = w.offset
	w.dataBuf = w.dataBuf[:0]
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

func (w *Writer) writeBloomBlock() (BlockHandle, error) {
	bloom := newBloomFilter(w.bloomKeys)
	data := bloom.Encode()

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
	_, err := w.file.Write(buf[:])
	return err
}
