package wal

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type LogReader struct {
	file   *os.File
	reader *bufio.Reader
}

func NewLogReader(path string) (*LogReader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}

	return &LogReader{
		file:   f,
		reader: bufio.NewReader(f),
	}, nil
}

// Next decodes the next entry from the log.
// Returns io.EOF when the log is fully consumed or a truncated tail is detected.
func (lr *LogReader) Next() (*LogEntry, error) {
	keyLen, err := binary.ReadUvarint(lr.reader)
	if err != nil {
		// io.EOF = clean end; anything else = truncated tail from a crash.
		// Both cases signal the end of the valid log.
		return nil, io.EOF
	}

	valueMeta, err := binary.ReadUvarint(lr.reader)
	if err != nil {
		return nil, io.EOF
	}

	isTombstone := valueMeta&1 == 1
	valueLen := valueMeta >> 1

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(lr.reader, key); err != nil {
		return nil, io.EOF
	}

	var value []byte
	if !isTombstone {
		value = make([]byte, valueLen)
		if _, err := io.ReadFull(lr.reader, value); err != nil {
			return nil, io.EOF
		}
	}

	return &LogEntry{Key: key, Value: value, IsTombstone: isTombstone}, nil
}

func (lr *LogReader) Close() error {
	return lr.file.Close()
}
