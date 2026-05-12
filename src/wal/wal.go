// Package wal
package wal

type LogWriterI interface {
	Append(key []byte, value []byte, isTombstone bool) error
	Close() error
}

type LogReaderI interface {
	Next() (*LogEntry, error)
	Close() error
}
