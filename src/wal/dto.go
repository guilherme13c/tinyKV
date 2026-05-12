package wal

type LogEntry struct {
	Key         []byte
	Value       []byte
	IsTombstone bool
}
