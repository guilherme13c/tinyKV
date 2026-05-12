// Package memtable implements a memtable
package memtable

type MemTableI interface {
	Put(key []byte, value []byte, isTombstone bool) error
	// Get returns the value, or KeyNotFoundError. Tombstoned keys also return KeyNotFoundError.
	// Use Lookup when you need to distinguish "not present" from "tombstoned".
	Get(key []byte) ([]byte, error)
	// Lookup returns (value, found, isTombstone). Does not collapse tombstones into errors.
	Lookup(key []byte) (value []byte, found bool, isTombstone bool)
	SizeInBytes() int
	Iterator() MemTableIteratorI
}

type MemTableIteratorI interface {
	Seek(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	IsTombstone() bool
	Close() error
}
