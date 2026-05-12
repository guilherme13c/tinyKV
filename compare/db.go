// Package compare provides a common interface and adapters used to benchmark
// tinyKV, LevelDB, and RocksDB under identical workloads.
package compare

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

// DB is the minimal key-value interface implemented by all three engines.
type DB interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Scan(start, end []byte) (Iterator, error)
	Close() error
}

// Iterator is a forward-only range iterator returned by Scan.
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Close() error
}

// ── workload helpers (mirrored from bench/bench_test.go) ──────────────────────

const (
	KeySize   = 16
	ValSize   = 64
	SeedCount = 10_000
)

func MakeKey(i, keySize int) []byte {
	k := make([]byte, keySize)
	binary.BigEndian.PutUint64(k[keySize-8:], uint64(i))
	return k
}

func MakeVal(valSize int) []byte {
	v := make([]byte, valSize)
	_, _ = rand.Read(v)
	return v
}

// SeedDB writes n entries with sequential keys and a fixed value into db.
// Returns the written keys.
func SeedDB(b *testing.B, db DB, n, keySize, valSize int) [][]byte {
	b.Helper()
	val := MakeVal(valSize)
	keys := make([][]byte, n)
	for i := range n {
		keys[i] = MakeKey(i, keySize)
		if err := db.Put(keys[i], val); err != nil {
			b.Fatal(err)
		}
	}
	return keys
}
