// Package bench contains end-to-end benchmarks for the tinyKV store.
//
// Run all benchmarks:
//
//	go test -bench=. -benchmem ./bench/...
//
// Run with profiling (see run_bench.sh for a full pipeline):
//
//	go test -bench=BenchmarkPutSeq -cpuprofile=cpu.pprof ./bench/...
//	go tool pprof -http=:6060 cpu.pprof
package bench_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/guilherme13c/tinyKV/src/store"
)

// Size parameters swept by sub-benchmarks.
var (
	keySizes = []int{16, 64, 256}
	valSizes = []int{64, 1_024, 16_384}
)

// seedCount is the number of entries pre-written for read/delete benchmarks.
const seedCount = 10_000

// ── helpers ───────────────────────────────────────────────────────────────────

// openStore creates a Store in dir and returns it.  Caller is responsible for
// calling Close() (or delegating to flushToDisk).
func openStore(b testing.TB, dir string) *store.Store {
	b.Helper()
	s, err := store.NewStore(filepath.Join(dir, "wal"), dir)
	if err != nil {
		b.Fatal(err)
	}
	return s
}

// makeKey returns a keySize-byte slice encoding i as a big-endian uint64 in the
// final 8 bytes, so keys sort numerically.
func makeKey(i, keySize int) []byte {
	k := make([]byte, keySize)
	if keySize >= 8 {
		binary.BigEndian.PutUint64(k[keySize-8:], uint64(i))
	} else {
		binary.BigEndian.PutUint32(k[keySize-4:], uint32(i))
	}
	return k
}

// makeVal returns a valSize-byte slice filled with random data.
func makeVal(valSize int) []byte {
	v := make([]byte, valSize)
	_, _ = rand.Read(v)
	return v
}

// seedStore writes n entries (sequential keys, fixed value) to s and returns
// the written keys.
func seedStore(b *testing.B, s *store.Store, n, keySize, valSize int) [][]byte {
	b.Helper()
	val := makeVal(valSize)
	keys := make([][]byte, n)
	for i := range n {
		keys[i] = makeKey(i, keySize)
		if err := s.Put(keys[i], val); err != nil {
			b.Fatal(err)
		}
	}
	return keys
}

// flushToDisk closes s (which synchronously flushes the memtable to SSTable)
// and reopens a fresh Store over the same directory.  The returned Store has
// an empty memtable; all reads hit SSTables on disk.
func flushToDisk(b *testing.B, s *store.Store, dir string) *store.Store {
	b.Helper()
	if err := s.Close(); err != nil {
		b.Fatal(err)
	}
	s2, err := store.NewStore(filepath.Join(dir, "wal"), dir)
	if err != nil {
		b.Fatal(err)
	}
	return s2
}

// ── write benchmarks ──────────────────────────────────────────────────────────

// BenchmarkPutSeq measures sequential Put throughput (key-0, key-1, …).
func BenchmarkPutSeq(b *testing.B) {
	for _, ks := range keySizes {
		for _, vs := range valSizes {
			b.Run(fmt.Sprintf("key=%dB/val=%dB", ks, vs), func(b *testing.B) {
				dir := b.TempDir()
				s := openStore(b, dir)
				defer s.Close()

				val := makeVal(vs)

				b.SetBytes(int64(ks + vs))
				b.ReportAllocs()
				b.ResetTimer()

				for i := range b.N {
					if err := s.Put(makeKey(i, ks), val); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkPutRandom measures random-key Put throughput.
func BenchmarkPutRandom(b *testing.B) {
	const keySpace = 10_000_000
	for _, ks := range keySizes {
		for _, vs := range valSizes {
			b.Run(fmt.Sprintf("key=%dB/val=%dB", ks, vs), func(b *testing.B) {
				dir := b.TempDir()
				s := openStore(b, dir)
				defer s.Close()

				val := makeVal(vs)

				b.SetBytes(int64(ks + vs))
				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					if err := s.Put(makeKey(rand.Intn(keySpace), ks), val); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// ── read benchmarks ───────────────────────────────────────────────────────────

// BenchmarkGetHot measures Get throughput when data lives in the active
// MemTable (no disk I/O).  Only small values are used to keep setup fast and
// ensure the entire dataset fits in the memtable without triggering a flush.
func BenchmarkGetHot(b *testing.B) {
	// Use only small values: large values would exceed the 4 MB flush threshold
	// and spawn background flush goroutines that contaminate allocation counts.
	for _, ks := range keySizes {
		for _, vs := range []int{64, 256} {
			b.Run(fmt.Sprintf("key=%dB/val=%dB", ks, vs), func(b *testing.B) {
				dir := b.TempDir()
				s := openStore(b, dir)
				defer s.Close()

				keys := seedStore(b, s, seedCount, ks, vs)

				b.SetBytes(int64(ks + vs))
				b.ReportAllocs()
				b.ResetTimer()

				for i := range b.N {
					if _, err := s.Get(keys[i%seedCount]); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkGetCold measures Get throughput when data has been flushed to
// SSTable (bloom filter + index lookup + block I/O).
func BenchmarkGetCold(b *testing.B) {
	for _, ks := range keySizes {
		for _, vs := range valSizes {
			b.Run(fmt.Sprintf("key=%dB/val=%dB", ks, vs), func(b *testing.B) {
				dir := b.TempDir()
				s := openStore(b, dir)
				keys := seedStore(b, s, seedCount, ks, vs)
				s = flushToDisk(b, s, dir)
				defer s.Close()

				b.SetBytes(int64(ks + vs))
				b.ReportAllocs()
				b.ResetTimer()

				for i := range b.N {
					if _, err := s.Get(keys[i%seedCount]); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkGetMiss measures Get latency for keys that do not exist (bloom
// filter is the first line of defence on cold storage).
func BenchmarkGetMiss(b *testing.B) {
	for _, ks := range keySizes {
		b.Run(fmt.Sprintf("key=%dB", ks), func(b *testing.B) {
			dir := b.TempDir()
			s := openStore(b, dir)
			seedStore(b, s, seedCount, ks, 64)
			s = flushToDisk(b, s, dir)
			defer s.Close()

			// A key that was never written.
			missKey := makeKey(seedCount+1_000_000, ks)

			b.SetBytes(int64(ks))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_, _ = s.Get(missKey)
			}
		})
	}
}

// ── delete benchmarks ─────────────────────────────────────────────────────────

// BenchmarkDelete measures Delete (tombstone write) throughput.
// Deletes are recycled over a fixed key pool; flushToDisk ensures no
// background flush goroutines pollute the allocation counters.
func BenchmarkDelete(b *testing.B) {
	const pool = 10_000
	for _, ks := range keySizes {
		b.Run(fmt.Sprintf("key=%dB", ks), func(b *testing.B) {
			dir := b.TempDir()
			s := openStore(b, dir)
			seedStore(b, s, pool, ks, 64)
			s = flushToDisk(b, s, dir)
			defer s.Close()

			b.SetBytes(int64(ks))
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				_ = s.Delete(makeKey(i%pool, ks))
			}
		})
	}
}

// ── scan benchmarks ───────────────────────────────────────────────────────────

// BenchmarkScan measures range-scan throughput for small, medium, and large
// result sets.  Data is flushed to SSTable so the merge-iterator exercises
// the SSTable path.
func BenchmarkScan(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			const ks, vs = 16, 64
			dir := b.TempDir()
			s := openStore(b, dir)
			seedStore(b, s, n, ks, vs)
			s = flushToDisk(b, s, dir)
			defer s.Close()

			startKey := makeKey(0, ks)
			endKey := makeKey(n, ks)

			b.SetBytes(int64(n) * (ks + vs))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				it, err := s.Scan(startKey, endKey)
				if err != nil {
					b.Fatal(err)
				}
				for ; it.Valid(); it.Next() {
				}
				_ = it.Close()
			}
		})
	}
}
