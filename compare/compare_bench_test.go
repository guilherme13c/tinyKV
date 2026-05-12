// Package compare runs the same benchmark workloads against tinyKV, LevelDB,
// and RocksDB so their performance can be compared on identical hardware.
//
// Run:
//
//	cd compare && go test -bench=. -benchmem -benchtime=5s ./...
//
// Configuration applied to all three engines to make comparisons fair:
//   - write_buffer_size = 4 MB  (tinyKV's memtable flush threshold)
//   - bloom filter (10 bits/key)
//   - GetHot: data written < 4 MB so all three stay in their write buffer
//   - GetCold/GetMiss: db closed and reopened with block cache disabled
package compare_test

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/guilherme13c/tinyKV/compare"
)

// ── engine registry ───────────────────────────────────────────────────────────

type opener struct {
	name   string
	open   func(dir string) (compare.DB, error)
	reopen func(db compare.DB) (compare.DB, error)
}

var engines = []opener{
	{"tinyKV", compare.OpenTinyKV, compare.ReopenTinyKV},
	{"LevelDB", compare.OpenLevelDB, compare.ReopenLevelDB},
	{"RocksDB", compare.OpenRocksDB, compare.ReopenRocksDB},
}

// ── write benchmarks ──────────────────────────────────────────────────────────

func BenchmarkPutSeq(b *testing.B) {
	val := compare.MakeVal(compare.ValSize)
	for _, e := range engines {
		b.Run(e.name, func(b *testing.B) {
			db, err := e.open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			b.SetBytes(int64(compare.KeySize + compare.ValSize))
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				if err := db.Put(compare.MakeKey(i, compare.KeySize), val); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPutRandom(b *testing.B) {
	const keySpace = 10_000_000
	val := compare.MakeVal(compare.ValSize)
	for _, e := range engines {
		b.Run(e.name, func(b *testing.B) {
			db, err := e.open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			b.SetBytes(int64(compare.KeySize + compare.ValSize))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				k := compare.MakeKey(rand.Intn(keySpace), compare.KeySize)
				if err := db.Put(k, val); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ── read benchmarks ───────────────────────────────────────────────────────────

// BenchmarkGetHot: data stays in the write buffer / memtable of each engine.
// seedCount × (KeySize+ValSize) = 10k × 80 B = ~800 KB < 4 MB write_buffer,
// so no flush is triggered for any engine.
func BenchmarkGetHot(b *testing.B) {
	for _, e := range engines {
		b.Run(e.name, func(b *testing.B) {
			db, err := e.open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			keys := compare.SeedDB(b, db, compare.SeedCount, compare.KeySize, compare.ValSize)

			b.SetBytes(int64(compare.KeySize + compare.ValSize))
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				if _, err := db.Get(keys[i%compare.SeedCount]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGetCold: close → reopen with block cache disabled → read from disk.
func BenchmarkGetCold(b *testing.B) {
	for _, e := range engines {
		b.Run(e.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := e.open(dir)
			if err != nil {
				b.Fatal(err)
			}

			keys := compare.SeedDB(b, db, compare.SeedCount, compare.KeySize, compare.ValSize)

			db, err = e.reopen(db)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			b.SetBytes(int64(compare.KeySize + compare.ValSize))
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				if _, err := db.Get(keys[i%compare.SeedCount]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGetMiss: bloom filter rejection — key never written.
func BenchmarkGetMiss(b *testing.B) {
	missKey := compare.MakeKey(compare.SeedCount+1_000_000, compare.KeySize)
	for _, e := range engines {
		b.Run(e.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := e.open(dir)
			if err != nil {
				b.Fatal(err)
			}

			compare.SeedDB(b, db, compare.SeedCount, compare.KeySize, compare.ValSize)

			db, err = e.reopen(db)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			b.SetBytes(int64(compare.KeySize))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				_, _ = db.Get(missKey)
			}
		})
	}
}

// ── delete benchmarks ─────────────────────────────────────────────────────────

func BenchmarkDelete(b *testing.B) {
	const pool = 10_000
	for _, e := range engines {
		b.Run(e.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := e.open(dir)
			if err != nil {
				b.Fatal(err)
			}

			compare.SeedDB(b, db, pool, compare.KeySize, compare.ValSize)

			db, err = e.reopen(db)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			b.SetBytes(int64(compare.KeySize))
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				_ = db.Delete(compare.MakeKey(i%pool, compare.KeySize))
			}
		})
	}
}

// ── scan benchmarks ───────────────────────────────────────────────────────────

func BenchmarkScan(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000} {
		for _, e := range engines {
			b.Run(fmt.Sprintf("n=%d/%s", n, e.name), func(b *testing.B) {
				dir := b.TempDir()
				db, err := e.open(dir)
				if err != nil {
					b.Fatal(err)
				}

				compare.SeedDB(b, db, n, compare.KeySize, compare.ValSize)

				db, err = e.reopen(db)
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()

				startKey := compare.MakeKey(0, compare.KeySize)
				endKey := compare.MakeKey(n, compare.KeySize)

				b.SetBytes(int64(n) * (compare.KeySize + compare.ValSize))
				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					it, err := db.Scan(startKey, endKey)
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
}

// ── concurrent benchmarks ─────────────────────────────────────────────────────

func BenchmarkConcurrentPut(b *testing.B) {
	val := compare.MakeVal(compare.ValSize)
	for _, e := range engines {
		b.Run(e.name, func(b *testing.B) {
			db, err := e.open(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			var counter atomic.Int64

			b.SetBytes(int64(compare.KeySize + compare.ValSize))
			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := int(counter.Add(1))
					_ = db.Put(compare.MakeKey(i, compare.KeySize), val)
				}
			})
		})
	}
}
