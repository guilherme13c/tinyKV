// Component-level benchmarks isolate each storage layer so bottlenecks can be
// attributed precisely: SkipList, WAL, and SSTable are tested independently of
// the full store pipeline.
package bench_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/guilherme13c/tinyKV/src/memtable"
	"github.com/guilherme13c/tinyKV/src/sstable"
	w "github.com/guilherme13c/tinyKV/src/wal"
)

// ── SkipList ──────────────────────────────────────────────────────────────────

// BenchmarkSkipListPut measures raw SkipList insertion throughput with no WAL
// or lock overhead.
func BenchmarkSkipListPut(b *testing.B) {
	for _, vs := range valSizes {
		b.Run(fmt.Sprintf("val=%dB", vs), func(b *testing.B) {
			sl := memtable.NewSkipList()
			val := makeVal(vs)
			const ks = 16

			b.SetBytes(int64(ks + vs))
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				_ = sl.Put(makeKey(i, ks), val, false)
			}
		})
	}
}

// BenchmarkSkipListGet measures SkipList lookup throughput against a
// pre-populated list.
func BenchmarkSkipListGet(b *testing.B) {
	const ks = 16
	for _, vs := range valSizes {
		b.Run(fmt.Sprintf("val=%dB", vs), func(b *testing.B) {
			sl := memtable.NewSkipList()
			val := makeVal(vs)
			keys := make([][]byte, seedCount)
			for i := range seedCount {
				keys[i] = makeKey(i, ks)
				_ = sl.Put(keys[i], val, false)
			}

			b.SetBytes(int64(ks + vs))
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				_, _ = sl.Get(keys[i%seedCount])
			}
		})
	}
}

// ── WAL ───────────────────────────────────────────────────────────────────────

// BenchmarkWALAppend measures the raw throughput of appending log entries,
// which is the critical write path for durability.
func BenchmarkWALAppend(b *testing.B) {
	const ks = 16
	for _, vs := range valSizes {
		b.Run(fmt.Sprintf("val=%dB", vs), func(b *testing.B) {
			dir := b.TempDir()
			lw, err := w.NewWriter(filepath.Join(dir, "wal"))
			if err != nil {
				b.Fatal(err)
			}
			defer lw.Close()

			key := makeKey(1, ks)
			val := makeVal(vs)

			b.SetBytes(int64(ks + vs))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				if err := lw.Append(key, val, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ── SSTable ───────────────────────────────────────────────────────────────────

// BenchmarkSSTBulkWrite measures SSTable write throughput: how fast can we
// stream entries into a new table and finalise it.
func BenchmarkSSTBulkWrite(b *testing.B) {
	const ks = 16
	for _, vs := range valSizes {
		b.Run(fmt.Sprintf("val=%dB", vs), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "bench.sst")
			sw, err := sstable.NewWriter(path, 0)
			if err != nil {
				b.Fatal(err)
			}
			val := makeVal(vs)

			b.SetBytes(int64(ks + vs))
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				if err := sw.Append(makeKey(i, ks), val, false); err != nil {
					b.Fatal(err)
				}
			}

			b.StopTimer()
			_ = sw.Close()
		})
	}
}

// BenchmarkSSTGet measures per-lookup cost of the SSTable reader:
// bloom-filter probe → binary-search on index → block I/O → linear scan.
func BenchmarkSSTGet(b *testing.B) {
	const n, ks = 10_000, 16
	for _, vs := range valSizes {
		b.Run(fmt.Sprintf("val=%dB", vs), func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "bench.sst")

			// Build the SSTable once.
			sw, err := sstable.NewWriter(path, 0)
			if err != nil {
				b.Fatal(err)
			}
			val := makeVal(vs)
			keys := make([][]byte, n)
			for i := range n {
				keys[i] = makeKey(i, ks)
				_ = sw.Append(keys[i], val, false)
			}
			if err := sw.Close(); err != nil {
				b.Fatal(err)
			}

			sr, err := sstable.NewReader(path, nil)
			if err != nil {
				b.Fatal(err)
			}
			defer sr.Close()

			b.SetBytes(int64(ks + vs))
			b.ReportAllocs()
			b.ResetTimer()

			for i := range b.N {
				_, _ = sr.Get(keys[i%n])
			}
		})
	}
}

// BenchmarkSSTGetMiss measures SSTable Get for a key that is absent, stressing
// the bloom filter false-negative rate and early-exit path.
func BenchmarkSSTGetMiss(b *testing.B) {
	const n, ks, vs = 10_000, 16, 64

	dir := b.TempDir()
	path := filepath.Join(dir, "bench.sst")

	sw, err := sstable.NewWriter(path, 0)
	if err != nil {
		b.Fatal(err)
	}
	val := makeVal(vs)
	for i := range n {
		_ = sw.Append(makeKey(i, ks), val, false)
	}
	if err := sw.Close(); err != nil {
		b.Fatal(err)
	}

	sr, err := sstable.NewReader(path, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer sr.Close()

	missKey := makeKey(n+1_000_000, ks)

	b.SetBytes(int64(ks))
	b.ReportAllocs()

	for b.Loop() {
		_, _ = sr.Get(missKey)
	}
}
