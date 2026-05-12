package bench_test

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
)

// workload drives a mix of Gets and Puts at a configurable read percentage.
type workload struct {
	name    string
	readPct int // 0–100; fraction of operations that are Gets
	keyPool int // number of distinct keys in circulation
	keySize int
	valSize int
}

var workloads = []workload{
	{"WriteHeavy_5r95w", 5, 50_000, 16, 256},
	{"Balanced_50r50w", 50, 50_000, 16, 256},
	{"ReadHeavy_95r5w", 95, 50_000, 16, 256},
}

// BenchmarkWorkload runs a single-goroutine mixed read/write workload against a
// pre-seeded store.  Use -cpu=1,2,4,8 together with BenchmarkConcurrentMixed
// to explore multi-core behaviour.
func BenchmarkWorkload(b *testing.B) {
	for _, wl := range workloads {
		b.Run(wl.name, func(b *testing.B) {
			dir := b.TempDir()
			s := openStore(b, dir)

			// Seed half the pool so reads have something to hit.
			keys := seedStore(b, s, wl.keyPool/2, wl.keySize, wl.valSize)
			s = flushToDisk(b, s, dir)
			defer s.Close()

			val := makeVal(wl.valSize)

			b.SetBytes(int64(wl.keySize + wl.valSize))
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				key := keys[rand.Intn(len(keys))]
				if rand.Intn(100) < wl.readPct {
					_, _ = s.Get(key)
				} else {
					_ = s.Put(key, val)
				}
			}
		})
	}
}

// BenchmarkConcurrentPut measures Put throughput when multiple goroutines write
// simultaneously.  Control parallelism with: go test -bench=. -cpu=1,2,4,8
func BenchmarkConcurrentPut(b *testing.B) {
	const ks, vs = 16, 256

	dir := b.TempDir()
	s := openStore(b, dir)
	defer s.Close()

	val := makeVal(vs)
	var counter atomic.Int64

	b.SetBytes(int64(ks + vs))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(counter.Add(1))
			_ = s.Put(makeKey(i, ks), val)
		}
	})
}

// BenchmarkConcurrentGet measures Get throughput with multiple goroutines
// reading from a pre-populated SSTable.
func BenchmarkConcurrentGet(b *testing.B) {
	const ks, vs = 16, 256

	dir := b.TempDir()
	s := openStore(b, dir)
	keys := seedStore(b, s, seedCount, ks, vs)
	s = flushToDisk(b, s, dir)
	defer s.Close()

	var counter atomic.Int64

	b.SetBytes(int64(ks + vs))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(counter.Add(1)) % seedCount
			_, _ = s.Get(keys[i])
		}
	})
}

// BenchmarkConcurrentMixed benchmarks concurrent readers and writers at
// several read percentages.  Run with -cpu=1,2,4,8 to see scaling behaviour.
func BenchmarkConcurrentMixed(b *testing.B) {
	const ks, vs = 16, 256

	for _, readPct := range []int{20, 50, 80} {
		b.Run(fmt.Sprintf("read=%d%%", readPct), func(b *testing.B) {
			dir := b.TempDir()
			s := openStore(b, dir)
			keys := seedStore(b, s, seedCount, ks, vs)
			s = flushToDisk(b, s, dir)
			defer s.Close()

			val := makeVal(vs)
			var counter atomic.Int64

			b.SetBytes(int64(ks + vs))
			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				local := rand.New(rand.NewSource(rand.Int63()))
				for pb.Next() {
					i := int(counter.Add(1)) % seedCount
					key := keys[i]
					if local.Intn(100) < readPct {
						_, _ = s.Get(key)
					} else {
						_ = s.Put(key, val)
					}
				}
			})
		})
	}
}

// BenchmarkCompactionTrigger measures write throughput across a workload large
// enough to fill the memtable multiple times, triggering background flushes
// and at least one compaction cycle.
func BenchmarkCompactionTrigger(b *testing.B) {
	const ks, vs = 16, 512 // ~500 B per entry; 4 MB threshold ≈ 8 k entries per flush

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
}
