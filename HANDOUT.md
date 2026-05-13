# tinyKV — Next Steps Handout

Benchmarked on: Intel Core i7-1165G7 @ 2.80GHz, linux/amd64, `-benchtime=5s -benchmem`.

Already implemented: xxHash64 bloom filter, per-SkipList PCG PRNG.

---

## 1. Block Cache (LRU)

**Impact:** High — eliminates repeated disk I/O for hot SSTable blocks.

### The problem

Every cold `get` currently reads the SSTable block from disk, even if the same block was accessed moments ago. There is no in-memory caching of decoded blocks.

```
Current cold read: bloom check → index lookup → disk read → decode → return
With block cache:  bloom check → index lookup → cache hit  → return   (no disk)
```

Current cold-read latency: **1,214–1,593 ns/op** (key=16–64 B).

### What to build

- An LRU cache keyed by `(sstable-path, block-offset)`.
- Evict least-recently-used blocks when the cache exceeds a capacity limit (e.g. 8 MB).
- The cache sits in `src/store` and is passed down to each SSTable reader.
- Cache entries hold the decoded `[]byte` block, not raw disk bytes.

### Concurrency note

The cache will be shared across concurrent readers. A `sync.RWMutex` (or a sharded equivalent) guards the map. Promotions (moving an entry to MRU on hit) require a write lock — keep the critical section short.

### Files to touch

| File | Change |
| ---- | ------ |
| `src/store/store.go` | Instantiate cache; pass to `sst.NewReader` |
| `src/sstable/reader.go` | Accept cache; look up before block read; populate on miss |
| `src/store/lru_cache.go` (new) | LRU implementation |

### Measuring success

```bash
go test -bench=BenchmarkGetCold -benchmem -benchtime=5s ./bench/...
```

Target: cold-read latency should approach hot-read latency (~350 ns) for a working set that fits in cache.

---

## 2. Leveled Compaction (L1 / L2)

**Impact:** High — caps read amplification regardless of dataset size; required for production correctness at scale.

### The problem

tinyKV currently has only **L0 compaction**: all SSTables are merged into one. As the dataset grows, more SSTables accumulate at L0 before the next compaction. Every `get` must probe all L0 files. Read amplification is **O(n)** in the number of unflushed SSTables.

```
L0 (current): [sst-5] [sst-4] [sst-3] [sst-2] [sst-1]   ← every get probes all
L1 (goal):    [sst-A: a–m] [sst-B: n–z]                  ← non-overlapping ranges
L2 (goal):    [sst-X: a–f] [sst-Y: g–m] [sst-Z: n–z]    ← denser, non-overlapping
```

### What to build

- **Level 0** stays as-is (files can overlap; sorted by flush time).
- **Levels 1+** contain non-overlapping key ranges. A compaction picks one L0 file and merges it with all overlapping L1 files → writes new non-overlapping L1 files.
- The MANIFEST tracks which level each SSTable belongs to.
- A background goroutine triggers L0→L1 compaction when `len(L0) > threshold` (e.g. 4 files).
- L1→L2 compaction triggers when L1 total size exceeds a limit (e.g. 10 MB).

### Key invariant

Within any level ≥1, SSTable key ranges must **not** overlap. The reader can binary-search the level to find the single candidate file — O(log n) instead of O(n).

### Files to touch

| File | Change |
| ---- | ------ |
| `src/store/manifest.go` | Add `Level int` field to SSTable metadata |
| `src/store/compaction.go` | New file: level-selection, merge logic, output splitting |
| `src/store/store.go` | Wire compaction trigger into the background flush goroutine |
| `src/sstable/reader.go` | Expose min/max key for compaction overlap detection |

### Measuring success

```bash
go test -bench=BenchmarkGetCold -benchmem -benchtime=5s ./bench/...
```

With leveled compaction, cold-read latency should remain flat as the dataset grows beyond a single memtable flush, instead of growing linearly with SSTable count.

---

## 3. mmap SSTable Reads

**Impact:** Medium — removes `read()` syscall overhead from the cold path; lets the OS page cache do the work.

### The problem

`sst.Reader` currently uses `os.File.ReadAt` for every block access. Each call crosses the user/kernel boundary and copies bytes into a Go-managed buffer. For large or frequently-accessed SSTables this adds measurable latency.

### What to build

- On `NewReader`, `mmap` the entire SSTable file into the process address space (`syscall.Mmap` with `PROT_READ | MAP_SHARED`).
- Replace `file.ReadAt(buf, offset)` with a slice into the mapped region: `data[offset : offset+length]`.
- `Close` calls `syscall.Munmap`.
- Zero-copy: the returned block slice points directly into the mmap'd region (read-only).

### Trade-offs

| Pro | Con |
| --- | --- |
| Eliminates `read()` syscall per block | Increases virtual address space usage |
| OS page cache handles eviction automatically | Large files can cause pressure on 32-bit systems (N/A here) |
| Zero-copy slice into mapped region | Slice lifetime tied to the mmap — must not outlive `Close` |

### Files to touch

| File | Change |
| ---- | ------ |
| `src/sstable/reader.go` | Replace `ReadAt` with mmap slice; add `Munmap` to `Close` |

### Measuring success

```bash
go test -bench=BenchmarkGetCold -benchmem -benchtime=5s ./bench/...
go test -bench=BenchmarkSSTGet  -benchmem -benchtime=5s ./bench/...
```

Target: cold-read allocs/op should drop to 0 (no buffer allocation; slice into mmap region).

---

## Known Limitation: Random-Write Performance

Random `put` is **−51% vs LevelDB** at a 10 M-key keyspace. This is a fundamental SkipList property: node pointers are scattered across the heap, so random-key insertion causes cache misses at every level traversal. LevelDB's memtable uses an arena-allocated SkipList with better locality, and its sorted block format further improves compaction write patterns.

Closing this gap would require replacing the SkipList with an Adaptive Radix Tree (ART) or a cache-friendly B+-tree. That is a significant architectural change and is left as a stretch goal.

---

## Quick Reference: Benchmark Commands

```bash
# Full tinyKV suite
go test -bench=. -benchmem -benchtime=5s ./bench/...

# Three-way comparison (requires CGO + LevelDB + RocksDB headers)
cd compare && go test -bench=. -benchmem -benchtime=5s .

# Single benchmark
go test -bench=BenchmarkGetCold -benchmem -benchtime=5s ./bench/...
```
