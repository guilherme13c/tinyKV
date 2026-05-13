# tinyKV

A small, pedagogical [LSM-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) key-value store written in Go with minimal dependencies.

Built to be readable first and to illustrate the core ideas behind production stores like LevelDB, RocksDB, and Cassandra — without the production complexity that makes those systems hard to learn from.

---

## Features

- **Write-Ahead Log (WAL)** with write-stealing leader election for low-latency durable writes
- **SkipList MemTable** for fast in-memory writes (O(log n))
- **Immutable SSTables** with bloom filter and binary-search index block
- **Background flush** — writes are never blocked by I/O
- **Full L0 compaction** — tombstone-safe merge of all SSTables
- **Crash recovery** — WAL replay on startup, including interrupted flushes
- **Interactive REPL** and non-interactive (pipe) modes
- Minimal external dependencies — one tiny package ([`xxhash`](https://github.com/cespare/xxhash)) for bloom-filter hashing; no generated code or encoding libraries

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                         main.go                         │
│          REPL: put / get / delete / scan / exit         │
└───────────────────────────┬─────────────────────────────┘
                            │ StoreI
┌───────────────────────────▼─────────────────────────────┐
│                      src/store/                         │
│  ┌──────────────┐  ┌─────────────┐   ┌───────────────┐  │
│  │  MemTable    │  │  Immutable  │   │  SSTable(s)   │  │
│  │  (SkipList)  │  │  MemTable   │   │  [newest→old] │  │
│  └──────┬───────┘  └──────┬──────┘   └───────┬───────┘  │
│         │   freeze/flush  │                  │          │
│  ┌──────▼───────┐         │          ┌───────▼───────┐  │
│  │     WAL      │         │          │   MANIFEST    │  │
│  │  (src/wal/)  │         │          │  (JSON log)   │  │
│  └──────────────┘         │          └───────────────┘  │
└───────────────────────────┴─────────────────────────────┘
```

| Package        | Concept                                                         |
| -------------- | --------------------------------------------------------------- |
| `src/wal`      | Append-only write-ahead log with write-stealing leader election |
| `src/memtable` | In-memory SkipList — mutable and immutable                      |
| `src/sstable`  | Sorted String Table: writer, reader, bloom filter               |
| `src/store`    | Orchestrates all components; exposes the public API             |

Full architecture documentation is in [`docs/README.md`](docs/README.md).

---

## Getting Started

**Requirements:** Go 1.21+

```bash
# Clone
git clone https://github.com/guilherme13c/tinyKV.git
cd tinyKV

# Build
go build -o tinyKV .

# Run (default data directory: ./data/)
./tinyKV

# Custom data directory
./tinyKV -dir /path/to/mydb
```

---

## Usage

```
tinyKV — commands: put <key> <value> | get <key> | delete <key> | scan <start> <end> | exit
```

### Interactive REPL

```
> put hello world
ok
> get hello
world
> put greeting hello world
ok
> get greeting
hello world
> scan a z
  greeting = hello world
  hello = world
> delete hello
ok
> get hello
(not found)
> scan a z
  greeting = hello world
> exit
```

### Non-interactive (pipe)

```bash
printf 'put a 1\nput b 2\nscan a z\nexit\n' | ./tinyKV -dir /tmp/mydb
```

### Key/value constraints

| Constraint                                  | Reason                            |
| ------------------------------------------- | --------------------------------- |
| Keys **cannot** contain spaces              | `SplitN` stops at the first space |
| Values **can** contain spaces               | Split is limited to 2 delimiters  |
| Keys and values **cannot** contain newlines | Scanner splits on `\n`            |

---

## On-Disk Layout

```
<dir>/
├── wal               ← active Write-Ahead Log
├── wal.immutable     ← WAL being flushed (present only during a background flush)
├── MANIFEST          ← ordered list of live SSTable paths
└── <nanoseconds>.sst ← one SSTable per memtable flush
```

---

## Testing

```bash
# Unit tests
go test ./...

# End-to-end tests
go test ./e2e/...

# Benchmarks
go test -bench=. -benchmem -benchtime=5s ./bench/...
```

---

## Benchmarks

All results: Intel Core i7-1165G7 @ 2.80GHz, linux/amd64, 8 logical cores, `-benchtime=5s -benchmem`.

### Three-way comparison: tinyKV vs LevelDB vs RocksDB

> LevelDB and RocksDB figures use their CGO bindings at identical settings (sync=false).
> Baseline: key=16 B, value=64 B.

#### Writes

| Operation        | tinyKV          | LevelDB     | RocksDB     | vs LevelDB | vs RocksDB |
| ---------------- | --------------- | ----------- | ----------- | ---------- | ---------- |
| `put` sequential | 4,567 ns/op     | 3,284 ns/op | 5,398 ns/op | −28%       | **+18%**   |
| `put` random     | 5,234 ns/op     | 4,235 ns/op | 8,461 ns/op | −19%       | **+62%**   |
| `delete`         | **2,806 ns/op** | 3,184 ns/op | 7,188 ns/op | **+13%**   | **+156%**  |
| concurrent `put` | 5,408 ns/op     | 5,378 ns/op | 9,236 ns/op | ~0%        | **+71%**   |

tinyKV beats RocksDB on every write operation. Against LevelDB, sequential and random puts are slower — LevelDB's sorted-block format and write-batch coalescing give it an edge in write-heavy microbenchmarks, while the SkipList's pointer-chasing causes more cache misses at a 10 M-key keyspace. Deletes favour tinyKV (+13% over LevelDB) and concurrent puts are essentially tied.

#### Reads

| Operation            | tinyKV        | LevelDB       | RocksDB       | vs LevelDB | vs RocksDB   |
| -------------------- | ------------- | ------------- | ------------- | ---------- | ------------ |
| `get` hot (memtable) | **242 ns/op** | 762 ns/op     | 1,903 ns/op   | **+215%**  | **+686%**    |
| `get` cold (SSTable) | **432 ns/op** | 1,492 ns/op   | 6,369 ns/op   | **+245%**  | **+1,374%**  |
| `get` miss (bloom)   | **164 ns/op** | 331 ns/op     | 606 ns/op     | **+102%**  | **+269%**    |

tinyKV dominates reads across all three scenarios. Hot reads are **3.1× faster than LevelDB** and **7.9× faster than RocksDB** — the CGO boundary adds hundreds of nanoseconds on every call; tinyKV is a direct Go function call. Cold reads are **3.5× faster than LevelDB** and **14.7× faster than RocksDB** thanks to the LRU block cache eliminating repeat disk I/O for hot SSTable blocks. Bloom-filter misses are **2× faster than LevelDB** thanks to xxHash64's throughput advantage.

#### Scans

| Range size  | tinyKV          | LevelDB       | RocksDB       | vs LevelDB | vs RocksDB   |
| ----------- | --------------- | ------------- | ------------- | ---------- | ------------ |
| 100 keys    | **9,928 ns**    | 46,526 ns     | 79,379 ns     | **+369%**  | **+700%**    |
| 1,000 keys  | **80,589 ns**   | 441,305 ns    | 984,778 ns    | **+447%**  | **+1,121%**  |
| 10,000 keys | **842,013 ns**  | 4,286,052 ns  | 9,230,844 ns  | **+409%**  | **+996%**    |

Scan allocs/op: tinyKV **10** (constant); LevelDB/RocksDB **202 / 201 per 100 keys** (one allocation per returned entry via CGO). tinyKV's merge iterator pre-allocates the heap once and reuses it for the entire range.

---

### Write throughput by payload size

> `put` sequential, key fixed at 16 B.

| Value size | ns/op   | Throughput | Allocs/op |
| ---------- | ------- | ---------- | --------- |
| 64 B       | 3,934   | 20 MB/s    | 1         |
| 1 KB       | 18,976  | 55 MB/s    | 4         |
| 16 KB      | 129,700 | 126 MB/s   | 9         |

Write cost grows sub-linearly with value size — the WAL write-stealing leader batches concurrent payloads into a single `file.Write()`, amortising syscall overhead across goroutines.

---

### Read latency breakdown (tinyKV, by key size)

| Scenario             | key=16 B | key=64 B | key=256 B | Allocs/op |
| -------------------- | -------- | -------- | --------- | --------- |
| **Hot** (memtable)   | 243 ns   | 258 ns   | 366 ns    | 0         |
| **Cold** (SSTable)   | 370 ns   | 503 ns   | 564 ns    | 1         |
| **Miss** (not found) | 136 ns   | 149 ns   | 167 ns    | 2         |

**Hot reads** hit the SkipList under a shared read-lock — no allocation, no I/O.  
**Cold reads** hit the LRU block cache on warm accesses, closing the gap with hot-read latency (~370–564 ns vs 243–366 ns hot).  
**Misses** short-circuit at the bloom filter before any disk access.

---

### Scan throughput (tinyKV, by range size)

| Range size  | ns/op   | Throughput | Allocs/op |
| ----------- | ------- | ---------- | --------- |
| 100 keys    | 8,360   | 957 MB/s   | 9         |
| 1,000 keys  | 74,215  | 1,078 MB/s | 9         |
| 10,000 keys | 785,818 | 1,018 MB/s | 9         |

Alloc count stays constant regardless of range size — the merge iterator heap is allocated once per `Scan` call.

---

### Memory efficiency

| Operation    | Allocs/op | Notes                                     |
| ------------ | --------- | ----------------------------------------- |
| `put`        | 1–4       | Arena-pooled slab; 1 alloc per key copy   |
| `get` (hot)  | 0         | Returns slice into arena; zero allocation |
| `get` (cold) | 1–2       | One slice for the decoded value           |
| `delete`     | 1         | Tombstone key copy only                   |
| `scan`       | 9         | Iterator + merge heap allocation          |

The arena pool (`chan []skipListNode`, capacity 4) eliminates per-node allocations in the SkipList and survives GC cycles — unlike `sync.Pool`, which is cleared at every GC, it is not emptied during flush-cycle pauses.

---

### Running the benchmarks yourself

```bash
# tinyKV micro-benchmarks (all operations, all sizes)
go test -bench=. -benchmem -benchtime=5s ./bench/...

# Three-way comparison (requires CGO + LevelDB + RocksDB headers)
cd compare && go test -bench=. -benchmem -benchtime=5s .
```

---

## License

tinyKV is licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE).

- **Free to use** for open-source projects — your code must also be AGPL-3.0.
- **Commercial use** with closed source requires a separate commercial license from the author.
- This dual-licensing model means: open source stays open; proprietary users pay.

For commercial licensing inquiries, open an issue or contact **guilherme13c** via GitHub.
