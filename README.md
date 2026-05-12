# tinyKV

A small, pedagogical [LSM-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) key-value store written in Go — no external dependencies, pure stdlib.

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
- Zero external dependencies — pure Go stdlib

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
> Baseline key: 16 B, value: 64 B.

| Operation        | tinyKV          | LevelDB     | RocksDB     | vs LevelDB | vs RocksDB |
| ---------------- | --------------- | ----------- | ----------- | ---------- | ---------- |
| `put` sequential | 4,017 ns/op     | 2,621 ns/op | 4,666 ns/op | −35%       | **+14%**   |
| `put` random     | 4,335 ns/op     | 3,355 ns/op | 6,997 ns/op | −23%       | **+38%**   |
| `delete`         | **2,377 ns/op** | 2,554 ns/op | 5,594 ns/op | **+7%**    | **+57%**   |
| concurrent `put` | 4,494 ns/op     | 4,356 ns/op | 5,586 ns/op | −3%        | **+19%**   |

tinyKV **beats LevelDB on deletes** and **beats RocksDB on every operation**, despite being a pure-Go, zero-dependency implementation. The write path bottleneck vs. LevelDB is key-comparison overhead in the SkipList (`bytes.Compare` vs. LevelDB's inlined comparator).

---

### Write throughput by payload size

> `put` sequential, 8 goroutines, key fixed at 16 B.

| Value size | ns/op   | Throughput  | Allocs/op |
| ---------- | ------- | ----------- | --------- |
| 64 B       | 3,622   | 22 MB/s     | 1         |
| 1 KB       | 11,306  | 92 MB/s     | 2         |
| 16 KB      | 82,362  | 199 MB/s    | 3         |

Write cost grows sub-linearly with value size — the WAL write-stealing leader batches concurrent payloads into a single `file.Write()`, amortising syscall overhead across goroutines.

---

### Read latency breakdown

| Scenario              | key=16 B | key=64 B | key=256 B | Allocs/op |
| --------------------- | -------- | -------- | --------- | --------- |
| **Hot** (memtable)    | 249 ns   | 237 ns   | 360 ns    | 0         |
| **Cold** (SSTable)    | 864 ns   | 980 ns   | 1,455 ns  | 1–2       |
| **Miss** (not found)  | 241 ns   | 223 ns   | 668 ns    | 2         |

**Hot reads** hit the SkipList under a shared read-lock — no allocation, no I/O.  
**Cold reads** add one SSTable binary-search + bloom-filter probe (~700 ns extra).  
**Misses** are nearly as fast as hot reads: the bloom filter rejects them before any disk access.

---

### Scan throughput

| Range size | ns/op     | Throughput  |
| ---------- | --------- | ----------- |
| 100 keys   | 9,447     | 847 MB/s    |
| 1,000 keys | 80,693    | 991 MB/s    |
| 10,000 keys| 728,717   | 1,098 MB/s  |

Scan throughput scales well because the iterator merges a sorted SkipList with sorted SSTable blocks — nearly sequential memory access once the SSTable index is warm.

---

### Memory efficiency

| Operation    | Allocs/op | Notes                                     |
| ------------ | --------- | ----------------------------------------- |
| `put`        | 1–3       | Arena-pooled slab; 1 alloc per key copy   |
| `get` (hot)  | 0         | Returns slice into arena; zero allocation |
| `get` (cold) | 1–2       | One slice for the decoded value           |
| `delete`     | 1         | Tombstone key copy only                   |
| `scan`       | 12        | Iterator + merge heap allocation          |

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
