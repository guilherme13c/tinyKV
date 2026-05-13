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

| Operation        | tinyKV          | LevelDB     | RocksDB      | vs LevelDB | vs RocksDB |
| ---------------- | --------------- | ----------- | ------------ | ---------- | ---------- |
| `put` sequential | **6,503 ns/op** | 9,124 ns/op | 8,879 ns/op  | **+40%**   | **+37%**   |
| `put` random     | 14,795 ns/op    | 7,282 ns/op | 12,400 ns/op | −51%       | −16%       |
| `delete`         | 3,190 ns/op     | 3,141 ns/op | 8,703 ns/op  | −2%        | **+173%**  |
| concurrent `put` | **5,761 ns/op** | 6,264 ns/op | 12,029 ns/op | **+9%**    | **+109%**  |

tinyKV wins sequential puts, concurrent puts, and deletes against RocksDB by a wide margin. Against LevelDB, sequential puts (+40%) and concurrent puts (+9%) favour tinyKV; random puts are slower because the SkipList's pointer-chasing access pattern causes more cache misses than LevelDB's sorted-block layout at a 10 M-key keyspace.

#### Reads

| Operation            | tinyKV          | LevelDB     | RocksDB     | vs LevelDB | vs RocksDB |
| -------------------- | --------------- | ----------- | ----------- | ---------- | ---------- |
| `get` hot (memtable) | **393 ns/op**   | 1,055 ns/op | 2,449 ns/op | **+168%**  | **+523%**  |
| `get` cold (SSTable) | **1,214 ns/op** | 2,021 ns/op | 9,488 ns/op | **+66%**   | **+681%**  |
| `get` miss (bloom)   | **200.8 ns/op** | 431.1 ns/op | 853.5 ns/op | **+115%**  | **+325%**  |

tinyKV dominates reads across all three scenarios. Hot reads are **2.7× faster than LevelDB** and **6.2× faster than RocksDB** — the CGO boundary adds hundreds of nanoseconds on every call; tinyKV is a direct Go function call. Cold reads are **66% faster than LevelDB** and **7.8× faster than RocksDB**. Bloom-filter misses are **2.1× faster than LevelDB** thanks to xxHash64's throughput advantage.

#### Scans

| Range size  | tinyKV        | LevelDB   | RocksDB    | vs LevelDB | vs RocksDB |
| ----------- | ------------- | --------- | ---------- | ---------- | ---------- |
| 100 keys    | **15,632 ns** | 55,526 ns | 116,650 ns | **+255%**  | **+646%**  |
| 1,000 keys  | **127 µs**    | 503 µs    | 1,220 µs   | **+296%**  | **+861%**  |
| 10,000 keys | **1,138 µs**  | 5,249 µs  | 11,485 µs  | **+361%**  | **+909%**  |

Scan allocs/op: tinyKV **13** (constant); LevelDB/RocksDB **202 / 201 per 100 keys** (one allocation per returned entry via CGO). tinyKV's merge iterator pre-allocates the heap once and reuses it for the entire range.

---

### Write throughput by payload size

> `put` sequential, key fixed at 16 B.

| Value size | ns/op   | Throughput | Allocs/op |
| ---------- | ------- | ---------- | --------- |
| 64 B       | 6,146   | 13 MB/s    | 1         |
| 1 KB       | 20,451  | 51 MB/s    | 2         |
| 16 KB      | 100,776 | 163 MB/s   | 4         |

Write cost grows sub-linearly with value size — the WAL write-stealing leader batches concurrent payloads into a single `file.Write()`, amortising syscall overhead across goroutines.

---

### Read latency breakdown (tinyKV, by key size)

| Scenario             | key=16 B | key=64 B | key=256 B | Allocs/op |
| -------------------- | -------- | -------- | --------- | --------- |
| **Hot** (memtable)   | 347 ns   | 332 ns   | 485 ns    | 0         |
| **Cold** (SSTable)   | 1,286 ns | 1,593 ns | 997 ns    | 1–2       |
| **Miss** (not found) | 143 ns   | 141 ns   | 160 ns    | 2         |

**Hot reads** hit the SkipList under a shared read-lock — no allocation, no I/O.  
**Cold reads** add one SSTable binary-search + bloom-filter probe (~700–1,200 ns extra).  
**Misses** short-circuit at the bloom filter before any disk access.

---

### Scan throughput (tinyKV, by range size)

| Range size  | ns/op   | Throughput | Allocs/op |
| ----------- | ------- | ---------- | --------- |
| 100 keys    | 10,173  | 786 MB/s   | 12        |
| 1,000 keys  | 87,821  | 911 MB/s   | 12        |
| 10,000 keys | 848,623 | 943 MB/s   | 12        |

Alloc count stays constant regardless of range size — the merge iterator heap is allocated once per `Scan` call.

---

### Memory efficiency

| Operation    | Allocs/op | Notes                                     |
| ------------ | --------- | ----------------------------------------- |
| `put`        | 1–4       | Arena-pooled slab; 1 alloc per key copy   |
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
