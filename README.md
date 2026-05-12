# tinyKV

A small, pedagogical [LSM-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) key-value store written in Go — no external dependencies, pure stdlib.

Built to be readable first and to illustrate the core ideas behind production stores like LevelDB, RocksDB, and Cassandra — without the production complexity that makes those systems hard to learn from.

---

## Features

- **Write-Ahead Log (WAL)** with group-commit fsync for durability
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
│  ┌──────────────┐  ┌─────────────┐  ┌───────────────┐  │
│  │  MemTable    │  │  Immutable  │  │  SSTable(s)   │  │
│  │  (SkipList)  │  │  MemTable   │  │  [newest→old] │  │
│  └──────┬───────┘  └──────┬──────┘  └───────┬───────┘  │
│         │   freeze/flush  │                  │          │
│  ┌──────▼───────┐         │          ┌───────▼───────┐  │
│  │     WAL      │         │          │   MANIFEST    │  │
│  │  (src/wal/)  │         │          │  (JSON log)   │  │
│  └──────────────┘         │          └───────────────┘  │
└───────────────────────────┴─────────────────────────────┘
```

| Package | Concept |
|---|---|
| `src/wal` | Append-only write-ahead log with group-commit |
| `src/memtable` | In-memory SkipList — mutable and immutable |
| `src/sstable` | Sorted String Table: writer, reader, bloom filter |
| `src/store` | Orchestrates all components; exposes the public API |

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

| Constraint | Reason |
|---|---|
| Keys **cannot** contain spaces | `SplitN` stops at the first space |
| Values **can** contain spaces | Split is limited to 2 delimiters |
| Keys and values **cannot** contain newlines | Scanner splits on `\n` |

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

On an Intel Core i7-1165G7 @ 2.80GHz (linux/amd64):

| Operation | Throughput | Notes |
|---|---|---|
| Sequential `put` | ~1 380 ns/op | After WAL group-commit optimisation |
| `get` (hot, memtable hit) | ~283 ns/op | O(log n) SkipList |
| `get` (cold, SSTable hit) | ~2 176 ns/op | Bloom filter + index binary search |
| `get` (miss) | ~143 ns/op | Bloom filter rejection |
| `scan` (100 keys) | ~26 µs | Merge iterator across all layers |

---

## License

tinyKV is licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE).

- **Free to use** for open-source projects — your code must also be AGPL-3.0.
- **Commercial use** with closed source requires a separate commercial license from the author.
- This dual-licensing model means: open source stays open; proprietary users pay.

For commercial licensing inquiries, open an issue or contact **guilherme13c** via GitHub.
