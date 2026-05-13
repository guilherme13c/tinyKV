# tinyKV — Architecture & Developer Guide

tinyKV is a small, pedagogical [LSM-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) (Log-Structured Merge-Tree) key-value store written in Go. It is designed to be readable and to illustrate the core ideas behind production stores such as LevelDB, RocksDB, and Cassandra — without the production complexity that makes those systems hard to learn from.

The store exposes four operations — `put`, `get`, `delete`, and `scan` — through an interactive command-line REPL and a clean Go interface.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Write Path](#3-write-path)
4. [Read Path](#4-read-path)
5. [Crash Recovery](#5-crash-recovery)
6. [Concurrency Model](#6-concurrency-model)
7. [On-Disk Format](#7-on-disk-format)
8. [Component Index](#8-component-index)
9. [Running tinyKV](#9-running-tinykv)
10. [Testing](#10-testing)

---

## 1. Project Overview

### What is an LSM-tree?

An LSM-tree stores data in a hierarchy of increasingly large, immutable sorted files. Writes always land in a small in-memory buffer (the **MemTable**) and are durable through an append-only **Write-Ahead Log (WAL)**. When the MemTable fills up it is flushed to disk as an immutable **SSTable** (Sorted String Table). Periodically, multiple SSTables are merged and compacted into fewer, larger ones.

This design trades some read complexity for extremely fast sequential writes: every mutation is an append, never an in-place update.

### Design philosophy

tinyKV intentionally keeps each component minimal and independently readable:

- Minimal external dependencies: only [`github.com/cespare/xxhash/v2`](https://github.com/cespare/xxhash) for the bloom-filter hash (single-file, zero transitive deps).
- Each package maps to exactly one LSM-tree concept.
- All interfaces are small — usually two to four methods.
- All on-disk formats are documented and hand-written (no protobuf, no encoding libraries).

---

## 2. Architecture

### Component overview

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

### LSM-tree data flow

```
WRITE PATH
══════════
  caller
    │
    ▼
  WAL.Append()          ← durability: survives crash before memtable flush
    │
    ▼
  MemTable.Put()         ← fast in-memory write (SkipList)
    │
    │ (size > 4 MB)
    ▼
  freeze()
    ├── rename wal → wal.immutable
    ├── open fresh wal
    ├── promote memtable → immutable
    └── spawn flushBackground() goroutine
              │
              ▼
          SSTable.Writer    ← immutable memtable serialised to disk
              │
              ▼
          manifest.recordAdd()
              │
              │ (L0 ≥ 4 files)
              ▼
          compact()         ← merge all L0 SSTables → one new SSTable
                              tombstones are dropped (full compaction)

READ PATH
═════════
  caller
    │
    ├─► MemTable.Lookup()        ← newest data; O(log n)
    │       │ not found
    ├─► Immutable.Lookup()       ← in-flight flush; O(log n)
    │       │ not found
    └─► SSTable[0..n].Get()      ← newest-first; O(1) bloom + O(log n) index
            │ ErrTombstone
            └─► stop (deleted)
```

---

## 3. Write Path

### `Put(key, value)` and `Delete(key)`

Both operations follow the same six-step write path. Steps 1–3 run under a **shared epoch lock** (`mu.RLock`); steps 4–5 are triggered as needed and require an exclusive lock.

#### Step 1 — Check background error

If a previous background flush goroutine failed, its error is stored in `Store.bgErr` and surfaced to the caller here. This prevents silent data loss.

#### Step 2 — WAL append (durability)

```
WAL record: uvarint(keyLen) | uvarint(valueLen<<1 | tombstoneBit) | key | [value]
```

The `LogWriter` enqueues the record and competes for a leader-election mutex. The winner (leader) drains **all** currently pending requests — including those from goroutines blocked on the mutex — serialises them into one buffer, and issues a single `file.Write` syscall for the entire batch, followed by one `fsync`. This **write-stealing** approach eliminates the goroutine round-trip overhead of the previous channel-based design, cutting sequential write latency by ~35–51%. Every caller blocks until its record is confirmed durable.

`Delete` is written as a tombstone: `isTombstone=true`, no value bytes.

#### Step 3 — MemTable insert

The record is inserted into the `SkipList`. If the key already exists the node is updated in-place; otherwise a new node at a random height (0–11) is linked in. The skip list tracks `sizeBytes` incrementally.

#### Step 4 — Freeze check

After every write:

```go
if s.memtable.SizeInBytes() > 4*1024*1024 && s.immutable == nil {
    s.freeze()
}
```

A freeze is only triggered when no flush is already in progress (`immutable == nil`), so writes to a full memtable are not blocked while a flush runs.

#### Step 5 — `freeze()` (still under write lock)

1. Close the active WAL and rename it to `wal.immutable`.
2. Open a new empty WAL at the original path.
3. Promote `memtable → immutable`; create a fresh empty `memtable`.
4. Spawn `flushBackground()` as a goroutine.

The write lock is released as soon as `freeze()` returns. Subsequent writes go to the new memtable immediately.

#### Step 6 — `flushBackground()` (outside lock)

All I/O-heavy work runs without holding the lock:

1. Create `<timestamp>.sst` via `sstable.Writer`.
2. Iterate the immutable memtable in key order, appending every record.
3. Call `Writer.Close()` — this writes the index block, bloom block, and footer, then `fsync`s.
4. Open an `sstable.Reader` on the new file.
5. Call `manifest.recordAdd(path)` — appends `{"op":"add","path":"..."}` to `MANIFEST` and `fsync`s.
6. Acquire write lock: prepend the new reader to `sstables`, clear `immutable`.
7. Remove `wal.immutable`.
8. If `len(sstables) >= 4`, call `compact()` while still holding the write lock.

#### Compaction (`compact()`)

Full L0 compaction merges **all** existing SSTables into one:

1. Create a `mergeIterator` over all SSTable iterators (`includeTombstones=true` so key-tie deduplication works correctly across files).
2. Iterate the merged stream; skip tombstone entries (safe because all older data is included in the merge).
3. Write non-tombstone entries to a new SSTable.
4. Record the new file as `add` in the manifest.
5. Record every old file as `del` in the manifest.
6. Close and `os.Remove` each old SSTable.
7. Replace `s.sstables` with the single new reader.

After compaction the store has exactly one SSTable.

---

## 4. Read Path

### `Get(key)`

Executed under a **read lock**, in newest-to-oldest order:

| Step | Source                     | Method            | Short-circuit condition                                                           |
| ---- | -------------------------- | ----------------- | --------------------------------------------------------------------------------- |
| 1    | Active MemTable            | `Lookup(key)`     | Returns value if found and not tombstoned; returns `ErrKeyNotFound` if tombstoned |
| 2    | Immutable MemTable         | `Lookup(key)`     | Same as above (only checked if non-nil)                                           |
| 3    | SSTables (newest → oldest) | `Reader.Get(key)` | `ErrTombstone` stops the search immediately                                       |

The key insight is **tombstone short-circuit**: when a layer returns `ErrTombstone` it means the most-recent record for that key is a deletion marker. There is no need to check older layers — the key is definitively deleted.

#### SSTable point lookup

`Reader.Get(key)` performs three stages:

1. **Bloom filter check** — `BloomFilter.MayContain(key)`. If the filter returns `false` (no false negatives), the key is definitely absent; return `ErrKeyNotFound` without any I/O.

2. **Index binary search** — The in-memory index stores `(lastKey, BlockHandle)` per data block. Binary search finds the first block whose `lastKey >= key`. If no such block exists the key is absent.

3. **Data block linear scan** — `readBlock()` issues one `ReadAt` syscall. `scanBlock()` walks entries sequentially until the key is found, exceeded (key absent), or the block is exhausted.

### `Scan(startKey, endKey)`

`Scan` returns a lazy iterator over the half-open range `[startKey, endKey)`. It works by creating a **k-way merge iterator** over every live source:

```
sources = [activeMemTable.Iterator(), immutable.Iterator(), sst[0].Iterator(), ...]
```

The `mergeIterator` uses a **min-heap** (`container/heap`). Construction:

1. Call `Seek(startKey)` on every source iterator.
2. Push the first valid entry from each source onto the heap with its `sourceIdx` (lower = more recent).
3. Initialize by calling `advance()`.

Each call to `Next()` / `advance()`:

1. Pop the minimum entry from the heap.
2. Stop if the key `>= endKey`.
3. **Deduplicate**: drain all heap entries with the same key from older sources (higher `sourceIdx`), advancing each of those iterators and pushing their next entry back.
4. Advance the winning iterator and push its next entry back.
5. Skip tombstones (the `includeTombstones=false` default for `Scan`).

The caller sees a clean, sorted, deduplicated, tombstone-free stream of `(key, value)` pairs.

---

## 5. Crash Recovery

On `NewStore()`, before opening the WAL for writing, the store replays all durable data in order from oldest to newest:

### Step 1 — Manifest replay

`replayManifest()` reads `MANIFEST` line by line, building a set of live SSTable paths:

```
{"op":"add","path":"data/1234.sst"}   → add to live set
{"op":"del","path":"data/1234.sst"}   → remove from live set
```

Malformed trailing lines (from a crash mid-write) are silently skipped. The result is the ordered list of live SSTable files.

### Step 2 — SSTable readers

SSTable readers are opened newest-first (manifest order is oldest-first, so the slice is reversed). Each reader loads its footer, bloom filter, and index into memory on open.

### Step 3 — Immutable WAL replay (`wal.immutable`)

If `wal.immutable` exists, a previous process crashed between `freeze()` (WAL rename) and the background flush completing. The file is replayed into the fresh memtable, then deleted. This ensures no flushed data is lost.

### Step 4 — Active WAL replay

The active `wal` file (if present) is replayed into the memtable. The `LogReader` treats truncated or corrupt tails as clean EOF — any partial record at the end is simply discarded, so a crash mid-write to the WAL does not corrupt the store.

### Step 5 — Open WAL for writing

A `LogWriter` is opened in append mode on the `wal` path. The store is now ready.

### Recovery invariants

| Scenario                                                      | Recovery                                                                                                             |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Crash before WAL fsync                                        | Record not in WAL → not recovered (caller got an error)                                                              |
| Crash after WAL fsync, before memtable insert                 | WAL replay re-inserts on restart                                                                                     |
| Crash during background flush (SSTable write)                 | Incomplete SSTable not in manifest → ignored; `wal.immutable` replayed                                               |
| Crash after manifest `add` but before `wal.immutable` removed | SSTable loaded from manifest; duplicate WAL replay is idempotent                                                     |
| Crash during compaction, after new SSTable `add`              | Old SSTables still in manifest (their `del` not written) → both old and new loaded; duplicate keys resolved by merge |

---

## 6. Concurrency Model

tinyKV uses a **dual-lock** design that allows WAL writes from multiple goroutines to overlap while still serialising in-memory SkipList mutations.

### Two mutexes

#### `mu sync.RWMutex` — epoch lock

`mu` guards the store's structural state (which memtable is active, which SSTables exist, and whether a flush is in progress).

| Operation                                      | Lock held              |
| ---------------------------------------------- | ---------------------- |
| `Put`, `Delete` (WAL append + SkipList insert) | Read lock (`mu.RLock`) |
| `Get`, `Scan` (iterator construction)          | Read lock (`mu.RLock`) |
| `freeze()`, `compact()`, `Close()`             | Write lock (`mu.Lock`) |
| Background flush I/O                           | **No lock**            |

Holding `mu.RLock` during `Put`/`Delete` means **multiple writers can proceed concurrently through the WAL** — the write-stealing leader inside `LogWriter` batches their records together without needing exclusive access to `mu`.

#### `memMu sync.RWMutex` — SkipList lock

`memMu` serialises access to the active SkipList. It is always acquired **inside** `mu` (i.e., with `mu` already held).

| Operation                         | Lock held                 |
| --------------------------------- | ------------------------- |
| SkipList insert (`Put`, `Delete`) | Write lock (`memMu.Lock`) |
| SkipList lookup (`Get`)           | Read lock (`memMu.RLock`) |
| SkipList scan (`Scan`)            | Read lock (`memMu.RLock`) |

### Lock ordering rule

> **Always acquire `mu` before `memMu`.** Never acquire `mu` while holding `memMu`.

### Why this design

The WAL uses write-stealing so multiple `Put` goroutines can overlap their WAL appends under `mu.RLock()` — only one becomes the leader per batch, but the others' data is piggybacked into the same `file.Write` + `fsync`. After the WAL confirms durability, each goroutine independently acquires `memMu.Lock()` to insert its record into the SkipList. Because SkipList inserts are fast (in-memory, O(log n)), the window of exclusive `memMu` contention is brief.

This split means the expensive operation (WAL I/O with `fsync`) is parallelised, while the cheap operation (SkipList insert) is serialised only as long as necessary.

### Background flush goroutine

`freeze()` spawns exactly one background goroutine per flush. Its lifetime is tracked by `flushWg`. `Close()` calls `flushWg.Wait()` to ensure any in-flight flush completes before shutdown.

Only one flush can be in progress at a time (`immutable == nil` guard in `Put`/`Delete`). If the memtable exceeds the threshold while a flush is running, writes continue into the active memtable. A second freeze is deferred until the first flush completes.

### WAL write-stealing leader election

Each `Append` caller enqueues its `LogEntry` and then races to acquire the internal leader mutex:

1. The **leader** (winner) drains all currently enqueued requests into a single buffer.
2. It calls `file.Write` once for the entire batch, then `file.Sync()`.
3. It signals each request's completion channel with the write result.
4. Goroutines that lost the race (followers) simply wait on their completion channel — their records were already written by the leader.

This eliminates a dedicated flusher goroutine and the channel round-trip latency associated with it, cutting sequential write latency by ~35–51%.

### `bgErr` error propagation

If the background flush goroutine fails, it stores the error in `Store.bgErr`. The next call to `Put` or `Delete` surfaces this error to the caller, preventing silent data loss.

---

## 7. On-Disk Format

### WAL record format

Each record is written with no framing or CRC. Crash-safety relies on treating any partial record at the end of the file as EOF.

```
┌─────────────────┬──────────────────────────────┬──────────┬───────────────┐
│ uvarint(keyLen) │ uvarint(valueLen<<1 | tsBit) │ key bytes│ value bytes   │
│   (1–10 bytes)  │         (1–10 bytes)         │ (keyLen) │ (0 if ts=1)   │
└─────────────────┴──────────────────────────────┴──────────┴───────────────┘
```

- `tsBit` (tombstone bit) is the LSB of the packed value field.
- `valueLen` is `packedField >> 1`.
- For tombstones, no value bytes are written at all (`valueLen` is always 0 for tombstones in practice, but the bit is the authoritative signal).

### SSTable file layout

```
┌─────────────────────────────────────────────┐
│  Data Block 0          (≤ 4 096 bytes)      │
│  Data Block 1          (≤ 4 096 bytes)      │
│  …                                          │
│  Data Block N          (≤ 4 096 bytes)      │
├─────────────────────────────────────────────┤
│  Index Block                                │
├─────────────────────────────────────────────┤
│  Bloom Block                                │
├─────────────────────────────────────────────┤
│  Footer                (exactly 32 bytes)   │
└─────────────────────────────────────────────┘
```

#### Data block record format

Identical to the WAL record format:

```
uvarint(keyLen) | uvarint(valueLen<<1 | tsBit) | key | [value]
```

Records are packed contiguously. A block is flushed when `len(dataBuf) >= 4096`.

#### Index block format

One entry per data block, written sequentially:

```
┌──────────────────┬──────────┬────────────────────┬──────────────────────┐
│ uvarint(keyLen)  │ key bytes│ uint64 LE (offset) │ uint64 LE (length)   │
│   (1–10 bytes)   │ (keyLen) │     (8 bytes)      │      (8 bytes)       │
└──────────────────┴──────────┴────────────────────┴──────────────────────┘
```

The `key` stored is the **last key** of the data block. This enables binary search: find the first block whose last key `>= target key`.

#### Bloom block format

```
┌────────────────────┬───────────────────────────────┐
│  k  (uint32 LE)    │  bit array (variable length)  │
│    (4 bytes)       │  ⌈n × 10⌉ / 8 bytes           │
└────────────────────┴───────────────────────────────┘
```

Parameters: `bitsPerKey = 10`, `k = round(10 × ln 2) = 7`. Double hashing uses FNV-1a (h1) and FNV-1 (h2); bit position `i` is `(h1 + i×h2) mod m`.

#### Footer format (32 bytes, always at end of file)

```
┌──────────────────────┬──────────────────────┬──────────────────────┬──────────────────────┐
│ indexOffset (uint64) │ indexLength (uint64) │ bloomOffset (uint64) │ bloomLength (uint64) │
│      8 bytes LE      │      8 bytes LE      │      8 bytes LE      │      8 bytes LE      │
└──────────────────────┴──────────────────────┴──────────────────────┴──────────────────────┘
```

The reader always starts by seeking to `fileSize - 32` to parse the footer, then uses the offsets to load the bloom and index blocks.

### MANIFEST format

The manifest (`MANIFEST` in the data directory) is an **append-only newline-delimited JSON log**. Each line is a `manifestRecord`:

```json
{"op":"add","path":"data/1700000000000000000.sst"}
{"op":"del","path":"data/1700000000000000000.sst"}
```

At startup, `replayManifest` scans all lines in order. It maintains an ordered slice of paths and an `alive` map; `add` marks a path alive, `del` marks it dead. Only paths that are alive at the end form the live set. Malformed lines are skipped (crash-safe tail).

SSTable file names are `<unix-nanosecond-timestamp>.sst`, which guarantees monotonically increasing names for easy ordering.

---

## 8. Component Index

| Package / File                | Role                                                                              | Dedicated doc                                       |
| ----------------------------- | --------------------------------------------------------------------------------- | --------------------------------------------------- |
| `main.go`                     | CLI entry point, REPL loop, signal handling                                       | —                                                   |
| `src/errors.go`               | `ErrKeyNotFound`, `ErrTombstone`, `KeyNotFoundError`                              | [src/errors.md](src/errors.md)                      |
| `src/memtable/memtable.go`    | `MemTableI` and `MemTableIteratorI` interfaces                                    | [src/memtable/README.md](../src/memtable/README.md) |
| `src/memtable/skip_list.go`   | `SkipList` (max height 12) + `skipListIterator`                                   | [src/memtable/README.md](../src/memtable/README.md) |
| `src/wal/wal.go`              | `LogWriterI` and `LogReaderI` interfaces                                          | [src/wal/README.md](../src/wal/README.md)           |
| `src/wal/dto.go`              | `LogEntry` data transfer object                                                   | [src/wal/README.md](../src/wal/README.md)           |
| `src/wal/writer.go`           | `LogWriter` — write-stealing leader election for low-latency durable batch writes | [src/wal/README.md](../src/wal/README.md)           |
| `src/wal/reader.go`           | `LogReader` — sequential decoder, crash-safe EOF                                  | [src/wal/README.md](../src/wal/README.md)           |
| `src/sstable/sstable.go`      | `BlockHandle`, `Footer`, `BlockSize`, `FooterSize` constants                      | [src/sstable/README.md](../src/sstable/README.md)   |
| `src/sstable/bloom.go`        | `BloomFilter` — double-hashing FNV, encode/decode                                 | [src/sstable/README.md](../src/sstable/README.md)   |
| `src/sstable/writer.go`       | `Writer` — buffers records into 4 KB data blocks                                  | [src/sstable/README.md](../src/sstable/README.md)   |
| `src/sstable/reader.go`       | `Reader` + `sstableIterator` — bloom + index + block scan                         | [src/sstable/README.md](../src/sstable/README.md)   |
| `src/store/store.go`          | `StoreI` interface, `Store` engine, write/read/flush/compact                      | [src/store/README.md](../src/store/README.md)       |
| `src/store/manifest.go`       | `manifest` — append-only JSON log of SSTable lifecycle                            | [src/store/README.md](../src/store/README.md)       |
| `src/store/merge_iterator.go` | `mergeIterator` — min-heap k-way merge, deduplication                             | [src/store/README.md](../src/store/README.md)       |
| `e2e/e2e_test.go`             | End-to-end tests (builds binary, drives via stdin)                                | —                                                   |

---

## 9. Running tinyKV

### Prerequisites

- Go 1.25 or later (`go.mod` declares `go 1.25.3`).

### Build

```bash
# From the repository root:
go build -o tinyKV .
```

### Run

```bash
./tinyKV [-dir <data-directory>]
```

- `-dir` specifies where SSTables, the WAL, and the MANIFEST are stored. Defaults to `data/` relative to the working directory. The directory is created automatically if it does not exist.

### REPL commands

Once running, the store presents a `>` prompt:

| Command                    | Description                       | Example               |
| -------------------------- | --------------------------------- | --------------------- |
| `put <key> <value>`        | Store or overwrite a key          | `put hello world`     |
| `get <key>`                | Retrieve a value                  | `get hello` → `world` |
| `delete <key>`             | Delete a key (writes a tombstone) | `delete hello`        |
| `scan <startKey> <endKey>` | Range scan, **end key exclusive** | `scan a z`            |
| `exit` / `quit`            | Flush and close gracefully        | `exit`                |

`Ctrl-D` (EOF on stdin) also triggers a graceful shutdown, as do `SIGINT` and `SIGTERM`.

### Example session

```
$ ./tinyKV -dir /tmp/demo
tinyKV — commands: put <key> <value> | get <key> | delete <key> | scan <start> <end> | exit
> put fruit apple
ok
> put veggie carrot
ok
> put grain rice
ok
> scan f z
  fruit = apple
  grain = rice
  veggie = carrot
> delete grain
ok
> get grain
(not found)
> scan f z
  fruit = apple
  veggie = carrot
> exit
```

Data persists across restarts:

```
$ ./tinyKV -dir /tmp/demo
> get fruit
apple
> exit
```

---

## 10. Testing

### End-to-end tests

The `e2e/` directory contains a full test suite that exercises the compiled binary through stdin/stdout, testing the same observable behaviour a real user would see.

```bash
# Run all e2e tests from the repository root:
go test ./e2e/...

# With verbose output:
go test -v ./e2e/...

# Run a specific test:
go test -v -run TestE2EPersistence ./e2e/...
```

`TestMain` builds the binary once into a temporary directory before running any tests. Each test case uses `t.TempDir()` for an isolated data directory.

### Test coverage

| Test                             | What it verifies                               |
| -------------------------------- | ---------------------------------------------- |
| `TestE2EPutGet`                  | Basic write and read                           |
| `TestE2EGetMissing`              | Missing key returns `(not found)`              |
| `TestE2EOverwrite`               | Second `put` on same key replaces the value    |
| `TestE2EDelete`                  | Delete followed by get returns `(not found)`   |
| `TestE2EDeleteNonExistent`       | Deleting a key that was never written succeeds |
| `TestE2EScan`                    | Scan returns keys sorted ascending             |
| `TestE2EScanEndKeyExclusive`     | End key is excluded from scan results          |
| `TestE2EScanEmpty`               | Scan on empty store returns `(no results)`     |
| `TestE2EScanTombstonesExcluded`  | Deleted keys do not appear in scan results     |
| `TestE2EPersistence`             | Data survives a process restart                |
| `TestE2EPersistenceAfterDelete`  | Delete persists across restart                 |
| `TestE2EPersistenceMultipleKeys` | Multiple keys survive restart                  |
| `TestE2EOverwriteThenScan`       | Scan after overwrite shows new value           |
| `TestE2ELargeWorkload`           | 50 keys written and verified                   |

### Unit tests

Individual packages can be tested in isolation:

```bash
go test ./src/...
go test ./...     # all packages
```
