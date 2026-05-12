# Package `store`

The `store` package is the **top-level engine** of tinyKV. It ties together every
major subsystem — the write-ahead log, the in-memory skip-list, on-disk SSTables,
and the manifest — into a single, coherent key/value store.

---

## Architecture overview

```
 ┌──────────────────────────────────────────────────────┐
 │                       Store                          │
 │                                                      │
 │  Writes ──►  WAL ──► memtable                        │
 │                          │                           │
 │               sizeThreshold (4 MB)                   │
 │                          │                           │
 │                       freeze()                       │
 │                          │                           │
 │               ┌──────────▼──────────┐                │
 │               │  immutable memtable  │  (background) │
 │               └──────────┬──────────┘                │
 │                          │  flushBackground()        │
 │                          ▼                           │
 │                    SSTables (L0)                     │
 │                          │                           │
 │           compactionThreshold (4 SSTables)           │
 │                          │                           │
 │                       compact()                      │
 │                          │                           │
 │                  Single merged SSTable               │
 │                                                      │
 │  Reads  ──►  memtable → immutable → SSTables[0..n]   │
 └──────────────────────────────────────────────────────┘
```

All persistent state is tracked by the **manifest** (`MANIFEST` file), which
records every SSTable file that is added or removed. This ensures correctness
across crash-and-restart cycles.

---

## Public surface

| Symbol | Kind | Description |
|--------|------|-------------|
| `StoreI` | interface | User-facing contract: Put, Get, Delete, Scan, Close |
| `Store` | struct | Concrete implementation of `StoreI` |
| `NewStore` | func | Opens (or creates) a store rooted at a directory |

---

## Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `sizeThreshold` | 4 MiB | When the active memtable exceeds this size, it is frozen and flushed to an SSTable in the background |
| `compactionThreshold` | 4 | When L0 accumulates this many SSTable files, a full compaction is triggered at the end of the flush |

---

## Lifecycle

```
NewStore(walPath, dir)
    │
    ├─ opens / replays manifest
    ├─ loads SSTable readers (newest-first)
    ├─ replays immutable WAL if present  (crash recovery)
    ├─ replays active WAL
    └─ opens active WAL writer
         │
         ▼
    [normal operation]
    Put / Get / Delete / Scan
         │
         │  memtable full?
         └──► freeze() ──► background goroutine flushes to SSTable
                                │
                                └──► too many SSTables?
                                         └──► compact()
         │
         ▼
    Close()
    ├─ waits for any in-flight flush goroutine
    ├─ flushSync() if memtable is non-empty
    ├─ closes WAL
    ├─ closes all SSTable readers
    └─ closes manifest
```

---

## Concurrency model

- A single `sync.RWMutex` (`mu`) guards all mutable fields of `Store`.
  - Write operations (`Put`, `Delete`, `freeze`) take the **write** lock.
  - Read operations (`Get`, `Scan`) take the **read** lock.
  - `Close` takes the write lock after waiting for background work.
- `flushBackground` runs **outside** the lock for all I/O (SSTable creation). It
  re-acquires the lock only for brief bookkeeping updates.
- `flushWg` (`sync.WaitGroup`) ensures `Close` waits for any in-flight flush before
  proceeding.
- The WAL writer has its own internal serialization; callers do not need to
  coordinate around it beyond holding `mu`.
- Background errors are communicated back to callers via the `bgErr` field, which
  is checked at the start of every write.

---

## Sub-documents

| File | Contents |
|------|----------|
| [`store.md`](store.md) | `StoreI` interface, `Store` struct fields, and every method in depth |
| [`manifest.md`](manifest.md) | `manifest` internals, MANIFEST file format, crash-safety analysis |
| [`merge_iterator.md`](merge_iterator.md) | `mergeIterator` algorithm, heap design, tombstone semantics |
