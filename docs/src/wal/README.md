# WAL — Write-Ahead Log

The `wal` package provides the durability layer that sits in front of tinyKV's
in-memory memtable. Before any key/value pair is acknowledged as written, its
record is appended to the Write-Ahead Log on disk. This guarantees that, even
if the process crashes before the memtable is flushed to an SSTable, the data
can be recovered by replaying the log at startup.

---

## Why a WAL?

An LSM-tree engine keeps recent writes in a mutable, in-memory buffer (the
memtable). Only when that buffer reaches a size threshold is it sorted and
written to an immutable SSTable on disk. Until that flush happens, the
in-memory data is the only copy of recent writes — a crash would lose it.

The WAL solves this: every `Append` is written **and fsynced** to disk before
the caller is unblocked. The memtable is therefore just an index over data that
already exists durably on disk. A crash at any point leaves the log intact; the
engine replays it at startup to reconstruct the memtable.

---

## Two-File WAL Scheme

tinyKV uses **one WAL per memtable**, which keeps the relationship between a
log and the data it protects simple and explicit. At any given moment there are
at most two WAL files on disk:

| File            | Role                                                   |
|-----------------|--------------------------------------------------------|
| `wal`           | Active log — all new writes go here                   |
| `wal.immutable` | Frozen log — being flushed to SSTable in the background |

### Lifecycle

```
Normal writes:
  Append → active WAL ("wal")

Memtable freeze (compaction trigger):
  1. Rename "wal" → "wal.immutable"
  2. Create a new empty "wal" (new active WAL)
  3. Swap the in-memory memtable pointer; old memtable starts flushing

Flush complete:
  4. Delete "wal.immutable"
     (the SSTable now owns all those records permanently)
```

Because the rename is atomic on POSIX filesystems, there is never a window
where a WAL file is missing or ambiguous.

---

## Crash Recovery

At startup the engine looks for both files and replays them in the correct
order:

```
1. If "wal.immutable" exists:
      replay it → reconstruct the frozen memtable
      trigger (or re-trigger) the flush to SSTable
2. If "wal" exists:
      replay it → reconstruct the active memtable
3. Begin serving requests normally
```

The reader maps any parse error on a partial tail to `io.EOF`, so a crash
mid-write simply stops replay at the last fully-fsynced record — no corruption
is propagated.

---

## Design Decisions

**One WAL per memtable, not one global WAL.**  
A single global WAL would require tracking which portions have been checkpointed
as each SSTable is flushed, adding complexity (log sequence numbers, log
compaction). By tying one WAL to one memtable, the WAL's entire lifetime maps
exactly to the memtable's lifetime: create together, delete together.

**Write-stealing leader election.**  
Every `Append` caller enqueues its request into a shared pending slice and then
races to acquire the leader lock (`mu`). The winner (the leader) drains *all*
currently-pending requests — including those from goroutines blocked waiting for
`mu` — serialises them into a single byte buffer, and issues one `file.Write`
syscall for the entire batch. It then signals every request's `errChan`. Losers
that subsequently win `mu` find their request already written (stolen by the
previous leader) and return immediately without touching the file. A separate
`syncLoop` goroutine calls `file.Sync` every 10 ms to push data to durable
storage without ever blocking `Append`. This design eliminates the ~900 ns
goroutine context-switch overhead of a dedicated flusher channel, making
sequential writes ~35–51% faster while preserving the same batching benefit for
concurrent workloads.

**No checksums.**  
The writer fsyncs after every batch, so every fully-written record is
guaranteed durable. A truncated tail (the only failure mode) is detected
implicitly by a failed uvarint or `ReadFull` decode and treated as `io.EOF`.
Adding a per-record CRC would catch silent corruption but is outside tinyKV's
current scope.

---

## Sub-documents

| Document | Contents |
|---|---|
| [wal.md](wal.md) | `LogEntry` DTO, `LogWriterI` and `LogReaderI` interfaces |
| [writer.md](writer.md) | `LogWriter` implementation — group commit, on-disk format, concurrency |
| [reader.md](reader.md) | `LogReader` implementation — sequential decode, crash-safety |
