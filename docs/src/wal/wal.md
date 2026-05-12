# WAL Interfaces and Data Transfer Objects

This document covers the public contract of the `wal` package: the `LogEntry`
DTO that flows between writer and reader, and the two interfaces that the rest
of the engine depends on.

---

## LogEntry

```go
type LogEntry struct {
    Key         []byte
    Value       []byte
    IsTombstone bool
}
```

`LogEntry` is the unit of information decoded by `LogReader.Next()`. It
represents a single logged operation.

| Field         | Type     | Semantics |
|---------------|----------|-----------|
| `Key`         | `[]byte` | The record key. Never nil; always has length ≥ 1. |
| `Value`       | `[]byte` | The record value. **nil for tombstone entries** (see below). |
| `IsTombstone` | `bool`   | `true` when this entry records a deletion. |

### Tombstone semantics

When a key is deleted in an LSM-tree, the engine does not immediately remove it
from disk (it may exist in older SSTables). Instead it writes a **tombstone** —
a marker that says "this key is deleted as of this log sequence". During
recovery, replaying a tombstone entry should remove the key from the memtable
(or insert the tombstone into it, depending on the engine's delete strategy).

For a tombstone entry:
- `IsTombstone == true`
- `Value == nil` (no value bytes are written to or read from disk)

---

## LogWriterI

```go
type LogWriterI interface {
    Append(key []byte, value []byte, isTombstone bool) error
    Close() error
}
```

### `Append(key, value []byte, isTombstone bool) error`

Durably records a single key/value operation in the log.

- The call **blocks** until the record has been written and fsynced to disk (or
  until an error occurs).
- `value` is ignored (and should be `nil`) when `isTombstone` is `true`.
- Returns `os.ErrClosed` if `Close()` has already been called or is called
  concurrently while waiting.
- Safe to call concurrently from multiple goroutines; the implementation
  serializes writes internally.

### `Close() error`

Signals that no further appends will be made, waits for any in-flight writes
to drain, fsyncs, and closes the underlying file.

- After `Close` returns, any subsequent `Append` call returns `os.ErrClosed`.
- Returns the first error encountered during the final sync or file close.

---

## LogReaderI

```go
type LogReaderI interface {
    Next() (*LogEntry, error)
    Close() error
}
```

### `Next() (*LogEntry, error)`

Decodes and returns the next log entry in sequential order.

- Returns `(entry, nil)` on success.
- Returns `(nil, io.EOF)` when the log has been fully consumed **or** when a
  truncated / partially-written record is detected at the tail.
- Never returns a non-EOF error for a truncated tail — any parse failure is
  silently mapped to `io.EOF` (see [reader.md](reader.md) for the rationale).

### `Close() error`

Closes the underlying file handle. Returns any file-close error.

---

## Why interfaces?

The engine's upper layers (`store`, `memtable`) depend on `LogWriterI` and
`LogReaderI` rather than on the concrete `LogWriter` / `LogReader` structs.
This serves two purposes:

1. **Testability** — unit tests can inject a no-op or in-memory writer without
   touching the filesystem.
2. **Replaceability** — an alternative implementation (e.g. a writer that
   targets a network journal) can be swapped in without changing call sites.
