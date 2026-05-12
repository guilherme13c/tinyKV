# LogReader — Write-Ahead Log Reader

`LogReader` is the concrete implementation of `LogReaderI`. It reads a WAL
file produced by `LogWriter` sequentially from beginning to end, decoding one
record at a time. It is used exclusively during startup (crash recovery) and is
not designed for concurrent use.

---

## Struct

```go
type LogReader struct {
    file   *os.File
    reader *bufio.Reader
}
```

| Field    | Purpose |
|----------|---------|
| `file`   | The raw OS file handle, opened `O_RDONLY`. Kept so it can be closed. |
| `reader` | A `bufio.Reader` wrapping `file`. Reduces the number of `read` syscalls by issuing large block reads internally and serving small decode requests from its in-memory buffer. |

### Why `bufio.Reader`?

Decoding a single WAL record requires many small reads:

- 1–10 bytes for the `keyLen` uvarint
- 1–10 bytes for the `valueMeta` uvarint
- N bytes for the key
- M bytes for the value

Without buffering, each of those would become a separate `read(2)` syscall.
`bufio.Reader` (default buffer size: 4096 bytes) reads ahead in large chunks,
so most small reads are served from memory. For a typical recovery workload
(replaying thousands of records) this can reduce syscall overhead by an order
of magnitude.

---

## `NewLogReader`

```go
func NewLogReader(path string) (*LogReader, error)
```

Opens the file at `path` with `O_RDONLY` and mode `0` (mode is irrelevant for
read-only opens). Returns an error if the file does not exist — the caller is
responsible for deciding whether a missing WAL is an error or simply means
there is nothing to replay.

---

## `Next`

```go
func (lr *LogReader) Next() (*LogEntry, error)
```

Decodes and returns the next `LogEntry`. Returns `(nil, io.EOF)` when the log
is exhausted or a corrupt tail is encountered. **No other error is ever
returned** — all errors are mapped to `io.EOF`.

### Decoding steps

The steps mirror the write order in `LogWriter.runFlusher` exactly:

```
Step 1 — Read keyLen
    keyLen, err := binary.ReadUvarint(lr.reader)
    if err != nil { return nil, io.EOF }
```

`binary.ReadUvarint` reads bytes one at a time until it finds the terminating
byte (high bit 0). Any error here — including a genuine `io.EOF` (no more
bytes), `io.ErrUnexpectedEOF` (file ended mid-uvarint), or any other read
error — is treated as `io.EOF`. See [Crash Safety](#crash-safety) for why.

```
Step 2 — Read valueMeta
    valueMeta, err := binary.ReadUvarint(lr.reader)
    if err != nil { return nil, io.EOF }
```

Same treatment as `keyLen`.

```
Step 3 — Extract tombstone flag and valueLen
    isTombstone := valueMeta & 1 == 1
    valueLen    := valueMeta >> 1
```

Reverses `encodeLength`: bit 0 is the tombstone flag, remaining bits are the
length. For a tombstone entry `valueMeta == 1`, so `isTombstone = true` and
`valueLen = 0`.

```
Step 4 — Read key bytes
    key := make([]byte, keyLen)
    if _, err := io.ReadFull(lr.reader, key); err != nil {
        return nil, io.EOF
    }
```

`io.ReadFull` returns `io.ErrUnexpectedEOF` if the file ends before `keyLen`
bytes are available (a truncated record). This is mapped to `io.EOF`.

```
Step 5 — Read value bytes (non-tombstones only)
    if !isTombstone {
        value := make([]byte, valueLen)
        if _, err := io.ReadFull(lr.reader, value); err != nil {
            return nil, io.EOF
        }
    }
```

Tombstone entries carry no value bytes on disk, so this step is skipped when
`isTombstone == true`.

```
Step 6 — Return the entry
    return &LogEntry{Key: key, Value: value, IsTombstone: isTombstone}, nil
```

For tombstones, `value` is the zero value `nil`.

### Decoding flow

```
┌──────────────────────────────────────────────────┐
│                  Next() call                     │
└──────────────────┬───────────────────────────────┘
                   │
         ┌─────────▼──────────┐
         │  ReadUvarint keyLen │
         └─────────┬──────────┘
          err? ────┤ no error
          ↓        │
        io.EOF  ┌──▼────────────────────┐
                │  ReadUvarint valueMeta │
                └──────────┬────────────┘
                 err? ──────┤ no error
                 ↓          │
               io.EOF    Extract: isTombstone = valueMeta & 1
                             valueLen = valueMeta >> 1
                         │
                    ┌────▼────────────────┐
                    │  ReadFull(key bytes) │
                    └────┬────────────────┘
                 err? ───┤ no error
                 ↓       │
               io.EOF  isTombstone?
                          │ yes              │ no
                          │           ┌──────▼──────────────────┐
                          │           │  ReadFull(value bytes)   │
                          │           └──────┬───────────────────┘
                          │        err? ─────┤ no error
                          │        ↓         │
                          │      io.EOF      │
                          └────────┬─────────┘
                                   │
                          ┌────────▼───────────────────┐
                          │  return &LogEntry{...}, nil  │
                          └─────────────────────────────┘
```

---

## Crash Safety

A key property of `Next` is that **every error is mapped to `io.EOF`**. The
rationale is grounded in the writer's durability guarantee:

- The writer calls `file.Sync()` after every batch. This means every record
  that was part of a completed batch is fully on disk before any caller's
  `Append` returns.
- If the process crashes, the only possible damage is a **partially written
  record at the very end of the file** — the last batch that was being written
  when the crash occurred.

A partially written record manifests as one of:
- An incomplete uvarint (the file ends mid-header).
- A complete header but truncated key or value data.
- Seemingly valid length fields but insufficient remaining bytes.

All of these produce either `io.EOF`, `io.ErrUnexpectedEOF`, or a generic read
error. By mapping all of them to `io.EOF`, `Next` stops replay at exactly the
right boundary: the last fully-written, fully-fsynced record.

This is correct because:
1. Any record that returned successfully from `Append` was part of a completed
   batch and was fsynced — it will be decoded without error.
2. Any record from a crashed batch was never acknowledged to the caller, so the
   memtable was never updated — dropping it during replay is safe.

> **Implication**: `Next` provides no defence against **silent corruption** in
> the middle of the file (e.g. a flipped bit in an older record). Such
> corruption could cause `keyLen` or `valueLen` to decode to a large but
> plausible value, causing `Next` to read garbage data as a valid entry. This
> is a known limitation. Checksums (e.g. CRC-32 per record) would be needed to
> detect this class of corruption.

---

## `Close`

```go
func (lr *LogReader) Close() error
```

Closes the underlying `*os.File`. Returns any OS-level error. After `Close`,
any further `Next` call will return an error (which will be mapped to
`io.EOF`).
