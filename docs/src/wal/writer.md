# LogWriter — Write-Ahead Log Writer

`LogWriter` is the concrete implementation of `LogWriterI`. Its defining
characteristic is **group commit**: concurrent `Append` calls from many
goroutines are batched together by a single background flusher goroutine, and
the expensive `fsync` syscall is paid once per batch rather than once per
record.

---

## On-Disk Format

Every log entry is a variable-length record with no fixed-size header:

```
┌─────────────────────────┬──────────────────────────────────┬────────────┬─────────────────────────────────┐
│  uvarint: keyLen        │  uvarint: (valueLen << 1) | tomb │  key bytes │  value bytes (omitted if tomb)  │
└─────────────────────────┴──────────────────────────────────┴────────────┴─────────────────────────────────┘
```

- **keyLen** — number of bytes in the key, encoded as a
  [Protocol Buffers uvarint](https://protobuf.dev/programming-guides/encoding/#varints).
- **packed value meta** — a single uvarint that encodes two things:
  - bit 0 (`& 1`): tombstone flag (`1` = tombstone, `0` = normal write)
  - bits 1–63 (`>> 1`): `valueLen` (number of bytes in the value)
- **key bytes** — raw key data, exactly `keyLen` bytes.
- **value bytes** — raw value data, exactly `valueLen` bytes. **Absent for
  tombstone entries** (`isTombstone == true`), because `valueLen` is `0` and
  no bytes are written.

### Example — normal write (`key="foo"`, `value="bar"`)

```
03          ← uvarint(3)         keyLen = 3
06          ← uvarint(3<<1|0=6)  valueLen = 3, tombstone = false
66 6f 6f    ← "foo"
62 61 72    ← "bar"
```

### Example — tombstone (`key="foo"`)

```
03          ← uvarint(3)         keyLen = 3
01          ← uvarint(0<<1|1=1)  valueLen = 0, tombstone = true
66 6f 6f    ← "foo"
            ← (no value bytes)
```

---

## Internal Types

### `writeRequest`

```go
type writeRequest struct {
    key         []byte
    value       []byte
    isTombstone bool
    errChan     chan error
}
```

A `writeRequest` is the message that `Append` sends to the flusher goroutine.
`errChan` is a **per-request, buffered-1 channel** — the flusher writes the
outcome of the write into it, and `Append` reads that outcome to unblock the
caller. The buffer size of 1 ensures the flusher is never blocked sending to
`errChan` even if the caller has already left (e.g. due to `doneChan` firing).

### `LogWriter`

```go
type LogWriter struct {
    file     *os.File
    reqChan  chan *writeRequest
    doneChan chan struct{}
    wg       sync.WaitGroup
}
```

| Field      | Purpose |
|------------|---------|
| `file`     | The underlying append-only file handle. Only the flusher goroutine writes to it. |
| `reqChan`  | Buffered channel (capacity 1024) carrying incoming write requests. Acts as a bounded queue and provides back-pressure when the flusher falls behind. |
| `doneChan` | An unbuffered channel that is **closed** (not sent to) when `Close()` is called. Closing broadcasts the shutdown signal to all goroutines simultaneously. |
| `wg`       | Tracks the flusher goroutine. `Close` waits on this before touching the file. |

---

## `NewWriter`

```go
func NewWriter(path string) (*LogWriter, error)
```

1. Opens the file at `path` with `O_APPEND | O_CREATE | O_WRONLY` and mode
   `0644`. `O_APPEND` makes every `Write` syscall atomic at the OS level (all
   writes go to the end of the file regardless of concurrent processes), and
   `O_CREATE` ensures the file is created if it does not yet exist.
2. Allocates the `LogWriter` struct with a 1024-capacity `reqChan` and a fresh
   `doneChan`.
3. Increments the `WaitGroup` and starts the flusher goroutine.
4. Returns the writer immediately — the flusher is already running and ready
   to accept requests.

---

## `Append`

```go
func (lw *LogWriter) Append(key []byte, value []byte, isTombstone bool) error
```

`Append` has two select statements. Each handles a different race with `Close`:

```
Phase 1 — submit the request:

    select {
    case lw.reqChan <- req:   // ① successfully queued
        ...
    case <-lw.doneChan:       // ② Close() called before we could queue
        return os.ErrClosed
    }
```

- **Case ①**: The request was placed in `reqChan`. Execution falls through to
  Phase 2.
- **Case ②**: `Close()` was called before the request could be queued (e.g.
  the channel was full and no flusher was draining it). Returns immediately
  with `ErrClosed`.

```
Phase 2 — wait for the result:

    select {
    case err := <-req.errChan:  // ③ flusher wrote the outcome
        return err
    case <-lw.doneChan:         // ④ Close() called while waiting
        return os.ErrClosed
    }
```

- **Case ③**: The flusher processed the request and sent the write error (or
  nil) back. This is the normal path.
- **Case ④**: `Close()` was called after the request was queued but before the
  flusher got to it. The flusher will drain `reqChan` and send `ErrClosed` to
  every pending request's `errChan`, but since `doneChan` fires first from the
  caller's perspective, `Append` returns `ErrClosed` without waiting.

Both selects are necessary because there are two distinct moments at which a
racing `Close` can preempt the caller.

---

## `Close`

```go
func (lw *LogWriter) Close() error
```

1. `close(lw.doneChan)` — broadcasts the shutdown signal. All goroutines
   selecting on `doneChan` wake up immediately.
2. `lw.wg.Wait()` — blocks until the flusher goroutine has exited. This is
   important: we must not touch the file while the flusher might still be
   writing.
3. `lw.file.Sync()` — issues a final fsync. (The flusher also syncs after each
   batch, so this is a no-op in most cases but provides an extra safety net.)
4. `lw.file.Close()` — releases the OS file descriptor.

---

## `runFlusher` — Group Commit Loop

`runFlusher` is the sole goroutine that writes to `lw.file`. It runs a
select loop over `doneChan` and `reqChan`.

### Shutdown branch (`<-lw.doneChan`)

```go
case <-lw.doneChan:
    for {
        select {
        case req := <-lw.reqChan:
            req.errChan <- os.ErrClosed
        default:
            return
        }
    }
```

When `Close` fires, the flusher drains any requests that were already in the
channel (callers who reached Phase 1 before the signal) and returns
`ErrClosed` to each. This ensures no `Append` call ever blocks forever.

### Write branch (`<-lw.reqChan`) — Group Commit

```go
case req := <-lw.reqChan:
    batch = append(batch, req)

drainLoop:
    for len(batch) < 1024 {
        select {
        case nextReq := <-lw.reqChan:
            batch = append(batch, nextReq)
        default:
            break drainLoop
        }
    }
```

After receiving the first request, the flusher immediately tries to collect
more from the channel **without blocking** (the `default` branch exits as soon
as the channel is empty). This is **group commit**: all `Append` calls that
arrived while the previous batch was being written are picked up together and
will share a single `fsync`. The batch is capped at 1024 entries to bound
latency.

### Write loop

```go
for _, r := range batch {
    packedValueMeta := encodeLength(len(r.value), r.isTombstone)

    n1 := binary.PutUvarint(headerBuf, uint64(len(r.key)))
    n2 := binary.PutUvarint(headerBuf[n1:], packedValueMeta)
    header := headerBuf[:n1+n2]

    lw.file.Write(header)
    lw.file.Write(r.key)
    if !r.isTombstone {
        lw.file.Write(r.value)
    }
}
writeErr = lw.file.Sync()
```

The header is encoded into a 20-byte stack buffer (`headerBuf`) to avoid heap
allocation per record. Two uvariants are packed back-to-back; only the live
slice (`[:n1+n2]`) is passed to `Write`. The write loop short-circuits on the
first error — subsequent requests in the batch get that same error.

A **single `Sync`** is called after all writes for the batch. This is the key
trade-off: every caller in the batch pays the latency of one fsync, but the
throughput cost of the fsync is amortised across all of them.

### Fan-out

```go
for _, r := range batch {
    r.errChan <- writeErr
}
```

The same `writeErr` (or `nil`) is sent to every request in the batch. All
blocked `Append` callers unblock simultaneously.

---

## `encodeLength`

```go
func encodeLength(length int, isTombstone bool) uint64 {
    packed := uint64(length) << 1
    if isTombstone {
        packed |= 1
    }
    return packed
}
```

The tombstone flag occupies bit 0; the length occupies bits 1 and above. This
works correctly because:

- For a normal write, `packed = length << 1` (even number; bit 0 is 0).
- For a tombstone, `packed = 0 << 1 | 1 = 1` (bit 0 is 1, length is 0).

The reader reverses this with `valueMeta & 1` (tombstone) and `valueMeta >> 1`
(length).

---

## Trade-offs and Limitations

| Concern | Decision | Rationale |
|---------|----------|-----------|
| **Latency vs throughput** | Group commit (batch fsync) | A single fsync per batch gives high throughput for concurrent workloads; individual fsync would give lower latency for single-writer workloads. |
| **Back-pressure** | `reqChan` capacity 1024 | Callers block when the flusher is more than 1024 requests behind, preventing unbounded memory growth. |
| **Crash detection** | No CRC / checksum | Truncated tails are detected implicitly by the reader's EOF mapping; silent mid-record corruption is not detected. |
| **Write ordering** | `O_APPEND` + single flusher | `O_APPEND` makes writes atomic at the OS boundary; the single flusher serializes records, so ordering matches the order requests arrive in `reqChan`. |
| **Concurrent safety** | Channel-based | `Append` is safe from any number of goroutines; no mutex is needed in `Append` itself because the channel provides the serialisation point. |
