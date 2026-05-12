# LogWriter — Write-Ahead Log Writer

`LogWriter` is the concrete implementation of `LogWriterI`. Its defining
characteristic is **write-stealing leader election**: every `Append` caller
enqueues its request into a shared pending slice and then races to acquire the
leader lock. The winner serialises all currently-pending requests into a single
`file.Write` syscall and signals every writer's `errChan`. Losers that acquire
the lock afterwards find their request already written and return immediately
— their write was "stolen" by the previous leader.

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

A `writeRequest` carries one `Append` call's data. `errChan` is a
**per-request, buffered-1 channel** — whoever writes the batch sends the
outcome (nil or an error) into it, and `Append` blocks reading from it until
that signal arrives. The capacity of 1 ensures the sender is never blocked,
even if the corresponding `Append` has already returned (e.g. on shutdown).
`writeRequest` objects are recycled via `reqPool` to reduce allocation
pressure.

### `LogWriter`

```go
type LogWriter struct {
    file      *os.File
    mu        sync.Mutex      // leader-election lock
    pendingMu sync.Mutex      // guards pending slice
    pending   []*writeRequest // queue of waiting writers
    batchBuf  []*writeRequest // scratch batch buffer; owned by leader under mu
    writeBuf  []byte          // scratch write buffer; owned by leader under mu

    closed   atomic.Bool
    doneChan chan struct{}
    wg       sync.WaitGroup

    syncInterval time.Duration
    reqPool      sync.Pool
}
```

| Field         | Purpose |
|---------------|---------|
| `file`        | The underlying append-only file handle. Written only by the leader under `mu`. |
| `mu`          | Leader-election lock. Exactly one goroutine holds this at a time; that goroutine is the current leader and owns `batchBuf` and `writeBuf`. |
| `pendingMu`   | Protects the `pending` slice. Held briefly by both enqueuing callers and the draining leader. |
| `pending`     | Shared queue of in-flight write requests. All callers append here (under `pendingMu`) before competing for `mu`. |
| `batchBuf`    | Scratch slice the leader uses to snapshot `pending`. Reused across rounds to avoid allocation. Owned by the leader while `mu` is held. |
| `writeBuf`    | Byte slice the leader serialises records into before the single `file.Write` call. Reused across rounds. Owned by the leader while `mu` is held. |
| `closed`      | Atomic flag set by `Close`. Read without any lock in the fast-path check at the top of `Append`. |
| `doneChan`    | Closed by `Close()` to signal `syncLoop` to stop. |
| `wg`          | Tracks `syncLoop`. `Close` waits on it before touching the file. |
| `syncInterval`| How often `syncLoop` calls `file.Sync`. Defaults to `DefaultSyncInterval` (10 ms). |
| `reqPool`     | `sync.Pool` of `*writeRequest` objects. Avoids per-`Append` heap allocation. |

**Lock ordering:** `pendingMu` is always acquired while `mu` is held (never the
reverse). `syncLoop` acquires neither lock.

---

## `NewWriter`

```go
func NewWriter(path string) (*LogWriter, error)
```

1. Opens the file at `path` with `O_APPEND | O_CREATE | O_WRONLY` and mode
   `0644`. `O_APPEND` ensures every `Write` syscall appends atomically at the
   OS level; `O_CREATE` creates the file if it does not yet exist.
2. Allocates `LogWriter` with pre-allocated backing arrays:
   - `pending` and `batchBuf` with initial capacity 64 (grow as needed)
   - `writeBuf` with initial capacity 64 KiB (covers ~1000 typical records
     before growing)
3. Initialises `reqPool` so every pooled `writeRequest` has a pre-allocated
   buffered-1 `errChan`.
4. Starts `syncLoop` in a goroutine tracked by `wg`.
5. Returns the writer immediately — it is ready to accept `Append` calls.

---

## `Append`

```go
func (lw *LogWriter) Append(key []byte, value []byte, isTombstone bool) error
```

`Append` proceeds in two phases: **enqueue** and **compete**.

### Phase 1 — Enqueue

```
1. Fast-path: if closed.Load() → return os.ErrClosed

2. req = reqPool.Get()           // recycle or allocate a writeRequest
   req.key, req.value, req.isTombstone = ...

3. pendingMu.Lock()
   pending = append(pending, req) // publish our request
   pendingMu.Unlock()
```

The request is published to `pending` **before** `mu` is acquired. This
ordering is the key correctness invariant: any leader that takes `mu` after
step 3 is guaranteed to see this request in `pending` and will signal
`req.errChan`.

### Phase 2 — Compete (leader election)

```
4. mu.Lock()   // compete; losers block here
```

Only one goroutine wins `mu` at a time. The winner becomes the **leader** for
this round. Losers block until the current leader releases `mu`.

**If `closed` is true after winning `mu`** (writer was closed while waiting):

```
5a. pendingMu.Lock()
    for each r in pending: r.errChan <- os.ErrClosed
    pending = pending[:0]
    pendingMu.Unlock()
    mu.Unlock()

    err := <-req.errChan   // our errChan was signalled above
    reqPool.Put(req)
    return err             // returns os.ErrClosed
```

**Normal leader path:**

```
5b. pendingMu.Lock()
    batchBuf = append(batchBuf[:0], pending...)  // snapshot all pending
    pending  = pending[:0]                        // reset for next round
    pendingMu.Unlock()
```

```
6. if len(batchBuf) > 0:
       writeBuf = writeBuf[:0]
       for each r in batchBuf:
           writeBuf = appendRecord(writeBuf, r)   // serialise
       file.Write(writeBuf)                        // single syscall
       for each r in batchBuf:
           r.errChan <- writeErr                   // fan-out result

   // batchBuf empty → a previous leader already wrote our request
```

```
7. mu.Unlock()

8. err := <-req.errChan   // block until our errChan is signalled
   reqPool.Put(req)
   return err
```

### Why "write-stealing"

Goroutines that lose the `mu` competition block at step 4. By the time they
win, the previous leader will have already drained `pending` (step 5b), which
included their request. When they win `mu`, `batchBuf` is empty after the
drain (step 6), and `req.errChan` has already been signalled by the previous
leader. They unblock at step 8 immediately without touching the file. Their
write was **stolen** by the leader.

### Correctness guarantee

Every `Append` call's `errChan` is signalled **exactly once**:

- The request is appended to `pending` (step 3) before `mu` is acquired
  (step 4).
- The leader that holds `mu` drains all of `pending` while still holding `mu`
  (step 5b). No two leaders can drain simultaneously.
- Therefore every request is drained by exactly one leader, and that leader
  sends to `errChan` exactly once (step 6 or step 5a on shutdown).
- Step 8 always reads from `errChan`, regardless of which code path ran.

---

## `Close`

```go
func (lw *LogWriter) Close() error
```

`Close` follows a strict four-step sequence:

1. **`close(lw.doneChan)`** — signals `syncLoop` to stop. `syncLoop` exits on
   its next tick or immediately if it is already selecting.
2. **`lw.wg.Wait()`** — blocks until `syncLoop` has exited. This ensures
   `file.Sync` in `syncLoop` does not race with the final `file.Sync` below.
3. **Acquire `mu`, set `closed`, drain `pending`** — acquiring `mu` waits for
   any in-flight leader to finish. Setting `closed = true` and draining
   `pending` with `ErrClosed` is done atomically from the perspective of racing
   `Append` callers: any caller that reaches step 3 of `Append` after this
   point will see `closed == true` immediately after winning `mu` and will
   follow the shutdown branch (step 5a).
4. **`file.Sync()` then `file.Close()`** — final durability flush and release
   of the OS file descriptor. `Close` always syncs regardless of when the last
   `syncLoop` tick ran.

---

## `syncLoop` — Periodic OS Sync

```go
func (lw *LogWriter) syncLoop()
```

`syncLoop` runs in a dedicated goroutine started by `NewWriter`. It ticks every
`syncInterval` (default 10 ms) and calls `file.Sync()` to push OS page-cache
data to durable storage. It exits as soon as `doneChan` is closed.

`syncLoop` never acquires `mu` or `pendingMu`. This is intentional:
`file.Sync` is a separate syscall that does not interfere with `file.Write`,
so `syncLoop` never blocks or is blocked by concurrent `Append` calls.

**Durability model:**

| Event | What is durable |
|-------|----------------|
| `file.Write` completes (inside leader) | Data in OS page cache — survives process crash, not power loss |
| Next `syncLoop` tick (≤10 ms later) | Data on disk — survives power loss |
| `Close()` returns | All data on disk — final sync always called |

`Append` returns to the caller as soon as `file.Write` completes. The gap
between `Append` returning and the next `syncLoop` sync is the window of
potential data loss on a power failure. For workloads that require stronger
guarantees, `syncInterval` can be reduced or `Close` can be called after every
logical batch.

---

## `appendRecord`

```go
func appendRecord(buf []byte, r *writeRequest) []byte
```

`appendRecord` appends a single record to `buf` and returns the extended slice.
It encodes:

1. `keyLen` as a uvarint
2. `(len(value) << 1) | isTombstone` as a uvarint (via `encodeLength`)
3. The raw key bytes
4. The raw value bytes (omitted for tombstones)

The two uvarint header fields are written into a 20-byte stack-allocated array
(`[20]byte`) — large enough for any two uvarints (max 10 bytes each) — and
only the live prefix is appended to `buf`. This avoids any heap allocation per
record. All records in a batch are serialised into the shared `writeBuf` by the
leader before a single `file.Write` call.

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
| **Latency vs throughput** | Write-stealing leader election with one `file.Write` per batch | Eliminates ~900 ns goroutine context-switch overhead of the old channel-based flusher. Sequential writes are ~35–51% faster; concurrent throughput is similar, as lock contention naturally batches writers. |
| **Back-pressure** | None (unbounded `pending` slice) | No fixed queue capacity means callers never block on enqueueing. Memory growth is bounded in practice by the rate at which leaders drain. |
| **Crash detection** | No CRC / checksum | Truncated tails are detected implicitly by the reader's failed uvarint or `ReadFull` decode, mapped to `io.EOF`. Silent mid-record corruption is not detected. |
| **Write ordering** | `O_APPEND` + single leader per round | `O_APPEND` makes the combined `file.Write` atomic at the OS boundary. The leader serialises all records in `batchBuf` order, which matches the order they were appended to `pending`. |
| **Concurrent safety** | `pendingMu` for enqueue, `mu` for leader election | `Append` is safe from any number of goroutines. `pendingMu` is held only for the brief slice append/snapshot; `mu` is held only for the file write + fan-out. |
| **Allocation** | `reqPool` recycles `writeRequest` objects; `batchBuf`/`writeBuf` are reused across rounds | Keeps per-`Append` allocation near zero at steady state. |
