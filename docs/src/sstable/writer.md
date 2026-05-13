# `writer.go` — Writer

## Overview

`Writer` creates a new SSTable file by accepting key/value pairs in **sorted ascending order** and producing the complete file layout: data blocks → index block → bloom block → footer. It never seeks backward; all writes are strictly sequential.

---

## `Writer` Struct

```go
type Writer struct {
    file         *os.File
    offset       uint64
    dataBuf      []byte
    lastKey      []byte
    indexKeys    [][]byte
    indexHandles []BlockHandle
    pooledBufs   *bloomBufs
    blockStart   uint64
}
```

| Field | Description |
|---|---|
| `file` | The open output file. |
| `offset` | Total bytes written so far (= current write position). Used to compute `BlockHandle.Offset` values. |
| `dataBuf` | Accumulation buffer for the current data block. Flushed when it reaches `BlockSize`. Pre-allocated with `cap = BlockSize`. |
| `lastKey` | The last key appended. Stored here so that when `flushBlock` fires it can record the correct index key without looking at `dataBuf`. |
| `indexKeys` | Slice of last-keys, one per flushed data block. Parallel array with `indexHandles`. |
| `indexHandles` | Slice of `BlockHandle` values, one per flushed data block. |
| `pooledBufs` | A `*bloomBufs` pulled from `bloomBufPool` at construction. Holds two slices — `lens []int` (per-key lengths) and `buf []byte` (concatenated key bytes) — that accumulate bloom filter input without per-key heap allocations. Returned to the pool by `returnBloomBufs()` in `Close`. |
| `blockStart` | Byte offset where the current (unflushed) block started. Used to compute `BlockHandle.Offset` when flushing. |

---

## `NewWriter`

```go
func NewWriter(path string) (*Writer, error)
```

Opens (or creates) the output file with flags `O_CREATE | O_WRONLY | O_TRUNC`, which ensures:

- The file is created if it does not exist.
- An existing file is **truncated to zero** — SSTables are always written fresh, never appended.
- Permissions are set to `0644`.

`dataBuf` is initialized with `make([]byte, 0, BlockSize)` so the backing array is pre-allocated but the slice length starts at zero.

A `*bloomBufs` is pulled from the package-level `bloomBufPool`. If the pool is empty a fresh `bloomBufs` is allocated. If the caller supplies a `keyHint` and the pooled buffer's capacity is smaller, both inner slices are grown to fit before use. See [Bloom Buffer Pool](#bloom-buffer-pool) for details.

---

## `Append`

```go
func (w *Writer) Append(key, value []byte, isTombstone bool) error
```

Encodes one key/value record and appends it to `dataBuf`.

### Record encoding

```
+──────────────────────────────────────────────────────────────+
│ uvarint(keyLen) │ uvarint(valueLen<<1 | tombstone) │ key… │ [value…] │
+──────────────────────────────────────────────────────────────+
```

| Field | Encoding | Notes |
|---|---|---|
| Key length | `uvarint` | Variable-length, 1–10 bytes |
| Value metadata | `uvarint` of `(len(value) << 1) \| isTombstone` | The LSB encodes the tombstone flag; the upper bits hold the value length |
| Key bytes | raw | `keyLen` bytes |
| Value bytes | raw | `valueLen` bytes; **omitted entirely if `isTombstone == true`** |

Tombstone records carry no value bytes. The value length encoded in `valueMeta` will be zero for tombstones (since `len(value)` is typically zero when called with `isTombstone=true`), but the critical indicator is the LSB tombstone flag.

**After encoding:**

- `w.lastKey` is updated to a copy of `key`.
- `key` length and bytes are appended to `w.pooledBufs` (`lens` and `buf` respectively) for later bloom filter construction.
- If `len(w.dataBuf) >= BlockSize`, `flushBlock()` is called.

---

## `flushBlock`

```go
func (w *Writer) flushBlock() error
```

Writes the contents of `dataBuf` to the file as a complete data block.

**Steps:**

1. Return immediately if `dataBuf` is empty (no-op, prevents writing empty blocks).
2. `file.Write(dataBuf)` — writes the entire buffer as one syscall.
3. Record the flushed block in the index:
   - Append `lastKey` (copied) to `indexKeys`.
   - Append `BlockHandle{Offset: blockStart, Length: n}` to `indexHandles`.
4. Advance `offset` and `blockStart` by `n` (bytes written).
5. Reset `dataBuf` to length zero (retains backing array capacity).

The result is that each entry in `indexKeys[i]` / `indexHandles[i]` describes the last key and byte range of data block `i`.

---

## `Close`

```go
func (w *Writer) Close() error
```

Finalizes and closes the SSTable. Must be called exactly once after all `Append` calls.

**Sequence:**

```
flushBlock()          ← flush the last partial data block
writeIndexBlock()     ← write index, capture IndexHandle
writeBloomBlock()     ← write bloom, capture BloomHandle
returnBloomBufs()     ← reset pooledBufs and return to bloomBufPool
writeFooter(...)      ← write 33-byte footer (handles + FormatVersion)
file.Sync()           ← fsync: ensure data reaches disk
file.Close()
```

`returnBloomBufs()` is called immediately after `writeBloomBlock()` on **both the success and error paths**, so the pooled buffer is reclaimed as early as possible regardless of whether the subsequent footer write or sync fails.

---

## `writeIndexBlock`

```go
func (w *Writer) writeIndexBlock() (BlockHandle, error)
```

Writes the index block immediately after the last data block. Returns a `BlockHandle` pointing to it.

### Index entry format

```
+─────────────────────────────────────────────────────────────────+
│ uvarint(keyLen) │ key bytes (lastKey of block) │ Offset (8B LE) │ Length (8B LE) │
+─────────────────────────────────────────────────────────────────+
```

| Field | Encoding | Notes |
|---|---|---|
| Key length | `uvarint` | Length of the index key |
| Key bytes | raw | Last key of the corresponding data block |
| `Offset` | 8-byte little-endian `uint64` | Start byte of the data block |
| `Length` | 8-byte little-endian `uint64` | Byte count of the data block |

Entries are written sequentially, one per flushed data block. The returned `BlockHandle.Offset` is `w.offset` before writing, and `BlockHandle.Length` is the total bytes written for all entries.

### Note on index key semantics

The index key is the **last key** of the data block, not a separator key. This means binary search during `Get` must locate the **leftmost** index entry whose key is `>=` the query key:

```
lo, hi := 0, len(index)-1
for lo <= hi {
    mid := (lo + hi) / 2
    if index[mid].lastKey < queryKey { lo = mid+1 } else { blockIdx = mid; hi = mid-1 }
}
```

If the query key equals the last key of block `i`, the key is in block `i`. If the query key falls between block `i`'s last key and block `i+1`'s last key, it is in block `i+1`.

---

## `writeBloomBlock`

```go
func (w *Writer) writeBloomBlock() (BlockHandle, error)
```

Builds a `BloomFilter` from all keys recorded in `w.pooledBufs` (every key ever passed to `Append`), encodes it, and writes it after the index block.

```go
bloom := newBloomFilter(w.pooledBufs)
data  := bloom.Encode()          // k (4B LE) | bits
```

Key data is read from `pooledBufs.lens` (per-key lengths) and `pooledBufs.buf` (concatenated raw key bytes). The buffer is **not** released here; `returnBloomBufs()` in `Close` handles that immediately after this call returns.

Returns a `BlockHandle` for the written bloom data.

---

## `writeFooter`

```go
func (w *Writer) writeFooter(indexHandle, bloomHandle BlockHandle) error
```

Writes the fixed **33-byte** footer:

```
bytes  0– 7:  indexHandle.Offset
bytes  8–15:  indexHandle.Length
bytes 16–23:  bloomHandle.Offset
bytes 24–31:  bloomHandle.Length
byte  32:     FormatVersion (0x02)
```

---

## Data Block Record Format

```
┌───────────────┬─────────────────────────────────┬──────────────┬───────────────────────────┐
│ uvarint       │ uvarint                          │ key bytes    │ value bytes               │
│ (keyLen)      │ (valueLen<<1 | tombstone_flag)   │ [keyLen B]   │ [valueLen B] (live only)  │
└───────────────┴─────────────────────────────────┴──────────────┴───────────────────────────┘
```

- **uvarint** encoding uses 1–10 bytes depending on the value magnitude; this avoids fixed-width overhead for small keys/values.
- The tombstone flag is the **least-significant bit** of the value-metadata varint.
- Value bytes are **absent** for tombstone records, saving space.

---

## Index Block Entry Format

```
┌───────────────┬──────────────────────────┬────────────────────┬────────────────────┐
│ uvarint       │ key bytes                │ Offset             │ Length             │
│ (keyLen)      │ [keyLen B] = lastKey     │ [8 B, little-end]  │ [8 B, little-end]  │
└───────────────┴──────────────────────────┴────────────────────┴────────────────────┘
```

---

## Trade-offs and Design Notes

| Aspect | Decision | Rationale |
|---|---|---|
| **No prefix compression** | Keys stored verbatim in index | Simplicity; LevelDB uses shared-prefix encoding for dense key spaces. |
| **No restart points** | Block scan is always linear from offset 0 | Restart points would allow binary search within a block but add complexity. |
| **Variable-size data blocks** | `dataBuf` is flushed when `>= BlockSize`, not exactly at `BlockSize` | A record that straddles the threshold is always written intact to the current block, so blocks may slightly exceed `BlockSize`. |
| **Tombstones in SSTables** | Tombstone records are stored with the LSB flag set | Tombstones must persist across flushes so that older entries in lower levels are suppressed. Compaction removes them once no older data exists. |
| **Bloom built at Close** | Key data accumulated in pooled `bloomBufs`; buffer returned immediately after `writeBloomBlock` | Trades memory (key data held during the write) for simplicity. The pool eliminates the allocation cost across repeated writes; returning the buffer before `writeFooter` bounds peak memory tightly. |
| **fsync before Close** | `file.Sync()` called | Ensures the SSTable is durable before the writer returns; prevents silent data loss on crash. |

---

## Bloom Buffer Pool

### Motivation

Before pooling, each `Writer` allocated `bloomKeys [][]byte` — a slice of copies of every key appended during the write session. For a 5-second compaction or flush run this produced **98+ MB** of bloom buffer allocations, consuming roughly **25% of total CPU time** in GC.

### Design

```
bloomBufPool = make(chan *bloomBufs, 4)   // pool of 4 pre-allocated buffer pairs
```

`bloomBufs` is a small struct holding two slices:

| Field | Purpose |
|---|---|
| `lens []int` | Per-key lengths, one entry per `Append` call |
| `buf  []byte` | Concatenated raw key bytes |

Using two parallel slices instead of `[][]byte` means there is **one contiguous allocation** for key bytes rather than one allocation per key.

### Lifecycle

| Event | Action |
|---|---|
| `NewWriter()` | Pulls `*bloomBufs` from pool; grows slices if `cap(bb.lens) < keyHint` |
| `Append(key, …)` | Appends `len(key)` to `bb.lens`; appends `key` bytes to `bb.buf` |
| `writeBloomBlock()` | Reads `bb.lens` and `bb.buf` to construct the bloom filter |
| `returnBloomBufs()` | Resets both slices to `[:0]` (retains backing arrays); non-blocking send back to pool |

`returnBloomBufs()` is invoked by `Close()` **immediately after** `writeBloomBlock()` returns, on both success and error paths. This is the earliest safe moment — the bloom data has been written and the buffer is no longer needed before `writeFooter`, `Sync`, or `file.Close` execute.

### Why a channel pool instead of `sync.Pool`?

`sync.Pool` drops all entries at each GC cycle. Because bloom buffer allocations are large and the flush/compaction cycle timing correlates with GC pressure, `sync.Pool` would reclaim the buffers exactly when they are needed again. A **channel pool** retains entries across GC pauses, guaranteeing reuse.

### Performance impact

Pooling reduces bloom-related allocations from 98+ MB per 5-second run to a fixed constant, and eliminates the associated ~25% GC CPU overhead.
