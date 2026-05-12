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
    bloomKeys    [][]byte
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
| `bloomKeys` | Every key appended to the writer. Passed to `newBloomFilter` in `Close`. |
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
- `key` is appended (as a copy) to `w.bloomKeys`.
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
writeFooter(...)      ← write 32-byte footer
file.Sync()           ← fsync: ensure data reaches disk
file.Close()
```

If any step returns an error, `Close` returns immediately without continuing the sequence.

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

Builds a `BloomFilter` from all keys recorded in `w.bloomKeys` (every key ever passed to `Append`), encodes it, and writes it after the index block.

```go
bloom := newBloomFilter(w.bloomKeys)
data  := bloom.Encode()          // k (4B LE) | bits
```

Returns a `BlockHandle` for the written bloom data.

---

## `writeFooter`

```go
func (w *Writer) writeFooter(indexHandle, bloomHandle BlockHandle) error
```

Writes the fixed 32-byte footer as eight `uint64` values in little-endian order:

```
bytes  0– 7:  indexHandle.Offset
bytes  8–15:  indexHandle.Length
bytes 16–23:  bloomHandle.Offset
bytes 24–31:  bloomHandle.Length
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
| **Bloom built at Close** | All keys buffered in `bloomKeys` | Trades memory (all keys held during the write) for simplicity; the alternative is incremental filter construction. |
| **fsync before Close** | `file.Sync()` called | Ensures the SSTable is durable before the writer returns; prevents silent data loss on crash. |
