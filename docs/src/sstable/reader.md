# `reader.go` — Reader and sstableIterator

## Overview

`Reader` opens an existing SSTable file, loads the index and bloom filter into memory at open time, and provides point lookups (`Get`) and sequential access (`Iterator`). All data-block reads are random-access `ReadAt` calls against the open file descriptor.

`sstableIterator` implements `memtable.MemTableIteratorI` and provides forward iteration, seeking, and tombstone visibility over a single SSTable.

---

## `indexEntry`

```go
type indexEntry struct {
    lastKey []byte
    handle  BlockHandle
}
```

An in-memory representation of one entry from the index block.

| Field | Description |
|---|---|
| `lastKey` | The last key of the data block. Used as the comparison value during binary search. |
| `handle` | The `BlockHandle` (offset + length) used to read the data block from disk. |

---

## `Reader` Struct

```go
type Reader struct {
    file  *os.File
    index []indexEntry
    bloom *BloomFilter
}
```

| Field | Description |
|---|---|
| `file` | The open file descriptor. Kept open for the lifetime of the `Reader` to serve `ReadAt` calls. |
| `index` | Slice of `indexEntry` values, one per data block, loaded entirely into memory at open time. |
| `bloom` | The deserialized `BloomFilter`, loaded entirely into memory at open time. |

Both `index` and `bloom` are held in memory for the life of the `Reader`, so every `Get` call can perform the bloom check and binary search without any file I/O.

---

## `NewReader`

```go
func NewReader(path string) (*Reader, error)
```

**Full open sequence:**

```
1.  os.OpenFile(path, O_RDONLY, 0)
2.  file.Stat()  →  get file size
3.  Validate: size >= FooterSize (33)  →  error if not
4.  file.ReadAt(buf[33], size−33)   →  read footer bytes
5.  Parse Footer:
      IndexHandle = {LE64(buf[0:8]),  LE64(buf[8:16])}
      BloomHandle = {LE64(buf[16:24]), LE64(buf[24:32])}
5a. Check buf[32] == FormatVersion (0x02)  →  error if not
      (prevents silent wrong results when bloom hash changes across versions)
6.  file.ReadAt(bloomData, BloomHandle.Offset)  →  read bloom block
7.  DecodeBloom(bloomData)  →  BloomFilter in memory
8.  file.ReadAt(indexData, IndexHandle.Offset)  →  read index block
9.  parseIndexBlock(indexData)  →  []indexEntry in memory
10. Return &Reader{file, index, bloom, blockPool}
```

On **any error** in steps 2–9, the file is closed before returning (`_ = f.Close()`). The caller never receives a `Reader` with a leaked file descriptor.

---

## `Path`

```go
func (r *Reader) Path() string { return r.file.Name() }
```

Returns the file path as reported by the OS. Used by the store layer when removing obsolete SSTables — the store identifies which file to delete by its path rather than by any internal ID.

---

## `Get`

```go
func (r *Reader) Get(key []byte) ([]byte, error)
```

Three-tier lookup that minimises disk I/O:

### Tier 1 — Bloom filter

```go
if !r.bloom.MayContain(key) {
    return nil, &src.KeyNotFoundError{Key: key}
}
```

If the bloom filter returns `false`, the key is **definitively absent** from this SSTable. No disk I/O is performed. For workloads with many lookups for non-existent keys, this filter eliminates ~99% of block reads.

### Tier 2 — Binary search on the in-memory index

```go
lo, hi := 0, len(r.index)-1
blockIdx := -1
for lo <= hi {
    mid := (lo + hi) / 2
    if bytes.Compare(r.index[mid].lastKey, key) < 0 {
        lo = mid + 1
    } else {
        blockIdx = mid
        hi = mid - 1
    }
}
if blockIdx == -1 {
    return nil, &src.KeyNotFoundError{Key: key}
}
```

The search finds the **leftmost** index entry whose `lastKey >= key`. Because the index key is the *last* key of each data block:

- If `lastKey[i] < key` for all `i`, the key is beyond all blocks → `KeyNotFoundError`.
- Otherwise, `blockIdx` identifies the first block that *could* contain `key`.

### Tier 3 — Block read + linear scan

```go
data, err := r.readBlock(r.index[blockIdx].handle)
// ...
return scanBlock(data, key)
```

Loads exactly one data block from disk and scans it linearly. See [`scanBlock`](#scanblock) below.

---

## `readBlock`

```go
func (r *Reader) readBlock(handle BlockHandle) ([]byte, error)
```

Allocates a `[]byte` of `handle.Length` bytes and fills it with a single `ReadAt` call:

```go
data := make([]byte, handle.Length)
_, err := r.file.ReadAt(data, int64(handle.Offset))
```

No buffering or block cache is involved. Each call allocates fresh memory. See [trade-offs](#trade-offs-and-design-notes).

---

## `parseIndexBlock`

```go
func parseIndexBlock(data []byte) ([]indexEntry, error)
```

Decodes the index block byte-by-byte. The format is a sequence of entries with no delimiter between them:

```
for each entry:
  ┌───────────────┬──────────────────────────┬────────────────────┬────────────────────┐
  │ uvarint       │ key bytes                │ Offset             │ Length             │
  │ (keyLen)      │ [keyLen B]               │ [8 B, little-end]  │ [8 B, little-end]  │
  └───────────────┴──────────────────────────┴────────────────────┴────────────────────┘
```

**Error conditions checked:**

- `binary.Uvarint` returns `n <= 0` → malformed index block.
- `pos + keyLen + 16 > len(data)` → truncated index block.

Parsing stops when `pos` reaches the end of `data`.

---

## `scanBlock`

```go
func scanBlock(data, key []byte) ([]byte, error)
```

Linear scan of a data block looking for `key`. Records are decoded in the same format written by `Writer.Append`:

```
uvarint(keyLen) | uvarint(valueLen<<1 | tombstone) | key bytes | [value bytes]
```

**Scan logic:**

1. Decode `keyLen` and `valueMeta` (varint); extract `valueLen = valueMeta >> 1` and `isTombstone = valueMeta & 1`.
2. Read `k = data[pos : pos+keyLen]`.
3. If `!isTombstone`, read `v = data[pos : pos+valueLen]`.
4. Compare `k` with `key`:
   - `cmp == 0`: found.
     - If tombstone: return `src.ErrTombstone`.
     - Otherwise: return a **copy** of `v` (safe to use after `data` is GC'd).
   - `cmp > 0`: the scan has passed the target key (sorted order) → break early.
   - `cmp < 0`: continue to next record.
5. If the loop ends without a match: return `KeyNotFoundError`.

The early-exit on `cmp > 0` prevents scanning the entire block when the key is absent.

---

## `Iterator`

```go
func (r *Reader) Iterator() memtable.MemTableIteratorI
```

Creates an `sstableIterator` positioned at the **first record** of the SSTable:

1. Construct `sstableIterator{r: r, blockIdx: 0}`.
2. Load block 0 into `it.blockData` (if the file has at least one block).
3. Call `it.advance()` to decode the first record and populate `currKey`, `currValue`, `currTombstone`.

Returns the iterator. The caller must call `it.Close()` when done (though this does not close the file).

---

## `Close` (Reader)

```go
func (r *Reader) Close() error { return r.file.Close() }
```

Closes the file descriptor. Must be called when the `Reader` is no longer needed. Outstanding `sstableIterator` values become invalid after this call.

---

## `sstableIterator` Struct

```go
type sstableIterator struct {
    r             *Reader
    blockIdx      int
    blockData     []byte
    blockPos      int
    currKey       []byte
    currValue     []byte
    currTombstone bool
    valid         bool
}
```

| Field | Description |
|---|---|
| `r` | Pointer to the owning `Reader`. Used for block reads and index access. |
| `blockIdx` | Index into `r.index` identifying the currently loaded block. |
| `blockData` | Raw bytes of the currently loaded data block. |
| `blockPos` | Byte offset within `blockData` for the next `readEntry` call. |
| `currKey` | Key of the current record (after last `advance`). |
| `currValue` | Value of the current record (`nil` for tombstones). |
| `currTombstone` | Whether the current record is a tombstone. |
| `valid` | `false` when the iterator has exhausted all records or been closed. |

---

## `loadBlock`

```go
func (it *sstableIterator) loadBlock(idx int) bool
```

Reads block `idx` from disk into `it.blockData` and resets `it.blockPos` to 0. Returns `false` if `idx` is out of range or the read fails. On success, sets `it.blockIdx = idx`.

---

## `readEntry`

```go
func (it *sstableIterator) readEntry() (key, value []byte, isTombstone bool, ok bool)
```

Decodes the next record from `blockData[blockPos:]`.

**Auto-advance to next block**: if `blockPos >= len(blockData)`, calls `loadBlock(blockIdx + 1)`. This transparently crosses block boundaries during sequential iteration — the caller (and user) see a flat stream of records.

**Boundary checks** prevent out-of-bounds slicing: if a varint decode fails or the data is shorter than expected, `ok = false` is returned.

If decoding succeeds, `it.blockPos` is advanced past the decoded record.

---

## `advance`

```go
func (it *sstableIterator) advance()
```

Calls `readEntry`. On success, updates `currKey`, `currValue`, `currTombstone`, and sets `valid = true`. On failure (end of file or decode error), sets `valid = false`.

---

## Iterator Interface Methods

| Method | Behaviour |
|---|---|
| `Valid() bool` | Returns `it.valid`. |
| `Key() []byte` | Returns `it.currKey`. Undefined if `!Valid()`. |
| `Value() []byte` | Returns `it.currValue`. `nil` for tombstone records. |
| `IsTombstone() bool` | Returns `it.currTombstone`. |
| `Next()` | Calls `advance()` to move to the next record. |

---

## `Seek`

```go
func (it *sstableIterator) Seek(key []byte)
```

Positions the iterator at the first record with `Key >= key`.

**Steps:**

1. **Binary search** on `r.index` for the leftmost block whose `lastKey >= key` (same logic as `Get`).
2. If no such block exists (`blockIdx >= len(r.index)`), set `valid = false`.
3. `loadBlock(blockIdx)`.
4. `advance()` to read the first record.
5. **Linear scan forward** while `currKey < key`:
   ```go
   for it.valid && bytes.Compare(it.currKey, key) < 0 {
       it.advance()
   }
   ```
   This handles keys that fall in the middle of a block.

---

## `Close` (sstableIterator)

```go
func (it *sstableIterator) Close() error { it.valid = false; return nil }
```

Marks the iterator as invalid. **Does not close the file** — that is the `Reader`'s responsibility. Multiple iterators can be open on the same `Reader` simultaneously; closing one does not affect the others.

---

## Trade-offs and Design Notes

| Aspect | Decision | Rationale |
|---|---|---|
| **No block cache** | Each `readBlock` allocates fresh memory | Simplicity. Production systems (RocksDB, LevelDB) cache hot blocks in an LRU to avoid repeated allocation and I/O. |
| **Read amplification** | `Get` reads at most one data block | The bloom filter reduces the common case (absent key) to zero block reads. For present keys, exactly one block is read. |
| **Index in memory** | Entire index loaded at `NewReader` | For typical SSTable sizes, the index is small (one 8–64 byte entry per 4 KB block). Holding it in memory eliminates index block reads during lookup. |
| **Tombstones exposed** | `IsTombstone()` returns `true` for deletions | Compaction and merge iterators need to see tombstones so they can suppress older versions of the same key from lower SSTable levels. |
| **Iterator does not own the file** | `Close()` on iterator sets `valid=false` only | Allows the store to create multiple iterators from one `Reader` (e.g., for merge compaction) without lifetime coupling between iterators and the file. |
| **Linear scan within block** | No binary search within a data block | Blocks are small (≤ 4 KB + one record). Binary search within a block would require storing record offsets. The simplicity win outweighs the minor scan cost. |
