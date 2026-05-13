# Store — struct and interface

## `StoreI` interface

```go
type StoreI interface {
    Put(key []byte, value []byte) error
    Get(key []byte) ([]byte, error)
    Delete(key []byte) error
    Scan(startKey []byte, endKey []byte) (mt.MemTableIteratorI, error)
    Close() error
}
```

`StoreI` is the user-facing contract. The concrete `Store` struct implements it.

### Method contracts

#### `Put(key, value) error`

Upsert. Writes `value` for `key`. If `key` already exists its value is replaced.
There is no uniqueness constraint — duplicate puts are silently accepted, and the
most recent put wins on a subsequent `Get`.

Steps (under the dual-lock protocol):
1. Check `bgErr` under `mu.RLock()`; surface any previous background-flush error immediately.
2. Append a non-tombstone record to the WAL (while holding `mu.RLock()`; WAL batches concurrent writers).
3. Insert into the active memtable under `memMu.Lock()`.
4. After releasing both locks, if `memtable.SizeInBytes() > sizeThreshold` and no flush is already
   in progress, re-acquire `mu.Lock()` and call `freeze()` (with a double-check inside).

Returns the first error encountered; a WAL error prevents the SkipList insert, so
partial writes are **not** possible.

---

#### `Get(key) ([]byte, error)`

Point lookup. Returns the value associated with `key`, or an error that satisfies
`errors.Is(err, src.ErrKeyNotFound)` if the key does not exist or has been deleted.

Lookup order (under read lock):

```
1. active memtable
2. immutable memtable (if a flush is in progress)
3. L0: probe every reader newest-first (files may overlap)
       found + tombstone → return KeyNotFoundError
       found + live      → return value
4. L1: binary search for the reader whose [MinKey, MaxKey] covers key
       (at most one reader can contain the key because L1 is non-overlapping)
       found + tombstone → return KeyNotFoundError
       found + live      → return value
5. L2: same binary-search strategy as L1
6. return KeyNotFoundError
```

L0 is probed exhaustively because L0 files may have overlapping key ranges (they
are written directly from memtable flushes). L1 and L2 maintain the invariant that
files are sorted and non-overlapping, so `findLevelReader` can binary-search to
the single candidate file in O(log n) rather than O(n).

At each source, if a **tombstone** is found the search stops and
`&KeyNotFoundError{Key: key}` is returned — the tombstone shadows all older
sources.

---

#### `Delete(key) error`

Tombstone write. Records a deletion marker for `key`. The operation succeeds even
if `key` does not exist; it is the caller's responsibility to decide whether
absence matters.

Internally identical to `Put` with `isTombstone=true`. The tombstone propagates
through the WAL, the memtable, and eventually into an SSTable on flush. Tombstones
are preserved in L0 and L1 output during compaction; they are dropped only in L2
output (the bottom level, where no older data source exists below it).

---

#### `Scan(startKey, endKey []byte) (MemTableIteratorI, error)`

Range scan over **[startKey, endKey)** (start inclusive, end exclusive).

Returns a `mergeIterator` that merges all live data sources across all levels in
newest-first order. Tombstoned keys are silently skipped (the iterator is created
with `includeTombstones=false`).

The caller **must** call `Close()` on the returned iterator when done.

**Concurrency note.** The read lock (`mu.RLock`) is held only for the duration of `Scan` itself
(while the iterator is constructed); `memMu.RLock` is held for the shorter inner
window of `memtable.Iterator()`. Once the iterator is returned, the caller
operates on it without holding any lock. This is safe because:
- The memtable is append-only — entries already present when the iterator was
  created are stable.
- The immutable memtable, if present, is frozen and never modified.
- SSTable readers are not closed until `Store.Close()` (or until they are replaced
  by compaction, which only happens under `mu.Lock()` and only after the in-memory
  state has been updated).

---

#### `Close() error`

Graceful shutdown. After `Close` returns, the `Store` must not be used.

Steps:
1. `flushWg.Wait()` — block until any in-flight background flush completes.
2. Acquire write lock.
3. If the active memtable is non-empty, call `flushSync()` to write a final SSTable.
4. Close the WAL writer.
5. Close every SSTable reader across all levels.
6. Close the manifest.

Returns the first error encountered. If an error occurs partway through, some
resources may not be fully released — callers should treat a non-nil error from
`Close` as unrecoverable.

---

## `Store` struct

```go
type Store struct {
    memtable  mt.MemTableI
    immutable mt.MemTableI
    wal       w.LogWriterI
    levels    [numLevels][]*sst.Reader
    manifest  *manifest
    cache     *blockCache
    walPath   string
    dir       string
    mu        sync.RWMutex
    memMu     sync.RWMutex
    bgErr     error
    flushWg   sync.WaitGroup
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `memtable` | `mt.MemTableI` | Active in-memory skip-list. All writes go here first. |
| `immutable` | `mt.MemTableI` | Non-nil only while a background flush is in progress. Frozen snapshot of the previous active memtable. |
| `wal` | `w.LogWriterI` | Write-ahead log writer for the active memtable. |
| `levels` | `[3][]*sst.Reader` | Three-level SSTable hierarchy. `levels[0]` (L0) is newest-first and may have overlapping key ranges. `levels[1]` (L1) and `levels[2]` (L2) are sorted by `MinKey` and non-overlapping within each level. |
| `manifest` | `*manifest` | Tracks which SSTable files are live on disk, including their level. |
| `cache` | `*blockCache` | Shared LRU block cache (default 8 MB). Passed to every `sst.NewReader` call. Entries for deleted SSTables are invalidated during compaction via `cache.remove(path)`. |
| `walPath` | `string` | Absolute path of the active WAL file. The immutable WAL lives at `walPath + ".immutable"`. |
| `dir` | `string` | Directory that holds SSTable files and the MANIFEST. |
| `mu` | `sync.RWMutex` | Guards the SSTable levels, the `immutable` pointer, `bgErr`, and `flushWg`. Held shared (`RLock`) during normal reads and writes; held exclusively (`Lock`) only for freeze and compaction. Always acquired before `memMu`. |
| `memMu` | `sync.RWMutex` | Guards the active memtable (SkipList). Held exclusively for SkipList inserts; held shared for SkipList reads. Always acquired inside `mu`. |
| `bgErr` | `error` | Last error from a background flush goroutine. Checked at the start of every write; surfaces the error to the caller. |
| `flushWg` | `sync.WaitGroup` | Tracks the single in-flight background flush goroutine. `flushWg.Wait()` in `Close` ensures all data is durable before shutdown. |

---

## `NewStore`

```go
func NewStore(walPath string, dir string) (*Store, error)
```

Full startup sequence:

```
newBlockCache(DefaultBlockCacheCapacity)   // create 8 MB LRU block cache

openManifest(dir)
    └─ replayManifest → live []sstMeta (path + level), oldest → newest
    └─ open MANIFEST for appending

Load SSTable readers into levels (reverse manifest order → newest first per level)
    for i := len(liveMetas)-1; i >= 0; i-- {
        m = liveMetas[i]
        r = sst.NewReader(m.Path, cache)
        levels[m.Level] = append(levels[m.Level], r)
    }
    sort levels[1] and levels[2] by r.MinKey()  // restore sorted order for binary search

Crash recovery: check for walPath+".immutable"
    if file exists:
        replay all entries into fresh SkipList
        os.Remove(immWALPath)

Replay active WAL into the same SkipList

Open WAL writer (truncates nothing; O_APPEND)

Return &Store{..., cache: cache}
```

**Crash-recovery detail.** If the process died during `flushBackground`, the
immutable memtable's data is still in `walPath+".immutable"`. `NewStore` detects
this file and replays it before the active WAL, so no writes are lost. After
replay, the file is removed; the SSTable that was being written at crash time is
either complete (and already in the manifest) or incomplete (and not in the
manifest, so not loaded).

---

## `Put`

```go
func (s *Store) Put(key []byte, value []byte) error
```

`Put` uses a **two-lock protocol** that allows concurrent WAL writes while still
serialising SkipList inserts:

```
mu.RLock()                          // shared: allows concurrent Put/Delete/Get
  check bgErr
  wal.Append(key, value, tombstone=false)   // WAL batches concurrent appends
  memMu.Lock()                      // exclusive: SkipList is not thread-safe
    memtable.Put(key, value, tombstone=false)
    size = memtable.SizeInBytes()
  memMu.Unlock()
mu.RUnlock()

if size > sizeThreshold && immutable == nil:
    mu.Lock()                       // re-acquire exclusive for freeze
      if memtable.SizeInBytes() > sizeThreshold && immutable == nil:
          freeze()                  // double-check: another goroutine may have frozen
    mu.Unlock()
```

**Lock ordering:** `mu` is always acquired before `memMu`. `memMu` is never held
when acquiring `mu`.

The `mu.RLock()` during the write phase prevents the flush goroutine from
swapping the memtable epoch mid-write (freeze requires `mu.Lock()`). This means
multiple goroutines can append to the WAL concurrently — the WAL writer uses
write-stealing to batch them — while `memMu.Lock()` serialises the subsequent
SkipList insertions.

Returns the first error encountered. A WAL error prevents the SkipList insert, so
partial writes are not possible.

---

## `Get`

```go
func (s *Store) Get(key []byte) ([]byte, error)
```

```
mu.RLock()
  memMu.RLock()
    memtable.Lookup(key)            // active SkipList needs memMu
        found + tombstone  → return KeyNotFoundError
        found + live       → return value
  memMu.RUnlock()
  immutable.Lookup(key)             // frozen; only mu.RLock needed
      found + tombstone  → return KeyNotFoundError
      found + live       → return value
  for each reader in sstables (newest first):
      reader.Get(key)
          ok             → return value
          ErrTombstone   → return KeyNotFoundError
  return KeyNotFoundError
mu.RUnlock()
```

---

## `Delete`

Identical to `Put` with `isTombstone=true`. The same two-lock protocol applies:
`mu.RLock()` is held while the WAL record is written; `memMu.Lock()` is then
taken exclusively for the SkipList insert; a conditional `mu.Lock()` follows if
a freeze is needed. The WAL record and memtable entry both carry the tombstone
flag. Subsequent `Get` calls will find the tombstone before any older value and
return `KeyNotFoundError`.

---

## `Scan`

```go
func (s *Store) Scan(startKey []byte, endKey []byte) (mt.MemTableIteratorI, error)
```

```
mu.RLock()
  memMu.RLock()
    iter = memtable.Iterator()      // active SkipList needs memMu
  memMu.RUnlock()
  if immutable != nil:
      iters = append(iters, immutable.Iterator())   // frozen; only mu.RLock needed
  for each r in levels[0]:         // L0, newest first
      iters = append(iters, r.Iterator())
  for each r in levels[1]:         // L1, sorted by MinKey
      iters = append(iters, r.Iterator())
  for each r in levels[2]:         // L2, sorted by MinKey
      iters = append(iters, r.Iterator())
  return newMergeIterator(iters, startKey, endKey)
mu.RUnlock()
```

Source ordering (`iters[0]` = newest) mirrors `Get`'s lookup order, so the merge
iterator automatically applies newest-wins semantics across all levels.

---

## `Close`

```go
func (s *Store) Close() error
```

```
flushWg.Wait()          // drain in-flight flush

Lock()
  if memtable.SizeInBytes() > 0:
      flushSync()       // write final SSTable synchronously

  wal.Close()
  for l in 0..numLevels-1:
      for each reader in levels[l]: reader.Close()
  manifest.close()
Unlock()
```

---

## `freeze`

```go
func (s *Store) freeze()
```

Called under the write lock. Atomically rotates the active memtable and WAL,
then starts a background flush.

```
Step 1  wal.Close()
Step 2  os.Rename(walPath, walPath+".immutable")
            ↑ crash-safe: data is in immutable WAL until SSTable is durable
Step 3  wal = w.NewWriter(walPath)   // fresh active WAL
Step 4  immutable = memtable         // hand off to background goroutine
Step 5  memtable = mt.NewSkipList()  // new empty memtable for live writes
Step 6  flushWg.Add(1)
        go flushBackground(immutable, immWALPath)
```

If `w.NewWriter` fails, `bgErr` is set and no goroutine is started. The immutable
WAL still holds the data and will be replayed on the next `NewStore`.

Only **one** immutable memtable exists at a time. If `Put` or `Delete` would
trigger another freeze while `immutable != nil`, the freeze is skipped. The
memtable continues growing until the background flush completes and clears
`immutable`.

---

## `flushBackground`

```go
func (s *Store) flushBackground(imm mt.MemTableI, immWALPath string)
```

Runs in a dedicated goroutine. All heavy I/O occurs **outside** the lock.

```
defer flushWg.Done()
defer imm.Release()                // return arena slab to pool after flush

path = dir + "/" + time.Now().UnixNano() + ".sst"
sstWriter = sst.NewWriter(path)

for entry in imm.Iterator():
    sstWriter.Append(entry.Key, entry.Value, entry.IsTombstone)

sstWriter.Close()
r = sst.NewReader(path, cache)     // verify the file is readable
manifest.recordAdd(path, 0)        // level 0 — durable before we expose the reader

Lock()
  levels[0] = [r] + levels[0]      // prepend: newest first in L0
  immutable = nil                   // release immutable slot
  triggerL0 = len(levels[0]) >= compactionThreshold
Unlock()

os.Remove(immWALPath)              // WAL data is now in the SSTable

if triggerL0:
    compactL0(s)                   // L0 → L1; may cascade into L1 → L2
```

**Error handling.** Any error sets `bgErr` (under the lock) and returns. The
immutable WAL is NOT deleted on error, so the data remains safe for replay on the
next `NewStore`.

---

## `compactL0`

```go
func compactL0(s *Store) error
```

Merges all L0 files with every overlapping L1 file, producing new non-overlapping
L1 files of at most `l1TargetFileSize` (2 MB). All compaction I/O is done
**outside** `s.mu`; the in-memory level swap is done **inside** a brief
`s.mu.Lock()`.

```
Lock()
  snapshot l0 = levels[0]
  snapshot l1 = levels[1]
Unlock()

keyRange = l0KeyRange(l0)          // [minKey, maxKey] across all L0 files
overlapping = findOverlapping(l1, keyRange)
inputs = l0 + overlapping
iters = [r.Iterator() for r in inputs]
merged = newMergeIteratorOpts(iters, nil, nil, includeTombstones=true)

newL1 = writeCompactionOutput(merged, dir, manifest, l1TargetFileSize,
                               preserveTombstones=true, level=1)

Lock()
  levels[0] = []                   // clear L0 (all files are now in newL1)
  keep = levels[1] \ overlapping   // non-overlapping L1 files that were untouched
  levels[1] = sort(keep + newL1, by MinKey)
Unlock()

for r in (l0 + overlapping): r.Close(); os.Remove(r.Path()); manifest.recordDel(r.Path())

if l1TotalSize(s) > l1SizeLimit:
    compactL1ToL2(s)
```

**Tombstone policy.** L0→L1 output is written with `preserveTombstones=true`:
tombstones must be kept because L2 may still hold an older version of a deleted
key.

---

## `compactL1ToL2`

```go
func compactL1ToL2(s *Store) error
```

Merges all current L1 files with every overlapping L2 file, producing new
non-overlapping L2 files of at most `l2TargetFileSize` (8 MB). Same lock
discipline as `compactL0` — I/O outside lock, swap inside.

```
Lock()
  snapshot l1 = levels[1]
  snapshot l2 = levels[2]
Unlock()

overlapping = findOverlapping(l2, fullRange(l1))
inputs = l1 + overlapping
iters = [r.Iterator() for r in inputs]
merged = newMergeIteratorOpts(iters, nil, nil, includeTombstones=true)

newL2 = writeCompactionOutput(merged, dir, manifest, l2TargetFileSize,
                               preserveTombstones=false, level=2)

Lock()
  levels[1] = []                   // L1 fully consumed
  keep = levels[2] \ overlapping
  levels[2] = sort(keep + newL2, by MinKey)
Unlock()

for r in (l1 + overlapping): r.Close(); os.Remove(r.Path()); manifest.recordDel(r.Path())
```

**Tombstone policy.** L1→L2 output is written with `preserveTombstones=false`:
L2 is the bottom level; no older data exists below it, so tombstones may be
safely dropped.

**Why merge all of L1 (not just one file at a time).** Because L0 files can
overlap any subset of L1, the previous `compactL0` pass may have merged L0 into
L1 files that span a wide key range. To keep L1 non-overlapping across the
whole key space, the safest strategy is to compact all of L1 together with all
overlapping L2 files in one pass. This is a "major compaction" approach — it
incurs higher write amplification than a single-file strategy but simplifies the
invariant maintenance.

---

## Why `includeTombstones=true` during merge

The merge iterator must _see_ tombstones to correctly deduplicate keys across
multiple SSTable files. Consider two files:

```
L0[0] (newer):  key="a" tombstone=true
L1[0] (older):  key="a" value="hello"
```

If tombstones were invisible to the iterator, `key="a"` would appear as `"hello"`
in the merged output — incorrectly resurrecting a deleted key. By passing
`includeTombstones=true`, the iterator emits the tombstone entry for `"a"`, which
shadows the older live value. The compaction writer then decides whether to keep
or drop the tombstone based on `preserveTombstones`.

---

## `flushSync`

```go
func (s *Store) flushSync() error
```

Synchronous variant of the flush, called by `Close` while holding the write lock.

```
path = dir + "/" + UnixNano + ".sst"
sstWriter = sst.NewWriter(path)

for entry in memtable.Iterator():
    sstWriter.Append(...)

sstWriter.Close()
r = sst.NewReader(path, cache)
old = memtable
levels[0] = [r] + levels[0]
memtable = mt.NewSkipList()
old.Release()                      // return arena slab to pool
manifest.recordAdd(path, 0)

wal.Close()
os.Truncate(walPath, 0)           // reset WAL in-place (vs rename in freeze)
wal = w.NewWriter(walPath)
```

Unlike `freeze`/`flushBackground`, `flushSync` does not rename the WAL — it
truncates it after the SSTable is safely recorded in the manifest. This is safe
because `Close` does not need crash recovery: if the process dies during
`flushSync`, the next `NewStore` will replay the WAL and re-flush.

---

## SSTable filename convention

Files are named `{time.Now().UnixNano()}.sst`.

- **Nanosecond timestamps** make collisions extremely unlikely in practice: the
  store's single flush goroutine and the single-threaded `flushSync` path cannot
  race with each other because flushes are serialized by `immutable == nil` check
  and by `flushWg`.
- Files sort roughly by creation time, which aids manual inspection.
- UUIDs or sequence numbers were not used; nanoseconds are simpler and sufficient.

---

## Trade-offs and limitations

| Aspect | Current behaviour | Alternative |
|--------|-------------------|-------------|
| Immutable slots | Only one at a time; a second freeze is blocked until the first flush completes | Allow a queue of immutables (more memory, faster write path) |
| Compaction strategy | 3-level LSM (L0→L1→L2); non-overlapping L1/L2; binary search on reads | More levels (e.g., 7) for lower write amplification |
| L1 compaction scope | Full L1 compacted in one pass | Compact one L1 file at a time (lower amplification, harder to maintain invariants) |
| Filename uniqueness | Nanosecond timestamp | UUID or monotonic sequence number |
| Compaction scheduling | Triggered synchronously at end of flush goroutine | Background compaction thread |
| bgErr recovery | Permanent; store is dead after a background error | Could retry or fall back to sync flush |
| Concurrent safety | Dual-lock: `mu.RLock` for WAL + epoch protection; `memMu.Lock` for SkipList insert. Concurrent `Put`/`Delete` calls share WAL writes and only serialise at the SkipList. `mu.Lock` is taken exclusively only for freeze/compaction. Lock ordering: always `mu` before `memMu`. | Single global lock (simpler, less throughput under concurrent writers) |
| Arena allocation | `NewSkipList()` draws a 65 536-node slab from `arenaPool` (cap 4); `Release()` zeroes and returns it. `flushBackground` calls `defer imm.Release()`; `flushSync` calls `old.Release()` before replacement. | Per-node heap allocation (simpler, higher GC pressure) |
