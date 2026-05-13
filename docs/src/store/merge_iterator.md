# MergeIterator

`mergeIterator` implements a **k-way sorted merge** over an arbitrary number of
`MemTableIteratorI` sources. It is used in two places:

| Caller | `startKey` | `endKey` | `includeTombstones` |
|--------|-----------|---------|---------------------|
| `Store.Scan` | user-supplied | user-supplied | `false` |
| `compactL0` | `nil` | `nil` | `true` |
| `compactL1ToL2` | `nil` | `nil` | `true` |

The iterator enforces **newest-wins** semantics: when the same key appears in
multiple sources, the entry from the source with the lowest index (most recently
flushed) is the authoritative version.

---

## Data structures

### `mergeEntry`

```go
type mergeEntry struct {
    key       []byte
    value     []byte
    tombstone bool
    sourceIdx int
    iter      mt.MemTableIteratorI
}
```

| Field | Description |
|-------|-------------|
| `key`, `value`, `tombstone` | Snapshot of the current entry from this source |
| `sourceIdx` | Index of the originating iterator in the slice passed to `newMergeIteratorOpts`. Lower index = newer source. |
| `iter` | Back-reference to the iterator so it can be advanced when this entry is consumed |

### `entryHeap`

```go
type entryHeap []mergeEntry
```

A min-heap implementing `heap.Interface` from `container/heap`.

**Ordering (`Less`):**

```go
func (h entryHeap) Less(i, j int) bool {
    cmp := bytes.Compare(h[i].key, h[j].key)
    if cmp != 0 { return cmp < 0 }
    return h[i].sourceIdx < h[j].sourceIdx
}
```

- **Primary key**: lexicographic order of `key`. The smallest key sits at the top
  of the heap, ensuring the iterator yields keys in sorted order.
- **Secondary key**: `sourceIdx` (ascending). When two sources contain the same
  key, the entry from the **lower** (newer) source reaches the top first. This
  drives the "drain stale duplicates" step in `advance`.

### `mergeIterator`

```go
type mergeIterator struct {
    h                 entryHeap
    endKey            []byte
    curr              *mergeEntry
    includeTombstones bool
}
```

| Field | Description |
|-------|-------------|
| `h` | The min-heap holding one "frontier" entry per active source |
| `endKey` | Exclusive upper bound. `nil` means unbounded. |
| `curr` | Pointer to the current entry. `nil` means the iterator is exhausted. |
| `includeTombstones` | When `false`, tombstone entries are silently skipped. When `true`, they are emitted (used by compaction). |

---

## Construction

```go
func newMergeIterator(iters []mt.MemTableIteratorI, startKey, endKey []byte) *mergeIterator
func newMergeIteratorOpts(iters []mt.MemTableIteratorI, startKey, endKey []byte,
                          includeTombstones bool) *mergeIterator
```

`newMergeIterator` is a convenience wrapper that calls `newMergeIteratorOpts`
with `includeTombstones=false`.

**Initialization steps:**

```
h = empty heap

for idx, it in enumerate(iters):
    if startKey != nil:
        it.Seek(startKey)       // position each source at or after startKey
    if it.Valid():
        push mergeEntry{key=it.Key(), value=it.Value(), tombstone=it.IsTombstone(),
                        sourceIdx=idx, iter=it}

heap.Init(&h)                   // O(k) heapify

mi = &mergeIterator{h, endKey, curr=nil, includeTombstones}
mi.advance()                    // position at first real entry
return mi
```

After `newMergeIteratorOpts` returns, `mi.curr` points to the first valid entry
(or is `nil` if no valid entry exists).

---

## `advance` — the core algorithm

```go
func (mi *mergeIterator) advance()
```

Every call to `Next()` delegates to `advance`. It is also called once during
construction to prime `curr`.

```
loop:
    if heap is empty:
        curr = nil; return        // exhausted

    top = heap.Pop()              // minimum key, newest source on tie

    if endKey != nil && top.key >= endKey:
        curr = nil; return        // past the requested range

    // --- Drain stale duplicates ---
    while heap is non-empty && heap.top.key == top.key:
        stale = heap.Pop()
        stale.iter.Next()
        if stale.iter.Valid():
            heap.Push(next entry from stale.iter)

    // --- Advance the winning iterator ---
    top.iter.Next()
    if top.iter.Valid():
        heap.Push(next entry from top.iter)

    // --- Tombstone filter ---
    if top.tombstone && !includeTombstones:
        continue                  // skip; loop again to find next entry

    curr = &top; return
```

### Step-by-step correctness argument

**Step: drain stale duplicates.**

Because the heap orders entries by `(key ASC, sourceIdx ASC)`, when `top` is
popped (the globally minimum entry), any remaining heap entry with the same key
must have a *higher* `sourceIdx` — meaning it comes from an *older* source. Those
entries are stale: the winning entry (`top`) already represents the authoritative
value for this key. We advance past them without emitting them.

```
Example heap state before pop (newest source = sourceIdx 0):

  key="b", sourceIdx=0  ← top (winner)
  key="b", sourceIdx=1  ← stale: same key, older source
  key="c", sourceIdx=0
```

After draining, `key="b"` from `sourceIdx=1` is advanced and its *next* key is
pushed back, which may be `"c"` or later.

**Step: advance winning iterator AFTER draining.**

This ordering is critical. If the winning iterator were advanced first, its next
entry might have the same key as the stale duplicates:

```
top.iter.Next()  →  next entry is also key="b"  (same key, different version)
```

Pushing that entry immediately would confuse the drain loop into thinking the new
entry is a duplicate too. By advancing *after* draining, we guarantee that all
`key="b"` entries from older sources are disposed of before the winning source
moves on.

**Step: tombstone filter.**

When `includeTombstones=false`, a tombstone entry causes the loop to `continue`
without setting `curr`. The iterator simply looks for the next valid (non-tombstone)
entry. When `includeTombstones=true` (compaction), tombstones are emitted
normally — the compaction writer is responsible for deciding what to do with them.

---

## Sequence diagram for a 3-source merge

```
Sources (newest → oldest):
  src[0]:  a=1   c=3
  src[1]:  a=2   b=4
  src[2]:  b=5   d=6

Initial heap after heapify:
  (a, src[0]),  (a, src[1]),  (b, src[2])

advance() call 1:
  pop  (a, src[0])               ← winner
  drain  (a, src[1]) → push (b, src[1])
  advance src[0] → push (c, src[0])
  heap: (b, src[1]),  (b, src[2]),  (c, src[0])
  curr = a, value=1

advance() call 2:
  pop  (b, src[1])               ← winner
  drain  (b, src[2]) → push (d, src[2])
  advance src[1] → src[1] exhausted
  heap: (c, src[0]),  (d, src[2])
  curr = b, value=4

advance() call 3:
  pop  (c, src[0])               ← winner, no duplicates
  advance src[0] → src[0] exhausted
  heap: (d, src[2])
  curr = c, value=3

advance() call 4:
  pop  (d, src[2])               ← winner, no duplicates
  advance src[2] → src[2] exhausted
  heap: empty
  curr = d, value=6

advance() call 5:
  heap is empty → curr = nil
```

Result sequence: `a=1, b=4, c=3, d=6` — correctly sorted with newest-wins.

---

## Interface methods

| Method | Behaviour |
|--------|-----------|
| `Valid() bool` | Returns `curr != nil` |
| `Key() []byte` | Returns `curr.key` |
| `Value() []byte` | Returns `curr.value` |
| `IsTombstone() bool` | Returns `curr.tombstone` |
| `Next()` | Calls `advance()` to move to the next entry |
| `Seek(_ []byte)` | **No-op.** The iterator is positioned at construction time via `startKey`. Repositioning an existing `mergeIterator` is not supported. |
| `Close() error` | **No-op.** The underlying source iterators are not closed here. The caller (e.g. `Store.Scan`) owns them. |

---

## `includeTombstones` in depth

```
includeTombstones = false  (Scan)
    Tombstoned keys are invisible to the caller.
    The drain step still removes older versions of a tombstoned key,
    so no stale live value leaks through.

includeTombstones = true   (compact)
    Tombstones ARE emitted by the iterator.
    compact() receives them and explicitly skips writing them to the output SSTable:

        for ; merged.Valid(); merged.Next() {
            if merged.IsTombstone() { continue }   // drop on full compaction
            sstWriter.Append(...)
        }

    The indirection (emit tombstone, then skip at writer) is necessary because
    the merge iterator needs to see the tombstone to suppress older live values
    for the same key. If tombstones were invisible, a deleted key could
    re-appear in the compacted output.
```

---

## Complexity

| Metric | Value |
|--------|-------|
| Time to iterate all `n` entries across `k` sources | O(n log k) |
| Heap space | O(k) — one frontier entry per source |
| Entry allocation | One `mergeEntry` per `heap.Push` call; not pooled |

---

## Trade-offs

| Aspect | Current behaviour | Alternative |
|--------|-------------------|-------------|
| Entry allocation | New `mergeEntry` allocated on every `heap.Push` | Object pool to reduce GC pressure |
| `Seek` | No-op; iterator cannot be repositioned | Full re-initialization from a new `startKey` |
| `endKey` check | Evaluated inside `advance`, not in `Valid` | Could check in `Valid` for a cleaner API, but the current approach is equally correct |
| Source iterator lifetime | Caller owns source iterators; `Close` is a no-op | `mergeIterator.Close` could close all source iterators, simplifying caller code |
