# `memtable` Package

The `memtable` package implements the **in-memory write buffer** of the tinyKV LSM-tree storage engine.

---

## Role in the LSM-tree

In a Log-Structured Merge-tree, all writes are directed to an in-memory buffer — the _memtable_ — before being flushed to disk as an SSTable. This design gives tinyKV three properties:

1. **Low write latency**: random writes become sequential I/O at flush time.
2. **Fast recent reads**: the most recent version of any key is always checked in the memtable first.
3. **Correct delete semantics**: deletes cannot erase data already flushed to SSTables, so they are recorded as _tombstones_ (see [Tombstones](#tombstones-and-lsm-delete-semantics) below).

```
┌────────────────────────────────────────────────────────────┐
│                         tinyKV Store                       │
│                                                            │
│  Write ──► WAL ──► MemTable ──(flush)──► SSTable-N         │
│                                                            │
│  Read  ──► MemTable ──► SSTable-N ──► SSTable-N-1 ──► …    │
└────────────────────────────────────────────────────────────┘
```

The memtable holds all recent writes in memory, sorted by key. Once its byte size crosses a threshold (reported by `SizeInBytes()`), the store freezes the current memtable, flushes it to a new SSTable on disk, and starts a fresh memtable.

---

## Two-Interface Design

The package exposes two interfaces:

| Interface           | Purpose                                                              |
| ------------------- | -------------------------------------------------------------------- |
| `MemTableI`         | Read/write access — Put, Get, Lookup, SizeInBytes, Iterator, Release |
| `MemTableIteratorI` | Forward sorted scan over all entries, including tombstones           |

Separating the iterator from the table provides two concrete benefits:

- **Decoupling**: flush and compaction code depends only on `MemTableIteratorI`; it does not need access to the full table.
- **Testability**: each interface can be mocked independently. A fake iterator is trivial to implement for compaction tests without needing a real skip list underneath.

---

## Current Implementation: SkipList

`SkipList` is the only current implementation of `MemTableI`. It provides expected O(log n) for all point operations (Get, Put, Lookup) and naturally produces entries in ascending byte-lexicographic key order when iterated at level 0 — a property required for correct SSTable flushing.

### Why a Skip List?

Skip lists are a pragmatic fit for memtables:

- Simple, compact code with probabilistic balancing (no tree rotations, no rebalancing).
- The level-0 list is a plain sorted singly-linked list; an O(n) sorted scan requires no extra bookkeeping.
- Performance is comparable to balanced BSTs in practice.
- Variable-height towers consume only the pointer slots actually needed per node.

### Alternative Data Structures

| Structure                 | Point Lookup   | Sorted Scan  | Notes                                                                                                      |
| ------------------------- | -------------- | ------------ | ---------------------------------------------------------------------------------------------------------- |
| **Skip List** _(current)_ | O(log n) avg   | O(n)         | Simple code; probabilistic balance; no worst-case guarantee                                                |
| **Red-Black Tree**        | O(log n) worst | O(n)         | Deterministic worst-case; more complex rotation/rebalancing logic; no wasted pointer space                 |
| **B-Tree / B+ Tree**      | O(log n)       | O(n)         | Better CPU cache locality due to node arrays; higher per-node overhead; suited to larger in-memory sets    |
| **Hash Map**              | O(1) avg       | O(n log n) † | Fastest point lookups; †sorted scan requires a full sort at flush time; no ordering during the scan itself |

A hash map is the only poor fit for this role: the flush path needs to write keys to an SSTable in sorted order. An ordered structure (skip list, tree) provides that for free; a hash map requires a separate O(n log n) sort step at flush time.

### Memory Management: Arena Pool

To eliminate per-node heap pressure on the garbage collector, `SkipList` allocates nodes from a **channel-based arena pool** rather than calling `new(skipListNode)` for each insertion.

#### Slab layout

```
arenaPool = make(chan []skipListNode, 4)   // pool of 4 pre-allocated slabs
```

Each slab is a `[]skipListNode` of length 65536 (≈ 7.3 MB). On creation, two slots are reserved:

| Index | Purpose                                              |
| ----- | ---------------------------------------------------- |
| 0     | Tail sentinel node                                   |
| 1     | Head sentinel node                                   |
| 2+    | Bump-allocated node storage (`arenaTop` starts at 2) |

`newNode()` advances `arenaTop` to hand out the next slot. If the arena is exhausted (rare, large memtable), it falls back to a plain heap allocation so correctness is never compromised.

#### Lifetime

- **Acquired** in `NewSkipList()`: a slab is pulled from `arenaPool` (or freshly allocated if the pool is empty).
- **Released** in `Release()`: all used slots are zeroed and the slab is returned to the pool via a non-blocking send.

`Release()` is part of the `MemTableI` interface so the store can reclaim the arena immediately after a flush, before starting the next memtable.

#### Per-SkipList PRNG

Each `SkipList` owns a private `*rand.Rand` (from `math/rand/v2`, backed by a PCG generator) that is
seeded from the global source at construction time:

```go
rng: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
```

`randomHeight()` calls `sl.rng.IntN(2)` instead of the package-level `rand.Intn(2)`. The
package-level source in `math/rand/v1` held a global mutex; every concurrent `Put` would serialise
on it during the height roll. With per-instance PRNGs, concurrent writes to different SkipLists
(e.g. active memtable vs. a compaction flush) never contend on the PRNG.

#### Why a channel pool instead of `sync.Pool`?

`sync.Pool` is cleared at every GC cycle. Because the store triggers a flush (and therefore arena reclamation) on a timer whose period aligns with GC pressure, using `sync.Pool` would cause slabs to be discarded precisely when they are most needed. A channel pool retains slabs **across GC pauses**, guaranteeing reuse regardless of collection timing.

#### Performance impact

The arena reduces allocation pressure to **1 alloc/op** for `PutSeq`, `PutRandom`, and `Delete` benchmarks. Without it, each node insertion incurred a separate heap allocation visible to the GC.

---

## Tombstones and LSM Delete Semantics

In an LSM-tree, `Delete(key)` cannot physically erase a key from SSTables that are already written to disk. Instead, a **tombstone** — a marker entry with `isTombstone = true` and `value = nil` — is written to the memtable exactly like any other write.

```
Time ──────────────────────────────────────────────────────────────►

 Put("alice", "v1")  → flushed to SSTable-1 (live entry)
 Put("alice", "v2")  → flushed to SSTable-2 (live entry)
 Put("alice", nil, isTombstone=true)  → written to current memtable

 Read("alice") ──► memtable has tombstone → return KeyNotFoundError
                   (SSTable-1 and SSTable-2 are never consulted)
```

When the memtable is flushed, tombstones are written into the resulting SSTable. During reads, a tombstone encountered in any layer stops the search — even if an older SSTable holds a live value for the same key, the tombstone wins. Tombstones are eventually reclaimed during compaction, once it is safe to discard all older live entries below them.

Storing tombstones in the memtable rather than performing physical deletes is fundamental to LSM-tree correctness. The `isTombstone` flag is the mechanism by which this is expressed at every layer of the stack.

---

## Sub-documents

- `memtable.md` — `MemTableI` and `MemTableIteratorI` interface reference *(not yet written)*
- `skip_list.md` — Deep-dive into the `SkipList` implementation *(not yet written)*
