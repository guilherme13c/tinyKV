# `memtable` Package

The `memtable` package implements the **in-memory write buffer** of the tinyKV LSM-tree storage engine.

---

## Role in the LSM-tree

In a Log-Structured Merge-tree, all writes are directed to an in-memory buffer — the *memtable* — before being flushed to disk as an SSTable. This design gives tinyKV three properties:

1. **Low write latency**: random writes become sequential I/O at flush time.
2. **Fast recent reads**: the most recent version of any key is always checked in the memtable first.
3. **Correct delete semantics**: deletes cannot erase data already flushed to SSTables, so they are recorded as *tombstones* (see [Tombstones](#tombstones-and-lsm-delete-semantics) below).

```
┌──────────────────────────────────────────────────────────────────┐
│                         tinyKV Store                             │
│                                                                  │
│  Write ──► WAL ──► MemTable ──(flush)──► SSTable-N              │
│                                                                  │
│  Read  ──► MemTable ──► SSTable-N ──► SSTable-N-1 ──► …        │
└──────────────────────────────────────────────────────────────────┘
```

The memtable holds all recent writes in memory, sorted by key. Once its byte size crosses a threshold (reported by `SizeInBytes()`), the store freezes the current memtable, flushes it to a new SSTable on disk, and starts a fresh memtable.

---

## Two-Interface Design

The package exposes two interfaces:

| Interface | Purpose |
|---|---|
| `MemTableI` | Read/write access — Put, Get, Lookup, SizeInBytes, Iterator |
| `MemTableIteratorI` | Forward sorted scan over all entries, including tombstones |

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

| Structure | Point Lookup | Sorted Scan | Notes |
|---|---|---|---|
| **Skip List** *(current)* | O(log n) avg | O(n) | Simple code; probabilistic balance; no worst-case guarantee |
| **Red-Black Tree** | O(log n) worst | O(n) | Deterministic worst-case; more complex rotation/rebalancing logic; no wasted pointer space |
| **B-Tree / B+ Tree** | O(log n) | O(n) | Better CPU cache locality due to node arrays; higher per-node overhead; suited to larger in-memory sets |
| **Hash Map** | O(1) avg | O(n log n) † | Fastest point lookups; †sorted scan requires a full sort at flush time; no ordering during the scan itself |

A hash map is the only poor fit for this role: the flush path needs to write keys to an SSTable in sorted order. An ordered structure (skip list, tree) provides that for free; a hash map requires a separate O(n log n) sort step at flush time.

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

- [`memtable.md`](memtable.md) — `MemTableI` and `MemTableIteratorI` interface reference
- [`skip_list.md`](skip_list.md) — Deep-dive into the `SkipList` implementation
