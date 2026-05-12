# Package `sstable`

## What is an SSTable?

A **Sorted String Table (SSTable)** is an **immutable, sorted, on-disk file** that maps byte-slice keys to byte-slice values. Once written, an SSTable is never modified — it is only created (during a memtable flush or compaction) and eventually deleted (when superseded by a newer compaction output).

Keys inside an SSTable are stored in lexicographic order. Every record is either a **live entry** (key → value) or a **tombstone** (key → deleted marker). Tombstones propagate deletions across LSM levels until compaction removes them.

---

## Role in the LSM-Tree

In the LSM-tree architecture, SSTables form the **persistent, sorted runs** at each level:

1. When the in-memory memtable fills up, its contents are flushed to a new SSTable on disk (Level 0).
2. Periodic **compaction** merges multiple SSTables — possibly across levels — into a new, de-duplicated SSTable, then discards the inputs.
3. **Reads** walk the level hierarchy: memtable → L0 SSTables → L1 … Each SSTable is consulted independently and bloom filters allow most lookups to skip the file entirely.

Because SSTables are immutable, concurrent reads require no locking and crashed writes can never corrupt existing files.

---

## File Format — High-Level

An SSTable file is structured as a linear sequence of sections written front-to-back:

```
+------------------+
|  Data Block 0    |
+------------------+
|  Data Block 1    |
+------------------+
|      ...         |
+------------------+
|  Index Block     |
+------------------+
|  Bloom Block     |
+------------------+
|  Footer (32 B)   |  <- always at file_size − 32
+------------------+
```

| Section | Description |
|---|---|
| **Data blocks** | Sequences of key/value records, flushed when the write buffer reaches `BlockSize` (4 KB). |
| **Index block** | One entry per data block: the block's last key and its `BlockHandle` (offset + length). |
| **Bloom block** | A serialized `BloomFilter` built from every key written to the file. |
| **Footer** | Two `BlockHandle` values (index + bloom), always 32 bytes, at the very end of the file. |

### Why the footer is at the end

The writer appends sections sequentially without seeking backward. Data block offsets are only known after writing each block, and the index block offset is only known after all data blocks are written. Placing the footer last allows the writer to record the final positions of the index and bloom blocks after they have been written, without any random-access seeks.

---

## Read Access Pattern

Opening and querying an SSTable follows a strict three-tier access pattern that minimises unnecessary I/O:

```
Open file
  └─> ReadAt(file_size − 32) ──> parse Footer
        ├─> ReadAt(bloom.Offset, bloom.Length) ──> decode BloomFilter  (held in memory)
        └─> ReadAt(index.Offset, index.Length) ──> parse index entries (held in memory)

Get(key):
  1. bloom.MayContain(key)?  ──No──> KeyNotFoundError  (zero disk I/O)
                              └─Yes─>
  2. binary-search index for first entry with lastKey >= key
                              └─found─>
  3. ReadAt(block.Offset, block.Length) ──> linear scan for key
```

The bloom filter eliminates the vast majority of unnecessary block reads for keys that do not exist in the file. The in-memory index means that a lookup requires at most **one** data-block read after the bloom check passes.

---

## Key Design Constraints

- **Immutable**: an SSTable is never modified after `Writer.Close()` returns. Compaction creates new files; it does not patch existing ones.
- **Block-based I/O**: all reads are aligned `ReadAt` calls for a whole block. There is no streaming parse of the entire file.
- **Sorted keys**: the writer requires keys to be appended in ascending lexicographic order. The reader and iterator rely on this invariant for binary search and early-exit scans.
- **Tombstones are visible**: the iterator exposes tombstone records. It is the responsibility of higher layers (compaction, merge iterators) to suppress or drop them.

---

## Sub-documents

| Document | Contents |
|---|---|
| [`sstable.md`](./sstable.md) | Constants (`BlockSize`, `FooterSize`), `BlockHandle`, `Footer`, file-layout diagram |
| [`bloom.md`](./bloom.md) | `BloomFilter` — construction, hashing, bit manipulation, serialization |
| [`writer.md`](./writer.md) | `Writer` — append, flush, index, bloom, footer, record format |
| [`reader.md`](./reader.md) | `Reader`, `sstableIterator` — open, Get, Iterator, Seek |
