# `sstable.go` — Constants, BlockHandle, Footer

## Constants

### `BlockSize = 4096`

```go
const BlockSize = 4096 // 4 KB
```

Data blocks are flushed to disk when the write buffer reaches this threshold. 4 KB is chosen because it matches the typical OS virtual-memory **page size**: a single page-aligned read retrieves exactly one block with no wasted bytes and no need for a second read to complete the record. Aligning block boundaries to 4 KB also prevents **partial-page I/O** — the OS will not need to read-modify-write a page when writing a complete block.

### `FooterSize = 33`

```go
const FooterSize = 33 // 32 bytes (4 uint64 handles) + 1 byte format version
```

The footer holds exactly two `BlockHandle` values plus one format-version byte. Each `BlockHandle`
contains two `uint64` fields (Offset and Length):

```
FooterSize = 2 BlockHandles × 2 uint64 × 8 bytes/uint64 + 1 version byte = 33 bytes
```

The fixed, known size means a reader can locate the footer with a single `ReadAt(file_size − 33, 33)`
call without scanning or parsing any other part of the file first.

### `FormatVersion = 0x02`

```go
const FormatVersion = 0x02 // v2: xxHash64 bloom filter (v1 used FNV64)
```

The single version byte at offset 32 of the footer identifies the on-disk format.
`NewReader` rejects any file whose version byte does not match `FormatVersion`, preventing
silent wrong results when the bloom filter's hash function changes.

| Version | Bloom hash | Notes |
|---|---|---|
| `0x01` | FNV-1a + FNV-1 (two allocating calls) | Original format |
| `0x02` | xxHash64 + rotate-multiply (zero allocs) | Current format |

> **Migration**: existing `v1` SSTable files are incompatible. Delete the data directory and
> restart the process to let tinyKV rebuild them in `v2` format.

---

## `BlockHandle`

```go
type BlockHandle struct {
    Offset uint64
    Length uint64
}
```

A `BlockHandle` is a **pointer into the SSTable file**. It carries everything needed to load any block with a single `ReadAt` call:

| Field | Type | Description |
|---|---|---|
| `Offset` | `uint64` | Byte offset from the start of the file where the block begins. |
| `Length` | `uint64` | Number of bytes in the block. |

**Usage pattern**:

```go
data := make([]byte, handle.Length)
file.ReadAt(data, int64(handle.Offset))
```

`BlockHandle` is used for three sections:

- **Data blocks** — recorded in the index block at flush time.
- **Index block** — stored in the footer as `Footer.IndexHandle`.
- **Bloom block** — stored in the footer as `Footer.BloomHandle`.

---

## `Footer`

```go
type Footer struct {
    IndexHandle BlockHandle
    BloomHandle BlockHandle
}
```

The `Footer` occupies the **last 32 bytes** of every SSTable file. It is the single fixed-position anchor that bootstraps all other reads: once the footer is parsed, the reader knows exactly where the index and bloom filter live on disk.

### Byte layout

```
Byte offset  Field                     Width
───────────────────────────────────────────────
 0 –  7      IndexHandle.Offset        8 bytes, little-endian uint64
 8 – 15      IndexHandle.Length        8 bytes, little-endian uint64
16 – 23      BloomHandle.Offset        8 bytes, little-endian uint64
24 – 31      BloomHandle.Length        8 bytes, little-endian uint64
32           FormatVersion             1 byte  (currently 0x02)
───────────────────────────────────────────────
Total                                 33 bytes
```

All integer values are encoded in **little-endian** byte order, consistent with the rest of the SSTable format.

### Reading the footer

```go
info, _ := file.Stat()
var buf [FooterSize]byte
file.ReadAt(buf[:], info.Size()-FooterSize)

footer := Footer{
    IndexHandle: BlockHandle{
        Offset: binary.LittleEndian.Uint64(buf[0:]),
        Length: binary.LittleEndian.Uint64(buf[8:]),
    },
    BloomHandle: BlockHandle{
        Offset: binary.LittleEndian.Uint64(buf[16:]),
        Length: binary.LittleEndian.Uint64(buf[24:]),
    },
}
version := buf[32] // FormatVersion byte
```

---

## Full File Layout

```
+─────────────────────────────────────────+
│  Data Block 0                           │  ← BlockHandle{Offset: 0,          Length: ?}
│  [record][record][record]...            │
+─────────────────────────────────────────+
│  Data Block 1                           │  ← BlockHandle{Offset: ?,          Length: ?}
│  [record][record][record]...            │
+─────────────────────────────────────────+
│               ...                       │
+─────────────────────────────────────────+
│  Data Block N−1                         │  ← BlockHandle{Offset: ?,          Length: ?}
+─────────────────────────────────────────+
│  Index Block                            │  ← Footer.IndexHandle
│  [entry][entry]...[entry]               │    one entry per data block
+─────────────────────────────────────────+
│  Bloom Block                            │  ← Footer.BloomHandle
│  k (4 B LE) | bits (variable)          │
+─────────────────────────────────────────+
│  Footer (33 bytes)                      │  ← always at file_size − 33
│  IndexHandle.Offset  (8 B LE)           │
│  IndexHandle.Length  (8 B LE)           │
│  BloomHandle.Offset  (8 B LE)           │
│  BloomHandle.Length  (8 B LE)           │
│  FormatVersion       (1 B)              │
+─────────────────────────────────────────+
```

Data blocks are written first, in order. The index and bloom blocks follow immediately after the last data block. The footer is the final 32 bytes of the file.
