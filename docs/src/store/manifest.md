# Manifest

The manifest is tinyKV's **durable catalog** of live SSTable files. Because
SSTable files are created during flushes and deleted during compactions, and
because either event can be interrupted by a crash, the store needs a persistent
record of exactly which files constitute valid state. The manifest provides that.

---

## Data structures

### `manifestRecord`

```go
type manifestRecord struct {
    Op    string `json:"op"`              // "add" or "del"
    Path  string `json:"path"`
    Level int    `json:"level,omitempty"` // SSTable level (0=L0, 1=L1, 2=L2); absent in old files → treated as 0
}
```

One record per line in the MANIFEST file. `Op` is either `"add"` (a new SSTable
was successfully flushed) or `"del"` (an SSTable was superseded by compaction).
`Path` is the absolute filesystem path of the SSTable file. `Level` records which
compaction level the file belongs to; the field is omitted for level-0 files
written by older versions of the store (backward-compatible default: 0).

### `sstMeta`

```go
type sstMeta struct {
    Path  string
    Level int
}
```

Returned by `replayManifest` as the richer per-file descriptor that carries both
the path and the compaction level. `NewStore` uses the level to place each
`sst.Reader` into the correct slot of `levels[numLevels]`.

### `manifest`

```go
type manifest struct {
    file *os.File
}
```

A thin wrapper around a single file handle opened with
`O_APPEND | O_CREATE | O_WRONLY`. All writes are append-only; the file is never
truncated or rewritten.

---

## MANIFEST file format

One JSON object per line (newline-delimited JSON):

```
{"op":"add","path":"/data/store/1700000000000000000.sst"}
{"op":"add","path":"/data/store/1700000001000000000.sst"}
{"op":"del","path":"/data/store/1700000000000000000.sst"}
{"op":"add","path":"/data/store/1700000002000000000.sst","level":1}
{"op":"del","path":"/data/store/1700000001000000000.sst"}
```

After replaying this sequence:
- `1700000000000000000.sst` → **dead** (added as L0, then deleted)
- `1700000001000000000.sst` → **dead** (added as L0, then compacted away)
- `1700000002000000000.sst` → **live**, L1

Live files are returned in the order they first appeared (`oldest → newest`)
together with their level. Records without a `"level"` field default to level 0
for backward compatibility with manifests written before leveled compaction was
introduced.

---

## `openManifest`

```go
func openManifest(dir string) (*manifest, []sstMeta, error)
```

```
path = filepath.Join(dir, "MANIFEST")

live = replayManifest(path)   // read-only scan of existing records

f = os.OpenFile(path, O_APPEND|O_CREATE|O_WRONLY, 0644)

return &manifest{file: f}, live, nil
```

Returns both the `manifest` struct (for appending future records) and the slice
of live `sstMeta` values (path + level) for loading readers on startup.

---

## `replayManifest`

```go
func replayManifest(path string) ([]sstMeta, error)
```

Single forward scan over the MANIFEST file. Builds two data structures:

| Variable | Type | Purpose |
|----------|------|---------|
| `ordered` | `[]sstMeta` | All files seen, in first-appearance order, with their level |
| `alive` | `map[string]bool` | Current liveness of each path |

**Scan rules:**

```
for each line:
    parse JSON → manifestRecord
    if parse fails: skip line   // safe: handles truncated crash tail
    switch rec.Op:
        "add":
            if !alive[rec.Path]:
                ordered = append(ordered, sstMeta{rec.Path, rec.Level})
                alive[rec.Path] = true
        "del":
            alive[rec.Path] = false

live = [m for m in ordered if alive[m.Path]]
return live, scanner.Err()
```

**Backward compatibility.** Records without a `"level"` field unmarshal with
`Level=0`, placing old files into L0. This is the safest default because L0
makes no ordering guarantees; old files will be compacted into L1 on the next
flush cycle.

**Idempotency.** The `!alive[rec.Path]` guard means a path that appears twice in
`"add"` records is only inserted into `ordered` once. This is important for the
case where `recordAdd` is called but the process crashes before the corresponding
SSTable flush is marked complete — on the next startup, the path may be re-added
without creating a duplicate entry.

**Result ordering.** `live` preserves the original insertion order of the
MANIFEST, meaning the returned slice is **oldest → newest**. `NewStore` iterates
this slice in reverse when loading readers so that `levels[l][0]` is the most
recently flushed file within each level.

---

## `recordAdd` / `recordDel`

```go
func (m *manifest) recordAdd(path string, level int) error
func (m *manifest) recordDel(path string) error
```

`recordAdd` now accepts a `level` parameter that is embedded in the JSON record.
`recordDel` records a deletion at whatever level the file was originally at (the
level is not needed for deletion — `Path` is the unique key).

Both delegate to `append`:

```go
func (m *manifest) append(rec manifestRecord) error {
    data, _ = json.Marshal(rec)
    data = append(data, '\n')
    m.file.Write(data)
    m.file.Sync()       // fsync: record is durable before caller proceeds
}
```

`file.Sync()` (an `fsync` syscall) is called after every record. This is the
cornerstone of crash safety: callers can rely on the fact that once `recordAdd`
returns without error, the manifest record is durable on the storage medium, even
if the OS cache is not yet flushed.

---

## `close`

```go
func (m *manifest) close() error { return m.file.Close() }
```

Flushes OS-level buffers and releases the file descriptor. Called by
`Store.Close()`.

---

## Crash-safety analysis

The following table covers every crash point and its outcome.

### During flush (SSTable creation)

| Crash point | Manifest state | Outcome |
|-------------|----------------|---------|
| After `sst.NewWriter`, before `sstWriter.Close()` | No `"add"` record yet | Incomplete SSTable file exists on disk but is not in the manifest. On restart, `replayManifest` does not return its path; it is never opened. File is leaked (see note below). |
| After `sstWriter.Close()`, before `recordAdd` | No `"add"` record yet | Same as above — file is complete but invisible to the manifest. Leaked. |
| After `recordAdd`, before `os.Remove(immWALPath)` | `"add"` record is durable | SSTable is loaded on restart. Immutable WAL is also replayed (harmless duplicate data). |
| After `os.Remove(immWALPath)`, before lock update | `"add"` record is durable | SSTable is loaded on restart. `s.immutable` is still non-nil but will be rebuilt from the WAL replay at startup — which is now empty (file was deleted). |

### During compaction (SSTable merge)

| Crash point | Manifest state | Outcome |
|-------------|----------------|---------|
| After writing new SSTable, before `recordAdd(outPath)` | Nothing changed | New file is leaked; all old SSTables still live. Fully correct on restart. |
| After `recordAdd(outPath)`, before `recordDel` of old files | New + old `"add"` records present | Both old and new SSTables are loaded on restart. There is data duplication (same keys in multiple files) but correctness is preserved because the merge iterator applies newest-wins semantics. |
| After all `recordDel` records, before `os.Remove` of old files | Old files marked dead | Old SSTables are not loaded on restart. Old files are leaked on disk but do not affect correctness. |
| After `os.Remove` of old files | Fully consistent | Normal state. |

### Potential improvements (not implemented)

- **Orphaned file GC.** On startup, scan `dir` for `.sst` files not referenced by
  the manifest and delete them. This would reclaim leaked files from all the
  "leaked" cases above.
- **`"add"` validation.** Before accepting a manifest `"add"` record, verify that
  the referenced file exists and is a valid SSTable. This catches the case where
  `recordAdd` was written but the flush was not yet complete.

---

## Trade-offs

| Aspect | Current behaviour | Alternative |
|--------|-------------------|-------------|
| Sync frequency | `fsync` after every record | Batch multiple records then sync (higher throughput, slightly less durability) |
| Encoding | Newline-delimited JSON | Binary encoding (smaller, faster to parse) |
| Manifest compaction | Never compacted; grows with every flush and compaction cycle | Periodically rewrite the manifest to contain only live `"add"` records |
| Error handling on bad lines | `continue` (skip and keep scanning) | Abort replay; require operator intervention |
