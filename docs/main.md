# `main.go` — CLI Entry Point

`main.go` is the thin command-line shell that wires together the tinyKV store engine
and exposes it as an interactive REPL. It is responsible for three things:

1. **Parsing flags** and preparing the on-disk data directory.
2. **Opening the store** (which replays the WAL and loads SSTables).
3. **Running the REPL loop** that reads commands from stdin and dispatches them to the
   store.

The store itself — memtable, WAL, SSTable I/O, compaction — lives entirely in
`src/store`. `main.go` contains no storage logic.

---

## Flag: `-dir`

```
-dir <path>   directory for WAL, SSTables, and the MANIFEST  (default: "data")
```

| Aspect | Detail |
|---|---|
| Default value | `"data"` (relative to CWD) |
| Created automatically | Yes — `os.MkdirAll` is called before the store is opened |
| Passed to the store as | WAL path = `<dir>/wal`; SSTable dir = `<dir>` |

---

## On-disk File Layout

All persistent files are rooted at the directory chosen by `-dir`.

```
<dir>/
├── wal               ← active Write-Ahead Log
├── wal.immutable     ← WAL being flushed (present only during a background flush)
├── MANIFEST          ← ordered list of live SSTable paths
├── <nanoseconds>.sst ← SSTable files (one per memtable flush)
└── …
```

| File | Purpose |
|---|---|
| `wal` | Append-only log; every `put` and `delete` is durably written here before touching the memtable |
| `wal.immutable` | Temporary rename of the active WAL while the corresponding memtable is being flushed to disk; removed once the flush completes |
| `MANIFEST` | Text file listing the live `.sst` paths in oldest-first order; rebuilt on compaction |
| `<nanoseconds>.sst` | Immutable sorted SSTable produced by a memtable flush; named by `time.Now().UnixNano()` to ensure monotonic ordering |

---

## Startup Sequence

```
main()
 │
 ├─ flag.Parse()                        // resolve -dir
 ├─ os.MkdirAll(*dir, 0o755)            // create directory if missing
 │
 └─ store.NewStore(walPath, *dir)
      │
      ├─ openManifest(dir)              // open/create MANIFEST; read live SSTable list
      ├─ sst.NewReader(path) × N        // open SSTable readers (newest-first)
      ├─ replay wal.immutable (if present)  // recover a crashed flush
      ├─ replay wal                     // recover un-flushed writes
      └─ w.NewWriter(walPath)           // open fresh WAL writer (appends)
```

If any step fails, `main` writes the error to stderr and exits with code 1.

---

## Signal Handling

A dedicated goroutine listens for `SIGINT` (Ctrl-C) and `SIGTERM` (sent by process
managers / `kill`):

```go
sig := make(chan os.Signal, 1)
signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
go func() {
    <-sig
    fmt.Println("\nshutting down…")
    if err := s.Close(); err != nil {
        fmt.Fprintf(os.Stderr, "close: %v\n", err)
    }
    os.Exit(0)
}()
```

`s.Close()` waits for any in-progress background flush (`flushWg.Wait()`), then
synchronously flushes the active memtable to a new SSTable, closes all file handles,
and syncs the MANIFEST. If the close fails the error is printed to stderr and the
process still exits 0 (the signal path). The REPL's own shutdown label exits 1 on
close failure (see [Shutdown Label](#shutdown-label)).

---

## REPL Loop

```
bufio.Scanner(os.Stdin)
  │
  ├─ print "> "
  ├─ scanner.Scan()       → false on EOF (Ctrl-D) → goto shutdown
  ├─ TrimSpace(line)      → skip empty lines
  ├─ SplitN(line, " ", 3) → parts[0]=cmd, parts[1]=key, parts[2]=value
  └─ switch cmd { put | get | delete | scan | exit/quit | default }
```

### Why `SplitN(line, " ", 3)`?

The limit of **3 parts** means the value field (`parts[2]`) can contain spaces — the
split stops after the first two space-delimiters. For example:

```
> put greeting hello world
parts = ["put", "greeting", "hello world"]
```

The stored value is `"hello world"` (with the space preserved). Keys, however, are
parsed as `parts[1]` and therefore **cannot contain spaces** — the first space always
ends the key token.

---

## Commands

### `put <key> <value>`

```
if len(parts) < 3 → print usage to stderr and continue
s.Put([]byte(parts[1]), []byte(parts[2]))
  → on success: print "ok"
  → on error:   print error to stderr and continue
```

Both key and value are passed as raw `[]byte` slices — no encoding or escaping is
applied.

---

### `get <key>`

```
if len(parts) < 2 → print usage to stderr and continue
s.Get([]byte(parts[1]))
  → on success:                    print the value as a string
  → on ErrKeyNotFound (errors.Is): print "(not found)"
  → on other error:                print error to stderr and continue
```

The `(not found)` path uses `errors.Is(err, pkgsrc.ErrKeyNotFound)` to correctly
unwrap `*pkgsrc.KeyNotFoundError` values returned by the store.

---

### `delete <key>`

```
if len(parts) < 2 → print usage to stderr and continue
s.Delete([]byte(parts[1]))
  → on success: print "ok"
  → on error:   print error to stderr and continue
```

`Delete` writes a **tombstone** entry to the WAL and memtable. The key does not need
to exist — deleting a non-existent key succeeds silently.

---

### `scan <startKey> <endKey>`

```
if len(parts) < 3 → print usage to stderr and continue
s.Scan([]byte(parts[1]), []byte(parts[2]))
  → on error: print error to stderr and continue
  → on success:
      for each it.Valid() entry:
          fmt.Printf("  %s = %s\n", it.Key(), it.Value())   ← 2-space indent
          count++
      it.Close()
      if count == 0: print "(no results)"
```

The scan range is **`[startKey, endKey)`** — `startKey` is inclusive and `endKey` is
exclusive, following standard LSM-tree iterator convention. Results are emitted in
lexicographic (byte-sorted) order because the underlying merge iterator merges all
sources in order.

Each result line is printed with a **two-space indent** (`"  key = value"`). The
`parseResponses` helper in the E2E test suite recognises this prefix to distinguish
scan continuation lines from single-line responses.

---

### `exit` / `quit`

Both aliases jump directly to the `shutdown` label via `goto shutdown`. No further
REPL iterations occur.

---

## EOF / Ctrl-D

When `scanner.Scan()` returns `false` (end of input — pipe closed or Ctrl-D in a
terminal), the `for` loop exits via `break` and falls through to the `shutdown` label.
This allows the binary to be driven non-interactively by piping commands to stdin
(as the E2E test suite does).

---

## Shutdown Label

```go
shutdown:
    if err := s.Close(); err != nil {
        fmt.Fprintf(os.Stderr, "close: %v\n", err)
        os.Exit(1)
    }
```

`s.Close()` flushes any buffered memtable data to a new SSTable and syncs all file
handles. Exiting with code **1** on close failure is intentional: a failed close likely
means data was not fully persisted, and a non-zero exit code signals this to the
caller (e.g., a shell script or test harness).

---

## Error Handling Philosophy

| Situation | Behaviour |
|---|---|
| Bad flag / missing data dir | Fatal — print to stderr, `os.Exit(1)` |
| Store open failure | Fatal — print to stderr, `os.Exit(1)` |
| `put` / `get` / `delete` / `scan` error | Non-fatal — print to stderr, continue REPL |
| `get` key not found | Non-fatal — print `(not found)` to stdout, continue REPL |
| `s.Close()` failure | Fatal — print to stderr, `os.Exit(1)` |
| SIGINT / SIGTERM close failure | Non-fatal — print to stderr, `os.Exit(0)` |

The guiding principle is that **individual command failures must not kill the process**
— a transient I/O error on one `put` should not destroy an interactive session.
Only startup and shutdown failures are treated as fatal because they indicate the
store is either unusable or data may be lost.

---

## Limitations and Trade-offs

| Constraint | Reason |
|---|---|
| Keys and values **cannot contain newlines** | `bufio.Scanner` splits on `\n`; a newline in the input would be interpreted as a new command |
| Keys **cannot contain spaces** | `SplitN` stops at the first space — everything after it becomes part of the value |
| Values **can** contain spaces | `SplitN(line, " ", 3)` limits splitting to 2 delimiters, preserving spaces in the value |
| No escaping or quoting mechanism | The CLI is intentionally minimal; the store API itself has no such restriction |
| No multi-line values | Same root cause as the newline restriction |

---

## Running tinyKV

```bash
# Build
go build -o tinyKV .

# Start with default data directory ("data/")
./tinyKV

# Start with a custom directory
./tinyKV -dir /path/to/mydb

# Example session
> put hello world
ok
> get hello
world
> put greeting hello world
ok
> get greeting
hello world
> scan a z
  greeting = hello world
  hello = world
> delete hello
ok
> get hello
(not found)
> scan a z
  greeting = hello world
> exit
```

The process can also be driven non-interactively via a pipe:

```bash
printf 'put a 1\nput b 2\nscan a z\nexit\n' | ./tinyKV -dir /path/to/mydb
```
