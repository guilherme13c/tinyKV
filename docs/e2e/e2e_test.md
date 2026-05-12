# `e2e/e2e_test.go` — End-to-End Test Suite

The E2E test suite validates the entire tinyKV stack — `main.go`, `store`, `sstable`,
`memtable`, and `WAL` — by driving the **compiled binary** through its stdin interface
and asserting on stdout. Because no package internals are imported, the tests are
true black-box tests: they know nothing about how data is stored, only what the CLI
promises to output.

---

## Package and Imports

```go
package e2e_test
```

Using `e2e_test` (an external test package) enforces that the tests cannot import
unexported identifiers from the `e2e` directory, preserving the black-box boundary.

---

## `TestMain` — Build-Once Fixture

```go
var binaryPath string

func TestMain(m *testing.M) {
    tmp, err := os.MkdirTemp("", "tinykv-bin-*")
    // …
    binaryPath = filepath.Join(tmp, "tinykv")
    out, err := exec.Command("go", "build", "-o", binaryPath,
        "github.com/guilherme13c/tinyKV").CombinedOutput()
    if err != nil {
        fmt.Fprintf(os.Stderr, "build failed: %v\n%s\n", err, out)
        os.Exit(1)
    }
    os.Exit(m.Run())
}
```

| Step | Detail |
|---|---|
| `os.MkdirTemp` | Creates a temporary directory for the binary; `defer os.RemoveAll` cleans it up after all tests finish |
| `go build -o binaryPath` | Compiles the full module once before any test runs; build output (compiler errors) is printed to stderr on failure |
| `os.Exit(1)` on build failure | Fails fast — there is no point running tests against a binary that does not compile |
| `os.Exit(m.Run())` | Propagates the overall test result (pass/fail count) as the process exit code, which `go test` interprets correctly |

The binary is compiled **once** and reused by all test functions. This avoids
repeated compilation overhead and ensures all tests exercise the exact same
executable.

---

## `run` Helper

```go
func run(t *testing.T, dir string, commands ...string) []string
```

`run` is the primary test helper. It:

1. Joins the supplied commands with newlines and appends `"exit\n"` automatically.
2. Starts the binary with `exec.Command(binaryPath, "-dir", dir)` and pipes the
   command string to its stdin.
3. Captures stdout via `cmd.Output()`.
4. On any non-zero exit or exec error, calls `t.Fatalf` with stderr and stdout so
   the failure is immediately diagnosable.
5. Passes the captured stdout to `parseResponses` and returns the flat response list.

### Signature

| Parameter | Role |
|---|---|
| `t *testing.T` | Test context; `t.Helper()` ensures failure locations point to the caller, not `run` itself |
| `dir string` | Data directory passed as `-dir`; typically `t.TempDir()` for isolation |
| `commands ...string` | One command string per invocation, e.g. `"put hello world"`, `"get hello"` |
| Return value `[]string` | One entry per response line (see `parseResponses`); scan entries are individual strings in the same flat slice |

### Persistence testing pattern

Because `run` starts a fresh binary process each time, calling `run` twice on the
**same `dir`** simulates a process restart — the second call must recover data written
by the first:

```go
dir := t.TempDir()
run(t, dir, "put persist-key persist-val")   // first process
resp := run(t, dir, "get persist-key")       // second process — must find the key
```

---

## `parseResponses` Helper

`parseResponses` converts the raw stdout of a binary invocation into a flat,
ordered list of response strings. It implements a small line-by-line state machine.

### Input format

The binary produces output in this shape:

```
tinyKV — commands: put <key> <value> | get <key> | …   ← header line (index 0)
> ok                                                    ← prompt + response
> world                                                 ← prompt + response
>   a = 1                                               ← prompt + first scan entry
  b = 2                                                 ← scan continuation (2-space indent)
  c = 3                                                 ← scan continuation
> (no results)                                          ← prompt + scan empty response
>                                                       ← bare prompt before exit (no response)
```

### Algorithm

```
skip lines[0]  (header)
for each remaining line:
    if HasPrefix("> "):
        content = TrimSpace(TrimPrefix("> "))
        if content != "":
            append content         ← single-line response or first scan line
    elif HasPrefix("  "):          ← exactly 2 spaces
        content = TrimSpace(line)
        if content != "":
            append content         ← scan continuation line
    else:
        ignore                     ← bare ">" before exit, or unexpected
```

### Example

For the session:

```
> put b 2
> put a 1
> put c 3
> scan a d
>   a = 1
  b = 2
  c = 3
>
```

`parseResponses` returns:

```go
["ok", "ok", "ok", "a = 1", "b = 2", "c = 3"]
```

---

## Test Cases

### Basic Operations

| Test | Commands | What is verified |
|---|---|---|
| `TestE2EPutGet` | `put hello world`, `get hello` | Smoke test: write and read back in the same session; `put` returns `"ok"`, `get` returns the value |
| `TestE2EGetMissing` | `get no-such-key` | A key that was never written returns `"(not found)"` |
| `TestE2EOverwrite` | `put k v1`, `put k v2`, `get k` | The most recent `put` wins; `get` returns `"v2"`, not `"v1"` |
| `TestE2EDelete` | `put gone bye`, `delete gone`, `get gone` | After deletion the key returns `"(not found)"` |
| `TestE2EDeleteNonExistent` | `delete never-existed` | Deleting a key that was never written still returns `"ok"` — the store writes a tombstone unconditionally |

---

### Scan

| Test | Commands | What is verified |
|---|---|---|
| `TestE2EScan` | `put b 2`, `put a 1`, `put c 3`, `scan a d` | Keys inserted out of order are returned **sorted** lexicographically |
| `TestE2EScanEndKeyExclusive` | `put a 1`, `put b 2`, `put c 3`, `scan a c` | The end key (`c`) is **excluded** from results — only `a` and `b` are returned |
| `TestE2EScanEmpty` | `scan aaa zzz` on a fresh store | An empty range (or empty store) prints `"(no results)"` |
| `TestE2EScanTombstonesExcluded` | `put a 1`, `put b 2`, `put c 3`, `delete b`, `scan a d` | Deleted keys are **invisible** in scan results; only `a` and `c` appear |

#### Response count verification

`TestE2EScan` expects exactly **6** responses: 3 `"ok"` strings from the puts, then
3 scan entries. Asserting the total count catches off-by-one errors in the parser as
well as unexpected extra or missing output.

---

### Persistence

Each persistence test uses a **shared `dir`** across two separate `run` calls to
simulate a process restart.

| Test | Session 1 | Session 2 | What is verified |
|---|---|---|---|
| `TestE2EPersistence` | `put persist-key persist-val` | `get persist-key` → `"persist-val"` | WAL replay restores a written value |
| `TestE2EPersistenceAfterDelete` | `put k v`, `delete k` | `get k` → `"(not found)"` | Tombstone survives process restart |
| `TestE2EPersistenceMultipleKeys` | `put key-00 val-00` … `put key-09 val-09` | `get key-00` … `get key-09` | All 10 keys are correctly recovered; tests WAL replay with multiple entries |

The persistence guarantee relies on `s.Close()` being called at clean shutdown. The
REPL's `exit` command (automatically appended by `run`) triggers this path.

---

### Mixed Workloads

| Test | What is verified |
|---|---|
| `TestE2EOverwriteThenScan` | After two writes to the same key, `scan` sees only the **most recent value** (`"x = new"`, not `"x = old"`) — tests that the merge iterator resolves duplicates correctly |
| `TestE2ELargeWorkload` | 50 keys written in session 1, all 50 read back in session 2 — tests correctness at a scale that exercises WAL replay with many entries; 50 small keys won't trigger a memtable flush (threshold: 4 MB), so data lives in the WAL and is replayed on restart |

---

## Design Decisions

### Black-box testing via the compiled binary

Importing internal packages directly would make tests faster but brittle: they would
break if internal APIs change even when the observable CLI behaviour is unchanged.
By testing only through stdin/stdout the suite validates the real contract: *what the
binary does when you run it*.

### `t.TempDir()` for data directories

`t.TempDir()` creates a unique temporary directory per test that Go's test runner
removes automatically after the test completes (or fails). This gives each test
complete isolation — no state leaks between tests, even if a test panics.

### Build-once pattern

`TestMain` compiles the binary once into a temp directory shared by all tests.
Compiling inside each test function would serialize compilation time with test
execution time, making the suite significantly slower. The build-once approach means
compilation cost is paid exactly once regardless of how many tests run or how many
times `-count` is set.

---

## Running the Tests

```bash
# Run all E2E tests with verbose output
go test ./e2e/ -v

# Run a specific test
go test ./e2e/ -v -run TestE2EPersistence

# Run with race detector
go test ./e2e/ -v -race
```

Expected output (abbreviated):

```
=== RUN   TestE2EPutGet
--- PASS: TestE2EPutGet (0.12s)
=== RUN   TestE2EGetMissing
--- PASS: TestE2EGetMissing (0.11s)
…
PASS
ok      github.com/guilherme13c/tinyKV/e2e    2.34s
```

---

## Coverage Gaps

| Area | Why it is not covered | How it could be tested |
|---|---|---|
| **Concurrent access** | Each `run` invocation is a single-threaded binary driven by stdin | Start the binary as a server with a network interface; send concurrent requests |
| **Crash recovery** | A clean `exit` is always appended — `s.Close()` always runs | Send `SIGKILL` (not `SIGTERM`) to the process mid-write, then restart and verify |
| **Compaction trigger** | 50 small keys are far below the 4 MB `sizeThreshold`, so no flush — and therefore no compaction — occurs in-process | Write enough data (> 4 MB) to trigger multiple flushes and reach the `compactionThreshold = 4` SSTable limit |
| **Immutable WAL replay** | No test kills the process during a background flush | Requires `SIGKILL` sent precisely while `wal.immutable` exists on disk |
| **Error injection** | All tests run against a healthy filesystem | Use a filesystem mock or `chroot` with limited permissions to simulate I/O errors |
