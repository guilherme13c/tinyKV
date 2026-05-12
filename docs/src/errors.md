# `src/errors.go` — Error Types

## Package Purpose

`errors.go` defines the two-tier error system used throughout tinyKV whenever a key lookup fails. The file exists because a single approach to error reporting cannot satisfy all call sites equally:

- **Simple callers** (e.g. `main.go`) only need to know *whether* a lookup failed, not *which* key caused it. A cheap sentinel value is sufficient.
- **Diagnostic callers** (e.g. tests, logging middleware) need the actual key to produce a useful message.

Solving this with a single type forces a choice between a sentinel with no context, or a rich type whose presence must be checked with a type assertion everywhere. tinyKV avoids that trade-off by providing both, wired together via the standard `errors.Is` mechanism so that both call sites use the same idiomatic check.

---

## Design Approach

### The `errors.Is` pattern

Since Go 1.13, `errors.Is(err, target)` does not require `err == target`. It walks the error chain by repeatedly calling `Unwrap()` on `err`, and it also checks whether `err` itself implements `Is(target error) bool`. If that method returns `true`, `errors.Is` reports a match regardless of the concrete type.

This gives tinyKV exactly the layering it needs:

```
errors.Is(err, ErrKeyNotFound)   // true for BOTH *KeyNotFoundError AND ErrKeyNotFound itself
errors.As(err, &knfe)            // only succeeds when the concrete type is *KeyNotFoundError
```

### Sentinel vs. rich error — the trade-off

| Approach | Pros | Cons |
|---|---|---|
| Sentinel only (`var ErrX = errors.New(…)`) | Trivial to check with `==` or `errors.Is`; zero allocation | Carries no context; callers cannot inspect which key triggered the error |
| Rich type only (`type T struct{ Key []byte }`) | Carries the offending key; printable message includes it | Every call site must use `errors.As` or a type assertion; `errors.Is` fails without an explicit `Is()` override |
| **Both (tinyKV)** | `errors.Is` works for all callers; `errors.As` available for those that need the key | Slightly more code; must keep `Is()` in sync with sentinel identity |

tinyKV uses both: the rich `*KeyNotFoundError` is what every internal component *returns*, while `ErrKeyNotFound` is the stable target that callers *check against*.

---

## `ErrKeyNotFound`

```go
var ErrKeyNotFound = errors.New("key not found")
```

`ErrKeyNotFound` is a package-level sentinel. It is never returned directly by any internal function — its sole purpose is to serve as a stable comparison target in `errors.Is` checks.

**When it is used by callers:**

```go
// main.go
val, err := s.Get([]byte(parts[1]))
if errors.Is(err, pkgsrc.ErrKeyNotFound) {
    fmt.Println("(not found)")
} else if err != nil {
    fmt.Fprintf(os.Stderr, "get: %v\n", err)
}
```

The caller does not need the key (it already has it from user input), so the sentinel is all it needs. Using `errors.Is` instead of a direct equality check (`err == ErrKeyNotFound`) is important: it ensures that `*KeyNotFoundError` values also match, since `KeyNotFoundError.Is` declares itself equivalent to this sentinel (see below).

---

## `ErrTombstone`

```go
var ErrTombstone = errors.New("key is tombstoned")
```

`ErrTombstone` is returned when a key is found in storage but has been **logically deleted** — its entry is a tombstone record written by a `Delete` operation.

### Semantics

A tombstone is not absence: it is an *intentional marker* that says "this key was deleted here; do not look further". This distinction matters in an LSM-tree architecture, where the same key can exist in multiple layers (active MemTable, immutable MemTable, and several SSTables ordered newest-to-oldest). Without a distinct tombstone error, a component that encounters a deleted key in a newer SSTable would have no way to tell the store layer to stop searching — it would fall through to an older SSTable and find a stale value.

### Why it is distinct from `ErrKeyNotFound`

| Error | Meaning | Caller action |
|---|---|---|
| `*KeyNotFoundError` | Key does not exist in this source | Continue searching older sources |
| `ErrTombstone` | Key was explicitly deleted in this source | **Stop searching** — any value in an older source is stale |

The store layer enforces this contract explicitly:

```go
// src/store/store.go
for _, reader := range s.sstables {
    val, err := reader.Get(key)
    if err == nil {
        return val, nil
    }
    if errors.Is(err, pkgsrc.ErrTombstone) {
        // Found a tombstone — the key is deleted; stop and surface "not found" to the caller.
        return nil, &pkgsrc.KeyNotFoundError{Key: key}
    }
    // err is *KeyNotFoundError — try the next (older) SSTable.
}
```

If `ErrTombstone` were merged with `ErrKeyNotFound`, this early-exit logic could not be expressed without an additional boolean return value or a separate type — exactly the complexity `ErrTombstone` is designed to avoid.

### Where it is returned

`ErrTombstone` is returned directly (not wrapped) by `scanBlock` in `src/sstable/reader.go`:

```go
// src/sstable/reader.go — scanBlock
if cmp == 0 {
    if isTombstone {
        return nil, src.ErrTombstone
    }
    // ...
}
```

`scanBlock` is called by `Reader.Get`, which is in turn called by `Store.Get` for each SSTable in newest-to-oldest order.

---

## `KeyNotFoundError` struct

```go
type KeyNotFoundError struct {
    Key []byte
}
```

`KeyNotFoundError` is the rich error type returned by all internal components when they cannot find a key. It carries the offending key so that diagnostic output and tests can include it.

### `Error() string`

```go
func (e *KeyNotFoundError) Error() string {
    return fmt.Sprintf("key not found: %q", e.Key)
}
```

`%q` formats the key as a Go-quoted byte string, which safely handles non-printable bytes and makes the key unambiguous in log output (e.g. `key not found: "user:42"`).

### `Is(target error) bool` — how it satisfies `errors.Is`

```go
func (e *KeyNotFoundError) Is(target error) bool {
    return target == ErrKeyNotFound
}
```

When `errors.Is(err, ErrKeyNotFound)` is called and `err` is a `*KeyNotFoundError`, the standard library invokes `err.Is(ErrKeyNotFound)`. This method returns `true`, so `errors.Is` reports a match even though the concrete type is not `ErrKeyNotFound`.

The mechanism in full:

1. `errors.Is(err, target)` first checks `err == target` (false — different types).
2. It checks whether `err` implements `interface{ Is(error) bool }` (it does).
3. It calls `err.Is(target)`, which returns `target == ErrKeyNotFound` — `true`.
4. `errors.Is` returns `true`.

No `Unwrap` chain is involved here; the `Is` method short-circuits directly. This is the correct approach when the rich type is not a *wrapper* around the sentinel but a *parallel representation* of the same condition.

---

## Usage Patterns

### Check "not found" without needing the key

```go
val, err := store.Get(key)
if errors.Is(err, ErrKeyNotFound) {
    // Key does not exist (or was deleted). Safe to treat as absent.
    return nil
}
if err != nil {
    // Some other error (I/O failure, corruption, etc.)
    return err
}
```

This is the pattern used in `main.go`. It works for both the sentinel and `*KeyNotFoundError` because of the `Is()` override.

### Extract the key for diagnostics

```go
var knfe *KeyNotFoundError
if errors.As(err, &knfe) {
    log.Printf("cache miss for key: %s", knfe.Key)
}
```

`errors.As` does a type assertion against the concrete type. It returns `true` and sets `knfe` only when the error is a `*KeyNotFoundError`. This is used in tests:

```go
// src/memtable/skip_list_test.go
var knfe *src.KeyNotFoundError
if !errors.As(err, &knfe) {
    t.Fatalf("error is not *KeyNotFoundError: %T", err)
}
if !errors.Is(err, src.ErrKeyNotFound) {
    t.Fatal("errors.Is(err, ErrKeyNotFound) should be true")
}
```

### Check for tombstone (internal / SSTable layer)

```go
if errors.Is(err, pkgsrc.ErrTombstone) {
    // Key was explicitly deleted in a newer layer — stop searching.
    return nil, &pkgsrc.KeyNotFoundError{Key: key}
}
```

`ErrTombstone` is a plain sentinel with no `Is()` override, so `errors.Is(err, ErrTombstone)` reduces to `err == ErrTombstone`. It is never wrapped.

---

## Trade-offs

### Why not just one type?

**Rich type only:** Every call site would need `errors.As` or a type assertion. The common case (checking "not found" vs. I/O error) becomes more verbose than necessary, and it is easy to mistakenly write `err == ErrKeyNotFound` which would always be false.

**Sentinel only:** Callers lose the offending key. Diagnostic messages become `"key not found"` with no context. Tests cannot verify which key was rejected.

**Both:** The additional `Is()` method is six lines of code. The payoff is that simple callers use the simple `errors.Is` check and diagnostics callers use `errors.As` — each site uses the minimum complexity it actually needs.

### Why not just panic?

`panic` is appropriate for programming errors (nil dereferences, invalid invariants), not for expected conditions. A missing key is a normal operating state in a key-value store. Panicking on every cache miss would make the store unusable in any application that legitimately queries for keys that may not exist.

### Why not return `(value []byte, found bool)`?

The boolean return pattern (as used by Go maps) works when there is only one failure mode. tinyKV has at least three states that must be distinguishable:

| State | Must return |
|---|---|
| Key found, live value | `(value, nil)` |
| Key not found | signal "absent" |
| Key found, tombstoned | signal "deleted — stop searching" |
| I/O or corruption error | signal "infrastructure failure" |

Encoding all four states into `([]byte, bool)` is impossible without repurposing the boolean or adding a third return. Errors carry all of this in a single idiomatic return value, and the two sentinel/rich-type distinction keeps the common case cheap.
