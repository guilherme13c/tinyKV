# tinyKV — Copilot Instructions

Full architecture and design documentation lives in [`docs/`](../docs/).

## Commands

```bash
# Build
go build -o tinyKV .

# Run (default data dir: ./data/)
./tinyKV -dir /path/to/mydb

# All tests
go test ./...

# Single package or single test
go test ./src/sstable/...
go test -run TestE2EPutGet ./e2e/...

# Benchmarks
go test -bench=. -benchmem -benchtime=5s ./bench/...
```

## Conventions

### Tombstones

Deletes write a tombstone through the same WAL+MemTable path as puts. Never skip tombstone propagation — the read path relies on seeing tombstones in newer sources to stop searching older ones.

- `MemTableI.Lookup` — returns `(value, found, isTombstone)`; use when callers must distinguish "absent" from "deleted".
- `MemTableI.Get` — collapses tombstones into `KeyNotFoundError`.
- `ErrTombstone` is returned only by `sst.Reader.Get`; `store.Get` converts it to `KeyNotFoundError`.

### Locking order in Store

Always acquire `s.mu` before `s.memMu`. Never hold `s.memMu` during I/O.

### Error handling

- Public API errors satisfy `errors.Is(err, ErrKeyNotFound)` via `*KeyNotFoundError`.
- Background flush errors land in `s.bgErr` and are surfaced on the next write, not the one that caused them.

### E2E tests

`e2e/e2e_test.go` builds the real binary once in `TestMain` and drives it through stdin. Add new cases with the `run(t, dir, commands...)` helper. Scan end-keys are exclusive.
