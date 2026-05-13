package store_test

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	src "github.com/guilherme13c/tinyKV/src"
	"github.com/guilherme13c/tinyKV/src/store"
)

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")
	s, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestStorePutGet(t *testing.T) {
	s := newTestStore(t)
	if err := s.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, err := s.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Errorf("Get: got %q, want %q", val, "world")
	}
}

func TestStoreGetMissing(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get([]byte("missing"))
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
	if !errors.Is(err, src.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestStoreDelete(t *testing.T) {
	s := newTestStore(t)
	if err := s.Put([]byte("todel"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete([]byte("todel")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := s.Get([]byte("todel"))
	if err == nil {
		t.Fatal("expected error after Delete, got nil")
	}
	if !errors.Is(err, src.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestStoreDeleteNonExistent(t *testing.T) {
	s := newTestStore(t)
	// Deleting a key that was never put should succeed (writes a tombstone).
	if err := s.Delete([]byte("never-existed")); err != nil {
		t.Errorf("Delete non-existent key: %v", err)
	}
}

func TestStorePutOverwrite(t *testing.T) {
	s := newTestStore(t)
	s.Put([]byte("k"), []byte("v1"))
	s.Put([]byte("k"), []byte("v2"))
	val, err := s.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "v2" {
		t.Errorf("Get: got %q, want %q", val, "v2")
	}
}

func TestStoreScan(t *testing.T) {
	s := newTestStore(t)
	const n = 10
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%02d", i)
		val := fmt.Sprintf("val-%02d", i)
		if err := s.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}

	it, err := s.Scan([]byte("key-00"), []byte("key-99"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer it.Close()

	idx := 0
	for ; it.Valid(); it.Next() {
		wantKey := fmt.Sprintf("key-%02d", idx)
		wantVal := fmt.Sprintf("val-%02d", idx)
		if string(it.Key()) != wantKey {
			t.Errorf("[%d] Key: got %q, want %q", idx, it.Key(), wantKey)
		}
		if string(it.Value()) != wantVal {
			t.Errorf("[%d] Value: got %q, want %q", idx, it.Value(), wantVal)
		}
		idx++
	}
	if idx != n {
		t.Errorf("Scan returned %d entries, want %d", idx, n)
	}
}

func TestStoreScanPartial(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%02d", i)
		s.Put([]byte(key), []byte("v"))
	}

	// Scan [key-03, key-07) → key-03, key-04, key-05, key-06 (4 results, endKey is exclusive)
	it, err := s.Scan([]byte("key-03"), []byte("key-07"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer it.Close()

	var results []string
	for ; it.Valid(); it.Next() {
		results = append(results, string(it.Key()))
	}
	if len(results) != 4 {
		t.Fatalf("Scan returned %v, want 4 keys", results)
	}
	expected := []string{"key-03", "key-04", "key-05", "key-06"}
	for i, k := range expected {
		if results[i] != k {
			t.Errorf("[%d]: got %q, want %q", i, results[i], k)
		}
	}
}

func TestStoreScanTombstonesExcluded(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%02d", i)
		s.Put([]byte(key), []byte("v"))
	}
	s.Delete([]byte("key-01"))
	s.Delete([]byte("key-03"))

	it, err := s.Scan([]byte("key-00"), []byte("key-99"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer it.Close()

	var results []string
	for ; it.Valid(); it.Next() {
		results = append(results, string(it.Key()))
	}
	if len(results) != 3 {
		t.Errorf("Scan returned %v, want 3 keys after 2 deletions", results)
	}
}

func TestStoreScanEmpty(t *testing.T) {
	s := newTestStore(t)
	it, err := s.Scan([]byte("aaa"), []byte("zzz"))
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer it.Close()
	if it.Valid() {
		t.Errorf("expected !Valid() on empty store scan, got key=%q", it.Key())
	}
}

func TestStorePersistence(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	// Write 5 keys then close.
	s1, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(1): %v", err)
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("persist-key-%03d", i)
		val := fmt.Sprintf("persist-val-%03d", i)
		if err := s1.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close(1): %v", err)
	}

	// Reopen and verify all keys are present.
	s2, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(2): %v", err)
	}
	defer s2.Close()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("persist-key-%03d", i)
		want := fmt.Sprintf("persist-val-%03d", i)
		got, err := s2.Get([]byte(key))
		if err != nil {
			t.Errorf("Get(%q) after reopen: %v", key, err)
			continue
		}
		if string(got) != want {
			t.Errorf("Get(%q): got %q, want %q", key, got, want)
		}
	}
}

func TestStoreWALReplay(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	// Write 3 keys to the first store WITHOUT calling Close (simulate crash).
	s1, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(1): %v", err)
	}
	keys := []string{"crash-key-1", "crash-key-2", "crash-key-3"}
	vals := []string{"crash-val-1", "crash-val-2", "crash-val-3"}
	for i, k := range keys {
		if err := s1.Put([]byte(k), []byte(vals[i])); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}
	// Do NOT call s1.Close() — simulate a crash. Append() returns once
	// write() has placed data in the OS page cache, so s2 can read it back
	// even without an fsync. True crash-recovery (power failure) depends on
	// the periodic WAL sync ticker; this test only verifies replay logic.

	// Open a second store at the same paths; it should replay the WAL.
	s2, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(2): %v", err)
	}
	defer s2.Close()

	for i, k := range keys {
		got, err := s2.Get([]byte(k))
		if err != nil {
			t.Errorf("Get(%q) after WAL replay: %v", k, err)
			continue
		}
		if string(got) != vals[i] {
			t.Errorf("Get(%q): got %q, want %q", k, got, vals[i])
		}
	}
}

func TestStoreFlushTrigger(t *testing.T) {
	s := newTestStore(t)

	// sizeThreshold = 4MB. Each entry: key ~14 bytes + value 4096 bytes ≈ 4110 bytes.
	// 1100 entries ≈ 4.52 MB → exceeds threshold.
	largeVal := bytes.Repeat([]byte("x"), 4096)
	const n = 1100
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("flush-key-%04d", i)
		if err := s.Put([]byte(key), largeVal); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}

	// All data should still be readable after a flush.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("flush-key-%04d", i)
		val, err := s.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q) after flush: %v", key, err)
		}
		if !bytes.Equal(val, largeVal) {
			t.Fatalf("Get(%q): value mismatch", key)
		}
	}
}

func TestStoreCompactionTrigger(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	largeVal := bytes.Repeat([]byte("x"), 4096)
	const batchSize = 1100
	const numBatches = 4

	// Write 4 batches, each causing one background flush.
	// Reopening the store between batches ensures each batch results in a
	// distinct SSTable being created.
	for batch := 0; batch < numBatches; batch++ {
		s, err := store.NewStore(walPath, dir)
		if err != nil {
			t.Fatalf("NewStore(batch %d): %v", batch, err)
		}
		for i := 0; i < batchSize; i++ {
			key := fmt.Sprintf("compact-batch%d-key-%04d", batch, i)
			if err := s.Put([]byte(key), largeVal); err != nil {
				t.Fatalf("Put[batch=%d, i=%d]: %v", batch, i, err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close(batch %d): %v", batch, err)
		}
	}

	// Reopen and verify data from all batches is readable.
	s, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(verify): %v", err)
	}
	defer s.Close()

	for batch := 0; batch < numBatches; batch++ {
		for i := 0; i < batchSize; i++ {
			key := fmt.Sprintf("compact-batch%d-key-%04d", batch, i)
			val, err := s.Get([]byte(key))
			if err != nil {
				t.Fatalf("Get(%q): %v", key, err)
			}
			if !bytes.Equal(val, largeVal) {
				t.Fatalf("Get(%q): value mismatch", key)
			}
		}
	}
}

func TestStoreConcurrentReads(t *testing.T) {
	s := newTestStore(t)

	const n = 100
	vals := make([]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("rkey-%03d", i)
		val := fmt.Sprintf("rval-%03d", i)
		vals[i] = val
		if err := s.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := fmt.Sprintf("rkey-%03d", i)
				want := vals[i]
				got, err := s.Get([]byte(key))
				if err != nil {
					errs[id] = fmt.Errorf("Get(%q): %w", key, err)
					return
				}
				if string(got) != want {
					errs[id] = fmt.Errorf("Get(%q): got %q, want %q", key, got, want)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	for g, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", g, err)
		}
	}
}

func TestStoreCloseWaitsFlush(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	s, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	// Write enough to trigger a background flush.
	largeVal := bytes.Repeat([]byte("x"), 4096)
	const n = 1100
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("cwf-key-%04d", i)
		if err := s.Put([]byte(key), largeVal); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}

	// Close immediately — should wait for the background flush to complete.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify data is present.
	s2, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(reopen): %v", err)
	}
	defer s2.Close()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("cwf-key-%04d", i)
		val, err := s2.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q) after reopen: %v", key, err)
		}
		if !bytes.Equal(val, largeVal) {
			t.Fatalf("Get(%q): value mismatch", key)
		}
	}
}

// TestStoreLeveledTombstoneAcrossLevels verifies that a delete applied after a
// key has been compacted into L1 correctly hides the key on subsequent reads.
// The test writes 4 batches of 1100 large entries to force L0→L1 compaction,
// then deletes a key that landed in L1 and confirms it is not found.
func TestStoreLeveledTombstoneAcrossLevels(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")
	largeVal := bytes.Repeat([]byte("x"), 4096)
	const batchSize = 1100

	// Batch 0: write the target key plus enough filler to fill the memtable.
	s, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(0): %v", err)
	}
	if err := s.Put([]byte("lvl-target"), []byte("original")); err != nil {
		t.Fatalf("Put target: %v", err)
	}
	for i := 0; i < batchSize; i++ {
		if err := s.Put([]byte(fmt.Sprintf("lvl-b0-%04d", i)), largeVal); err != nil {
			t.Fatalf("Put filler: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close(0): %v", err)
	}

	// Batches 1–3: accumulate 3 more L0 files so that the 4th flush triggers
	// L0 → L1 compaction (compactionThreshold = 4).
	for batch := 1; batch < 4; batch++ {
		s, err = store.NewStore(walPath, dir)
		if err != nil {
			t.Fatalf("NewStore(%d): %v", batch, err)
		}
		for i := 0; i < batchSize; i++ {
			if err := s.Put([]byte(fmt.Sprintf("lvl-b%d-%04d", batch, i)), largeVal); err != nil {
				t.Fatalf("Put[batch=%d,i=%d]: %v", batch, i, err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close(%d): %v", batch, err)
		}
	}

	// At this point "lvl-target" is in L1. Open a fresh store and delete it.
	s, err = store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(delete): %v", err)
	}
	if err := s.Delete([]byte("lvl-target")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close(delete): %v", err)
	}

	// Reopen: tombstone is in L0 (flushed by Close), original value in L1.
	s, err = store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(verify): %v", err)
	}
	defer s.Close()

	_, err = s.Get([]byte("lvl-target"))
	if err == nil {
		t.Fatal("expected key-not-found for tombstoned key, got nil")
	}
	if !errors.Is(err, src.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

// TestStoreLeveledScanAcrossLevels writes keys into two distinct batches that
// end up in different levels after compaction, then verifies that a range scan
// returns all expected keys in sorted order.
func TestStoreLeveledScanAcrossLevels(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")
	largeVal := bytes.Repeat([]byte("x"), 4096)
	const batchSize = 1100

	// Batch 0: include anchor keys "zz-first" and "zz-last" so they persist
	// through compaction.
	s, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(0): %v", err)
	}
	s.Put([]byte("zz-first"), []byte("1"))
	for i := 0; i < batchSize; i++ {
		s.Put([]byte(fmt.Sprintf("lvls-b0-%04d", i)), largeVal)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close(0): %v", err)
	}

	// Batches 1–2: filler.
	for batch := 1; batch <= 2; batch++ {
		s, err = store.NewStore(walPath, dir)
		if err != nil {
			t.Fatalf("NewStore(%d): %v", batch, err)
		}
		for i := 0; i < batchSize; i++ {
			s.Put([]byte(fmt.Sprintf("lvls-b%d-%04d", batch, i)), largeVal)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close(%d): %v", batch, err)
		}
	}

	// Batch 3: adds "zz-last" and triggers L0→L1 compaction.
	s, err = store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(3): %v", err)
	}
	s.Put([]byte("zz-last"), []byte("2"))
	for i := 0; i < batchSize; i++ {
		s.Put([]byte(fmt.Sprintf("lvls-b3-%04d", i)), largeVal)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close(3): %v", err)
	}

	// Reopen and scan only the "zz-*" keys.
	s, err = store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(scan): %v", err)
	}
	defer s.Close()

	it, err := s.Scan([]byte("zz-"), []byte("zz~")) // "zz~" > all "zz-*" keys
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer it.Close()

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	want := []string{"zz-first", "zz-last"}
	if len(got) != len(want) {
		t.Fatalf("Scan: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Scan[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

// TestStoreLeveledManifestPersistence verifies that the store survives a
// reopen after data has been spread across multiple levels: the manifest
// correctly records per-file levels, and NewStore reloads them correctly.
func TestStoreLeveledManifestPersistence(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")
	largeVal := bytes.Repeat([]byte("x"), 4096)
	const batchSize = 1100
	// 8 batches to trigger at least one L0→L1 and potentially L1→L2.
	const numBatches = 8

	for batch := 0; batch < numBatches; batch++ {
		s, err := store.NewStore(walPath, dir)
		if err != nil {
			t.Fatalf("NewStore(batch %d): %v", batch, err)
		}
		for i := 0; i < batchSize; i++ {
			if err := s.Put([]byte(fmt.Sprintf("mlvl-b%d-%04d", batch, i)), largeVal); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		if err := s.Close(); err != nil {
			t.Fatalf("Close(batch %d): %v", batch, err)
		}
	}

	// Reopen and verify every written key is still readable.
	s, err := store.NewStore(walPath, dir)
	if err != nil {
		t.Fatalf("NewStore(verify): %v", err)
	}
	defer s.Close()

	for batch := 0; batch < numBatches; batch++ {
		for i := 0; i < batchSize; i++ {
			key := fmt.Sprintf("mlvl-b%d-%04d", batch, i)
			val, err := s.Get([]byte(key))
			if err != nil {
				t.Fatalf("Get(%q): %v", key, err)
			}
			if !bytes.Equal(val, largeVal) {
				t.Fatalf("Get(%q): value mismatch", key)
			}
		}
	}
}
func TestStoreConcurrentWrites(t *testing.T) {
	s := newTestStore(t)

	const goroutines = 20
	const writesPerGoroutine = 50

	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range writesPerGoroutine {
				key := fmt.Sprintf("cw-g%02d-k%03d", id, i)
				val := fmt.Sprintf("val-%02d-%03d", id, i)
				if err := s.Put([]byte(key), []byte(val)); err != nil {
					errs[id] = fmt.Errorf("Put(%q): %w", key, err)
					return
				}
			}
			// Interleave deletes to exercise the Delete dual-lock path too.
			for i := range writesPerGoroutine / 2 {
				key := fmt.Sprintf("cw-g%02d-k%03d", id, i)
				if err := s.Delete([]byte(key)); err != nil {
					errs[id] = fmt.Errorf("Delete(%q): %w", key, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	for g, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", g, err)
		}
	}

	// Verify the second half of each goroutine's keys are still readable.
	for g := range goroutines {
		for i := writesPerGoroutine / 2; i < writesPerGoroutine; i++ {
			key := fmt.Sprintf("cw-g%02d-k%03d", g, i)
			want := fmt.Sprintf("val-%02d-%03d", g, i)
			got, err := s.Get([]byte(key))
			if err != nil {
				t.Errorf("Get(%q): %v", key, err)
				continue
			}
			if string(got) != want {
				t.Errorf("Get(%q): got %q, want %q", key, got, want)
			}
		}
		// The first half were deleted — they must be absent.
		for i := range writesPerGoroutine / 2 {
			key := fmt.Sprintf("cw-g%02d-k%03d", g, i)
			_, err := s.Get([]byte(key))
			if err == nil {
				t.Errorf("Get(%q): expected key-not-found after Delete, got nil error", key)
			} else if !errors.Is(err, src.ErrKeyNotFound) {
				t.Errorf("Get(%q): unexpected error %v", key, err)
			}
		}
	}
}
