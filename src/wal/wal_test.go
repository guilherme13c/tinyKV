package wal_test

import (
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/guilherme13c/tinyKV/src/wal"
)

func TestWALWriteReadSingle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := wal.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Append([]byte("key1"), []byte("value1"), false); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := wal.NewLogReader(path)
	if err != nil {
		t.Fatalf("NewLogReader: %v", err)
	}
	defer r.Close()

	entry, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if string(entry.Key) != "key1" {
		t.Errorf("Key: got %q, want %q", entry.Key, "key1")
	}
	if string(entry.Value) != "value1" {
		t.Errorf("Value: got %q, want %q", entry.Value, "value1")
	}
	if entry.IsTombstone {
		t.Error("IsTombstone should be false")
	}

	_, err = r.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestWALWriteReadMultiple(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multi.wal")

	w, err := wal.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	const n = 100
	for i := 0; i < n; i++ {
		key := []byte{byte(i)}
		val := []byte{byte(i + 1)}
		if err := w.Append(key, val, false); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := wal.NewLogReader(path)
	if err != nil {
		t.Fatalf("NewLogReader: %v", err)
	}
	defer r.Close()

	for i := 0; i < n; i++ {
		entry, err := r.Next()
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if entry.Key[0] != byte(i) {
			t.Errorf("entry[%d] key: got %d, want %d", i, entry.Key[0], i)
		}
		if entry.Value[0] != byte(i+1) {
			t.Errorf("entry[%d] value: got %d, want %d", i, entry.Value[0], i+1)
		}
	}

	_, err = r.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF after all entries, got %v", err)
	}
}

func TestWALTombstone(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tomb.wal")

	w, err := wal.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Append([]byte("deadkey"), nil, true); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := wal.NewLogReader(path)
	if err != nil {
		t.Fatalf("NewLogReader: %v", err)
	}
	defer r.Close()

	entry, err := r.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if string(entry.Key) != "deadkey" {
		t.Errorf("Key: got %q, want %q", entry.Key, "deadkey")
	}
	if !entry.IsTombstone {
		t.Error("IsTombstone should be true")
	}
	if entry.Value != nil {
		t.Errorf("Value should be nil for tombstone, got %q", entry.Value)
	}
}

func TestWALMixedEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed.wal")

	type testEntry struct {
		key         string
		value       string
		isTombstone bool
	}
	entries := []testEntry{
		{"k1", "v1", false},
		{"k2", "", true},
		{"k3", "v3", false},
		{"k4", "", true},
		{"k5", "v5", false},
	}

	w, err := wal.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, e := range entries {
		var val []byte
		if !e.isTombstone {
			val = []byte(e.value)
		}
		if err := w.Append([]byte(e.key), val, e.isTombstone); err != nil {
			t.Fatalf("Append(%q): %v", e.key, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := wal.NewLogReader(path)
	if err != nil {
		t.Fatalf("NewLogReader: %v", err)
	}
	defer r.Close()

	for i, want := range entries {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if string(got.Key) != want.key {
			t.Errorf("[%d] Key: got %q, want %q", i, got.Key, want.key)
		}
		if got.IsTombstone != want.isTombstone {
			t.Errorf("[%d] IsTombstone: got %v, want %v", i, got.IsTombstone, want.isTombstone)
		}
		if !want.isTombstone && string(got.Value) != want.value {
			t.Errorf("[%d] Value: got %q, want %q", i, got.Value, want.value)
		}
	}
}

func TestWALEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.wal")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	f.Close()

	r, err := wal.NewLogReader(path)
	if err != nil {
		t.Fatalf("NewLogReader: %v", err)
	}
	defer r.Close()

	_, err = r.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF on empty file, got %v", err)
	}
}

func TestWALTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "trunc.wal")

	// Write 5 valid entries via the WAL writer.
	w, err := wal.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := 0; i < 5; i++ {
		key := []byte{byte('a' + i)}
		val := []byte{byte('A' + i)}
		if err := w.Append(key, val, false); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append garbage bytes that look like an incomplete uvarint sequence.
	// 0xff bytes keep the continuation bit set, causing ReadUvarint to fail.
	garbage := []byte{0xff, 0xff, 0xff, 0xff, 0xff}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	f.Write(garbage)
	f.Close()

	r, err := wal.NewLogReader(path)
	if err != nil {
		t.Fatalf("NewLogReader: %v", err)
	}
	defer r.Close()

	count := 0
	for {
		_, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error at entry %d: %v", count, err)
		}
		count++
	}
	if count != 5 {
		t.Fatalf("read %d good entries, want 5", count)
	}
}

func TestWALConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent.wal")

	w, err := wal.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	const goroutines = 10
	const perGoroutine = 100
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := []byte{byte(id), byte(i)}
				val := []byte{byte(id + 100), byte(i + 100)}
				if err := w.Append(key, val, false); err != nil {
					errs[id] = err
					return
				}
			}
		}(g)
	}
	wg.Wait()

	for g, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d error: %v", g, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify we can read all entries (order may vary).
	r, err := wal.NewLogReader(path)
	if err != nil {
		t.Fatalf("NewLogReader: %v", err)
	}
	defer r.Close()

	total := 0
	for {
		_, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		total++
	}
	if total != goroutines*perGoroutine {
		t.Fatalf("read %d entries, want %d", total, goroutines*perGoroutine)
	}
}

func TestWALCloseAndReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reopen.wal")

	// First writer: write 3 entries.
	w1, err := wal.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter(1): %v", err)
	}
	for i := 0; i < 3; i++ {
		key := []byte{byte(i)}
		val := []byte{byte(i + 10)}
		if err := w1.Append(key, val, false); err != nil {
			t.Fatalf("Append w1[%d]: %v", i, err)
		}
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("Close w1: %v", err)
	}

	// Second writer: append 3 more entries.
	w2, err := wal.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter(2): %v", err)
	}
	for i := 3; i < 6; i++ {
		key := []byte{byte(i)}
		val := []byte{byte(i + 10)}
		if err := w2.Append(key, val, false); err != nil {
			t.Fatalf("Append w2[%d]: %v", i, err)
		}
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("Close w2: %v", err)
	}

	// Read all 6 entries in order.
	r, err := wal.NewLogReader(path)
	if err != nil {
		t.Fatalf("NewLogReader: %v", err)
	}
	defer r.Close()

	for i := 0; i < 6; i++ {
		entry, err := r.Next()
		if err != nil {
			t.Fatalf("Next[%d]: %v", i, err)
		}
		if entry.Key[0] != byte(i) {
			t.Errorf("[%d] Key: got %d, want %d", i, entry.Key[0], i)
		}
		if entry.Value[0] != byte(i+10) {
			t.Errorf("[%d] Value: got %d, want %d", i, entry.Value[0], i+10)
		}
	}
	_, err = r.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF after all entries, got %v", err)
	}
}

func TestWALReaderMissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := wal.NewLogReader(filepath.Join(dir, "nonexistent.wal"))
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestWALWriterClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "closed.wal")

	w, err := wal.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append after Close should return an error.
	// Use a goroutine with a timeout to guard against potential deadlock.
	done := make(chan error, 1)
	go func() {
		done <- w.Append([]byte("k"), []byte("v"), false)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after Close, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Append after Close timed out — possible deadlock")
	}
}
