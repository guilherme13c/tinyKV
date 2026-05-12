package sstable_test

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	src "github.com/guilherme13c/tinyKV/src"
	"github.com/guilherme13c/tinyKV/src/sstable"
)

type sstEntry struct {
	key, value  string
	isTombstone bool
}

func buildSST(t *testing.T, dir string, entries []sstEntry) *sstable.Reader {
	t.Helper()
	path := filepath.Join(dir, "test.sst")
	w, err := sstable.NewWriter(path)
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
		t.Fatalf("Close writer: %v", err)
	}
	r, err := sstable.NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestSSTWriteReadSingle(t *testing.T) {
	r := buildSST(t, t.TempDir(), []sstEntry{{"hello", "world", false}})

	val, err := r.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Errorf("Get: got %q, want %q", val, "world")
	}
}

func TestSSTWriteReadMultiple(t *testing.T) {
	const n = 50
	entries := make([]sstEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = sstEntry{
			key:   fmt.Sprintf("key-%03d", i),
			value: fmt.Sprintf("val-%03d", i),
		}
	}

	r := buildSST(t, t.TempDir(), entries)

	for i, e := range entries {
		val, err := r.Get([]byte(e.key))
		if err != nil {
			t.Errorf("[%d] Get(%q): %v", i, e.key, err)
			continue
		}
		if string(val) != e.value {
			t.Errorf("[%d] Get(%q): got %q, want %q", i, e.key, val, e.value)
		}
	}
}

func TestSSTGetMissing(t *testing.T) {
	r := buildSST(t, t.TempDir(), []sstEntry{{"key-001", "val-001", false}})

	_, err := r.Get([]byte("key-999"))
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
	if !errors.Is(err, src.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestSSTGetTombstone(t *testing.T) {
	r := buildSST(t, t.TempDir(), []sstEntry{{"dead", "", true}})

	_, err := r.Get([]byte("dead"))
	if err == nil {
		t.Fatal("expected error for tombstone key, got nil")
	}
	if !errors.Is(err, src.ErrTombstone) {
		t.Errorf("expected ErrTombstone, got %v", err)
	}
}

func TestSSTBloomFilterNegative(t *testing.T) {
	entries := []sstEntry{
		{"bloom-key-1", "v1", false},
		{"bloom-key-2", "v2", false},
		{"bloom-key-3", "v3", false},
	}
	r := buildSST(t, t.TempDir(), entries)

	// "definitely-absent" is not in the SSTable.
	// Even if there's a bloom false positive, block scan correctly returns not-found.
	_, err := r.Get([]byte("definitely-absent"))
	if err == nil {
		t.Fatal("expected error for absent key, got nil")
	}
	if !errors.Is(err, src.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestSSTIteratorForward(t *testing.T) {
	const n = 20
	entries := make([]sstEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = sstEntry{
			key:   fmt.Sprintf("key-%03d", i),
			value: fmt.Sprintf("val-%03d", i),
		}
	}
	r := buildSST(t, t.TempDir(), entries)

	it := r.Iterator()
	defer it.Close()

	idx := 0
	for ; it.Valid(); it.Next() {
		if idx >= len(entries) {
			t.Fatalf("iterator returned more than %d entries", n)
		}
		want := entries[idx]
		if string(it.Key()) != want.key {
			t.Errorf("[%d] Key: got %q, want %q", idx, it.Key(), want.key)
		}
		if string(it.Value()) != want.value {
			t.Errorf("[%d] Value: got %q, want %q", idx, it.Value(), want.value)
		}
		if it.IsTombstone() {
			t.Errorf("[%d] IsTombstone should be false", idx)
		}
		idx++
	}
	if idx != n {
		t.Errorf("iterator returned %d entries, want %d", idx, n)
	}
}

func TestSSTIteratorTombstones(t *testing.T) {
	entries := []sstEntry{
		{"key-001", "val-001", false},
		{"key-002", "", true},
		{"key-003", "val-003", false},
		{"key-004", "", true},
	}
	r := buildSST(t, t.TempDir(), entries)

	it := r.Iterator()
	defer it.Close()

	idx := 0
	for ; it.Valid(); it.Next() {
		want := entries[idx]
		if it.IsTombstone() != want.isTombstone {
			t.Errorf("[%d] IsTombstone: got %v, want %v", idx, it.IsTombstone(), want.isTombstone)
		}
		idx++
	}
	if idx != len(entries) {
		t.Errorf("iterator returned %d entries, want %d", idx, len(entries))
	}
}

func TestSSTIteratorSeek(t *testing.T) {
	entries := make([]sstEntry, 20)
	for i := 0; i < 20; i++ {
		entries[i] = sstEntry{key: fmt.Sprintf("key-%03d", i), value: fmt.Sprintf("val-%03d", i)}
	}
	r := buildSST(t, t.TempDir(), entries)

	it := r.Iterator()
	defer it.Close()

	it.Seek([]byte("key-010"))
	if !it.Valid() {
		t.Fatal("expected Valid() after Seek to existing key")
	}
	if string(it.Key()) != "key-010" {
		t.Errorf("Key after Seek: got %q, want %q", it.Key(), "key-010")
	}
}

func TestSSTIteratorSeekMissing(t *testing.T) {
	entries := []sstEntry{
		{"key-001", "v1", false},
		{"key-003", "v3", false},
		{"key-005", "v5", false},
	}
	r := buildSST(t, t.TempDir(), entries)

	it := r.Iterator()
	defer it.Close()

	it.Seek([]byte("key-002")) // between key-001 and key-003
	if !it.Valid() {
		t.Fatal("expected Valid() after Seek to between-key")
	}
	if string(it.Key()) != "key-003" {
		t.Errorf("Seek to missing key: got %q, want %q", it.Key(), "key-003")
	}
}

func TestSSTIteratorSeekPastEnd(t *testing.T) {
	entries := []sstEntry{
		{"key-001", "v1", false},
		{"key-002", "v2", false},
	}
	r := buildSST(t, t.TempDir(), entries)

	it := r.Iterator()
	defer it.Close()

	it.Seek([]byte("zzz-past-end"))
	if it.Valid() {
		t.Errorf("expected !Valid() after Seek past end, got key=%q", it.Key())
	}
}

func TestSSTMultipleBlocks(t *testing.T) {
	// BlockSize = 4096; write enough entries to create multiple blocks.
	// Each entry ≈ key(8 bytes) + value(200 bytes) + headers ≈ 210 bytes.
	// 4096/210 ≈ 19 entries per block, so 500 entries → ~26 blocks.
	dir := t.TempDir()
	path := filepath.Join(dir, "multiblock.sst")

	w, err := sstable.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	const n = 500
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("%0200d", i) // 200-byte value
		if err := w.Append([]byte(key), []byte(val), false); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := sstable.NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%04d", i)
		want := fmt.Sprintf("%0200d", i)
		val, err := r.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if string(val) != want {
			t.Errorf("Get(%q): value mismatch", key)
		}
	}
}

func TestSSTPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "path-check.sst")

	w, err := sstable.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	w.Append([]byte("k"), []byte("v"), false)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := sstable.NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	if r.Path() != path {
		t.Errorf("Path(): got %q, want %q", r.Path(), path)
	}
}

func TestSSTEmptySSTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.sst")

	w, err := sstable.NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close empty writer: %v", err)
	}

	r, err := sstable.NewReader(path)
	if err != nil {
		t.Fatalf("NewReader on empty SSTable: %v", err)
	}
	defer r.Close()

	// Get should return not-found.
	_, err = r.Get([]byte("any-key"))
	if err == nil {
		t.Fatal("expected error from Get on empty SSTable, got nil")
	}
	if !errors.Is(err, src.ErrKeyNotFound) {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}

	// Iterator should be immediately invalid.
	it := r.Iterator()
	defer it.Close()
	if it.Valid() {
		t.Errorf("expected !Valid() on empty SSTable iterator, got key=%q", it.Key())
	}
}
