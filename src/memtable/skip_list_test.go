package memtable

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	src "github.com/guilherme13c/tinyKV/src"
)

func TestSkipListPutGet(t *testing.T) {
	sl := NewSkipList()
	if err := sl.Put([]byte("hello"), []byte("world"), false); err != nil {
		t.Fatalf("Put: %v", err)
	}
	val, err := sl.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Fatalf("got %q, want %q", val, "world")
	}
}

func TestSkipListPutOverwrite(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("k"), []byte("v1"), false)
	sl.Put([]byte("k"), []byte("v2"), false)
	val, err := sl.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "v2" {
		t.Fatalf("got %q, want %q", val, "v2")
	}
}

func TestSkipListGetMissing(t *testing.T) {
	sl := NewSkipList()
	_, err := sl.Get([]byte("missing"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var knfe *src.KeyNotFoundError
	if !errors.As(err, &knfe) {
		t.Fatalf("error is not *KeyNotFoundError: %T", err)
	}
	if !errors.Is(err, src.ErrKeyNotFound) {
		t.Fatal("errors.Is(err, ErrKeyNotFound) should be true")
	}
}

func TestSkipListTombstone(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("gone"), nil, true)
	_, err := sl.Get([]byte("gone"))
	if err == nil {
		t.Fatal("expected error for tombstone key, got nil")
	}
	var knfe *src.KeyNotFoundError
	if !errors.As(err, &knfe) {
		t.Fatalf("expected *KeyNotFoundError, got %T", err)
	}
}

func TestSkipListLookup(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("live"), []byte("data"), false)
	sl.Put([]byte("dead"), nil, true)

	// live key
	val, found, isTombstone := sl.Lookup([]byte("live"))
	if !found || isTombstone {
		t.Fatalf("Lookup(live): found=%v tombstone=%v", found, isTombstone)
	}
	if string(val) != "data" {
		t.Fatalf("Lookup(live): got %q", val)
	}

	// tombstone key
	val, found, isTombstone = sl.Lookup([]byte("dead"))
	if !found || !isTombstone {
		t.Fatalf("Lookup(dead): found=%v tombstone=%v", found, isTombstone)
	}
	if val != nil {
		t.Fatalf("Lookup(dead): expected nil value, got %q", val)
	}

	// missing key
	val, found, isTombstone = sl.Lookup([]byte("missing"))
	if found || isTombstone {
		t.Fatalf("Lookup(missing): found=%v tombstone=%v", found, isTombstone)
	}
	if val != nil {
		t.Fatalf("Lookup(missing): expected nil, got %q", val)
	}
}

func TestSkipListSizeInBytes(t *testing.T) {
	sl := NewSkipList()
	before := sl.SizeInBytes()

	// Insert key+value
	sl.Put([]byte("mykey"), []byte("myvalue"), false)
	after := sl.SizeInBytes()
	want := len("mykey") + len("myvalue")
	if after-before != want {
		t.Fatalf("size after insert: diff=%d, want=%d", after-before, want)
	}

	// Overwrite with longer value — size should grow by the difference
	sizeBeforeOverwrite := sl.SizeInBytes()
	sl.Put([]byte("mykey"), []byte("longervalue"), false)
	sizeAfterOverwrite := sl.SizeInBytes()
	diff := len("longervalue") - len("myvalue")
	if sizeAfterOverwrite-sizeBeforeOverwrite != diff {
		t.Fatalf("size after overwrite: diff=%d, want=%d", sizeAfterOverwrite-sizeBeforeOverwrite, diff)
	}

	// Tombstone — size should shrink by the value length
	sizeBeforeTombstone := sl.SizeInBytes()
	sl.Put([]byte("mykey"), nil, true)
	sizeAfterTombstone := sl.SizeInBytes()
	if sizeAfterTombstone >= sizeBeforeTombstone {
		t.Fatalf("tombstone should reduce size: before=%d after=%d", sizeBeforeTombstone, sizeAfterTombstone)
	}
}

func TestSkipListIteratorBasic(t *testing.T) {
	sl := NewSkipList()
	keys := []string{"banana", "apple", "cherry", "date"}
	for _, k := range keys {
		sl.Put([]byte(k), []byte("v-"+k), false)
	}

	it := sl.Iterator()
	defer it.Close()

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}

	sort.Strings(keys)
	if len(got) != len(keys) {
		t.Fatalf("iterator returned %d keys, want %d", len(got), len(keys))
	}
	for i, k := range keys {
		if got[i] != k {
			t.Fatalf("index %d: got %q, want %q", i, got[i], k)
		}
	}
}

func TestSkipListIteratorSeekExact(t *testing.T) {
	sl := NewSkipList()
	for _, k := range []string{"aaa", "bbb", "ccc", "ddd"} {
		sl.Put([]byte(k), []byte("v"), false)
	}

	it := sl.Iterator()
	defer it.Close()

	it.Seek([]byte("bbb"))
	if !it.Valid() {
		t.Fatal("expected Valid() after Seek to existing key")
	}
	if string(it.Key()) != "bbb" {
		t.Fatalf("Key() after Seek: got %q, want %q", it.Key(), "bbb")
	}
}

func TestSkipListIteratorSeekBetween(t *testing.T) {
	sl := NewSkipList()
	for _, k := range []string{"aaa", "ccc", "eee"} {
		sl.Put([]byte(k), []byte("v"), false)
	}

	it := sl.Iterator()
	defer it.Close()

	it.Seek([]byte("bbb")) // between "aaa" and "ccc"
	if !it.Valid() {
		t.Fatal("expected Valid() after Seek between two keys")
	}
	if string(it.Key()) != "ccc" {
		t.Fatalf("Seek between keys: got %q, want %q", it.Key(), "ccc")
	}
}

func TestSkipListIteratorSeekPastEnd(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("aaa"), []byte("v"), false)
	sl.Put([]byte("bbb"), []byte("v"), false)

	it := sl.Iterator()
	defer it.Close()

	it.Seek([]byte("zzz"))
	if it.Valid() {
		t.Fatalf("expected !Valid() after Seek past all keys, got key=%q", it.Key())
	}
}

func TestSkipListIteratorTombstone(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("alive"), []byte("val"), false)
	sl.Put([]byte("dead"), nil, true)

	it := sl.Iterator()
	defer it.Close()

	found := map[string]bool{}
	for ; it.Valid(); it.Next() {
		found[string(it.Key())] = it.IsTombstone()
	}

	if ts, ok := found["dead"]; !ok || !ts {
		t.Fatalf("iterator should return tombstone entry: ok=%v, isTombstone=%v", ok, ts)
	}
	if ts, ok := found["alive"]; !ok || ts {
		t.Fatalf("iterator should return live entry as non-tombstone: ok=%v, isTombstone=%v", ok, ts)
	}
}

func TestSkipListIteratorNext(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("only"), []byte("one"), false)

	it := sl.Iterator()
	defer it.Close()

	if !it.Valid() {
		t.Fatal("expected Valid() at start")
	}
	it.Next()
	if it.Valid() {
		t.Fatal("expected !Valid() after advancing past last entry")
	}
}

func TestSkipListIteratorClose(t *testing.T) {
	sl := NewSkipList()
	sl.Put([]byte("k"), []byte("v"), false)

	it := sl.Iterator()
	if !it.Valid() {
		t.Fatal("expected Valid() before Close")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if it.Valid() {
		t.Fatal("expected !Valid() after Close")
	}
}

func TestSkipListLarge(t *testing.T) {
	const n = 10000
	sl := NewSkipList()

	indices := rand.Perm(n)
	for _, i := range indices {
		key := fmt.Sprintf("key-%06d", i)
		val := fmt.Sprintf("val-%06d", i)
		if err := sl.Put([]byte(key), []byte(val), false); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}

	// Verify iterator returns all keys in sorted order.
	it := sl.Iterator()
	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	it.Close()

	if len(keys) != n {
		t.Fatalf("iterator returned %d keys, want %d", len(keys), n)
	}
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Fatalf("keys not sorted at index %d: %q <= %q", i, keys[i], keys[i-1])
		}
	}

	// Verify Get for each key.
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		want := fmt.Sprintf("val-%06d", i)
		got, err := sl.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("Get(%q): got %q, want %q", key, got, want)
		}
	}
}

func TestSkipListRelease(t *testing.T) {
	// Release must not panic and must allow the arena slab to be reused.
	sl := NewSkipList()
	for i := range 100 {
		key := fmt.Sprintf("key-%03d", i)
		if err := sl.Put([]byte(key), []byte("val"), false); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	sl.Release()

	// A new SkipList created after Release should get a fresh slab from the pool
	// and work correctly — confirming the slab was returned and zeroed properly.
	sl2 := NewSkipList()
	defer sl2.Release()

	if err := sl2.Put([]byte("after-release"), []byte("ok"), false); err != nil {
		t.Fatalf("Put after pool reuse: %v", err)
	}
	val, err := sl2.Get([]byte("after-release"))
	if err != nil {
		t.Fatalf("Get after pool reuse: %v", err)
	}
	if string(val) != "ok" {
		t.Fatalf("Get: got %q, want %q", val, "ok")
	}

	// Verify that keys from the released list are not visible (arena was zeroed).
	if _, err := sl2.Get([]byte("key-000")); err == nil {
		t.Fatal("expected key-000 to be absent in new SkipList, but Get succeeded")
	}

	// Double-Release must not panic.
	sl2.Release()
	sl2.Release()
}
