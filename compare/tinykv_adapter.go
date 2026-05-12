package compare

import (
	"path/filepath"

	"github.com/guilherme13c/tinyKV/src/store"
)

// ── tinyKV adapter ────────────────────────────────────────────────────────────

type tinyKVDB struct {
	s   *store.Store
	dir string
}

// OpenTinyKV opens (or creates) a tinyKV store under dir.
func OpenTinyKV(dir string) (DB, error) {
	s, err := store.NewStore(filepath.Join(dir, "wal"), dir)
	if err != nil {
		return nil, err
	}
	return &tinyKVDB{s: s, dir: dir}, nil
}

func (d *tinyKVDB) Put(key, value []byte) error    { return d.s.Put(key, value) }
func (d *tinyKVDB) Get(key []byte) ([]byte, error) { return d.s.Get(key) }
func (d *tinyKVDB) Delete(key []byte) error        { return d.s.Delete(key) }
func (d *tinyKVDB) Close() error                   { return d.s.Close() }

func (d *tinyKVDB) Scan(start, end []byte) (Iterator, error) {
	it, err := d.s.Scan(start, end)
	if err != nil {
		return nil, err
	}
	return &tinyKVIter{it: it}, nil
}

// tinyKVIter wraps the store's merge iterator into the common Iterator interface.
// Tombstone entries are skipped so behaviour matches LevelDB/RocksDB Scan.
type tinyKVIter struct {
	it interface {
		Valid() bool
		Next()
		Key() []byte
		Value() []byte
		IsTombstone() bool
		Close() error
	}
}

func (i *tinyKVIter) Valid() bool {
	for i.it.Valid() && i.it.IsTombstone() {
		i.it.Next()
	}
	return i.it.Valid()
}
func (i *tinyKVIter) Next()         { i.it.Next() }
func (i *tinyKVIter) Key() []byte   { return i.it.Key() }
func (i *tinyKVIter) Value() []byte { return i.it.Value() }
func (i *tinyKVIter) Close() error  { return i.it.Close() }

// ReopenTinyKV closes db (flushing memtable to SSTable) and reopens it.
// This is the equivalent of flushToDisk in the existing bench suite.
func ReopenTinyKV(db DB) (DB, error) {
	d := db.(*tinyKVDB)
	if err := d.s.Close(); err != nil {
		return nil, err
	}
	return OpenTinyKV(d.dir)
}
