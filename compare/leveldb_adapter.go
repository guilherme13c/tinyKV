package compare

import (
	"github.com/jmhodges/levigo"
)

// ── LevelDB adapter ───────────────────────────────────────────────────────────

type levelDB struct {
	db  *levigo.DB
	dir string
	ro  *levigo.ReadOptions
	wo  *levigo.WriteOptions
}

func openLevelDB(dir string, blockCacheBytes int) (DB, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(4 * 1024 * 1024) // match tinyKV's 4 MB memtable threshold

	if blockCacheBytes > 0 {
		opts.SetCache(levigo.NewLRUCache(blockCacheBytes))
	}
	// bloom filter to match tinyKV's per-SSTable bloom filter
	opts.SetFilterPolicy(levigo.NewBloomFilter(10))

	db, err := levigo.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	return &levelDB{
		db:  db,
		dir: dir,
		ro:  levigo.NewReadOptions(),
		wo:  levigo.NewWriteOptions(),
	}, nil
}

// OpenLevelDB opens LevelDB with a default block cache (8 MB).
func OpenLevelDB(dir string) (DB, error) { return openLevelDB(dir, 8*1024*1024) }

// OpenLevelDBNocache opens LevelDB with no block cache (cold-read benchmarks).
func OpenLevelDBNocache(dir string) (DB, error) { return openLevelDB(dir, 0) }

func (d *levelDB) Put(key, value []byte) error    { return d.db.Put(d.wo, key, value) }
func (d *levelDB) Get(key []byte) ([]byte, error) { return d.db.Get(d.ro, key) }
func (d *levelDB) Delete(key []byte) error        { return d.db.Delete(d.wo, key) }

func (d *levelDB) Scan(start, end []byte) (Iterator, error) {
	it := d.db.NewIterator(d.ro)
	it.Seek(start)
	return &levelDBIter{it: it, end: end}, nil
}

func (d *levelDB) Close() error {
	d.ro.Close()
	d.wo.Close()
	d.db.Close()
	return nil
}

// ReopenLevelDB flushes and reopens LevelDB without a block cache.
func ReopenLevelDB(db DB) (DB, error) {
	d := db.(*levelDB)
	dir := d.dir
	if err := d.Close(); err != nil {
		return nil, err
	}
	return OpenLevelDBNocache(dir)
}

type levelDBIter struct {
	it  *levigo.Iterator
	end []byte
}

func (i *levelDBIter) Valid() bool {
	return i.it.Valid() && compareBytes(i.it.Key(), i.end) < 0
}
func (i *levelDBIter) Next()         { i.it.Next() }
func (i *levelDBIter) Key() []byte   { return i.it.Key() }
func (i *levelDBIter) Value() []byte { return i.it.Value() }
func (i *levelDBIter) Close() error  { i.it.Close(); return nil }

func compareBytes(a, b []byte) int {
	la, lb := len(a), len(b)
	for i := range min(la, lb) {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	if la < lb {
		return -1
	}
	if la > lb {
		return 1
	}
	return 0
}
