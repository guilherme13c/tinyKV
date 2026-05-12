package compare

// #cgo LDFLAGS: -lrocksdb -lstdc++
// #include <rocksdb/c.h>
// #include <stdlib.h>
import "C"

import (
	"errors"
	"unsafe"
)

// ── RocksDB adapter (minimal CGO, no external Go binding needed) ──────────────

type rocksDB struct {
	db  *C.rocksdb_t
	dir string
	ro  *C.rocksdb_readoptions_t
	wo  *C.rocksdb_writeoptions_t
}

func openRocksDB(dir string, blockCacheBytes uint64) (DB, error) {
	bbto := C.rocksdb_block_based_options_create()

	if blockCacheBytes > 0 {
		cache := C.rocksdb_cache_create_lru(C.size_t(blockCacheBytes))
		C.rocksdb_block_based_options_set_block_cache(bbto, cache)
	} else {
		C.rocksdb_block_based_options_set_no_block_cache(bbto, 1)
	}

	bf := C.rocksdb_filterpolicy_create_bloom(10)
	C.rocksdb_block_based_options_set_filter_policy(bbto, bf)

	opts := C.rocksdb_options_create()
	C.rocksdb_options_set_create_if_missing(opts, 1)
	C.rocksdb_options_set_write_buffer_size(opts, 4*1024*1024) // 4 MB: match tinyKV
	C.rocksdb_options_set_block_based_table_factory(opts, bbto)

	cdir := C.CString(dir)
	defer C.free(unsafe.Pointer(cdir))

	var cerr *C.char
	db := C.rocksdb_open(opts, cdir, &cerr)
	if cerr != nil {
		msg := C.GoString(cerr)
		C.rocksdb_free(unsafe.Pointer(cerr))
		return nil, errors.New(msg)
	}

	C.rocksdb_options_destroy(opts)
	C.rocksdb_block_based_options_destroy(bbto)

	return &rocksDB{
		db:  db,
		dir: dir,
		ro:  C.rocksdb_readoptions_create(),
		wo:  C.rocksdb_writeoptions_create(),
	}, nil
}

// OpenRocksDB opens RocksDB with an 8 MB block cache.
func OpenRocksDB(dir string) (DB, error) { return openRocksDB(dir, 8*1024*1024) }

// OpenRocksDBNocache opens RocksDB with no block cache (cold-read benchmarks).
func OpenRocksDBNocache(dir string) (DB, error) { return openRocksDB(dir, 0) }

func (d *rocksDB) Put(key, value []byte) error {
	ckey := (*C.char)(unsafe.Pointer(&key[0]))
	cval := (*C.char)(unsafe.Pointer(&value[0]))
	var cerr *C.char
	C.rocksdb_put(d.db, d.wo, ckey, C.size_t(len(key)), cval, C.size_t(len(value)), &cerr)
	return cErrToGo(cerr)
}

func (d *rocksDB) Get(key []byte) ([]byte, error) {
	ckey := (*C.char)(unsafe.Pointer(&key[0]))
	var vlen C.size_t
	var cerr *C.char
	ptr := C.rocksdb_get(d.db, d.ro, ckey, C.size_t(len(key)), &vlen, &cerr)
	if cerr != nil {
		return nil, cErrToGo(cerr)
	}
	if ptr == nil {
		return nil, nil // key not found
	}
	val := C.GoBytes(unsafe.Pointer(ptr), C.int(vlen))
	C.rocksdb_free(unsafe.Pointer(ptr))
	return val, nil
}

func (d *rocksDB) Delete(key []byte) error {
	ckey := (*C.char)(unsafe.Pointer(&key[0]))
	var cerr *C.char
	C.rocksdb_delete(d.db, d.wo, ckey, C.size_t(len(key)), &cerr)
	return cErrToGo(cerr)
}

func (d *rocksDB) Scan(start, end []byte) (Iterator, error) {
	it := C.rocksdb_create_iterator(d.db, d.ro)
	C.rocksdb_iter_seek(it, (*C.char)(unsafe.Pointer(&start[0])), C.size_t(len(start)))
	return &rocksDBIter{it: it, end: end}, nil
}

func (d *rocksDB) Close() error {
	C.rocksdb_readoptions_destroy(d.ro)
	C.rocksdb_writeoptions_destroy(d.wo)
	C.rocksdb_close(d.db)
	return nil
}

// ReopenRocksDB flushes and reopens RocksDB without a block cache.
func ReopenRocksDB(db DB) (DB, error) {
	d := db.(*rocksDB)
	dir := d.dir

	fo := C.rocksdb_flushoptions_create()
	C.rocksdb_flushoptions_set_wait(fo, 1)
	var cerr *C.char
	C.rocksdb_flush(d.db, fo, &cerr)
	C.rocksdb_flushoptions_destroy(fo)
	if cerr != nil {
		return nil, cErrToGo(cerr)
	}

	if err := d.Close(); err != nil {
		return nil, err
	}
	return OpenRocksDBNocache(dir)
}

// ── iterator ──────────────────────────────────────────────────────────────────

type rocksDBIter struct {
	it  *C.rocksdb_iterator_t
	end []byte
}

func (i *rocksDBIter) Valid() bool {
	if C.rocksdb_iter_valid(i.it) == 0 {
		return false
	}
	return compareBytes(i.Key(), i.end) < 0
}

func (i *rocksDBIter) Next() { C.rocksdb_iter_next(i.it) }

func (i *rocksDBIter) Key() []byte {
	var klen C.size_t
	ptr := C.rocksdb_iter_key(i.it, &klen)
	return C.GoBytes(unsafe.Pointer(ptr), C.int(klen))
}

func (i *rocksDBIter) Value() []byte {
	var vlen C.size_t
	ptr := C.rocksdb_iter_value(i.it, &vlen)
	return C.GoBytes(unsafe.Pointer(ptr), C.int(vlen))
}

func (i *rocksDBIter) Close() error {
	C.rocksdb_iter_destroy(i.it)
	return nil
}

// ── helper ────────────────────────────────────────────────────────────────────

func cErrToGo(cerr *C.char) error {
	if cerr == nil {
		return nil
	}
	err := errors.New(C.GoString(cerr))
	C.rocksdb_free(unsafe.Pointer(cerr))
	return err
}
