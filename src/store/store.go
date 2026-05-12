// Package store defines and implements the system interface and operations
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	pkgsrc "github.com/guilherme13c/tinyKV/src"
	mt "github.com/guilherme13c/tinyKV/src/memtable"
	sst "github.com/guilherme13c/tinyKV/src/sstable"
	w "github.com/guilherme13c/tinyKV/src/wal"
)

const sizeThreshold = 4 * 1024 * 1024 // 4 MB
const compactionThreshold = 4          // compact when L0 reaches this many SSTables

type StoreI interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Scan(startKey []byte, endKey []byte) (mt.MemTableIteratorI, error)
	Close() error
}

type Store struct {
	memtable  mt.MemTableI
	immutable mt.MemTableI  // non-nil while a background flush is in progress
	wal       w.LogWriterI
	sstables  []*sst.Reader
	manifest  *manifest
	walPath   string
	dir       string
	mu        sync.RWMutex
	memMu     sync.RWMutex  // protects active memtable reads/writes; held briefly (no I/O)
	compactMu sync.Mutex    // serializes concurrent compaction attempts
	bgErr     error          // last background flush error, surfaced on next write
	flushWg   sync.WaitGroup // tracks in-flight background flush goroutine
}

func NewStore(walPath string, dir string) (*Store, error) {
	// Open manifest and recover the list of live SSTables.
	mf, livePaths, err := openManifest(dir)
	if err != nil {
		return nil, err
	}

	// Load SSTable readers newest-first (manifest records oldest-first).
	readers := make([]*sst.Reader, 0, len(livePaths))
	for i := len(livePaths) - 1; i >= 0; i-- {
		r, err := sst.NewReader(livePaths[i])
		if err != nil {
			_ = mf.close()
			return nil, err
		}
		readers = append(readers, r)
	}

	memtable := mt.NewSkipList()

	// If a crash left behind an immutable WAL, replay it first.
	immWALPath := walPath + ".immutable"
	if lr, err := w.NewLogReader(immWALPath); err == nil {
		for {
			entry, err := lr.Next()
			if err != nil {
				break
			}
			if err := memtable.Put(entry.Key, entry.Value, entry.IsTombstone); err != nil {
				_ = lr.Close()
				_ = mf.close()
				return nil, err
			}
		}
		_ = lr.Close()
		_ = os.Remove(immWALPath)
	}

	// Replay the active WAL.
	if lr, err := w.NewLogReader(walPath); err == nil {
		for {
			entry, err := lr.Next()
			if err != nil {
				break
			}
			if err := memtable.Put(entry.Key, entry.Value, entry.IsTombstone); err != nil {
				_ = lr.Close()
				_ = mf.close()
				return nil, err
			}
		}
		_ = lr.Close()
	}

	logWriter, err := w.NewWriter(walPath)
	if err != nil {
		_ = mf.close()
		return nil, err
	}

	return &Store{
		wal:      logWriter,
		walPath:  walPath,
		manifest: mf,
		memtable: memtable,
		sstables: readers,
		dir:      dir,
	}, nil
}

func (s *Store) Put(key []byte, value []byte) error {
	// Hold s.mu.RLock for the WAL+MemTable write pair so the flush goroutine
	// cannot swap the epoch under us. Multiple goroutines can hold RLock
	// concurrently, letting the WAL group-commit actually batch their writes.
	s.mu.RLock()
	if s.bgErr != nil {
		s.mu.RUnlock()
		return s.bgErr
	}
	if err := s.wal.Append(key, value, false); err != nil {
		s.mu.RUnlock()
		return err
	}
	s.memMu.Lock()
	if err := s.memtable.Put(key, value, false); err != nil {
		s.memMu.Unlock()
		s.mu.RUnlock()
		return err
	}
	size := s.memtable.SizeInBytes()
	immNil := s.immutable == nil // safe: s.mu.RLock held
	s.memMu.Unlock()
	s.mu.RUnlock()

	if size > sizeThreshold && immNil {
		s.mu.Lock()
		if s.memtable.SizeInBytes() > sizeThreshold && s.immutable == nil {
			s.freeze()
		}
		s.mu.Unlock()
	}
	return nil
}

func (s *Store) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Active MemTable: needs memMu because concurrent Puts modify it.
	s.memMu.RLock()
	val, found, isTombstone := s.memtable.Lookup(key)
	s.memMu.RUnlock()

	if found {
		if isTombstone {
			return nil, &pkgsrc.KeyNotFoundError{Key: key}
		}
		return val, nil
	}

	// Immutable MemTable is frozen after freeze(); only s.mu.RLock needed.
	if s.immutable != nil {
		if val, found, isTombstone := s.immutable.Lookup(key); found {
			if isTombstone {
				return nil, &pkgsrc.KeyNotFoundError{Key: key}
			}
			return val, nil
		}
	}

	// Search SSTables newest-to-oldest.
	for _, reader := range s.sstables {
		val, err := reader.Get(key)
		if err == nil {
			return val, nil
		}
		if errors.Is(err, pkgsrc.ErrTombstone) {
			return nil, &pkgsrc.KeyNotFoundError{Key: key}
		}
	}

	return nil, &pkgsrc.KeyNotFoundError{Key: key}
}

func (s *Store) Delete(key []byte) error {
	s.mu.RLock()
	if s.bgErr != nil {
		s.mu.RUnlock()
		return s.bgErr
	}
	if err := s.wal.Append(key, nil, true); err != nil {
		s.mu.RUnlock()
		return err
	}
	s.memMu.Lock()
	if err := s.memtable.Put(key, nil, true); err != nil {
		s.memMu.Unlock()
		s.mu.RUnlock()
		return err
	}
	size := s.memtable.SizeInBytes()
	immNil := s.immutable == nil
	s.memMu.Unlock()
	s.mu.RUnlock()

	if size > sizeThreshold && immNil {
		s.mu.Lock()
		if s.memtable.SizeInBytes() > sizeThreshold && s.immutable == nil {
			s.freeze()
		}
		s.mu.Unlock()
	}
	return nil
}

func (s *Store) Scan(startKey []byte, endKey []byte) (mt.MemTableIteratorI, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iters := make([]mt.MemTableIteratorI, 0, 2+len(s.sstables))
	s.memMu.RLock()
	iters = append(iters, s.memtable.Iterator())
	s.memMu.RUnlock()
	if s.immutable != nil {
		iters = append(iters, s.immutable.Iterator())
	}
	for _, r := range s.sstables {
		iters = append(iters, r.Iterator())
	}

	return newMergeIterator(iters, startKey, endKey), nil
}

func (s *Store) Close() error {
	// Wait for any in-progress background flush before shutting down.
	s.flushWg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.memtable.SizeInBytes() > 0 {
		if err := s.flushSync(); err != nil {
			return err
		}
	}
	if err := s.wal.Close(); err != nil {
		return err
	}
	for _, r := range s.sstables {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return s.manifest.close()
}

// freeze swaps the active MemTable to immutable, rotates the WAL, and launches
// a background goroutine to flush the immutable data to disk.
// Must be called with s.mu held for writing.
func (s *Store) freeze() {
	immWALPath := s.walPath + ".immutable"
	// Seal the current WAL by renaming it; new writes go to the fresh WAL.
	_ = s.wal.Close()
	_ = os.Rename(s.walPath, immWALPath)
	newWAL, err := w.NewWriter(s.walPath)
	if err != nil {
		// If we can't open a new WAL, surface the error on the next write.
		s.bgErr = err
		return
	}
	s.wal = newWAL
	s.immutable = s.memtable
	s.memtable = mt.NewSkipList()

	s.flushWg.Add(1)
	go s.flushBackground(s.immutable, immWALPath)
}

// flushBackground writes imm to a new SSTable and updates the store state.
// It runs outside the lock for all I/O-heavy work.
func (s *Store) flushBackground(imm mt.MemTableI, immWALPath string) {
	defer s.flushWg.Done()
	// Return the arena to the pool once all I/O and state updates are complete.
	defer imm.Release()

	path := filepath.Join(s.dir, fmt.Sprintf("%d.sst", time.Now().UnixNano()))
	sstWriter, err := sst.NewWriter(path, imm.Len())
	if err != nil {
		s.mu.Lock()
		s.bgErr = err
		s.immutable = nil
		s.mu.Unlock()
		return
	}

	it := imm.Iterator()
	for ; it.Valid(); it.Next() {
		if err = sstWriter.Append(it.Key(), it.Value(), it.IsTombstone()); err != nil {
			break
		}
	}
	_ = it.Close()

	if err == nil {
		err = sstWriter.Close()
	} else {
		_ = sstWriter.Close()
	}

	if err != nil {
		_ = os.Remove(path)
		s.mu.Lock()
		s.bgErr = err
		s.immutable = nil
		s.mu.Unlock()
		return
	}

	r, err := sst.NewReader(path)
	if err != nil {
		_ = os.Remove(path)
		s.mu.Lock()
		s.bgErr = err
		s.immutable = nil
		s.mu.Unlock()
		return
	}

	// Record in manifest before updating in-memory state.
	if err = s.manifest.recordAdd(path); err != nil {
		_ = r.Close()
		_ = os.Remove(path)
		s.mu.Lock()
		s.bgErr = err
		s.immutable = nil
		s.mu.Unlock()
		return
	}

	s.mu.Lock()
	s.sstables = append([]*sst.Reader{r}, s.sstables...)
	s.immutable = nil
	needsCompaction := len(s.sstables) >= compactionThreshold
	s.mu.Unlock()

	_ = os.Remove(immWALPath)

	if needsCompaction {
		// Serialize compactions: at most one runs at a time. A concurrent flush
		// goroutine may have already compacted by the time we acquire this lock,
		// so we re-check the threshold after taking the snapshot.
		s.compactMu.Lock()

		s.mu.RLock()
		if len(s.sstables) < compactionThreshold {
			// Another goroutine already compacted; nothing to do.
			s.mu.RUnlock()
			s.compactMu.Unlock()
			return
		}
		oldReaders := make([]*sst.Reader, len(s.sstables))
		copy(oldReaders, s.sstables)
		s.mu.RUnlock()

		newReader, compactErr := s.compactIO(oldReaders)
		if compactErr != nil {
			s.mu.Lock()
			s.bgErr = compactErr
			s.mu.Unlock()
			s.compactMu.Unlock()
			return
		}

		// Swap under write lock; preserve any SSTables added during compaction.
		s.mu.Lock()
		updated := make([]*sst.Reader, 0, 1+len(s.sstables)-len(oldReaders)+1)
		updated = append(updated, newReader)
		for _, r := range s.sstables {
			wasCompacted := false
			for _, old := range oldReaders {
				if r == old {
					wasCompacted = true
					break
				}
			}
			if !wasCompacted {
				updated = append(updated, r)
			}
		}
		s.sstables = updated
		s.mu.Unlock()

		s.compactMu.Unlock()
	}
}

// compactIO merges the provided oldReaders into a single new SSTable.
// It holds no lock and operates entirely on the given snapshot.
// Tombstones are dropped — since all sources are merged, no older data remains.
// Old SSTables are removed from disk after the manifest is updated.
func (s *Store) compactIO(oldReaders []*sst.Reader) (*sst.Reader, error) {
	iters := make([]mt.MemTableIteratorI, len(oldReaders))
	for i, r := range oldReaders {
		iters[i] = r.Iterator()
	}

	// includeTombstones=true so duplicates across files are resolved correctly,
	// but we will not write tombstones to the output (full compaction).
	merged := newMergeIteratorOpts(iters, nil, nil, true)

	// Estimate the output key count from the input SSTables' bloom filter sizes.
	totalEstimatedKeys := 0
	for _, r := range oldReaders {
		totalEstimatedKeys += r.EstimatedKeyCount()
	}

	outPath := filepath.Join(s.dir, fmt.Sprintf("%d.sst", time.Now().UnixNano()))
	sstWriter, err := sst.NewWriter(outPath, totalEstimatedKeys)
	if err != nil {
		return nil, err
	}

	for ; merged.Valid(); merged.Next() {
		if merged.IsTombstone() {
			continue // safe to drop — no older SSTables remain after full compaction
		}
		if err := sstWriter.Append(merged.Key(), merged.Value(), false); err != nil {
			_ = sstWriter.Close()
			_ = os.Remove(outPath)
			return nil, err
		}
	}

	if err := sstWriter.Close(); err != nil {
		_ = os.Remove(outPath)
		return nil, err
	}

	// Record the new SSTable then remove old ones from the manifest.
	if err := s.manifest.recordAdd(outPath); err != nil {
		_ = os.Remove(outPath)
		return nil, err
	}
	for _, r := range oldReaders {
		_ = s.manifest.recordDel(r.Path())
	}

	// Open the new reader before closing the old ones.
	newReader, err := sst.NewReader(outPath)
	if err != nil {
		_ = os.Remove(outPath)
		return nil, err
	}

	for _, r := range oldReaders {
		path := r.Path()
		_ = r.Close()
		_ = os.Remove(path)
	}

	return newReader, nil
}

// flushSync synchronously flushes the active MemTable to a new SSTable.
// Must be called with s.mu held for writing.
func (s *Store) flushSync() error {
	path := filepath.Join(s.dir, fmt.Sprintf("%d.sst", time.Now().UnixNano()))

	sstWriter, err := sst.NewWriter(path, s.memtable.Len())
	if err != nil {
		return err
	}

	it := s.memtable.Iterator()
	for ; it.Valid(); it.Next() {
		if err := sstWriter.Append(it.Key(), it.Value(), it.IsTombstone()); err != nil {
			return err
		}
	}
	_ = it.Close()

	if err := sstWriter.Close(); err != nil {
		return err
	}

	r, err := sst.NewReader(path)
	if err != nil {
		return err
	}

	s.sstables = append([]*sst.Reader{r}, s.sstables...)
	old := s.memtable
	s.memtable = mt.NewSkipList()
	old.Release()

	if err := s.manifest.recordAdd(path); err != nil {
		return err
	}

	_ = s.wal.Close()
	if err = os.Truncate(s.walPath, 0); err != nil {
		return err
	}
	s.wal, err = w.NewWriter(s.walPath)
	return err
}
