// Package store defines and implements the system interface and operations
package store

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	pkgsrc "github.com/guilherme13c/tinyKV/src"
	mt "github.com/guilherme13c/tinyKV/src/memtable"
	sst "github.com/guilherme13c/tinyKV/src/sstable"
	w "github.com/guilherme13c/tinyKV/src/wal"
)

const sizeThreshold = 4 * 1024 * 1024 // 4 MB memtable flush threshold
const compactionThreshold = 4          // compact L0 → L1 when L0 reaches this many SSTables
const numLevels = 3                    // L0, L1, L2
const l1SizeLimit = 10 * 1024 * 1024  // 10 MB: trigger L1 → L2 when L1 exceeds this

type StoreI interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Scan(startKey []byte, endKey []byte) (mt.MemTableIteratorI, error)
	Close() error
}

type Store struct {
	memtable  mt.MemTableI
	immutable mt.MemTableI // non-nil while a background flush is in progress
	wal       w.LogWriterI
	// levels[0] = L0 SSTables, newest-first (may overlap).
	// levels[1+] = sorted by MinKey, non-overlapping.
	levels    [numLevels][]*sst.Reader
	manifest  *manifest
	cache     *blockCache
	walPath   string
	dir       string
	mu        sync.RWMutex
	memMu     sync.RWMutex // protects active memtable reads/writes; held briefly (no I/O)
	compactMu sync.Mutex   // serializes concurrent compaction attempts
	bgErr     error         // last background flush error, surfaced on next write
	flushWg   sync.WaitGroup // tracks in-flight background flush goroutine
}

func NewStore(walPath string, dir string) (*Store, error) {
	cache := newBlockCache(DefaultBlockCacheCapacity)

	// Open manifest and recover the list of live SSTables with their levels.
	mf, liveMetas, err := openManifest(dir)
	if err != nil {
		return nil, err
	}

	// Load SSTable readers into their respective levels.
	// liveMetas is ordered oldest-first; we load all then sort each level.
	var levels [numLevels][]*sst.Reader
	for i := len(liveMetas) - 1; i >= 0; i-- {
		meta := liveMetas[i]
		r, err := sst.NewReader(meta.Path, cache)
		if err != nil {
			_ = mf.close()
			return nil, err
		}
		lvl := meta.Level
		if lvl < 0 || lvl >= numLevels {
			lvl = 0
		}
		levels[lvl] = append(levels[lvl], r)
	}
	// L0 is already newest-first (we iterated the manifest in reverse above).
	// L1+ must be sorted by MinKey for binary-search lookups.
	for lvl := 1; lvl < numLevels; lvl++ {
		sort.Slice(levels[lvl], func(i, j int) bool {
			return bytes.Compare(levels[lvl][i].MinKey(), levels[lvl][j].MinKey()) < 0
		})
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
		levels:   levels,
		cache:    cache,
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

	// L0: probe all files newest-first (files may overlap).
	for _, reader := range s.levels[0] {
		val, err := reader.Get(key)
		if err == nil {
			return val, nil
		}
		if errors.Is(err, pkgsrc.ErrTombstone) {
			return nil, &pkgsrc.KeyNotFoundError{Key: key}
		}
	}

	// L1+: files are sorted by MinKey and non-overlapping — binary search.
	for lvl := 1; lvl < numLevels; lvl++ {
		if r := findLevelReader(s.levels[lvl], key); r != nil {
			val, err := r.Get(key)
			if err == nil {
				return val, nil
			}
			if errors.Is(err, pkgsrc.ErrTombstone) {
				return nil, &pkgsrc.KeyNotFoundError{Key: key}
			}
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

	// Capacity: memtable + immutable + all files across all levels.
	total := 2
	for lvl := 0; lvl < numLevels; lvl++ {
		total += len(s.levels[lvl])
	}
	iters := make([]mt.MemTableIteratorI, 0, total)

	s.memMu.RLock()
	iters = append(iters, s.memtable.Iterator())
	s.memMu.RUnlock()
	if s.immutable != nil {
		iters = append(iters, s.immutable.Iterator())
	}
	// Add all levels; sourceIdx order ensures newer data wins (L0 first, then L1, L2).
	for lvl := 0; lvl < numLevels; lvl++ {
		for _, r := range s.levels[lvl] {
			iters = append(iters, r.Iterator())
		}
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
	for lvl := 0; lvl < numLevels; lvl++ {
		for _, r := range s.levels[lvl] {
			if err := r.Close(); err != nil {
				return err
			}
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

// flushBackground writes imm to a new L0 SSTable and updates the store state.
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

	r, err := sst.NewReader(path, s.cache)
	if err != nil {
		_ = os.Remove(path)
		s.mu.Lock()
		s.bgErr = err
		s.immutable = nil
		s.mu.Unlock()
		return
	}

	// Record in manifest as L0 before updating in-memory state.
	if err = s.manifest.recordAdd(path, 0); err != nil {
		_ = r.Close()
		_ = os.Remove(path)
		s.mu.Lock()
		s.bgErr = err
		s.immutable = nil
		s.mu.Unlock()
		return
	}

	s.mu.Lock()
	s.levels[0] = append([]*sst.Reader{r}, s.levels[0]...)
	s.immutable = nil
	needsCompaction := len(s.levels[0]) >= compactionThreshold
	s.mu.Unlock()

	_ = os.Remove(immWALPath)

	if needsCompaction {
		// Serialize compactions: at most one runs at a time.
		s.compactMu.Lock()

		s.mu.RLock()
		if len(s.levels[0]) < compactionThreshold {
			// Another goroutine already compacted.
			s.mu.RUnlock()
			s.compactMu.Unlock()
			return
		}
		l0Snap := make([]*sst.Reader, len(s.levels[0]))
		copy(l0Snap, s.levels[0])
		l1Snap := make([]*sst.Reader, len(s.levels[1]))
		copy(l1Snap, s.levels[1])
		s.mu.RUnlock()

		newL1, removedL1, compactErr := s.compactL0(l0Snap, l1Snap)
		if compactErr != nil {
			s.mu.Lock()
			s.bgErr = compactErr
			s.mu.Unlock()
			s.compactMu.Unlock()
			return
		}

		// Swap L0 and L1 under write lock; preserve any new SSTables added
		// to L0 while compaction was running.
		s.mu.Lock()
		newL0 := s.levels[0][:0:len(s.levels[0])]
		newL0 = newL0[:0]
		for _, r := range s.levels[0] {
			if !containsReader(l0Snap, r) {
				newL0 = append(newL0, r)
			}
		}
		s.levels[0] = newL0

		// Build new L1: keep files that were not compacted, add new outputs.
		updatedL1 := make([]*sst.Reader, 0, len(s.levels[1])-len(removedL1)+len(newL1))
		for _, r := range s.levels[1] {
			if !containsReader(removedL1, r) {
				updatedL1 = append(updatedL1, r)
			}
		}
		updatedL1 = append(updatedL1, newL1...)
		sort.Slice(updatedL1, func(i, j int) bool {
			return bytes.Compare(updatedL1[i].MinKey(), updatedL1[j].MinKey()) < 0
		})
		s.levels[1] = updatedL1

		needsL1Compact := s.l1TotalSize() > l1SizeLimit
		l1SnapForL2 := make([]*sst.Reader, len(s.levels[1]))
		copy(l1SnapForL2, s.levels[1])
		l2Snap := make([]*sst.Reader, len(s.levels[2]))
		copy(l2Snap, s.levels[2])
		s.mu.Unlock()

		// Close and remove compacted L0 and L1 files now that they are no
		// longer reachable from the in-memory state.
		for _, r := range l0Snap {
			p := r.Path()
			_ = r.Close()
			_ = os.Remove(p)
			s.cache.remove(p)
		}
		for _, r := range removedL1 {
			p := r.Path()
			_ = r.Close()
			_ = os.Remove(p)
			s.cache.remove(p)
		}

		if needsL1Compact {
			newL2, removedL1ForL2, removedL2, compactErr := s.compactL1ToL2(l1SnapForL2, l2Snap)
			if compactErr != nil {
				s.mu.Lock()
				s.bgErr = compactErr
				s.mu.Unlock()
				s.compactMu.Unlock()
				return
			}

			s.mu.Lock()
			updatedL1Again := make([]*sst.Reader, 0, len(s.levels[1]))
			for _, r := range s.levels[1] {
				if !containsReader(removedL1ForL2, r) {
					updatedL1Again = append(updatedL1Again, r)
				}
			}
			s.levels[1] = updatedL1Again

			updatedL2 := make([]*sst.Reader, 0, len(s.levels[2])-len(removedL2)+len(newL2))
			for _, r := range s.levels[2] {
				if !containsReader(removedL2, r) {
					updatedL2 = append(updatedL2, r)
				}
			}
			updatedL2 = append(updatedL2, newL2...)
			sort.Slice(updatedL2, func(i, j int) bool {
				return bytes.Compare(updatedL2[i].MinKey(), updatedL2[j].MinKey()) < 0
			})
			s.levels[2] = updatedL2
			s.mu.Unlock()

			for _, r := range removedL1ForL2 {
				p := r.Path()
				_ = r.Close()
				_ = os.Remove(p)
				s.cache.remove(p)
			}
			for _, r := range removedL2 {
				p := r.Path()
				_ = r.Close()
				_ = os.Remove(p)
				s.cache.remove(p)
			}
		}

		s.compactMu.Unlock()
	}
}

// flushSync synchronously flushes the active MemTable to a new L0 SSTable.
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

	r, err := sst.NewReader(path, s.cache)
	if err != nil {
		return err
	}

	s.levels[0] = append([]*sst.Reader{r}, s.levels[0]...)
	old := s.memtable
	s.memtable = mt.NewSkipList()
	old.Release()

	if err := s.manifest.recordAdd(path, 0); err != nil {
		return err
	}

	_ = s.wal.Close()
	if err = os.Truncate(s.walPath, 0); err != nil {
		return err
	}
	s.wal, err = w.NewWriter(s.walPath)
	return err
}

// l1TotalSize returns the sum of file sizes of all L1 SSTables.
// Must be called with s.mu held (at least RLock).
func (s *Store) l1TotalSize() int64 {
	var total int64
	for _, r := range s.levels[1] {
		if sz, err := r.FileSize(); err == nil {
			total += sz
		}
	}
	return total
}

// containsReader reports whether readers contains r (by pointer identity).
func containsReader(readers []*sst.Reader, r *sst.Reader) bool {
	for _, x := range readers {
		if x == r {
			return true
		}
	}
	return false
}
