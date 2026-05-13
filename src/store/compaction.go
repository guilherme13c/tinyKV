package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	mt "github.com/guilherme13c/tinyKV/src/memtable"
	sst "github.com/guilherme13c/tinyKV/src/sstable"
)

const l1TargetFileSize = 2 * 1024 * 1024 // 2 MB per output file when writing L1
const l2TargetFileSize = 8 * 1024 * 1024 // 8 MB per output file when writing L2

// compactL0 merges all l0Snap files with any overlapping l1Snap files into new
// L1 SSTables. It updates the manifest (add new L1, del L0, del merged L1) and
// returns the new L1 readers and the L1 readers that were merged away.
// Tombstones are preserved since L2 may still hold older versions of the keys.
func (s *Store) compactL0(l0Snap, l1Snap []*sst.Reader) (newL1 []*sst.Reader, removedL1 []*sst.Reader, err error) {
	if len(l0Snap) == 0 {
		return nil, nil, nil
	}

	// Key range covered by the union of all L0 files.
	minKey, maxKey := l0KeyRange(l0Snap)

	// L1 files whose range overlaps [minKey, maxKey].
	overlapping := findOverlapping(l1Snap, minKey, maxKey)

	// Merge all L0 + overlapping L1 → new L1 files (tombstones preserved).
	allSources := make([]*sst.Reader, 0, len(l0Snap)+len(overlapping))
	allSources = append(allSources, l0Snap...)
	allSources = append(allSources, overlapping...)

	const dropTombstones = false // L2 may have older data
	newL1, err = s.writeCompactionOutput(allSources, 1, l1TargetFileSize, dropTombstones)
	if err != nil {
		return nil, nil, err
	}

	// Manifest: add new L1, remove L0 sources, remove merged L1 sources.
	for _, r := range newL1 {
		if merr := s.manifest.recordAdd(r.Path(), 1); merr != nil {
			// Best-effort cleanup of files already written.
			for _, nr := range newL1 {
				p := nr.Path()
				_ = nr.Close()
				_ = os.Remove(p)
			}
			return nil, nil, merr
		}
	}
	for _, r := range l0Snap {
		_ = s.manifest.recordDel(r.Path())
	}
	for _, r := range overlapping {
		_ = s.manifest.recordDel(r.Path())
	}

	return newL1, overlapping, nil
}

// compactL1ToL2 merges all l1Snap files with all overlapping l2Snap files into
// new L2 SSTables. L2 is the bottom level, so tombstones are dropped.
// Returns new L2 readers, L1 readers that were merged, and L2 readers that
// were merged.
func (s *Store) compactL1ToL2(l1Snap, l2Snap []*sst.Reader) (newL2 []*sst.Reader, removedL1 []*sst.Reader, removedL2 []*sst.Reader, err error) {
	if len(l1Snap) == 0 {
		return nil, nil, nil, nil
	}

	// Compute key range of all L1 files being compacted.
	minKey, maxKey := l0KeyRange(l1Snap) // same logic as l0KeyRange

	// Find overlapping L2 files.
	overlappingL2 := findOverlapping(l2Snap, minKey, maxKey)

	allSources := make([]*sst.Reader, 0, len(l1Snap)+len(overlappingL2))
	allSources = append(allSources, l1Snap...)
	allSources = append(allSources, overlappingL2...)

	const dropTombstones = true // L2 is the bottom level
	newL2, err = s.writeCompactionOutput(allSources, 2, l2TargetFileSize, dropTombstones)
	if err != nil {
		return nil, nil, nil, err
	}

	// Manifest: add new L2, remove L1 sources, remove merged L2 sources.
	for _, r := range newL2 {
		if merr := s.manifest.recordAdd(r.Path(), 2); merr != nil {
			for _, nr := range newL2 {
				p := nr.Path()
				_ = nr.Close()
				_ = os.Remove(p)
			}
			return nil, nil, nil, merr
		}
	}
	for _, r := range l1Snap {
		_ = s.manifest.recordDel(r.Path())
	}
	for _, r := range overlappingL2 {
		_ = s.manifest.recordDel(r.Path())
	}

	return newL2, l1Snap, overlappingL2, nil
}

// writeCompactionOutput merges readers into one or more new SSTables at the
// given level. Each output file is capped at targetSize bytes. Tombstones are
// either preserved or dropped based on dropTombstones. Returns opened readers
// for all output files.
func (s *Store) writeCompactionOutput(readers []*sst.Reader, level int, targetSize uint64, dropTombstones bool) ([]*sst.Reader, error) {
	iters := make([]mt.MemTableIteratorI, len(readers))
	for i, r := range readers {
		iters[i] = r.Iterator()
	}

	// includeTombstones=true so the merge iterator surfaces them; we handle
	// dropping in the write loop below.
	merged := newMergeIteratorOpts(iters, nil, nil, true)

	totalKeys := 0
	for _, r := range readers {
		totalKeys += r.EstimatedKeyCount()
	}
	keyHint := totalKeys / max(len(readers), 1)

	var (
		newReaders    []*sst.Reader
		currentWriter *sst.Writer
		currentPath   string
	)

	// closeCurrentWriter finalises the current SSTable writer and opens a reader.
	closeCurrentWriter := func() error {
		if currentWriter == nil {
			return nil
		}
		if err := currentWriter.Close(); err != nil {
			_ = os.Remove(currentPath)
			return err
		}
		r, err := sst.NewReader(currentPath, s.cache)
		if err != nil {
			_ = os.Remove(currentPath)
			return err
		}
		newReaders = append(newReaders, r)
		currentWriter = nil
		currentPath = ""
		return nil
	}

	// abortAll closes and removes all output files created so far.
	abortAll := func() {
		_ = closeCurrentWriter()
		for _, r := range newReaders {
			p := r.Path()
			_ = r.Close()
			_ = os.Remove(p)
		}
	}

	for ; merged.Valid(); merged.Next() {
		if merged.IsTombstone() && dropTombstones {
			continue
		}

		// Open a new output file when the current one doesn't exist yet or has
		// reached the target size.
		if currentWriter == nil {
			currentPath = filepath.Join(s.dir, fmt.Sprintf("%d.sst", time.Now().UnixNano()))
			w, err := sst.NewWriter(currentPath, keyHint)
			if err != nil {
				abortAll()
				return nil, err
			}
			currentWriter = w
		}

		if err := currentWriter.Append(merged.Key(), merged.Value(), merged.IsTombstone()); err != nil {
			abortAll()
			return nil, err
		}

		if currentWriter.BytesWritten() >= targetSize {
			if err := closeCurrentWriter(); err != nil {
				abortAll()
				return nil, err
			}
		}
	}

	if err := closeCurrentWriter(); err != nil {
		abortAll()
		return nil, err
	}

	return newReaders, nil
}

// l0KeyRange returns the combined [minKey, maxKey] range of a set of SSTables.
// The name reflects its original use for L0 but the logic is level-agnostic.
func l0KeyRange(readers []*sst.Reader) (minKey, maxKey []byte) {
	for i, r := range readers {
		if i == 0 {
			minKey = r.MinKey()
			maxKey = r.MaxKey()
		} else {
			if bytes.Compare(r.MinKey(), minKey) < 0 {
				minKey = r.MinKey()
			}
			if bytes.Compare(r.MaxKey(), maxKey) > 0 {
				maxKey = r.MaxKey()
			}
		}
	}
	return
}

// findOverlapping returns all readers whose key range overlaps [minKey, maxKey].
// Assumes readers is sorted by MinKey (as L1/L2 always are).
func findOverlapping(readers []*sst.Reader, minKey, maxKey []byte) []*sst.Reader {
	var result []*sst.Reader
	for _, r := range readers {
		// Overlap condition: r.MaxKey >= minKey AND r.MinKey <= maxKey
		if bytes.Compare(r.MaxKey(), minKey) >= 0 && bytes.Compare(r.MinKey(), maxKey) <= 0 {
			result = append(result, r)
		}
	}
	return result
}

// findLevelReader binary-searches a sorted, non-overlapping slice of readers
// for the one whose [MinKey, MaxKey] range contains key.
// Returns nil if no reader covers key.
func findLevelReader(readers []*sst.Reader, key []byte) *sst.Reader {
	lo, hi := 0, len(readers)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		r := readers[mid]
		if bytes.Compare(key, r.MinKey()) < 0 {
			hi = mid - 1
		} else if bytes.Compare(key, r.MaxKey()) > 0 {
			lo = mid + 1
		} else {
			return r
		}
	}
	return nil
}

