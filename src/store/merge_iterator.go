package store

import (
	"bytes"

	mt "github.com/guilherme13c/tinyKV/src/memtable"
)

type mergeEntry struct {
	key       []byte
	value     []byte
	tombstone bool
	sourceIdx int // lower index = more recent source
	iter      mt.MemTableIteratorI
}

type entryHeap []mergeEntry

func (h entryHeap) less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].sourceIdx < h[j].sourceIdx // newer source wins on tie
}

func (h *entryHeap) push(e mergeEntry) {
	*h = append(*h, e)
	h.siftUp(len(*h) - 1)
}

func (h *entryHeap) pop() mergeEntry {
	root := (*h)[0]
	n := len(*h) - 1
	(*h)[0] = (*h)[n]
	*h = (*h)[:n]
	if n > 0 {
		h.siftDown(0)
	}
	return root
}

func (h *entryHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.less(i, parent) {
			(*h)[i], (*h)[parent] = (*h)[parent], (*h)[i]
			i = parent
		} else {
			break
		}
	}
}

func (h *entryHeap) siftDown(i int) {
	n := len(*h)
	for {
		smallest := i
		if left := 2*i + 1; left < n && h.less(left, smallest) {
			smallest = left
		}
		if right := 2*i + 2; right < n && h.less(right, smallest) {
			smallest = right
		}
		if smallest == i {
			break
		}
		(*h)[i], (*h)[smallest] = (*h)[smallest], (*h)[i]
		i = smallest
	}
}

func (h *entryHeap) heapify() {
	for i := len(*h)/2 - 1; i >= 0; i-- {
		h.siftDown(i)
	}
}

type mergeIterator struct {
	h                 entryHeap
	endKey            []byte
	curr              mergeEntry
	currValid         bool
	includeTombstones bool
}

func newMergeIterator(iters []mt.MemTableIteratorI, startKey, endKey []byte) *mergeIterator {
	return newMergeIteratorOpts(iters, startKey, endKey, false)
}

func newMergeIteratorOpts(iters []mt.MemTableIteratorI, startKey, endKey []byte, includeTombstones bool) *mergeIterator {
	h := make(entryHeap, 0, len(iters))
	for idx, it := range iters {
		if startKey != nil {
			it.Seek(startKey)
		}
		if it.Valid() {
			h = append(h, mergeEntry{
				key:       it.Key(),
				value:     it.Value(),
				tombstone: it.IsTombstone(),
				sourceIdx: idx,
				iter:      it,
			})
		}
	}
	h.heapify()
	mi := &mergeIterator{h: h, endKey: endKey, includeTombstones: includeTombstones}
	mi.advance()
	return mi
}

func (mi *mergeIterator) advance() {
	for {
		if len(mi.h) == 0 {
			mi.currValid = false
			return
		}

		top := mi.h.pop()

		if mi.endKey != nil && bytes.Compare(top.key, mi.endKey) >= 0 {
			mi.currValid = false
			return
		}

		// Drain stale entries with the same key from older sources.
		for len(mi.h) > 0 && bytes.Equal(mi.h[0].key, top.key) {
			stale := mi.h.pop()
			stale.iter.Next()
			if stale.iter.Valid() {
				mi.h.push(mergeEntry{
					key:       stale.iter.Key(),
					value:     stale.iter.Value(),
					tombstone: stale.iter.IsTombstone(),
					sourceIdx: stale.sourceIdx,
					iter:      stale.iter,
				})
			}
		}

		// Advance the winning iterator and push its next entry back.
		top.iter.Next()
		if top.iter.Valid() {
			mi.h.push(mergeEntry{
				key:       top.iter.Key(),
				value:     top.iter.Value(),
				tombstone: top.iter.IsTombstone(),
				sourceIdx: top.sourceIdx,
				iter:      top.iter,
			})
		}

		if top.tombstone && !mi.includeTombstones {
			continue
		}

		mi.curr = top
		mi.currValid = true
		return
	}
}

func (mi *mergeIterator) Valid() bool       { return mi.currValid }
func (mi *mergeIterator) Key() []byte       { return mi.curr.key }
func (mi *mergeIterator) Value() []byte     { return mi.curr.value }
func (mi *mergeIterator) IsTombstone() bool { return mi.curr.tombstone }
func (mi *mergeIterator) Next()             { mi.advance() }
func (mi *mergeIterator) Seek(_ []byte)     {} // already positioned at construction
func (mi *mergeIterator) Close() error      { return nil }
