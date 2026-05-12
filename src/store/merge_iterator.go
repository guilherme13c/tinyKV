package store

import (
	"bytes"
	"container/heap"

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

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].sourceIdx < h[j].sourceIdx // newer source wins on tie
}
func (h entryHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(x any)         { *h = append(*h, x.(mergeEntry)) }
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type mergeIterator struct {
	h                entryHeap
	endKey           []byte
	curr             *mergeEntry
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
	heap.Init(&h)
	mi := &mergeIterator{h: h, endKey: endKey, includeTombstones: includeTombstones}
	mi.advance()
	return mi
}

func (mi *mergeIterator) advance() {
	for {
		if len(mi.h) == 0 {
			mi.curr = nil
			return
		}

		top := heap.Pop(&mi.h).(mergeEntry)

		if mi.endKey != nil && bytes.Compare(top.key, mi.endKey) >= 0 {
			mi.curr = nil
			return
		}

		// Drain stale entries with the same key from older sources.
		for len(mi.h) > 0 && bytes.Equal(mi.h[0].key, top.key) {
			stale := heap.Pop(&mi.h).(mergeEntry)
			stale.iter.Next()
			if stale.iter.Valid() {
				heap.Push(&mi.h, mergeEntry{
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
			heap.Push(&mi.h, mergeEntry{
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

		entry := top
		mi.curr = &entry
		return
	}
}

func (mi *mergeIterator) Valid() bool       { return mi.curr != nil }
func (mi *mergeIterator) Key() []byte       { return mi.curr.key }
func (mi *mergeIterator) Value() []byte     { return mi.curr.value }
func (mi *mergeIterator) IsTombstone() bool { return mi.curr.tombstone }
func (mi *mergeIterator) Next()             { mi.advance() }
func (mi *mergeIterator) Seek(_ []byte)     {} // already positioned at construction
func (mi *mergeIterator) Close() error      { return nil }
