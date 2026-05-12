package memtable

import (
	"bytes"
	"math/rand"

	src "github.com/guilherme13c/tinyKV/src"
)

const SkipListMaxHeight = 12

// arenaSize is the number of nodes in a single pre-allocated arena slab.
// 65 536 nodes × ~112 bytes ≈ 7.3 MB per slab; pool holds at most 2 slabs.
const arenaSize = 1 << 16

// arenaPool is a bounded channel pool of reusable node slabs.
// Capacity 4 prevents pool misses when rapid compaction keeps 3+ slabs in flight.
var arenaPool = make(chan []skipListNode, 4)

func init() {
	for range 4 {
		arenaPool <- make([]skipListNode, arenaSize)
	}
}

type skipListNode struct {
	key        []byte
	value      []byte
	isDeleted  bool
	nextFixed  [4]*skipListNode // avoids separate alloc for height ≤ 3 (~87% of nodes)
	next       []*skipListNode
}

type SkipList struct {
	head      *skipListNode
	tail      *skipListNode
	sizeBytes uint64
	keyCount  int
	arena     []skipListNode
	arenaTop  int
}

func NewSkipList() *SkipList {
	var slab []skipListNode
	select {
	case slab = <-arenaPool:
	default:
		slab = make([]skipListNode, arenaSize)
	}

	// Slots 0 and 1 are reserved for tail and head.
	sl := &SkipList{arena: slab, arenaTop: 2}
	sl.tail = &slab[0]
	sl.head = &slab[1]

	headNext := make([]*skipListNode, SkipListMaxHeight)
	for l := range SkipListMaxHeight {
		headNext[l] = sl.tail
	}
	sl.head.next = headNext

	return sl
}

// newNode returns the next free slot from the arena, or a heap node if exhausted.
func (sl *SkipList) newNode() *skipListNode {
	if sl.arenaTop < len(sl.arena) {
		n := &sl.arena[sl.arenaTop]
		sl.arenaTop++
		return n
	}
	return &skipListNode{}
}

// Release clears all node data and returns the arena slab to the pool.
// The SkipList must not be used after this call.
func (sl *SkipList) Release() {
	if sl.arena == nil {
		return
	}
	for i := range sl.arenaTop {
		sl.arena[i] = skipListNode{}
	}
	select {
	case arenaPool <- sl.arena:
	default:
		// Pool full; let the GC collect the slab.
	}
	sl.arena = nil
	sl.head = nil
	sl.tail = nil
	sl.arenaTop = 0
}

// randomHeight returns a height in [0, SkipListMaxHeight-1].
func (sl *SkipList) randomHeight() int {
	height := 0
	for rand.Intn(2) == 1 && height < SkipListMaxHeight-1 {
		height++
	}
	return height
}

// findUpdate fills update with the rightmost node at each level whose key is < key.
func (sl *SkipList) findUpdate(key []byte, update *[SkipListMaxHeight]*skipListNode) {
	curr := sl.head
	for level := SkipListMaxHeight - 1; level >= 0; level-- {
		for curr.next[level] != sl.tail && bytes.Compare(curr.next[level].key, key) < 0 {
			curr = curr.next[level]
		}
		update[level] = curr
	}
}

func (sl *SkipList) Get(key []byte) ([]byte, error) {
	var update [SkipListMaxHeight]*skipListNode
	sl.findUpdate(key, &update)
	candidate := update[0].next[0]
	if candidate != sl.tail && bytes.Equal(candidate.key, key) && !candidate.isDeleted {
		return candidate.value, nil
	}
	return nil, &src.KeyNotFoundError{Key: key}
}

func (sl *SkipList) Lookup(key []byte) ([]byte, bool, bool) {
	var update [SkipListMaxHeight]*skipListNode
	sl.findUpdate(key, &update)
	candidate := update[0].next[0]
	if candidate != sl.tail && bytes.Equal(candidate.key, key) {
		return candidate.value, true, candidate.isDeleted
	}
	return nil, false, false
}

func (sl *SkipList) Put(key []byte, value []byte, isTombstone bool) error {
	var update [SkipListMaxHeight]*skipListNode
	sl.findUpdate(key, &update)
	candidate := update[0].next[0]

	if candidate != sl.tail && bytes.Equal(candidate.key, key) {
		// Update existing node.
		if !candidate.isDeleted {
			sl.sizeBytes -= uint64(len(candidate.value))
		} else {
			sl.sizeBytes += uint64(len(key))
		}
		candidate.isDeleted = isTombstone
		candidate.value = value
		sl.sizeBytes += uint64(len(value))
		return nil
	}

	// Insert new node using arena allocation.
	height := sl.randomHeight()
	newNode := sl.newNode()
	newNode.key = key
	newNode.value = value
	newNode.isDeleted = isTombstone
	if height < len(newNode.nextFixed) {
		newNode.next = newNode.nextFixed[:height+1]
	} else {
		newNode.next = make([]*skipListNode, height+1)
	}
	for l := 0; l <= height; l++ {
		newNode.next[l] = update[l].next[l]
		update[l].next[l] = newNode
	}
	sl.sizeBytes += uint64(len(key) + len(value))
	sl.keyCount++
	return nil
}

func (sl *SkipList) Delete(key []byte) error {
	var update [SkipListMaxHeight]*skipListNode
	sl.findUpdate(key, &update)
	candidate := update[0].next[0]
	if candidate == sl.tail || !bytes.Equal(candidate.key, key) || candidate.isDeleted {
		return &src.KeyNotFoundError{Key: key}
	}
	candidate.isDeleted = true
	sl.sizeBytes -= uint64(len(candidate.value))

	return nil
}

func (sl *SkipList) SizeInBytes() int {
	return int(sl.sizeBytes)
}

func (sl *SkipList) Len() int {
	return sl.keyCount
}

func (sl *SkipList) Iterator() MemTableIteratorI {
	return &skipListIterator{sl: sl, curr: sl.head.next[0]}
}

type skipListIterator struct {
	sl   *SkipList
	curr *skipListNode
}

func (it *skipListIterator) Valid() bool {
	return it.curr != nil && it.curr != it.sl.tail
}

func (it *skipListIterator) Key() []byte {
	return it.curr.key
}

func (it *skipListIterator) Value() []byte {
	return it.curr.value
}

func (it *skipListIterator) IsTombstone() bool {
	return it.curr.isDeleted
}

func (it *skipListIterator) Next() {
	if !it.Valid() {
		return
	}
	it.curr = it.curr.next[0]
}

func (it *skipListIterator) Seek(key []byte) {
	var update [SkipListMaxHeight]*skipListNode
	it.sl.findUpdate(key, &update)
	it.curr = update[0].next[0]
}

func (it *skipListIterator) Close() error {
	it.curr = nil
	return nil
}
