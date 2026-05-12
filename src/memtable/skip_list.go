package memtable

import (
	"bytes"
	"math/rand"

	src "github.com/guilherme13c/tinyKV/src"
)

const SkipListMaxHeight = 12

type skipListNode struct {
	key       []byte
	value     []byte
	isDeleted bool
	next      []*skipListNode
}

type SkipList struct {
	head      *skipListNode
	tail      *skipListNode
	sizeBytes uint64
}

func NewSkipList() *SkipList {
	tail := &skipListNode{}
	head := &skipListNode{}

	headNext := make([]*skipListNode, SkipListMaxHeight)
	for l := range SkipListMaxHeight {
		headNext[l] = tail
	}
	head.next = headNext

	return &SkipList{head: head, tail: tail}
}

// randomHeight returns a height in [0, SkipListMaxHeight-1].
func (sl *SkipList) randomHeight() int {
	height := 0
	for rand.Intn(2) == 1 && height < SkipListMaxHeight-1 {
		height++
	}
	return height
}

// findUpdate returns the rightmost node at each level whose key is < key.
func (sl *SkipList) findUpdate(key []byte) []*skipListNode {
	update := make([]*skipListNode, SkipListMaxHeight)
	curr := sl.head
	for level := SkipListMaxHeight - 1; level >= 0; level-- {
		for curr.next[level] != sl.tail && bytes.Compare(curr.next[level].key, key) < 0 {
			curr = curr.next[level]
		}
		update[level] = curr
	}
	return update
}

func (sl *SkipList) Get(key []byte) ([]byte, error) {
	update := sl.findUpdate(key)
	candidate := update[0].next[0]
	if candidate != sl.tail && bytes.Equal(candidate.key, key) && !candidate.isDeleted {
		return candidate.value, nil
	}
	return nil, &src.KeyNotFoundError{Key: key}
}

func (sl *SkipList) Lookup(key []byte) ([]byte, bool, bool) {
	update := sl.findUpdate(key)
	candidate := update[0].next[0]
	if candidate != sl.tail && bytes.Equal(candidate.key, key) {
		return candidate.value, true, candidate.isDeleted
	}
	return nil, false, false
}

func (sl *SkipList) Put(key []byte, value []byte, isTombstone bool) error {
	update := sl.findUpdate(key)
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

	// Insert new node.
	height := sl.randomHeight()
	newNode := &skipListNode{
		key:       key,
		value:     value,
		isDeleted: isTombstone,
		next:      make([]*skipListNode, height+1),
	}
	for l := 0; l <= height; l++ {
		newNode.next[l] = update[l].next[l]
		update[l].next[l] = newNode
	}
	sl.sizeBytes += uint64(len(key) + len(value))
	return nil
}

func (sl *SkipList) Delete(key []byte) error {
	update := sl.findUpdate(key)
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
	update := it.sl.findUpdate(key)
	it.curr = update[0].next[0]
}

func (it *skipListIterator) Close() error {
	it.curr = nil
	return nil
}
