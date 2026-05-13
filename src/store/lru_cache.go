package store

import (
	"container/list"
	"sync"
)

// DefaultBlockCacheCapacity is the default capacity of the block cache in bytes.
const DefaultBlockCacheCapacity = 8 * 1024 * 1024 // 8 MB

type cacheKey struct {
	path   string
	offset uint64
}

type cacheEntry struct {
	key  cacheKey
	data []byte
}

// blockCache is a thread-safe, capacity-bounded LRU cache for SSTable data
// blocks. It implements sstable.BlockCache.
type blockCache struct {
	mu   sync.Mutex
	cap  int64 // maximum bytes of cached block data
	used int64 // current bytes of cached block data
	idx  map[cacheKey]*list.Element
	lru  *list.List // front = most recently used
}

func newBlockCache(capacityBytes int64) *blockCache {
	return &blockCache{
		cap: capacityBytes,
		idx: make(map[cacheKey]*list.Element),
		lru: list.New(),
	}
}

// GetBlock returns the cached block data for the given (path, offset), or
// (nil, false) if the block is not present in the cache.
func (c *blockCache) GetBlock(path string, offset uint64) ([]byte, bool) {
	k := cacheKey{path, offset}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.idx[k]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(elem)
	return elem.Value.(*cacheEntry).data, true
}

// PutBlock inserts a block into the cache. Blocks larger than the total cache
// capacity are silently dropped. Least-recently-used blocks are evicted until
// the cache is within capacity.
func (c *blockCache) PutBlock(path string, offset uint64, data []byte) {
	size := int64(len(data))
	if size > c.cap {
		return
	}
	k := cacheKey{path, offset}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.idx[k]; ok {
		old := elem.Value.(*cacheEntry)
		c.used -= int64(len(old.data))
		old.data = data
		c.used += size
		c.lru.MoveToFront(elem)
		return
	}
	entry := &cacheEntry{key: k, data: data}
	elem := c.lru.PushFront(entry)
	c.idx[k] = elem
	c.used += size
	for c.used > c.cap {
		back := c.lru.Back()
		if back == nil {
			break
		}
		ev := back.Value.(*cacheEntry)
		c.lru.Remove(back)
		delete(c.idx, ev.key)
		c.used -= int64(len(ev.data))
	}
}

// remove evicts all cached blocks belonging to path. Called when an SSTable is
// deleted during compaction so stale entries don't occupy cache capacity.
func (c *blockCache) remove(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, elem := range c.idx {
		if k.path == path {
			ev := elem.Value.(*cacheEntry)
			c.lru.Remove(elem)
			delete(c.idx, k)
			c.used -= int64(len(ev.data))
		}
	}
}
