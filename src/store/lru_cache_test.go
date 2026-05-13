package store

import (
	"fmt"
	"sync"
	"testing"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func makeBlock(size int, fill byte) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = fill
	}
	return b
}

// ── basic get/put ─────────────────────────────────────────────────────────────

func TestBlockCacheGetPut(t *testing.T) {
	c := newBlockCache(1024)

	data := makeBlock(64, 0xAB)
	c.PutBlock("a.sst", 0, data)

	got, ok := c.GetBlock("a.sst", 0)
	if !ok {
		t.Fatal("GetBlock: expected hit, got miss")
	}
	if len(got) != len(data) || got[0] != 0xAB {
		t.Errorf("GetBlock: unexpected data %v", got)
	}
}

func TestBlockCacheMiss(t *testing.T) {
	c := newBlockCache(1024)
	_, ok := c.GetBlock("a.sst", 0)
	if ok {
		t.Fatal("GetBlock: expected miss, got hit")
	}
}

func TestBlockCacheOverwrite(t *testing.T) {
	c := newBlockCache(1024)
	c.PutBlock("a.sst", 0, makeBlock(16, 0x11))
	c.PutBlock("a.sst", 0, makeBlock(16, 0x22))

	got, ok := c.GetBlock("a.sst", 0)
	if !ok {
		t.Fatal("expected hit after overwrite")
	}
	if got[0] != 0x22 {
		t.Errorf("expected overwritten value 0x22, got 0x%02x", got[0])
	}
}

// ── capacity and LRU eviction ─────────────────────────────────────────────────

func TestBlockCacheEvictsLRU(t *testing.T) {
	// Cache holds exactly 2 × 64-byte blocks.
	c := newBlockCache(128)

	c.PutBlock("a.sst", 0, makeBlock(64, 0x01)) // entry A
	c.PutBlock("a.sst", 64, makeBlock(64, 0x02)) // entry B  (cache full)

	// Access A so B becomes LRU.
	if _, ok := c.GetBlock("a.sst", 0); !ok {
		t.Fatal("expected hit for A before eviction")
	}

	// Adding C must evict B (LRU).
	c.PutBlock("a.sst", 128, makeBlock(64, 0x03)) // entry C

	if _, ok := c.GetBlock("a.sst", 64); ok {
		t.Error("expected B to be evicted")
	}
	if _, ok := c.GetBlock("a.sst", 0); !ok {
		t.Error("expected A to still be present (was recently used)")
	}
	if _, ok := c.GetBlock("a.sst", 128); !ok {
		t.Error("expected C to be present")
	}
}

func TestBlockCacheBlockLargerThanCapacity(t *testing.T) {
	c := newBlockCache(32)
	c.PutBlock("a.sst", 0, makeBlock(64, 0xFF)) // larger than cap; should be dropped
	_, ok := c.GetBlock("a.sst", 0)
	if ok {
		t.Error("block larger than cache capacity should not be cached")
	}
}

func TestBlockCacheUsedTracking(t *testing.T) {
	c := newBlockCache(200)
	c.PutBlock("a.sst", 0, makeBlock(80, 0x01))
	c.PutBlock("a.sst", 80, makeBlock(80, 0x02))
	if c.used != 160 {
		t.Errorf("expected used=160, got %d", c.used)
	}

	// A third block (80 bytes) pushes used to 240 > 200; one block must be evicted.
	c.PutBlock("a.sst", 160, makeBlock(80, 0x03))
	if c.used > 200 {
		t.Errorf("used %d exceeds capacity %d after eviction", c.used, c.cap)
	}
}

// ── remove ────────────────────────────────────────────────────────────────────

func TestBlockCacheRemove(t *testing.T) {
	c := newBlockCache(4096)
	c.PutBlock("old.sst", 0, makeBlock(64, 0x01))
	c.PutBlock("old.sst", 64, makeBlock(64, 0x02))
	c.PutBlock("keep.sst", 0, makeBlock(64, 0x03))

	c.remove("old.sst")

	if _, ok := c.GetBlock("old.sst", 0); ok {
		t.Error("expected old.sst[0] to be evicted after remove")
	}
	if _, ok := c.GetBlock("old.sst", 64); ok {
		t.Error("expected old.sst[64] to be evicted after remove")
	}
	if _, ok := c.GetBlock("keep.sst", 0); !ok {
		t.Error("expected keep.sst[0] to still be present")
	}
	// used should only reflect the surviving entry.
	if c.used != 64 {
		t.Errorf("expected used=64 after remove, got %d", c.used)
	}
}

func TestBlockCacheRemoveNonExistent(t *testing.T) {
	c := newBlockCache(1024)
	c.PutBlock("a.sst", 0, makeBlock(64, 0x01))
	c.remove("missing.sst") // must not panic or corrupt state
	if _, ok := c.GetBlock("a.sst", 0); !ok {
		t.Error("a.sst entry should survive removal of a different path")
	}
}

// ── concurrency ───────────────────────────────────────────────────────────────

func TestBlockCacheConcurrent(t *testing.T) {
	const goroutines = 16
	const blocksPerGoroutine = 64

	c := newBlockCache(DefaultBlockCacheCapacity)
	var wg sync.WaitGroup

	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range blocksPerGoroutine {
				path := fmt.Sprintf("sst-%d.sst", id)
				offset := uint64(i * 4096)
				data := makeBlock(128, byte(id))
				c.PutBlock(path, offset, data)
				got, ok := c.GetBlock(path, offset)
				if ok && got[0] != byte(id) {
					t.Errorf("data corruption: got fill 0x%02x, want 0x%02x", got[0], byte(id))
				}
			}
		}(g)
	}
	wg.Wait()

	// used must never exceed capacity.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.used > c.cap {
		t.Errorf("used %d exceeds capacity %d", c.used, c.cap)
	}
	if int64(c.lru.Len()) != int64(len(c.idx)) {
		t.Errorf("lru.Len()=%d != len(idx)=%d: list and map out of sync", c.lru.Len(), len(c.idx))
	}
}
