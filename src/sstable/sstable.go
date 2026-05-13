// Package sstable
package sstable

const (
	BlockSize       = 4096 // 4Kb
	FooterSize      = 33   // 32 bytes (4 uint64 handles) + 1 byte format version
	FormatVersion   = 0x02 // v2: xxHash64 bloom filter (v1 used FNV64)
	RestartInterval = 16   // one restart point every N entries
)

// defaultBloomLensCap is the default pre-allocated capacity for the bloomLens slice.
// Sized to cover a full 4 MB MemTable flush at minimum entry size (17 bytes: 16B key + 1B value).
const defaultBloomLensCap = 1 << 17 // 131072 entries

// defaultBloomBufCap is the default pre-allocated byte capacity for the bloomBuf slice.
// 1 MB covers 4 MB memtable flushes (52K×16B keys = 832 KB) without any reallocation.
const defaultBloomBufCap = 1 << 20 // 1 MB

// bloomBufs is a reusable pair of bloom accumulation buffers pooled across Writer calls.
type bloomBufs struct {
	lens []int
	buf  []byte
}

// bloomBufPool recycles bloom accumulation buffers across SSTable writer calls.
// Capacity 4 matches the arena pool, covering concurrent flush + compaction writers.
var bloomBufPool = make(chan *bloomBufs, 4)

func init() {
	for range 4 {
		bloomBufPool <- &bloomBufs{
			lens: make([]int, 0, defaultBloomLensCap),
			buf:  make([]byte, 0, defaultBloomBufCap),
		}
	}
}

type BlockHandle struct {
	Offset uint64
	Length uint64
}

type Footer struct {
	IndexHandle BlockHandle
	BloomHandle BlockHandle
}
