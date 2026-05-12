// Package sstable
package sstable

const (
	BlockSize  = 4096 // 4Kb
	FooterSize = 32   // 2 * 2 * 8 (4 uint64) (2 BlockHandles)
)

type BlockHandle struct {
	Offset uint64
	Length uint64
}

type Footer struct {
	IndexHandle BlockHandle
	BloomHandle BlockHandle
}
