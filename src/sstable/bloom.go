package sstable

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

const bitsPerKey = 10

type BloomFilter struct {
	bits []byte
	k    uint32
}

func newBloomFilter(keys [][]byte) *BloomFilter {
	n := len(keys)
	if n == 0 {
		return &BloomFilter{
			bits: []byte{},
			k:    0,
		}
	}

	byteLen := int(math.Ceil(float64(n)*bitsPerKey)) / 8
	byteLen = max(byteLen, 1)

	k := uint32(math.Round(bitsPerKey * math.Log(2)))
	k = max(k, 1)

	bf := &BloomFilter{
		bits: make([]byte, byteLen),
		k:    k,
	}
	for _, key := range keys {
		bf.Add(key)
	}
	return bf
}

func (bf *BloomFilter) hash(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write(key)
	h2 := fnv.New64()
	h2.Write(key)

	return h1.Sum64(), h2.Sum64()
}

func (bf *BloomFilter) Add(key []byte) {
	m := uint64(len(bf.bits) * 8)
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < bf.k; i++ {
		bit := (h1 + uint64(i)*h2) % m
		bf.bits[bit/8] |= 1 << (bit % 8)
	}
}

func (bf *BloomFilter) MayContain(key []byte) bool {
	if len(bf.bits) == 0 {
		return false
	}
	m := uint64(len(bf.bits) * 8)
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < bf.k; i++ {
		bit := (h1 + uint64(i)*h2) % m
		if bf.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the filter as: k (4 bytes LE) | bits.
func (bf *BloomFilter) Encode() []byte {
	buf := make([]byte, 4+len(bf.bits))
	binary.LittleEndian.PutUint32(buf, bf.k)
	copy(buf[4:], bf.bits)
	return buf
}

// DecodeBloom deserializes a bloom filter produced by Encode.
func DecodeBloom(data []byte) *BloomFilter {
	if len(data) < 4 {
		return &BloomFilter{}
	}
	k := binary.LittleEndian.Uint32(data[:4])
	bits := make([]byte, len(data)-4)
	copy(bits, data[4:])

	return &BloomFilter{
		bits: bits,
		k:    k,
	}
}
