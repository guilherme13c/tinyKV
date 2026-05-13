# `bloom.go` — BloomFilter

## What is a Bloom Filter?

A **Bloom filter** is a space-efficient probabilistic data structure that answers the question "is this key in the set?" with the following guarantees:

- **No false negatives**: if a key was added, `MayContain` always returns `true`.
- **Bounded false positives**: `MayContain` may return `true` for a key that was never added, but the probability is controlled by the parameters chosen at construction time.

In tinyKV the bloom filter sits in front of every SSTable data-block read. When `MayContain` returns `false`, the reader returns `KeyNotFoundError` immediately — no file I/O is performed. When it returns `true`, a block read follows and a linear scan confirms or denies the key's presence.

---

## Parameters

### `bitsPerKey = 10`

```go
const bitsPerKey = 10
```

This constant controls the space/accuracy trade-off. A standard result in Bloom filter theory states that, for an optimal number of hash functions, the false-positive rate is approximately:

```
fpr ≈ (1 − e^(−k/bitsPerKey))^k ≈ (ln 2)^bitsPerKey   [at optimal k]
```

At `bitsPerKey = 10`, the false-positive rate is approximately **1%** (1 in 100 probes for absent keys results in a needless block read). Increasing this value reduces false positives at the cost of a larger bloom block on disk.

### Optimal `k` — number of hash probes

```go
k := uint32(math.Round(bitsPerKey * math.Log(2)))
```

Given a fixed number of bits per key, the number of hash probes `k` that minimises the false-positive rate is:

```
k_opt = (m/n) × ln 2 = bitsPerKey × ln 2
k_opt = 10 × 0.6931 ≈ 6.93  →  rounded to 7
```

where `m` is the total number of bits and `n` is the number of keys. The value is clamped to a minimum of 1 to avoid degenerate filters.

---

## `BloomFilter` Struct

```go
type BloomFilter struct {
    bits []byte   // the bit array, length = ceil(n × bitsPerKey) / 8
    k    uint32   // number of hash probes per operation
}
```

| Field | Description |
|---|---|
| `bits` | The underlying bit array stored as a `[]byte`. Bit `i` lives at `bits[i/8]`, mask `1 << (i%8)`. |
| `k` | The number of bit positions probed for each key. Stored so that `MayContain` uses the same `k` the filter was built with. |

---

## `newBloomFilter`

```go
func newBloomFilter(keys [][]byte) *BloomFilter
```

Constructs a `BloomFilter` from a slice of keys.

**Steps:**

1. If `len(keys) == 0`, return an empty filter (`bits: []byte{}, k: 0`). `MayContain` on an empty filter always returns `false`.
2. Compute byte length:
   ```
   byteLen = max(ceil(n × bitsPerKey) / 8, 1)
   ```
   Integer division by 8 converts bits to bytes; the minimum of 1 prevents a zero-length bit array for tiny key sets.
3. Compute `k = max(round(bitsPerKey × ln2), 1)`.
4. Allocate `bits` (all zeros), then call `bf.Add(key)` for every key.

---

## `hash`

```go
func (bf *BloomFilter) hash(key []byte) (uint64, uint64)
```

Produces **two independent 64-bit hash values** using a single `xxHash64` evaluation plus a
**Kirsch–Mitzenmacher-style derivation** for the second value:

```
h1 = xxhash.Sum64(key)
h2 = RotateLeft64(h1, 31) × 0x9e3779b97f4a7c15   // golden-ratio multiply
```

`xxHash64` is a non-cryptographic, extremely fast hash (several GB/s on modern hardware) with
excellent avalanche properties. Deriving `h2` from `h1` via a rotate-and-multiply mix avoids a
second hash evaluation entirely while keeping the two probes statistically independent — a
well-known technique from Kirsch & Mitzenmacher (2008).

This replaced the previous `FNV-1a` + `FNV-1` double-hash (format version v1), which required two
separate allocating `hash.Hash64` calls per probe. The new approach:

- **Zero allocations** per probe (no `hash.Hash64` object created)
- **~3–5× faster** on short keys
- **Better distribution** (xxHash64 passes SMHasher; FNV does not)

---

## `Add`

```go
func (bf *BloomFilter) Add(key []byte)
```

Sets `k` bits in the filter using the **Kirsch–Mitzenmacher double-hashing** scheme:

```
bit_i = (h1 + i × h2) mod m,  for i = 0, 1, …, k−1
```

where `m = len(bits) × 8` is the total number of bits.

This construction is provably equivalent to using `k` independent hash functions, but requires only two underlying hash evaluations regardless of `k`. Each bit is set by:

```go
bf.bits[bit/8] |= 1 << (bit % 8)
```

**Bit addressing**: bit index `b` maps to byte `b/8` and bit position `b%8` within that byte (little-endian bit order within each byte).

---

## `MayContain`

```go
func (bf *BloomFilter) MayContain(key []byte) bool
```

Tests whether all `k` bit positions for `key` are set:

1. If `len(bits) == 0`, return `false` immediately (empty filter).
2. Compute `h1, h2 = hash(key)`.
3. For `i = 0 … k−1`, compute `bit_i = (h1 + i×h2) % m`.
4. If **any** bit is zero, return `false` — the key was definitely never added.
5. If all `k` bits are set, return `true` — the key was probably added (or a false positive).

The **fast-exit on first zero bit** means lookups for absent keys are cheap in practice: the first unset bit terminates the loop.

---

## `Encode`

```go
func (bf *BloomFilter) Encode() []byte
```

Serializes the filter to a flat byte slice for writing to the bloom block:

```
+──────────────+────────────────────────────────+
│  k (4 B LE)  │  bits[0] bits[1] … bits[n-1]  │
+──────────────+────────────────────────────────+
```

`k` is stored explicitly because different SSTable files may have been built with a different number of keys (and thus a different optimal `k`). Without storing `k`, `MayContain` would use the wrong number of hash probes when decoding.

---

## `DecodeBloom`

```go
func DecodeBloom(data []byte) *BloomFilter
```

Deserializes a filter produced by `Encode`:

1. If `len(data) < 4`, return an empty `BloomFilter{}` (guards against truncated or corrupt data).
2. Read `k` from the first 4 bytes (little-endian `uint32`).
3. Copy the remaining bytes into `bits`.

---

## Trade-offs and Design Notes

| Aspect | Decision | Rationale |
|---|---|---|
| **No deletion** | Bloom filters cannot remove keys | Bit-setting is irreversible; a key's bits may overlap with other keys. Deletion would require a counting variant. |
| **Hash function** | xxHash64 + rotate-multiply derivation | Fast, zero-allocation per probe. Replaced FNV (v1) in format version v2. |
| **Single filter per SSTable** | One `BloomFilter` covers all keys in the file | Simpler than per-block filters (as used by LevelDB). For large SSTables this means the bloom block is proportionally larger, but lookup is a single `MayContain` call. |
| **False-positive rate ≈ 1%** | `bitsPerKey = 10` | Reduces block reads for absent keys by ~99×. Increasing to 13 bits/key would drop fpr to ~0.1% at the cost of ~30% more bloom storage. |
