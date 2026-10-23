package segment

import (
	"encoding/binary"
	"fmt"

	"github.com/cespare/xxhash/v2"
)

// bloom is a split-block Bloom filter (Putze, Sanders, and Singler, 2007; Parquet's): blocks of
// eight 32-bit words, a key's 64-bit hash choosing a block with its high half and one bit in each
// word with its low half. About 10 bits a key: false positives near 1%, one cache line read.
type bloom struct {
	words []uint32 // 8 per block
}

// salts are the eight odd constants each word's bit is chosen by.
var salts = [8]uint32{0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31}

// bitsPerKey is the filter's size per key.
const bitsPerKey = 10

func newBloom(keys int) bloom {
	blocks := max(1, (keys*bitsPerKey+255)/256)
	return bloom{words: make([]uint32, 8*blocks)}
}

func (b bloom) mask(h uint64) (block int, bits [8]uint32) {
	block = int(((h >> 32) * uint64(len(b.words)/8)) >> 32)
	lo := uint32(h)
	for i, s := range salts {
		bits[i] = 1 << ((lo * s) >> 27)
	}
	return block, bits
}

func (b bloom) add(key []byte) {
	block, bits := b.mask(xxhash.Sum64(key))
	for i := range 8 {
		b.words[8*block+i] |= bits[i]
	}
}

func (b bloom) has(key []byte) bool {
	block, bits := b.mask(xxhash.Sum64(key))
	for i := range 8 {
		if b.words[8*block+i]&bits[i] == 0 {
			return false
		}
	}
	return true
}

func (b bloom) bytes() []byte {
	out := make([]byte, 4*len(b.words))
	for i, w := range b.words {
		binary.LittleEndian.PutUint32(out[4*i:], w)
	}
	return out
}

func loadBloom(raw []byte) (bloom, error) {
	if len(raw) == 0 || len(raw)%32 != 0 {
		return bloom{}, fmt.Errorf("%w: a Bloom filter of %d bytes", ErrCorrupt, len(raw))
	}
	b := bloom{words: make([]uint32, len(raw)/4)}
	for i := range b.words {
		b.words[i] = binary.LittleEndian.Uint32(raw[4*i:])
	}
	return b, nil
}
