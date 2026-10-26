package encoding

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// A delta (docs/scm.md, "Deltas") is a file written as copies from a base it shares bytes with and
// the bytes it does not, so a client holding the base receives only what changed:
//
//	version  u8 = 1
//	base     uvarint: the base's length
//	length   uvarint: the result's length
//	ops      to the end, each a tag uvarint — n<<1 for a copy of n bytes, n<<1|1 for an insert
//	         of n — then, for a copy, its offset in the base (uvarint), and for an insert, its n
//	         bytes
//
// What goes on the wire is the delta compressed (CHUNK_CODEC_DELTA); this is what it decompresses
// to. The client applies it to the base it named and verifies the result against the file's ID.
const deltaVersion = 1

// deltaWindow is the fewest bytes a copy shares with the base: shorter matches cost about what
// they save.
const deltaWindow = 16

// deltaBase is the multiplier of the rolling hash over deltaWindow bytes.
const deltaBase = 0x100000001b3

// EncodeDelta writes target as a delta against base: every run of at least deltaWindow bytes that
// base holds too is a copy, longest found first, and the rest inserts.
func EncodeDelta(base, target []byte) []byte {
	out := []byte{deltaVersion}
	out = binary.AppendUvarint(out, uint64(len(base)))
	out = binary.AppendUvarint(out, uint64(len(target)))
	insert := func(b []byte) {
		if len(b) > 0 {
			out = binary.AppendUvarint(out, uint64(len(b))<<1|1)
			out = append(out, b...)
		}
	}
	if len(base) < deltaWindow || len(target) < deltaWindow {
		insert(target)
		return out
	}
	// Every window of the base, or every fourth of a large one, by hash: a run the target shares
	// of a window and three bytes more holds an indexed window wherever it starts.
	stride := 1
	if len(base) > 1<<20 {
		stride = 4
	}
	slots := 1
	for slots < 2*(len(base)/stride+1) {
		slots <<= 1
	}
	table := make([]int32, slots)
	for i := range table {
		table[i] = -1
	}
	mask := uint64(slots - 1)
	var pow uint64 = 1 // deltaBase^(deltaWindow-1)
	for range deltaWindow - 1 {
		pow *= deltaBase
	}
	hash := func(b []byte) uint64 {
		var h uint64
		for _, c := range b[:deltaWindow] {
			h = h*deltaBase + uint64(c)
		}
		return h
	}
	// The first window of each hash is kept, so copies come from the base's start.
	for i := (len(base) - deltaWindow) / stride * stride; i >= 0; i -= stride {
		table[mix(hash(base[i:]))&mask] = int32(i)
	}
	lit, p := 0, 0
	h := hash(target)
	for {
		if c := table[mix(h)&mask]; c >= 0 && string(base[c:int(c)+deltaWindow]) == string(target[p:p+deltaWindow]) {
			start, n := int(c), deltaWindow
			for start+n < len(base) && p+n < len(target) && base[start+n] == target[p+n] {
				n++
			}
			back := 0
			for start-back > 0 && p-back > lit && base[start-back-1] == target[p-back-1] {
				back++
			}
			insert(target[lit : p-back])
			out = binary.AppendUvarint(out, uint64(n+back)<<1)
			out = binary.AppendUvarint(out, uint64(start-back))
			p += n
			lit = p
			if p+deltaWindow > len(target) {
				break
			}
			h = hash(target[p:])
			continue
		}
		if p+deltaWindow >= len(target) {
			break
		}
		h = (h-uint64(target[p])*pow)*deltaBase + uint64(target[p+deltaWindow])
		p++
	}
	insert(target[lit:])
	return out
}

// mix spreads a rolling hash's bits over the table's index.
func mix(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	return h ^ h>>33
}

// ApplyDelta writes the file a delta makes of base: it refuses a delta for a base of another
// length, one making more than limit bytes, and one whose copies reach past the base.
func ApplyDelta(base, delta []byte, limit uint64) ([]byte, error) {
	if len(delta) == 0 || delta[0] != deltaVersion {
		return nil, errors.New("delta: not a version 1 delta")
	}
	r := delta[1:]
	next := func() (uint64, error) {
		v, n := binary.Uvarint(r)
		if n <= 0 {
			return 0, errors.New("delta: truncated")
		}
		r = r[n:]
		return v, nil
	}
	baseLen, err := next()
	if err != nil {
		return nil, err
	}
	if baseLen != uint64(len(base)) {
		return nil, fmt.Errorf("delta: made against a base of %d bytes, given %d", baseLen, len(base))
	}
	length, err := next()
	if err != nil {
		return nil, err
	}
	if length > limit {
		return nil, fmt.Errorf("delta: makes %d bytes, more than %d", length, limit)
	}
	out := make([]byte, 0, length)
	for len(r) > 0 {
		tag, err := next()
		if err != nil {
			return nil, err
		}
		n := tag >> 1
		if n > length-uint64(len(out)) {
			return nil, errors.New("delta: makes more than its length")
		}
		if tag&1 == 1 {
			if n > uint64(len(r)) {
				return nil, errors.New("delta: an insert past its end")
			}
			out = append(out, r[:n]...)
			r = r[n:]
			continue
		}
		off, err := next()
		if err != nil {
			return nil, err
		}
		if off > uint64(len(base)) || n > uint64(len(base))-off {
			return nil, errors.New("delta: a copy past the base's end")
		}
		out = append(out, base[off:off+n]...)
	}
	if uint64(len(out)) != length {
		return nil, fmt.Errorf("delta: made %d bytes of %d", len(out), length)
	}
	return out, nil
}
