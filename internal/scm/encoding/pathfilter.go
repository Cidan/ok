package encoding

import (
	"encoding/binary"
	"slices"
)

// A commit's changed-path filter (docs/scm.md, "The commit graph"): a Bloom filter of the paths it
// changed from its one parent — each changed file and every directory above one, as main's path
// history records them — after git's commit-graph filters. Each path sets PathFilterProbes bits of
// PathFilterBitsPerPath times as many as there are paths, rounded up to bytes; the bits a path sets
// are (h1 + i·h2) mod the filter's bits, for i below PathFilterProbes, h1 and h2 the first two
// little-endian 32-bit words of the BLAKE3 of the path. A filter answers that a commit did not
// change a path, or that it may have:
//
//   - a commit that changed no path has the empty filter, which holds none;
//   - one that changed more than PathFilterMaxPaths has the one byte 0xff, which holds every path.

const (
	PathFilterBitsPerPath = 10
	PathFilterProbes      = 7
	PathFilterMaxPaths    = 512
)

// PathFilter is the filter of paths, each named once or more.
func PathFilter(paths []string) []byte {
	ps := slices.Clone(paths)
	slices.Sort(ps)
	ps = slices.Compact(ps)
	if len(ps) > PathFilterMaxPaths {
		return []byte{0xff}
	}
	out := make([]byte, (len(ps)*PathFilterBitsPerPath+7)/8)
	for _, p := range ps {
		for _, b := range pathFilterBits(p, len(out)) {
			out[b/8] |= 1 << (b % 8)
		}
	}
	return out
}

// PathFilterHolds reports whether filter may hold path: false is certain.
func PathFilterHolds(filter []byte, path string) bool {
	if len(filter) == 0 {
		return false
	}
	for _, b := range pathFilterBits(path, len(filter)) {
		if filter[b/8]&(1<<(b%8)) == 0 {
			return false
		}
	}
	return true
}

func pathFilterBits(path string, size int) [PathFilterProbes]uint64 {
	h := Hash([]byte(path))
	h1 := uint64(binary.LittleEndian.Uint32(h[0:4]))
	h2 := uint64(binary.LittleEndian.Uint32(h[4:8]))
	bits := uint64(size) * 8
	var out [PathFilterProbes]uint64
	for i := range out {
		out[i] = (h1 + uint64(i)*h2) % bits
	}
	return out
}
