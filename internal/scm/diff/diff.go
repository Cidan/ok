// Package diff computes line diffs with the histogram algorithm as imara-diff implements
// it — the algorithm gix, and so jj's git backend, uses. The SCM uses it to score rename
// and copy similarity (docs/scm.md, "The client": copy records) and for Browse's
// file diffs.
//
// The histogram algorithm recursively splits both sides around the common region whose
// lines occur least often in the "before" side, falling back to Myers' algorithm where
// every common line is too frequent to be a useful anchor.
package diff

// Lines splits data into lines, each keeping its terminating '\n'. A final line without
// one is its own line.
func Lines(data []byte) [][]byte {
	var lines [][]byte
	for len(data) > 0 {
		i := 0
		for i < len(data) && data[i] != '\n' {
			i++
		}
		if i < len(data) {
			i++
		}
		lines = append(lines, data[:i])
		data = data[i:]
	}
	return lines
}

// Diff marks which lines of each side are outside the common subsequence the algorithm
// chose: removed lines of the "before" side and added lines of the "after" side. The
// unmarked lines of both sides are equal, in order.
type Diff struct {
	Removed []bool
	Added   []bool
}

// Hunk is one run of changed lines: [BeforeStart, BeforeEnd) removed and
// [AfterStart, AfterEnd) added, either of which may be empty.
type Hunk struct {
	BeforeStart, BeforeEnd int
	AfterStart, AfterEnd   int
}

// Compute diffs two sequences of lines, comparing lines by content.
func Compute(before, after [][]byte) Diff {
	tokens := map[string]int32{}
	intern := func(lines [][]byte) []int32 {
		out := make([]int32, len(lines))
		for i, l := range lines {
			t, ok := tokens[string(l)]
			if !ok {
				t = int32(len(tokens))
				tokens[string(l)] = t
			}
			out[i] = t
		}
		return out
	}
	a, b := intern(before), intern(after)
	d := Diff{Removed: make([]bool, len(a)), Added: make([]bool, len(b))}

	prefix := 0
	for prefix < len(a) && prefix < len(b) && a[prefix] == b[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(a)-prefix && suffix < len(b)-prefix && a[len(a)-1-suffix] == b[len(b)-1-suffix] {
		suffix++
	}
	h := &histogram{occurrences: make([][]int32, len(tokens))}
	h.run(a[prefix:len(a)-suffix], b[prefix:len(b)-suffix], d.Removed[prefix:len(a)-suffix], d.Added[prefix:len(b)-suffix])
	return d
}

// Hunks returns the diff's runs of changed lines in order.
func (d Diff) Hunks() []Hunk {
	var hunks []Hunk
	i, j := 0, 0
	for i < len(d.Removed) || j < len(d.Added) {
		if i < len(d.Removed) && j < len(d.Added) && !d.Removed[i] && !d.Added[j] {
			i, j = i+1, j+1
			continue
		}
		h := Hunk{BeforeStart: i, AfterStart: j}
		for i < len(d.Removed) && d.Removed[i] {
			i++
		}
		for j < len(d.Added) && d.Added[j] {
			j++
		}
		h.BeforeEnd, h.AfterEnd = i, j
		if h.BeforeEnd == h.BeforeStart && h.AfterEnd == h.AfterStart {
			// Only an inconsistent Diff (unequal unchanged counts) gets here.
			break
		}
		hunks = append(hunks, h)
	}
	return hunks
}

// maxChainLen caps the occurrences tracked per token. A token seen more often is
// recorded maxChainLen+1 times, and a region anchored only on such tokens is rejected in
// favor of Myers.
const maxChainLen = 63

type histogram struct {
	occurrences [][]int32 // per token: its positions in the current "before" side
}

func (h *histogram) populate(before []int32) {
	for i, t := range before {
		if len(h.occurrences[t]) <= maxChainLen {
			h.occurrences[t] = append(h.occurrences[t], int32(i))
		}
	}
}

func (h *histogram) clear(before []int32) {
	for _, t := range before {
		h.occurrences[t] = h.occurrences[t][:0]
	}
}

func (h *histogram) count(t int32) int { return len(h.occurrences[t]) }

func (h *histogram) run(before, after []int32, removed, added []bool) {
	for {
		if len(before) == 0 {
			fill(added)
			return
		}
		if len(after) == 0 {
			fill(removed)
			return
		}
		h.populate(before)
		lcs, ok := h.findLCS(before, after)
		h.clear(before)
		switch {
		case !ok:
			// Every common line is too frequent to anchor on: two long repetitive inputs.
			myers(before, after, removed, added)
			return
		case lcs.len == 0:
			fill(removed)
			fill(added)
			return
		}
		h.run(before[:lcs.beforeStart], after[:lcs.afterStart], removed[:lcs.beforeStart], added[:lcs.afterStart])
		be, ae := lcs.beforeStart+lcs.len, lcs.afterStart+lcs.len
		before, removed = before[be:], removed[be:]
		after, added = after[ae:], added[ae:]
	}
}

type lcs struct {
	beforeStart, afterStart, len int
}

// findLCS finds the common region to split around: the longest region, or failing that
// the one whose rarest line occurs least in "before". ok is false when common lines exist
// but all occur more than maxChainLen times.
func (h *histogram) findLCS(before, after []int32) (best lcs, ok bool) {
	minOccurrences := maxChainLen + 1
	foundCommon := false
	pos := 0
	for pos < len(after) {
		t := after[pos]
		if n := h.count(t); n != 0 {
			foundCommon = true
			if n <= minOccurrences {
				pos = h.updateLCS(pos, t, before, after, &best, &minOccurrences)
				continue
			}
		}
		pos++
	}
	return best, !foundCommon || minOccurrences <= maxChainLen
}

func (h *histogram) updateLCS(afterPos int, t int32, before, after []int32, best *lcs, minOccurrences *int) int {
	next := afterPos + 1
	occ := h.occurrences[t]
	for k := 0; k < len(occ); {
		idx := int(occ[k])
		occurrences := h.count(t)
		start1, start2 := idx, afterPos
		for start1 > 0 && start2 > 0 && before[start1-1] == after[start2-1] {
			start1, start2 = start1-1, start2-1
			occurrences = min(occurrences, h.count(before[start1]))
		}
		end1, end2 := idx+1, afterPos+1
		for end1 < len(before) && end2 < len(after) && before[end1] == after[end2] {
			occurrences = min(occurrences, h.count(before[end1]))
			end1, end2 = end1+1, end2+1
		}
		if next < end2 {
			next = end2
		}
		if n := end2 - start2; best.len < n || *minOccurrences > occurrences {
			*minOccurrences = occurrences
			*best = lcs{beforeStart: start1, afterStart: start2, len: n}
		}
		// Skip occurrences inside the region just measured (imara compares against the
		// after-side end, which this reproduces).
		for k++; k < len(occ) && int(occ[k]) <= end2; k++ {
		}
	}
	return next
}

func fill(b []bool) {
	for i := range b {
		b[i] = true
	}
}

// myers marks a minimal diff of a and b with Myers' linear-space algorithm: trim the
// common ends, split at a middle snake, recurse.
func myers(a, b []int32, removed, added []bool) {
	for len(a) > 0 && len(b) > 0 && a[0] == b[0] {
		a, b, removed, added = a[1:], b[1:], removed[1:], added[1:]
	}
	for len(a) > 0 && len(b) > 0 && a[len(a)-1] == b[len(b)-1] {
		a, b = a[:len(a)-1], b[:len(b)-1]
		removed, added = removed[:len(removed)-1], added[:len(added)-1]
	}
	if len(a) == 0 {
		fill(added)
		return
	}
	if len(b) == 0 {
		fill(removed)
		return
	}
	x, y, ok := bisect(a, b)
	if !ok {
		fill(removed)
		fill(added)
		return
	}
	myers(a[:x], b[:y], removed[:x], added[:y])
	myers(a[x:], b[y:], removed[x:], added[y:])
}

// bisect finds the middle snake of an optimal edit path from a to b and returns the
// point to split at (the bisection of diff-match-patch).
func bisect(a, b []int32) (int, int, bool) {
	n, m := len(a), len(b)
	maxD := (n + m + 1) / 2
	off := maxD
	// Diagonals span [-maxD, maxD] and are read at k±1, so the arrays need room past
	// both ends.
	size := 2*maxD + 2
	v1 := make([]int, size)
	v2 := make([]int, size)
	for i := range v1 {
		v1[i], v2[i] = -1, -1
	}
	v1[off+1], v2[off+1] = 0, 0
	delta := n - m
	front := delta%2 != 0
	k1start, k1end, k2start, k2end := 0, 0, 0, 0
	for d := 0; d < maxD; d++ {
		for k1 := -d + k1start; k1 <= d-k1end; k1 += 2 {
			k1off := off + k1
			var x1 int
			if k1 == -d || (k1 != d && v1[k1off-1] < v1[k1off+1]) {
				x1 = v1[k1off+1]
			} else {
				x1 = v1[k1off-1] + 1
			}
			y1 := x1 - k1
			for x1 < n && y1 < m && a[x1] == b[y1] {
				x1, y1 = x1+1, y1+1
			}
			v1[k1off] = x1
			switch {
			case x1 > n:
				k1end += 2
			case y1 > m:
				k1start += 2
			case front:
				k2off := off + delta - k1
				if k2off >= 0 && k2off < size && v2[k2off] != -1 {
					if x1 >= n-v2[k2off] {
						return x1, y1, true
					}
				}
			}
		}
		for k2 := -d + k2start; k2 <= d-k2end; k2 += 2 {
			k2off := off + k2
			var x2 int
			if k2 == -d || (k2 != d && v2[k2off-1] < v2[k2off+1]) {
				x2 = v2[k2off+1]
			} else {
				x2 = v2[k2off-1] + 1
			}
			y2 := x2 - k2
			for x2 < n && y2 < m && a[n-x2-1] == b[m-y2-1] {
				x2, y2 = x2+1, y2+1
			}
			v2[k2off] = x2
			switch {
			case x2 > n:
				k2end += 2
			case y2 > m:
				k2start += 2
			case !front:
				k1off := off + delta - k2
				if k1off >= 0 && k1off < size && v1[k1off] != -1 {
					x1 := v1[k1off]
					y1 := off + x1 - k1off
					if x1 >= n-x2 {
						return x1, y1, true
					}
				}
			}
		}
	}
	return 0, 0, false
}
