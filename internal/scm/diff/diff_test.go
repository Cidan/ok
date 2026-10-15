package diff

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
)

func lines(s string) [][]byte { return Lines([]byte(s)) }

func TestLines(t *testing.T) {
	for in, want := range map[string][]string{
		"":         nil,
		"a":        {"a"},
		"a\n":      {"a\n"},
		"a\nb":     {"a\n", "b"},
		"a\n\nb\n": {"a\n", "\n", "b\n"},
		"\n\n":     {"\n", "\n"},
	} {
		var got []string
		for _, l := range Lines([]byte(in)) {
			got = append(got, string(l))
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Lines(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestComputeKnown(t *testing.T) {
	for _, tc := range []struct {
		before, after string
		hunks         []Hunk
	}{
		{"a\nb\nc\n", "a\nb\nc\n", nil},
		{"a\nb\nc\n", "a\nx\nc\n", []Hunk{{1, 2, 1, 2}}},
		{"a\nb\nc\n", "a\nc\n", []Hunk{{1, 2, 1, 1}}},
		{"a\nc\n", "a\nb\nc\n", []Hunk{{1, 1, 1, 2}}},
		{"", "a\nb\n", []Hunk{{0, 0, 0, 2}}},
		{"a\nb\n", "", []Hunk{{0, 2, 0, 0}}},
		{"a\nb\n", "c\nd\n", []Hunk{{0, 2, 0, 2}}},
		// The common prefix and suffix are stripped first, so the removal lands between
		// them, as imara-diff places it.
		{"{\nfoo\n}\n{\nbar\n}\n", "{\nbar\n}\n", []Hunk{{1, 4, 1, 1}}},
		// The histogram anchors on the rarest common line.
		{"x\na\nx\nb\nx\n", "y\nb\ny\n", []Hunk{{0, 3, 0, 1}, {4, 5, 2, 3}}},
	} {
		d := Compute(lines(tc.before), lines(tc.after))
		if got := d.Hunks(); !reflect.DeepEqual(got, tc.hunks) {
			t.Errorf("Compute(%q, %q) hunks = %v, want %v", tc.before, tc.after, got, tc.hunks)
		}
	}
}

// checkConsistent verifies d's unmarked lines are equal on both sides, in order.
func checkConsistent(t *testing.T, before, after [][]byte, d Diff) {
	t.Helper()
	var keptA, keptB [][]byte
	for i, l := range before {
		if !d.Removed[i] {
			keptA = append(keptA, l)
		}
	}
	for j, l := range after {
		if !d.Added[j] {
			keptB = append(keptB, l)
		}
	}
	if len(keptA) != len(keptB) {
		t.Fatalf("common subsequence lengths differ: %d vs %d", len(keptA), len(keptB))
	}
	for i := range keptA {
		if !bytes.Equal(keptA[i], keptB[i]) {
			t.Fatalf("common subsequence differs at %d: %q vs %q", i, keptA[i], keptB[i])
		}
	}
	// Hunks cover exactly the marked lines.
	var removed, added int
	for _, h := range d.Hunks() {
		removed += h.BeforeEnd - h.BeforeStart
		added += h.AfterEnd - h.AfterStart
	}
	if removed != len(before)-len(keptA) || added != len(after)-len(keptB) {
		t.Fatalf("hunks cover %d/%d lines, marks cover %d/%d", removed, added, len(before)-len(keptA), len(after)-len(keptB))
	}
}

func randomLines(r *rand.Rand, n, alphabet int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte(fmt.Sprintf("line %d\n", r.IntN(alphabet)))
	}
	return out
}

func mutate(r *rand.Rand, in [][]byte, alphabet int) [][]byte {
	out := append([][]byte(nil), in...)
	for range r.IntN(8) {
		switch op := r.IntN(3); {
		case op == 0 && len(out) > 0:
			i := r.IntN(len(out))
			out = append(out[:i], out[i+1:]...)
		case op == 1:
			i := r.IntN(len(out) + 1)
			out = append(out[:i], append([][]byte{[]byte(fmt.Sprintf("line %d\n", r.IntN(alphabet)))}, out[i:]...)...)
		case len(out) > 0:
			out[r.IntN(len(out))] = []byte(fmt.Sprintf("new %d\n", r.IntN(alphabet)))
		}
	}
	return out
}

func TestComputeConsistent(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 2000 {
		alphabet := []int{3, 20, 1000}[i%3]
		before := randomLines(r, r.IntN(60), alphabet)
		after := mutate(r, before, alphabet)
		if i%7 == 0 {
			after = randomLines(r, r.IntN(60), alphabet)
		}
		checkConsistent(t, before, after, Compute(before, after))
	}
}

// Every line repeats far past maxChainLen, forcing the Myers fallback.
func TestComputeRepetitiveFallsBackToMyers(t *testing.T) {
	before := lines(strings.Repeat("x\ny\n", 200))
	after := lines(strings.Repeat("x\ny\n", 100) + "z\n" + strings.Repeat("x\ny\n", 100))
	d := Compute(before, after)
	checkConsistent(t, before, after, d)
	if got := d.Hunks(); !reflect.DeepEqual(got, []Hunk{{200, 200, 200, 201}}) {
		t.Fatalf("hunks = %v", got)
	}
}

// lcsLen is the exact longest common subsequence length, by dynamic programming.
func lcsLen(a, b []int32) int {
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			switch {
			case a[i-1] == b[j-1]:
				cur[j] = prev[j-1] + 1
			case prev[j] >= cur[j-1]:
				cur[j] = prev[j]
			default:
				cur[j] = cur[j-1]
			}
		}
		prev, cur = cur, prev
	}
	return prev[len(b)]
}

func TestMyersIsMinimal(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for range 3000 {
		a := make([]int32, r.IntN(30))
		b := make([]int32, r.IntN(30))
		for i := range a {
			a[i] = int32(r.IntN(4))
		}
		for i := range b {
			b[i] = int32(r.IntN(4))
		}
		removed, added := make([]bool, len(a)), make([]bool, len(b))
		myers(a, b, removed, added)
		kept := 0
		var ka, kb []int32
		for i, x := range a {
			if !removed[i] {
				kept++
				ka = append(ka, x)
			}
		}
		for j, x := range b {
			if !added[j] {
				kb = append(kb, x)
			}
		}
		if !reflect.DeepEqual(ka, kb) {
			t.Fatalf("myers(%v, %v) kept %v vs %v", a, b, ka, kb)
		}
		if want := lcsLen(a, b); kept != want {
			t.Fatalf("myers(%v, %v) kept %d lines, the LCS is %d", a, b, kept, want)
		}
	}
}
