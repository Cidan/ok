package codesearch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math/rand/v2"
	"regexp"
	"regexp/syntax"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type corpus map[string]string

// index indexes a corpus, returning it and its reader.
func (c corpus) index(t *testing.T) (*Index, Reader) {
	t.Helper()
	b := NewBuilder([32]byte{7})
	byID := map[[32]byte][]byte{}
	paths := make([]string, 0, len(c))
	for p := range c {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	for _, p := range paths {
		id := sha256.Sum256([]byte(c[p]))
		byID[id] = []byte(c[p])
		b.Add(File{Path: p, ID: id, Size: uint64(len(c[p]))}, []byte(c[p]))
	}
	var buf bytes.Buffer
	if err := b.Index().Encode(&buf); err != nil {
		t.Fatal(err)
	}
	ix, err := Decode(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return ix, func(_ context.Context, ids [][32]byte) (map[[32]byte][]byte, error) {
		out := map[[32]byte][]byte{}
		for _, id := range ids {
			out[id] = byID[id]
		}
		return out, nil
	}
}

func lines(r Result) []string {
	var out []string
	for _, m := range r.Matches {
		out = append(out, m.Path+":"+strconv.Itoa(m.Line)+":"+m.Text[m.Start:m.End])
	}
	return out
}

// Literals and regular expressions, in any case or only their own, within a path; a binary file
// and a file past MaxFile not searched; the limit.
func TestSearch(t *testing.T) {
	c := corpus{
		"api/search.go":    "package api\n\nfunc Search(q string) {\n\trateLimit(q)\n}\n",
		"api/limit.go":     "package api\n\n// RateLimit limits.\nfunc rateLimit(q string) {}\n",
		"web/index.html":   "<h1>Search</h1>\n",
		"bin/blob":         "Search\x00binary",
		"docs/big.txt":     strings.Repeat("Search ", MaxFile/6),
		"docs/unicode.txt": "Ünïcode Straße\n",
	}
	ix, read := c.index(t)
	if len(ix.Files) != 4 {
		t.Errorf("%d files indexed, want the 4 text files under MaxFile", len(ix.Files))
	}
	for _, tc := range []struct {
		q    Query
		want []string
	}{
		{Query{Pattern: "ratelimit"}, []string{"api/limit.go:3:RateLimit", "api/limit.go:4:rateLimit", "api/search.go:4:rateLimit"}},
		{Query{Pattern: "rateLimit", CaseSensitive: true}, []string{"api/limit.go:4:rateLimit", "api/search.go:4:rateLimit"}},
		{Query{Pattern: `func \w+\(q`, Regexp: true}, []string{"api/limit.go:4:func rateLimit(q", "api/search.go:3:func Search(q"}},
		{Query{Pattern: "search", Path: "*.html"}, []string{"web/index.html:1:Search"}},
		{Query{Pattern: "search", Path: "api/"}, []string{"api/search.go:3:Search"}},
		{Query{Pattern: "(Search|RateLimit)\\b", Regexp: true, CaseSensitive: true}, []string{"api/limit.go:3:RateLimit", "api/search.go:3:Search",
			"web/index.html:1:Search"}},
		{Query{Pattern: "STRASSE"}, nil},
		{Query{Pattern: "straße"}, []string{"docs/unicode.txt:1:Straße"}},
		{Query{Pattern: "ÜNÏ"}, []string{"docs/unicode.txt:1:Ünï"}},
		{Query{Pattern: "nothing like it"}, nil},
	} {
		r, err := ix.Search(context.Background(), tc.q, read)
		if err != nil {
			t.Fatalf("%+v: %v", tc.q, err)
		}
		if got := lines(r); !slices.Equal(got, tc.want) {
			t.Errorf("%+v: %v, want %v", tc.q, got, tc.want)
		}
	}
	r, err := ix.Search(context.Background(), Query{Pattern: "a", Limit: 2}, read)
	if err != nil || len(r.Matches) != 2 || !r.Truncated {
		t.Errorf("a search of limit 2: %+v, %v", r, err)
	}
	if _, err := ix.Search(context.Background(), Query{Pattern: "(", Regexp: true}, read); err == nil {
		t.Error("a regular expression that does not parse")
	}
	if _, err := Decode([]byte("okcs\x01short")); err == nil {
		t.Error("a truncated index decoded")
	}
}

// A regular expression's plan requires the trigrams its literals hold, and nothing of what may be
// absent.
func TestPlan(t *testing.T) {
	for expr, want := range map[string]string{
		"hello":         "and([hel ell llo])",
		"he(llo|lp)":    "all",        // the parser factors it to he(l(lo|p)): no piece of three
		"foo(bar|baz)":  "and([foo])", // factored to foo(ba[rz]): the capture keeps ba apart
		"ab":            "all",
		"x*":            "all",
		"(abc)+def":     "and([abc def])",
		"abc|de":        "all",
		"abc|def":       "or([and([abc]) and([def])])",
		"a.c":           "all",
		"foo[0-9]+bar":  "and([foo bar])",
		"(?:abcd){2,3}": "and([abc bcd cda dab])", // simplified to abcdabcd(abcd)?
	} {
		re, err := syntax.Parse(expr, syntax.Perl)
		if err != nil {
			t.Fatal(err)
		}
		if got := plan(re.Simplify()).String(); got != want {
			t.Errorf("plan(%q) = %s, want %s", expr, got, want)
		}
	}
}

// Over random files and queries, the trigrams never drop a file a match is in: what the index
// finds is what reading every file finds.
func TestNoMissedMatch(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	alphabet := "abcAB \n_"
	word := func(n int) string {
		var b strings.Builder
		for range n {
			b.WriteByte(alphabet[rng.IntN(len(alphabet))])
		}
		return b.String()
	}
	c := corpus{}
	for i := range 60 {
		c["f"+string(rune('a'+i%26))+word(3)] = word(200)
	}
	ix, read := c.index(t)
	exprs := []string{"abc", "aBc", "ab+c", "(ab|ba)c", "a.b", "cab*a", "[ab]{3}", "a\\sb", "bca|acb", "(?i)AbA", "b(a|c)+b", "^ab", "ca$"}
	for range 40 {
		exprs = append(exprs, regexp.QuoteMeta(word(3+rng.IntN(3))))
	}
	for _, expr := range exprs {
		for _, cs := range []bool{true, false} {
			q := Query{Pattern: expr, Regexp: true, CaseSensitive: cs, Limit: 1 << 30}
			got, err := ix.Search(context.Background(), q, read)
			if err != nil {
				t.Fatal(err)
			}
			re, _, _ := q.compile()
			var want []Match
			paths := make([]string, 0, len(c))
			for p := range c {
				paths = append(paths, p)
			}
			slices.Sort(paths)
			for _, p := range paths {
				match(re, p, []byte(c[p]), 1<<30, &want)
			}
			if len(got.Matches) != len(want) {
				t.Errorf("%q (case-sensitive %v): %d matches through the index, %d reading every file", expr, cs, len(got.Matches), len(want))
			}
		}
	}
}
