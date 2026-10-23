package codesearch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"regexp/syntax"
	"strings"
	"unicode"
	"unicode/utf8"
)

// query is what files a match must be in: all, none, every trigram and sub-query (and), or any
// sub-query (or).
type query struct {
	op       int
	trigrams []uint32
	sub      []*query
}

const (
	qAll = iota
	qNone
	qAnd
	qOr
)

var all = &query{op: qAll}

// indexable reports whether a literal's trigrams are a match's: not so of one that folds case and
// holds a letter beyond ASCII with other cases, which the index does not lowercase.
func indexable(re *syntax.Regexp) bool {
	if re.Flags&syntax.FoldCase == 0 {
		return true
	}
	for _, r := range re.Rune {
		if r >= utf8.RuneSelf && unicode.SimpleFold(r) != r {
			return false
		}
	}
	return true
}

// literal is the query of text a match holds: each of its trigrams.
func literal(s string) *query {
	if len(s) < 3 {
		return all
	}
	q := &query{op: qAnd}
	seen := map[uint32]bool{}
	for i := 0; i+2 < len(s); i++ {
		t := trigram(s[i], s[i+1], s[i+2])
		if !seen[t] {
			seen[t] = true
			q.trigrams = append(q.trigrams, t)
		}
	}
	return q
}

func and(qs ...*query) *query {
	out := &query{op: qAnd}
	for _, q := range qs {
		switch q.op {
		case qAll:
		case qNone:
			return &query{op: qNone}
		case qAnd:
			out.trigrams = append(out.trigrams, q.trigrams...)
			out.sub = append(out.sub, q.sub...)
		default:
			out.sub = append(out.sub, q)
		}
	}
	if len(out.trigrams) == 0 && len(out.sub) == 0 {
		return all
	}
	return out
}

func or(qs ...*query) *query {
	out := &query{op: qOr}
	for _, q := range qs {
		switch q.op {
		case qAll:
			return all
		case qNone:
		default:
			out.sub = append(out.sub, q)
		}
	}
	switch len(out.sub) {
	case 0:
		return &query{op: qNone}
	case 1:
		return out.sub[0]
	}
	return out
}

// plan is the query of the files a regular expression can match in: a superset of them. Literal
// runs are joined across concatenation, so their trigrams span what they hold together.
func plan(re *syntax.Regexp) *query {
	switch re.Op {
	case syntax.OpNoMatch:
		return &query{op: qNone}
	case syntax.OpLiteral:
		if !indexable(re) {
			return all
		}
		return literal(string(re.Rune))
	case syntax.OpCapture:
		return plan(re.Sub[0])
	case syntax.OpPlus:
		return plan(re.Sub[0])
	case syntax.OpRepeat:
		if re.Min >= 1 {
			return plan(re.Sub[0])
		}
		return all
	case syntax.OpConcat:
		var qs []*query
		var run strings.Builder
		flush := func() {
			if run.Len() > 0 {
				qs = append(qs, literal(run.String()))
				run.Reset()
			}
		}
		for _, s := range re.Sub {
			if s.Op == syntax.OpLiteral && indexable(s) {
				run.WriteString(string(s.Rune))
				continue
			}
			flush()
			qs = append(qs, plan(s))
		}
		flush()
		return and(qs...)
	case syntax.OpAlternate:
		qs := make([]*query, 0, len(re.Sub))
		for _, s := range re.Sub {
			qs = append(qs, plan(s))
		}
		return or(qs...)
	}
	// Character classes, any character, empty strings, stars and quests: anything.
	return all
}

// Query is a search of a commit's files.
type Query struct {
	// Pattern is the text to find: literal, unless Regexp.
	Pattern string
	Regexp  bool
	// CaseSensitive: letters match only themselves.
	CaseSensitive bool
	// Path, when set, is a glob a file's path matches (path.Match), or a directory it is under
	// (a pattern ending in /).
	Path string
	// Limit bounds the lines found: 100 unless set.
	Limit int
}

// Match is a line found.
type Match struct {
	Path string
	Line int // 1-based
	Text string
	// Start and End are the match's bytes within Text.
	Start, End int
}

// Result is a search's answer.
type Result struct {
	Matches []Match
	// Candidates are the files the trigrams left to read; Truncated, that Limit was reached.
	Candidates int
	Truncated  bool
}

// ErrQuery is a query that is not one.
var ErrQuery = errors.New("codesearch")

// maxLine is the most of a line a match carries.
const maxLine = 500

// compile is a query's matcher and plan.
func (q Query) compile() (*regexp.Regexp, *query, error) {
	if q.Pattern == "" {
		return nil, nil, fmt.Errorf("%w: a search names what to find", ErrQuery)
	}
	expr := q.Pattern
	if !q.Regexp {
		expr = regexp.QuoteMeta(expr)
	}
	flags := syntax.Perl
	if !q.CaseSensitive {
		flags |= syntax.FoldCase
	}
	parsed, err := syntax.Parse(expr, flags)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrQuery, err)
	}
	prefix := ""
	if !q.CaseSensitive {
		prefix = "(?i)"
	}
	re, err := regexp.Compile(prefix + expr)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrQuery, err)
	}
	return re, plan(parsed.Simplify()), nil
}

// inPath reports whether a file's path is within a query's.
func (q Query) inPath(p string) bool {
	switch {
	case q.Path == "":
		return true
	case strings.HasSuffix(q.Path, "/"):
		return strings.HasPrefix(p, q.Path)
	}
	if ok, _ := path.Match(q.Path, p); ok {
		return true
	}
	ok, _ := path.Match(q.Path, path.Base(p))
	return ok
}

// Reader reads files' contents by ID, as they were indexed.
type Reader func(ctx context.Context, ids [][32]byte) (map[[32]byte][]byte, error)

// readBatch is how many candidate files are read at once.
const readBatch = 64

// Search answers a query over an index, reading candidates' files through read.
func (ix *Index) Search(ctx context.Context, q Query, read Reader) (Result, error) {
	re, pl, err := q.compile()
	if err != nil {
		return Result{}, err
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	var files []File
	for _, i := range ix.candidates(pl) {
		if f := ix.Files[i]; q.inPath(f.Path) {
			files = append(files, f)
		}
	}
	res := Result{Candidates: len(files)}
	for start := 0; start < len(files); start += readBatch {
		batch := files[start:min(start+readBatch, len(files))]
		ids := make([][32]byte, 0, len(batch))
		for _, f := range batch {
			ids = append(ids, f.ID)
		}
		data, err := read(ctx, ids)
		if err != nil {
			return Result{}, err
		}
		for _, f := range batch {
			if found := match(re, f.Path, data[f.ID], limit-len(res.Matches), &res.Matches); found {
				res.Truncated = true
				return res, nil
			}
		}
	}
	return res, nil
}

// match appends a file's matching lines, up to left of them, and reports whether it ran out.
func match(re *regexp.Regexp, p string, data []byte, left int, out *[]Match) bool {
	line := 0
	for len(data) > 0 {
		line++
		end := bytes.IndexByte(data, '\n')
		var text []byte
		if end < 0 {
			text, data = data, nil
		} else {
			text, data = data[:end], data[end+1:]
		}
		loc := re.FindIndex(text)
		if loc == nil {
			continue
		}
		if left == 0 {
			return true
		}
		left--
		shown := text
		if len(shown) > maxLine {
			cut := maxLine
			for cut > 0 && !utf8.RuneStart(shown[cut]) {
				cut--
			}
			shown = shown[:cut]
		}
		m := Match{Path: p, Line: line, Text: string(shown), Start: loc[0], End: loc[1]}
		if m.End > len(m.Text) {
			m.Start, m.End = min(m.Start, len(m.Text)), len(m.Text)
		}
		*out = append(*out, m)
	}
	return false
}
