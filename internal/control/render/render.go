// Package render turns repository contents into the HTML ok's pages show: source
// highlighted by chroma (with the class names internal/control/pages's stylesheet colours), intra-
// line word diffs merged into that highlighting, and markdown rendered by goldmark and
// sanitized by bluemonday, its relative links resolved against the repository.
package render

import (
	"html"
	"path"
	"strings"

	"github.com/alecthomas/chroma/v2"
	"github.com/alecthomas/chroma/v2/lexers"
	"github.com/sergi/go-diff/diffmatchpatch"
)

// maxHighlightBytes bounds what is tokenized as one file; larger files are shown plain.
const maxHighlightBytes = 1 << 20

// Token is a run of text with one highlight class ("" for none).
type Token struct {
	Class string
	Text  string
}

// Lexer picks the lexer for a file: by its name, else (for small files) by its contents,
// else plain text.
func Lexer(name string, data []byte) chroma.Lexer {
	if l := lexers.Match(path.Base(name)); l != nil {
		return l
	}
	if len(data) > 0 && len(data) <= 64<<10 {
		if l := lexers.Analyse(string(data)); l != nil {
			return l
		}
	}
	return lexers.Fallback
}

// Language names a file's language for display ("" for plain text).
func Language(name string, data []byte) string {
	l := Lexer(name, data)
	if l == lexers.Fallback {
		return ""
	}
	return l.Config().Name
}

// class is the CSS class of a token type: its own short name, else its nearest
// ancestor's, as chroma's HTML formatter names them.
func class(t chroma.TokenType) string {
	for {
		if c, ok := chroma.StandardTypes[t]; ok {
			return c
		}
		p := t.Parent()
		if p == t {
			return ""
		}
		t = p
	}
}

// Lines splits data into its lines' tokens, highlighted as one file so a construct
// spanning lines (a block comment, a string) is coloured throughout. There is one entry per
// line of data, each without its newline.
func Lines(name string, data []byte) [][]Token {
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	n := strings.Count(text, "\n")
	if text != "" && !strings.HasSuffix(text, "\n") {
		n++
	}
	out := make([][]Token, 0, n)
	if len(data) > maxHighlightBytes {
		for _, l := range strings.SplitAfter(text, "\n")[:n] {
			out = append(out, []Token{{Text: strings.TrimSuffix(l, "\n")}})
		}
		return out
	}
	it, err := chroma.Coalesce(Lexer(name, data)).Tokenise(&chroma.TokeniseOptions{State: "root", EnsureLF: true}, text)
	if err != nil {
		for _, l := range strings.SplitAfter(text, "\n")[:n] {
			out = append(out, []Token{{Text: strings.TrimSuffix(l, "\n")}})
		}
		return out
	}
	var line []Token
	for _, tok := range it.Tokens() {
		c := class(tok.Type)
		for {
			v, rest, nl := strings.Cut(tok.Value, "\n")
			if v != "" {
				line = append(line, Token{Class: c, Text: v})
			}
			if !nl {
				break
			}
			out = append(out, line)
			line = nil
			tok.Value = rest
		}
	}
	if len(line) > 0 {
		out = append(out, line)
	}
	for len(out) < n {
		out = append(out, nil)
	}
	return out[:n]
}

// LineTokens highlights one line on its own, for a line whose file is not at hand.
func LineTokens(name, line string) []Token {
	ls := Lines(name, []byte(line))
	if len(ls) == 0 {
		return nil
	}
	return ls[0]
}

// HTML renders tokens as escaped, classed spans.
func HTML(tokens []Token) string {
	var b strings.Builder
	for _, t := range tokens {
		span(&b, t.Class, "", t.Text)
	}
	return b.String()
}

func span(b *strings.Builder, class, mark, text string) {
	if text == "" {
		return
	}
	if mark != "" {
		b.WriteString(`<span class="` + mark + `">`)
	}
	if class != "" {
		b.WriteString(`<span class="` + class + `">`)
	}
	b.WriteString(html.EscapeString(text))
	if class != "" {
		b.WriteString(`</span>`)
	}
	if mark != "" {
		b.WriteString(`</span>`)
	}
}

// Plain is text as its line tokens, unhighlighted.
func Plain(text string) []Token { return []Token{{Text: text}} }

// joined is the tokens' text.
func joined(tokens []Token) string {
	var b strings.Builder
	for _, t := range tokens {
		b.WriteString(t.Text)
	}
	return b.String()
}

// maxWordDiffShare is how much of a line may change for a word diff to be worth showing;
// beyond it the whole line is simply removed and added.
const maxWordDiffShare = 0.6

// WordDiff renders a removed line and the line that replaced it with the changed runs of
// each marked (removed-code, added-code) over their highlighting. ok is false when the
// lines differ too much for marks to help; the caller then renders them plain.
func WordDiff(removed, added []Token) (oldHTML, newHTML string, ok bool) {
	a, b := joined(removed), joined(added)
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffCleanupSemantic(dmp.DiffMain(a, b, false))
	changed := 0
	for _, d := range diffs {
		if d.Type != diffmatchpatch.DiffEqual {
			changed += len(d.Text)
		}
	}
	if longer := max(len(a), len(b)); longer == 0 || float64(changed) > maxWordDiffShare*float64(longer) {
		return "", "", false
	}
	oldMarks := make([]bool, len(a))
	newMarks := make([]bool, len(b))
	i, j := 0, 0
	for _, d := range diffs {
		switch d.Type {
		case diffmatchpatch.DiffEqual:
			i += len(d.Text)
			j += len(d.Text)
		case diffmatchpatch.DiffDelete:
			for k := range len(d.Text) {
				oldMarks[i+k] = true
			}
			i += len(d.Text)
		case diffmatchpatch.DiffInsert:
			for k := range len(d.Text) {
				newMarks[j+k] = true
			}
			j += len(d.Text)
		}
	}
	return marked(removed, oldMarks, "removed-code"), marked(added, newMarks, "added-code"), true
}

// marked renders tokens with the bytes marks sets wrapped in a span of class mark.
func marked(tokens []Token, marks []bool, mark string) string {
	var b strings.Builder
	pos := 0
	for _, t := range tokens {
		start := 0
		for start < len(t.Text) {
			m := marks[pos+start]
			end := start
			for end < len(t.Text) && marks[pos+end] == m {
				end++
			}
			// Never split a multi-byte character: extend to its end.
			for end < len(t.Text) && !isRuneStart(t.Text[end]) {
				end++
			}
			cls := ""
			if m {
				cls = mark
			}
			span(&b, t.Class, cls, t.Text[start:end])
			start = end
		}
		pos += len(t.Text)
	}
	return b.String()
}

func isRuneStart(c byte) bool { return c&0xC0 != 0x80 }
