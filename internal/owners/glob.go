package owners

import (
	"errors"
	"regexp"
	"strings"
)

// matcher is a compiled glob.
type matcher struct {
	re *regexp.Regexp
}

func (m matcher) match(rel string) bool { return m.re.MatchString(rel) }

// compileGlob compiles a per-file glob, matched against a path relative to the governing OWNERS
// file's directory. A glob with "/" matches the whole path; one without matches from any
// directory down (*.go is a.go and sub/a.go). * is anything but "/", ** is anything at all, ? is
// one character but "/", [a-z] and [!a-z] (or [^a-z]) are a character and its complement — a ]
// first in the class is its own — and {a,b} is either of its alternatives, which may be empty but
// not hold another {. \ makes the next character itself, and a } outside braces is itself.
func compileGlob(g string) (matcher, error) {
	var b strings.Builder
	b.WriteString("^")
	if !strings.Contains(g, "/") {
		b.WriteString("(?:.*/)?")
	}
	inBraces := false
	for i := 0; i < len(g); {
		c := g[i]
		switch {
		case c == '*' && strings.HasPrefix(g[i:], "**"):
			b.WriteString(".*")
			i += 2
		case c == '*':
			b.WriteString("[^/]*")
			i++
		case c == '?':
			b.WriteString("[^/]")
			i++
		case c == '\\':
			if i+1 == len(g) {
				return matcher{}, errors.New(`a \ ends the glob`)
			}
			b.WriteString(regexp.QuoteMeta(g[i+1 : i+2]))
			i += 2
		case c == '[':
			n, err := class(g[i:], &b)
			if err != nil {
				return matcher{}, err
			}
			i += n
		case c == '{':
			if inBraces {
				return matcher{}, errors.New("a { inside braces")
			}
			inBraces = true
			b.WriteString("(?:")
			i++
		case c == ',' && inBraces:
			b.WriteString("|")
			i++
		case c == '}' && inBraces:
			inBraces = false
			b.WriteString(")")
			i++
		default:
			b.WriteString(regexp.QuoteMeta(g[i : i+1]))
			i++
		}
	}
	if inBraces {
		return matcher{}, errors.New("an unmatched {")
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return matcher{}, err
	}
	return matcher{re: re}, nil
}

// class writes the character class g begins with, and returns how much of g it is.
func class(g string, b *strings.Builder) (int, error) {
	i := 1
	negate := i < len(g) && (g[i] == '!' || g[i] == '^')
	if negate {
		i++
	}
	start := i
	if i < len(g) && g[i] == ']' {
		i++ // a ] first is the class's own
	}
	for i < len(g) && g[i] != ']' {
		i++
	}
	if i == len(g) {
		return 0, errors.New("an unmatched [")
	}
	b.WriteString("[")
	if negate {
		b.WriteString("^/")
	}
	for _, r := range g[start:i] {
		if r == '\\' || r == '[' || r == ']' || r == '^' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteString("]")
	return i + 1, nil
}
