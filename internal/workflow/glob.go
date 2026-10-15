package workflow

import (
	"errors"
	"regexp"
	"strings"
)

// compileGlob compiles a placeholder's glob into an anchored regexp over a whole
// repository-relative path. `*` is any run of non-`/` characters and `?` one; `**` is any run
// including `/`, and `**/` also matches no directory at all, so `**/*.go` matches `a.go` as
// well as `x/y/a.go`, and `apps/**` matches everything under `apps/`. `[abc]`, `[a-c]`, and
// `[!a]` are character classes, `{a,b}` is alternation (not nested), and `\` escapes the next
// character.
func compileGlob(glob string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	rs := []rune(glob)
	inBrace := false
	for i := 0; i < len(rs); i++ {
		switch c := rs[i]; c {
		case '*':
			if i+1 < len(rs) && rs[i+1] == '*' {
				switch {
				case i+2 < len(rs) && rs[i+2] == '/':
					b.WriteString("(?:.*/)?")
					i += 2
				default:
					b.WriteString(".*")
					i++
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '[':
			end := i + 1
			if end < len(rs) && rs[end] == '!' {
				end++
			}
			if end < len(rs) && rs[end] == ']' {
				end++
			}
			for end < len(rs) && rs[end] != ']' {
				end++
			}
			if end >= len(rs) {
				return nil, errors.New("a [ opens a character class that is never closed")
			}
			class := string(rs[i+1 : end])
			b.WriteString("[")
			if strings.HasPrefix(class, "!") {
				b.WriteString("^/")
				class = class[1:]
			}
			b.WriteString(strings.ReplaceAll(class, `\`, `\\`))
			b.WriteString("]")
			i = end
		case '{':
			if inBrace {
				return nil, errors.New("alternations {a,b} do not nest")
			}
			inBrace = true
			b.WriteString("(?:")
		case '}':
			if !inBrace {
				return nil, errors.New("a } closes no alternation")
			}
			inBrace = false
			b.WriteString(")")
		case ',':
			if inBrace {
				b.WriteString("|")
			} else {
				b.WriteString(",")
			}
		case '\\':
			if i+1 >= len(rs) {
				return nil, errors.New(`a \ at the end escapes nothing`)
			}
			i++
			b.WriteString(regexp.QuoteMeta(string(rs[i])))
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	if inBrace {
		return nil, errors.New("a { opens an alternation that is never closed")
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}
