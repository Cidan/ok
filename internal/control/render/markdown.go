package render

import (
	"bytes"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/alecthomas/chroma/v2"
	chromahtml "github.com/alecthomas/chroma/v2/formatters/html"
	"github.com/microcosm-cc/bluemonday"
	"github.com/yuin/goldmark"
	highlighting "github.com/yuin/goldmark-highlighting/v2"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	east "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/renderer/html"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

// Links resolves a markdown document's relative links against the repository it is in:
// Page is where a link to a repository path goes, Raw where an image's bytes are.
type Links struct {
	Dir  string // the document's directory in the repository ("" for the root)
	Page func(p string) string
	Raw  func(p string) string
}

// resolve maps a link destination to a repository path, or reports it is not one (an
// absolute URL, a scheme, or an anchor), keeping its query and fragment.
func (l Links) resolve(dest string, raw bool) (string, bool) {
	if dest == "" || strings.HasPrefix(dest, "#") || strings.HasPrefix(dest, "//") {
		return "", false
	}
	u, err := url.Parse(dest)
	if err != nil || u.Scheme != "" || u.Host != "" || u.Path == "" {
		return "", false
	}
	p := u.Path
	if strings.HasPrefix(p, "/") {
		p = path.Clean(p)
	} else {
		p = path.Clean("/" + path.Join(l.Dir, p))
	}
	p = strings.TrimPrefix(p, "/")
	var out string
	if raw {
		out = l.Raw(p)
	} else {
		out = l.Page(p)
	}
	if u.RawQuery != "" {
		out += "?" + u.RawQuery
	}
	if u.Fragment != "" {
		out += "#" + u.EscapedFragment()
	}
	return out, true
}

// transformer rewrites relative links and images to repository URLs, and marks task list
// items for the stylesheet.
type transformer struct{ links Links }

func (t transformer) Transform(doc *ast.Document, _ text.Reader, _ parser.Context) {
	_ = ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch v := n.(type) {
		case *ast.Link:
			if dest, ok := t.links.resolve(string(v.Destination), false); ok {
				v.Destination = []byte(dest)
			}
		case *ast.Image:
			if dest, ok := t.links.resolve(string(v.Destination), true); ok {
				v.Destination = []byte(dest)
			}
		case *east.TaskCheckBox:
			for p := n.Parent(); p != nil; p = p.Parent() {
				if li, ok := p.(*ast.ListItem); ok {
					li.SetAttributeString("class", []byte("task-list-item"))
					break
				}
			}
		}
		return ast.WalkContinue, nil
	})
}

// markupClasses matches a class attribute rendered markdown may keep: the highlighter's classes
// (chroma's), a fenced block's language, and task lists'. Nothing else: the page's own
// stylesheet would otherwise be the document's — a README's div of "fixed inset-0 z-50" laid
// over the page, a sign-in prompt in it.
var markupClasses = func() *regexp.Regexp {
	names := []string{"task-list-item", `language-[A-Za-z0-9_+#.-]+`}
	for _, c := range chroma.StandardTypes {
		if c != "" {
			names = append(names, regexp.QuoteMeta(c))
		}
	}
	sort.Strings(names)
	one := "(?:" + strings.Join(names, "|") + ")"
	return regexp.MustCompile(`^` + one + `(?: ` + one + `)*$`)
}()

// policy sanitizes rendered markdown: user-generated content's elements and attributes,
// the classes highlighting and task lists use, and task-list checkboxes.
var policy = func() *bluemonday.Policy {
	p := bluemonday.UGCPolicy()
	p.AllowAttrs("class").Matching(markupClasses).OnElements("span", "code", "pre", "div", "li")
	p.AllowAttrs("type").Matching(regexp.MustCompile(`^checkbox$`)).OnElements("input")
	p.AllowAttrs("checked", "disabled").OnElements("input")
	p.AllowElements("details", "summary", "kbd", "sup", "sub")
	return p
}()

// Markdown renders a markdown document as sanitized HTML. Raw HTML in it passes the
// sanitizer's allow-list; fenced code is highlighted with the stylesheet's classes.
func Markdown(src []byte, links Links) (string, error) {
	md := goldmark.New(
		goldmark.WithExtensions(
			extension.GFM,
			highlighting.NewHighlighting(highlighting.WithFormatOptions(chromahtml.WithClasses(true))),
		),
		goldmark.WithParserOptions(parser.WithASTTransformers(util.Prioritized(transformer{links: links}, 100))),
		goldmark.WithRendererOptions(html.WithUnsafe()),
	)
	var buf bytes.Buffer
	if err := md.Convert(src, &buf); err != nil {
		return "", err
	}
	return policy.Sanitize(buf.String()), nil
}

// IsMarkdown reports whether a file is rendered as markdown.
func IsMarkdown(name string) bool {
	switch strings.ToLower(path.Ext(name)) {
	case ".md", ".markdown", ".mdown", ".mkd":
		return true
	}
	return false
}

// imageTypes are the image formats a file view shows, by extension.
var imageTypes = map[string]string{
	".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif",
	".webp": "image/webp", ".avif": "image/avif", ".bmp": "image/bmp", ".ico": "image/x-icon",
	".svg": "image/svg+xml",
}

// ImageType is a file's image MIME type by extension, or "".
func ImageType(name string) string { return imageTypes[strings.ToLower(path.Ext(name))] }

// IsBinary reports whether contents are binary: a NUL byte in the first 8,000 bytes, the
// SCM's (and git's) test.
func IsBinary(b []byte) bool { return bytes.IndexByte(b[:min(len(b), 8000)], 0) >= 0 }
