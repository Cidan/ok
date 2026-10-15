package render

import (
	"strings"
	"testing"
)

func TestLines(t *testing.T) {
	src := "package x\n\n/* a comment\n   spanning lines */\nfunc f() {}"
	lines := Lines("x.go", []byte(src))
	if len(lines) != 5 {
		t.Fatalf("%d lines, want 5", len(lines))
	}
	if got := joined(lines[3]); got != "   spanning lines */" {
		t.Errorf("line 4 = %q", got)
	}
	// The block comment's second line is still coloured as a comment.
	if lines[3][0].Class != "cm" {
		t.Errorf("line 4's class = %q, want the multi-line comment's", lines[3][0].Class)
	}
	if got := HTML(lines[0]); !strings.Contains(got, `<span class="kn">package</span>`) {
		t.Errorf("line 1 = %q", got)
	}
	if n := len(Lines("x.txt", []byte("a\nb\n"))); n != 2 {
		t.Errorf("a trailing newline makes no extra line: %d", n)
	}
	if n := len(Lines("x.txt", nil)); n != 0 {
		t.Errorf("an empty file: %d lines", n)
	}
	if got := HTML(Lines("x.txt", []byte("<script>&"))[0]); strings.Contains(got, "<script>") {
		t.Errorf("text is not escaped: %q", got)
	}
	big := strings.Repeat("x\n", maxHighlightBytes)
	if n := len(Lines("x.go", []byte(big))); n != maxHighlightBytes {
		t.Errorf("a file too large to highlight still has every line: %d", n)
	}
	if Language("main.go", nil) != "Go" || Language("notes", []byte("")) != "" {
		t.Error("Language")
	}
}

func TestWordDiff(t *testing.T) {
	old := LineTokens("x.go", `return "hello", nil`)
	cur := LineTokens("x.go", `return "world", nil`)
	o, n, ok := WordDiff(old, cur)
	if !ok || !strings.Contains(o, `<span class="removed-code">`) || !strings.Contains(n, `<span class="added-code">`) {
		t.Fatalf("word diff: %q / %q, %v", o, n, ok)
	}
	if strings.Contains(o, "world") || strings.Contains(n, "hello") {
		t.Error("a side shows the other's text")
	}
	if _, _, ok := WordDiff(Plain("completely"), Plain("different text")); ok {
		t.Error("lines with nothing in common get a word diff")
	}
	o, n, ok = WordDiff(Plain("naïve café"), Plain("naïve cafe"))
	if !ok || !strings.Contains(o, "café") && !strings.Contains(o, "é") {
		t.Errorf("multi-byte characters: %q / %q", o, n)
	}
}

func TestMarkdown(t *testing.T) {
	links := Links{
		Dir:  "docs",
		Page: func(p string) string { return "/repo/tree/main/" + p },
		Raw:  func(p string) string { return "/repo/raw/main/" + p },
	}
	out, err := Markdown([]byte(strings.Join([]string{
		"# Title",
		"",
		"[sibling](other.md#part) [up](../README.md) [root](/x/y.go) [web](https://example.com) [anchor](#here)",
		"",
		"![logo](img/logo.png)",
		"",
		"- [x] done",
		"",
		"```go",
		"func f() {}",
		"```",
		"",
		`<script>alert(1)</script><a href="javascript:alert(1)">x</a><img src="x" onerror="alert(1)">`,
		"",
		`<div class="fixed inset-0 z-50 bg-zinc-950"><a href="https://example.com/x">Sign in</a></div>`,
		`<span class="k absolute">mixed</span><code class="language-go">x</code>`,
	}, "\n")), links)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`href="/repo/tree/main/docs/other.md#part"`,
		`href="/repo/tree/main/README.md"`,
		`href="/repo/tree/main/x/y.go"`,
		`href="https://example.com"`,
		`href="#here"`,
		`src="/repo/raw/main/docs/img/logo.png"`,
		`class="task-list-item"`,
		`type="checkbox"`,
		`<span class="kd">func</span>`,
		`<code class="language-go">x</code>`,
		`<pre class="chroma">`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s in\n%s", want, out)
		}
	}
	// The page's own stylesheet is not the document's: a class it does not render is dropped,
	// and so is a class attribute that mixes one in.
	for _, bad := range []string{"<script", "javascript:", "onerror", "fixed", "inset-0", "absolute"} {
		if strings.Contains(out, bad) {
			t.Errorf("unsanitized %s in\n%s", bad, out)
		}
	}
}
