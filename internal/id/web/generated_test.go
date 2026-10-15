package web

import (
	"bytes"
	"go/format"
	"os"
	"testing"

	"github.com/a-h/templ"
	"github.com/a-h/templ/generator"
	parser "github.com/a-h/templ/parser/v2"
)

// The committed Go is what templ generates from views.templ: regenerate it with
// go tool templ generate -path internal/id/web.
func TestGeneratedCurrent(t *testing.T) {
	tf, err := parser.Parse("views.templ")
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if _, err := generator.Generate(tf, &b, generator.WithVersion(templ.Version()), generator.WithFileName("views.templ")); err != nil {
		t.Fatal(err)
	}
	want, err := format.Source(b.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("views_templ.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Error("views_templ.go is not what views.templ generates: run go tool templ generate -path internal/id/web")
	}
}
