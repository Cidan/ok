package encoding

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"
)

//go:embed testdata/path_filter.json
var pathFilterJSON []byte

type pathFilterVectors struct {
	Comment []string `json:"comment"`
	Cases   []struct {
		Name    string   `json:"name"`
		Paths   []string `json:"paths"`
		Filter  hexLines `json:"filter"`
		Queries []struct {
			Path  string `json:"path"`
			Holds bool   `json:"holds"`
		} `json:"queries"`
	} `json:"cases"`
}

// Each case's paths make exactly its filter, which answers each query as the case says.
func TestPathFilterVectors(t *testing.T) {
	var v pathFilterVectors
	dec := json.NewDecoder(bytes.NewReader(pathFilterJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("parsing testdata/path_filter.json: %v", err)
	}
	if len(v.Cases) == 0 {
		t.Fatal("testdata/path_filter.json has no cases")
	}
	for _, c := range v.Cases {
		t.Run(c.Name, func(t *testing.T) {
			want := c.Filter.decode(t)
			got := PathFilter(c.Paths)
			if !bytes.Equal(got, want) {
				t.Fatalf("filter %x, want %x", got, want)
			}
			for _, q := range c.Queries {
				if h := PathFilterHolds(want, q.Path); h != q.Holds {
					t.Errorf("%q: holds %v, want %v", q.Path, h, q.Holds)
				}
			}
		})
	}
}

// A filter holds every path it was made of, and of paths it was not, few: its false positives are
// near git's, about 1% at ten bits a path.
func TestPathFilterRate(t *testing.T) {
	var paths []string
	for i := range 300 {
		paths = append(paths, fmt.Sprintf("dir%02d/file%03d.go", i%20, i))
	}
	f := PathFilter(paths)
	for _, p := range paths {
		if !PathFilterHolds(f, p) {
			t.Fatalf("the filter does not hold %q, one of its paths", p)
		}
	}
	held := 0
	const tries = 20_000
	for i := range tries {
		if PathFilterHolds(f, fmt.Sprintf("elsewhere/%d", i)) {
			held++
		}
	}
	if rate := float64(held) / tries; rate > 0.02 {
		t.Errorf("%.2f%% of paths it was not made of are held", 100*rate)
	}
}
