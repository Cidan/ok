package secrets

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"strconv"
	"strings"
	"testing"
)

// updateVectors rewrites the vectors' digests from this implementation: the one way they are
// made. go test ./internal/secrets
// -test.run TestVectors -vectors.update=$PWD/internal/secrets/testdata/vectors.json
var updateVectors = flag.String("vectors.update", "", "write the vectors' digests to this file")

type vectors struct {
	Context     string       `json:"context"`
	Commitments []commitment `json:"commitments"`
	Pointers    []struct {
		Name  string `json:"name"`
		Text  string `json:"text"`
		Valid bool   `json:"valid"`
	} `json:"pointers"`
	Names []struct {
		Name  string `json:"name"`
		Valid bool   `json:"valid"`
	} `json:"names"`
	Paths []struct {
		Path   string `json:"path"`
		InTree bool   `json:"in_tree"`
		Secret bool   `json:"secret"`
		ACL    bool   `json:"acl"`
		Valid  bool   `json:"valid"`
	} `json:"paths"`
}

type commitment struct {
	Name       string `json:"name"`
	Repository int64  `json:"repository"`
	Path       string `json:"path"`
	Opening    string `json:"opening"`
	Value      string `json:"value"`
	Digest     string `json:"digest"`
}

// value decodes a commitment's value: hex, or "repeat:<byte hex>:<count>".
func (c commitment) value(t *testing.T) []byte {
	if rest, ok := strings.CutPrefix(c.Value, "repeat:"); ok {
		b, n, _ := strings.Cut(rest, ":")
		by, err := hex.DecodeString(b)
		count, err2 := strconv.Atoi(n)
		if err != nil || err2 != nil || len(by) != 1 {
			t.Fatalf("%s: bad repeat %q", c.Name, c.Value)
		}
		return bytes.Repeat(by, count)
	}
	v, err := hex.DecodeString(c.Value)
	if err != nil {
		t.Fatalf("%s: value: %v", c.Name, err)
	}
	return v
}

func readVectors(t *testing.T) (vectors, []byte) {
	data, err := os.ReadFile("testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectors
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	return v, data
}

func TestVectors(t *testing.T) {
	v, _ := readVectors(t)
	if v.Context != CommitmentContext {
		t.Fatalf("context %q, want %q", v.Context, CommitmentContext)
	}
	seen := map[string]string{}
	for i, c := range v.Commitments {
		var o Opening
		ob, err := hex.DecodeString(c.Opening)
		if err != nil || len(ob) != len(o) {
			t.Fatalf("%s: opening %q", c.Name, c.Opening)
		}
		copy(o[:], ob)
		d := Commit(c.Repository, c.Path, o, c.value(t))
		if *updateVectors != "" {
			v.Commitments[i].Digest = d.String()
			continue
		}
		if d.String() != c.Digest {
			t.Errorf("%s: digest %s, want %s", c.Name, d, c.Digest)
		}
		if !Verify(d, c.Repository, c.Path, o, c.value(t)) {
			t.Errorf("%s: does not verify", c.Name)
		}
		if Verify(d, c.Repository, c.Path, o, append(c.value(t), 0)) {
			t.Errorf("%s: verifies another value", c.Name)
		}
		got, err := ParsePointer(Pointer(d))
		if err != nil || got != d {
			t.Errorf("%s: pointer round trip: %v", c.Name, err)
		}
		if prev, dup := seen[d.String()]; dup {
			t.Errorf("%s: same digest as %s", c.Name, prev)
		}
		seen[d.String()] = c.Name
	}
	if *updateVectors != "" {
		out, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(*updateVectors, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, p := range v.Pointers {
		d, err := ParsePointer([]byte(p.Text))
		if (err == nil) != p.Valid {
			t.Errorf("pointer %s: valid %v, want %v", p.Name, err == nil, p.Valid)
		}
		if err == nil && string(Pointer(d)) != p.Text {
			t.Errorf("pointer %s: does not format back", p.Name)
		}
	}
	for _, n := range v.Names {
		if ValidName(n.Name) != n.Valid {
			t.Errorf("name %q: valid %v, want %v", n.Name, !n.Valid, n.Valid)
		}
	}
	for _, p := range v.Paths {
		if InTree(p.Path) != p.InTree || IsSecret(p.Path) != p.Secret || IsACL(p.Path) != p.ACL || ValidPath(p.Path) != p.Valid {
			t.Errorf("path %q: in tree %v, secret %v, acl %v, valid %v; want %v %v %v %v", p.Path,
				InTree(p.Path), IsSecret(p.Path), IsACL(p.Path), ValidPath(p.Path), p.InTree, p.Secret, p.ACL, p.Valid)
		}
	}
}
