package encoding

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"slices"
	"testing"
)

//go:embed testdata/graph.json
var graphJSON []byte

type graphVectors struct {
	Comment []string `json:"comment"`
	Valid   []struct {
		Name    string `json:"name"`
		Records []struct {
			ID         string   `json:"id"`
			ChangeID   string   `json:"change_id"`
			Generation uint64   `json:"generation"`
			Committed  int64    `json:"committed"`
			Zone       int32    `json:"zone"`
			Tree       []string `json:"tree"`
			Parents    []string `json:"parents"`
			Filter     *string  `json:"filter"`
		} `json:"records"`
		Bytes hexLines `json:"bytes"`
	} `json:"valid"`
	Invalid []struct {
		Name  string   `json:"name"`
		Bytes hexLines `json:"bytes"`
	} `json:"invalid"`
}

func vectorID(t *testing.T, s string) ID {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != IDLen {
		t.Fatalf("ID %q", s)
	}
	return ID(b)
}

// Every valid vector's records encode to exactly its bytes, which decode back to them; every
// invalid vector's bytes are refused.
func TestGraphVectors(t *testing.T) {
	var v graphVectors
	dec := json.NewDecoder(bytes.NewReader(graphJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("parsing testdata/graph.json: %v", err)
	}
	if len(v.Valid) == 0 || len(v.Invalid) == 0 {
		t.Fatal("testdata/graph.json has no vectors")
	}
	for _, c := range v.Valid {
		t.Run(c.Name, func(t *testing.T) {
			var recs []GraphRecord
			for _, r := range c.Records {
				change, err := hex.DecodeString(r.ChangeID)
				if err != nil {
					t.Fatalf("change ID %q", r.ChangeID)
				}
				rec := GraphRecord{ID: vectorID(t, r.ID), ChangeID: change, Generation: r.Generation,
					Committed: Timestamp{Millis: r.Committed, TZOffset: r.Zone}}
				for _, x := range r.Tree {
					rec.Tree = append(rec.Tree, vectorID(t, x))
				}
				for _, x := range r.Parents {
					rec.Parents = append(rec.Parents, vectorID(t, x))
				}
				if r.Filter != nil {
					if rec.Filter, err = hex.DecodeString(*r.Filter); err != nil {
						t.Fatalf("filter %q", *r.Filter)
					}
				}
				recs = append(recs, rec)
			}
			want := c.Bytes.decode(t)
			if got := EncodeGraphRecords(recs); !bytes.Equal(got, want) {
				t.Errorf("encoded %x, want %x", got, want)
			}
			got, err := DecodeGraphRecords(want)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(recs) {
				t.Fatalf("decoded %d records, want %d", len(got), len(recs))
			}
			for i := range got {
				g, w := got[i], recs[i]
				if g.ID != w.ID || !bytes.Equal(g.ChangeID, w.ChangeID) || g.Generation != w.Generation || g.Committed != w.Committed ||
					!slices.Equal(g.Tree, w.Tree) || !slices.Equal(g.Parents, w.Parents) || (g.Filter == nil) != (w.Filter == nil) ||
					!bytes.Equal(g.Filter, w.Filter) {
					t.Errorf("record %d: %+v, want %+v", i, g, w)
				}
			}
		})
	}
	for _, c := range v.Invalid {
		t.Run(c.Name, func(t *testing.T) {
			if got, err := DecodeGraphRecords(c.Bytes.decode(t)); err == nil {
				t.Errorf("decoded %d records", len(got))
			}
		})
	}
}

// A record whose one parent is the record before it names it once, in the flag: a linear run of
// history costs no parent IDs.
func TestGraphRecordsLinear(t *testing.T) {
	var recs []GraphRecord
	parent := RootCommitID
	for i := range 100 {
		id := Hash([]byte{byte(i), 1})
		recs = append(recs, GraphRecord{ID: id, ChangeID: make([]byte, ChangeIDLen), Generation: uint64(i + 1),
			Tree: Merge[ID]{EmptyTreeID}, Parents: []ID{parent}})
		parent = id
	}
	b := EncodeGraphRecords(recs)
	if per := len(b) / len(recs); per > 2*IDLen+ChangeIDLen+16 {
		t.Errorf("%d bytes a record in a linear run", per)
	}
	got, err := DecodeGraphRecords(b)
	if err != nil {
		t.Fatal(err)
	}
	for i := range got {
		if !slices.Equal(got[i].Parents, recs[i].Parents) {
			t.Fatalf("record %d's parents %v, want %v", i, got[i].Parents, recs[i].Parents)
		}
	}
}
