package encoding

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"testing"
)

//go:embed testdata/layer.json
var layerJSON []byte

type layerVectors struct {
	Comment []string `json:"comment"`
	Valid   []struct {
		Name    string `json:"name"`
		Records []struct {
			Kind string   `json:"kind"`
			ID   string   `json:"id"`
			Body hexLines `json:"body"`
		} `json:"records"`
		Bytes hexLines `json:"bytes"`
	} `json:"valid"`
	Invalid []struct {
		Name  string   `json:"name"`
		Bytes hexLines `json:"bytes"`
	} `json:"invalid"`
}

// Every valid vector's records encode to exactly its bytes, which decode back to them in order;
// every invalid vector's bytes are refused.
func TestLayerVectors(t *testing.T) {
	var v layerVectors
	dec := json.NewDecoder(bytes.NewReader(layerJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("parsing testdata/layer.json: %v", err)
	}
	if len(v.Valid) == 0 || len(v.Invalid) == 0 {
		t.Fatal("testdata/layer.json has no vectors")
	}
	for _, c := range v.Valid {
		t.Run(c.Name, func(t *testing.T) {
			var recs []LayerRecord
			for _, r := range c.Records {
				k, err := hex.DecodeString(r.Kind)
				if err != nil || len(k) != 1 {
					t.Fatalf("kind %q", r.Kind)
				}
				id, err := hex.DecodeString(r.ID)
				if err != nil || len(id) != IDLen {
					t.Fatalf("ID %q", r.ID)
				}
				rec := LayerRecord{Kind: Kind(k[0]), Body: r.Body.decode(t)}
				copy(rec.ID[:], id)
				recs = append(recs, rec)
			}
			want := c.Bytes.decode(t)
			// Given in reverse, they are written in order all the same.
			rev := make([]LayerRecord, len(recs))
			for i, r := range recs {
				rev[len(recs)-1-i] = r
			}
			if got := EncodeLayerRecords(rev); !bytes.Equal(got, want) {
				t.Errorf("encoded %x, want %x", got, want)
			}
			got, err := DecodeLayerRecords(want)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(recs) {
				t.Fatalf("decoded %d records, want %d", len(got), len(recs))
			}
			for i := range got {
				if got[i].Kind != recs[i].Kind || got[i].ID != recs[i].ID || !bytes.Equal(got[i].Body, recs[i].Body) {
					t.Errorf("record %d: %v %s, want %v %s", i, got[i].Kind, got[i].ID, recs[i].Kind, recs[i].ID)
				}
			}
		})
	}
	for _, c := range v.Invalid {
		t.Run(c.Name, func(t *testing.T) {
			if got, err := DecodeLayerRecords(c.Bytes.decode(t)); err == nil {
				t.Errorf("decoded %d records", len(got))
			}
		})
	}
}

// A record given twice is written once.
func TestLayerRecordsOnce(t *testing.T) {
	file := []byte("x")
	r := LayerRecord{Kind: KindFile, ID: Hash(append([]byte{byte(KindFile)}, file...)), Body: file}
	got, err := DecodeLayerRecords(EncodeLayerRecords([]LayerRecord{r, r}))
	if err != nil || len(got) != 1 {
		t.Fatalf("%d records, %v; want one", len(got), err)
	}
}
