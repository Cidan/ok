package encoding

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

// A checkout layer's records (docs/scm.md, "Checkouts"): objects of a commit's checkout — trees,
// symlinks, copy histories, the files planning reads — and trees' sizes records, which a client
// takes into its cache as if it had read each:
//
//	count     uvarint
//	records   sorted by kind, then ID, none twice:
//	  kind    u8: the object's kind, or KindTreeSizes
//	  id      32 bytes: the object's ID, or a sizes record's tree's
//	  length  uvarint
//	  body    length bytes: the object's canonical bytes after its kind byte, or the sizes record
//
// A layer stored or sent is these records compressed (internal/scm/layer); this is what it
// decompresses to.

// LayerRecord is one record of a layer.
type LayerRecord struct {
	Kind Kind
	ID   ID
	Body []byte
}

// layerKinds are the kinds a layer holds.
var layerKinds = map[Kind]bool{KindTree: true, KindSymlink: true, KindCopyHistory: true, KindFile: true, KindTreeSizes: true}

// EncodeLayerRecords writes records in their order (sorted, without duplicates by kind and ID).
func EncodeLayerRecords(records []LayerRecord) []byte {
	recs := slices.Clone(records)
	slices.SortFunc(recs, compareRecords)
	recs = slices.CompactFunc(recs, func(a, b LayerRecord) bool { return compareRecords(a, b) == 0 })
	n := 0
	for _, r := range recs {
		n += 1 + IDLen + binary.MaxVarintLen64 + len(r.Body)
	}
	out := make([]byte, 0, binary.MaxVarintLen64+n)
	out = binary.AppendUvarint(out, uint64(len(recs)))
	for _, r := range recs {
		out = append(out, byte(r.Kind))
		out = append(out, r.ID[:]...)
		out = binary.AppendUvarint(out, uint64(len(r.Body)))
		out = append(out, r.Body...)
	}
	return out
}

func compareRecords(a, b LayerRecord) int {
	if a.Kind != b.Kind {
		return int(a.Kind) - int(b.Kind)
	}
	return bytes.Compare(a.ID[:], b.ID[:])
}

// DecodeLayerRecords reads a layer's records, refusing one out of order or repeated, of a kind a
// layer does not hold, an object whose body does not hash to its ID, and a sizes record that does
// not decode. Bodies alias b.
func DecodeLayerRecords(b []byte) ([]LayerRecord, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, errors.New("layer: truncated")
	}
	b = b[n:]
	if count > uint64(len(b))/(1+IDLen+1) {
		return nil, fmt.Errorf("layer: %d records in %d bytes", count, len(b))
	}
	out := make([]LayerRecord, 0, count)
	for i := range count {
		if len(b) < 1+IDLen {
			return nil, errors.New("layer: truncated")
		}
		r := LayerRecord{Kind: Kind(b[0])}
		copy(r.ID[:], b[1:1+IDLen])
		b = b[1+IDLen:]
		length, n := binary.Uvarint(b)
		if n <= 0 || length > uint64(len(b)-n) {
			return nil, errors.New("layer: truncated")
		}
		r.Body = b[n : n+int(length)]
		b = b[n+int(length):]
		if !layerKinds[r.Kind] {
			return nil, fmt.Errorf("layer: record %d is a %s", i, r.Kind)
		}
		if i > 0 && compareRecords(out[i-1], r) >= 0 {
			return nil, fmt.Errorf("layer: record %d is out of order", i)
		}
		if r.Kind == KindTreeSizes {
			if _, err := DecodeTreeSizes(r.Body); err != nil {
				return nil, fmt.Errorf("layer: tree %s's sizes: %w", r.ID, err)
			}
		} else {
			canonical := append([]byte{byte(r.Kind)}, r.Body...)
			if Hash(canonical) != r.ID {
				return nil, fmt.Errorf("layer: %s %s does not hash to its ID", r.Kind, r.ID)
			}
		}
		out = append(out, r)
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("layer: %d bytes after its records", len(b))
	}
	return out, nil
}
