package encoding

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// The commit graph's records (docs/scm.md, "The commit graph"): what a client's index of a
// repository's commits needs of each — its ID, change ID, generation, committer's time, tree, and
// parents — and, where the SCM derived it, a filter of the paths it changed, without the commit's
// body:
//
//	count       uvarint
//	records     in topological order, each after every parent the records hold:
//	  flags       u8: 1 its one parent is the record before it; 2 a changed-path filter follows
//	  id          32 bytes
//	  change id   uvarint length, then the bytes
//	  generation  uvarint: 1 + its highest parent's, the root's 0
//	  committed   zigzag varint: the committer's time, milliseconds since the Unix epoch
//	  zone        zigzag varint: the committer's time zone, minutes east of UTC
//	  tree        uvarint terms (odd), then each tree's ID
//	  parents     unless flag 1: uvarint count, then each parent's ID
//	  filter      with flag 2: uvarint length, then the filter (ChangedPathFilter)
//
// A main graph layer stored or sent is these records compressed (internal/scm/graph); a client
// building its index reads them instead of every commit.

// GraphRecord is one commit's record in the commit graph.
type GraphRecord struct {
	ID         ID
	ChangeID   []byte
	Generation uint64
	Committed  Timestamp
	Tree       Merge[ID]
	Parents    []ID
	// Filter is the commit's changed-path filter; nil where none was derived.
	Filter []byte
}

const (
	graphParentBefore = 1 << iota
	graphFilter
	graphFlags = graphParentBefore | graphFilter
)

// maxGraphTerms bounds a record's tree terms: a conflict of more sides than this is not one jj makes.
const maxGraphTerms = 255

// EncodeGraphRecords writes records in their order, which must be topological.
func EncodeGraphRecords(records []GraphRecord) []byte {
	out := binary.AppendUvarint(nil, uint64(len(records)))
	for i, r := range records {
		var flags byte
		if i > 0 && len(r.Parents) == 1 && r.Parents[0] == records[i-1].ID {
			flags |= graphParentBefore
		}
		if r.Filter != nil {
			flags |= graphFilter
		}
		out = append(out, flags)
		out = append(out, r.ID[:]...)
		out = binary.AppendUvarint(out, uint64(len(r.ChangeID)))
		out = append(out, r.ChangeID...)
		out = binary.AppendUvarint(out, r.Generation)
		out = binary.AppendVarint(out, r.Committed.Millis)
		out = binary.AppendVarint(out, int64(r.Committed.TZOffset))
		out = binary.AppendUvarint(out, uint64(len(r.Tree)))
		for _, t := range r.Tree {
			out = append(out, t[:]...)
		}
		if flags&graphParentBefore == 0 {
			out = binary.AppendUvarint(out, uint64(len(r.Parents)))
			for _, p := range r.Parents {
				out = append(out, p[:]...)
			}
		}
		if r.Filter != nil {
			out = binary.AppendUvarint(out, uint64(len(r.Filter)))
			out = append(out, r.Filter...)
		}
	}
	return out
}

// DecodeGraphRecords reads records, refusing truncated ones, unknown flags, a tree of an even
// number of terms or none, a record twice, and one whose generation is not above a parent's the
// records hold. The parent a record shares with the one before it is named. Change IDs and
// filters alias b.
func DecodeGraphRecords(b []byte) ([]GraphRecord, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, errors.New("graph: truncated")
	}
	b = b[n:]
	// The smallest record: flags, an ID, an empty change ID, a generation, a time, a zone, one tree.
	if count > uint64(len(b))/(1+IDLen+1+1+1+1+1+IDLen) {
		return nil, fmt.Errorf("graph: %d records in %d bytes", count, len(b))
	}
	out := make([]GraphRecord, 0, count)
	gens := make(map[ID]uint64, count)
	truncated := errors.New("graph: truncated")
	uvarint := func() (uint64, error) {
		v, n := binary.Uvarint(b)
		if n <= 0 {
			return 0, truncated
		}
		b = b[n:]
		return v, nil
	}
	varint := func() (int64, error) {
		v, n := binary.Varint(b)
		if n <= 0 {
			return 0, truncated
		}
		b = b[n:]
		return v, nil
	}
	id := func() (ID, error) {
		var x ID
		if len(b) < IDLen {
			return x, truncated
		}
		copy(x[:], b)
		b = b[IDLen:]
		return x, nil
	}
	bytesOf := func() ([]byte, error) {
		l, err := uvarint()
		if err != nil {
			return nil, err
		}
		if l > uint64(len(b)) {
			return nil, truncated
		}
		v := b[:l:l]
		b = b[l:]
		return v, nil
	}
	for i := range count {
		if len(b) == 0 {
			return nil, truncated
		}
		flags := b[0]
		b = b[1:]
		if flags&^graphFlags != 0 {
			return nil, fmt.Errorf("graph: record %d has flags %#x", i, flags)
		}
		var r GraphRecord
		var err error
		if r.ID, err = id(); err != nil {
			return nil, err
		}
		if r.ChangeID, err = bytesOf(); err != nil {
			return nil, err
		}
		if r.Generation, err = uvarint(); err != nil {
			return nil, err
		}
		if r.Committed.Millis, err = varint(); err != nil {
			return nil, err
		}
		zone, err := varint()
		if err != nil {
			return nil, err
		}
		if zone < -24*60 || zone > 24*60 {
			return nil, fmt.Errorf("graph: record %d's time zone is %d minutes", i, zone)
		}
		r.Committed.TZOffset = int32(zone)
		terms, err := uvarint()
		if err != nil {
			return nil, err
		}
		if terms%2 == 0 || terms > maxGraphTerms {
			return nil, fmt.Errorf("graph: record %d's tree has %d terms", i, terms)
		}
		for range terms {
			t, err := id()
			if err != nil {
				return nil, err
			}
			r.Tree = append(r.Tree, t)
		}
		if flags&graphParentBefore != 0 {
			if i == 0 {
				return nil, errors.New("graph: the first record's parent is a record before it")
			}
			r.Parents = []ID{out[i-1].ID}
		} else {
			k, err := uvarint()
			if err != nil {
				return nil, err
			}
			if k > uint64(len(b))/IDLen {
				return nil, truncated
			}
			for range k {
				p, err := id()
				if err != nil {
					return nil, err
				}
				r.Parents = append(r.Parents, p)
			}
		}
		if flags&graphFilter != 0 {
			if r.Filter, err = bytesOf(); err != nil {
				return nil, err
			}
		}
		if _, dup := gens[r.ID]; dup {
			return nil, fmt.Errorf("graph: commit %s twice", r.ID)
		}
		for _, p := range r.Parents {
			if g, ok := gens[p]; ok && g >= r.Generation {
				return nil, fmt.Errorf("graph: commit %s's generation %d is not above its parent's, %d", r.ID, r.Generation, g)
			}
		}
		gens[r.ID] = r.Generation
		out = append(out, r)
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("graph: %d bytes after its records", len(b))
	}
	return out, nil
}
