package encoding

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

//go:embed testdata/vectors.json
var vectorsJSON []byte

type vectorFile struct {
	Comment []string        `json:"comment"`
	Valid   []validVector   `json:"valid"`
	Invalid []invalidVector `json:"invalid"`
}

type validVector struct {
	Name       string   `json:"name"`
	Object     jObject  `json:"object"`
	Bytes      hexLines `json:"bytes"`
	SignedData hexLines `json:"signed_data"`
	ID         string   `json:"id"`
}

type invalidVector struct {
	Name  string   `json:"name"`
	Bytes hexLines `json:"bytes"`
	Error Code     `json:"error"`
}

// hexLines is a byte string written as lines of hex, each optionally followed by a
// '#' comment.
type hexLines []string

func (h hexLines) bytes() ([]byte, error) {
	var b []byte
	for _, line := range h {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		seg, err := hex.DecodeString(strings.Join(strings.Fields(line), ""))
		if err != nil {
			return nil, fmt.Errorf("bad hex line %q: %w", line, err)
		}
		b = append(b, seg...)
	}
	return b, nil
}

func (h hexLines) decode(t *testing.T) []byte {
	t.Helper()
	b, err := h.bytes()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type jObject struct {
	Kind           string  `json:"kind"`
	Contents       *string `json:"contents"`
	ContentsRepeat *struct {
		Hex   string `json:"hex"`
		Count int    `json:"count"`
	} `json:"contents_repeat"`
	Target  string `json:"target"`
	Entries []struct {
		Name  string `json:"name"`
		Value struct {
			File *struct {
				ID         string `json:"id"`
				Executable bool   `json:"executable"`
				CopyID     string `json:"copy_id"`
			} `json:"file"`
			Symlink *string `json:"symlink"`
			Tree    *string `json:"tree"`
		} `json:"value"`
	} `json:"entries"`
	CurrentPath        string                 `json:"current_path"`
	Parents            []string               `json:"parents"`
	Salt               string                 `json:"salt"`
	Predecessors       []string               `json:"predecessors"`
	RootTree           []string               `json:"root_tree"`
	ConflictLabels     []string               `json:"conflict_labels"`
	ChangeID           string                 `json:"change_id"`
	Description        string                 `json:"description"`
	Author             jSignature             `json:"author"`
	Committer          jSignature             `json:"committer"`
	SecureSig          *string                `json:"secure_sig"`
	ViewID             string                 `json:"view_id"`
	Metadata           jMetadata              `json:"metadata"`
	CommitPredecessors *map[string][]string   `json:"commit_predecessors"`
	HeadIDs            []string               `json:"head_ids"`
	LocalBookmarks     map[string][]*string   `json:"local_bookmarks"`
	LocalTags          map[string][]*string   `json:"local_tags"`
	RemoteViews        map[string]jRemoteView `json:"remote_views"`
	GitRefs            map[string][]*string   `json:"git_refs"`
	GitHeads           map[string][]*string   `json:"git_heads"`
	WCCommitIDs        map[string]string      `json:"wc_commit_ids"`
}

type jTimestamp struct {
	Millis   int64 `json:"millis"`
	TZOffset int32 `json:"tz_offset"`
}

type jSignature struct {
	Name  string     `json:"name"`
	Email string     `json:"email"`
	Time  jTimestamp `json:"time"`
}

type jMetadata struct {
	Start         jTimestamp        `json:"start"`
	End           jTimestamp        `json:"end"`
	Description   string            `json:"description"`
	Hostname      string            `json:"hostname"`
	Username      string            `json:"username"`
	IsSnapshot    bool              `json:"is_snapshot"`
	WorkspaceName *string           `json:"workspace_name"`
	Attributes    map[string]string `json:"attributes"`
}

type jRemoteView struct {
	Bookmarks map[string]jRemoteRef `json:"bookmarks"`
	Tags      map[string]jRemoteRef `json:"tags"`
}

type jRemoteRef struct {
	Target []*string `json:"target"`
	State  string    `json:"state"`
}

// conv turns the vectors' JSON into objects, failing the test on malformed fixtures.
type conv struct{ t *testing.T }

func (c conv) bytes(s string) []byte {
	c.t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		c.t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

func (c conv) id(s string) ID {
	c.t.Helper()
	b := c.bytes(s)
	if len(b) != IDLen {
		c.t.Fatalf("id %q is %d bytes", s, len(b))
	}
	return ID(b)
}

func (c conv) ids(ss []string) []ID {
	out := make([]ID, len(ss))
	for i, s := range ss {
		out[i] = c.id(s)
	}
	return out
}

func (c conv) sig(s jSignature) Signature {
	return Signature{Name: s.Name, Email: s.Email, Time: Timestamp(s.Time)}
}

func (c conv) refTarget(terms []*string) RefTarget {
	rt := make(RefTarget, len(terms))
	for i, s := range terms {
		if s != nil {
			id := c.id(*s)
			rt[i] = &id
		}
	}
	return rt
}

func (c conv) refTargets(m map[string][]*string) map[string]RefTarget {
	out := map[string]RefTarget{}
	for k, v := range m {
		out[k] = c.refTarget(v)
	}
	return out
}

func (c conv) remoteRefs(m map[string]jRemoteRef) map[string]RemoteRef {
	out := map[string]RemoteRef{}
	for k, v := range m {
		state := map[string]RemoteRefState{"new": RemoteRefNew, "tracked": RemoteRefTracked}[v.State]
		out[k] = RemoteRef{Target: c.refTarget(v.Target), State: state}
	}
	return out
}

func (c conv) contents(o jObject) []byte {
	if o.ContentsRepeat != nil {
		return bytes.Repeat(c.bytes(o.ContentsRepeat.Hex), o.ContentsRepeat.Count)
	}
	return c.bytes(*o.Contents)
}

func (c conv) object(o jObject) Object {
	c.t.Helper()
	switch o.Kind {
	case "file":
		return &File{Contents: c.contents(o)}
	case "chunk":
		return &Chunk{Contents: c.contents(o)}
	case "symlink":
		return &Symlink{Target: o.Target}
	case "tree":
		tr := &Tree{}
		for _, e := range o.Entries {
			var v TreeValue
			switch {
			case e.Value.File != nil:
				v = TreeValue{Kind: TreeValueFile, ID: c.id(e.Value.File.ID), Executable: e.Value.File.Executable}
				if e.Value.File.CopyID != "" {
					id := c.id(e.Value.File.CopyID)
					v.CopyID = &id
				}
			case e.Value.Symlink != nil:
				v = TreeValue{Kind: TreeValueSymlink, ID: c.id(*e.Value.Symlink)}
			case e.Value.Tree != nil:
				v = TreeValue{Kind: TreeValueTree, ID: c.id(*e.Value.Tree)}
			default:
				c.t.Fatalf("tree entry %q has no value", e.Name)
			}
			tr.Entries = append(tr.Entries, TreeEntry{Name: e.Name, Value: v})
		}
		return tr
	case "copy_history":
		return &CopyHistory{CurrentPath: o.CurrentPath, Parents: c.ids(o.Parents), Salt: c.bytes(o.Salt)}
	case "commit":
		cm := &Commit{
			Parents:        c.ids(o.Parents),
			Predecessors:   c.ids(o.Predecessors),
			RootTree:       Merge[ID](c.ids(o.RootTree)),
			ConflictLabels: Merge[string](o.ConflictLabels),
			ChangeID:       ChangeID(c.bytes(o.ChangeID)),
			Description:    o.Description,
			Author:         c.sig(o.Author),
			Committer:      c.sig(o.Committer),
		}
		if o.SecureSig != nil {
			cm.SecureSig = c.bytes(*o.SecureSig)
		}
		return cm
	case "operation":
		m := o.Metadata
		op := &Operation{
			ViewID:  c.id(o.ViewID),
			Parents: c.ids(o.Parents),
			Metadata: OperationMetadata{
				Start:         Timestamp(m.Start),
				End:           Timestamp(m.End),
				Description:   m.Description,
				Hostname:      m.Hostname,
				Username:      m.Username,
				IsSnapshot:    m.IsSnapshot,
				WorkspaceName: m.WorkspaceName,
				Attributes:    m.Attributes,
			},
		}
		if o.CommitPredecessors != nil {
			op.CommitPredecessors = map[ID][]ID{}
			for k, v := range *o.CommitPredecessors {
				op.CommitPredecessors[c.id(k)] = c.ids(v)
			}
		}
		return op
	case "view":
		v := &View{
			HeadIDs:        c.ids(o.HeadIDs),
			LocalBookmarks: c.refTargets(o.LocalBookmarks),
			LocalTags:      c.refTargets(o.LocalTags),
			RemoteViews:    map[string]RemoteView{},
			GitRefs:        c.refTargets(o.GitRefs),
			GitHeads:       c.refTargets(o.GitHeads),
			WCCommitIDs:    map[string]ID{},
		}
		for k, rv := range o.RemoteViews {
			v.RemoteViews[k] = RemoteView{Bookmarks: c.remoteRefs(rv.Bookmarks), Tags: c.remoteRefs(rv.Tags)}
		}
		for k, id := range o.WCCommitIDs {
			v.WCCommitIDs[k] = c.id(id)
		}
		return v
	}
	c.t.Fatalf("unknown kind %q", o.Kind)
	return nil
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	var vf vectorFile
	dec := json.NewDecoder(bytes.NewReader(vectorsJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&vf); err != nil {
		t.Fatalf("parsing testdata/vectors.json: %v", err)
	}
	if len(vf.Valid) == 0 || len(vf.Invalid) == 0 {
		t.Fatal("testdata/vectors.json has no vectors")
	}
	return vf
}

func TestValidVectors(t *testing.T) {
	vf := loadVectors(t)
	for _, v := range vf.Valid {
		t.Run(v.Name, func(t *testing.T) {
			c := conv{t}
			obj := c.object(v.Object)
			want := v.Bytes.decode(t)
			if v.Object.ContentsRepeat != nil {
				want = append([]byte{byte(obj.Kind())}, c.contents(v.Object)...)
			}

			got, err := Encode(obj)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("Encode:\n got %x\nwant %x", got, want)
			}
			if id := Hash(got); id.String() != v.ID {
				t.Errorf("ID: got %s, want %s", id, v.ID)
			}

			back, err := Decode(want)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if back.Kind() != obj.Kind() {
				t.Fatalf("Decode kind: got %s, want %s", back.Kind(), obj.Kind())
			}
			again, err := Encode(back)
			if err != nil {
				t.Fatalf("re-Encode: %v", err)
			}
			if !bytes.Equal(again, want) {
				t.Fatalf("Encode(Decode(b)) != b:\n got %x\nwant %x", again, want)
			}

			if cm, ok := obj.(*Commit); ok && cm.SecureSig != nil {
				sd, err := SignedData(cm)
				if err != nil {
					t.Fatalf("SignedData: %v", err)
				}
				if want := v.SignedData.decode(t); !bytes.Equal(sd, want) {
					t.Fatalf("SignedData:\n got %x\nwant %x", sd, want)
				}
			}
		})
	}
}

func TestInvalidVectors(t *testing.T) {
	vf := loadVectors(t)
	for _, v := range vf.Invalid {
		t.Run(v.Name, func(t *testing.T) {
			b := v.Bytes.decode(t)
			obj, err := Decode(b)
			if err == nil {
				t.Fatalf("Decode accepted %x as %s", b, obj.Kind())
			}
			if got := CodeOf(err); got != v.Error {
				t.Fatalf("Decode: got %v, want code %q", err, v.Error)
			}
		})
	}
}

// TestMutatedVectors checks canonicality on every one-byte change, truncation, and
// extension of each valid vector's bytes: whatever still decodes must re-encode to exactly
// the input.
func TestMutatedVectors(t *testing.T) {
	vf := loadVectors(t)
	check := func(t *testing.T, b []byte) {
		obj, err := Decode(b)
		if err != nil {
			if CodeOf(err) == "" {
				t.Fatalf("Decode(%x) failed without a code: %v", b, err)
			}
			return
		}
		got, err := Encode(obj)
		if err != nil {
			t.Fatalf("Decode(%x) produced an object that does not encode: %v", b, err)
		}
		if !bytes.Equal(got, b) {
			t.Fatalf("Decode accepted non-canonical %x (canonical %x)", b, got)
		}
	}
	for _, v := range vf.Valid {
		if v.Object.ContentsRepeat != nil {
			continue
		}
		t.Run(v.Name, func(t *testing.T) {
			orig := v.Bytes.decode(t)
			for i := range orig {
				check(t, orig[:i])
				for _, delta := range []byte{1, 0x7f, 0x80, 0xff} {
					b := bytes.Clone(orig)
					b[i] += delta
					check(t, b)
				}
			}
			check(t, append(bytes.Clone(orig), 0))
		})
	}
}

func FuzzDecode(f *testing.F) {
	var vf vectorFile
	if err := json.Unmarshal(vectorsJSON, &vf); err != nil {
		f.Fatalf("parsing testdata/vectors.json: %v", err)
	}
	seed := func(h hexLines) {
		b, err := h.bytes()
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	for _, v := range vf.Valid {
		if v.Bytes != nil {
			seed(v.Bytes)
		}
	}
	for _, v := range vf.Invalid {
		seed(v.Bytes)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		obj, err := Decode(b)
		if err != nil {
			return
		}
		got, err := Encode(obj)
		if err != nil {
			t.Fatalf("decoded object does not encode: %v", err)
		}
		if !bytes.Equal(got, b) {
			t.Fatalf("Decode accepted non-canonical %x (canonical %x)", b, got)
		}
	})
}
