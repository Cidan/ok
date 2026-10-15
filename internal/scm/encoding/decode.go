package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

// decoder reads canonical bytes in order and stops at the first violation, which it
// records in err with its offset; later reads return zero values. Every length and count
// is checked against the bytes remaining as it is read, and collections grow only as
// elements actually decode, so a hostile count never drives an allocation out of
// proportion to the input. Violations are reported in encoding order, which
// jj//encoding follows too, so both report the same code for the same input.
type decoder struct {
	b   []byte
	off int
	err *Error
}

func (d *decoder) fail(code Code, format string, args ...any) {
	if d.err == nil {
		d.err = &Error{Code: code, Offset: d.off, Detail: fmt.Sprintf(format, args...)}
	}
}

func (d *decoder) failed() bool { return d.err != nil }

func (d *decoder) remaining() int { return len(d.b) - d.off }

// take consumes the next n bytes.
func (d *decoder) take(n int) []byte {
	if d.failed() {
		return nil
	}
	if n > d.remaining() {
		d.fail(CodeTruncated, "need %d bytes, have %d", n, d.remaining())
		return nil
	}
	s := d.b[d.off : d.off+n]
	d.off += n
	return s
}

func (d *decoder) u8() uint8 {
	b := d.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}

func (d *decoder) u64() uint64 {
	b := d.take(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

func (d *decoder) i64() int64 { return int64(d.u64()) }

func (d *decoder) i32() int32 {
	b := d.take(4)
	if b == nil {
		return 0
	}
	return int32(binary.LittleEndian.Uint32(b))
}

// count reads a length or element count. Every element encodes to at least one byte, so a
// count above the bytes remaining is truncation.
func (d *decoder) count() int {
	start := d.off
	n := d.u64()
	if d.failed() {
		return 0
	}
	if n > uint64(d.remaining()) {
		d.off = start
		d.fail(CodeTruncated, "count %d exceeds the %d bytes remaining", n, d.remaining()-8)
		return 0
	}
	return int(n)
}

func (d *decoder) boolean() bool {
	start := d.off
	v := d.u8()
	if v > 1 {
		d.off = start
		d.fail(CodeBool, "bool byte %d", v)
	}
	return v == 1
}

// option reads an opt tag and reports whether a value follows.
func (d *decoder) option() bool {
	start := d.off
	v := d.u8()
	if v > 1 {
		d.off = start
		d.fail(CodeOption, "option tag %d", v)
	}
	return v == 1
}

func (d *decoder) id() ID {
	var id ID
	copy(id[:], d.take(IDLen))
	return id
}

func (d *decoder) changeID() ChangeID {
	var c ChangeID
	copy(c[:], d.take(ChangeIDLen))
	return c
}

func (d *decoder) bytes() []byte {
	return bytes.Clone(d.take(d.count()))
}

func (d *decoder) str() string {
	start := d.off
	b := d.take(d.count())
	if d.failed() {
		return ""
	}
	if !utf8.Valid(b) {
		d.off = start
		d.fail(CodeUTF8, "str is not valid UTF-8")
		return ""
	}
	return string(b)
}

func (d *decoder) ids() []ID {
	n := d.count()
	if d.failed() {
		return nil
	}
	out := make([]ID, 0, min(n, d.remaining()/IDLen))
	for range n {
		id := d.id()
		if d.failed() {
			return nil
		}
		out = append(out, id)
	}
	return out
}

func (d *decoder) timestamp() Timestamp {
	return Timestamp{Millis: d.i64(), TZOffset: d.i32()}
}

func (d *decoder) signature() Signature {
	return Signature{Name: d.str(), Email: d.str(), Time: d.timestamp()}
}

func decodeMerge[T any](d *decoder, term func() T) Merge[T] {
	start := d.off
	n := d.count()
	if d.failed() {
		return nil
	}
	if n%2 == 0 {
		d.off = start
		d.fail(CodeMerge, "a merge has %d terms", n)
		return nil
	}
	var m Merge[T]
	for range n {
		t := term()
		if d.failed() {
			return nil
		}
		m = append(m, t)
	}
	return m
}

func (d *decoder) refTarget() RefTarget {
	return decodeMerge(d, func() *ID {
		if !d.option() {
			return nil
		}
		id := d.id()
		return &id
	})
}

// decodeStrMap reads a map<str, V>, requiring strictly increasing keys.
func decodeStrMap[V any](d *decoder, value func() V) map[string]V {
	n := d.count()
	if d.failed() {
		return nil
	}
	m := map[string]V{}
	var prev string
	for i := range n {
		start := d.off
		k := d.str()
		if d.failed() {
			return nil
		}
		if i > 0 {
			d.order(start, k, prev, "map key")
		}
		v := value()
		if d.failed() {
			return nil
		}
		m[k], prev = v, k
	}
	return m
}

// order checks that k strictly follows prev, blaming the value that started at start.
func (d *decoder) order(start int, k, prev, what string) {
	switch {
	case k == prev:
		d.off = start
		d.fail(CodeDuplicate, "%s %q repeats", what, k)
	case k < prev:
		d.off = start
		d.fail(CodeUnsorted, "%s %q follows %q", what, k, prev)
	}
}

func (d *decoder) object() Object {
	start := d.off
	switch k := Kind(d.u8()); {
	case d.failed():
		return nil
	case k == KindFile:
		return &File{Contents: bytes.Clone(d.take(d.remaining()))}
	case k == KindChunk:
		return &Chunk{Contents: bytes.Clone(d.take(d.remaining()))}
	case k == KindSymlink:
		bodyStart := d.off
		body := d.take(d.remaining())
		if !utf8.Valid(body) {
			d.off = bodyStart
			d.fail(CodeUTF8, "symlink target is not valid UTF-8")
			return nil
		}
		return &Symlink{Target: string(body)}
	case k == KindTree:
		return d.tree()
	case k == KindCopyHistory:
		return d.copyHistory()
	case k == KindCommit:
		return d.commit()
	case k == KindOperation:
		return d.operation()
	case k == KindView:
		return d.view()
	default:
		d.off = start
		d.fail(CodeKind, "unknown kind byte %#02x", uint8(k))
		return nil
	}
}

func (d *decoder) tree() *Tree {
	n := d.count()
	t := &Tree{Entries: []TreeEntry{}}
	for i := 0; i < n && !d.failed(); i++ {
		start := d.off
		name := d.str()
		if d.failed() {
			break
		}
		if !validName(name) {
			d.off = start
			d.fail(CodeName, "tree entry name %q", name)
			break
		}
		if i > 0 {
			d.order(start, name, t.Entries[i-1].Name, "tree entry")
		}
		t.Entries = append(t.Entries, TreeEntry{Name: name, Value: d.treeValue()})
	}
	return t
}

func (d *decoder) treeValue() TreeValue {
	start := d.off
	tag := TreeValueKind(d.u8())
	switch {
	case d.failed():
		return TreeValue{}
	case tag == TreeValueFile:
		v := TreeValue{Kind: tag, ID: d.id(), Executable: d.boolean()}
		copyStart := d.off
		copyID := d.bytes()
		switch {
		case d.failed(), len(copyID) == 0:
		case len(copyID) == IDLen:
			v.CopyID = (*ID)(copyID)
		default:
			d.off = copyStart
			d.fail(CodeCopyID, "copy ID of %d bytes", len(copyID))
		}
		return v
	case tag == TreeValueSymlink, tag == TreeValueTree:
		return TreeValue{Kind: tag, ID: d.id()}
	default:
		d.off = start
		d.fail(CodeTag, "tree value tag %d", tag)
		return TreeValue{}
	}
}

func (d *decoder) copyHistory() *CopyHistory {
	start := d.off
	c := &CopyHistory{CurrentPath: d.str()}
	if !d.failed() && !validPath(c.CurrentPath) {
		d.off = start
		d.fail(CodePath, "copy history path %q", c.CurrentPath)
	}
	c.Parents = d.ids()
	c.Salt = d.bytes()
	return c
}

func (d *decoder) commit() *Commit {
	c := &Commit{
		Parents:        d.ids(),
		Predecessors:   d.ids(),
		RootTree:       decodeMerge(d, d.id),
		ConflictLabels: decodeMerge(d, d.str),
		ChangeID:       d.changeID(),
		Description:    d.str(),
		Author:         d.signature(),
		Committer:      d.signature(),
	}
	if d.option() {
		c.SecureSig = d.bytes()
	}
	return c
}

func (d *decoder) operation() *Operation {
	o := &Operation{ViewID: d.id(), Parents: d.ids()}
	m := &o.Metadata
	m.Start = d.timestamp()
	m.End = d.timestamp()
	m.Description = d.str()
	m.Hostname = d.str()
	m.Username = d.str()
	m.IsSnapshot = d.boolean()
	if d.option() {
		ws := d.str()
		m.WorkspaceName = &ws
	}
	m.Attributes = decodeStrMap(d, d.str)
	if !d.option() {
		return o
	}
	n := d.count()
	o.CommitPredecessors = map[ID][]ID{}
	var prev ID
	for i := 0; i < n && !d.failed(); i++ {
		start := d.off
		k := d.id()
		if d.failed() {
			break
		}
		if i > 0 {
			d.order(start, string(k[:]), string(prev[:]), "commit")
		}
		o.CommitPredecessors[k], prev = d.ids(), k
	}
	return o
}

func (d *decoder) view() *View {
	v := &View{}
	n := d.count()
	v.HeadIDs = make([]ID, 0, min(n, d.remaining()/IDLen))
	for i := 0; i < n && !d.failed(); i++ {
		start := d.off
		id := d.id()
		if d.failed() {
			break
		}
		if i > 0 {
			d.order(start, string(id[:]), string(v.HeadIDs[i-1][:]), "head")
		}
		v.HeadIDs = append(v.HeadIDs, id)
	}
	v.LocalBookmarks = decodeStrMap(d, d.refTarget)
	v.LocalTags = decodeStrMap(d, d.refTarget)
	v.RemoteViews = decodeStrMap(d, func() RemoteView {
		return RemoteView{Bookmarks: decodeStrMap(d, d.remoteRef), Tags: decodeStrMap(d, d.remoteRef)}
	})
	v.GitRefs = decodeStrMap(d, d.refTarget)
	v.GitHeads = decodeStrMap(d, d.refTarget)
	v.WCCommitIDs = decodeStrMap(d, d.id)
	return v
}

func (d *decoder) remoteRef() RemoteRef {
	r := RemoteRef{Target: d.refTarget()}
	start := d.off
	state := d.u8()
	if state > uint8(RemoteRefTracked) {
		d.off = start
		d.fail(CodeState, "remote ref state %d", state)
	}
	r.State = RemoteRefState(state)
	return r
}
