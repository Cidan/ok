package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

// encoder appends canonical bytes to buf. The first value that cannot be encoded sets err;
// later writes are ignored.
type encoder struct {
	buf []byte
	err error
}

func (e *encoder) fail(code Code, format string, args ...any) {
	if e.err == nil {
		e.err = &Error{Code: code, Offset: -1, Detail: fmt.Sprintf(format, args...)}
	}
}

func (e *encoder) u8(v uint8)          { e.buf = append(e.buf, v) }
func (e *encoder) u64(v uint64)        { e.buf = binary.LittleEndian.AppendUint64(e.buf, v) }
func (e *encoder) i64(v int64)         { e.u64(uint64(v)) }
func (e *encoder) i32(v int32)         { e.buf = binary.LittleEndian.AppendUint32(e.buf, uint32(v)) }
func (e *encoder) id(v ID)             { e.buf = append(e.buf, v[:]...) }
func (e *encoder) changeID(v ChangeID) { e.buf = append(e.buf, v[:]...) }
func (e *encoder) count(n int)         { e.u64(uint64(n)) }

func (e *encoder) boolean(v bool) {
	if v {
		e.u8(1)
	} else {
		e.u8(0)
	}
}

func (e *encoder) bytes(v []byte) {
	e.count(len(v))
	e.buf = append(e.buf, v...)
}

func (e *encoder) str(v string) {
	if !utf8.ValidString(v) {
		e.fail(CodeUTF8, "%q is not valid UTF-8", v)
	}
	e.count(len(v))
	e.buf = append(e.buf, v...)
}

func (e *encoder) ids(v []ID) {
	e.count(len(v))
	for _, id := range v {
		e.id(id)
	}
}

func (e *encoder) timestamp(t Timestamp) {
	e.i64(t.Millis)
	e.i32(t.TZOffset)
}

func (e *encoder) signature(s Signature) {
	e.str(s.Name)
	e.str(s.Email)
	e.timestamp(s.Time)
}

func encodeMerge[T any](e *encoder, m Merge[T], term func(T)) {
	if len(m)%2 == 0 {
		e.fail(CodeMerge, "a merge has %d terms", len(m))
	}
	e.count(len(m))
	for _, t := range m {
		term(t)
	}
}

func (e *encoder) refTarget(t RefTarget) {
	encodeMerge(e, t, func(id *ID) {
		if id == nil {
			e.u8(0)
			return
		}
		e.u8(1)
		e.id(*id)
	})
}

// encodeStrMap writes a map<str, V> in increasing bytewise order of its keys.
func encodeStrMap[V any](e *encoder, m map[string]V, value func(V)) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	e.count(len(keys))
	for _, k := range keys {
		e.str(k)
		value(m[k])
	}
}

func (f *File) encodeBody(e *encoder)  { e.buf = append(e.buf, f.Contents...) }
func (c *Chunk) encodeBody(e *encoder) { e.buf = append(e.buf, c.Contents...) }

func (s *Symlink) encodeBody(e *encoder) {
	if !utf8.ValidString(s.Target) {
		e.fail(CodeUTF8, "symlink target %q is not valid UTF-8", s.Target)
	}
	e.buf = append(e.buf, s.Target...)
}

func (t *Tree) encodeBody(e *encoder) {
	e.count(len(t.Entries))
	for i, entry := range t.Entries {
		if !validName(entry.Name) {
			e.fail(CodeName, "tree entry name %q", entry.Name)
		}
		if i > 0 {
			switch prev := t.Entries[i-1].Name; {
			case entry.Name == prev:
				e.fail(CodeDuplicate, "tree entry %q repeats", entry.Name)
			case entry.Name < prev:
				e.fail(CodeUnsorted, "tree entry %q follows %q", entry.Name, prev)
			}
		}
		e.str(entry.Name)
		e.treeValue(entry.Value)
	}
}

func (e *encoder) treeValue(v TreeValue) {
	switch v.Kind {
	case TreeValueFile:
		e.u8(uint8(TreeValueFile))
		e.id(v.ID)
		e.boolean(v.Executable)
		if v.CopyID == nil {
			e.count(0)
		} else {
			e.bytes(v.CopyID[:])
		}
	case TreeValueSymlink, TreeValueTree:
		e.u8(uint8(v.Kind))
		e.id(v.ID)
	default:
		e.fail(CodeTag, "tree value kind %d", v.Kind)
	}
}

func (c *CopyHistory) encodeBody(e *encoder) {
	if !validPath(c.CurrentPath) {
		e.fail(CodePath, "copy history path %q", c.CurrentPath)
	}
	e.str(c.CurrentPath)
	e.ids(c.Parents)
	e.bytes(c.Salt)
}

func (c *Commit) encodeBody(e *encoder) {
	e.ids(c.Parents)
	e.ids(c.Predecessors)
	encodeMerge(e, c.RootTree, e.id)
	encodeMerge(e, c.ConflictLabels, e.str)
	e.changeID(c.ChangeID)
	e.str(c.Description)
	e.signature(c.Author)
	e.signature(c.Committer)
	if c.SecureSig == nil {
		e.u8(0)
	} else {
		e.u8(1)
		e.bytes(c.SecureSig)
	}
}

func (o *Operation) encodeBody(e *encoder) {
	e.id(o.ViewID)
	e.ids(o.Parents)
	m := &o.Metadata
	e.timestamp(m.Start)
	e.timestamp(m.End)
	e.str(m.Description)
	e.str(m.Hostname)
	e.str(m.Username)
	e.boolean(m.IsSnapshot)
	if m.WorkspaceName == nil {
		e.u8(0)
	} else {
		e.u8(1)
		e.str(*m.WorkspaceName)
	}
	encodeStrMap(e, m.Attributes, e.str)
	if o.CommitPredecessors == nil {
		e.u8(0)
		return
	}
	e.u8(1)
	keys := make([]ID, 0, len(o.CommitPredecessors))
	for k := range o.CommitPredecessors {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b ID) int { return bytes.Compare(a[:], b[:]) })
	e.count(len(keys))
	for _, k := range keys {
		e.id(k)
		e.ids(o.CommitPredecessors[k])
	}
}

func (v *View) encodeBody(e *encoder) {
	heads := slices.Clone(v.HeadIDs)
	slices.SortFunc(heads, func(a, b ID) int { return bytes.Compare(a[:], b[:]) })
	for i := 1; i < len(heads); i++ {
		if heads[i] == heads[i-1] {
			e.fail(CodeDuplicate, "head %s repeats", heads[i])
		}
	}
	e.ids(heads)
	encodeStrMap(e, v.LocalBookmarks, e.refTarget)
	encodeStrMap(e, v.LocalTags, e.refTarget)
	encodeStrMap(e, v.RemoteViews, func(rv RemoteView) {
		encodeStrMap(e, rv.Bookmarks, e.remoteRef)
		encodeStrMap(e, rv.Tags, e.remoteRef)
	})
	encodeStrMap(e, v.GitRefs, e.refTarget)
	encodeStrMap(e, v.GitHeads, e.refTarget)
	encodeStrMap(e, v.WCCommitIDs, e.id)
}

func (e *encoder) remoteRef(r RemoteRef) {
	e.refTarget(r.Target)
	if r.State > RemoteRefTracked {
		e.fail(CodeState, "remote ref state %d", r.State)
	}
	e.u8(uint8(r.State))
}

// validName reports whether name can name a tree entry: non-empty, neither "." nor "..",
// and free of '/' and NUL. jj itself forbids only empty names and '/'.
func validName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, "/\x00")
}

// validPath reports whether p is a non-empty path whose '/'-separated components are all
// valid tree entry names.
func validPath(p string) bool {
	if p == "" {
		return false
	}
	for c := range strings.SplitSeq(p, "/") {
		if !validName(c) {
			return false
		}
	}
	return true
}
