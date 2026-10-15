// Package encoding is ok's SCM's canonical object encoding: the single byte form of every
// object the SCM stores, and the BLAKE3-256 IDs computed over it (docs/scm.md,
// "Objects and identity"). The jj client implements the same encoding in Rust
// (jj//encoding); testdata/vectors.json holds the two to identical bytes, IDs, and
// rejections.
//
// An object's canonical bytes are its kind byte followed by its body, and its ID is the
// BLAKE3-256 digest of those bytes. Decode accepts only canonical bytes, so
// Encode(Decode(b)) == b for every b that decodes.
package encoding

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/zeebo/blake3"
)

// Kind is an object's kind byte. It prefixes the body both in the canonical bytes and in
// the hashed input, so objects of different kinds never share an ID.
type Kind uint8

const (
	KindFile        Kind = 0x01
	KindSymlink     Kind = 0x02
	KindTree        Kind = 0x03
	KindCopyHistory Kind = 0x04
	KindCommit      Kind = 0x05
	KindOperation   Kind = 0x06
	KindView        Kind = 0x07
	// KindChunk is a piece of a file's contents as the SCM stores it. It is not a jj
	// object, and a file's ID never depends on how it was chunked.
	KindChunk Kind = 0x10
)

func (k Kind) String() string {
	switch k {
	case KindFile:
		return "file"
	case KindSymlink:
		return "symlink"
	case KindTree:
		return "tree"
	case KindCopyHistory:
		return "copy_history"
	case KindCommit:
		return "commit"
	case KindOperation:
		return "operation"
	case KindView:
		return "view"
	case KindChunk:
		return "chunk"
	case KindTreeSizes:
		return "tree_sizes"
	}
	return fmt.Sprintf("kind(%#02x)", uint8(k))
}

// IDLen is the length of every object ID: a BLAKE3-256 digest.
const IDLen = 32

// ChangeIDLen is the length of a change ID, which the client generates at random.
const ChangeIDLen = 16

// ID identifies a commit, tree, file, symlink, copy history, operation, view, or chunk.
type ID [IDLen]byte

func (id ID) String() string { return hex.EncodeToString(id[:]) }

// ChangeID is jj's stable identifier for a change across rewrites.
type ChangeID [ChangeIDLen]byte

func (c ChangeID) String() string { return hex.EncodeToString(c[:]) }

// The virtual roots. None of them is ever stored: the root commit reads back as jj's
// make_root_commit, the root operation and its view as jj's make_root, and the empty tree
// is present in every repository.
var (
	RootCommitID    ID
	RootChangeID    ChangeID
	RootOperationID ID
	RootViewID      ID
	// EmptyTreeID is the ID of the tree with no entries: BLAKE3(0x03 ‖ u64 0).
	EmptyTreeID = Hash([]byte{byte(KindTree), 0, 0, 0, 0, 0, 0, 0, 0})
)

// Hash returns the ID of an object from its canonical bytes.
func Hash(canonical []byte) ID {
	return blake3.Sum256(canonical)
}

// Hasher computes an object's ID from a body written in pieces, for file contents too
// large to hold in memory. Its Sum equals Hash of the kind byte followed by everything
// written.
type Hasher struct {
	h *blake3.Hasher
}

// NewHasher returns a Hasher for an object of kind k.
func NewHasher(k Kind) *Hasher {
	h := blake3.New()
	h.Write([]byte{byte(k)})
	return &Hasher{h: h}
}

// Write adds p to the body. It never returns an error.
func (h *Hasher) Write(p []byte) (int, error) { return h.h.Write(p) }

// Sum returns the ID of the kind byte and the body written so far.
func (h *Hasher) Sum() ID {
	var id ID
	h.h.Sum(id[:0])
	return id
}

// Code classifies why bytes do not decode, or a value does not encode. The codes are
// shared with jj//encoding, and the golden vectors name them.
type Code string

const (
	// CodeTruncated: the input ends inside a value, or a length or count exceeds the
	// bytes remaining when it is read.
	CodeTruncated Code = "truncated"
	// CodeTrailing: bytes follow a complete object.
	CodeTrailing Code = "trailing"
	// CodeKind: the kind byte names no object.
	CodeKind Code = "kind"
	// CodeBool: a bool byte above 1.
	CodeBool Code = "bool"
	// CodeOption: an opt tag above 1.
	CodeOption Code = "option"
	// CodeUTF8: a str, or a symlink target, that is not valid UTF-8.
	CodeUTF8 Code = "utf8"
	// CodeUnsorted: set elements, map keys, or tree entry names out of increasing order.
	CodeUnsorted Code = "unsorted"
	// CodeDuplicate: a set element, map key, or tree entry name that repeats.
	CodeDuplicate Code = "duplicate"
	// CodeMerge: a merge with an even number of terms, or none.
	CodeMerge Code = "merge"
	// CodeTag: a tree value tag other than file, symlink, or tree.
	CodeTag Code = "tag"
	// CodeName: a tree entry name that is empty, ".", or "..", or contains '/' or NUL.
	CodeName Code = "name"
	// CodePath: a copy history path that is empty or has a component that is not a valid
	// tree entry name.
	CodePath Code = "path"
	// CodeCopyID: a file's copy ID that is neither empty nor 32 bytes.
	CodeCopyID Code = "copy-id"
	// CodeState: a remote ref state above 1.
	CodeState Code = "state"
	// CodeVersion: a tree sizes record of a version this does not read.
	CodeVersion Code = "version"
	// CodeVarint: a varint that is not minimal, or exceeds 64 bits.
	CodeVarint Code = "varint"
)

// Error reports a value that does not decode or encode canonically.
type Error struct {
	Code Code
	// Offset is the byte offset in the input at which decoding failed, or -1 for an
	// encoding error.
	Offset int
	Detail string
}

func (e *Error) Error() string {
	if e.Offset < 0 {
		return fmt.Sprintf("encoding: %s: %s", e.Code, e.Detail)
	}
	return fmt.Sprintf("decoding at byte %d: %s: %s", e.Offset, e.Code, e.Detail)
}

// CodeOf returns the Code of an *Error in err's chain, or "" if there is none.
func CodeOf(err error) Code {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// Object is one decoded object: *File, *Symlink, *Tree, *CopyHistory, *Commit,
// *Operation, *View, or *Chunk.
type Object interface {
	Kind() Kind
	encodeBody(e *encoder)
}

// Encode returns o's canonical bytes: its kind byte, then its body.
func Encode(o Object) ([]byte, error) {
	e := &encoder{}
	e.u8(uint8(o.Kind()))
	o.encodeBody(e)
	if e.err != nil {
		return nil, e.err
	}
	return e.buf, nil
}

// IDOf returns o's ID.
func IDOf(o Object) (ID, error) {
	b, err := Encode(o)
	if err != nil {
		return ID{}, err
	}
	return Hash(b), nil
}

// SignedData returns the bytes a commit's signature signs: the commit's canonical bytes
// with the signature absent.
func SignedData(c *Commit) ([]byte, error) {
	unsigned := *c
	unsigned.SecureSig = nil
	return Encode(&unsigned)
}

// Decode parses canonical bytes into the object they encode, refusing anything that is
// not canonical.
func Decode(b []byte) (Object, error) {
	d := &decoder{b: b}
	o := d.object()
	if d.err != nil {
		return nil, d.err
	}
	if d.off != len(d.b) {
		d.fail(CodeTrailing, "%d bytes after the %s", len(d.b)-d.off, o.Kind())
		return nil, d.err
	}
	return o, nil
}
