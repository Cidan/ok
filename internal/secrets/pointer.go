package secrets

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/zeebo/blake3"
)

// Digest is a pointer's commitment to a secret's value.
type Digest [32]byte

// String is the digest in lowercase hex.
func (d Digest) String() string { return hex.EncodeToString(d[:]) }

// Short is the digest's first 12 hex digits, for logs.
func (d Digest) Short() string { return d.String()[:12] }

// Opening is the random half of a commitment, kept sealed beside the value and given only to its
// readers: without it a digest cannot be checked against a guess.
type Opening [32]byte

// pointerPrefix begins every pointer; the digest and a newline follow.
const pointerPrefix = "ok-secret v1 blake3:"

// PointerLen is the length of every pointer, in bytes.
const PointerLen = len(pointerPrefix) + 2*len(Digest{}) + 1

// CommitmentContext is BLAKE3's derive-key context for commitments: never used for anything
// else, so no other BLAKE3 output can be passed off as one.
const CommitmentContext = "ok 2026-10-22 secret commitment v1"

// ErrNotPointer is what ParsePointer returns for anything that is not exactly a pointer.
var ErrNotPointer = errors.New("not a secret pointer (ok-secret v1 blake3:<64 lowercase hex digits> and a newline)")

// Pointer is the line a secret's path holds in the repository.
func Pointer(d Digest) []byte {
	b := make([]byte, 0, PointerLen)
	b = append(b, pointerPrefix...)
	b = hex.AppendEncode(b, d[:])
	return append(b, '\n')
}

// ParsePointer reads a pointer: exactly the line Pointer writes, lowercase, with its newline.
func ParsePointer(b []byte) (Digest, error) {
	var d Digest
	if len(b) != PointerLen || string(b[:len(pointerPrefix)]) != pointerPrefix || b[len(b)-1] != '\n' {
		return d, ErrNotPointer
	}
	h := b[len(pointerPrefix) : len(b)-1]
	for _, c := range h {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return d, ErrNotPointer
		}
	}
	if _, err := hex.Decode(d[:], h); err != nil {
		return d, ErrNotPointer
	}
	return d, nil
}

// IsPointer reports whether b is exactly a pointer.
func IsPointer(b []byte) bool {
	_, err := ParsePointer(b)
	return err == nil
}

// Commit is the digest committing to a value at a path of a repository, with an opening:
// BLAKE3-256 in derive-key mode (CommitmentContext) over u64le(repository) ‖ u64le(len(path)) ‖
// path ‖ opening ‖ value (docs/secrets.md, "Pointers and commitments").
func Commit(repository int64, path string, opening Opening, value []byte) Digest {
	h := blake3.NewDeriveKey(CommitmentContext)
	var n [8]byte
	binary.LittleEndian.PutUint64(n[:], uint64(repository))
	h.Write(n[:])
	binary.LittleEndian.PutUint64(n[:], uint64(len(path)))
	h.Write(n[:])
	h.WriteString(path)
	h.Write(opening[:])
	h.Write(value)
	var d Digest
	copy(d[:], h.Sum(nil))
	return d
}

// Verify reports whether a value and opening are the ones a digest commits to, at a path of a
// repository.
func Verify(d Digest, repository int64, path string, opening Opening, value []byte) bool {
	got := Commit(repository, path, opening, value)
	return subtle.ConstantTimeCompare(got[:], d[:]) == 1
}

// NewOpening draws an opening from the system's CSPRNG.
func NewOpening() (Opening, error) {
	var o Opening
	if _, err := rand.Read(o[:]); err != nil {
		return o, fmt.Errorf("drawing an opening: %w", err)
	}
	return o, nil
}
