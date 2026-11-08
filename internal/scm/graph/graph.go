// Package graph is a main graph layer as the SCM stores and sends it (docs/scm.md, "The commit
// graph"):
//
//	magic    "OKGRAPHL"
//	version  u8 = 1
//	records  one zstd frame of encoding.EncodeGraphRecords
//
// A layer's ID is the BLAKE3 of those bytes, so a client checks a layer it received against the ID
// it asked for, and the same records make the same layer whichever replica derived them.
package graph

import (
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"

	"github.com/Cidan/ok/internal/scm/encoding"
)

const (
	magic   = "OKGRAPHL"
	version = 1
	// MaxRecordBytes bounds what a layer decompresses to: a decade of a large repository's main is
	// a few GiB of records, in layers geometrically smaller above the first.
	MaxRecordBytes = 8 << 30
)

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	decoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(MaxRecordBytes))
)

// Encode writes records, in their topological order, as a layer and returns it with its ID.
func Encode(records []encoding.GraphRecord) ([]byte, encoding.ID) {
	out := append([]byte(magic), version)
	out = encoder.EncodeAll(encoding.EncodeGraphRecords(records), out)
	return out, encoding.Hash(out)
}

// Decode reads a layer's records, checked as encoding.DecodeGraphRecords checks them.
func Decode(b []byte) ([]encoding.GraphRecord, error) {
	if len(b) < len(magic)+1 || string(b[:len(magic)]) != magic {
		return nil, errors.New("graph: not a graph layer")
	}
	if b[len(magic)] != version {
		return nil, fmt.Errorf("graph: version %d", b[len(magic)])
	}
	raw, err := decoder.DecodeAll(b[len(magic)+1:], nil)
	if err != nil {
		return nil, fmt.Errorf("graph: %w", err)
	}
	return encoding.DecodeGraphRecords(raw)
}
