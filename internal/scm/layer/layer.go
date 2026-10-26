// Package layer is a checkout layer as the SCM stores and sends it (docs/scm.md, "Checkouts"):
//
//	magic    "OKCHKLAY"
//	version  u8 = 1
//	records  one zstd frame of encoding.EncodeLayerRecords
//
// A layer's ID is the BLAKE3 of those bytes, so a client checks a layer it received against the
// ID it asked for, and the same records make the same layer whichever replica derived them.
package layer

import (
	"errors"
	"fmt"

	"github.com/klauspost/compress/zstd"

	"github.com/Cidan/ok/internal/scm/encoding"
)

const (
	magic   = "OKCHKLAY"
	version = 1
	// MaxRecordBytes bounds what a layer decompresses to: a monorepo's whole checkout is tens of
	// MiB of records.
	MaxRecordBytes = 1 << 30
)

var (
	encoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1))
	decoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(MaxRecordBytes))
)

// Encode writes records as a layer and returns it with its ID.
func Encode(records []encoding.LayerRecord) ([]byte, encoding.ID) {
	out := append([]byte(magic), version)
	out = encoder.EncodeAll(encoding.EncodeLayerRecords(records), out)
	return out, encoding.Hash(out)
}

// Decode reads a layer's records, checked as encoding.DecodeLayerRecords checks them.
func Decode(b []byte) ([]encoding.LayerRecord, error) {
	if len(b) < len(magic)+1 || string(b[:len(magic)]) != magic {
		return nil, errors.New("layer: not a checkout layer")
	}
	if b[len(magic)] != version {
		return nil, fmt.Errorf("layer: version %d", b[len(magic)])
	}
	raw, err := decoder.DecodeAll(b[len(magic)+1:], nil)
	if err != nil {
		return nil, fmt.Errorf("layer: %w", err)
	}
	return encoding.DecodeLayerRecords(raw)
}
