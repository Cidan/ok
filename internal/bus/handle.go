package bus

import (
	"encoding/binary"
	"errors"
)

// EncodeHandle is a lease's handle made of parts, for a backend's Leased.Handle: each part's length,
// then its bytes.
func EncodeHandle(parts ...string) []byte {
	var out []byte
	for _, p := range parts {
		out = binary.AppendUvarint(out, uint64(len(p)))
		out = append(out, p...)
	}
	return out
}

// errBadHandle refuses a handle that is not n parts.
var errBadHandle = errors.New("bus: not a lease's handle")

// DecodeHandle is the n parts of a handle EncodeHandle made.
func DecodeHandle(h []byte, n int) ([]string, error) {
	out := make([]string, 0, n)
	for len(h) > 0 {
		l, k := binary.Uvarint(h)
		if k <= 0 || uint64(len(h)-k) < l {
			return nil, errBadHandle
		}
		out = append(out, string(h[k:k+int(l)]))
		h = h[k+int(l):]
	}
	if len(out) != n {
		return nil, errBadHandle
	}
	return out, nil
}
