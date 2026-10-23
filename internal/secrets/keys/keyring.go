package keys

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Keyring is a root kept in a file of AES-256 keys, outside any database: for an installation on
// one machine, and for tests. Each line is a key's ID and its 64 hex digits; '#' begins a comment.
// The first key is the primary, which wraps; every key unwraps what it wrapped, so a new key is
// added first and an old one removed only once nothing it wrapped is left (ok secrets rewrap).
type Keyring struct {
	id      string
	primary string
	keys    map[string]Key
}

// OpenKeyring reads a keyring file: its root's ID is keyring:<file>.
func OpenKeyring(file string) (*Keyring, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("keys: %w", err)
	}
	k, err := ParseKeyring(data)
	if err != nil {
		return nil, fmt.Errorf("keys: %s: %w", file, err)
	}
	k.id = "keyring:" + file
	return k, nil
}

// ParseKeyring reads a keyring's text.
func ParseKeyring(data []byte) (*Keyring, error) {
	k := &Keyring{keys: map[string]Key{}}
	sc := bufio.NewScanner(bytes.NewReader(data))
	for n := 1; sc.Scan(); n++ {
		line, _, _ := strings.Cut(sc.Text(), "#")
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 2 || !validKeyID(fields[0]) {
			return nil, fmt.Errorf("line %d is not a key's ID and its 64 hex digits", n)
		}
		b, err := hex.DecodeString(fields[1])
		if err != nil || len(b) != len(Key{}) {
			return nil, fmt.Errorf("line %d: %s's key is not 64 hex digits", n, fields[0])
		}
		if _, dup := k.keys[fields[0]]; dup {
			return nil, fmt.Errorf("line %d: %s is named twice", n, fields[0])
		}
		k.keys[fields[0]] = Key(b)
		if k.primary == "" {
			k.primary = fields[0]
		}
	}
	if k.primary == "" {
		return nil, errors.New("holds no key")
	}
	return k, nil
}

// NewKeyringLine is a keyring line for a new key: ID and key.
func NewKeyringLine(id string) (string, error) {
	if !validKeyID(id) {
		return "", fmt.Errorf("keys: %q is not a key's ID (letters, digits, - and _)", id)
	}
	key, err := NewKey()
	if err != nil {
		return "", err
	}
	defer key.Clear()
	return id + " " + hex.EncodeToString(key[:]) + "\n", nil
}

func validKeyID(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func (k *Keyring) ID() string { return k.id }

// Wrap seals under the primary: its ID's length and the ID, then what Seal makes.
func (k *Keyring) Wrap(_ context.Context, plaintext, aad []byte) ([]byte, error) {
	sealed, err := Seal(k.keys[k.primary], plaintext, aad)
	if err != nil {
		return nil, err
	}
	return append(append([]byte{byte(len(k.primary))}, k.primary...), sealed...), nil
}

func (k *Keyring) Unwrap(_ context.Context, wrapped, aad []byte) ([]byte, error) {
	if len(wrapped) < 1 || len(wrapped) < 1+int(wrapped[0]) {
		return nil, ErrOpen
	}
	key, ok := k.keys[string(wrapped[1:1+wrapped[0]])]
	if !ok {
		return nil, fmt.Errorf("%w: wrapped by key %q, which the keyring does not hold", ErrOpen, wrapped[1:1+wrapped[0]])
	}
	return Open(key, wrapped[1+wrapped[0]:], aad)
}
