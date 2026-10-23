// Package password hashes and checks people's passwords (docs/identity.md, "Accounts"): argon2id,
// each with a salt of its own, kept as a PHC string ($argon2id$v=19$m=…,t=…,p=…$salt$hash), so a
// stronger setting later still checks what was hashed before. Hashing takes 64 MiB; a few at once
// per process, so a flood of sign-ins waits rather than exhausting memory.
package password

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"
)

// The settings new hashes take: OWASP's argon2id recommendations, at their memory-heavy end.
const (
	memory  = 64 * 1024 // KiB
	passes  = 3
	threads = 4
	keyLen  = 32
	saltLen = 16
)

// MinLength and MaxLength bound a password, in characters.
const (
	MinLength = 12
	MaxLength = 1024
)

// hashing bounds the hashes a process computes at once.
var hashing = make(chan struct{}, 4)

func acquire(ctx context.Context) error {
	select {
	case hashing <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func release() { <-hashing }

// Check refuses a password a person may not choose: too short or long, or holding their handle or
// their email's name.
func Check(pw, handle, email string) error {
	n := utf8.RuneCountInString(pw)
	switch {
	case n < MinLength:
		return fmt.Errorf("a password is at least %d characters", MinLength)
	case n > MaxLength:
		return fmt.Errorf("a password is at most %d characters", MaxLength)
	}
	lower := strings.ToLower(pw)
	local, _, _ := strings.Cut(strings.ToLower(email), "@")
	for _, s := range []string{strings.ToLower(handle), local} {
		if len(s) >= 3 && strings.Contains(lower, s) {
			return errors.New("a password holds neither your handle nor your email's name")
		}
	}
	return nil
}

// Hash hashes a password with a new salt.
func Hash(ctx context.Context, pw string) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	if err := acquire(ctx); err != nil {
		return "", err
	}
	defer release()
	key := argon2.IDKey([]byte(pw), salt, passes, memory, threads, keyLen)
	b64 := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version, memory, passes, threads, b64.EncodeToString(salt), b64.EncodeToString(key)), nil
}

// Verify reports whether a password is the one a hash was made of, in time that does not depend on
// where they differ. A hash it cannot read is no match.
func Verify(ctx context.Context, hash, pw string) (bool, error) {
	p, err := parse(hash)
	if err != nil {
		return false, err
	}
	if err := acquire(ctx); err != nil {
		return false, err
	}
	defer release()
	key := argon2.IDKey([]byte(pw), p.salt, p.time, p.memory, p.threads, uint32(len(p.key)))
	return subtle.ConstantTimeCompare(key, p.key) == 1, nil
}

// Waste spends what a Verify would, for a sign-in with no account: so a refusal takes as long
// whether or not the account is there.
func Waste(ctx context.Context, pw string) {
	if acquire(ctx) != nil {
		return
	}
	defer release()
	argon2.IDKey([]byte(pw), make([]byte, saltLen), passes, memory, threads, keyLen)
}

type phc struct {
	memory, time uint32
	threads      uint8
	salt, key    []byte
}

func parse(hash string) (phc, error) {
	parts := strings.Split(hash, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return phc{}, errors.New("not an argon2id hash")
	}
	var v int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &v); err != nil || v != argon2.Version {
		return phc{}, errors.New("an argon2id hash of another version")
	}
	var p phc
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.memory, &p.time, &p.threads); err != nil {
		return phc{}, errors.New("an argon2id hash's settings")
	}
	if p.memory < 8*1024 || p.memory > 1024*1024 || p.time < 1 || p.time > 16 || p.threads < 1 {
		return phc{}, errors.New("an argon2id hash's settings are out of bounds")
	}
	var err error
	b64 := base64.RawStdEncoding
	if p.salt, err = b64.DecodeString(parts[4]); err != nil || len(p.salt) < 8 {
		return phc{}, errors.New("an argon2id hash's salt")
	}
	if p.key, err = b64.DecodeString(parts[5]); err != nil || len(p.key) < 16 || len(p.key) > 64 {
		return phc{}, errors.New("an argon2id hash's key")
	}
	return p, nil
}
