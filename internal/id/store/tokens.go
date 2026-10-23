package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Cidan/ok/internal/tenancy"
)

// Tokens (docs/hosted.md, "Robots and tokens per organization"): a person's personal tokens, for
// what a device credential does not suit — a script, CI. A token narrows its holder: it reaches the
// repositories it names (none: every one its holder may) with at most its role; its holder's own
// roles still decide, and it never manages an account (the service's).

// PersonalTokenPrefix starts every personal token, so one is told from any other secret.
const PersonalTokenPrefix = "okp_"

// MaxTokenLifetime bounds how long a token lives.
const MaxTokenLifetime = 366 * 24 * time.Hour

// Token is a token, without its secret.
type Token struct {
	ID           string
	Principal    string // its holder, by ID
	Kind         string // personal, or installation
	Label        string
	Role         tenancy.Role
	Repositories []int64 // none: every one its holder may reach
	// Installation is an installation token's installation, and Owner its installing organization,
	// whose repositories alone it reaches.
	Installation string
	Owner        string
	Created      time.Time
	Expires      time.Time
	LastUsed     time.Time // zero before its first use
	Revoked      time.Time // zero while it stands
}

// NewToken is a token to make.
type NewToken struct {
	Principal    string
	Label        string
	Role         tenancy.Role
	Repositories []int64
	Lifetime     time.Duration
}

// CreateToken makes a personal token for an active person, returning its secret, which is shown
// once and kept only as its hash.
func (s *Store) CreateToken(ctx context.Context, n NewToken) (string, Token, error) {
	label := strings.TrimSpace(n.Label)
	switch {
	case label == "" || len(label) > 100:
		return "", Token{}, invalid("a token is labeled, in 1 to 100 characters, as what uses it")
	case n.Role < tenancy.Read || n.Role > tenancy.Admin:
		return "", Token{}, invalid("a token allows at most read, triage, write, maintain, or admin")
	case n.Lifetime <= 0 || n.Lifetime > MaxTokenLifetime:
		return "", Token{}, invalid("a token lives from a moment to a year")
	case len(n.Repositories) > 1000:
		return "", Token{}, invalid("a token names at most 1,000 repositories")
	}
	p, err := s.b.PrincipalByID(ctx, n.Principal)
	if err != nil {
		return "", Token{}, err
	}
	if p.Kind != Human || !p.Active() {
		return "", Token{}, invalid("%s is no active person: a personal token is a person's", p.Name)
	}
	repos := slices.Clone(n.Repositories)
	slices.Sort(repos)
	repos = slices.Compact(repos)
	for _, id := range repos {
		if _, err := s.b.Repository(ctx, id); err != nil {
			return "", Token{}, fmt.Errorf("repository %d: %w", id, err)
		}
	}
	secret := PersonalTokenPrefix + token()
	now := s.now()
	t := Token{ID: newID("tok_"), Principal: p.ID, Kind: "personal", Label: label, Role: n.Role, Repositories: repos,
		Created: now, Expires: now.Add(n.Lifetime)}
	if err := s.b.InsertToken(ctx, t, hashToken(secret)); err != nil {
		return "", Token{}, err
	}
	return secret, t, nil
}

// TokenFor is the live token a secret is — not revoked, not expired, its holder active, an
// installation token's installation standing — and its holder; ErrNotFound for none. Its last use is
// noted at most once a minute.
func (s *Store) TokenFor(ctx context.Context, secret string) (Token, Principal, error) {
	t, err := s.b.TokenByHash(ctx, hashToken(secret))
	if err != nil {
		return Token{}, Principal{}, err
	}
	now := s.now()
	if !t.Revoked.IsZero() || !now.Before(t.Expires) {
		return Token{}, Principal{}, ErrNotFound
	}
	if t.Kind == "installation" && t.Owner == "" {
		return Token{}, Principal{}, ErrNotFound // its installation is gone
	}
	p, err := s.b.PrincipalByID(ctx, t.Principal)
	if err != nil {
		return Token{}, Principal{}, err
	}
	if !p.Active() {
		return Token{}, Principal{}, ErrNotFound
	}
	if now.Sub(t.LastUsed) > time.Minute {
		if err := s.b.TouchToken(ctx, t.ID, now); err != nil {
			return Token{}, Principal{}, err
		}
		t.LastUsed = now
	}
	return t, p, nil
}

// Tokens are a person's tokens, standing or not, newest first.
func (s *Store) Tokens(ctx context.Context, principal string) ([]Token, error) {
	return s.b.Tokens(ctx, principal)
}

// RevokeToken revokes one of a person's tokens, reporting whether it stood.
func (s *Store) RevokeToken(ctx context.Context, principal, id string) (bool, error) {
	return s.b.RevokeToken(ctx, principal, id, s.now())
}
