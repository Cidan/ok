package store

import (
	"context"
	"slices"
	"strings"
)

// Search (docs/hosted.md, "ok control at scale, as built"): repositories, people, and
// organizations by part of their names.

// searchScan is how many of each kind a search reads, for the closest of those the searcher may
// see.
const searchScan = 200

// SearchRepositories are repositories whose names hold a query's — owner/name narrows them to the
// owners whose handles start with owner — the closest first: a name equal to it, then one starting
// with it, then the shorter.
func (s *Store) SearchRepositories(ctx context.Context, query string) ([]Repository, error) {
	owner, name := "", strings.ToLower(strings.TrimSpace(query))
	if o, n, ok := strings.Cut(name, "/"); ok {
		owner, name = o, n
	}
	if owner == "" && name == "" {
		return nil, nil
	}
	rs, err := s.b.SearchRepositories(ctx, owner, name, searchScan)
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(rs, func(a, b Repository) int { return closer(strings.ToLower(a.Name), strings.ToLower(b.Name), name) })
	return rs, nil
}

// SearchPrincipals are the active people and organizations whose handles or display names hold a
// query's, the closest handles first.
func (s *Store) SearchPrincipals(ctx context.Context, query string) ([]Principal, error) {
	q := strings.ToLower(strings.TrimSpace(query))
	if q == "" {
		return nil, nil
	}
	ps, err := s.b.SearchPrincipals(ctx, q, searchScan)
	if err != nil {
		return nil, err
	}
	slices.SortStableFunc(ps, func(a, b Principal) int { return closer(handleOf(a), handleOf(b), q) })
	return ps, nil
}

// handleOf is a person's or an organization's handle, lowercase: its name's first part
// (ada.human.<domain>, acme.org.<domain>).
func handleOf(p Principal) string {
	h, _, _ := strings.Cut(p.Name, ".")
	return strings.ToLower(h)
}

// closer orders a before b when a is the closer of the two to q.
func closer(a, b, q string) int {
	rank := func(s string) int {
		switch {
		case s == q:
			return 0
		case strings.HasPrefix(s, q):
			return 1
		}
		return 2
	}
	if ra, rb := rank(a), rank(b); ra != rb {
		return ra - rb
	}
	if len(a) != len(b) {
		return len(a) - len(b)
	}
	return strings.Compare(a, b)
}

// likeEscape is s as a LIKE pattern's literal part.
func likeEscape(s string) string {
	return strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(s)
}

// LikeEscape is s as a LIKE pattern's literal part: a backend's.
func LikeEscape(s string) string { return likeEscape(s) }
