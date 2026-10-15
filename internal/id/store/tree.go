package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Cidan/ok/internal/identity"
)

// ErrStale is a tree older than the one applied, or applied by another meanwhile.
var ErrStale = errors.New("the tree changed meanwhile, or is older than the one applied")

// TreeSet is a set of the applied tree, with its owner.
type TreeSet struct {
	identity.Set
	Owner string
}

// Applied is which commit's tree the store holds.
type Applied struct {
	Commit   string
	Sequence int64
	At       time.Time
}

// Tree returns the applied tree's sets, ordered by name, and which commit's they are (a zero
// Applied before any tree was applied).
func (s *Store) Tree(ctx context.Context) ([]TreeSet, Applied, error) {
	var a Applied
	err := s.pool.QueryRow(ctx, `SELECT commit, sequence, applied_at FROM tree_state`).Scan(&a.Commit, &a.Sequence, &a.At)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, a, err
	}
	rows, err := s.pool.Query(ctx, `SELECT name, kind, path, owner, members FROM tree_sets ORDER BY name`)
	if err != nil {
		return nil, a, err
	}
	defer rows.Close()
	var out []TreeSet
	for rows.Next() {
		var full, kind string
		var ts TreeSet
		if err := rows.Scan(&full, &kind, &ts.Path, &ts.Owner, &ts.Members); err != nil {
			return nil, a, err
		}
		ts.Kind = identity.Group
		if kind == "acl" {
			ts.Kind = identity.ACL
		}
		ts.Name = identity.Root
		if ts.Path != "" {
			ts.Name = ts.Path[lastSlash(ts.Path)+1:]
		}
		out = append(out, ts)
	}
	return out, a, rows.Err()
}

func lastSlash(p string) int {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return i
		}
	}
	return -1
}

// ReplaceTree makes tree the applied tree, as of commit at sequence, in one transaction: if the
// applied tree is still prev's (prev.Sequence; a zero prev for none) and sequence is not below it.
// Every set the tree adds must take a name no one holds or held, and every set it drops has its
// name retired.
func (s *Store) ReplaceTree(ctx context.Context, prev Applied, commit string, sequence int64, tree *identity.Tree) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		var cur Applied
		err := tx.QueryRow(ctx, `SELECT commit, sequence FROM tree_state`).Scan(&cur.Commit, &cur.Sequence)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			if prev != (Applied{}) {
				return ErrStale
			}
		case err != nil:
			return err
		case cur.Commit != prev.Commit || cur.Sequence != prev.Sequence || sequence < cur.Sequence:
			return ErrStale
		}
		before := map[string]bool{}
		rows, err := tx.Query(ctx, `SELECT name FROM tree_sets`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				rows.Close()
				return err
			}
			before[n] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		names := tree.Names()
		after := map[string]bool{}
		for _, set := range tree.Sets() {
			full := names.OfSet(set)
			after[full] = true
			if before[full] {
				continue
			}
			free, err := nameFree(ctx, tx, full)
			if err != nil {
				return err
			}
			if !free {
				return fmt.Errorf("%s: %w", full, ErrTaken)
			}
		}
		for n := range before {
			if !after[n] {
				if err := retire(ctx, tx, n, "deleted from the tree at "+commit); err != nil {
					return err
				}
			}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM tree_sets`); err != nil {
			return err
		}
		for _, set := range tree.Sets() {
			full := names.OfSet(set)
			owner, _ := tree.Owner(full)
			kind := "group"
			if set.Kind == identity.ACL {
				kind = "acl"
			}
			members := set.Members
			if members == nil {
				members = []string{}
			}
			if _, err := tx.Exec(ctx, `INSERT INTO tree_sets (name, kind, path, owner, members) VALUES ($1, $2, $3, $4, $5)`,
				full, kind, set.Path, owner, members); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `INSERT INTO tree_state (singleton, commit, sequence, applied_at) VALUES (true, $1, $2, now())
			ON CONFLICT (singleton) DO UPDATE SET commit = $1, sequence = $2, applied_at = now()`, commit, sequence)
		return err
	})
}
