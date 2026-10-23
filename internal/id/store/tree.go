package store

import (
	"context"
	"errors"
	"fmt"
	"time"

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
	a, _, err := s.b.TreeState(ctx)
	if err != nil {
		return nil, a, err
	}
	stored, err := s.b.TreeSets(ctx)
	if err != nil {
		return nil, a, err
	}
	out := make([]TreeSet, 0, len(stored))
	for _, st := range stored {
		ts := TreeSet{Owner: st.Owner}
		ts.Path, ts.Members = st.Path, st.Members
		ts.Kind = identity.Group
		if st.Kind == "acl" {
			ts.Kind = identity.ACL
		}
		ts.Name = identity.Root
		if ts.Path != "" {
			ts.Name = ts.Path[lastSlash(ts.Path)+1:]
		}
		out = append(out, ts)
	}
	return out, a, nil
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
	return s.b.Txn(ctx, func(tx Ops) error {
		if err := tx.LockNames(ctx); err != nil {
			return err
		}
		cur, ok, err := tx.TreeState(ctx)
		switch {
		case err != nil:
			return err
		case !ok:
			if prev.Commit != "" || prev.Sequence != 0 {
				return ErrStale
			}
		case cur.Commit != prev.Commit || cur.Sequence != prev.Sequence || sequence < cur.Sequence:
			return ErrStale
		}
		stored, err := tx.TreeSets(ctx)
		if err != nil {
			return err
		}
		before := map[string]bool{}
		for _, st := range stored {
			before[st.Name] = true
		}
		names := tree.Names()
		after := map[string]bool{}
		var sets []StoredSet
		for _, set := range tree.Sets() {
			full := names.OfSet(set)
			after[full] = true
			if !before[full] {
				held, err := tx.NameHeld(ctx, full)
				if err != nil {
					return err
				}
				if held {
					return fmt.Errorf("%s: %w", full, ErrTaken)
				}
			}
			owner, _ := tree.Owner(full)
			kind := "group"
			if set.Kind == identity.ACL {
				kind = "acl"
			}
			members := set.Members
			if members == nil {
				members = []string{}
			}
			sets = append(sets, StoredSet{Name: full, Kind: kind, Path: set.Path, Owner: owner, Members: members})
		}
		for n := range before {
			if !after[n] {
				if err := tx.RetireName(ctx, n, "deleted from the tree at "+commit); err != nil {
					return err
				}
			}
		}
		if err := tx.ReplaceTreeSets(ctx, sets); err != nil {
			return err
		}
		return tx.PutTreeState(ctx, commit, sequence)
	})
}
