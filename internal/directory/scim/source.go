package scim

import (
	"context"
	"strings"

	"github.com/Cidan/ok/internal/directory"
)

// Source is what clients provisioned, as a directory.Source: a user is a person (their handle the
// part of their userName before any @), a group a group. It reads its store whole, which is
// local, so it says no deltas.
type Source struct{ Store Store }

// Changes says no deltas.
func (Source) Changes(context.Context, string) (*directory.Delta, error) {
	return nil, directory.ErrNoDelta
}

// Snapshot is everything provisioned.
func (s Source) Snapshot(ctx context.Context) (*directory.Snapshot, error) {
	users, err := s.Store.Users(ctx)
	if err != nil {
		return nil, err
	}
	groups, err := s.Store.Groups(ctx)
	if err != nil {
		return nil, err
	}
	snap := &directory.Snapshot{}
	isUser, isGroup := map[string]bool{}, map[string]bool{}
	for _, u := range users {
		handle, _, _ := strings.Cut(u.UserName, "@")
		snap.People = append(snap.People, directory.Person{
			ExternalID: u.ID, Handle: handle, Email: strings.ToLower(u.PrimaryEmail()), Name: u.FullName(), Active: u.Active,
		})
		isUser[u.ID] = true
	}
	for _, g := range groups {
		isGroup[g.ID] = true
	}
	for _, g := range groups {
		dg := directory.Group{ExternalID: g.ID, Name: g.DisplayName}
		for _, m := range g.Members {
			switch {
			case isUser[m]:
				dg.Members = append(dg.Members, directory.Member{ExternalID: m})
			case isGroup[m] && m != g.ID:
				dg.Members = append(dg.Members, directory.Member{ExternalID: m, Group: true})
			}
		}
		snap.Groups = append(snap.Groups, dg)
	}
	return snap, nil
}
