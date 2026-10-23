package store

import "github.com/Cidan/ok/internal/directory/scim"

// SCIM is the store's scim.Store: what an identity provider provisioned.
func (s *Store) SCIM() scim.Store { return s.b.SCIM() }

// SCIMOf is what an organization's identity provider provisioned over SCIM.
func (s *Store) SCIMOf(org string) scim.Store { return s.b.SCIMOf(org) }
