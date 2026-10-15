// Package scim is a SCIM 2.0 service provider (RFC 7643, RFC 7644) for ok's directory: an identity
// provider — Okta, Microsoft Entra ID, OneLogin, JumpCloud — provisions people and groups into it
// over HTTP, ok keeps what it is told, and the sync reads that (Source) as its system of record.
// Users and Groups are served with the filtering, paging, and PATCH operations provisioning
// clients use.
package scim

import (
	"context"
	"errors"
	"strings"
	"time"
)

// User is a provisioned user, as ok keeps it.
type User struct {
	ID          string    `json:"id"` // ok's: stable for the user's life
	UserName    string    `json:"userName"`
	ExternalID  string    `json:"externalId,omitempty"`
	DisplayName string    `json:"displayName,omitempty"`
	Name        Name      `json:"name,omitzero"`
	Emails      []Email   `json:"emails,omitempty"`
	Active      bool      `json:"active"`
	Created     time.Time `json:"-"`
	Modified    time.Time `json:"-"`
}

// Name is a user's name parts.
type Name struct {
	Formatted  string `json:"formatted,omitempty"`
	GivenName  string `json:"givenName,omitempty"`
	FamilyName string `json:"familyName,omitempty"`
}

// Email is one of a user's addresses.
type Email struct {
	Value   string `json:"value"`
	Type    string `json:"type,omitempty"`
	Primary bool   `json:"primary,omitempty"`
}

// PrimaryEmail is the user's primary address, else the first, else a userName that is one.
func (u User) PrimaryEmail() string {
	for _, e := range u.Emails {
		if e.Primary {
			return e.Value
		}
	}
	if len(u.Emails) > 0 {
		return u.Emails[0].Value
	}
	if strings.Contains(u.UserName, "@") {
		return u.UserName
	}
	return ""
}

// FullName is what to call the user.
func (u User) FullName() string {
	switch {
	case u.DisplayName != "":
		return u.DisplayName
	case u.Name.Formatted != "":
		return u.Name.Formatted
	}
	return strings.TrimSpace(u.Name.GivenName + " " + u.Name.FamilyName)
}

// Group is a provisioned group: its members are users' and groups' IDs.
type Group struct {
	ID          string    `json:"id"`
	DisplayName string    `json:"displayName"`
	ExternalID  string    `json:"externalId,omitempty"`
	Members     []string  `json:"-"`
	Created     time.Time `json:"-"`
	Modified    time.Time `json:"-"`
}

// Errors a Store answers with.
var (
	ErrNotFound = errors.New("not found")
	// ErrConflict is a userName or displayName another resource has.
	ErrConflict = errors.New("taken by another resource")
)

// Store keeps what was provisioned. userName and displayName are unique, case-insensitively.
type Store interface {
	CreateUser(ctx context.Context, u User) (User, error)
	ReplaceUser(ctx context.Context, u User) (User, error)
	User(ctx context.Context, id string) (User, error)
	Users(ctx context.Context) ([]User, error)
	DeleteUser(ctx context.Context, id string) error
	CreateGroup(ctx context.Context, g Group) (Group, error)
	ReplaceGroup(ctx context.Context, g Group) (Group, error)
	Group(ctx context.Context, id string) (Group, error)
	Groups(ctx context.Context) ([]Group, error)
	DeleteGroup(ctx context.Context, id string) error
}
