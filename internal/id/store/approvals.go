package store

import (
	"context"
	"errors"
	"time"

	"github.com/Cidan/ok/internal/secrets"
)

// An approval is a person's answer, given with a passkey, to one request of ok secrets': to unseal
// exactly the secrets its scope names (docs/secrets.md, "Unsealing"). ok secrets asks for one,
// the person answers on identity's page with their passkey, and ok secrets reads the answer.

// Decisions an approval holds.
const (
	ApprovalApproved = "approved"
	ApprovalDeclined = "declined"
)

// DefaultApprovalLifetime is how long an approval waits for its person.
const DefaultApprovalLifetime = 5 * time.Minute

// Approval is one request for a person's approval.
type Approval struct {
	ID          string
	PrincipalID string
	// Scope is exactly what is approved, as the asker names it; Description, what the person
	// is shown.
	Scope, Description string
	Created, Expires   time.Time
	// Decision is ApprovalApproved, ApprovalDeclined, or "" while it waits.
	Decision string
	Decided  *time.Time
}

// ErrNoApproval is an approval to decide that is not waiting: none, another's, expired, or
// decided.
var ErrNoApproval = errors.New("no approval of yours is waiting under that ID: it may have expired, or been answered")

// StartApproval asks a person to approve what scope names, for lifetime.
func (s *Store) StartApproval(ctx context.Context, principalID, scope, description string, lifetime time.Duration) (Approval, error) {
	if lifetime <= 0 {
		lifetime = DefaultApprovalLifetime
	}
	now := s.now()
	a := Approval{ID: token(), PrincipalID: principalID, Scope: scope, Description: description, Created: now, Expires: now.Add(lifetime)}
	return a, s.b.InsertApproval(ctx, a)
}

// Approval reads an approval.
func (s *Store) Approval(ctx context.Context, id string) (Approval, error) {
	return s.b.Approval(ctx, id)
}

// DecideApproval records a person's answer to their approval: ErrNoApproval unless it is theirs,
// waiting, and unexpired.
func (s *Store) DecideApproval(ctx context.Context, id, principalID string, approved bool) error {
	ok, err := s.b.DecideApproval(ctx, id, principalID, approved)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNoApproval
	}
	return nil
}

// SecretsRoot is an organization's own root for its repositories' secrets (docs/secrets.md,
// "Keys"); "" for none, the installation's then.
func (s *Store) SecretsRoot(ctx context.Context, org string) (string, error) {
	return s.b.SecretsRoot(ctx, org)
}

// SetSecretsRoot names an organization's own root, as a principal; "" forgets it. Only a key in a
// KMS is one.
func (s *Store) SetSecretsRoot(ctx context.Context, org, root, by string) error {
	if root != "" {
		if err := secrets.ValidOrganizationRoot(root); err != nil {
			return invalid("%v", err)
		}
	}
	return s.b.SetSecretsRoot(ctx, org, root, by)
}
