package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/lock"
)

// Backend is identity's database: what each database it runs on implements (postgres), and the
// conformance suite in storetest checks. Its operations are the database's steps; the rules they
// make up — a name is given once and never again, a deletion retires the name and revokes what
// the principal held, a device login is approved once — are Store's, written once.
type Backend interface {
	Ops
	// Txn runs fn in one read-write transaction, again if the database aborts it to break a
	// deadlock or a conflict; fn has no effects outside the transaction.
	Txn(ctx context.Context, fn func(tx Ops) error) error
	// Lock runs fn holding the lock id in bucket, which one holder has at a time, waiting up to
	// wait for it (0: not at all), with a context that ends if the lock is lost. It returns
	// lock.ErrNotAcquired when it could not take it, and otherwise fn's error.
	Lock(ctx context.Context, bucket, id string, wait time.Duration, fn lock.LockFn) error
	// Ping reports whether the database answers.
	Ping(ctx context.Context) error
	// SCIM is what an identity provider provisioned (scim.Store), kept beside the directory.
	SCIM() scim.Store
}

// Ops are the database's operations. Outside a transaction each is its own; inside Txn they are
// the transaction's, and a Lock… read holds what it read against other writers until it ends.
type Ops interface {
	// ─── Principals and names ───

	// PrincipalByName, PrincipalByID, and PrincipalByExternalID read a principal; ErrNotFound
	// when there is none.
	PrincipalByName(ctx context.Context, name string) (Principal, error)
	PrincipalByID(ctx context.Context, id string) (Principal, error)
	PrincipalByExternalID(ctx context.Context, externalID string) (Principal, error)
	// LockPrincipalByName is PrincipalByName, held.
	LockPrincipalByName(ctx context.Context, name string) (Principal, error)
	// Principals lists principals by name, as f selects them.
	Principals(ctx context.Context, f Filter) ([]Principal, error)
	// PeopleByEmail are up to two people whose email is email, compared case-insensitively.
	PeopleByEmail(ctx context.Context, email string) ([]Principal, error)
	// InsertPrincipal records a new principal and returns it as stored.
	InsertPrincipal(ctx context.Context, p Principal) (Principal, error)
	// UpdatePerson changes the person the system of record knows by externalID, returning their
	// ID; ErrNotFound when there is none.
	UpdatePerson(ctx context.Context, externalID, name string, status directory.Status, email, displayName string) (string, error)
	// SetStatus sets a principal's status.
	SetStatus(ctx context.Context, id string, status directory.Status) error
	// LockNames, in a transaction, serializes it with every other that gives out names until it
	// ends: names live in several tables, and two must never give one name at once.
	LockNames(ctx context.Context) error
	// NameHeld reports whether a principal, a synced group, or a tree set holds name, or it is
	// retired.
	NameHeld(ctx context.Context, name string) (bool, error)
	// RetireName retires a name, once: no one will hold it again.
	RetireName(ctx context.Context, name, reason string) error
	// RetiredNames are every retired name, by name.
	RetiredNames(ctx context.Context) ([]RetiredName, error)
	// AppendAudit records what an actor did.
	AppendAudit(ctx context.Context, actor, action string, detail map[string]any) error

	// ─── Robots' bindings ───

	// InsertBinding binds a principal to tokens for b; ErrBindingTaken when b names another.
	InsertBinding(ctx context.Context, principalID string, b Binding) error
	// DeleteBinding removes a robot's binding by the robot's name, reporting whether it had it.
	DeleteBinding(ctx context.Context, robot string, b Binding) (bool, error)
	// DeleteBindingsOf removes every binding of a principal.
	DeleteBindingsOf(ctx context.Context, principalID string) error
	// BoundPrincipal is the principal a binding names; ErrNotFound when none.
	BoundPrincipal(ctx context.Context, b Binding) (Principal, error)
	// Bindings are every binding, by principal ID, each principal's by issuer and subject.
	Bindings(ctx context.Context) (map[string][]Binding, error)

	// ─── Credentials ───

	// CredentialByKey finds the credential a public key is enrolled as: its ID, its owner, and
	// whether it was revoked; ok is false when the key was never enrolled.
	CredentialByKey(ctx context.Context, publicKey []byte) (id, owner string, revoked, ok bool, err error)
	// InsertCredential enrolls a key; ErrKeyEnrolled when it was enrolled already.
	InsertCredential(ctx context.Context, id, principalID, label string, publicKey []byte) error
	// Credential reads a credential with its principal; ErrNotFound when none.
	Credential(ctx context.Context, id string) (Credential, error)
	// CredentialsOf lists a principal's credentials, newest first.
	CredentialsOf(ctx context.Context, principalID string) ([]Credential, error)
	// SetIssued records a certificate's issue for a credential, valid until notAfter.
	SetIssued(ctx context.Context, id string, notAfter time.Time) error
	// RevokeCredential revokes a credential, keeping an earlier revocation's time, reporting
	// whether it exists.
	RevokeCredential(ctx context.Context, id string) (bool, error)
	// RevokeCredentialsOf revokes every standing credential of a principal.
	RevokeCredentialsOf(ctx context.Context, principalID string) error

	// ─── Device logins ───

	// InsertDeviceLogin records a device login; ErrUserCodeTaken when another holds its code.
	InsertDeviceLogin(ctx context.Context, deviceCodeHash []byte, userCode string, publicKey []byte, label, clientAddress string, expires time.Time) error
	// PendingDeviceLogin is the pending, unexpired device login a user code names; ErrNotFound
	// when none.
	PendingDeviceLogin(ctx context.Context, userCode string) (DeviceLogin, error)
	// DecideDeviceLogin moves a pending, unexpired device login to state ("approved" or
	// "denied"), as principalID, reporting whether there was one.
	DecideDeviceLogin(ctx context.Context, userCode, principalID, state string) (bool, error)
	// LockDeviceLogin reads the device login a device code's hash names, held, with when it was
	// last polled (zero: never); ok is false when there is none.
	LockDeviceLogin(ctx context.Context, deviceCodeHash []byte) (d DeviceLogin, lastPoll time.Time, ok bool, err error)
	// SetPolled records a poll of a device login, now.
	SetPolled(ctx context.Context, deviceCodeHash []byte) error
	// CompleteDeviceLogin moves an approved device login to done with its credential, reporting
	// whether it was approved.
	CompleteDeviceLogin(ctx context.Context, deviceCodeHash []byte, credentialID string) (bool, error)
	// DeleteStaleDeviceLogins deletes the device logins that expired more than a day ago, by the
	// database's clock.
	DeleteStaleDeviceLogins(ctx context.Context) error

	// ─── Sessions and ceremonies ───

	// InsertSession records a session by its token's hash.
	InsertSession(ctx context.Context, tokenHash []byte, principalID, via string, expires, idleUntil time.Time) error
	// LiveSession is the principal of the session a token's hash names, if it has neither expired
	// nor idled out, and its idle deadline; ErrNotFound otherwise.
	LiveSession(ctx context.Context, tokenHash []byte) (Principal, time.Time, error)
	// ExtendSession pushes a session's idle deadline to idleUntil, never past its expiry.
	ExtendSession(ctx context.Context, tokenHash []byte, idleUntil time.Time) error
	// DeleteSession ends a session.
	DeleteSession(ctx context.Context, tokenHash []byte) error
	// DeleteSessionsOf ends every session of a principal.
	DeleteSessionsOf(ctx context.Context, principalID string) error
	// DeleteExpired deletes the sessions expired or idled out and the ceremonies expired.
	DeleteExpired(ctx context.Context) error
	// InsertCeremony keeps a ceremony's state until expires.
	InsertCeremony(ctx context.Context, id, kind string, data []byte, expires time.Time) error
	// TakeCeremony deletes an unexpired ceremony of kind and returns its state; ErrNotFound when
	// none.
	TakeCeremony(ctx context.Context, id, kind string) ([]byte, error)

	// ─── Passkeys ───

	InsertPasskey(ctx context.Context, p Passkey) error
	// PasskeysOf lists a principal's passkeys, oldest first.
	PasskeysOf(ctx context.Context, principalID string) ([]Passkey, error)
	// Passkey reads a passkey by its credential ID; ErrNotFound when none.
	Passkey(ctx context.Context, id []byte) (Passkey, error)
	// UsePasskey records a passkey's use, now, with its updated record.
	UsePasskey(ctx context.Context, id []byte, data json.RawMessage) error
	// DeletePasskey removes a principal's passkey, reporting whether it had it.
	DeletePasskey(ctx context.Context, principalID string, id []byte) (bool, error)

	// ─── The sync ───

	// SyncedGroups are the system of record's groups, deleted ones too.
	SyncedGroups(ctx context.Context) ([]directory.SyncedGroup, error)
	// InsertSyncedGroup records a new synced group.
	InsertSyncedGroup(ctx context.Context, g directory.SyncedGroup) error
	// UpdateSyncedGroup changes the synced group the system of record knows by its ExternalID.
	UpdateSyncedGroup(ctx context.Context, g directory.SyncedGroup) error
	// SyncState is the sync's state: the mirror it followed (nil: none), its last run (zero:
	// never), its last error, and the waiting and notes it recorded, as JSON.
	SyncState(ctx context.Context) (mirror []byte, lastRun time.Time, lastError string, waiting, notes []byte, err error)
	// PutSyncState records a sync that succeeded, now: its mirror, waiting, and notes.
	PutSyncState(ctx context.Context, mirror, waiting, notes []byte) error
	// PutSyncFailure records a sync that failed, now, and why.
	PutSyncFailure(ctx context.Context, cause string) error
	// SyncNames are the names administrators gave people (Handles) and groups (GroupNames) of
	// the system of record, by external ID.
	SyncNames(ctx context.Context) (handles, groups map[string]string, err error)
	// PutSyncName sets the name a person or group of the system of record gets.
	PutSyncName(ctx context.Context, externalID string, group bool, name string) error
	// DeleteSyncName removes it.
	DeleteSyncName(ctx context.Context, externalID string, group bool) error

	// ─── The groups-and-ACLs tree ───

	// TreeState is which commit's tree is applied; ok is false before any.
	TreeState(ctx context.Context) (a Applied, ok bool, err error)
	// TreeSets are the applied tree's sets, by name, each with its full name.
	TreeSets(ctx context.Context) ([]StoredSet, error)
	// ReplaceTreeSets replaces every set of the applied tree.
	ReplaceTreeSets(ctx context.Context, sets []StoredSet) error
	// PutTreeState records the applied tree's commit and sequence, now.
	PutTreeState(ctx context.Context, commit string, sequence int64) error
}

// StoredSet is a set of the applied tree as the database keeps it: its full name, kind ("group"
// or "acl"), path, owner, and members.
type StoredSet struct {
	Name, Kind, Path, Owner string
	Members                 []string
}
