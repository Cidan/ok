package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Cidan/ok/internal/directory"
	"github.com/Cidan/ok/internal/directory/scim"
	"github.com/Cidan/ok/internal/tenancy"
	"github.com/Cidan/ok/internal/wait"
	"github.com/Cidan/ok/internal/work"
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
	// Work is its keyed work's store: mail's sending, syncs, moves, housekeeping.
	Work() work.Store
	// Waits are the live requests waiting on its subjects (internal/wait): a sync's claim.
	Waits() wait.Store
	// Ping reports whether the database answers.
	Ping(ctx context.Context) error
	// SCIM is what an identity provider provisioned (scim.Store), kept beside the directory.
	SCIM() scim.Store
	// SCIMOf is an organization's SCIM, by its ID.
	SCIMOf(org string) scim.Store
	// RelayDirectoryEvents takes up to limit pending events of repo — of every repository, for 0 —
	// no other relay holds, calls publish with them in order, and marks them published if it
	// succeeds; it returns how many it took.
	RelayDirectoryEvents(ctx context.Context, repo int64, limit int, publish func([]DirectoryEventRow) error) (int, error)
	// DirectoryEventRelayed reports whether a repository's event at seq is placed — the
	// transaction that placed it committed — and whether it no longer waits to be published; for
	// seq 0, whether none of the repository's events waits.
	DirectoryEventRelayed(ctx context.Context, repo, seq int64) (written, relayed bool, err error)
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
	// NameHeld reports whether a principal, a synced group, a tree set, or a team holds name, or
	// it is retired.
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
	// DeleteExpired deletes the sessions expired or idled out, the ceremonies expired, and the
	// approvals expired a day ago.
	DeleteExpired(ctx context.Context) error
	// InsertCeremony keeps a ceremony's state until expires.
	InsertCeremony(ctx context.Context, id, kind string, data []byte, expires time.Time) error
	// TakeCeremony deletes an unexpired ceremony of kind and returns its state; ErrNotFound when
	// none.
	TakeCeremony(ctx context.Context, id, kind string) ([]byte, error)
	// PeekCeremony returns an unexpired ceremony's state, leaving it; ErrNotFound when there is none.
	PeekCeremony(ctx context.Context, id, kind string) ([]byte, error)

	// ─── Secrets' roots ───

	// SecretsRoot is an organization's own root, by its ID; "" for none.
	SecretsRoot(ctx context.Context, org string) (string, error)
	// SetSecretsRoot names it, as a principal (by ID); "" forgets it.
	SetSecretsRoot(ctx context.Context, org, root, by string) error

	// ─── Approvals ───

	// InsertApproval records an approval a person is asked for.
	InsertApproval(ctx context.Context, a Approval) error
	// Approval reads an approval; ErrNotFound when there is none.
	Approval(ctx context.Context, id string) (Approval, error)
	// DecideApproval records a principal's answer to their approval, while it waits and has not
	// expired; false when it does not.
	DecideApproval(ctx context.Context, id, principalID string, approved bool) (bool, error)

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

	// ─── Sets, by their members (docs/identity.md, "Sets, by their members") ───
	//
	// Every set's members are indexed by their elements. These read them as the directory's rules
	// have it: a synced group or an alias whose name the applied tree declares is the tree's set; an
	// alias stands for its team, holding what the team holds, and a synced group an alias names is
	// the alias. They read outside any transaction: on Spanner, a read-write transaction uses no
	// search index.

	// SetsHolding are the sets that directly hold any of names (tree sets and synced groups, which
	// hold names), any of ids (teams, and the aliases standing for them, which hold principals by
	// ID), or any of teams (a team's parent holds the team, and so does an alias standing for that
	// parent), each with which of those it holds, sorted; ordered by set, then source.
	SetsHolding(ctx context.Context, names, ids, teams []string) ([]Holding, error)
	// SetRecords are those of names that are sets, as stored: tree sets, synced groups that stand,
	// teams, and aliases with their teams'. With tree false, a synced group or alias is read as
	// stored even where the applied tree declares its name — as a tree yet to be applied sees them
	// — and the applied tree's sets are not read.
	SetRecords(ctx context.Context, names []string, tree bool) ([]SetRecord, error)
	// PrincipalsByID reads principals by ID; an ID none has is absent.
	PrincipalsByID(ctx context.Context, ids []string) ([]Principal, error)

	// ─── Tenancy ───

	// InsertOrganization records an organization's settings; its principal is already recorded.
	InsertOrganization(ctx context.Context, o Organization) error
	// Organization reads an organization with its principal; ErrNotFound for none.
	Organization(ctx context.Context, id string) (Organization, error)
	// LockOrganization is Organization, held.
	LockOrganization(ctx context.Context, id string) (Organization, error)
	// UpdateOrganization writes an organization's settings.
	UpdateOrganization(ctx context.Context, o Organization) error
	// PutMember makes a principal a member or owner of an organization, or changes which.
	PutMember(ctx context.Context, org, principal string, role tenancy.Membership) error
	// DeleteMember takes a principal out of an organization, reporting whether it was in it.
	DeleteMember(ctx context.Context, org, principal string) (bool, error)
	// Members are an organization's members, by principal ID.
	Members(ctx context.Context, org string) ([]Member, error)
	// MembershipsOf are a principal's memberships, by organization ID.
	MembershipsOf(ctx context.Context, principal string) (map[string]tenancy.Membership, error)
	// InsertTeam records a team.
	InsertTeam(ctx context.Context, t Team) error
	// Team reads a team by name; ErrNotFound for none. LockTeam holds it.
	Team(ctx context.Context, name string) (Team, error)
	LockTeam(ctx context.Context, name string) (Team, error)
	// Teams are an organization's teams, by name; AllTeams every organization's.
	Teams(ctx context.Context, org string) ([]Team, error)
	AllTeams(ctx context.Context) ([]Team, error)
	// SetTeamMembers replaces a team's members (IDs, sorted).
	SetTeamMembers(ctx context.Context, name string, members []string) error
	// DeleteTeam removes a team.
	DeleteTeam(ctx context.Context, name string) error
	// InsertRepository records a repository's entry and returns it as stored; ErrTaken when its
	// owner has a repository of its name.
	InsertRepository(ctx context.Context, r Repository) (Repository, error)
	// Repository reads an entry by ID; ErrNotFound for none. LockRepository holds it.
	Repository(ctx context.Context, id int64) (Repository, error)
	LockRepository(ctx context.Context, id int64) (Repository, error)
	// RepositoryByName reads the entry an owner's name names; ErrNotFound for none.
	RepositoryByName(ctx context.Context, owner, name string) (Repository, error)
	// RepositoryCellCounts are how many repositories each cell holds, deleted ones but not purged
	// among them, by cell ("" for those made before there were cells).
	RepositoryCellCounts(ctx context.Context) (map[string]int64, error)
	// NetworkRepositories are a fork network's repositories, every state, by ID.
	NetworkRepositories(ctx context.Context, network int64) ([]Repository, error)
	// OwnerRegion is the region an owner (by ID) named for its new repositories; "" for none.
	// SetOwnerRegion names it; "" forgets it.
	OwnerRegion(ctx context.Context, owner string) (string, error)
	SetOwnerRegion(ctx context.Context, owner, region string) error
	// AccountQuotas are the quotas administrators set accounts, by account ID; an account with none
	// is absent. SetAccountQuota sets one; ClearAccountQuota forgets it.
	AccountQuotas(ctx context.Context, accounts []string) (map[string]Quota, error)
	SetAccountQuota(ctx context.Context, account string, q Quota) error
	ClearAccountQuota(ctx context.Context, account string) error
	// AddNotification records a notification unless its recipient has it from its source already,
	// and reports whether it did. NotificationsOf are up to limit of a person's latest (unread ones
	// alone, if asked); UnreadNotifications counts their unread ones. MarkNotificationsRead marks
	// some read, or all.
	AddNotification(ctx context.Context, n Notification) (bool, error)
	NotificationsOf(ctx context.Context, principal string, unread bool, limit int) ([]Notification, error)
	UnreadNotifications(ctx context.Context, principal string) (int64, error)
	MarkNotificationsRead(ctx context.Context, principal string, sources []string, all bool) error
	// NotificationPrefs are the kinds a person has mailed (a bit each), and whether they set them;
	// SetNotificationPrefs sets them.
	NotificationPrefs(ctx context.Context, principal string) (uint64, bool, error)
	SetNotificationPrefs(ctx context.Context, principal string, mailed uint64) error
	// QueueMail queues a message, its sending's work marked (MailWork) in the same transaction;
	// MailByID reads one; UpdateMail writes its state, attempts, next try, and last error.
	QueueMail(ctx context.Context, m Mail) error
	MailByID(ctx context.Context, id int64) (Mail, bool, error)
	UpdateMail(ctx context.Context, m Mail) error
	// InboxVersion is the version a CL's inbox entries were set at (0: never); ReplaceInbox replaces
	// them, at a version. InboxOf are up to limit of a person's entries — of a relation (0: both), of
	// active CLs unless closed — those with attention first, then the latest changed.
	InboxVersion(ctx context.Context, repo, number int64) (int64, error)
	ReplaceInbox(ctx context.Context, repo, number, version int64, entries []InboxEntry) error
	InboxOf(ctx context.Context, principal string, relation InboxRelation, closed bool, limit int) ([]InboxEntry, error)
	// SearchRepositories are up to limit repositories, in every state, whose names hold name,
	// lowercase, of owners whose handles start with owner ("" for any); SearchPrincipals up to
	// limit active people and organizations whose handles or display names hold q, lowercase.
	SearchRepositories(ctx context.Context, owner, name string, limit int) ([]Repository, error)
	SearchPrincipals(ctx context.Context, q string, limit int) ([]Principal, error)
	// ReportUsage records a cell's use this month by accounts, now. UsageElsewhere sums accounts'
	// use in a month in the cells but one: their seconds, and their running runs as reported since
	// a time.
	ReportUsage(ctx context.Context, cell, month string, usage []Usage, at time.Time) error
	UsageElsewhere(ctx context.Context, accounts []string, cell, month string, since time.Time) (map[string]Usage, error)

	// MarkWorkAt marks identity's keyed work (internal/work), due at at, in the operations'
	// transaction, which enqueues its job before it commits.
	MarkWorkAt(ctx context.Context, kind string, key int64, at time.Time) error
	// PutMove records a network's move over the one it had; LockMove reads it, held in a
	// transaction (ErrNotFound for none); Move reads it; Moves are the latest, newest first;
	// DueMoves the unfinished whose next_at has come, by network.
	PutMove(ctx context.Context, m Move) error
	LockMove(ctx context.Context, network int64) (Move, error)
	Move(ctx context.Context, network int64) (Move, error)
	Moves(ctx context.Context, limit int) ([]Move, error)
	DueMoves(ctx context.Context, now time.Time, limit int) ([]int64, error)
	// UpdateRepository writes an entry's owner, name, visibility, proposing role, and state.
	UpdateRepository(ctx context.Context, r Repository) error
	// DeleteRepository removes an entry, its grants, redirects, inbox entries, and notifications with it.
	DeleteRepository(ctx context.Context, id int64) error
	// RepositoriesOf are an owner's entries, by name.
	RepositoriesOf(ctx context.Context, owner string) ([]Repository, error)
	// ForksOf locks and reads a repository's direct forks, in any state, oldest first.
	ForksOf(ctx context.Context, parent int64) ([]Repository, error)
	// PersonalForksIn are the active forks people own in networks.
	PersonalForksIn(ctx context.Context, networks []int64) ([]Repository, error)
	// RepositoriesIn are up to limit entries in a state since before then, oldest first.
	RepositoriesIn(ctx context.Context, state RepositoryState, before time.Time, limit int) ([]Repository, error)
	// Redirect is the repository a name an owner's repository had names; ErrNotFound for none.
	Redirect(ctx context.Context, owner, name string) (int64, error)
	// PutRedirect points a name an owner's repository had at it.
	PutRedirect(ctx context.Context, owner, name string, repo int64) error
	// DeleteRedirect removes a redirect, if there is one.
	DeleteRedirect(ctx context.Context, owner, name string) error
	// PutGrant grants, or changes, a subject's role on a repository.
	PutGrant(ctx context.Context, g Grant) error
	// DeleteGrant revokes a subject's grant on a repository, reporting whether it had one.
	DeleteGrant(ctx context.Context, repo int64, subject string) (bool, error)
	// DeleteGrantsTo revokes every grant to a subject, returning the repositories it had grants on.
	DeleteGrantsTo(ctx context.Context, subject string) ([]int64, error)
	// Grants are a repository's grants, by subject.
	Grants(ctx context.Context, repo int64) ([]Grant, error)

	// ─── Organizations' robots ───

	// PutDeployAgent designates an organization's deploy agent, replacing any; DeleteDeployAgent
	// designates none. DeployAgentOf and DeployAgentByRobot read a designation; ErrNotFound when
	// there is none.
	PutDeployAgent(ctx context.Context, a DeployAgent) error
	DeleteDeployAgent(ctx context.Context, org string) error
	DeployAgentOf(ctx context.Context, org string) (DeployAgent, error)
	DeployAgentByRobot(ctx context.Context, robot string) (DeployAgent, error)
	// OwnerCells are the cells holding an owner's repositories, made ones.
	OwnerCells(ctx context.Context, owner string) ([]string, error)
	// InsertOrganizationRobot records a robot as an organization's.
	InsertOrganizationRobot(ctx context.Context, robot, org string) error
	// RobotOrganization is the organization a robot is of; ErrNotFound for none.
	RobotOrganization(ctx context.Context, robot string) (string, error)
	// OrganizationRobots are an organization's robots, by name, deleted ones too.
	OrganizationRobots(ctx context.Context, org string) ([]Principal, error)
	// InsertIssuer has an organization trust an issuer; again is no change.
	InsertIssuer(ctx context.Context, org, issuer string, at time.Time) error
	// DeleteIssuer stops an organization trusting an issuer, and removes its robots' bindings to
	// it, reporting whether it trusted it.
	DeleteIssuer(ctx context.Context, org, issuer string) (bool, error)
	// Issuers are the issuers an organization trusts, sorted.
	Issuers(ctx context.Context, org string) ([]string, error)
	// TrustsIssuer reports whether an organization trusts an issuer.
	TrustsIssuer(ctx context.Context, org, issuer string) (bool, error)
	// OrganizationBindings are an organization's robots' bindings, by robot ID.
	OrganizationBindings(ctx context.Context, org string) (map[string][]Binding, error)
	// DeleteOrganizationBinding removes a binding of an organization's robot, by its ID, reporting
	// whether it had it.
	DeleteOrganizationBinding(ctx context.Context, robot string, b Binding) (bool, error)

	// ─── Enterprise organizations ───

	// PutIdentityProvider sets an organization's identity provider; DeleteIdentityProvider removes
	// it, reporting whether there was one.
	PutIdentityProvider(ctx context.Context, p IdentityProvider) error
	IdentityProviderOf(ctx context.Context, org string) (IdentityProvider, error)
	DeleteIdentityProvider(ctx context.Context, org string) (bool, error)
	// DueIdentityProviders are the organizations whose provider's sync is due at now.
	DueIdentityProviders(ctx context.Context, now time.Time) ([]string, error)
	// IdentityProviderOrg is the organization whose provider's sync key is key; ErrNotFound for
	// none. SetIdentityProviderKey gives an organization's provider a key, if it has none.
	IdentityProviderOrg(ctx context.Context, key int64) (string, error)
	SetIdentityProviderKey(ctx context.Context, org string, key int64) error
	// PutOrganizationSync records a sync that went: the mirror it followed, what waits, its notes.
	// PutOrganizationSyncFailure one that failed.
	PutOrganizationSync(ctx context.Context, org string, mirror, waiting, notes []byte, at time.Time) error
	PutOrganizationSyncFailure(ctx context.Context, org, cause string, at time.Time) error
	// OrganizationPeople are the people an organization's provider provisioned, by their external
	// ID; InsertOrganizationPerson links one; PersonOrganization reads a principal's organization and
	// external ID, ErrNotFound for one no organization provisioned.
	OrganizationPeople(ctx context.Context, org string) (map[string]Principal, error)
	InsertOrganizationPerson(ctx context.Context, org, externalID, principal string) error
	PersonOrganization(ctx context.Context, principal string) (org, externalID string, err error)
	// UpdatePrincipal changes a principal's name, status, email, and display name.
	UpdatePrincipal(ctx context.Context, id, name string, status directory.Status, email, displayName string) error
	// UpdateSyncedTeam renames a synced team and sets its members, its grants following the name;
	// DeleteSyncedTeam removes one and its grants. Each answers the team's name before.
	UpdateSyncedTeam(ctx context.Context, org, externalID, name string, members []string) (string, error)
	DeleteSyncedTeam(ctx context.Context, org, externalID string) (string, error)
	// PutSCIMToken sets an organization's SCIM bearer token's hash; SCIMTokenOrganization reads the
	// organization a hash is of.
	PutSCIMToken(ctx context.Context, org string, hash []byte, at time.Time) error
	SCIMTokenOrganization(ctx context.Context, hash []byte) (string, error)

	// ─── Moving in ───

	// TenancyState reads the mode the directory serves, and the organization moved in; mode is ""
	// for a directory that has served none yet.
	TenancyState(ctx context.Context) (mode, movedIn string, err error)
	// PutTenancyState records them.
	PutTenancyState(ctx context.Context, mode, movedIn string, at time.Time) error
	// InsertAlias makes an old name an alias of a new one; Alias reads one, ErrNotFound for none;
	// Aliases reads them all, by old name.
	InsertAlias(ctx context.Context, old, new string) error
	Alias(ctx context.Context, old string) (string, error)
	Aliases(ctx context.Context) (map[string]string, error)
	// RenameGrantSubject moves every grant to a subject onto another, answering the repositories
	// whose grants changed.
	RenameGrantSubject(ctx context.Context, old, new string) ([]int64, error)
	// DeleteSyncedGroup forgets a synced group by name.
	DeleteSyncedGroup(ctx context.Context, name string) error
	// ClearSyncState forgets the installation's sync's mirror. The names administrators set over the
	// system of record's stay: an organization moved in syncs by them.
	ClearSyncState(ctx context.Context) error
	// MoveSCIM makes what the installation's SCIM provisioned an organization's.
	MoveSCIM(ctx context.Context, org string) error

	// ─── Deletions ───

	// InsertDeletion records a principal's deletion; ErrTaken when one is recorded already.
	InsertDeletion(ctx context.Context, d Deletion) error
	// DeletionOf reads a principal's deletion; ErrNotFound for none.
	DeletionOf(ctx context.Context, principal string) (Deletion, error)
	// RemoveDeletion forgets a principal's deletion, reporting whether there was one.
	RemoveDeletion(ctx context.Context, principal string) (bool, error)
	// DueDeletions locks and reads up to limit deletions requested before a time, skipping those
	// another transaction holds.
	DueDeletions(ctx context.Context, before time.Time, limit int) ([]Deletion, error)
	// RemoveFromTeams takes a principal out of every team.
	RemoveFromTeams(ctx context.Context, principal string) error
	// DeleteMembersOf drops every membership of an organization; DeleteMembershipsOf every one of
	// a principal.
	DeleteMembersOf(ctx context.Context, org string) error
	DeleteMembershipsOf(ctx context.Context, principal string) error
	// RevokeTokensOf revokes every live token of a principal.
	RevokeTokensOf(ctx context.Context, principal string, at time.Time) error
	// DeleteSignInsOf drops a person's password, provider links, and passkeys.
	DeleteSignInsOf(ctx context.Context, principal string) error

	// ─── Accounts ───

	// PutPassword sets a principal's password hash.
	PutPassword(ctx context.Context, principal, hash string, at time.Time) error
	// Password reads a principal's password hash; ErrNotFound for none.
	Password(ctx context.Context, principal string) (string, error)
	// InsertEmailToken records an emailed link's token by its hash.
	InsertEmailToken(ctx context.Context, hash []byte, t EmailToken) error
	// TakeEmailToken reads and removes a live token of a purpose; ErrNotFound for none.
	TakeEmailToken(ctx context.Context, hash []byte, purpose string, now time.Time) (EmailToken, error)
	// InsertLoginLink links a principal to a provider's subject; ErrTaken when it names another.
	InsertLoginLink(ctx context.Context, provider, subject, principal string, at time.Time) error
	// LinkedPrincipal reads the principal a provider's subject is linked to; ErrNotFound for none.
	LinkedPrincipal(ctx context.Context, provider, subject string) (Principal, error)
	// LoginLinks are a principal's links, by provider.
	LoginLinks(ctx context.Context, principal string) ([]LoginLink, error)
	// DeleteLoginLink removes a principal's link to a provider, reporting whether it had one.
	DeleteLoginLink(ctx context.Context, principal, provider string) (bool, error)
	// SignInFailures reads a key's failures since a time; NoteSignInFailure counts one more, in a
	// window starting at now if the last is older than since; ClearSignInFailures forgets them.
	SignInFailures(ctx context.Context, key string, since time.Time) (int, error)
	NoteSignInFailure(ctx context.Context, key string, now, since time.Time) error
	ClearSignInFailures(ctx context.Context, key string) error
	// DeleteExpiredEmailTokens removes links whose time is up.
	DeleteExpiredEmailTokens(ctx context.Context, now time.Time) error

	// ─── Apps ───

	// InsertApp records an app.
	InsertApp(ctx context.Context, a App) error
	// App reads an app by ID; ErrNotFound for none.
	App(ctx context.Context, id string) (App, error)
	// AppOfRobot reads the app a robot is; ErrNotFound for none.
	AppOfRobot(ctx context.Context, robot string) (App, error)
	// Apps are an organization's apps.
	Apps(ctx context.Context, org string) ([]App, error)
	// SetAppKey replaces an app's public key.
	SetAppKey(ctx context.Context, id string, key []byte) error
	// DeleteApp removes an app, its installations, and their tokens.
	DeleteApp(ctx context.Context, id string) error
	// InsertInstallation records an installation; ErrTaken when the app is installed there already.
	InsertInstallation(ctx context.Context, i Installation) error
	// UpdateInstallation changes an installation's role and repositories.
	UpdateInstallation(ctx context.Context, i Installation) error
	// Installation reads an installation by ID; ErrNotFound for none.
	Installation(ctx context.Context, id string) (Installation, error)
	// InstallationOf reads an app's installation in an organization; ErrNotFound for none.
	InstallationOf(ctx context.Context, app, org string) (Installation, error)
	// Installations are an organization's installations; InstallationsOfApp an app's.
	Installations(ctx context.Context, org string) ([]Installation, error)
	InstallationsOfApp(ctx context.Context, app string) ([]Installation, error)
	// DeleteInstallation removes an installation and its tokens, reporting whether there was one.
	DeleteInstallation(ctx context.Context, id string) (bool, error)

	// ─── Tokens ───

	// InsertToken records a token by its secret's hash.
	InsertToken(ctx context.Context, t Token, hash []byte) error
	// TokenByHash reads the token a secret's hash names, standing or not; ErrNotFound for none.
	TokenByHash(ctx context.Context, hash []byte) (Token, error)
	// TouchToken notes a token's use.
	TouchToken(ctx context.Context, id string, at time.Time) error
	// Tokens are a principal's tokens, newest first.
	Tokens(ctx context.Context, principal string) ([]Token, error)
	// RevokeToken revokes a principal's standing token, reporting whether there was one.
	RevokeToken(ctx context.Context, principal, id string, at time.Time) (bool, error)

	// ─── The directory's log ───

	// NextDirectorySeq takes the next place in a repository's log.
	NextDirectorySeq(ctx context.Context, repo int64) (int64, error)
	// InsertDirectoryEvent records an event at its place, pending its publishing.
	InsertDirectoryEvent(ctx context.Context, e DirectoryEventRow, at time.Time) error
	// PendStep has a step taken once the transaction commits (work.Pend): the relay of an event it
	// logged. Only in a transaction.
	PendStep(ctx context.Context, kind string, key int64, at time.Time, body []byte) error
	// DirectoryEvents are up to limit of a repository's events after a place, in order.
	DirectoryEvents(ctx context.Context, repo, after int64, limit int) ([]DirectoryEventRow, error)
	// DirectoryHeads are how far up to limit repositories' logs reach, those after one, by
	// repository.
	DirectoryHeads(ctx context.Context, after int64, limit int) ([]DirectoryHead, error)
	// AddFollower records a role as one of a topic's followers, once; Followers are a topic's, by
	// name: whom the relay enqueues for (internal/bus/follow).
	AddFollower(ctx context.Context, topic, name string) error
	Followers(ctx context.Context, topic string) ([]string, error)
}

// StoredSet is a set of the applied tree as the database keeps it: its full name, kind ("group"
// or "acl"), path, owner, and members.
type StoredSet struct {
	Name, Kind, Path, Owner string
	Members                 []string
}

// SetSource is where a set is stored.
type SetSource int

const (
	// SetFromTree is the applied tree's set, holding names.
	SetFromTree SetSource = iota + 1
	// SetFromSynced is a synced group that stands, holding names.
	SetFromSynced
	// SetFromTeam is a team, holding its members by principal ID and its child teams by name.
	SetFromTeam
	// SetFromAlias is a name a move gave up, standing for a team: it holds what the team holds.
	SetFromAlias
)

// Holding is a set holding some of what SetsHolding was asked: which, by name or ID as asked.
type Holding struct {
	Set    string
	Source SetSource
	Holds  []string
}

// SetRecord is a set as SetRecords reads it.
type SetRecord struct {
	Name   string
	Source SetSource
	// Kind ("group" or "acl"), Path, and Owner are a tree set's.
	Kind, Path, Owner string
	// Members are a tree set's and a synced group's, by name; a team's and an alias's — its
	// team's — by principal ID, sorted.
	Members []string
	// Children are a team's child teams — an alias's, its team's — by name, sorted.
	Children []string
	// Team is the team an alias stands for, and Organization a team's — an alias's team's — by ID.
	Team, Organization string
}
