# ok identity

ok identity is ok's directory of principals and how they prove who they are. Every other part of
ok asks it who a caller is and what they are in.

- **People** come from your **system of record** — Google Workspace, Microsoft Entra ID, an LDAP
  directory, SCIM provisioning, or a static file — and are named under your domain:
  `ada.human.example.com`. ok follows the system of record: a person who joins is made, one who is
  deactivated is suspended, one who is renamed keeps their history under the new name, and one who
  is deleted becomes a tombstone. **A name is never given to two principals**: a deleted person's
  name, or a renamed person's old one, is retired for good.
- **Robots** are workloads — ok's own roles, CI, deploy jobs — named `ci.robot.example.com` or
  `control.ok.robot.example.com`. Administrators make them; they sign in with a Kubernetes
  ServiceAccount's token, a CI system's OIDC token, or a key ok certified.
- **Groups and ACLs** are sets of principals. The system of record's groups are synced as
  `eng.group.example.com`. More groups, and the ACLs that authorize, are declared in a repository as
  the **groups-and-ACLs tree**, a folder per set, where nesting is ownership: changes to a set are
  approved by the ACL that owns it.
- **Organizations** own repositories (`hosted.md`, "Tenancy, as built"): `example.org.example.com`,
  whose handle no person may have, nor any person's handle an organization. An installation has one,
  made when ok identity first starts, of all its people; a hosted service has any number, each with
  its owners, members, and teams (`eng.acme.team.example.com`, sets as groups are). An organization
  acts as no one.
- **Signing in**: people sign in in a browser with OpenID Connect (Google, Entra, Okta, Keycloak,
  any), their LDAP password, or a passkey. A command line signs in the way `gh auth login` does: it
  shows a code, the person approves it in a browser, and the machine gets a **credential** — a key it
  keeps and a 24-hour certificate the identity CA issues for it, renewed silently. Each request
  proves the key, so a certificate copied off a machine is useless.

## Running it

```bash
ok identity ca init -dir /etc/ok/ca -name "Example"      # once: the CA's key, keep it secret
ok identity serve -config /etc/ok/ok.yaml
```

ok identity keeps its state in PostgreSQL, and brings the database to its schema when it starts. It
serves one port — gRPC, REST, and its pages under `/id/` — and its metrics on another.

### The configuration

ok identity reads the installation's configuration file ([configuration.md](configuration.md)):
`domain`, `public_url`, `database`, `roles`, and its own section, `identity`:

```yaml
identity:
  ca_dir: /etc/ok/ca                    # tls.crt, tls.key, and optionally trusted.crt
  bootstrap_admins: [ada]               # administer until a tree declares admins.acl
  session: {lifetime: 168h, idle: 12h}
  trusted_header: X-Auth-Principal      # optional: an authenticating proxy's header
  sign_in: {oidc: […], ldap: {…}}       # Signing in, below
  system_of_record: {interval: 10m, google: {…}}   # Systems of record, below
  federation: [{issuer: …, audience: ok}]          # Robots, below
  robots: [{name: scm.ok, bindings: [{issuer: …, subject: …}]}]
```

The OpenID Connect callback is `<public_url>/id/login/oidc/callback`; register it with your
provider.

## Systems of record

Name exactly one under `identity.system_of_record`. ok reads it at start, every `interval`, and when
an administrator runs `ok admin sync`; with several replicas, one syncs at a time.

**Google Workspace** reads users, groups, and members with the Admin SDK's Directory API, as a
service account with domain-wide delegation acting as an administrator. Delegate the scopes
`https://www.googleapis.com/auth/admin.directory.user.readonly` and
`https://www.googleapis.com/auth/admin.directory.group.readonly`. A user's handle is their primary
email's local part; suspended and archived users are suspended.

```yaml
google: {key_file: /etc/ok/secrets/sa.json, subject: admin@example.com, group_query: "email:eng*"}
```

**Microsoft Entra ID** reads users, groups, and members through Microsoft Graph as an application
with the permissions `User.Read.All` and `GroupMember.Read.All`. A user's handle is their user
principal name's local part; disabled accounts are suspended; guests are left out unless
`include_guests`.

```yaml
entra: {tenant_id: …, client_id: …, client_secret_file: /etc/ok/secrets/graph, group_filter: "startswith(displayName,'eng')"}
```

**LDAP** (Active Directory, OpenLDAP, FreeIPA) searches for people and groups under bases you name,
over TLS only. Name an immutable ID attribute — `entryUUID`, or Active Directory's `objectGUID` — so
renames are followed.

```yaml
ldap:
  url: ldaps://dc.example.com
  bind_dn: CN=ok,OU=Services,DC=example,DC=com
  bind_password_file: /etc/ok/secrets/ldap
  people:
    base_dn: OU=Staff,DC=example,DC=com
    filter: (&(objectCategory=person)(objectClass=user))
    disabled_filter: (userAccountControl:1.2.840.113556.1.4.803:=2)
    id_attribute: objectGUID
    handle_attribute: sAMAccountName
  groups: {base_dn: OU=Groups,DC=example,DC=com, filter: (objectClass=group)}
```

**SCIM** turns it around: your identity provider provisions people and groups into ok at
`<public_url>/id/scim/v2` with a bearer token, and ok syncs within seconds of each change. Configure
your provider (Okta, Entra, OneLogin, JumpCloud) with that URL and the token in `token_file` (at
least 32 characters).

```yaml
scim: {token_file: /etc/ok/secrets/scim-token}
```

**A static file** lists people and groups, for a small installation or trying ok out:

```yaml
static: {file: /etc/ok/people.yaml}
```

```yaml
# people.yaml
people:
  - {id: "1001", email: ada@example.com, name: Ada Lovelace}      # handle: ada
  - {id: "1002", handle: grace, email: grace@example.com, active: false}
groups:
  - {id: eng, name: eng, members: ["1001", "1002"]}
```

### When names collide

Two people who would get the same handle, or someone whose handle is retired, **wait**: ok does not
guess. `ok admin status` lists them, and an administrator gives each a handle with
`ok admin name EXTERNAL_ID HANDLE` (`-group` for a group's name), which the next sync applies. The
same page is at `<public_url>/id/admin`.

## Signing in

| How | Configure |
|---|---|
| OpenID Connect | `identity.sign_in.oidc`: each provider's `issuer`, `client_id`, `client_secret_file`, and `ca_file` when its certificate is from a CA the system does not trust. A person is matched by their verified email (`match: email`, the default), or by an ID the provider and the system of record share (`match: external_id`, `claim: oid` for Entra). ok reaches a provider when someone first signs in with it, and again after a failure: a provider that is down keeps only its own sign-in from working. |
| LDAP password | `identity.sign_in.ldap`: the directory, the account ok searches as, and `user_filter` with `{username}`. |
| Passkeys | Always on when `public_url` has a host name: people add them on their account page, `<public_url>/id/account`, after signing in another way. |
| A proxy | `identity.trusted_header`: a header an authenticating proxy in front of ok writes with the principal's name, and strips from every request it forwards. |

A command line signs in with `ok auth login -server https://ok.example.com`: it prints a code and
opens `<public_url>/id/device`, where the person signs in and approves it, seeing the machine's name
and address. Their account page lists every signed-in machine
and signs any out.

### Accounts, in a hosted service

A hosted service (`tenancy.mode: hosted`) has no system of record for everyone: people make their
own accounts, as `identity.sign_up` allows.

- **Email and password** (`sign_up.email`, its links mailed through `smtp`, STARTTLS required but
  to a server on loopback): `<public_url>/id/signup` takes an email, a handle, and a password — at
  least 12 characters, holding neither the handle nor the email's name — hashed with argon2id at
  once. A link mailed to the address makes the account within a day, after a button (so a mail
  scanner that follows it makes nothing), and signs the person in. The page answers the same
  whether the address has an account; one that has gets a note instead of a link. A person then
  signs in with the email and password; a wrong password and an address with no account are
  refused alike, in the same time, and ten failures in fifteen minutes stop an address's password
  sign-ins until the window passes. `<public_url>/id/reset` mails a link, good for an hour, that
  sets a new password and ends every other session (a role that resolved one lately lets it go
  within 30 seconds, as it does a revoked credential).
- **A social provider** (`sign_up.oidc`, among `sign_in.oidc`): its sign-in finds the account
  linked to the provider's subject — never one matched by email, which would hand an account to
  whoever controls that provider's claim — or, the first time, makes one under a handle the person
  chooses, with the provider's verified email if no account has it. An account links and unlinks
  providers on its page, and keeps a way in: its only link is not unlinked without a password.

Passkeys are added after, as anywhere; jj and okfs sign in as always (`jj auth login`). A person
deletes their account on its page, typing their handle: it is suspended and its repositories
deleted, an administrator can restore it within 30 days (`Admin.RestoreAccount`), and after that it
is gone for good, its handle never anyone's again.

### Enterprise organizations

An organization may bring its own identity provider (ok identity's `Tenancy.SetIdentityProvider`,
by its owners): a system of record — Google Workspace, Entra ID, LDAP, or SCIM, configured as an
installation's are, but per organization — and, optionally, its single sign-on, OpenID Connect. Its
secrets (a service account's key, a client secret, a bind password) are sealed with the
installation's key (`identity.sealing_key_file`: 32 random bytes, base64, `ok`'s `seal.NewKey`) and
never answered; one left out when the provider is set again keeps what was set. Without the key, no
organization keeps a provider.

Its sync provisions the organization's people as an installation's sync does — made, renamed,
suspended, and deleted as the provider says — named `<handle>-<org>` (`erin-acme`), so they take no
handle anyone else holds, members of the organization, and linked to the provider by their ID there;
its groups become the organization's teams, `<group>.<org>.team.<domain>`, whose members the sync
alone changes, a renamed team's grants following it. It runs every `interval_secs` (five minutes at
least), at once when an owner asks (`SyncIdentityProvider`), and soon after a SCIM provider pushes.
A SCIM provider pushes to `<public_url>/id/scim/v2/orgs/<org>` with the token `ResetSCIMToken`
makes (shown once); what it provisions is the organization's alone. ok reaches an organization's
LDAP directory and OpenID Connect provider at public addresses alone, as federation's issuers.

The organization's people sign in at `<public_url>/id/login/org/<org>`, matched among its people
alone — by the provider's verified email, or by an external ID claim (`sub`; `oid` for Entra) — and
a SCIM provider's by email, since its people are known by ok's own IDs. Removing the provider leaves
its people as they are, signing in no more through it.

### Personal tokens

Where no device signs in — a CI job, a script — a person uses a **personal token**, made on their
account page (`<public_url>/id/account`) or through the `Tokens` service: a label, the repositories
it reaches (none: every one they may reach), the most it allows (read, triage, write, maintain, or
admin), and how many days it lives (at most 366). Its secret, `okp_…`, is shown once, and ok keeps
only its SHA-256. A request presents it as `Authorization: Bearer okp_…` (jj: `OK_TOKEN`), and is
the person's, narrowed: what they may do, and no more than the token allows, on the repositories it
names. A token reaches repositories and nothing else — it signs no one in to the account pages, and
mints no token, credential, or organization. It stops working when revoked or expired, or when its
person no longer stands; a role that cached it stops within 30 seconds.

## Robots

```bash
ok admin robot create ci
ok admin robot bind ci https://token.actions.githubusercontent.com repo:acme/app:ref:refs/heads/main
```

A bound robot signs in by presenting its issuer's token as `Authorization: Bearer …`, verified
against the issuer listed under `identity.federation` and the audience you named there — never a
value another service accepts too. For a robot that runs where no issuer vouches for it, an
administrator certifies a key it holds (`Admin.CertifyRobot`), and it renews its certificate as a
command line does.

### An organization's robots

In a hosted service an organization keeps robots of its own — a CI identity, a deploy agent — which
its owners make, certify, and delete through ok identity's `Tenancy` service
(`CreateOrganizationRobot`, `CertifyOrganizationRobot`, `DeleteOrganizationRobot`), and its
members list. One is named under the organization, `ci.acme.robot.<domain>`; it is a member of
nothing, so the organization's base role gives it nothing, and it reaches the organization's
repositories through the grants they give it alone — never another organization's. It signs in with
a key an owner certified, renewing its certificate as a command line does. (An installation's
robots are its administrators', above, and are members of the installation.)

One of them may be its **deploy agent** (`DeployAgents.SetDeployAgent`, by its owners;
`GetDeployAgent`, by its members): the robot that runs in its cluster, through which ok's deployer
rolls its workloads and runs its migrations (deploy.md, "Deploy agents"). The deployer asks which
organization a connecting robot is the agent of, and the agent which cells hold its organization's
repositories (`DeployCells`) and whether a repository is its organization's (`AgentRepository`). A
deleted or suspended robot is no one's agent.

An organization's robot may sign in with a CI system's token instead, as an installation's robot does — but from an
issuer its organization trusts (`TrustIssuer`, by its owners; `ListIssuers`, by its members), and
bound by its owners (`BindOrganizationRobot`) to that issuer's subject. A token for an organization
carries the organization's ID as its audience (`ListIssuers` says it), so one minted for one
organization signs in no other's robot, and two organizations may bind the same subject, each to its
own robot. ok fetches an organization's issuer's discovery document and keys over HTTPS from
public addresses only — refusing loopback, private, link-local, and shared addresses on each
connection, after DNS — and keeps them a day (a development server, `-dev`, reaches one anywhere).
Distrusting an issuer unbinds the organization's robots from it. In GitHub Actions:

```yaml
- run: |
    token=$(curl -s -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
      "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=<the organization's audience>" | jq -r .value)
```

### Apps

An **app** is one of an organization's robots with a public key — ECDSA P-256 or Ed25519 — which
its owners make (ok identity's `Apps.CreateApp`), replace the key of, and delete. An organization's
owners install one — their own, or another organization's made public — with a role on their
repositories, the ones named or every one (`Apps.InstallApp`); installing it again changes what it
was installed with, and its tokens go. The app signs in by signing a JWT with its key — ES256 or
EdDSA; its `iss` the app's ID, its `aud` ok's public URL, issued now and expiring within ten
minutes — and handing it to `AppTokens`: `ListAppInstallations` lists where it is installed, and
`CreateInstallationToken` trades it for an installation's token, `oki_…`, good for an hour, which
it presents as `Authorization: Bearer oki_…` (jj: `OK_TOKEN`). The token is the app's robot,
narrowed to its installation: the installing organization's repositories alone, as far as the
installation allows, wherever else the app is installed. Uninstalled, or the app deleted, its tokens
are none.

### ok's own roles

Each of ok's roles runs as the robot `roles` names. ok identity proves itself with its own CA; every
other role holds a key it made when it started, certified by ok identity, and renewed before it
expires — the same credential a command line holds — so every request between roles is
authenticated the same way. A role in Kubernetes certifies its key with its ServiceAccount's token
(`workload.token_file`), which needs the robot bound to the ServiceAccount:

```yaml
identity:
  federation:
    - issuer: https://kubernetes.default.svc.cluster.local
      audience: ok
      jwks_url: https://kubernetes.default.svc/openid/v1/jwks
      ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
      token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
  robots:
    - name: scm.ok
      bindings: [{issuer: "https://kubernetes.default.svc.cluster.local", subject: "system:serviceaccount:ok:scm"}]
```

A role elsewhere uses a credential an administrator certified, `ok admin robot certify scm.ok -dir
/etc/ok/credential` (`workload.credential_dir`). Before anyone can sign in — a first start, as
[deploy/compose](../deploy/compose/README.md)'s setup is — the operator certifies one from identity's
database and CA instead: `ok identity certify -config ok.yaml -dir /etc/ok/credential worker.ok`, for
a robot the configuration names. Run again, it keeps a credential good for another hour, and
otherwise certifies a new key and revokes the one it replaces; the role renews it as it runs.

## Administration

Administrators are the members of `admins.acl.<domain>`, which the groups-and-ACLs tree's root
declares; until a tree is applied, they are `identity.bootstrap_admins`.

## The groups-and-ACLs tree

One repository holds ok's tree: the one `identity.tree_repository` names, in the folder its
`.ok/repository.yaml` names as `identity:`. A folder declares an ACL (`acl.yaml`), a group
(`group.yaml`), or both, named by the folder's name — `platform/eng/acl.yaml` is `eng.acl.<domain>`, so each folder's name is
used once in the tree. The root's `acl.yaml` is `admins.acl.<domain>`, which owns itself; any other
ACL is owned by the nearest ACL in a folder above its own, and a group by the ACL in its own folder,
or else the nearest above.

```yaml
# identity/eng/acl.yaml
members:
  - ada.human.example.com
  - eng.group.example.com     # a synced group, or one the tree declares
```

A change to the tree is approved by the ACLs that own what it changes, not by OWNERS
([changelists.md](changelists.md)); pre checks every OWNERS set against it; and once it lands, the
deployer applies it to ok identity ([deploy.md](deploy.md), "The groups-and-ACLs tree"), which
refuses a tree that would delete an ACL still owning sets. `ok identity check -domain D DIR` checks a
tree, and shows each set's owner.

```
ok admin sync | status | name | robots | robot create|delete|bind|unbind|certify | credentials | revoke
```

`ok identity move-in [-config ok.yaml] [-organization HANDLE]` moves an installation in as an
organization of a hosted service, from identity's database with ok's roles stopped
([hosted.md](hosted.md), "Moving an installation in, as built"); it says what of the configuration
to change before the roles start again.

Every administrative act, sign-in, and sync is recorded in the audit table.
