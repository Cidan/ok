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
| OpenID Connect | `identity.sign_in.oidc`: each provider's `issuer`, `client_id`, `client_secret_file`. A person is matched by their verified email (`match: email`, the default), or by an ID the provider and the system of record share (`match: external_id`, `claim: oid` for Entra). |
| LDAP password | `identity.sign_in.ldap`: the directory, the account ok searches as, and `user_filter` with `{username}`. |
| Passkeys | Always on when `public_url` has a host name: people add them on their account page, `<public_url>/id/account`, after signing in another way. |
| A proxy | `identity.trusted_header`: a header an authenticating proxy in front of ok writes with the principal's name, and strips from every request it forwards. |

A command line signs in with `ok auth login -server https://ok.example.com`: it prints a code and
opens `<public_url>/id/device`, where the person signs in and approves it, seeing the machine's name
and address. Their account page lists every signed-in machine
and signs any out.

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
/etc/ok/credential` (`workload.credential_dir`).

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

Every administrative act, sign-in, and sync is recorded in the audit table.
