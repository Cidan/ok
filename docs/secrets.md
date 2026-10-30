# Secrets — `//secret`, kept by reference, held by `ok secrets`

> **Status: built (2026-10-23).** A repository keeps its secrets in the folder
> `//secret` by reference — the tree, `acl.yaml`, pointers and their commitments, the rules upload
> and publish enforce — and ok holds the values in a role of its own, `ok secrets`, under a key per
> repository. A person **unseals** a secret before each read, and a folder may require a
> **passkey** to; values never pass through the SCM; and post runs — of changes reviewed and
> submitted — may read what a folder grants their job, beside deploys. [Progress](#progress) says
> how each part was proven.

Related: [scm.md](scm.md) (the SCM, upload and publish), [okfs.md](okfs.md) (virtual workspaces,
where people read and write secrets), [identity.md](identity.md) (ACLs, passkeys),
[deploy.md](deploy.md) (migrations), [automation.md](automation.md) (post), [hosted.md](hosted.md)
(cells, moves).

## What it is

A repository keeps its secrets — passwords, keys, tokens, certificates — in a folder at its root,
`//secret`, a tree of any depth. Each folder holds an `acl.yaml` naming the ACLs whose members
**read** its secrets and those whose members **write** them; `OWNERS` files there decide who
**approves** a change, as anywhere, and confer no access. Every other file is a **secret**: its
value never enters the repository. What the repository holds at a secret's path is a **pointer** —
one line naming a commitment to the value — and the value is held by **ok secrets**, sealed under
a key of the repository's own.

```
secret/
  OWNERS                user: @ada                               approves changes here
  acl.yaml              readers: [ops.acl.example.com]           secret/'s own secrets
                        writers: [ops.acl.example.com]
  prod/
    acl.yaml            readers: [prod-db.acl.example.com]
                        writers: [prod-db.acl.example.com]
                        unseal: passkey                          reading needs a passkey, each time
                        deploys: [//services/api/.ok/workflow/deploy.yaml:deploy:api]
    db-password         ok-secret v1 blake3:9f2c…                the value is in ok secrets
```

A person reads a secret in a **virtual workspace** (okfs): `jj secret unseal //secret/prod/db-password`
lets them read it **once** — the next open of the file serves its value, fetched from ok secrets,
and the one after fails until they unseal it again. A folder whose `acl.yaml` says `unseal:
passkey` asks for their passkey each time. Writing needs no unsealing: a writer writes the file,
and the workspace's next snapshot stores the value and puts its pointer in the tree. Nothing else
ever holds a value: a real checkout, a worker running pre, ok control's pages, `jj file show`, and
every jj object hold pointers only. A deploy of `main`, and a post run of a submitted change, read
exactly what their folders' `acl.yaml` grant them, with no person present.

## Decisions

1. **Secrets by reference, in the tree.** The folder `//secret` (in a repository whose settings
   on `main` name a store), an `acl.yaml` in every folder with `readers` and `writers` — ACLs
   only, no inheritance, neither implying the other — `OWNERS` approving and nothing more,
   pointers with hiding commitments bound to the repository and the path, access decided by
   `main`, and plaintext never a jj object.
2. **Secrets are special: a role of their own.** `ok secrets` holds every value, every key below
   the root, every unseal, and its own audit, in its own database, under its own principal. The SCM
   holds pointers and enforces the tree's rules; it never holds a value, and a dump of its database
   holds no key material.
3. **Values never pass through the SCM.** People's workspaces, the deployer, and workers reach ok
   secrets directly. ok secrets decides access itself: it reads the folder's `acl.yaml` at `main`'s
   head from the SCM and checks membership with ok identity. A compromised SCM must forge `main` to
   misdirect a secret, which its history shows, rather than simply read one.
4. **A key per repository; nothing stored bare.** Each value is sealed under a data key of its own;
   each data key is wrapped by its repository's key; each repository key by a **root key** that
   never leaves a key management service — or, on one machine, a keyring outside the database.
   Deleting a repository's keys destroys its every secret, backups included; rotating one
   repository's key touches no other.
5. **People unseal; robots are granted.** A person reads a secret only after unsealing it, and an
   unseal is good for one read, for ten minutes. A folder may require a passkey for its unseals
   (`unseal: passkey`). Robots never unseal: a deploy of a commit of `main`, and a post run of a
   change reviewed and submitted, read what their folder's `acl.yaml` on `main` names them under
   `deploys:` and `runs:`, and nothing else does — not pre, which runs a CL's unreviewed code. A
   post run runs before its change lands, on its candidate, but on code that was approved: its
   reading a secret takes the job's stanza naming it, approved by the workflow file's `OWNERS`, and
   the folder granting the job, approved by the folder's.
6. **Stores are pluggable; values are always sealed.** The sealed value is kept in ok secrets'
   database by default, or in an external store — HashiCorp Vault's KV, AWS Secrets Manager, Google
   Secret Manager — that the repository's settings name among those the installation configures.
   ok seals before it stores, so an external store holds ciphertext, and crypto-shredding a
   repository works whatever holds its values.

## The tree

Its rules:

- **Names.** A folder's or a secret's name is `[A-Za-z0-9][A-Za-z0-9._-]*`, at most 255 bytes.
- **`acl.yaml`**:

  ```yaml
  readers: [prod-db.acl.example.com, oncall.acl.example.com]   # ACLs, each once; [] for none
  writers: [prod-db.acl.example.com]
  unseal: passkey               # optional: device (the default) or passkey
  deploys:                      # optional: deploys by identity, //<dir>/.ok/workflow/<file>.yaml:deploy:<name>
    - //services/api/.ok/workflow/deploy.yaml:deploy:api
  runs:                         # optional: post jobs by identity, //<dir>/.ok/workflow/<file>.yaml:post:<job>
    - //.ok/workflow/ci.yaml:post:push
  ```

  Every ACL is one ok identity knows, checked at upload. A name is presented as the installation
  presents names (`identity.Names`): `prod-db.acl.example.com`, or `@acme/prod-db` in a hosted
  service.
- **`OWNERS`** files are the repository's OWNERS config, validated at upload, and no secret.
- **A secret** is a regular, non-executable file holding exactly a pointer:

  ```
  ok-secret v1 blake3:<64 lowercase hex digits>
  ```

  Its value, up to 1 MiB, may be anything, the empty value included.

## Pointers and commitments

```
digest = BLAKE3-256 in derive-key mode, context "ok 2026-10-22 secret commitment v1",
         over  u64le(repository id) ‖ u64le(len(path)) ‖ path ‖ opening (32 bytes) ‖ value
```

The opening is 32 bytes ok secrets draws for each value written and keeps sealed with it; only a
reader of the value receives it. So a digest hides the value — a guess cannot be checked against it
— binds exactly one value, and names nothing at another path or in another repository. The model is
`internal/secrets` (Go) and `jj/secret` (Rust), held together by golden vectors
(`internal/secrets/testdata/vectors.json`).

## Keys

```
root key          in a KMS (GCP Cloud KMS, AWS KMS) or a keyring file; never in a database
 └─ repository key   AES-256, one per repository (versioned); stored wrapped by the root
     └─ data key       AES-256, one per secret version; stored wrapped by the repository key
         └─ the version   AES-256-GCM over opening ‖ value
```

- **Wrapping.** Each wrap is AES-256-GCM (the root's is the KMS's own encryption) and binds, as
  associated data, what it is for: the repository, the key's version, and — for a data key and a
  value — the path and the digest. No ciphertext can be moved to another row and opened.
- **Roots.** The installation's configuration names its root (`secrets.root`): `keyring:<file>`, a
  file of AES-256 keys by ID (the first the primary) for an installation on one machine and for
  tests; `gcp-kms:projects/…/cryptoKeys/…`; or `aws-kms:arn:aws:kms:…`. An organization may name its
  own root, a key in its own KMS that ok's workload is granted to use (`secrets_root`, an
  organization setting its owners set with `Tenancy.SetSecretsRoot`; never a keyring): its
  repositories' keys are wrapped by it, and revoking ok's grant locks ok out of every one. A
  repository key records the root that wrapped it, so roots can change: ok secrets reads a
  repository's root from ok identity (`SecretsRoots`, its alone), kept a minute; the first write
  after it changed makes a fresh repository key under the new root, and the next daily pass
  rewraps the rest under it and retires every key the old one wraps. An installation's
  `secrets.root` changes the same way: keep the old root reachable until that pass.
- **Caching.** An unwrapped repository key is kept in memory for ten minutes; a data key never.
- **Rotation.** `ok secrets rotate -repository R` makes a repository's next key version, which every
  new value is sealed under; `ok secrets rewrap` re-wraps every data key under its repository's
  newest key and every repository key under its root's current version, after which old versions
  are deleted. A KMS root rotates as its KMS does: a new primary encrypts, every version decrypts.
- **Destruction.** Destroying a version deletes its sealed value and wrapped data key. Deleting a
  repository deletes its repository keys first: whatever survives — a backup, an external store's
  copy — is ciphertext no key opens.
- **Moves.** Roots are the installation's or an organization's, never a cell's, so a network
  moves between cells with its repository keys and versions as they are (`move.Role`): the
  destination unwraps with the same roots.

## ok secrets

A role of every cell, as the SCM is: `ok secrets serve`, its own database (`secrets` — a
PostgreSQL schema, or a Spanner database), its robot (`roles.secrets`), and its own address in the
cell (`Cell.secrets`), which the edge routes `ok.secrets.*` to by the repository a request names.

| Table | Holds |
|---|---|
| `repository_key` | each repository's keys: version, the root that wrapped it, the wrapped key, when made and retired |
| `secret_version` | each version: repository, path, digest, key version, wrapped data key, the sealed value (or its external store's reference), who wrote it and when, when it became unreferenced, when destroyed |
| `unseal` | each person's unseals: repository, path, principal, expiry, an approval it waits for, when used |
| `audit` | every decision: who, for whom, what, which digest's prefix, the outcome, when |

### Who reads and writes

With F a secret's folder, and `F/acl.yaml` read from `main`'s head:

- **write** at a path in F: a member of one of its `writers`, or anyone where `main` has no
  `F/acl.yaml` yet (a folder still in review: what is written there is inert until it lands);
- **read** a version, as a person: a member of one of its `readers` holding an unused, unexpired
  unseal of that path — or its writer, while still a writer of F, reading back what they wrote;
- **read** as a deploy: the deployer, for a commit on `main`, a secret that commit's deploy stanza
  names and `F/acl.yaml` on `main` lists the deploy under `deploys:`;
- **read** as a run: the worker holding a post run's lease — a run of a submitted change's
  attempt, running — a secret the job's stanza at the run's commit names and `F/acl.yaml` on
  `main` lists the job under `runs:`.

Membership is ok identity's `CheckMembership`, cached ten seconds; `acl.yaml` is read again when
`main` moves. Every decision is audited.

### Unsealing

`jj secret unseal //secret/prod/db-password [more …]` — paths from the repository's root (`//`) or
from the working directory — asks ok secrets for an unseal of each. Where the folder's `acl.yaml`
says `unseal: device` (the default), the person's device credential is enough, and the unseal is
granted at once. Where it says `unseal: passkey`, ok secrets asks ok identity to start an approval
(`Approvals.StartApproval`, ok secrets' alone) of exactly those paths, for five minutes: jj prints
(and opens) its page, `/id/approve/<id>`, where the person — signed in, however — approves with one
of their own passkeys, user verification required, or declines; jj waits, asking again with the
approval's ID as `jj auth login` does, and the unseals are granted once, however often it asks. A
person with no passkey is told to add one; an installation whose public URL is an address, which
WebAuthn binds no passkey to, refuses such folders' unseals. An unseal is good for **one read** of that
path, by that person, for ten minutes: the read uses it in the transaction that serves the value.
`jj secret status` lists a workspace's unseals.

### The services

| Service, RPC | Called by | Does |
|---|---|---|
| `Secrets.WriteSecret(repository, path, value)` | people | a new version: a fresh opening and data key, the digest, the sealed value stored; returns the pointer |
| `Secrets.ReadSecret(repository, path, digest)` | people | the value and opening, using an unseal |
| `Secrets.Unseal(repository, paths, approval)` | people | unseals, or an approval to wait for: called again naming it |
| `Secrets.ListUnseals(repository)` | people | the caller's unused, unexpired unseals |
| `Secrets.CheckAccess(repository, path)` | people | whether the caller reads, writes, and must use a passkey there; whether the repository keeps secrets |
| `DeploySecrets.ReadDeploySecret(repository, commit, deploy, path)` | the deployer | as above, for deploys |
| `RunSecrets.ReadRunSecret(repository, run, path)` | workers | as above, for post runs |
| `Versions.CheckPointers(repository, pointers)` | the SCM | whether each pointer names a version ok secrets keeps at its path |
| `Versions.CheckWriters(repository, principal, folders)` | the SCM | whether a principal writes each folder, on `main` |

## The SCM

The SCM keeps no value and decides no read; it enforces the tree, asking ok secrets what only ok
secrets knows:

- **A repository keeps secrets** when `.ok/repository.yaml` on `main` has a `secrets:` stanza:

  ```yaml
  secrets:
    store: default     # the installation's store to keep sealed values in (default: ok secrets' database)
  ```

- **Upload** refuses (`UploadRejection.Reason.INVALID_SECRETS`, every problem listed) a commit whose
  `//secret` breaks the tree's rules; whose pointers ok secrets does not keep at their paths
  (`CheckPointers`); or that changes a secret in a folder `main` has, uploaded by someone ok secrets
  says is no writer of it (`CheckWriters`). A rollback changing a secret is held to the same.
- **Publish** refuses a client that does not promise to guard secrets (`guards_secrets`) in a
  repository that keeps them, and a commit or checkpoint whose `//secret` holds anything but a
  pointer, an `acl.yaml`, or an OWNERS file.
- **References.** ok secrets' daily collection per repository asks the SCM which pointers `main`'s
  head, every open CL's current patchset, and every workspace's latest checkpoint hold
  (`Secrets.ReferencedPointers`, on the SCM); a version none holds is marked, and destroyed thirty
  days after. Each repository's collection is keyed work (hosted.md, "Work without leaders"),
  booked a day on by the transaction that makes its first key, and by each pass for the next day's.

## Clients

- **ok's jj** refuses to store any file under `secret/` that is not a pointer, an `acl.yaml`, or an
  OWNERS file, in every write it makes, and sets `guards_secrets` on publishes. It asks ok secrets
  whether a repository keeps secrets (`CheckAccess`), and takes its own settings' word in a virtual
  workspace.
- **A real checkout** holds pointers: `cat secret/prod/db-password` prints the pointer line. `jj
  secret cat //path` unseals and prints a value; `jj secret write //path` keeps its standard
  input as a new version and writes the pointer to the working copy; `jj secret status` lists the
  person's unused unseals.
- **A virtual workspace** (Linux, FUSE) serves a secret as a file whose contents are its value:
  - *Reading.* Opening a secret for reading fetches its value (`ReadSecret`), using an unseal, checks
    it against the pointer, and keeps it in the daemon's memory for that open file alone: the open
    reads it, its release forgets it. Without an unseal, the open fails with `EACCES` and the daemon
    logs what to run. A file whose contents are not a pointer (a conflict's markers) reads as it is.
  - *Attributes.* A secret is `0600`, its size 0 until it is read; reads bypass the kernel's page
    cache (`FOPEN_DIRECT_IO`), so a value never sits in kernel memory past the read.
  - *Writing.* A writer (`CheckAccess`) replaces a value (`O_TRUNC`), kept in the
    daemon's memory until the next snapshot stores it (`WriteSecret`) and writes its pointer to the
    overlay. Renames and links into, out of, or within `secret/` fail with `EXDEV`; symlinks, FIFOs,
    and sockets there with `EPERM`.
  - *macOS* (NFS): the kernel's NFS client caches file data, so no read can be made the only one;
    there a secret's file reads as its pointer, and `jj secret cat //path` unseals and prints it.

## Readers that are not people

- **Deploys.** A deploy stanza names the secrets its migrations need:

  ```yaml
  deploy:
    api:
      migrate: [//services/api/db:migrate]
      secrets: [//secret/prod/db-password]
  ```

  Upload refuses, in a workflow file a change edits, a stanza naming a secret the change does not
  hold, or whose folder's `acl.yaml` there does not give the deploy (`deploys:`); and any stanza
  naming secrets in a repository that keeps none. The deployer reads each (`ReadDeploySecret`, the
  deployer's alone) for a deployment of a commit on `main` — ok secrets reads the stanza at that
  commit, `main`'s `acl.yaml` for the folder, and the commit's pointer — and gives them to the
  deploy's migrations as files — `$OK_SECRETS_DIR/prod/db-password`, mode 0400 — in a directory of
  their own on a memory file system (`/dev/shm`, a tmpfs), removed when the migration ends; it
  replaces any value in what a migration printed before recording it. A migration an organization's
  deploy agent runs in its own cluster is sent the values with it (`MigrationRequest.secrets`), and
  the agent does the same. A deploy ok secrets refuses fails, saying why, its migration never run.
  ok's PostgreSQL migrator reads its URL from a secret given it (`-url-secret
  //secret/prod/db-url`).
- **Post runs.** A post job names the secrets it needs:

  ```yaml
  post:
    push:
      target: //images:push
      secrets: [//secret/registry/token]
  ```

  Each lease ok automation gives a worker carries a key minted for it alone (`run_key`; the run
  keeps its hash). The worker running a post run reads each secret its job names
  (`ReadRunSecret`, workers' alone) with that key: ok secrets asks ok automation (`SecretRuns`,
  ok secrets' alone) whether the run is running under the lease the key was minted for, and that it
  is a post run; then reads the job's stanza at the run's commit, `main`'s `acl.yaml` for the folder
  (`runs:`), and the commit's pointer. Only a binary, run in the job's sandbox, reads secrets — a
  post job naming secrets whose target is a test or a build is a problem in its plan, as their
  actions are cached. The values are files in a directory of the run's own on a memory file system,
  bound read-only into the sandbox alone (bubblewrap's `--ro-bind`, or shared read-only into the
  run's microVM) and named there by `$OK_SECRETS_DIR` — Bazel itself never sees them — and removed
  when the run ends. The worker replaces any value in the run's log as it is written, holding back
  only what may begin a value until the next write. Pre never reads a secret: its runs carry no
  grant, and a pre job naming secrets is refused as its file is parsed.
- **Workloads** — a deployed service reading a secret as it runs — are not served: the deploy
  agent holds no Kubernetes Secrets by design ([deploy.md](deploy.md), "Deploy agents"). Giving it
  them is its own decision.

## Stores

ok secrets keeps sealed values in its own database (`default`), or in an external store the
installation configures and a repository's settings name (`secrets.store`):

```yaml
# ok.yaml
secrets:
  root: gcp-kms:projects/acme/locations/us/keyRings/ok/cryptoKeys/root
  stores:
    vault: {kind: vault, address: https://vault.internal:8200, mount: ok, role: ok-secrets}
    aws: {kind: aws-secrets-manager, region: us-east-1}
    gcp: {kind: gcp-secret-manager, project: acme-secrets}
```

A store is a package implementing `secretstore.Store` — `Put(key, sealed)` (put-if-absent), `Get`,
`Delete` — that passes its conformance suite (`secretstore/storetest`), against a fake of the
service in the test's process. Each keeps only what ok secrets sealed, under the key
`ok/<repository>/<path>@<digest>`, and reaches its service as ok secrets' workload: no credential
is in the configuration or the repository.

- **Vault** (`secretstore/vault`, its HTTP API): a KV version 2 mount, each key a path of it,
  written once (check-and-set at version 0), its bytes base64 in the one field Vault's JSON holds;
  deleted with its metadata and every version. ok secrets logs in with its Kubernetes service
  account's token as the configured role, and again when Vault refuses the token.
- **AWS Secrets Manager** (`secretstore/awssm`): a secret per key, named by it (by its hash past
  512 characters), created once, its binary the sealed bytes after a format byte; deleted without
  a recovery window. At most 64 KiB.
- **Google Secret Manager** (`secretstore/gcpsm`): a secret per key, its ID the key's hash (an ID
  holds no slash), the key in its `ok-key` annotation, holding one version sent and read with its
  CRC32C; replicated automatically, or to the configured locations; deleted with every version.
  At most 64 KiB.

A value larger than a store holds is refused as it is written, saying so. Each version records
the store it is in, so a repository's settings naming another store changes only where new
versions go; `ok secrets transfer -to STORE REPOSITORY` moves the versions it has — each copied
into STORE, its row then naming STORE, its old copy deleted — and is run again after the
settings' change lands, for what was written meanwhile. A network moving between cells needs its
stores configured, by the same names, in the cell it moves to.

## Security

| Threat | What stops it |
|---|---|
| Someone reads the repository, public or not | Values never enter it: pointers only, whose commitments hide them |
| A dump of the SCM's database or of ok secrets' | The SCM holds pointers; ok secrets holds ciphertext under keys wrapped by a root outside it |
| A backup, or an external store, of a deleted repository | Its repository keys were deleted first: the ciphertext opens with nothing |
| A CL grants its author access | Access is read from `main`; `acl.yaml` changes need `OWNERS`, and grant nothing until they land |
| A compromised SCM | It holds no value and serves none; misdirecting ok secrets needs a forged `main`, which history shows |
| A stolen laptop, or a leaked device credential | Every read needs an unseal, one read each; a folder that needs a passkey needs the person's passkey too |
| A reader's process reads a secret again later | An unseal is one read: the daemon keeps the value for one open and forgets it |
| A CL's unreviewed code reads a secret in pre | Pre never reads one: workers read for running post runs of submitted changes alone, and only what `runs:` grants |
| A deploy or a job reads what it was not given | Two grants, each approved by its own `OWNERS`: the folder's `deploys:`/`runs:`, and the stanza's `secrets:` |
| A value leaks through a log | The deployer and the worker replace every value in what they record |
| A removed reader keeps reading | Membership is checked on every read (ten-second cache); unseals expire in ten minutes |

What it does not stop: a reader can copy what they read; so can the code a deploy or a post job
runs with what it was granted. Access to a secret is trust in its holder.

## Build plan

1. **The model**: `internal/secrets` and `jj/secret`, held together by golden vectors.
2. **ok secrets**: the role — keys (the keyring, GCP KMS, AWS KMS, each against a fake), its
   database (PostgreSQL and Spanner, one conformance suite), the services, unseals, audit,
   collection, moves; configuration, the chart, the edge, and `ok serve`.
3. **The SCM**: the tree's rules at upload, rollbacks, and publish; the references.
4. **ok identity**: approvals with a passkey.
5. **ok's jj**: the write guard, `guards_secrets`, okfs's secret files, `jj secret`.
6. **Deploys and post runs**: the stanzas' `secrets:`, the deployer's and the worker's readers.
7. **External stores**: Vault, AWS Secrets Manager, Google Secret Manager.

## Progress

- 2026-10-22 — Designed.
- 2026-10-22 — Built, steps 1–7, each with its tests: the model's golden vectors in Go and Rust;
  ok secrets' store conformance on PostgreSQL and Spanner Omni; the roles together (`TestRole`);
  ok's jj in real and virtual workspaces; post runs end to end through real automation, a worker,
  Bazel, and bubblewrap (`TestPostSecrets`); each external store against a fake of its service.
- 2026-10-23 — Proven where the fakes stood in:
  - *A deploy agent.* A deploy's migration, run by its organization's agent in its own cluster,
    reads the secret ok secrets gave the deployer for that commit of `main`
    (`TestDeployThroughAnAgent`).
  - *A microVM.* A post job in a run's microVM reads its secret, shared into the guest read-only
    (`TestMicroVMPostSecrets`).
  - *Google Cloud.* A Cloud KMS root passes the roots' conformance suite, and Secret Manager the
    stores'; ok secrets' store, its root in Cloud KMS, keeps a repository's values in Secret
    Manager, sealed, moves them out and back, rotates, rewraps, and destroys (`…Live`,
    `OK_TEST_GCP_*`).
  - *On GKE* (deploy/staging): ok secrets as its own Google service account through workload
    identity, its root a Cloud KMS key; the tree applied by the deployer; a person writing a
    secret with `jj secret write`, into Secret Manager as ciphertext, and reading it back with an
    unseal each time; a post job on a real worker reading it in its sandbox, its log naming it,
    never its value; `ok secrets transfer` out of Secret Manager and back; the audit recording each
    write, unseal, and read. It found ok secrets refused by ok identity's `Verify` and `Cells`, which
    resolve every role's callers — fixed, with identity's test of every role reaching them.
