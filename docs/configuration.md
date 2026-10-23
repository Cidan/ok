# Configuring ok

One YAML file configures every role of an installation; each role reads the sections it needs
(`ok identity serve`, `ok scm serve`, `ok control serve`, `ok automation serve`, `ok worker run`,
and `ok deploy serve`, each `-config ok.yaml`), or several server roles in one process (`ok serve`,
below). `ok config check -config ok.yaml` reads a configuration as every role does. Unknown keys are refused, so
a misspelling is never a silently missing setting. Secrets are never in the file: a field ending in
`_file` names a file holding the secret — a mounted Kubernetes Secret — read when the role starts.

```yaml
domain: example.com                     # names are ada.human.example.com, ci.robot.example.com
installation: Example ok                # shown on the pages
public_url: https://ok.example.com      # where people and command lines reach ok
listen: ":8472"                         # each role's one port: gRPC, REST, and pages
metrics_listen: ":8473"                 # its Prometheus metrics
tls: {cert: /etc/ok/tls/tls.crt, key: /etc/ok/tls/tls.key}   # omit behind a TLS proxy or mesh
database:
  url_file: /etc/ok/secrets/database-url  # postgres://ok:…@db:5432/ok
  max_connections: 20                   # each role's pool
bus:                                    # how the roles hear of events and hand out work
  stream: {type: postgres}              # the default: in the database above, schema bus
  queue: {type: postgres}
roles:                                  # the robots ok's roles run as: <name>.robot.<domain>
  identity: identity.ok
  scm: scm.ok
  control: control.ok
  automation: automation.ok
  worker: worker.ok
  deploy: deploy.ok
services:                               # where the roles reach each other
  tls: true
  ca_file: /etc/ok/tls/ca.crt
  identity: ok-identity.ok.svc:8472
  scm: ok-scm.ok.svc:8472
  automation: ok-automation.ok.svc:8472
  deploy: ok-deploy.ok.svc:8472
workload:                               # how a role other than identity proves itself
  token_file: /var/run/secrets/ok/token
identity:
  ca_dir: /etc/ok/ca
  bootstrap_admins: [ada]               # administer until a tree declares admins.acl
  tree_repository: infra                # the repository whose groups-and-ACLs tree is ok's
  session: {lifetime: 168h, idle: 12h}
  sign_in:
    oidc:
      - id: google
        name: Google
        issuer: https://accounts.google.com
        client_id: 1234.apps.googleusercontent.com
        client_secret_file: /etc/ok/secrets/google-client-secret
        hosted_domain: example.com       # Google: only your Workspace's accounts
        # ca_file: /etc/ok/secrets/corp-ca.crt   # a provider behind your own CA
  system_of_record:
    interval: 10m
    google:
      key_file: /etc/ok/secrets/directory-sync.json
      subject: admin@example.com
  federation:
    - issuer: https://kubernetes.default.svc.cluster.local
      audience: ok
      jwks_url: https://kubernetes.default.svc/openid/v1/jwks
      ca_file: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
      token_file: /var/run/secrets/kubernetes.io/serviceaccount/token
  robots:
    - name: scm.ok
      bindings:
        - {issuer: "https://kubernetes.default.svc.cluster.local", subject: "system:serviceaccount:ok:scm"}
scm:
  objects:
    endpoint: https://s3.us-east-1.amazonaws.com
    bucket: example-ok-scm
    region: us-east-1                   # no key files: AWS's credential chain, or the pod's role
    # or, on Google Cloud: {type: gcs, bucket: example-ok-scm}
  email_domain: example.com             # <handle>@example.com in the commits ok writes
  gc_hour: 9                            # each repository's daily garbage collection, from 09:00 UTC
automation:
  logs: {endpoint: https://s3.us-east-1.amazonaws.com, bucket: example-ok-logs, region: us-east-1}
  speculation_depth: 8                  # how many submissions ahead the landing queue speculates on
worker:
  state: /var/lib/ok-worker             # slots' checkouts, Bazel's output bases, kept graphs
  slots: 4                              # runs at once
  run_memory_gib: 4                     # kept free under the cgroup's limit for each run starting
  checkout: virtual                     # okfs (needs /dev/fuse); real writes each commit to disk
  bazel_flags: ["--remote_cache=grpcs://cache.example.com"]
deploy:
  state: /var/lib/ok-deploy             # its checkout, kept graphs, Bazel's output base
  self: {namespace: ok, deployment: ok-deploy}
  migration_env:                        # what the migrations it runs get
    APP_DATABASE_URL: {file: /etc/ok/secrets/app-database-url}
```

## The installation

| Key | |
|---|---|
| `domain` | What every name is under: people are `<handle>.human.<domain>`, robots `<name>.robot.<domain>`, groups and ACLs `<name>.group.<domain>` and `<name>.acl.<domain>`. Choose it once: names are never reused, so changing it is a new installation. |
| `installation` | The name the pages show. Default `ok`. |
| `public_url` | Where people and command lines reach ok, with no path: the OpenID Connect callback, device-login links, and passkeys are under it. |
| `listen`, `metrics_listen` | Each role's serving and metrics addresses. Defaults `:8472` and `:8473`. |
| `tls` | The certificate a role serves with, re-read as it is renewed. Without it, roles serve cleartext HTTP/2, for a TLS proxy or service mesh in front. |
| `database` | The PostgreSQL every role keeps its tables in: `url_file` holds the URL, with its password. Each role keeps its tables in a PostgreSQL schema of its own (`identity`, `scm`, …) and makes it when it first starts, which needs the `CREATE` privilege on the database; without that, make the schemas beforehand and grant the role's user their use. |
| `bus` | How the roles hear of what happened — `stream`, which carries the SCM's events to their followers — and hand out work — `queue` (`hosted.md`, "Events and the bus"). Each names its backend's `type`: `postgres`, the default and for now the only one, keeps it in the database above, in a schema of its own (`bus`). |
| `roles` | The robot each of ok's roles runs as, by what precedes `.robot.<domain>`. ok identity makes them when it starts. A service that admits a role admits that robot. |

## One public address

People and command lines reach every role at `public_url`, so whatever serves it — an ingress, a
load balancer, a proxy — routes each request to its role by path:

| Path | Role |
|---|---|
| `/id/…`, `/ok.identity.v1.…` | ok identity: sign-in, accounts, the device login, and its services |
| `/ok.scm.v1.…` | the SCM |
| everything else | ok control: the pages, and `/ok.control.v1.…` |

A browser's session (identity's `ok_session` cookie) then reaches ok control on the same host, and a
page sends someone not signed in to `/id/login`. A route must carry HTTP/2 end to end for gRPC.

## How the roles reach each other

Every role but identity calls identity: to resolve its callers, to read the CA certificates that
credentials are checked against, and to prove itself. ok control also calls the SCM, ok automation,
and the deployer, as its own robot.

| Key | |
|---|---|
| `services.identity`, `services.scm`, `services.automation`, `services.deploy` | Each role's address, `host:port`. ok control needs identity and the SCM; without automation or the deployer, a CL's checks and deployments show as unavailable, and nothing can be submitted. Workers need identity, the SCM, and automation; the deployer, the same. |
| `services.tls`, `services.ca_file` | Whether the roles serve TLS to each other, and the CA their certificates are checked against (the system's without it). Off, roles speak cleartext on a network the installation trusts — a service mesh. |
| `workload.token_file` | A token an issuer under `identity.federation` signs for the role's robot — a projected Kubernetes ServiceAccount token with the audience that issuer expects. The role makes a key when it starts, certifies it with the token (identity's `Credentials.Certify`), renews the certificate before it expires, and certifies anew if it lapses. The robot needs a binding to the token's subject (`identity.robots`). |
| `workload.credential_dir` | Instead of a token: a key and certificate an administrator certified for the robot with `ok admin robot certify NAME -dir DIR`, renewed in place. For a role that runs where no issuer vouches for it. |

A Kubernetes pod gets its token from a projected volume:

```yaml
volumes:
  - name: ok-token
    projected:
      sources:
        - serviceAccountToken: {audience: ok, expirationSeconds: 3600, path: token}
# mounted at /var/run/secrets/ok
```

## ok identity

`identity.*` is ok identity's: its CA, its administrators before a tree is applied, sessions,
sign-in, the system of record, federated issuers, and the robots it makes. Each is described in
[identity.md](identity.md). `identity.tree_repository` names the repository whose groups-and-ACLs
tree — the folder its settings name as `identity:` — is ok's: ok control has its changes approved by
the ACLs that own what they change, pre checks every OWNERS set against it, and the deployer applies
it to ok identity once it lands. Other repositories' `acl:` owners are ok identity's applied ACLs.

## The SCM

| Key | |
|---|---|
| `scm.objects` | The object store file contents are kept in, as packs, by its `type`. **`s3`** (the default): `endpoint` (whose scheme selects TLS), `bucket`, `region`, and `access_key_file` and `secret_key_file`, without which credentials come from AWS's chain — its environment variables, its credentials file, or the instance's or pod's role; any S3-compatible store works: AWS, MinIO, Ceph, Cloudflare R2. **`gcs`**: a Google Cloud Storage `bucket`, through GCS's own API, with `credentials_file` (a service account's JSON key), without which credentials come from Google's application default chain — `GOOGLE_APPLICATION_CREDENTIALS`, or the workload's own identity; `endpoint` points it at an emulator. **`fs`**: a directory, `dir`, for one machine. |
| `scm.email_domain` | What a person's handle is an address at in the commits ok writes for them — landings and rollbacks — and what reviewers may be named by (`<handle>@<email_domain>`). Default: `domain`. |
| `scm.gc_hour` | The hour (UTC) each repository's daily garbage collection is due from. Default 9. |
| `scm.cache` | The SCM's in-memory caches, in bytes: `objects` (default 256 MiB), `chunks` (512 MiB), `diffs` (64 MiB). |

The SCM's design is [scm.md](scm.md). `ok scm fsck REPOSITORY` verifies a repository against its
invariants, and `ok scm gc REPOSITORY` runs its garbage collection now; both read the same file.

## ok control

ok control reads `database`, `roles`, `services`, `workload`, `public_url` (the links it gives, and
what a request from another site is told apart from), `installation`, and `scm.email_domain` (what
reviewers may be named by). Its tables are in the schema `control`. What it does is
[changelists.md](changelists.md); its pages are [web.md](web.md).

## ok automation

ok automation reads `database`, `roles`, `services` (identity and the SCM), `workload`, and
`automation`:

| Key | |
|---|---|
| `automation.logs` | The object store finished runs' logs are kept in, as `scm.objects` names one. |
| `automation.speculation_depth` | How many earlier submissions the landing queue speculates a submission lands after, at most. Default 8. |

Its tables are in the schema `automation`. What it does is [automation.md](automation.md).

## Workers

A worker (`ok worker run`) reads `services` (automation and the SCM), `workload`, `listen` and
`metrics_listen` (its health, ready while automation answers it, and its metrics), and `worker`:

| Key | |
|---|---|
| `worker.state` | Where it keeps its slots' checkouts, Bazel's output bases, the graphs it has queried, and its okfs daemon's state and caches. Required. |
| `worker.name` | Its name among the workers. Default: the host's (its pod's). |
| `worker.slots` | How many runs it runs at once, each in a checkout and Bazel output base of its own. Default 1. |
| `worker.run_memory_gib` | The memory it keeps free under its cgroup's limit for each run it starts beside the others. Default 4. |
| `worker.checkout` | `virtual` — each commit presented through okfs, which needs `/dev/fuse` and user namespaces, and runs ok's jj as the worker's daemon — or `real`, each commit written to disk. Default `virtual` (the Helm chart's: `real`; [install.md](install.md), "Workers' nodes"). |
| `worker.jj` | ok's jj, for virtual checkouts. Default: `jj` on the `PATH`. |
| `worker.bazel` | Bazel, or bazelisk, which runs the version a repository's `.bazelversion` names. Default: `bazelisk`, else `bazel`, on the `PATH`. |
| `worker.bazel_flags` | Flags every Bazel command gets before the repository's own: a remote cache, a disk cache, `--jobs`. |
| `worker.bwrap` | bubblewrap, which runs a run job's program and a lint's formatter. Default: `bwrap` on the `PATH`. |

A worker hands Bazel its `PATH`, proxy variables (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`), and
`DOCKER_CONFIG` — a directory whose `config.json` holds the registry credentials pushes use (a
mounted Secret) — and a `HOME` under `worker.state`, where every slot's Bazel shares its downloads.

## The deployer

The deployer (`ok deploy serve`) reads `database`, `roles`, `services` (identity, the SCM, and
automation), `workload`, `listen` and `metrics_listen`, `identity.tree_repository`, and `deploy`.
Its tables are in the schema `deploy`; what it does is [deploy.md](deploy.md).

| Key | |
|---|---|
| `deploy.state` | Where it keeps its checkout, its okfs daemon's state and caches, the graphs it has queried, and Bazel's output base. Required. |
| `deploy.self` | Its own Deployment, `namespace` and `deployment`, which it rolls last, from its old copy. Default `ok`/`ok-deploy`. |
| `deploy.checkout`, `deploy.jj`, `deploy.bazel`, `deploy.bazel_flags` | As a worker's. |
| `deploy.migration_env` | The variables the migrations it runs get, besides its `PATH` and `HOME`: where each finds its database. Each is `{value: …}` or `{file: …}`, a file holding the value (a mounted Secret). |
| `deploy.kubeconfig` | A kubeconfig file, for a deployer outside the cluster it rolls. Default: its pod's ServiceAccount. |

Each copy of the deployer is told which it is by `OK_DEPLOY_COLOR` (`blue` or `green`), or `-color`:
its pod's `deploy.ok.<domain>/color` label, through the downward API. The namespaces whose workloads
it rolls grant it access ([deploy.md](deploy.md), "Grants").

## Several roles in one process

`ok serve -config ok.yaml -roles identity,scm,automation,control` (or `-roles all`, which adds the
deployer) runs those roles in one process, on one port (`listen`): a small installation's whole ok,
as the Helm chart runs it ([install.md](install.md)). ok identity is always among them: it certifies
each other role's key in process, as that role's robot, so the process needs no token
(`workload`) and no CA certificates read over the network. The roles reach each other on the
process's own port, and any role not among them at its `services` address. With `tls`, the process
calls itself over TLS, checking its certificate as its `public_url`'s host.
