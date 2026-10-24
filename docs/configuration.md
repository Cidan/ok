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
tenancy: {mode: installation}           # one organization, example, of the installation's people
public_url: https://ok.example.com      # where people and command lines reach ok
listen: ":8472"                         # each role's one port: gRPC, REST, and pages
metrics_listen: ":8473"                 # its Prometheus metrics
tls: {cert: /etc/ok/tls/tls.crt, key: /etc/ok/tls/tls.key}   # omit behind a TLS proxy or mesh
database:
  url_file: /etc/ok/secrets/database-url  # postgres://ok:…@db:5432/ok
  max_connections: 20                   # each role's pool
  # or Spanner, each role in a database of its own, <prefix>-<role>, made when the role first
  # starts.
  # Its bus is another backend's:
  # Spanner has no bus of its own.
  # type: spanner
  # spanner:
  #   instance: projects/example/instances/ok   # Google's, reached with the workload's credentials
  #   prefix: ok                        # ok-deploy, …
  #   omni:                             # or a Spanner Omni deployment's (instance projects/default/instances/default)
  #     address: spanner-omni.db.svc:15000
  #     ca_file: /etc/ok/tls/omni-ca.crt  # or plain_text: true, for one server on its loopback
  #     username: ok                    # when it asks for a password
  #     password_file: /etc/ok/secrets/omni-password
bus:                                    # how the roles hear of events and hand out work
  stream: {type: postgres}              # the default: in the database above, schema bus
  queue: {type: postgres}
  # or NATS JetStream, for either or both (the two share a connection to the same servers):
  # stream:
  #   type: nats
  #   nats:
  #     url: nats://nats-0:4222,nats://nats-1:4222,nats://nats-2:4222
  #     credentials_file: /etc/ok/secrets/nats.creds   # the user's JWT and seed, when asked
  #     ca_file: /etc/ok/tls/nats-ca.crt              # servers under your own CA
  #     prefix: ok                      # subjects and streams; installations sharing an account
  #     replicas: 3                     # each stream on three servers
  #     retention: 168h                 # events, jobs, and dead letters kept
  #     ack_wait: 30s                   # the floor of a job's lease
  #     max_attempts: 10                # deliveries before a job is a dead letter
  #     concurrency: 16                 # events a role's replica handles at once
  # or Google Pub/Sub; its service agent publishes to the queues' dead-letter topics and acknowledges
  # on their subscriptions (roles/pubsub.publisher and roles/pubsub.subscriber), as Pub/Sub's own
  # dead lettering asks:
  # queue:
  #   type: pubsub
  #   pubsub:
  #     project: example-ok
  #     # credentials_file: /etc/ok/secrets/pubsub.json   # unset: the workload's own identity
  #     # endpoint: pubsub.us-east1.rep.googleapis.com:443  # a regional endpoint
  #     prefix: ok
  #     retention: 168h                 # at most seven days
  #     max_attempts: 10                # at most 100
  #     concurrency: 32                 # events a role's replica handles, and holds, at once
  # or Amazon SNS (the stream) and SQS. The role's identity creates the topics and queues it names
  # (sns:CreateTopic, sns:Subscribe, sns:PublishBatch; sqs:CreateQueue, sqs:GetQueueUrl,
  # sqs:GetQueueAttributes, sqs:SetQueueAttributes, sqs:SendMessage, sqs:ReceiveMessage,
  # sqs:DeleteMessage, sqs:ChangeMessageVisibility, on arn:aws:sns:*:*:ok-* and arn:aws:sqs:*:*:ok-*),
  # and sets each group's queue policy to let its topic send to it:
  # queue:
  #   type: aws
  #   aws:
  #     region: us-east-1
  #     # access_key_file: /etc/ok/secrets/aws-access-key   # unset: AWS's chain (the pod's role)
  #     # secret_key_file: /etc/ok/secrets/aws-secret-key
  #     # endpoint: http://localstack:4566                # not AWS's own
  #     prefix: ok
  #     retention: 168h                 # at most 336h
  #     max_attempts: 10
  #     concurrency: 32                 # events a role's replica handles at once
  # or Kafka, for the stream alone (it acknowledges no single message; the queue comes from
  # another backend):
  # stream:
  #   type: kafka
  #   kafka:
  #     brokers: [b-1.ok.kafka.us-east-1.amazonaws.com:9098, b-2.ok.kafka.us-east-1.amazonaws.com:9098]
  #     tls: true
  #     # ca_file: /etc/ok/tls/kafka-ca.crt  # brokers under your own CA
  #     sasl: {mechanism: aws-msk-iam}     # or plain, scram-sha-256, scram-sha-512:
  #     # sasl: {mechanism: scram-sha-512, username: ok, password_file: /etc/ok/secrets/kafka-password}
  #     prefix: ok
  #     partitions: 12
  #     replication_factor: 3
  #     retention: 168h
  #     concurrency: 32                 # events a role's replica handles at once, and fetches at most
roles:                                  # the robots ok's roles run as: <name>.robot.<domain>
  identity: identity.ok
  scm: scm.ok
  control: control.ok
  automation: automation.ok
  worker: worker.ok
  deploy: deploy.ok
  secrets: secrets.ok
services:                               # where the roles reach each other
  tls: true
  ca_file: /etc/ok/tls/ca.crt
  identity: ok-identity.ok.svc:8472
  scm: ok-scm.ok.svc:8472
  automation: ok-automation.ok.svc:8472
  deploy: ok-deploy.ok.svc:8472
  secrets: ok-secrets.ok.svc:8472
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
  # sealing_key_file: /etc/ok/secrets/sealing-key   # seals organizations' identity providers' secrets
  # sign_up:                             # a hosted service's (tenancy.mode: hosted) instead of a system of record
  #   email:                              # accounts with a verified email and a password
  #     smtp: {address: smtp.example.com:587, username: ok, password_file: /etc/ok/secrets/smtp, from: "ok <no-reply@example.com>"}
  #   oidc: [github]                      # sign_in.oidc providers whose people make accounts with them
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
  landing_batch: 32                     # how many submissions the landing queue lands at once
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
secrets:
  root: gcp-kms:projects/acme/locations/us/keyRings/ok/cryptoKeys/root   # wraps repositories' keys
```

## The installation

| Key | |
|---|---|
| `domain` | What every name is under: people are `<handle>.human.<domain>`, robots `<name>.robot.<domain>`, groups and ACLs `<name>.group.<domain>` and `<name>.acl.<domain>`. Choose it once: names are never reused, so changing it is a new installation. |
| `installation` | The name the pages show. Default `ok`. |
| `public_url` | Where people and command lines reach ok, with no path: the OpenID Connect callback, device-login links, and passkeys are under it. |
| `listen`, `metrics_listen` | Each role's serving and metrics addresses. Defaults `:8472` and `:8473`. |
| `profiling` | Serve Go's profiles (`/debug/pprof/`) on the metrics address — CPU, heap, goroutines — for what a role spends under load. The metrics address is the cluster's alone; leave it off where it is not. |
| `tls` | The certificate a role serves with, re-read as it is renewed. Without it, roles serve cleartext HTTP/2, for a TLS proxy or service mesh in front. |
| `database` | Where the roles keep their tables, by `type`. **`postgres`** (the default): `url_file` holds the URL, with its password. Each role keeps its tables in a PostgreSQL schema of its own (`identity`, `scm`, …) and makes it when it first starts, which needs the `CREATE` privilege on the database; without that, make the schemas beforehand and grant the role's user their use. **`spanner`**: `spanner.instance` (`projects/P/instances/I`), each role's tables in a database of its own there, `<prefix>-<role>` (`prefix` is `ok` unless set; at most 19 characters), which the role makes when it first starts, with the workload's Google credentials — `roles/spanner.databaseAdmin` on the instance, for the database and its schema; or, with `spanner.omni`, a Spanner Omni deployment's (`address`, and `plain_text`, or `ca_file`, and `username` and `password_file` when it asks for a password), its instance `projects/default/instances/default`. `spanner.channels` are each role's gRPC connections to Spanner (16 unless set): a replica serving thousands of requests at once makes as many calls, which Spanner's client's four by default would queue. The bus is then another backend's: `bus.stream` and `bus.queue` of type `postgres` are refused. The admin commands that work on identity's tables (`ok identity`'s, `ok move-in`, and the SCM's that name a repository) open them where the configuration says. |
| `bus` | How the roles hear of what happened — `stream`, which carries the SCM's events to their followers — and hand out work — `queue` (`hosted.md`, "Events and the bus"). Each names its backend's `type`: `postgres`, the default and for now the only one, keeps it in the database above, in a schema of its own (`bus`). |
| `roles` | The robot each of ok's roles runs as, by what precedes `.robot.<domain>`. ok identity makes them when it starts. A service that admits a role admits that robot. |
| `tenancy` | Who owns what (`hosted.md`, "Tenancy, as built"). `mode` is `installation`, the default, or `hosted`. An installation has one organization, named by `organization` — by default the domain's first label, `example` for `example.com` — which ok identity makes when it first starts: its members are the installation's people and robots, its owners ok's administrators, each member makes and lands on its repositories, and they are internal to it. People also own repositories of their own, private unless they say. A hosted service has any number of organizations, which people make and own. |

## One public address

People and command lines reach every role at `public_url`, so whatever serves it — an ingress, a
load balancer, a proxy — routes each request to its role by path:

| Path | Role |
|---|---|
| `/id/…`, `/ok.identity.v1.…` | ok identity: sign-in, accounts, the device login, and its services |
| `/ok.scm.v1.…` | the SCM |
| `/ok.secrets.v1.Secrets` | ok secrets, where it runs apart ([secrets.md](secrets.md)) |
| everything else | ok control: the pages, and `/ok.control.v1.…` |

A browser's session (identity's `ok_session` cookie) then reaches ok control on the same host, and a
page sends someone not signed in to `/id/login`. A route must carry HTTP/2 end to end for gRPC.

## How the roles reach each other

Every role but identity calls identity: to resolve its callers, to read the CA certificates that
credentials are checked against, and to prove itself. ok control also calls the SCM, ok automation,
and the deployer, as its own robot.

| Key | |
|---|---|
| `services.identity`, `services.scm`, `services.automation`, `services.deploy`, `services.secrets` | Each role's address, `host:port`. ok control needs identity and the SCM; without automation or the deployer, a CL's checks and deployments show as unavailable, and nothing can be submitted. Workers need identity, the SCM, and automation; the deployer, the same. ok secrets needs identity and the SCM; the SCM, workers, and the deployer reach it where repositories keep secrets. |
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

## Cells

An installation is one cell; a deployment of several serves one namespace from many, the edge in
front of them ([hosted.md](hosted.md), "Cells, as built"). Leave these keys unset for one.

| Key | |
|---|---|
| `cell.name`, `cell.region` | The cell a role serves: every role's but identity's, which serves them all. Default `default`, in no region. A role refuses a request about a repository another cell holds, naming that cell (`ok.v1.Moved`; a page answers 421 with an `Ok-Cell` header). |
| `identity.cells` | The cells ok identity places repositories in, each a `name`, a `region`, whether it is `closed` to new repositories, and where its roles are reached — `scm` (required), `control`, `automation`, `deploy`, `host:port` each — and `secrets`, where ok secrets runs. A new repository goes to the open cell holding the fewest, a fork to its upstream's; the first cell holds the repositories made before there were cells. ok identity moves fork networks between cells (`ok admin move REPOSITORY CELL`; [hosted.md](hosted.md), "Moving"), reaching each cell's roles at these addresses as its own robot: a cell names every role another cell runs, or no network moves into or out of it. Unset, the one cell `default`, its roles at `services`'. |
| `identity.default_region` | Where an owner's new repositories are placed when it names no region of its own (identity's `Tenancy.SetRegion`, a person's own or an organization's by its owners): an open cell of that region, any open cell when none is open there. One of `identity.cells`' regions; unset, any open cell's. |
| `identity.default_quota` | An account's quota of CI when an administrator set it none (`ok admin quota ACCOUNT`; [hosted.md](hosted.md), "Hosted automation, as built"): `concurrent` runs, run `minutes` in a calendar month (UTC), each 0 for unlimited (the default), and `weight` in fair share, 1 unless set. A quota holds across the cells an account's repositories are in. |
| `identity.mail.smtp` | The mail server people's notifications are mailed through ([hosted.md](hosted.md), "ok control at scale, as built"), as `identity.sign_up.email.smtp` names one: `address`, `username`, `password_file`, `from`, `ca_file`. Unset, notifications are kept but none is mailed. |
| `roles.edge` | The robot the edge runs as (`ok edge serve -config ok.yaml`, alone in its process, at the deployment's public address): it reaches ok identity at `services.identity`, as the other roles do, and the cells' roles where `identity.cells` says. |
| `edge.region` | The edge's region: a request that names no repository goes to an open cell there. |
| `edge.limits.principal`, `edge.limits.address` | Each principal's, and each client address's, requests at each edge replica: `per_second` and `burst`. Default 50 and 200 for a principal, 200 and 800 for an address (many people may share one). A request past them is refused: 429, or `RESOURCE_EXHAUSTED`. |
| `edge.address_header` | A header a proxy in front of the edge writes with the client's address, which the address limit counts by; without it, the connection's. |

## ok identity

`identity.*` is ok identity's: its CA, its administrators before a tree is applied, sessions,
sign-in, a hosted service's sign-up, the system of record, federated issuers, and the robots it
makes. Each is described in
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
| `scm.cache` | The SCM's in-memory caches, in bytes: `objects` (default 256 MiB), `chunks` (512 MiB), `diffs` (64 MiB), `path_commits` (32 MiB: the newest commits of main that changed paths, which a tree's listing with its entries' last commits reads), `code_indexes` (256 MiB: the code search indexes a replica holds, [hosted.md](hosted.md), "ok control at scale, as built"). |

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
| `automation.landing_batch` | How many submissions the landing queue lands in one landing — one transaction that moves main once — at most: those it would land one after another, each on the queue as the ones before leave it. Default 32; 1 lands each alone. |
| `automation.plan_pool` | The pool of workers planning runs run in. Default `default`. |
| `control.sealing_key_file` | The key repositories' webhooks' secrets are sealed under ([hosted.md](hosted.md), "ok control at scale, as built"): 32 bytes, base64, on a line — the same file in every cell, so a moved repository keeps its webhooks. Unset, repositories have no webhooks. |
| `control.webhooks.allow_private_networks` | Webhooks may reach private, loopback, and link-local addresses, and over http: an installation whose endpoints are its own. Unset, a webhook is https to a public address, checked as each connection is made. |
| `automation.cache` | Set, ok automation serves its workers' Bazel a remote cache ([hosted.md](hosted.md), "Hosted automation, as built"), in `automation.logs`' bucket under `cache/`: each run's within its account's namespace, or a pre's of someone who may not land within its author's own, reading the account's. Workers point Bazel at it themselves; give `worker.bazel_flags` no `--remote_cache` of their own. |
| `automation.cache.ttl` | How long an entry is kept after it is written. Default `720h` (30 days). |
| `automation.cache.memory_mib` | Each replica's memory of small blobs (up to 1 MiB each). Default 256. |
| `automation.pools` | The pools of workers ok automation sizes ([hosted.md](hosted.md), "Hosted automation, as built"): each a `name` (as `worker.pool` and jobs name it), its workers' `namespace` and `deployment`, each worker's `slots`, and the `min` and `max` workers. Every 15 seconds, one replica sets each Deployment's replicas through its scale subresource to the pool's running and claimable runs over its slots — up at once, down once fewer have done for five minutes, the idlest workers first. |
| `automation.kubeconfig` | A kubeconfig reaching the pools' cluster from outside it; unset, the cluster ok automation runs in. |

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
| `worker.pool` | The pool of workers it serves: it takes the runs of jobs whose workflow job, or repository (`.ok/repository.yaml` `pool:` on main), names it — `default` for jobs that name none, and planning runs unless `automation.plan_pool` says. Lowercase letters, digits, and `-`. Default `default`. |
| `worker.microvm` | Set, every run runs in a microVM of its own ([hosted.md](hosted.md), "Hosted automation, as built"): Cloud Hypervisor on KVM, the run's trust domain's checkouts and output bases shared with it through virtio-fs, no network device. The worker needs `/dev/kvm`, and ok built with `CGO_ENABLED=0` (as ok's images are), the guests' init. |
| `worker.microvm.kernel` | The guests' kernel, a bzImage or vmlinux with virtio-fs and vsock built in (Cloud Hypervisor's published kernel has them). Required. |
| `worker.microvm.hypervisor`, `worker.microvm.virtiofsd` | Cloud Hypervisor, and virtiofsd 1.13 or later (for `--readonly`). Default: `cloud-hypervisor` and `virtiofsd` on the `PATH`, else `/usr/libexec/virtiofsd`. |
| `worker.microvm.cpus`, `worker.microvm.memory_gib` | Each microVM's. Default: the node's CPUs over `worker.slots`, and `worker.run_memory_gib`. |
| `worker.microvm.share` | Host directories every guest sees read-only, beside `/usr` and what TLS trusts (and Bazel's and bubblewrap's directories): a cache of Bazel's releases, say, with `BAZELISK_HOME` and `USE_BAZEL_VERSION` in the worker's environment. |
| `worker.microvm.egress` | The hosts a run may reach, through the worker's proxy (the guest's `HTTPS_PROXY`): `host`, `*.domain`, or `host:port` (443 otherwise). Default: where Bazel and common rules download from (`config.DefaultEgress`); empty, nothing. Each host refused is said in the run's log. |
| `worker.microvm.push_egress` | What a push run may reach beside: its registries. |
| `worker.microvm.reserve_gib`, `worker.microvm.domains` | The space kept free on `worker.state`'s filesystem, and the copies of trust domains kept: past either, the least recently used go. Default 20 GiB and 32. |

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

## ok secrets

ok secrets (`ok secrets serve`) holds repositories' secrets ([secrets.md](secrets.md)). It reads
`database` (or its own, `secrets.database`), `roles` (`roles.secrets`, its robot), `services`
(identity and the SCM), `workload`, `listen` and `metrics_listen`, and `secrets`. Its tables are in
the schema `secrets`, or a Spanner database of its own.

| Key | |
|---|---|
| `secrets.root` | The root that wraps every repository's key, unless the repository's organization names its own: `keyring:<file>`, a file of AES-256 keys by ID outside any database, for an installation on one machine (`ok secrets keyring -id ID` prints a key; the first is the primary); `gcp-kms:<crypto key's resource name>`; or `aws-kms:<key ARN>` — each reached as the workload, by its default credentials. Required to run ok secrets; ok secrets fails at start on a root it cannot reach. Every replica names the same root, by the same ID (a keyring at the same path): a repository key records the root that wrapped it, and one whose root is not the current one is replaced by the next write and rewrapped by the next daily pass ([secrets.md](secrets.md), "Keys"), so changing it is a rolling restart, the old root kept reachable until that pass. An organization's owners may name its own root instead (identity's `Tenancy.SetSecretsRoot`): a Cloud KMS or AWS KMS key that ok secrets' workload is granted to use. |
| `secrets.database` | ok secrets' own database, as `database` names one: so a credential to the other roles' reaches none of its tables. Unset, the installation's. |
| `secrets.stores` | The external stores a repository's settings may name (`secrets: {store: NAME}`) to keep its secrets' sealed values in, by name — lowercase letters, digits, and dashes, never `default` (ok secrets' own database) — each `{kind, …}`, reached as ok secrets' workload, with no credential here ([secrets.md](secrets.md), "Stores"). `kind: vault`: `address`; `mount`, a KV version 2 mount; `role`, the Kubernetes auth role it logs in as with its service account's token (`token_file`, the pod's own unless set), at `auth/<auth_mount>` (`kubernetes` unless set); `namespace` (Vault Enterprise's); `ca_file`. `kind: aws-secrets-manager`: `region`; `kms_key_id`, what Secrets Manager encrypts under (its own unless set). `kind: gcp-secret-manager`: `project`; `locations`, where each secret is replicated (Google's choice unless set). AWS and Google hold at most 64 KiB in a secret: a larger value cannot be written to them. |

`ok secrets rotate REPOSITORY` makes a repository's next key version, `ok secrets rewrap REPOSITORY`
wraps its data keys again under the newest and retires the rest, and `ok secrets audit REPOSITORY`
prints every decision about its secrets — each over ok secrets' database, as the configuration
names it.

## Several roles in one process

`ok serve -config ok.yaml -roles identity,scm,automation,control` (or `-roles all`, which adds the
deployer, and ok secrets where `secrets.root` names a root) runs those roles in one process, on one port (`listen`): a small installation's whole ok,
as the Helm chart runs it ([install.md](install.md)). ok identity is always among them: it certifies
each other role's key in process, as that role's robot, so the process needs no token
(`workload`) and no CA certificates read over the network. The roles reach each other on the
process's own port, and any role not among them at its `services` address. With `tls`, the process
calls itself over TLS, checking its certificate as its `public_url`'s host.
