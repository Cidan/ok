# Installing ok

ok runs on Kubernetes from its Helm chart: ok identity, the SCM, ok automation, and ok control in
one Deployment (`ok serve`), the workers in another, and, optionally, the deployer. Everything
they read is one configuration file, `ok.yaml`, which the chart renders from its values
([configuration.md](configuration.md) is its reference).

## What it needs

- **Kubernetes 1.30 or later.** The deployer's admission policy uses ValidatingAdmissionPolicy,
  which is GA in 1.30.
- **PostgreSQL**, one database, which every role keeps its tables in under a schema of its own:
  a Secret holding its URL, with its password (`database.urlSecret`).
- **Two S3 buckets**, any S3-compatible store: one for the SCM's file contents, one for automation's
  logs. Credentials are key files mounted from Secrets, or, without them, AWS's credential chain:
  the pod's role.
- **The identity CA**, which issues every credential: made once, kept secret.
- **Sign-in**: an OpenID Connect client (Google, Entra ID, Okta, Keycloak, …) or LDAP, and a
  system of record to sync people and groups from ([identity.md](identity.md)).
- **An ingress that speaks HTTP/2 to ok** (gRPC): ok serves its pages, its gRPC services, and its
  REST on one port, and jj calls it over gRPC.
- **Workers' nodes** that let a container make user namespaces (bubblewrap's sandbox; and a virtual
  checkout's mount, which also needs `/dev/fuse` on the node). The chart runs workers with an
  unconfined seccomp profile for it; `worker.checkout: real`, the default, needs no FUSE.

## Steps

```bash
kubectl create namespace ok

# The identity CA: once. Keep ca/ somewhere safe; a new CA invalidates every credential.
ok identity ca init -dir ca -name "Example"
kubectl -n ok create secret generic ok-identity-ca --from-file=tls.crt=ca/tls.crt --from-file=tls.key=ca/tls.key

# The database's URL, and the sign-in client's secret.
kubectl -n ok create secret generic ok-database --from-literal=url='postgres://ok:…@db.example.com:5432/ok'
kubectl -n ok create secret generic ok-oidc --from-literal=client-secret='…'
```

A values file for a Google Workspace installation, its buckets on AWS with the pod's role:

```yaml
domain: example.com
installation: Example ok
publicURL: https://ok.example.com
identity:
  bootstrapAdmins: [ada]          # administer until the groups-and-ACLs tree declares admins
  treeRepository: infra           # the repository whose groups-and-ACLs tree is ok's
config:
  identity:
    sign_in:
      oidc:
        - {id: google, name: Google, issuer: https://accounts.google.com, client_id: 123.apps.googleusercontent.com,
           client_secret_file: /etc/ok/secrets/oidc/client-secret}
    system_of_record:
      google: {key_file: /etc/ok/secrets/google/key.json, subject: admin@example.com}
  scm:
    objects: {endpoint: https://s3.us-east-1.amazonaws.com, bucket: example-ok-scm, region: us-east-1}
  automation:
    logs: {endpoint: https://s3.us-east-1.amazonaws.com, bucket: example-ok-logs, region: us-east-1}
server:
  extraSecretMounts:
    - {name: oidc, secretName: ok-oidc, mountPath: /etc/ok/secrets/oidc}
    - {name: google, secretName: ok-google, mountPath: /etc/ok/secrets/google}
ingress:
  enabled: true
  className: nginx
  host: ok.example.com
  annotations: {nginx.ingress.kubernetes.io/backend-protocol: GRPC}
  tls: [{secretName: ok-tls, hosts: [ok.example.com]}]
worker:
  replicas: 2
  bazelFlags: ["--remote_cache=grpcs://cache.example.com"]
  dockerConfigSecret: registry-credentials   # config.json for the registries deploys push to
```

```bash
helm install ok oci://ghcr.io/cidan/charts/ok --version 0.1.0 -n ok -f values.yaml
kubectl -n ok rollout status deployment/ok-server
ok config check -config ok.yaml   # the same check the roles make, on a file of your own
```

The chart's `ci/example-values.yaml` uses every part of it, the deployer included.

## Signing in, and the first repository

```bash
jj auth login https://ok.example.com     # approve the code in a browser
jj create infra ~/src/infra              # the repository identity.treeRepository names
```

The first administrators are `identity.bootstrapAdmins`, until the tree in `treeRepository`
declares `admins.acl.<domain>` and the deployer applies it ([identity.md](identity.md), "The
groups-and-ACLs tree"). A repository is checked by Bazel once its `.ok/repository.yaml` says
`build: bazel` ([automation.md](automation.md)).

## The deployer

`deploy.enabled: true` runs it. It rolls only the workloads `deploy.grants` names, each granted in
its own namespace, and runs the migrations deploy stanzas name with the environment
`deploy.migrationEnv` gives them ([deploy.md](deploy.md)):

```yaml
deploy:
  enabled: true
  grants:
    - {namespace: shop, deployment: api, accounts: [api]}
    - {namespace: shop, statefulset: db}
  migrationEnv:
    APP_DATABASE_URL: {secretName: app-database, key: url}
  admissionPolicy: {enabled: true}
```

A deployed Deployment's replicas are the deployer's once it rolls it: the manifests that declare it
should not set them, or an upgrade brings the idle copy back.

## What runs

| Deployment | Image | What |
|---|---|---|
| `<release>-server` | `ok` | `ok serve`: identity, the SCM, automation, and control, on one port. Identity certifies the others' keys in process. |
| `<release>-worker` | `ok-runner` | `ok worker run`: Bazel on each run's commit, as the robot `worker.ok`, certified with its ServiceAccount's token. |
| `<release>-deploy` | `ok-runner` | `ok deploy serve`, as the robot `deploy.ok`. |

`ok serve` runs the roles `server.roles` names in one process. A larger installation runs each role
as its own Deployment (`ok scm serve`, `ok control serve`, …), with the configuration's
`services` naming where each reaches the others.
