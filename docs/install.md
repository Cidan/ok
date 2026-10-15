# Installing ok

ok runs on Kubernetes from its Helm chart: ok identity, the SCM, ok automation, and ok control in
one Deployment (`ok serve`), the workers in another, and, optionally, the deployer. Everything
they read is one configuration file, `ok.yaml`, which the chart renders from its values
([configuration.md](configuration.md) is its reference).

To try ok first, on one machine, use Docker Compose or Podman instead:
[deploy/compose](../deploy/compose/README.md).

## What it needs

- **Kubernetes 1.30 or later.** The deployer's admission policy uses ValidatingAdmissionPolicy,
  which is GA in 1.30.
- **PostgreSQL**, one database, which every role keeps its tables in under a schema of its own:
  a Secret holding its URL, with its password (`database.urlSecret`).
- **Two S3 buckets**, any S3-compatible store: one for the SCM's file contents, one for automation's
  logs. Credentials are key files mounted from Secrets, or, without them, AWS's credential chain:
  the pod's role.
- **The identity CA**, which issues every credential: made once, kept secret.
- **Sign-in**: an OpenID Connect client (Google, Entra ID, Okta, Keycloak, Dex, …) or LDAP, and a
  system of record to sync people and groups from ([identity.md](identity.md)). A provider behind
  your own CA is trusted with its `ca_file`.
- **A gateway that speaks HTTP/2 to ok** (gRPC): ok serves its pages, its gRPC services, and its
  REST on one port, and jj calls it over gRPC. The chart makes a Gateway API HTTPRoute
  (`httpRoute`), which any Gateway API implementation serves — Envoy Gateway, Cilium, Istio,
  Traefik, NGINX Gateway Fabric; its Service's `appProtocol: kubernetes.io/h2c` has the gateway
  speak HTTP/2 to it. An Ingress works too, with your controller's annotation for HTTP/2 backends.
- **Workers' nodes** that let a container make user namespaces: see [Workers' nodes](#workers-nodes).

## Steps

```bash
kubectl create namespace ok

# The identity CA: once. Keep ca/ somewhere safe; a new CA invalidates every credential.
ok identity ca init -dir ca -name "Example"
kubectl -n ok create secret generic ok-identity-ca --from-file=tls.crt=ca/tls.crt --from-file=tls.key=ca/tls.key

# The database's URL (it reaches only the server and the deployer, never a worker), and the
# sign-in client's secret.
kubectl -n ok create secret generic ok-database --from-literal=url='postgres://ok:…@db.example.com:5432/ok'
kubectl -n ok create secret generic ok-oidc --from-literal=client-secret='…'
```

A values file for a Google Workspace installation, its buckets on AWS with the pod's role, reached
through a Gateway named `public` in the `gateway` namespace:

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
httpRoute:
  enabled: true
  parentRefs: [{name: public, namespace: gateway}]   # its HTTPS listener holds ok.example.com's certificate
worker:
  replicas: 2
  bazelFlags: ["--remote_cache=grpcs://cache.example.com"]
  dockerConfigSecret: registry-credentials   # config.json for the registries deploys push to
```

```bash
helm install ok oci://ghcr.io/cidan/charts/ok --version 1.0.0 -n ok -f values.yaml
kubectl -n ok rollout status deployment/ok-server
ok config check -config ok.yaml   # the same check the roles make, on a file of your own
```

The chart's `ci/example-values.yaml` uses every part of it, the deployer included.

## Workers' nodes

Workers run each run job's program in a sandbox (bubblewrap), and a virtual checkout mounts FUSE;
both make **user namespaces**. The chart runs workers and the deployer with the seccomp profile
`Unconfined`, which allows them, and a worker checks as it starts that it can make its sandbox,
failing with what to change on the node if it cannot.

- **Ubuntu 23.10 and later** restrict unprivileged user namespaces through AppArmor
  (`kernel.apparmor_restrict_unprivileged_userns = 1`). Either load the `ok-sandbox` profile
  ([deploy/apparmor/ok-sandbox](../deploy/apparmor/ok-sandbox)) on each node and give workers and
  the deployer it —

  ```yaml
  worker:
    securityContext:
      seccompProfile: {type: Unconfined}
      appArmorProfile: {type: Localhost, localhostProfile: ok-sandbox}
      allowPrivilegeEscalation: false
  ```

  — or set the sysctl to 0 on the nodes. Inside kind, whose nodes cannot use AppArmor profiles, set
  it on the machine that runs kind.
- **SELinux** (Fedora, RHEL, openSUSE) runs containers as `container_t`, which refuses the sandbox
  its own `/dev/pts`. Run workers and the deployer as `spc_t` —
  `securityContext.seLinuxOptions: {type: spc_t}`; under podman or Docker, `--security-opt
  label=disable` — as compose.yaml's worker does.
- **A virtual checkout** (`worker.checkout: virtual`) also opens `/dev/fuse`, which a container
  runtime allows a pod only as a device plugin offers it. Run one that offers `/dev/fuse` —
  [generic-device-plugin](https://github.com/squat/generic-device-plugin), with a `fuse` device —
  and name the resource it offers: `worker.fuseResource: devic.es/fuse`. Without a device plugin,
  check out real (the default), which needs no FUSE.
- **The runner image** (`ok-runner`) carries bazelisk, which runs the Bazel your repository's
  `.bazelversion` names; bubblewrap; FUSE's mount helper; Python; and a C and C++ compiler, which
  Bazel's default C++ toolchain looks for. A repository whose builds need more brings it as a
  hermetic toolchain, or you build your own image from this one.

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
