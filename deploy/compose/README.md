# ok on one machine

To try ok: everything it needs, on your machine, with Docker Compose or Podman.

```bash
cd deploy/compose
docker compose up -d          # or: podman compose up -d
```

Then open <http://localhost:8080> and sign in with **Dex** as `admin@example.com`, password
`ok`. You are an administrator.

## What runs

| Service | What it is |
|---|---|
| `ok` | ok identity, the SCM, ok automation, and ok control in one process (`ok serve`), on <http://localhost:8080> |
| `dex` | Sign-in: [Dex](https://dexidp.io) with one user, in `ok`'s network, at <http://localhost:5556/dex> |
| `worker` | A worker: Bazel on every CL's commit |
| `postgres` | PostgreSQL, for everything but file contents |
| `s3` | [versitygw](https://github.com/versity/versitygw), S3 over a directory, for file contents and logs |
| `setup` | Once each start: the buckets, the identity CA, and the worker's credential (`ok identity certify`) |

Everything listens on 127.0.0.1 only, and the passwords in `secrets/` and `dex.yaml` are fixed
values for this machine: this is for trying ok, not for others to use. To install ok for a team, use
the Helm chart ([docs/install.md](../../docs/install.md)). The deployer, which rolls services out on
Kubernetes, is not here.

## Your first CL

Get `jj` — ok's jj, from the [release](https://github.com/Cidan/ok/releases) for your platform —
and sign it in. It shows a code, which you approve in the browser:

```bash
jj auth login http://localhost:8080
```

Make a repository, put the example in it — a shell binary and its test, as a Bazel workspace — and
upload it:

```bash
jj create hello ~/src/hello && cd ~/src/hello
cp -r path/to/ok/deploy/compose/example/. .
jj describe -m 'hello: a greeting and its test'
jj cl --ready
```

The CL's page, <http://localhost:8080/hello/cl/1>, shows pre: the worker plans what the change
affects and runs it — here `//hello:hello_test`. The first run fetches Bazel and its rules, so give
it a minute. When it passes, approve it — with no OWNERS file, your own approval counts — and
submit it:

```bash
jj cl review 1 --lgtm --approve
jj cl submit 1 --until landed
jj sync
```

`jj create` and `jj clone` make a virtual workspace, its files read from ok as they are used: on
Linux over FUSE (`/dev/fuse`, and `fusermount3`), on macOS over NFS. `--real` writes every file
instead. [docs/jj.md](../../docs/jj.md) has the rest.

## People

`people.yaml` is who ok knows, and `dex.yaml` how each signs in. To add someone, give them an entry
in each with the same email, a bcrypt hash of their password in `dex.yaml` —
`htpasswd -bnBC 10 "" 'their password' | tr -d ':\n'` — and restart: `docker compose restart ok dex`.

## The worker

The worker runs each run job's program in a sandbox (bubblewrap), which makes user namespaces and
mounts: compose.yaml runs it with seccomp, AppArmor, and SELinux unconfined. On Ubuntu 23.10 and
later, which restrict user namespaces, also run

```bash
sudo sysctl kernel.apparmor_restrict_unprivileged_userns=0
```

or the worker stops at start and says so (`docker compose logs worker`). On macOS and Windows,
Docker Desktop's Linux needs nothing more.

## Stopping, and starting over

```bash
docker compose down        # stop; everything stays in its volumes
docker compose down -v     # stop, and delete every repository, CL, and credential
```

To run images you built yourself, name them: `OK_IMAGE=ok:dev OK_RUNNER_IMAGE=ok-runner:dev docker
compose up -d`.
