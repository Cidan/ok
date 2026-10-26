# ok's staging cell

Phase 11's staging (docs/hosted.md, "Proof at scale, as built"): one installation on GKE in a
region's three zones, on Spanner, GCS, and NATS, signed in to through Dex, driven by `ok load` or by
people's jj and a real worker while `scripts/chaos.sh` stops its parts, then priced by `ok load
cost` and destroyed. **It costs money while it stands**: stand it up for a session, and destroy it
after.

## What it costs

At list prices in us-central1 (deploy/load/gcp-us-central1.yaml; the Cloud Billing Catalog API,
2026-10-19), as it ran on 2026-10-21 (`-var nodes_per_zone=2 -var spanner_processing_units=3000`):

| Part | An hour |
|---|---|
| GKE: a regional cluster's fee | $0.10 |
| Six n2-standard-8 nodes, two a zone (48 cores, 192 GiB) | $2.33 |
| Their disks: 6 × 100 GiB of SSD persistent disk | $0.14 |
| Spanner: three nodes (3,000 processing units), Enterprise edition — ok identity's directory search needs it | $3.69 |
| Cloud Storage: its writes, at a quarter of a cell's share | ~$1 |
| **In all** | **~$7** |

## Running it

```sh
# 1. Stand it up: Terraform's state in a bucket of the project's. A zone short of the machine type
#    leaves the node pool RUNNING_WITH_ERROR (GKE keeps trying it); keep what came up with
#    `terraform untaint google_container_node_pool.ok`.
cd deploy/staging
terraform init -backend-config="bucket=<state bucket>" -backend-config="prefix=ok-staging"
terraform apply -var project=<project> -var nodes_per_zone=2 -var spanner_processing_units=3000
gcloud container clusters get-credentials ok-staging --region us-central1 --project <project>

# 2. ok's images, from the repository's root, into the registry Terraform made.
reg=$(terraform output -raw registry); tag=staging-$(jj log -r @ --no-graph -T 'commit_id.short(8)')
podman build -f deploy/images/ok.Dockerfile -t $reg/ok:$tag . && podman push $reg/ok:$tag
podman build -f deploy/images/ok-runner.Dockerfile -t $reg/ok-runner:$tag . && podman push $reg/ok-runner:$tag

# 3. NATS; Dex, with the load's person (a password, its bcrypt hash) and ok's client (a secret).
helm install nats nats/nats -n nats --create-namespace --set config.jetstream.enabled=true --set config.cluster.enabled=true
sed "s|HASH|<bcrypt hash>|; s|CLIENT_SECRET|<client secret>|" dex-values.yaml > /tmp/dex-values.yaml
helm install dex dex/dex -n dex --create-namespace -f /tmp/dex-values.yaml

# 4. ok: the identity CA, the person, Dex's client secret, and the chart.
kubectl create namespace ok
ok identity ca init -dir ca && kubectl -n ok create secret tls ok-identity-ca --cert ca/tls.crt --key ca/tls.key
kubectl -n ok create secret generic ok-people --from-file=people.yaml   # people: [{id: "1", handle: load, email: load@example.com, name: Load}]
kubectl -n ok create secret generic ok-oidc --from-literal=client-secret=<client secret>
sed "s/PROJECT/<project>/g; s/TAG/$tag/" values.yaml > /tmp/values.yaml
helm install ok ../helm/ok -n ok -f /tmp/values.yaml

# 5. The person signs in through the device flow, approved through Dex by device-login.py, which
#    reaches the cluster's names through these port-forwards.
kubectl -n ok port-forward svc/ok-server 18472:8472 & kubectl -n dex port-forward svc/dex 15556:5556 &
OK_CONFIG_DIR=$PWD/person ok auth login -server http://127.0.0.1:18472 -no-browser   # prints the device URL
./device-login.py '<the device URL>' <password file>
kubectl -n ok create secret generic ok-load-person --from-file=person/credential/

# 6. Load as one person: REPOSITORIES at SPEED for DURATION, by the load generator and its
#    synthetic workers.
sed "s/PROJECT/<project>/; s/TAG/$tag/; s/REPOSITORIES/500/; s/\"SPEED\"/\"5\"/; s/DURATION/10m/" load-job.yaml | kubectl apply -f -
kubectl -n ok logs -f job/ok-load > load.log   # the summary, then the report's JSON

# 6b. Or as PEOPLE people at once — each signed in through the device flow and on a connection of
#     their own, reviewing each other's CLs — from PODS pods: the system of record's people and
#     Dex's users first (one password, its bcrypt hash), then the indexed Job, then its pods'
#     reports added up. REPOSITORIES 1 is everyone's monorepo. Each synthetic worker keeps a real
#     checkout in its pod's scratch volume. A fresh installation imports its people over a minute
#     or so: start the Job once it has them all (ok load signs in again a person it lacks, a few
#     times, ten seconds apart).
ok load people -n 2000 -hash-file <bcrypt hash file> -dir people
kubectl -n ok create secret generic ok-people --from-file=people/people.yaml --dry-run=client -o yaml | kubectl apply -f -
helm upgrade dex dex/dex -n dex -f /tmp/dex-values.yaml -f people/dex-people.yaml
kubectl -n ok create secret generic ok-load-password --from-file=password=<password file>
sed "s/PROJECT/<project>/; s/TAG/$tag/; s/\"PEOPLE\"/\"2000\"/; s/PODS/4/g; s/\"REPOSITORIES\"/\"100\"/; s/PREFIX/load/; s/\"SPEED\"/\"1\"/; s/DURATION/10m/" \
  people-job.yaml | kubectl apply -f -
for p in $(kubectl -n ok get pods -l job-name=ok-people -o name); do kubectl -n ok logs $p > $(basename $p).log; done
ok load merge -report merged.json ok-people-*.log

# 7. Or real CI: a real worker (worker-values.yaml) and people's jj, through the port-forward.
sed "s/PROJECT/<project>/g; s/TAG/$tag/" worker-values.yaml > /tmp/worker-values.yaml
helm upgrade ok ../helm/ok -n ok -f /tmp/values.yaml -f /tmp/worker-values.yaml
jj --config ok.endpoint=http://127.0.0.1:18472 create smoke   # then a CL: jj cl --ready, jj cl review, jj cl submit -f

# 7b. Secrets (docs/secrets.md), beside 7's real worker: ok secrets, its root a Cloud KMS key and its
#     values in Secret Manager, each reached as ok secrets' own Google service account; the deployer,
#     which applies the groups-and-ACLs tree that //secret's acl.yaml files name (tree-values.yaml).
#     The key is made once, apart: a key ring outlives everything. Terraform's secrets_root gives ok
#     secrets the key, Secret Manager, and its database, and the deployer its workload identity.
gcloud services enable cloudkms.googleapis.com secretmanager.googleapis.com --project <project>
gcloud kms keyrings create ok-test --location us-central1 --project <project>
gcloud kms keys create staging --keyring ok-test --location us-central1 --purpose encryption --project <project>
root=projects/<project>/locations/us-central1/keyRings/ok-test/cryptoKeys/staging
terraform apply -var project=<project> -var secrets_root=$root   # with 1's other variables
for f in secrets-values tree-values; do sed "s/PROJECT/<project>/g; s/TAG/$tag/; s|SECRETS_ROOT|$root|" $f.yaml > /tmp/$f.yaml; done
helm upgrade ok ../helm/ok -n ok -f /tmp/values.yaml -f /tmp/worker-values.yaml -f /tmp/secrets-values.yaml -f /tmp/tree-values.yaml
#     People reach ok secrets at the installation's address, which the chart's Ingress routes; a
#     port-forward reaches one Service, so a router stands in, which jj's clones are made through.
kubectl -n ok port-forward svc/ok-secrets 18473:8472 &
go run ./router -listen 127.0.0.1:18480 &
#     The tree, in a repository called tree (identity: identity in its settings): an ACL, ops, of
#     the person. Then a repository keeping secrets in Secret Manager — `secrets: {store: gcp}`,
#     //secret/acl.yaml giving ops the folder and a post job its secrets (runs:) — a secret written,
#     its pointer and the job naming it landed, and a change the job runs for: its log names the
#     secret, never its value. The person reads it back, an unseal each time.
jj --config ok.endpoint=http://127.0.0.1:18480 create tree --real    # land the tree: jj cl --ready, review, submit
jj --config ok.endpoint=http://127.0.0.1:18480 create smoke --real   # land the settings and acl.yaml
printf %s '<a value>' | jj secret write //secret/db-password          # its pointer, then .ok/workflow/ci.yaml's post job
jj secret cat //secret/db-password                                    # unseals, and reads it once
kubectl -n ok exec deploy/ok-secrets -- ok secrets audit -config /etc/ok/config/ok.yaml smoke

# 8. Chaos, beside either: pods of ok's server deleted, a zone drained, the bus restarted.
../../scripts/chaos.sh -n ok -d 900 -i 45 -b nats/nats | tee chaos.log

# 9. What it used: `kubectl top`, ok-server's cores (Cloud Monitoring's
#    kubernetes.io/container/cpu/core_usage_time), and Spanner's own statistics —
#    SPANNER_SYS.TXN_STATS_TOP_10MINUTE, LOCK_STATS_TOP_10MINUTE, QUERY_STATS_TOP_10MINUTE, and
#    spanner.googleapis.com/instance/cpu/utilization — into measured.json, then priced. Where it
#    spends: each server's transactions past half a second are logged by where they began
#    (`spanner: a slow transaction`), and their times, attempts, and statements are metrics
#    (ok_spanner_txn_*); values.yaml turns on profiling (`/debug/pprof/` on the metrics port).
ok load cost -report report.json -measured measured.json -prices ../load/gcp-us-central1.yaml

# 10. Destroy it (the registry's images with it): the namespaces first, so NATS's volumes' disks go;
#     Secret Manager's secrets, which nothing else deletes; and the root key's versions, as its ring
#     cannot be.
kubectl delete namespace ok dex nats
for s in $(gcloud secrets list --project <project> --format='value(name.basename())' | grep '^ok-'); do gcloud secrets delete $s --project <project> --quiet; done
terraform destroy -var project=<project> -var nodes_per_zone=2 -var spanner_processing_units=3000 -var secrets_root=$root
gcloud kms keys versions destroy 1 --key staging --keyring ok-test --location us-central1 --project <project>
```

As one person, the load generator dials ok-server's Service with one connection, which the Service
balances to one replica; as people, each its own, spread across them.
