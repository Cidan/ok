#!/bin/sh
# compose.yaml's setup, run as root on every start: the S3 buckets (versitygw's folders), the
# identity CA once, a credential for the worker (kept while it is good), and the volumes ok's user
# (65532) owns.
set -eu
mkdir -p /var/lib/ok-s3/ok-scm /var/lib/ok-s3/ok-logs
if [ ! -f /var/lib/ok/ca/tls.key ]; then
  ok identity ca init -dir /var/lib/ok/ca -name "ok on this machine"
fi
ok identity certify -config /etc/ok/ok.yaml -dir /var/lib/ok/worker-credential worker.ok
chown -R 65532:65532 /var/lib/ok/ca /var/lib/ok/worker-credential /var/lib/ok/state
