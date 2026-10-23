#!/usr/bin/env bash
# Checks the Helm chart with its defaults and with ci/example-values.yaml — and the deploy agent's with
# its example values: helm lint, every manifest
# it renders valid against Kubernetes' schemas (kubeconform, strict), and the ok.yaml it renders
# read by ok config check as every role reads it. Needs helm, yq, and Go.
set -euo pipefail
cd "$(dirname "$0")/.."
chart=deploy/helm/ok
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
for values in "" "$chart/ci/example-values.yaml"; do
  echo "== the chart${values:+ with $values}"
  args=(-n ok)
  [ -n "$values" ] && args+=(-f "$values")
  helm lint "$chart" "${args[@]:2}"
  helm template ok "$chart" "${args[@]}" > "$out/chart.yaml"
  # The Gateway API's resources against their CRDs' schemas, from the community catalog.
  go run github.com/yannh/kubeconform/cmd/kubeconform@v0.7.0 -strict -summary -kubernetes-version 1.33.0 \
    -schema-location default \
    -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
    "$out/chart.yaml"
  yq 'select(.kind == "ConfigMap") | .data["ok.yaml"]' "$out/chart.yaml" > "$out/ok.yaml"
  go run ./cmd/ok config check -config "$out/ok.yaml"
done
# Staging's values (deploy/staging), on Spanner, its placeholders filled.
echo "== the chart with deploy/staging/values.yaml"
sed 's/PROJECT/example-project/g; s/TAG/staging/' deploy/staging/values.yaml > "$out/staging-values.yaml"
helm template ok "$chart" -n ok -f "$out/staging-values.yaml" > "$out/staging.yaml"
go run github.com/yannh/kubeconform/cmd/kubeconform@v0.7.0 -strict -summary -kubernetes-version 1.33.0 "$out/staging.yaml"
yq 'select(.kind == "ConfigMap") | .data["ok.yaml"]' "$out/staging.yaml" > "$out/staging-ok.yaml"
go run ./cmd/ok config check -config "$out/staging-ok.yaml"
# The deploy agent's chart, an organization's in its own cluster: it needs where ok is and how its
# robot signs in, so it is checked with its example values alone.
agent=deploy/helm/ok-agent
echo "== the agent's chart with $agent/ci/example-values.yaml"
helm lint "$agent" -f "$agent/ci/example-values.yaml"
helm template ok-agent "$agent" -n acme -f "$agent/ci/example-values.yaml" > "$out/agent.yaml"
go run github.com/yannh/kubeconform/cmd/kubeconform@v0.7.0 -strict -summary -kubernetes-version 1.33.0 "$out/agent.yaml"
