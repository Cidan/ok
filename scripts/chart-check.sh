#!/usr/bin/env bash
# Checks the Helm chart with its defaults and with ci/example-values.yaml: helm lint, every manifest
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
  go run github.com/yannh/kubeconform/cmd/kubeconform@v0.7.0 -strict -summary -kubernetes-version 1.33.0 "$out/chart.yaml"
  yq 'select(.kind == "ConfigMap") | .data["ok.yaml"]' "$out/chart.yaml" > "$out/ok.yaml"
  go run ./cmd/ok config check -config "$out/ok.yaml"
done
