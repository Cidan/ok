#!/bin/sh
# Regenerates every .proto's Go code beside it: protoc (on PATH, or $PROTOC) with the Go plugins
# go.mod pins as tools, so generated code always matches the runtime it compiles against.
#   scripts/protoc.sh
set -eu
cd "$(dirname "$0")/.."
bin=$(mktemp -d)
trap 'rm -rf "$bin"' EXIT
go build -o "$bin/" google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc
protoc=${PROTOC:-protoc}
find . -name '*.proto' -not -path './jj/*' | sort | while read -r f; do
	"$protoc" -I . \
		--plugin=protoc-gen-go="$bin/protoc-gen-go" --go_out=. --go_opt=paths=source_relative \
		--plugin=protoc-gen-go-grpc="$bin/protoc-gen-go-grpc" --go-grpc_out=. --go-grpc_opt=paths=source_relative \
		"$f"
done
