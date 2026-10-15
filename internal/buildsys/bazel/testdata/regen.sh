#!/bin/sh
# Regenerates the sample workspace's variants and the real `bazel query` output of each, which
# the driver's tests read. Run from this directory, with bazel (or bazelisk) on PATH:
#   ./regen.sh
set -eu
cd "$(dirname "$0")/sample"
variant() { # variant <name> <edit command…>: base, copied and edited
	rm -rf "$1"; cp -r base "$1"; (cd "$1" && shift && sh -c "$*")
}
variant src-edit     'echo changed > lib/a.txt'
variant bzl-edit     'sed -i "s/cat %s > %s/cat %s >> %s/" defs.bzl'
variant attr-edit    'sed -i "s/cp \$< \$@/cp \$< \$@ \&\& true/" lib/BUILD.bazel'
variant new-target   'printf "\ncat(\n    name = \"extra\",\n    srcs = [\":joined\"],\n)\n" >> app/BUILD.bazel'
variant deleted-src  'rm app/c.txt'
variant module-edit  'printf "# a comment\n" >> MODULE.bazel'
variant readme-edit  'echo more >> docs/README.md'
# One output base for every variant, outside them; the paths of it that macro-made rules in
# external repositories carry (generator_location, which their hashes do not include) are
# written as $OUTPUT_BASE.
ob=$(mktemp -d)
trap 'bazel --output_base="$ob" shutdown >/dev/null 2>&1; rm -rf "$ob"' EXIT
for d in */; do
	(cd "$d" && bazel --output_base="$ob" query 'deps(//...)' --output=streamed_jsonproto \
		--proto:include_synthetic_attribute_hash --noproto:locations --noproto:default_values \
		--lockfile_mode=off) | sed "s#$ob#\$OUTPUT_BASE#g" > "${d%/}".jsonl
done
