#!/bin/sh
set -eu
out=$(hello/hello ok)
[ "$out" = "hello, ok" ] || { echo "hello said: $out"; exit 1; }
