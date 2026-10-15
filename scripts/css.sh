#!/bin/sh
# Rebuilds every page stylesheet from its input.css: the Tailwind CSS v4 command line (on PATH as
# tailwindcss, or $TAILWINDCSS) scans the templates each input.css names.
#   scripts/css.sh
set -eu
cd "$(dirname "$0")/.."
tw=${TAILWINDCSS:-tailwindcss}
find . -name input.css -not -path './jj/*' | sort | while read -r f; do
	dir=$(dirname "$f")
	"$tw" --input "$f" --output "$dir/app.css" --minify
done
