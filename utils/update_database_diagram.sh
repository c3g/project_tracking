#!/usr/bin/env bash
# Regenerate the database diagram from the models: docs/database.dbml (DBML for dbdiagram.io) and
# docs/data/database.svg (Graphviz, laid out as a pipeline). Run it from the repository root after any model change;
# tests/test_dbml.py fails until the DBML is regenerated. The SVG needs Node.js (npx).
set -euo pipefail
cd "$(dirname "$0")/.."
flask --app project_tracking dbml -o docs/database.dbml
dot_file=$(mktemp)
trap 'rm -f "$dot_file"' EXIT
flask --app project_tracking dbml --format dot -o "$dot_file"
npx --yes -p @hpcc-js/wasm-graphviz-cli@1.11.3 wasm-graphviz-cli -K dot -T svg "$dot_file" > docs/data/database.svg
echo "Diagram written to docs/data/database.svg"
