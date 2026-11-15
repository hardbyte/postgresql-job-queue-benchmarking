#!/usr/bin/env bash
# Point a checkout's awa-bench at awa main (0.7) for the operator scenarios.
#
#   bash scripts/awa_operator_pin.sh [awa-rev] [checkout]
#
# Rewrites awa-bench/Cargo.toml in <checkout> (default: this repository) to
# hardbyte/awa at <awa-rev> (default: main; a branch or tag is resolved to
# its commit), moves sqlx to 0.9 as awa 0.7 requires, and makes the `awa07`
# feature default: find_jobs/count_jobs probes, and the tag-index build after
# the haystack. Cargo.lock is refreshed for the new pin only.
#
# Use a separate checkout so the 0.6 pin stays as committed, then run the
# same cells there:
#
#   git worktree add --detach ../bench-awa07 HEAD
#   bash scripts/awa_operator_pin.sh main ../bench-awa07
#   cd ../bench-awa07 && uv run --frozen bench run --systems awa ...
set -euo pipefail
rev="${1:-main}"
root="$(cd "${2:-$(dirname "$0")/..}" && pwd)"
manifest="$root/awa-bench/Cargo.toml"
repo="https://github.com/hardbyte/awa"

if [[ ! "$rev" =~ ^[0-9a-f]{40}$ ]]; then
  resolved=$(git ls-remote "$repo" "$rev" "refs/tags/$rev" | awk 'NR==1 {print $1}')
  [[ -n "$resolved" ]] || { echo "cannot resolve awa rev '$rev'" >&2; exit 2; }
  echo "awa $rev -> $resolved"
  rev="$resolved"
fi

sed -i -E \
  -e "s#^(awa-(model|worker|macros) = \{ git = \"$repo\", rev = )\"[0-9a-f]+\"#\1\"$rev\"#" \
  -e 's|^sqlx = \{ version = "0\.8"|sqlx = { version = "0.9"|' \
  "$manifest"
grep -q '^default = \["awa07"\]' "$manifest" \
  || sed -i 's|^awa07 = \[\]$|default = ["awa07"]\nawa07 = []|' "$manifest"

grep -c "rev = \"$rev\"" "$manifest" | grep -qx 3 \
  || { echo "awa-model/worker/macros pins not rewritten in $manifest" >&2; exit 1; }
cargo fetch --manifest-path "$manifest"
grep -E '^(default|awa-|sqlx)' "$manifest"
