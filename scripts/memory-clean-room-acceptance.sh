#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
TMP_ROOT="$(cd -- "${TMPDIR:-/tmp}" && pwd -P)"
SCRATCH="$(mktemp -d "$TMP_ROOT/bamboo-memory-clean-room.XXXXXX")"
trap 'rm -rf -- "$SCRATCH"' EXIT
export TMPDIR="$SCRATCH"
export BAMBOO_DATA_DIR="$TMPDIR/bamboo"
export BAMBOO_JIANDU_DATA_DIR="$TMPDIR/jiandu"
export BAMBOO_FRONTEND_BUILD_MODE="${BAMBOO_FRONTEND_BUILD_MODE:-api-only}"
export RUST_MIN_STACK="${RUST_MIN_STACK:-8388608}"

printf '\n==> Bamboo memory clean-room acceptance\n'
printf 'repo: %s\n' "$REPO_ROOT"
printf 'BAMBOO_DATA_DIR: %s\n' "$BAMBOO_DATA_DIR"
printf 'BAMBOO_JIANDU_DATA_DIR: %s\n' "$BAMBOO_JIANDU_DATA_DIR"
mkdir -p -- "$BAMBOO_DATA_DIR" "$BAMBOO_JIANDU_DATA_DIR"

cd "$REPO_ROOT"

run_suite() {
  local package="$1"
  shift
  local log="$TMPDIR/$package.log"
  local selector
  printf '\n==> %s\n' "$package"
  cargo test --locked -p "$package" --lib -- "$@" \
    --test-threads=1 --format=pretty --color=never | tee "$log"

  for selector in "$@"; do
    if ! grep -Eq "^test ([[:alnum:]_]+::)*${selector} \.\.\. ok$" "$log"; then
      printf 'Expected a passing test for %s in %s\n' "$selector" "$package" >&2
      return 1
    fi
  done
}

cat <<'EOF'
This script validates on-demand memory, Project access, ledger context, Dream
rebuilding and legacy prompt diagnostics in fresh Bamboo and Jiandu directories.
The selected tests use deterministic fixtures.
EOF

run_suite bamboo-engine \
  all_session_roles_keep_jiandu_memory_out_of_round_context \
  round_context_clears_when_ledger_is_disabled_or_empty \
  provider_gets_jiandu_text_only_after_explicit_memory_call \
  run_auto_dream_once_grounds_notebook_in_durable_index_not_prior_prose \
  run_auto_dream_once_forces_periodic_full_rebuild_using_memory_index \
  snapshot_extracts_project_dream_heading_from_external_memory

run_suite bamboo-server-tools \
  assigned_project_memory_uses_only_the_canonical_project_store \
  assigned_project_cannot_get_memory_from_an_unrelated_project \
  unassigned_workspace_cannot_select_or_discover_project_memory

run_suite bamboo-server \
  supervisor_created_project_root_can_query_its_own_project_memory \
  build_memory_summary_includes_prompt_memory_observability_aggregates \
  handler_returns_project_dream_snapshot_from_persisted_session \
  system_prompt_snapshot_route_returns_project_dream_over_http

printf '\nClean-room acceptance completed successfully.\n'
