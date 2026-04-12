#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
DEFAULT_DATA_DIR="${TMPDIR:-/tmp}/bamboo-memory-clean-room"
export BAMBOO_DATA_DIR="${BAMBOO_DATA_DIR:-$DEFAULT_DATA_DIR}"

printf '\n==> Bamboo memory clean-room acceptance\n'
printf 'repo: %s\n' "$REPO_ROOT"
printf 'BAMBOO_DATA_DIR: %s\n' "$BAMBOO_DATA_DIR"

rm -rf "$BAMBOO_DATA_DIR"
mkdir -p "$BAMBOO_DATA_DIR"

cd "$REPO_ROOT"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

cat <<'EOF'
This script validates the current memory acceptance surface in a clean Bamboo data dir.

It focuses on the highest-value regression paths covered in the current memory-refactor work:
- Dream refine / rebuild behavior
- same-project relevant durable recall
- cross-project durable memory isolation
- old-memory staleness guidance in prompt injection
- system-prompt snapshot extraction / handler / HTTP exposure
- prompt-memory metrics summary aggregation

It does NOT require a live provider.
It relies on deterministic targeted tests added during the memory refactor.
EOF

run cargo test auto_dream --lib
run cargo test run_auto_dream_once_refine_mode_includes_recent_durable_memory_in_prompt --lib
run cargo test run_auto_dream_once_forces_periodic_full_rebuild_using_memory_index --lib
run cargo test inject_external_memory_includes_project_memory_index_and_omits_global_dream_fallback --lib
run cargo test inject_external_memory_renders_relevant_memory_section_for_project_hits --lib
run cargo test inject_external_memory_adds_stale_guidance_for_old_relevant_memory_hits --lib
run cargo test inject_external_memory_prefers_project_dream_over_global_fallback --lib
run cargo test inject_external_memory_uses_global_dream_fallback_when_project_dream_and_index_are_missing --lib
run cargo test inject_external_memory_excludes_other_project_memory_index_content --lib
run cargo test build_memory_summary_includes_prompt_memory_observability_aggregates --lib
run cargo test snapshot_extracts_project_dream_heading_from_external_memory --lib
run cargo test handler_returns_project_dream_snapshot_from_persisted_session --lib
run cargo test system_prompt_snapshot_route_returns_project_dream_over_http --lib

cat <<EOF

Clean-room acceptance completed successfully.

Validated surfaces:
- Auto Dream refine/rebuild behavior
- Same-project relevant durable recall
- Old-memory staleness guidance in prompt injection
- Project vs global Dream prompt behavior
- Cross-project durable memory isolation in prompt injection
- Prompt-memory metrics summary aggregation
- System prompt snapshot extraction of Dream/session memory
- Handler-level system prompt JSON response
- Real HTTP route: GET /api/v1/sessions/{session_id}/system-prompt

If you also want live/provider-backed validation, run the manual Layer 5 steps in:
  docs/development/memory-system-e2e-checklist.md
with a configured provider and background model.
EOF
