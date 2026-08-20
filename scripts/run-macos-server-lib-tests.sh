#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "error: run-macos-server-lib-tests.sh requires macOS" >&2
  exit 2
fi

log_file="$(mktemp "${TMPDIR:-/tmp}/bamboo-server-link.XXXXXX")"
trap 'rm -f -- "$log_file"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

set +e
cargo test --locked -p bamboo-server --all-features --lib server::tls::tests -- --nocapture \
  2>&1 | tee "$log_file"
pipeline_status=("${PIPESTATUS[@]}")
cargo_status=${pipeline_status[0]}
tee_status=${pipeline_status[1]}
set -e

if ((tee_status != 0)); then
  echo "error: failed to capture bamboo-server's macOS linker diagnostics" >&2
  exit 1
fi

grep_status=0
grep -Fq "__eh_frame section too large" "$log_file" || grep_status=$?
if ((grep_status == 0)); then
  echo "error: bamboo-server's macOS lib-test emitted the oversized __eh_frame warning" >&2
  exit 1
fi
if ((grep_status != 1)); then
  echo "error: failed to inspect bamboo-server's macOS linker diagnostics" >&2
  exit 1
fi

exit "$cargo_status"
