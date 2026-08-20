#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly script_dir
repo_root="$(cd -- "${script_dir}/.." && pwd)"
readonly repo_root
readonly fixture_dir="${repo_root}/crates/app/bamboo-broker/tests/fixtures/russh"

for command_name in cargo docker ssh-keygen; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "required command is unavailable: ${command_name}" >&2
    exit 1
  fi
done

if ! docker info >/dev/null 2>&1; then
  echo "Docker daemon is unavailable; start Docker before running the live transport test" >&2
  exit 1
fi

fixture_tmp_dir=""
fixture_container=""
fixture_image=""

cleanup() {
  local exit_status=$?
  local cleanup_failed=0
  trap - EXIT INT TERM HUP
  set +e

  if [[ -n "${fixture_container}" ]]; then
    if ! docker container rm --force "${fixture_container}" >/dev/null 2>&1; then
      echo "failed to remove the SSH fixture container" >&2
      cleanup_failed=1
    fi
  fi
  if [[ -n "${fixture_image}" ]]; then
    if ! docker image rm --force "${fixture_image}" >/dev/null 2>&1; then
      echo "failed to remove the SSH fixture image tag" >&2
      cleanup_failed=1
    fi
  fi
  if [[ -n "${fixture_tmp_dir}" && -d "${fixture_tmp_dir}" ]]; then
    if ! rm -rf -- "${fixture_tmp_dir}"; then
      echo "failed to remove the SSH fixture key directory" >&2
      cleanup_failed=1
    fi
  fi

  if (( exit_status == 0 && cleanup_failed != 0 )); then
    exit_status=1
  fi

  exit "${exit_status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

umask 077
fixture_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/bamboo-russh-live.XXXXXX")"
readonly private_key="${fixture_tmp_dir}/client_ed25519"
readonly public_key="${private_key}.pub"
readonly run_identity="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-$$"
fixture_container="bamboo-russh-live-${run_identity}"
fixture_image="bamboo-russh-live:${run_identity}"

ssh-keygen -q -t ed25519 -N '' -C bamboo-russh-live -f "${private_key}"

docker build --pull --tag "${fixture_image}" "${fixture_dir}"
docker run --detach \
  --name "${fixture_container}" \
  --publish 127.0.0.1::22/tcp \
  --mount "type=bind,src=${public_key},dst=/run/bamboo-russh-fixture/authorized_key.pub,readonly" \
  "${fixture_image}" >/dev/null

readonly readiness_deadline=$((SECONDS + 30))
fixture_health=""
while (( SECONDS < readiness_deadline )); do
  fixture_health="$(docker inspect --format '{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' "${fixture_container}" 2>/dev/null || true)"
  if [[ "${fixture_health}" == "running healthy" ]]; then
    break
  fi
  if [[ "${fixture_health}" == exited* || "${fixture_health}" == dead* ]]; then
    break
  fi
  sleep 0.25
done

if [[ "${fixture_health}" != "running healthy" ]]; then
  echo "SSH fixture did not become healthy within 30 seconds (state: ${fixture_health:-missing})" >&2
  docker logs "${fixture_container}" >&2 || true
  exit 1
fi

ssh_endpoint="$(docker port "${fixture_container}" 22/tcp | head -n 1)"
readonly ssh_port="${ssh_endpoint##*:}"
if [[ ! "${ssh_port}" =~ ^[0-9]+$ ]] || (( ssh_port < 1 || ssh_port > 65535 )); then
  echo "Docker returned an invalid fixture SSH endpoint: ${ssh_endpoint}" >&2
  exit 1
fi

cd "${repo_root}"
RUSSH_HOST=127.0.0.1 \
RUSSH_PORT="${ssh_port}" \
RUSSH_USER=deploy \
RUSSH_KEY_PATH="${private_key}" \
RUSSH_WORKER_ID="node-russh-live-${run_identity}" \
cargo test --locked -p bamboo-broker --test russh_live \
  russh_deploys_through_reverse_tunnel -- \
  --exact --ignored --nocapture --test-threads=1
