# Terminal-Bench 2.1 local evaluation

Bamboo solved **60 of 78 tasks (76.92%)** in a local Terminal-Bench 2.1 subset with DS Flash high and Auto permissions. The run used the Linux x86-64 build of `7efc6bbd0bcb141c28ad261de74f4a6bc660ba74`, the merge that restored `exec_command` and `write_stdin` and changed quiet polling. [PR #2754](https://github.com/bigduu/Bamboo-agent/pull/2754)

[Download the per-task CSV](./results.csv) · [Run configuration and task roster](./run-config.json) · [Rerun commands](#rerun)

## Scoring and the Bash baseline

Harbor verifier reward `1` counts as a pass. A normal exit requires a Bamboo `finished` event and exit code `0`. Of the 60 passes, 51 ended normally and 9 earned reward `1` after the agent exceeded its time limit. The 23 tasks without a valid Bash pass and the 55 tasks with a Bash pass were run in separate batches with the same Bamboo binary. One trial of `filter-js-from-html` was retried after a model stream reset. Its retry exited normally and earned reward `0`.

| Outcome on the same 76 tasks with scores for both versions | Bash baseline | Merged Bamboo |
| --- | ---: | ---: |
| Verifier passes | 55 | 58 |
| Verifier passes with normal Bamboo exit | 51 | 49 |

Ten tasks changed from fail to pass and seven changed from pass to fail. Two other tasks had infrastructure failures in the Bash run and passed in the merged run. The CSV leaves invalid Bash rewards blank and records the agent exit and verifier outcome separately.

The 78 tasks came from an existing local cohort. Eight of the public board's 87 tasks were outside that prior cohort, and one Windows task was excluded after a known failure in the local VM environment. The configuration lists the included and omitted task IDs. Each task used the images, resource limits and deadlines from the pinned dataset.

## Same-task reference ranking

On these 78 tasks the local result places **2nd of 11 by observed pass rate**. The comparison recomputes the public **Harness Leaderboard v0.1.7** DeepSeek-V4.1-Flash high results using only the same task IDs. The scorer reproduced the published 87-task tallies before taking the intersection. The snapshot was generated on November 15, 2026 at 23:52:47 China Standard Time. [Public results](https://huggingface.co/buckets/joelniklaus/harness-leaderboard)

| Reference rank | Harness | Passed / attempts | Pass rate |
| --- | --- | ---: | ---: |
| 1 | Kimi Code | 181 / 234 | 77.35% |
| **2** | **Bamboo (local run)** | **60 / 78** | **76.92%** |
| 3 | Codex | 172 / 234 | 73.50% |
| 4 | Claude Code | 171 / 234 | 73.08% |
| 4 | OpenCode | 171 / 234 | 73.08% |
| 6 | Pi | 169 / 234 | 72.22% |
| 6 | DeepSeek Harness | 169 / 234 | 72.22% |
| 8 | mini-swe-agent | 166 / 234 | 70.94% |
| 9 | Qwen Code | 165 / 234 | 70.51% |
| 10 | Hermes Agent | 145 / 234 | 61.97% |
| 11 | OpenClaw | 120 / 234 | 51.28% |

Each public harness has three scored attempts per task and the board applies its own trajectory review. Bamboo contributes one valid trial per task, scored by Harbor on OrbStack with a local inference gateway. The response logs identify the gateway alias `deepseek-flash` and the request logs record `high` effort. Independent evidence of provider equivalence with the public runs is unavailable. One task changes the local score by 1.28 percentage points, compared with a 0.43-point gap to Kimi Code in this snapshot. [Public evaluation method](https://huggingface.co/spaces/joelniklaus/harness-leaderboard/blob/main/README.md#method-in-short)

## Rerun

Use Bash, Git, `jq`, Docker, `uv` and a container-reachable OpenAI-compatible inference endpoint. The original gateway exposed the model alias `deepseek-flash` and forwarded `reasoning_effort=high` on every request, including auxiliary calls. Export `BAMBOO_BENCH_BASE_URL` for a route with that alias and effort behavior, and `BAMBOO_BENCH_API_KEY` to its inference-only credential in your shell. The adapter reads the credential from the environment.

Run from a checkout containing this report.

```bash
evidence="$(git rev-parse --show-toplevel)/docs/benchmarks/terminal-bench-2.1-2026-11-15"
config="$evidence/run-config.json"
replay_dir="$(mktemp -d)"
```

Build the pinned source in a Linux x86-64 environment with Rust 1.99.1, a GNU toolchain, pkg-config and OpenSSL development headers. The recorded build used a Debian Bookworm environment. If you already built the pinned x86-64 Linux binary, set and export `BAMBOO_BENCH_BINARY` and skip this build block.

```bash
git clone --no-checkout https://github.com/bigduu/Bamboo-agent.git "$replay_dir/bamboo"
git -C "$replay_dir/bamboo" checkout --detach "$(jq -r '.bamboo.source_commit' "$config")"
(
  cd "$replay_dir/bamboo" &&
  BAMBOO_FRONTEND_BUILD_MODE=api-only cargo +1.99.1 build --locked --release \
    --target x86_64-unknown-linux-gnu --bin bamboo
)
export BAMBOO_BENCH_BINARY="$replay_dir/bamboo/target/x86_64-unknown-linux-gnu/release/bamboo"
```

The binary hash in the configuration identifies the actual measured build. The source build recipe uses the same revision and build mode. Build environments and inference routes can affect the result.

```bash
: "${BAMBOO_BENCH_BASE_URL:?Set a container-reachable OpenAI-compatible route}"
: "${BAMBOO_BENCH_API_KEY:?Set an inference-only credential for that route}"
: "${BAMBOO_BENCH_BINARY:?Set the path to the pinned Linux x86-64 binary}"

git clone --no-checkout "$(jq -r '.dataset_repository' "$config")" "$replay_dir/dataset"
git -C "$replay_dir/dataset" checkout --detach "$(jq -r '.dataset_commit' "$config")"
uv venv --python 3.12 "$replay_dir/harbor"
uv pip install --python "$replay_dir/harbor/bin/python" 'harbor==0.25.0'

task_args=()
while IFS= read -r task; do
  task_args+=(--include-task-name "$task")
done < <(jq -r '.tasks[]' "$config")

DOCKER_DEFAULT_PLATFORM=linux/amd64 HARBOR_TELEMETRY=0 PYTHONPATH="$evidence" \
  "$replay_dir/harbor/bin/harbor" run -p "$replay_dir/dataset/tasks" -e docker \
  -a harbor_agent:BambooBench -m deepseek/deepseek-flash \
  -n 1 -k 1 -r 0 --job-name bamboo-tb21-78 --jobs-dir "$replay_dir/jobs" \
  "${task_args[@]}"
```

Append `--dry-run` to validate the selection and Harbor configuration. The rerun command uses one concurrent container so it can run within the largest task's resource limit. The recorded run admitted up to five trials under an eight-CPU and 13 GiB budget. Both use the same per-task limits. To rerun the Bash baseline, build `.bamboo.baseline_commit` from the configuration and use that binary with the same Harbor command and a new job name.

To read the new verifier totals from the saved Harbor trial results:

```bash
jq -s 'map(.verifier_result.rewards.reward) | {
  trials: length,
  passed: map(select(. == 1)) | length,
  failed: map(select(. == 0)) | length,
  unscored: map(select(. == null)) | length
}' "$replay_dir"/jobs/bamboo-tb21-78/*/result.json
```
