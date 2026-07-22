# Codex CLI executor

`ExecutorSpec::Codex` runs one official Codex CLI process per child activation
using `codex exec --json`. Bamboo passes the assignment through stdin, consumes
bounded JSONL events, records the resolved binary and version in bootstrap
metadata, and captures the final answer independently with
`--output-last-message`.

The minimum supported version is **Codex CLI 0.144.0**. The implementation was
verified against 0.144.5 and also checks the required `exec --json`, stdin
prompt, `--output-last-message`, and `exec resume --json` help surfaces during
preflight. Unknown future event and item types are debug-logged and skipped.

The core executor defaults to `--sandbox read-only`, clears the child
environment before forwarding a fixed OS allowlist, and owns a process group.
Cancellation sends SIGTERM to that group, waits five seconds, then escalates to
SIGKILL. Authentication/provider configuration, Bamboo permission-profile
mapping, and durable resume are introduced by the dependent issues in epic
#568.

For an opt-in real-machine check against the logged-in CLI:

```sh
cargo test --test e2e_codex_cli_manual -- --ignored --nocapture
```
