# Docs index

New here? Start at [`guides/GETTING_STARTED.md`](guides/GETTING_STARTED.md).
The top-level [`README.md`](../README.md) has the project overview, capability
summary, and CLI command table.

## User guides

Task-oriented, kept current with the shipped codebase.

| Doc | What it covers |
|---|---|
| [`guides/GETTING_STARTED.md`](guides/GETTING_STARTED.md) | Install, configure a provider, run your first turn three ways (CLI/HTTP/SDK). |
| [`config-reference.md`](config-reference.md) | Every `config.json` key, the sibling config files (`connect.json`, `schedules.json`, `model_limits.json`), every `BAMBOO_*` env var, the secret-masking contract, encryption at rest, and corrupt-config recovery. |
| [`lifecycle-hooks.md`](lifecycle-hooks.md) | Configure command or external multi-language script lifecycle handlers, event payloads, decisions, runtime selection, and practical examples. |
| [`guides/CONNECT.md`](guides/CONNECT.md) | Drive Bamboo from Telegram or Feishu/Lark. |
| [`guides/PLUGINS.md`](guides/PLUGINS.md) | Install/update/remove plugins, and the three-layer URL trust model. |
| [`guides/DEPLOY.md`](guides/DEPLOY.md) | Run `bamboo serve` long-lived: bare binary, systemd, Docker, reverse proxy, backups. |
| [`guides/API.md`](guides/API.md) | HTTP/SSE API reference. |
| [`guides/MIGRATION_GUIDE.md`](guides/MIGRATION_GUIDE.md) | Upgrading across breaking changes. |
| [`architecture-crates.md`](architecture-crates.md) | The Cargo workspace's crate layers and dependency direction — useful when extending Bamboo itself, not just using it. |
| [`claude-code-executor.md`](claude-code-executor.md) | Protocol reference for driving the `claude` CLI as a sub-agent executor (`subagents.executor = "claude_code"` in the [config reference](config-reference.md#sub-agents--external-cli-executors)). |
| [`codex-executor.md`](codex-executor.md) | Configure the four Codex authentication/billing modes, isolated `CODEX_HOME`, custom providers, and the recommended parent-routed per-run token mode. |
| [`security/KNOWN_VULNERABILITIES.md`](security/KNOWN_VULNERABILITIES.md) | Tracked `cargo audit` advisories and their resolution status. |
| [`../examples/`](../examples/) | Compiling, runnable code — quickstart, streaming real event types, a custom tool, resuming a session, the `ExecuteRequest` escape hatch, connecting an MCP server. Built in CI (`cargo build --examples`), so they can't silently drift from the real API. |

## Design notes (historical)

[`design/`](design/) holds implementation plans and architecture RFCs written
*before* or *during* the work they describe — most of what they propose has
since shipped. They're kept for provenance (the "why" behind a design, ratified
decisions, alternatives considered) but are **not maintained** against the
current codebase and may describe an intermediate or superseded state. If a
design doc and the code disagree, the code is correct — read the doc for
history, not as a spec.

<details>
<summary>Full list</summary>

- [`design/api-v2-transport.md`](design/api-v2-transport.md) — unified WS transport + in-process TLS design (API v2 transport epic).
- [`design/architecture-overview.md`](design/architecture-overview.md) — broker-mediated remote sub-agents, hub-and-spoke topology.
- [`design/ask-agent-design.md`](design/ask-agent-design.md) — durable mailbox request/reply to a sub-agent (`ask_agent`).
- [`design/edit-fuzzy-matching-design.md`](design/edit-fuzzy-matching-design.md) — fuzzy matching design for the `Edit` tool.
- [`design/ergonomic-sdk-plan.md`](design/ergonomic-sdk-plan.md) — the plan behind the current `bamboo-sdk` facade (`Agent`/`AgentBuilder`).
- [`design/feishu-adapter-plan.md`](design/feishu-adapter-plan.md) — Feishu/Lark `bamboo-connect` adapter plan (epic #447 phase 3).
- [`design/multi-provider-routing-refactor-plan.md`](design/multi-provider-routing-refactor-plan.md) — multi-provider/multi-model routing (session-level provider+model).
- [`design/personal-assistant-ledger.md`](design/personal-assistant-ledger.md) — the ledger memory subsystem design.
- [`design/phase1-provider-registry-plan.md`](design/phase1-provider-registry-plan.md) / [`phase1-provider-registry-patches.md`](design/phase1-provider-registry-patches.md) — `ProviderRegistry` multi-instance provider runtime, plan + patch breakdown.
- [`design/prompt-cache-architecture-plan.md`](design/prompt-cache-architecture-plan.md) — prompt/message/cache reorganization for stable provider prompt-cache hits.
- [`design/provider-model-first-class-plan.md`](design/provider-model-first-class-plan.md) — making provider+model a first-class selection unit.
- [`design/remote-actor-plan.md`](design/remote-actor-plan.md) — remote sub-agent actor seams (P0/P1/P2).
- [`design/remote-mailbox-broker-design.md`](design/remote-mailbox-broker-design.md) — the standalone `bamboo broker` network mailbox design.
- [`design/subagent-actor-runtime-design.md`](design/subagent-actor-runtime-design.md) — the virtual-actor model sub-agents run on today.
- [`design/subagent-store-mailbox-interface.md`](design/subagent-store-mailbox-interface.md) — `bamboo-subagent`'s `store/`/`mailbox/` interface spec.
- [`design/reviews/actor-runtime-self-review-2026-06-12.md`](design/reviews/actor-runtime-self-review-2026-06-12.md) — a self-review snapshot of the actor runtime.

</details>

Adding a new design doc? Put it in `design/` from the start if it's a plan/RFC
for work not yet shipped — promote genuinely user-facing outcomes into a guide
above once the feature lands, rather than leaving the plan doc as the only
documentation.
