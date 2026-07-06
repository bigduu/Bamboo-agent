<div align="center">

# Bamboo 🎋

<img src="./docs/assets/bamboo-agent-hero.svg" alt="Bamboo agent runtime overview" width="100%" />

### The local-first AI agent runtime, in Rust.

**Persistent memory, 22 built-in tools, skills, MCP, workflows & schedules — behind one HTTP + SSE API.**
Run it as a server, or embed the same agent loop as a Rust crate. Your data stays on your machine.

[![Crates.io](https://img.shields.io/crates/v/bamboo-agent.svg?logo=rust)](https://crates.io/crates/bamboo-agent)
[![docs.rs](https://img.shields.io/docsrs/bamboo-agent?logo=docsdotrs&label=docs.rs)](https://docs.rs/bamboo-agent)
[![CI](https://img.shields.io/github/actions/workflow/status/bigduu/Bamboo-agent/ci.yml?branch=main&logo=github&label=CI)](https://github.com/bigduu/Bamboo-agent/actions/workflows/ci.yml)
[![License MIT](https://img.shields.io/badge/license-MIT-green)](./LICENSE)
[![中文 README](https://img.shields.io/badge/lang-中文-red)](./README.zh-CN.md)

</div>

---

## What is this

Bamboo is the "brain" of an AI assistant that runs on your own machine. It does far more than chat — it takes notes, grows a searchable long-term memory, uses tools (read/write files, run commands, search the web), and automatically compacts very long conversations so the assistant never "forgets" or grinds to a halt. All of this lives inside one compact, self-hostable program, with your data staying local by default.

If Bodhi is the AI product you see, **Bamboo is the engine running underneath it.**

---

## Key Capabilities at a Glance

| Capability | What it does |
|---|---|
| 🧠 **Memory system** | Session notes, Dream notebook, cross-session durable memory, with auto-dream and background gardener |
| 🗜️ **Context compression** | Hybrid compression with rolling summary + recent-window retention, automatic trimming of oversized tool output, executed against the model's context-window budget |
| 🛠️ **Built-in tools** | 22 built-in tools: files, search, Shell, Web, plan mode, tasks, permission requests, and more |
| 🎯 **Skills** | Optional/discoverable skills with lightweight selection based on request hints, including built-in docx / pdf / pptx / xlsx / skill-creator |
| 🔌 **MCP** | Model Context Protocol client that hooks into external tool servers |
| ⏰ **Workflows & schedules** | Declarative workflow loading + a cron-style schedule trigger engine |
| 🌐 **HTTP / SSE** | Actix server, REST API, Server-Sent Events streaming, compatible with OpenAI / Anthropic / Gemini endpoints |
| 🏗️ **Multi-provider** | anthropic (default), openai, gemini, copilot, bodhi routing |

---

## Architecture

Bamboo is a Cargo **workspace**: a thin root binary (`bamboo-agent`, which exposes the `bamboo` command) sits on top of focused crates organized into four tiers — `crates/core/` (types + interfaces), `crates/infra/` (independent services), `crates/engine/` (core logic), and `crates/app/` (executables + entry points). The live server is `crates/app/bamboo-server` (there is no duplicate server tree). `bamboo-agent-core` depends **only** on `bamboo-domain`, keeping the core abstractions clean.

```mermaid
graph TD
  CLI["bamboo (root bin)<br/>serve / config / -p headless / actor / broker"] --> SRV[bamboo-server<br/>Actix HTTP + SSE, routes, schedules, workflows]
  SRV --> ENG[bamboo-engine<br/>agent runtime, auto-dream, gardener, metrics]
  ENG --> CORE[bamboo-agent-core<br/>core abstractions]
  CORE --> DOM[bamboo-domain<br/>pure domain types]
  ENG --> MEM[bamboo-memory<br/>session notes, durable memory, plan store, budget]
  ENG --> CMP[bamboo-compression<br/>token budgeting, summarizer, limits]
  ENG --> SKILLS[bamboo-skills<br/>selection, access control, runtime metadata]
  ENG --> MCP[bamboo-mcp<br/>MCP client: manager, protocol, transports, tool_index]
  ENG --> TOOLS[bamboo-tools<br/>22 built-in tools, registry, guides, permissions]
  ENG --> INFRA[bamboo-infrastructure<br/>config, LLM providers, session store]
  SRV --> INFRA
  TOOLS --> INFRA
  MEM --> INFRA
  CLI2["bamboo-cli / bamboo-tui<br/>thin clients over HTTP"] -.-> SRV
```

**Workspace members** (from `Cargo.toml`), organized by tier:

- **`crates/core/`** — `bamboo-domain` (pure domain types), `bamboo-agent-core` (core abstractions)
- **`crates/infra/`** — `bamboo-config`, `bamboo-llm`, `bamboo-storage`, `bamboo-a2a`, `bamboo-infrastructure`, `bamboo-memory`, `bamboo-metrics`, `bamboo-notification`, `bamboo-skills`, `bamboo-mcp`, `bamboo-permission`, `bamboo-compression`, `bamboo-subagent`, `bamboo-analytics` (dev-only)
- **`crates/engine/`** — `bamboo-engine`, `bamboo-tools`
- **`crates/app/`** — `bamboo-server`, `bamboo-server-tools`, `bamboo-sdk`, `bamboo-cli`, `bamboo-tui`, `bamboo-client-core`, `bamboo-broker`

…plus the root `bamboo-agent` binary.

**Place in the Zenith stack:** lotus (the React UI) and bamboo communicate over **HTTP**; bodhi (the Tauri shell) is just the container that hosts the interface. Bamboo is the execution engine, and bodhi-server (Go) handles accounts/persistence/billing and the LLM proxy.

---

## Signature Deep-Dives

### Memory System · `crates/infra/bamboo-memory`

Memory has three layers:

- **Session notes** — written by the `session_note` tool (actions: `session_read` / `session_append` / `session_replace` / `session_clear` / `session_list_topics`); these are temporary drafts/facts within the current session.
- **Dream notebook** — a background process "dreams" over a stretch of conversation, distilling it into structured candidate memories and consolidating them into the notebook (`auto_dream.rs`).
- **Durable memory** — survives across sessions, with frontmatter (type, status, source, relations, retrieval metadata), scoped as `session` / `project` / `global` (`memory_store/types.rs`).

**Auto-dream** (`MemoryConfig.auto_dream_enabled`, **off by default** because it consumes model tokens) performs extraction, consolidation, and Dream generation as the conversation evolves; it supports three modes: `Incremental`, `Refine`, `Rebuild`.

**Gardener** (`bamboo-engine/src/gardener.rs`, `gardener_enabled` off by default) specializes in splitting "multi-topic blob memories." It has cost guardrails: a hard per-run split cap, a slow cadence (daily by default), and **it calls no LLM when the deterministic pre-screen finds no candidates** — an idle gardener costs nothing. The split "work list" is produced for free by `MemoryStore::scan_blob_candidates`; only the split "decision" uses the model.

> Why it matters: the memory system lets the assistant understand your project better over long-term use, while keeping cost controlled and data local.

### Context Compression · `crates/infra/bamboo-compression`

Long conversations don't grow without bound. Bamboo uses a **hybrid strategy**: a rolling summary + a recent message window.

- `counter` — counts tokens via tiktoken BPE or heuristic estimation (`TiktokenTokenCounter` / `HeuristicTokenCounter`).
- `segmenter` — preserves the atomicity of tool calls when segmenting (it won't split a single tool call apart).
- `limits` — **deliberately ships no per-model table**. Real context/output limits come from (1) provider runtime metadata, (2) user overrides in `model_limits.json`; with neither, it falls back to a global default of **200K context / 64K output**. This way the table never goes stale as models are updated.
- `summarizer` / `preparation` — builds the compression plan, generates the summary message, prepares context against the budget (`prepare_hybrid_context`), and can estimate prompt-cache savings.
- **Oversized output** — oversized output produced by tools is trimmed/managed at `bamboo-tools/output_manager.rs`, avoiding stuffing the context all at once.

> Why it matters: the assistant can do long, multi-step work without crashing from context overflow or "losing its memory."

### Skill System · `crates/infra/bamboo-skills`

Skills are enableable capability bundles. At runtime it resolves the "selected skills" from session metadata (supporting JSON arrays or the legacy comma-separated format), and performs lightweight, request-hint-based relevance selection for **unselected skills** to inject into context (capped at `MAX_UNSELECTED_SKILLS_IN_CONTEXT = 24`), avoiding stuffing every skill into the prompt. It also includes access control and runtime metadata.

Built-in skills live in `builtin_skills/`: `docx`, `pdf`, `pptx`, `xlsx`, `skill-creator`.

### Tools, Workflows, Schedules, MCP

- **Tools** (`bamboo-tools`, **22 built-in**, registered in `executor.rs::register_builtin_tools`): `Bash`, `BashOutput`, `KillShell`, `Read`, `Write`, `Edit`, `NotebookEdit`, `Glob`, `Grep`, `GetFileInfo`, `Workspace`, `WebFetch`, `WebSearch`, `JsRepl`, `Task`, `Sleep`, `EnterPlanMode`, `ExitPlanMode`, `RequestPermissions`, `SessionNote`, `ConclusionWithOptions`, and more. Tools come with **usage guides** injected at runtime, a **permission/policy-aware** execution path, and parallel execution support (`parallel.rs`).
- **Workflows** — declarative loading (`bamboo-server/src/workflow/loader.rs`), exposed via `/bamboo/workflows`.
- **Schedules** — a cron-style trigger engine and store (`bamboo-server/src/schedules/`: `manager`, `trigger_engine`, `session_factory`, `store`).
- **MCP** — Model Context Protocol client (`crates/infra/bamboo-mcp/`: `manager`, `protocol`, `transports`, `tool_index`), managing external tool servers via the `/mcp`, `/servers` routes.

---

## Quick Start & Development

### First-run setup

Configure a provider + API key without hand-editing JSON:

```bash
# interactive — prompts for provider + API key (uses a default model unless --model is given)
bamboo init

# or non-interactive (CI / scripting)
bamboo init --non-interactive --provider anthropic --api-key "sk-ant-..."

# verify the install (config present, provider keyed, server reachable)
bamboo doctor

# set/rotate a single value later
bamboo config set providers.openai.api_key "sk-..."
bamboo config set provider openai
```

`init` writes `~/.bamboo/config.json` (override with `--data-dir`) and stores the key **encrypted at rest**. `doctor` exits non-zero if a blocking problem is found, so it doubles as a readiness check.

### Run the server

```bash
# build & run from the workspace
cargo run --bin bamboo -- serve

# or install then run
cargo install --path .
bamboo serve
```

Arguments supported by `bamboo serve` (all override the config file):
`--port`, `--bind`, `--data-dir`, `--static-dir`, `--workers` (plus `--parent-pid`, a sidecar orphan-guard: the process exits when that PID goes away).

**Other subcommands** (`bamboo --help` / `bamboo <cmd> --help` for the full list):

| Command | What it does |
|---|---|
| `bamboo serve` | Start the HTTP/SSE server (above). |
| `bamboo init` | First-run setup: write `config.json` with a provider + API key (interactive, or `--non-interactive` for CI). |
| `bamboo doctor` | Diagnose the install (config present, provider keyed, server reachable); exits non-zero on a blocking problem. |
| `bamboo config [--path] [--show-secrets]` | Inspect the resolved configuration. |
| `bamboo config set <key> <value>` | Set one value, e.g. `providers.anthropic.api_key`, `providers.<p>.model`, or `provider`. |
| `bamboo -p "<prompt>"` | One-shot **headless** agent run (boots the full runtime incl. sub-agents, prints the result, exits). Optional `-s <session>` to continue, `-m provider:model` to pin the model, `--workspace`, `--data-dir`, `--stream-json` (NDJSON on stdout), `--echo` (keyless transport smoke). |
| `bamboo actor run\|serve\|list\|call` | Drive the sub-agent actor fabric from the terminal (spawn + stream, run as a service, discover, or send a task). |
| `bamboo broker serve` | Run the standalone sub-agent message broker (WebSocket bus over durable mailboxes). |
| `bamboo broker-agent serve` | Run a broker-connected agent (local / Docker / remote) that answers Ask/Task for its mailbox. |
| `bamboo health` | Probe a running server's `/health` (exit non-zero if unreachable/unhealthy — usable as a readiness check). |
| `bamboo status` | One-screen overview of a running server: address, health, session counts. |
| `bamboo sessions` | List sessions on a running server (stop one with `bamboo stop <id>`). |
| `bamboo stop <session_id>` | Stop a running session's agent loop. |

The admin commands (`health` / `status` / `sessions` / `stop`) are thin HTTP clients over a running `bamboo serve`; point them at a non-default server with `--server-url` / `--port` / `--data-dir`. (`bamboo subagent-worker` also exists but is an internal worker process spawned by the server — not for interactive use.)

**Defaults** (verified against code):

- HTTP API: `http://127.0.0.1:9562/api/v1` (port defaults to `9562`, bind defaults to `127.0.0.1`)
- Health: `GET /api/v1/health`
- Data dir: `BAMBOO_DATA_DIR` or `${HOME}/.bamboo`
- Default provider: `anthropic`

### Call the agent loop

Once the server is running, driving the **full agent loop** — the LLM plans, calls tools, and streams its work — is three HTTP calls: create the turn with `POST /api/v1/chat`, **start the loop** with `POST /api/v1/execute/{session_id}`, then watch the SSE feed `GET /api/v1/events/{session_id}`.

```bash
# 1. Create a turn. This PERSISTS the message and returns immediately — it does
#    NOT run the loop yet. Response includes the session id and events URL:
#    { "session_id": "...", "stream_url": "/api/v1/events/<id>", "status": "streaming" }
SID=$(curl -s http://127.0.0.1:9562/api/v1/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"List the files here and tell me what this project does.","model":"claude-sonnet-4-6"}' \
  | jq -r .session_id)

# 2. Start the agent loop for that session. The body may be empty ({}) — every
#    field (model/provider/skill_mode/reasoning_effort/…) is an optional override.
curl -s -X POST "http://127.0.0.1:9562/api/v1/execute/$SID" \
  -H 'Content-Type: application/json' -d '{}'

# 3. Watch the loop in real time (SSE): assistant text, tool calls, tool results,
#    token usage, and completion arrive as they happen.
curl -N "http://127.0.0.1:9562/api/v1/events/$SID"
```

On `POST /api/v1/chat`, `message` and `model` are the only required fields; useful optionals are `session_id` (continue a conversation), `system_prompt`, `selected_skill_ids`, `workspace_path`, `provider`, `images`. Note that `chat` only **persists** the turn — you must then `POST /api/v1/execute/{session_id}` to actually run the loop. Besides the per-session `GET /api/v1/events/{session_id}` feed, there is an account-wide, resumable change feed `GET /api/v1/stream` (SSE, resumable via `?since=<seq>` or the `Last-Event-ID` header) that streams events across **all** sessions — handy for multi-session sync.

### Use it as a Rust SDK (in-process)

No server needed — the **same agent loop** runs in-process. The `bamboo_sdk` crate is an ergonomic **facade** over the engine: you supply a model and an instruction, `.with_defaults_for_data_dir` wires the eight runtime dependencies (storage, persistence, attachment reader, skills, metrics, config, provider, default tools) from `~/.bamboo`, and then `agent.run(&mut session, input)` drives one turn (draining events internally) while `agent.run_stream(session, input)` streams `AgentEvent`s back over an `mpsc` channel. Every call funnels into the engine's single canonical execution path — the facade never forks the loop. The ergonomic types live in `bamboo_sdk::agent` (`Agent`, `AgentBuilder`, `ExecuteRequestBuilder`, plus re-exported `AgentEvent`, `Session`, …).

```rust
use bamboo_sdk::agent::{Agent, Session};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let home = dirs::home_dir().unwrap().join(".bamboo");

    // Build the agent. One call assembles storage, persistence, skills,
    // metrics, the provider (from ~/.bamboo/config.json), and the default
    // built-in tool set — no manual dependency wiring.
    let agent = Agent::builder()
        .model("claude-sonnet-4-6")
        .instruction("You are a helpful coding agent.")
        .with_defaults_for_data_dir(home)
        .await
        .expect("wire runtime deps")
        .build()
        .expect("agent fully configured");

    // Stream one turn: `run_stream` appends the user message, runs the loop on
    // a background task, and hands back a receiver of AgentEvents.
    let session = Session::new("demo-session", "claude-sonnet-4-6");
    let mut rx = agent.run_stream(
        session,
        "List the files here and tell me what this project does.",
    );
    while let Some(event) = rx.recv().await {
        println!("{event:?}"); // assistant text, tool calls, tool results, token usage, completion
    }
    Ok(())
}
```

> **Precondition:** `with_defaults_for_data_dir` reads `~/.bamboo/config.json` (the same config `bamboo serve` uses) and needs the active provider configured with a non-empty `api_key` — otherwise provider creation returns an error (here surfaced by `.expect`). A fresh data dir with no `config.json` defaults to `anthropic` with no key and will fail; `copilot` is the only provider that authenticates keyless (cached OAuth). Fix it with `bamboo init` (or `bamboo config set providers.<p>.api_key …`), or pass `.api_key("sk-…")` on the builder before `with_defaults_for_data_dir`.

> Don't need the event stream? `agent.run(&mut session, input).await?` drives the turn to completion and leaves the answer as the last message on `session`. For full control over per-request overrides (split fast/background/summarization models, skill selection, provider handles, …) build an `ExecuteRequest` with `ExecuteRequestBuilder` (both re-exported from `bamboo_sdk::agent`) and call `agent.execute(&mut session, req)` — the same canonical engine path `run` / `run_stream` funnel into.

Add the facade crate as a dependency (path or git):

```toml
[dependencies]
bamboo-sdk = { git = "https://github.com/bigduu/Bamboo-agent" }
tokio = { version = "1", features = ["full"] }
dirs = "5"
anyhow = "1"
```

> Prefer not to manage these dependencies yourself? Run `bamboo serve` and use the HTTP API above — it drives the exact same loop. The full SDK type reference is the rustdoc at [docs.rs/bamboo-agent](https://docs.rs/bamboo-agent) (the published crate re-exports the facade as `bamboo_agent::agent`); [`docs/guides/API.md`](./docs/guides/API.md) covers the HTTP/SSE surface.

### Example configuration

The easiest way to create this is `bamboo init` (see [First-run setup](#first-run-setup)), which writes it for you and encrypts the key. The equivalent file at `${HOME}/.bamboo/config.json`:

```json
{
  "provider": "anthropic",
  "server": {
    "port": 9562,
    "bind": "127.0.0.1"
  },
  "providers": {
    "anthropic": {
      "api_key": "sk-ant-...",
      "model": "claude-sonnet-4-6"
    }
  }
}
```

> Config precedence: file < environment variables < CLI arguments. Environment variables include `BAMBOO_DATA_DIR`, `BAMBOO_PORT`, `BAMBOO_BIND`, `BAMBOO_PROVIDER`, `BAMBOO_WORKERS`, `BAMBOO_CORS_ALLOW_ORIGINS`.

### Docker

```bash
cd docker && docker compose up -d --build
curl http://localhost:9562/api/v1/health
```

`docker-compose.yml` maps `9562:9562` and sets `BAMBOO_DATA_DIR=/data`, `BAMBOO_PORT=9562`, `BAMBOO_BIND=0.0.0.0`.

### Selected API routes

REST prefix `/api/v1`: `chat`, `execute/{session_id}`, `stream`, `sessions`, `skills`, `tools`, `tools/execute`, `models`, `commands`, `workflows`, `metrics/*`, `mcp`, `servers`, `stop/{session_id}`, `health`.
There are also provider-compatible endpoints: `/openai/v1`, `/anthropic/v1`, `/gemini/v1beta`, `/v1/{chat/completions,responses,messages}`.

### Tests & quality

```bash
cargo test            # workspace tests
cargo clippy          # lints (.clippy.toml present)
cargo build --release
```

---

## The Rest of the Stack

Zenith is a monorepo, and bamboo is the execution-engine submodule within it.

| Module | Role |
|---|---|
| [**bodhi**](../bodhi) | Desktop AI product surface (Tauri shell) |
| [**lotus**](../lotus) | React+Vite UI layer (talks to bamboo over HTTP) |
| **bamboo** | Local-first Rust agent runtime (this repo) |
| [**bodhi-server**](../bodhi-server) | Go backend: auth, persistence, billing+quota, LLM proxy |
| [**pavilion**](../pavilion) | Official website & docs |
| [**Zenith (root)**](../) | Monorepo entry, submodule pointers, release train |

**In-module docs:**
- API reference: [`docs/guides/API.md`](./docs/guides/API.md)
- Migration: [`docs/guides/MIGRATION_GUIDE.md`](./docs/guides/MIGRATION_GUIDE.md)
- [CONTRIBUTING](./CONTRIBUTING.md) · [CHANGELOG](./CHANGELOG.md) · [SECURITY](./SECURITY.md)

---

## License

MIT
