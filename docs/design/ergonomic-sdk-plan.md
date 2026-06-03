# Ergonomic SDK Plan: Promoting `SubagentProfile` into `bamboo_agent::agent`

**Status:** Design / implementation spec
**Author:** Lead architect (reconciled from 6 explorer reports + direct code re-read)
**Date:** 2026-06-04

## 0. Goal & guiding constraints

Promote the existing `SubagentProfile` machinery into a first-class, ergonomic SDK
surfaced at `bamboo_agent::agent` so library consumers can do:

```rust
let agent = Agent::builder().researcher().model("...").build()?;
agent.run(&mut session, "investigate X").await?;
// or, profile-driven child spawn:
runner.run_profile(profile, input).await?;
```

### Hard constraints (these override any explorer suggestion that conflicts)

1. **Dependency direction is sacred:** `domain ← tools ← engine ← root facade`,
   with `infrastructure` and `agent-core` as shared lower layers. **No reverse
   edges.** Verified Cargo edges:
   - `bamboo-tools` → `bamboo-agent-core`, `bamboo-domain`, `bamboo-infrastructure`
   - `bamboo-engine` → `bamboo-domain`, `bamboo-infrastructure`, `bamboo-agent-core`, `bamboo-tools`
   - `bamboo-server` → all of the above
   - root crate → all of the above + `bamboo-server`
2. **Anti-fork:** The SDK runner MUST NOT duplicate `run_spawn_job`'s 330 lines of
   spawn/finalize logic. It MUST reuse the single canonical spawn path. The server
   must be *rewired onto* the runner, leaving exactly one implementation.
3. **All existing tests stay green.** No behavioral drift in events, prompts,
   tool-policy, or model resolution.

### Contradictions found between explorers — RESOLVED

| # | Conflict | Resolution (after re-reading code) |
|---|----------|-----------------------------------|
| C1 | Explorer 1 proposes `sdk/runner.rs` with a fat `RuntimeDeps`/`RunOutcomeStream` (broadcast). Explorer 5 proposes `sdk/spawn.rs` extracting `run_spawn_job` into `spawn_profile_child`. | **Merge.** One module `crates/bamboo-engine/src/sdk/`. The *core* extraction is `spawn.rs` (refactor `run_spawn_job` so its body becomes a reusable `run_child_spawn(ctx, job)` taking the existing `SpawnContext` + `SpawnJob`). `runner.rs` is a thin ergonomic facade (`ProfileRunner`) over that core. **No new `RuntimeDeps` god-struct** — reuse `SpawnContext`, which already holds exactly the dependencies (agent, tools, caches, router, completion_handler). Explorer 1's 14-field `RuntimeDeps` is rejected as a redundant parallel of `SpawnContext`. |
| C2 | Explorer 1: `ExecuteRequest` has 23 fields incl. `fast_model*`, `background_model*`, `summarization_model*`. Explorer 4: "21 optional". | **Code is authoritative.** Real `ExecuteRequest` (spawn.rs:563-587) has split provider fields: `fast_model` + `fast_model_provider`, `background_model` + `background_model_provider`, `summarization_model` + `summarization_model_provider`. Any builder must enumerate the *actual* fields. The runner sets them all `None` (matching current spawn behavior). |
| C3 | Explorer 3: move `loader.rs` AND `builtin.rs` to engine. Explorer 6 risk note: loader could stay in server. | **Move both to `crates/bamboo-engine/src/profiles/`.** `loader.rs` only uses `std::fs`, `serde`, `thiserror`, `bamboo_domain` — all available in engine. Keeping it in server would split the profile system across crates. Server keeps a thin re-export shim for back-compat. |
| C4 | Explorer 2: move `PolicyAwareToolExecutor` to `bamboo-tools`. Explorer 6 risk: needs injectable session cache. | **Move to `bamboo-tools`.** Verified: it depends only on `bamboo_agent_core::tools`, `bamboo_agent_core::Session`, `bamboo_domain::subagent`, `tokio::sync::RwLock` — all available in `bamboo-tools` (which already depends on `bamboo-domain`). The `Arc<RwLock<HashMap<String, Session>>>` is a ctor parameter, already injectable. No risk. |
| C5 | Explorer 3 test-ref fix: change `crate::session_app::...` → `bamboo_engine::session_app::...`. | **Wrong after move.** Once `builtin.rs` lives *inside* `bamboo-engine`, the path stays `crate::session_app::child_session::CHILD_SYSTEM_PROMPT` (crate-relative; engine IS the owner). Do NOT change to `bamboo_engine::` (self-reference). Verified constants live at `crates/bamboo-engine/src/session_app/child_session/{helpers.rs,mod.rs}`. |
| C6 | Explorer 2: extract pure `infer_provider(model_name) -> Option<String>` and unified `resolve_model`. Explorer 6 notes model precedence is `Config.subagent_models[id] > model_hint.model_ref > model_hint.tier`. | **Add `infer_provider` + `resolve_model` as pure helpers in `bamboo-engine/src/model_config_helper.rs`** (engine already owns `resolve_subagent_model_ref`). Do NOT move into `bamboo-tools` (would need `Config`/`ProviderRegistry`, pulling infrastructure model-routing into tools unnecessarily). Keep existing `resolve_subagent_model*` as-is; the new helpers are additive. |
| C7 | Explorer 4/6: add `Agent::researcher()`/`.coder()` to root, requiring profile registry. | **Root facade re-exports `bamboo_engine::profiles`** (post-relocation). `.researcher()`/`.coder()`/`.from_profile()` resolve from `builtin_profiles()` and set the builder's system-prompt/tool-policy. No duplication of profile defs in root. |

### Non-goals (explicitly deferred)

- Removing the double tool-policy enforcement (schema `disabled_tools` + `PolicyAwareToolExecutor` runtime net). Keep both; document authority. (tech-debt item TD-7.)
- Converting `ChildStatus` string literals to an enum at the wire level. Internal enum is fine; wire strings unchanged. (tech-debt TD-5.)
- A2A/external runner changes.

---

## 1. Target architecture (end state)

```
bamboo-domain
  subagent/{model.rs, registry.rs}        # SubagentProfile, ToolPolicy, disabled_tools_for_profile  (UNCHANGED)

bamboo-tools
  policy_aware.rs                          # PolicyAwareToolExecutor  (MOVED here from server)

bamboo-engine
  model_config_helper.rs                   # + infer_provider(), + resolve_model()  (ADDITIVE)
  profiles/{mod.rs, builtin.rs, loader.rs} # MOVED here from server/subagent_profiles
  sdk/{mod.rs, runner.rs, spawn.rs}        # NEW: ProfileRunner + run_child_spawn core
  runtime/execution/spawn.rs               # run_spawn_job becomes thin caller of sdk::spawn::run_child_spawn

bamboo-server
  tools/policy_aware.rs                     # DELETED → re-export shim (pub use bamboo_tools::PolicyAwareToolExecutor)
  subagent_profiles/mod.rs                  # → re-export shim (pub use bamboo_engine::profiles::*)
  tools/child_session_adapter.rs           # enqueue_child_run unchanged behavior; still builds SpawnJob
                                            # (scheduler still drives run_spawn_job → now sdk core)

root crate (bamboo_agent)
  src/agent/{mod.rs, builder.rs, tools.rs, execute_request.rs}  # ergonomic facade
  src/lib.rs                                # cleaned re-exports + pub use agent::*
```

**Anti-fork guarantee:** `run_spawn_job` and `ProfileRunner::run` both funnel into
`sdk::spawn::run_child_spawn(ctx: SpawnContext, job: SpawnJob)`. There is exactly
one spawn/execute/finalize implementation.

---

## 2. Dependency-ordered phases

Each phase ends with a **GATE** (`cargo build` + `cargo test`). Phases are
sequential at the gate boundary. Within a phase, steps marked **[PAR]** touch
disjoint files and may proceed in parallel; **[SEQ]** steps are compile-dependent.

> All cargo commands run from repo root `/Users/bigduu/Workspace/TauriProjects/zenith/bamboo`.
> Use `cargo build --workspace` and `cargo test --workspace` (or per-crate `-p` for fast inner loops).

---

### PHASE 0 — Pre-flight tech-debt (no behavior change, lowest risk)

Pure comment/doc fixes; safe to do first and in parallel.

- **[PAR] S0.1** Fix stale `bamboo-application-agent` references:
  - `src/agent/mod.rs:4` → "via bamboo-engine"
  - `crates/bamboo-domain/src/session/hook_types.rs:5`
  - `crates/bamboo-domain/src/session/composition/condition.rs:4`
  - `crates/bamboo-domain/src/session/composition/mod.rs:4`
- **[PAR] S0.2** `src/lib.rs:48` remove "Placeholder modules (will be populated during migration)" comment (module is being populated this PR).

**GATE 0:** `cargo build --workspace` (comments only; must still compile).

---

### PHASE 1 — Bridges (no engine/runner deps yet)

Two independent bridge migrations. **[PAR]** across S1.A and S1.B (disjoint files,
disjoint crates).

#### S1.A — Move `PolicyAwareToolExecutor` → `bamboo-tools`  (resolves C4)

- **[SEQ] S1.A.1** Create `crates/bamboo-tools/src/policy_aware.rs`: move the full
  module body from `crates/bamboo-server/src/tools/policy_aware.rs` (incl. tests).
  Imports already valid in tools (`bamboo_agent_core::tools::*`, `bamboo_agent_core::Session`,
  `bamboo_domain::subagent::{SubagentProfileRegistry, ToolPolicy}`, `tokio::sync::RwLock`).
- **[SEQ] S1.A.2** `crates/bamboo-tools/src/lib.rs`: add `pub mod policy_aware;` and
  `pub use policy_aware::PolicyAwareToolExecutor;`.
- **[SEQ] S1.A.3** Replace `crates/bamboo-server/src/tools/policy_aware.rs` content
  with a re-export shim: `pub use bamboo_tools::PolicyAwareToolExecutor;` (keeps
  `crate::tools::PolicyAwareToolExecutor` path alive at
  `crates/bamboo-server/src/tools/mod.rs:29` and builder.rs:240). *Alternative:* delete
  file + change `tools/mod.rs` re-export to point at `bamboo_tools`. Shim is lower-risk.

#### S1.B — Add pure model helpers in engine  (resolves C6)

- **[SEQ] S1.B.1** `crates/bamboo-engine/src/model_config_helper.rs`: add
  `pub fn infer_provider(model_name: &str) -> Option<String>` (pattern:
  `claude*`→`anthropic`, `gpt*`/`o[0-9]*`→`openai`, `gemini*`→`gemini`, else `None`).
  Extract the implicit pattern-matches currently scattered in resolve functions to
  call this helper (refactor, behavior-preserving).
- **[SEQ] S1.B.2** Add `pub fn resolve_model(model_hint: &ModelHint, provider_name: &str,
  config: &Config, provider_registry: &Arc<ProviderRegistry>) -> Option<ResolvedModel>`
  honoring precedence **`model_hint.model_ref` > `model_hint.tier` > fallback chain**
  (`subagent_models[type]` → `sub_agent` → `fast` → `chat`). Reuse existing
  `resolve_subagent_model_ref`. Keep existing `resolve_subagent_model*` untouched.

**GATE 1:** `cargo build --workspace` then
`cargo test -p bamboo-tools -p bamboo-engine -p bamboo-server`.
Existing 9 `policy_aware` tests must pass *in their new home*; engine model tests unchanged.

---

### PHASE 2 — Engine SDK runner (depends on Phase 1 helpers)

Refactor the canonical spawn path into a reusable core, then add the ergonomic
runner facade. **Mostly [SEQ]** (all touch `bamboo-engine/src/sdk` + `spawn.rs`).

- **[SEQ] S2.1** Create `crates/bamboo-engine/src/sdk/mod.rs` (`pub mod runner; pub mod spawn;`).
- **[SEQ] S2.2** Create `crates/bamboo-engine/src/sdk/spawn.rs`. Move the **body** of
  `run_spawn_job` (currently `runtime/execution/spawn.rs:320-651`) into
  `pub async fn run_child_spawn(ctx: SpawnContext, job: SpawnJob) -> Result<(), String>`.
  Preserve EXACTLY:
  - SubAgentStarted is emitted by the *adapter* (not here) — unchanged.
  - Event forwarder + 5s heartbeat tasks, watchdog, runner reservation.
  - `ExecuteRequest` construction with ALL real fields incl. split provider fields
    (`fast_model_provider`, `background_model_provider`, `summarization_model_provider`)
    — see C2. `disabled_tools = job.disabled_tools.map(|v| v.into_iter().collect())`.
  - `publish_child_completion_parts` terminal path with status strings
    `completed|cancelled|error|skipped|timeout`.
- **[SEQ] S2.3** `runtime/execution/spawn.rs`: `run_spawn_job` becomes a 1-line
  delegator: `crate::sdk::spawn::run_child_spawn(ctx, job).await`. (Keeps the
  `SpawnScheduler` queue mechanics in place.) **Anti-fork checkpoint.**
- **[SEQ] S2.4** Create `crates/bamboo-engine/src/sdk/runner.rs`:
  - `pub struct ProfileRunner { ctx: SpawnContext }` — **reuse `SpawnContext`**, not a
    new `RuntimeDeps` (resolves C1).
  - `pub fn profile_runner(ctx: SpawnContext) -> ProfileRunner`.
  - `pub struct RunProfileInput { child_session_id, parent_session_id, model, /* derived */ }`
    — minimal; the assignment prompt + system prompt already live in the persisted
    child session (matching real spawn semantics: `initial_message` is empty, the
    last user message in the child drives execution).
  - `impl ProfileRunner { pub async fn run_profile(&self, profile: &SubagentProfile, input: RunProfileInput) -> Result<(), String> }`:
    computes `disabled_tools` via `bamboo_domain::subagent::disabled_tools_for_profile(&profile.tools, &tool_names)`,
    builds a `SpawnJob`, calls `run_child_spawn(self.ctx.clone(), job)`.
  - Streaming variant `run_profile_stream` returns a `broadcast::Receiver<AgentEvent>`
    obtained from `ctx.session_event_senders` for the child id (reuse existing
    broadcast infra; do NOT invent `RunOutcomeStream`/`status_rx` mpsc — resolves C1).
- **[SEQ] S2.5** `crates/bamboo-engine/src/lib.rs`: add `pub mod sdk;` and
  `pub use sdk::runner::{ProfileRunner, profile_runner, RunProfileInput};`
  `pub use sdk::spawn::run_child_spawn;`.

**GATE 2:** `cargo build --workspace` then `cargo test -p bamboo-engine -p bamboo-server`.
The 29 `sub_agent.rs` tests (esp. `create_emits_sub_agent_started_event_after_queueing`
at line ~797) MUST stay green — they exercise the scheduler→`run_spawn_job`→`run_child_spawn`
path unchanged. Add new engine tests S-T2.* (see §4).

---

### PHASE 3 — Profiles relocation (depends on engine session_app; independent of sdk)

> Could overlap Phase 2 in calendar time (different files), but gate it *after*
> Phase 2 to keep one clean engine build gate. Steps within are **[SEQ]**.

- **[SEQ] S3.1** Create `crates/bamboo-engine/src/profiles/builtin.rs`: copy from
  `crates/bamboo-server/src/subagent_profiles/builtin.rs` verbatim. **Keep** the
  test refs `crate::session_app::child_session::{CHILD_SYSTEM_PROMPT, PLAN_AGENT_SYSTEM_PROMPT}`
  unchanged — they resolve correctly because engine owns `session_app` (resolves C5).
  Update the stale module doc-comment about "(future) FilteredExecutor" → reference
  `bamboo_tools::PolicyAwareToolExecutor`.
- **[SEQ] S3.2** Create `crates/bamboo-engine/src/profiles/loader.rs`: move from
  `crates/bamboo-server/src/subagent_profiles/loader.rs`. Imports unchanged
  (`bamboo_domain::subagent::*`, `std::fs`, `thiserror`). Update doc comment about
  "consumer typically bamboo-server".
- **[SEQ] S3.3** Create `crates/bamboo-engine/src/profiles/mod.rs`:
  `pub mod builtin; pub mod loader; pub use builtin::builtin_profiles; pub use loader::{load_registry, LoaderError};`.
- **[SEQ] S3.4** `crates/bamboo-engine/src/lib.rs`: add `pub mod profiles;` +
  `pub use profiles::{builtin_profiles, load_registry, LoaderError};`.
- **[SEQ] S3.5** Convert `crates/bamboo-server/src/subagent_profiles/mod.rs` to a
  shim: `pub use bamboo_engine::profiles::{builtin_profiles, load_registry, LoaderError};
  pub mod builtin { pub use bamboo_engine::profiles::builtin::*; }` (preserves
  `crate::subagent_profiles::builtin::builtin_profiles` at sub_agent.rs:710 and
  `crate::subagent_profiles::load_registry` at builder.rs:228). Delete now-duplicate
  `builtin.rs`/`loader.rs` from server.

**GATE 3:** `cargo build --workspace` then
`cargo test -p bamboo-engine -p bamboo-server`.
Moved profile tests (6 builtin + 7 loader) run in engine and pass; server route test
`GET /v1/subagent_profiles` (routes/tests.rs) still green.

---

### PHASE 4 — Root facade (`src/agent/`) (depends on engine profiles + sdk + tools)

All new/edited files under `src/agent/`. **[PAR]** across the three new files
(S4.1 tools.rs, S4.2 execute_request.rs, S4.3 builder.rs are disjoint), then **[SEQ]**
S4.4 mod.rs wires them, S4.5 lib.rs.

- **[PAR] S4.1** `src/agent/tools.rs`: `pub struct ToolSpec { name, description, disabled }`
  + consts mapped to **real** names from `bamboo_domain::tool_names::BUILTIN_TOOL_NAMES`
  (verify against that const array; do not hand-list). Re-export the canonical list.
- **[PAR] S4.2** `src/agent/execute_request.rs`: `ExecuteRequestBuilder` forwarding to
  `bamboo_engine::ExecuteRequest` with ALL real fields (3 required + the rest, incl.
  split provider fields per C2). Defaults match current spawn defaults (`None`).
- **[PAR] S4.3** `src/agent/builder.rs`: wrap `bamboo_engine::AgentBuilder`. Ergonomic
  methods: `.from_profile(&SubagentProfile)` (sets system_prompt + tool policy),
  `.researcher()`/`.coder()`/etc. (resolve via `bamboo_engine::profiles::builtin_profiles()`,
  resolves C7), `.model()`, `.instruction()`, `.tools()`, `.api_key()`,
  `.with_defaults_for_data_dir(PathBuf)` (assembles the 8 deps:
  `Config::from_data_dir`/`Config::new`, `JsonlStorage::new`+`init`,
  `SkillManager::new`+`initialize`, `MetricsCollector::spawn` with
  `SqliteMetricsStorage` (verified to exist: `bamboo_engine::SqliteMetricsStorage`),
  `create_provider`, `BuiltinToolExecutor::new_with_config`).
- **[SEQ] S4.4** `src/agent/mod.rs`: replace passthrough. Define
  `pub struct Agent { inner: Arc<AgentRuntime> }` with `from_runtime`/`builder`/`run`/
  `run_stream`/`storage`/`persistence`; `mod builder; mod tools; mod execute_request;`
  `pub use {builder::AgentBuilder, tools::*, execute_request::ExecuteRequestBuilder};`
  `pub use bamboo_engine::profiles;` for consumers. Keep the existing convenience
  type re-exports (Session, Message, etc.).
- **[SEQ] S4.5** `src/lib.rs:63`: `pub use agent::{Agent, AgentBuilder};` (now from the
  new wrappers). Add `pub use agent::profiles;` if desired.

**GATE 4:** `cargo build --workspace` then `cargo test --workspace`.
Add new root SDK tests S-T4.* (see §4).

---

### PHASE 5 — Server rewire onto the runner (anti-fork enforcement)

The server already routes through `run_spawn_job` → (now) `run_child_spawn`, so the
core is unified after Phase 2. This phase **optionally** lets `ChildSessionAdapter`
call `ProfileRunner` directly instead of `scheduler.enqueue`, *only if it preserves
the async-enqueue semantics and SubAgentStarted ordering*. **Conservative default:
leave the scheduler path as-is** (it already calls the unified core) and just verify
no second implementation exists.

- **[SEQ] S5.1** Audit: `grep` for any remaining inline spawn/execute/finalize logic
  outside `sdk::spawn::run_child_spawn`. There must be none.
- **[SEQ] S5.2** (optional) Refactor `enqueue_child_run` to construct `SpawnJob` via the
  same `disabled_tools_for_profile` call already present (line 318-339) — no change
  needed; document that adapter remains the SpawnJob factory + parent-wait registrar.
- **[SEQ] S5.3** Confirm `PolicyAwareToolExecutor` still wraps child tools at
  `builder.rs:240` via the new `bamboo_tools` path (shim makes this transparent).

**GATE 5:** `cargo test --workspace`. **CRITICAL invariant checks:**
- SubAgentStarted emitted *after* parent-wait persisted (sub_agent.rs:797).
- Allowlist profile child: tool not in schema (disabled_tools) AND blocked at exec
  (policy_aware). New integration test S-T5.2.

---

### PHASE 6 — Docs

- **[PAR] S6.1** This document (already at `docs/design/ergonomic-sdk-plan.md`).
- **[PAR] S6.2** Add module-level docs to `src/agent/mod.rs` and `bamboo-engine/src/sdk/mod.rs`
  describing the public SDK surface and the anti-fork invariant.
- **[PAR] S6.3** Update `bamboo-engine/src/profiles/{mod,loader}.rs` and
  `bamboo-tools/src/policy_aware.rs` doc comments to reflect new homes.

**GATE 6:** `cargo doc --workspace --no-deps` (no broken intra-doc links).

---

## 3. Parallelization summary

| Can run in parallel | Must be sequential |
|---------------------|--------------------|
| S1.A vs S1.B (different crates) | Everything *within* Phase 2 (shared sdk/spawn files) |
| S0.1 vs S0.2 | S2.2 → S2.3 (extract before delegate) |
| S4.1 vs S4.2 vs S4.3 (disjoint new files) | S2.x → S3.x → S4.x → S5.x at every GATE |
| S6.1 vs S6.2 vs S6.3 | S4.4 after S4.1/4.2/4.3 (mod wires them) |

Phases are strictly ordered by the GATEs. Two developers could own S1.A and S1.B
simultaneously; the engine-runner author (Phase 2) blocks the facade author (Phase 4).

---

## 4. Test plan

### Must-stay-green (regression)

- `bamboo-domain`: all `subagent/model.rs` policy tests + `registry.rs` (8) — untouched.
- `bamboo-tools` (post-move): the 9 `PolicyAwareToolExecutor` tests
  (`inherit_policy_forwards_all_calls`, `allowlist_permits/blocks`,
  `denylist_blocks/permits`, `missing_session_id_falls_through`,
  `unknown_session_falls_through`, `missing_subagent_type_metadata_falls_through`,
  `execute_without_context_forwards`).
- `bamboo-engine`: `model_areas.rs` (9), `model_config_helper.rs`, child_session
  `tests.rs` (10), runtime `tests.rs` (5).
- `bamboo-engine` (post-move): 6 builtin-profile + 7 loader tests (incl. prompt-drift
  cross-check against `CHILD_SYSTEM_PROMPT`/`PLAN_AGENT_SYSTEM_PROMPT`).
- `bamboo-server`: 29 `sub_agent.rs` tests, `routes/tests.rs` profile-list smoke,
  `policy_aware` shim re-export compiles.

### New tests

- **S-T1.1** `infer_provider`: claude/gpt/o-series/gemini/unknown mapping.
- **S-T1.2** `resolve_model`: precedence `model_ref` > `tier` > fallback chain.
- **S-T2.1** `run_child_spawn` integration: parent+child sessions, assert
  SubAgentStarted (adapter) → SubAgentEvent → SubAgentCompleted ordering; child
  status persisted (completed).
- **S-T2.2** `ProfileRunner::run_profile` with Allowlist profile: assert
  `disabled_tools` excludes non-allowlisted names (schema-level).
- **S-T2.3** `run_profile` with `ToolPolicy::Inherit`: `disabled_tools` empty.
- **S-T2.4** Model precedence at runner: `model_override` honored over session model.
- **S-T2.5** Watchdog timeout → SubAgentCompleted status=`timeout` (reuse existing
  watchdog plumbing).
- **S-T4.1** `Agent::builder().researcher().model("m").build()` → resolved
  system_prompt matches researcher profile + model override applied.
- **S-T4.2** `ExecuteRequestBuilder` round-trip: all required fields enforced, all
  optional default to `None`.
- **S-T4.3** `with_defaults_for_data_dir(tmp)`: builds an `Agent` with a NoopProvider/
  mock; `SkillManager.initialize` + `MetricsCollector.spawn` succeed.
- **S-T5.2** End-to-end policy: child `subagent_type=researcher` (read-only allowlist)
  → Edit/Write blocked at execute *and* absent from schema.

> Use a Noop/mock `LLMProvider` for SDK integration tests (existing pattern in
> model-resolution tests) to avoid network I/O.

---

## 5. Tech-debt cleanup (refactor-adjacent, do within the relevant phase)

- **TD-1 (Phase 0):** Remove 4 stale `bamboo-application-agent` comments + the
  `src/lib.rs:48` placeholder comment.
- **TD-2 (Phase 0/4):** Collapse the duplicate Agent re-export chain
  (`src/lib.rs:63` → `agent/mod.rs:24` → `bamboo_engine`) into the new wrapper.
- **TD-3 (Phase 1):** Extract scattered provider-pattern matches in
  `model_config_helper.rs` into the single `infer_provider`.
- **TD-4 (Phase 3):** Update builtin.rs "(future) FilteredExecutor" comment →
  `PolicyAwareToolExecutor`; update loader.rs "consumer typically bamboo-server".
- **TD-5 (deferred, documented):** Internal `ChildStatus` enum vs wire strings —
  keep strings on the wire; note for future.
- **TD-6 (Phase 4):** Add `ExecuteRequestBuilder` so consumers aren't exposed to the
  raw multi-field `ExecuteRequest`.
- **TD-7 (Phase 5, documented):** Double tool-policy enforcement (schema
  `disabled_tools` is *authoritative for discovery*; `PolicyAwareToolExecutor` is the
  *execution-time safety net*). Document; do not remove.
- **TD-8 (Phase 2):** `disabled_tools_for_profile` needs `all_tool_names` from caller;
  document that `SpawnContext.tools.list_tools()` is the canonical source so callers
  stop threading a separate `tool_names: Vec<String>`.

---

## 6. Reverse-dependency risk register

| Risk | Mitigation |
|------|-----------|
| Moving `PolicyAwareToolExecutor` to `bamboo-tools` would fail if it pulled any `bamboo-server`/`bamboo-engine` symbol. | Verified: only `agent-core` + `domain` + tokio. Safe. No reverse edge. |
| Moving `profiles` to engine: server still needs them → server already depends on engine. No new edge; *removes* server-owned logic. | Re-export shim keeps server paths; no circular import (domain owns the types). |
| Root facade `.with_defaults_for_data_dir` pulling `bamboo-server` into the agent builder. | Build deps from `infrastructure`/`engine`/`tools` only. `bamboo-server` stays out of the `Agent` builder path. |
| `sdk::runner` reusing `SpawnContext` could tempt importing server `AppState`. | `SpawnContext` lives in engine and is server-agnostic (completion_handler is a trait object). No `AppState` reference. |
| `infer_provider`/`resolve_model` in engine needing `ProviderRegistry` (infrastructure) — fine (engine→infra exists), but must NOT land in `bamboo-tools`. | Helpers stay in engine per C6. |

---

## 7. Anti-fork verification checklist (run at GATE 5)

1. `grep -rn "ExecuteRequest {" crates/bamboo-engine/src` → only `sdk/spawn.rs`
   (and any pre-existing root-session execute paths; sub-agent path single).
2. `run_spawn_job` body is a single delegation to `run_child_spawn`.
3. `ProfileRunner::run_profile` constructs `SpawnJob` + calls `run_child_spawn`; no
   inline execute/finalize.
4. No duplicate `builtin_profiles()` / `load_registry()` outside `bamboo-engine`
   (server shim only re-exports).
5. No duplicate `PolicyAwareToolExecutor` impl outside `bamboo-tools`.
