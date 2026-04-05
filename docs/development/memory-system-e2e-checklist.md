# Bamboo Memory System E2E Checklist

- **Date**: 2026-04-05
- **Author**: Bodhi
- **Status**: Draft
- **Scope**: End-to-end validation checklist for Bamboo memory system v1, including session memory, durable memory, Dream Notebook, Auto Dream, backend tool exposure, and Lotus UI wiring

## Executive Summary

This checklist is intended to validate the current Bamboo memory system as a complete product slice rather than as isolated unit tests.

It covers five layers together:

1. **Configuration layer**
   - `Config.memory.background_model`
   - `Config.memory.auto_dream_enabled`
2. **Tool/runtime layer**
   - `session_note`
   - `memory` tool (`session_*`, durable actions)
3. **Persistence layer**
   - session memory topics
   - durable memory topic files / indexes / views
   - session state markers such as `last_extracted_at`
4. **Background automation layer**
   - Dream Notebook generation
   - durable candidate extraction via Auto Dream
5. **Frontend/UI layer**
   - Lotus settings for Auto Dream
   - System Prompt card wording and section display

The goal is to answer three practical questions:

- **Is the memory system exposed correctly?**
- **Is it enabled/configurable correctly?**
- **Can it run through the full user-visible workflow correctly?**

---

## Preconditions

Before running the checklist, ensure:

- Bamboo backend builds and tests cleanly in the current branch
- Lotus frontend type-check passes in the current branch
- You know the active Bamboo data dir (`${BAMBOO_DATA_DIR}` or `~/.bamboo`)
- You have a provider configured with a valid main model
- For Auto Dream validation, you also have a valid fast/background model path available:
  - either `memory.background_model`
  - or the provider's `fast_model`

Recommended clean-room setup for manual validation:

```bash
export BAMBOO_DATA_DIR=/tmp/bamboo-memory-e2e
rm -rf "$BAMBOO_DATA_DIR"
mkdir -p "$BAMBOO_DATA_DIR"
```

If you also want an isolated workspace/project identity:

```bash
mkdir -p /tmp/bamboo-memory-workspace/demo-project
cd /tmp/bamboo-memory-workspace/demo-project
git init
```

---

## Automated Validation Commands

### Backend

Run these first:

```bash
cd bamboo
cargo check --tests
cargo test --lib
cargo test --tests
```

Minimum targeted suites for memory system changes:

```bash
cd bamboo
cargo test session_note --lib
cargo test session_memory --lib
cargo test memory_session_ --lib
cargo test inject_external_memory_includes_dream_notebook_and_session_note --lib
cargo test auto_dream --lib
cargo test parse_extraction_candidates_accepts_fenced_json --lib
cargo test extract_and_persist_durable_candidates_writes_memory_and_marks_session --lib
```

### Frontend

Run these after backend passes:

```bash
cd lotus
npm run test:run -- useSystemPromptContent SystemMessageCard SystemSettingsConfigTab.autoDream
npm run type-check
```

Optional broader frontend regression:

```bash
cd lotus
npm run test:run
```

---

## Layer 1 — Configuration Validation

### Goal
Verify that memory-related configuration exists, is readable, and is persisted correctly.

### What to check

- `memory.background_model` exists in config schema
- `memory.auto_dream_enabled` exists in config schema
- `auto_dream_enabled` defaults to `false`
- `background_model` defaults to `None` / unset
- Lotus can save both values via `/bamboo/config`

### Manual steps

1. Start Bamboo against a clean data dir.
2. Open Lotus settings.
3. Navigate to:
   - **Settings → Config → General → Memory & Auto Dream**
4. Toggle **Enable Auto Dream** on.
5. Enter a **Background Memory Model** value.
6. Save.
7. Inspect `${BAMBOO_DATA_DIR}/config.json`.

### Expected results

- `config.json` contains:

```json
{
  "memory": {
    "auto_dream_enabled": true,
    "background_model": "<your-model>"
  }
}
```

- If the background model field is cleared and saved, `auto_dream_enabled` remains persisted while `background_model` may be omitted or stored empty depending on patch path normalization.
- No unrelated provider settings are clobbered by saving Memory & Auto Dream settings.

---

## Layer 2 — Tool Exposure Validation

### Goal
Verify that `session_note` and `memory` are present in the runtime tool surface and that `memory` is usable.

### What to check

- `session_note` exists
- `memory` exists
- `memory` is registered in root tools
- `memory` is available in child tools
- `memory` is discoverable (not core-first-round by default)
- `/bamboo/tools` lists `memory`

### Manual/API steps

1. Call the backend tool list endpoint or inspect via UI/dev tools:

```bash
curl http://127.0.0.1:9562/v1/bamboo/tools
```

2. Verify the returned tool list includes:
   - `session_note`
   - `memory`

3. In an interactive session, use the tool directly:

```json
{"action":"session_read","topic":"default"}
```

### Expected results

- `memory` appears in the tool inventory.
- `session_note` appears in the tool inventory.
- `memory` calls require `session_id` context and return a meaningful error if invoked without session context.
- The tool is not globally disabled unless explicitly present in `config.tools.disabled`.

### Negative case

Add `memory` to `config.tools.disabled`, reload, and verify that runtime tool schemas no longer offer it to the model.

---

## Layer 3 — Session Memory Validation

### Goal
Verify current-session continuity memory works end-to-end via both entrypoints.

### What to check

- `session_note` and `memory session_*` share behavior
- 12k limit enforcement works
- read truncation metadata is present
- topic listing/count behaves consistently

### Manual steps

Use `session_note` first:

```json
{"action":"append","topic":"default","content":"User prefers terse responses."}
```

Then read it back:

```json
{"action":"read","topic":"default"}
```

Then do the same through `memory`:

```json
{"action":"session_read","topic":"default","options":{"max_chars":4000}}
```

Add a second topic:

```json
{"action":"session_append","topic":"release","content":"Release freeze begins on Tuesday."}
```

List topics:

```json
{"action":"session_list_topics"}
```

### Expected results

- `session_note read` and `memory session_read` both return:
  - `content`
  - `length_chars`
  - `body_truncated`
  - `max_chars`
- `session_list_topics` returns `topics` and `count`
- both entrypoints reference the same on-disk session memory
- `append` respects the shared 12k character limit

### Negative cases

- Try appending when the note would exceed 12k
- expected: actionable compression guidance is returned
- try `read` with very small `max_chars`
- expected: `body_truncated = true`

---

## Layer 4 — Durable Memory Validation

### Goal
Verify that the unified `memory` tool can create, query, inspect, and rebuild durable memory artifacts.

### What to check

- `write` persists canonical topic files
- `query` returns bounded shortlist-style results
- `get` returns a full durable item
- `inspect` and `rebuild` work on durable scopes
- project-scoped memory resolves via workspace/project key

### Manual steps

#### A. Project-scoped durable write
Set workspace first if needed, then call:

```json
{
  "action":"write",
  "scope":"project",
  "type":"project",
  "title":"Release freeze begins next week",
  "content":"Merge freeze begins on Tuesday for the mobile release cut.",
  "tags":["release","freeze"]
}
```

#### B. Query shortlist

```json
{
  "action":"query",
  "scope":"project",
  "query":"release freeze mobile",
  "options":{"limit":5,"max_chars":3000}
}
```

#### C. Read one durable item
Take the returned `id` and call:

```json
{
  "action":"get",
  "id":"<memory-id>",
  "options":{"max_chars":5000}
}
```

#### D. Inspect durable scope

```json
{
  "action":"inspect",
  "scope":"project"
}
```

#### E. Rebuild derived artifacts

```json
{
  "action":"rebuild",
  "scope":"project"
}
```

### Expected results

- `write` creates a canonical durable memory file under the memory store
- `query` returns bounded summaries rather than the entire body dump
- `get` returns full frontmatter/body for one durable memory item
- `inspect` shows counts, views, index files, and state files
- `rebuild` refreshes derived artifacts successfully

### On-disk checks

Inspect `${BAMBOO_DATA_DIR}` and verify:

- durable topic files exist
- derived views/indexes exist
- Dream view and durable memory files are separate concerns

---

## Layer 5 — Auto Dream / Dream Notebook Validation

### Goal
Verify that background memory maintenance works end-to-end:

- Dream Notebook updates
- durable candidate extraction happens
- `last_extracted_at` is updated for touched sessions

### Required setup

- `memory.auto_dream_enabled = true`
- provider fast model configured or `memory.background_model` set

### Manual steps

1. Create one or more real sessions containing durable signal, for example:
   - stable user preference
   - confirmed project decision
   - durable reference fact
2. Add session memory content, e.g.:

```json
{"action":"append","topic":"default","content":"User prefers terse responses and no recap."}
```

3. Allow Auto Dream to run or trigger a code-path that causes the periodic background task to process.
4. Inspect the Dream Notebook through the system prompt snapshot UI or directly on disk.
5. Query durable memory for the extracted fact.
6. Inspect the session state file for `last_extracted_at`.

### Expected results

- Dream Notebook is written/updated
- it includes recent durable context summary
- one or more durable memory candidates are persisted when the model output supports it
- the corresponding session state has `last_extracted_at` populated

### Optional direct verification

If debugging locally, inspect:

- Dream view file under the global memory views directory
- session memory state JSON for the session used in extraction

---

## Layer 6 — Lotus UI Validation

### Goal
Verify the frontend exposes the right controls and wording.

### Settings validation

Navigate to:

- **Settings → Config → General → Memory & Auto Dream**

Verify the UI contains:

- section title: **Memory & Auto Dream**
- description referencing:
  - session memory
  - long-term memory
  - Dream Notebook
  - Auto Dream
- toggle: **Enable Auto Dream**
- input: **Background Memory Model**

### System Prompt card validation

In a session with prompt snapshot data available:

1. Open the System Prompt card
2. View the snapshot sections
3. Verify labels include:
   - **Dream Notebook**
   - **Session Memory** / 中文：**会话记忆**
   - **Memory Layers** / 中文：**记忆层**

### Expected results

- wording is product-layer oriented, not raw internal implementation language
- English and Simplified Chinese both show the new labels
- no broken fallback keys appear in the UI

---

## Release/QA Smoke Checklist

Use this condensed version before merging/releasing:

### Backend
- [ ] `cargo check --tests`
- [ ] `cargo test --lib`
- [ ] `cargo test --tests`
- [ ] `memory` tool visible in `/bamboo/tools`
- [ ] `session_note` and `memory session_*` both operate on the same session note data
- [ ] durable `write/query/get/inspect/rebuild` all work
- [ ] Auto Dream updates Dream Notebook when enabled and background model is available

### Frontend
- [ ] `npm run type-check`
- [ ] relevant vitest suites pass
- [ ] Memory & Auto Dream settings card renders correctly
- [ ] save persists `memory.auto_dream_enabled`
- [ ] save persists `memory.background_model`
- [ ] System Prompt card shows updated memory terminology

### Integration
- [ ] create session memory note
- [ ] confirm note visible in prompt snapshot
- [ ] write durable memory item
- [ ] query and get the durable memory item
- [ ] enable Auto Dream and confirm Dream Notebook updates
- [ ] confirm `last_extracted_at` changes after background extraction

---

## Known Non-Goals for This Checklist

This checklist does **not** attempt to validate:

- deep recall/re-ranking quality under large memory corpora
- contradiction resolution workflows beyond basic durable persistence
- stale candidate lifecycle tuning
- performance/load testing of very large memory stores
- multi-provider evaluation quality comparisons for background extraction

Those should be handled by separate performance/eval documents.

---

## Recommended Follow-Up Documents

After this checklist, useful next documents would be:

- `memory-eval-quality-plan.md` — extraction/retrieval quality evaluation
- `memory-ops-runbook.md` — operator/debugging runbook
- `memory-release-checklist.md` — shortened release-grade smoke checklist
