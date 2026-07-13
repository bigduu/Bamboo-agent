# Personal Assistant Capability: Prospective Memory & the Record Ledger

**Status:** Fully implemented (phases 1–7)
**Date:** 2026-07-13
**Scope:** How Bamboo becomes a personal-assistant agent (todos, task decomposition,
scheduling, reminders) as **one generic capability**, not a pile of feature-specific tools.

---

## 0. The question this answers

> "Bamboo needs personal-assistant abilities — todo lists, task decomposition,
> scheduling. Is this, abstracted, just an application of long-term + short-term
> memory? Should we carve out a dedicated disk area for these events? Design a
> *generic* capability, not just the named features."

**Short answer:** it is *almost* an application of the memory system, but not quite.
Bamboo's existing memory is **retrospective** — it records what already happened
(facts, preferences, decisions) as free-text markdown, consolidated by auto-dream and
curated by the gardener. Personal-assistant work is **prospective memory**: structured
records about the *future* that have a **lifecycle** (open → done/cancelled/expired),
**time semantics** (due, scheduled, remind-at, recurrence), and **relations**
(decomposition, dependency). Free-text durable memory cannot answer "what is due
tomorrow?" or drive a reminder; a todo is not a fact, it is a commitment with state.

So the generic capability is a third store — the **Record Ledger** — that sits beside
durable memory, reuses its storage idioms and disk conventions, and is wired into the
four machines Bamboo already has: prompt injection, the schedule engine, the
notification pipeline, and the background consolidation loops. Todos, events,
reminders, habits, and task decomposition all become *record kinds* over one model.

And yes: it gets its own versioned disk area, `~/.bamboo/ledger/v1/`, parallel to
`~/.bamboo/memory/v1/`.

---

## 1. What exists today (audit)

The building blocks are already unusually strong. Nothing below needs to be rebuilt —
the design is mostly *wiring*.

| Subsystem | What it does | Key locations |
|---|---|---|
| **Session task list** (`Task` tool) | Rich todo model: status, `depends_on`, `parent_id`, phase, priority, completion criteria, evidence, blockers, transition history. Rendered into the prompt. | `crates/core/bamboo-domain/src/session/task.rs`; persistence via `crates/engine/bamboo-engine/src/runtime/runner/tool_execution/task/taskwrite.rs` |
| **Plan mode** | Read-only planning + approval handshake; PlanStore artifacts under `~/.bamboo/plan/<slug>/`. | `crates/engine/bamboo-tools/src/tools/{enter,exit}_plan_mode.rs`; `crates/infra/bamboo-memory/src/plan_store.rs` |
| **Memory system** | Session notes (short-term, `memory/v1/sessions/<id>/note/*.md`) + durable memory (long-term, `memory/v1/scopes/{global,projects/<key>}/topics/*.md`, YAML-frontmatter markdown, BM25 recall, audit logs) + Dream notebook (derived view). | `crates/infra/bamboo-memory/src/memory_store/` |
| **Background loops** | Auto-dream (extracts durable memories from recent sessions every 30 min) and gardener (blob-split / dedup / capacity passes, deterministic prefilter → LLM only when there is work). | `crates/engine/bamboo-engine/src/{auto_dream,gardener}.rs`, spawned in `crates/app/bamboo-server/src/app_state/builder.rs:442,455` |
| **Prompt injection** | `## External Memory (Persistent)` volatile block, layered by priority: observed state > session notes > top-3 relevant durable memories > project index > dream summaries. Cache-friendly (kept out of the cached system prefix). | `crates/engine/bamboo-engine/src/runtime/runner/prompt_context/external_memory.rs` |
| **Schedule engine** | Interval/Daily/Weekly/Monthly/Cron triggers, misfire & overlap policies, per-run records; **firing creates a real agent session** and (with `auto_execute`) runs the loop headlessly with a notification relay. Persisted to `~/.bamboo/schedules.json`. LLM-facing `scheduler` overlay tool. | `crates/app/bamboo-server/src/schedule_app/`; domain in `crates/core/bamboo-domain/src/schedule/domain.rs` |
| **Notifications** | Policy engine (categories, dedup) + sinks: desktop popup, ntfy, bark (phone push). Works for headless scheduled runs. | `crates/infra/bamboo-notification/`; `crates/app/bamboo-server/src/notify_sinks/` |
| **Overlay tools** | Server-layer stateful tools (`memory`, `scheduler`, `notify`, `load_skill`, `SubAgent`, …) composed as a chain over the builtin executor — the seam for tools that need stores/services. | `crates/app/bamboo-server/src/app_state/tools.rs` |
| **Skills** | Prompt-fragment + tool-ref bundles with progressive disclosure (`load_skill`). Built-ins seeded from `/builtin_skills`. | `crates/infra/bamboo-skills/` |

## 2. Gap analysis — why these pieces don't yet make an assistant

1. **Tasks die with the session tree.** `TaskList` is keyed to the *root session id*
   and stored on the `Session` record. Ask Bamboo to "remind me to renew my passport"
   today and the todo lives inside one conversation; tomorrow's session knows nothing.
   There is no *user-level* task store.
2. **Durable memory is the wrong shape for commitments.** It is free-text, atomic
   *facts* with freshness/staleness semantics. No status machine, no due dates, no
   time-range queries, no way for a reminder to fire from it. Stuffing todos into it
   would pollute recall and still not produce reminders.
3. **The schedule engine is task-agnostic.** It can run "every morning at 8", but a
   schedule is not linked to any record — completing a todo cannot cancel its
   reminder, and a fired reminder session has no structured handle back to the thing
   it is about. It also lacks a **one-shot** trigger ("at 2026-07-20T09:00"), which is
   the single most common reminder shape.
4. **Nothing extracts commitments.** Auto-dream extracts *facts* from conversations;
   nobody extracts "I promised to send the report Friday" into anything actionable.
5. **No agenda in the prompt.** The assistant cannot proactively say "you have two
   things due today" because nothing injects time-relevant open items into context.

## 3. Core abstraction

Three memory horizons, one of which is new:

| Horizon | Contents | Store | Exists? |
|---|---|---|---|
| **Working / short-term** | Current-conversation continuity: session notes, session `TaskList`, plan artifacts | Session store + `memory/v1/sessions/` + `plan/` | ✅ |
| **Prospective (NEW)** | Future-facing structured records: todos, events, reminders, habits — lifecycle + time + relations | **`ledger/v1/`** | ❌ this design |
| **Retrospective / long-term** | Facts, preferences, decisions; dream summaries | `memory/v1/scopes/` | ✅ |

The flows between horizons are the interesting part:

```
conversation ──(Task tool / explicit ask / extractor)──▶ LEDGER record
session TaskList ──("promote" action)────────────────▶ LEDGER record (survives session)
LEDGER record.remind_at ──(schedule bridge)──────────▶ ScheduleSpec ──fires──▶ headless session + push notification
LEDGER agenda view ──(prompt injection)──────────────▶ every session's context ("due today: …")
LEDGER completed/expired ──(ledger gardener)─────────▶ distilled into durable memory ("user renews passport every 10y", habit stats)
```

Generic means: the ledger does not know what a "todo" is beyond a `kind` tag and which
time fields it uses. New assistant behaviors (habit tracking, birthdays, medication,
follow-ups on emails) are new *kinds* + views, not new subsystems.

## 4. Domain model (`bamboo-domain`)

New module `crates/core/bamboo-domain/src/ledger/`. Deliberately reuses existing
vocabulary (`TaskPriority`, `MemoryScope`, `CreatedBy`, `ScheduleTrigger`) instead of
inventing parallel enums.

```rust
pub enum RecordKind { Todo, Event, Reminder, Habit, Custom(String) }

pub enum RecordStatus { Open, InProgress, Blocked, Done, Cancelled, Expired }

/// Time semantics. All optional — a plain note-to-self has none.
pub struct RecordTime {
    pub due_at: Option<DateTime<Utc>>,        // deadline (todos)
    pub starts_at: Option<DateTime<Utc>>,     // calendar events
    pub ends_at: Option<DateTime<Utc>>,
    pub remind_at: Vec<DateTime<Utc>>,        // explicit reminder points
    pub recurrence: Option<ScheduleTrigger>,  // REUSE the schedule trigger model
    pub timezone: Option<String>,
}

pub struct RecordRelations {
    pub parent_id: Option<String>,            // decomposition tree
    pub depends_on: Vec<String>,              // ordering
    pub related: Vec<String>,                 // memory ids, session ids, urls
}

pub struct RecordSource {
    pub session_id: Option<String>,           // provenance: which conversation
    pub created_by: CreatedBy,                // user | agent | background loop
    pub excerpt: Option<String>,              // the sentence that spawned it
}

pub struct LedgerRecord {
    pub id: String,
    pub kind: RecordKind,
    pub title: String,
    pub body: String,                          // markdown
    pub status: RecordStatus,
    pub priority: TaskPriority,                // REUSE
    pub scope: MemoryScope,                    // REUSE: Global (personal) | Project
    pub time: RecordTime,
    pub relations: RecordRelations,
    pub source: RecordSource,
    pub tags: Vec<String>,
    pub schedule_ids: Vec<String>,             // managed ScheduleSpecs (see §7)
    pub transitions: Vec<RecordTransition>,    // status history, TaskTransition-style
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

**Task decomposition is not a feature — it is `parent_id` + `depends_on`**, exactly the
shape `TaskItem` already proved out in-session. "Split this into steps" = the agent
writing child records. A tree of records with one root *is* a project plan that
survives sessions.

## 5. Storage: `~/.bamboo/ledger/v1/` (the dedicated disk area)

New module `crates/infra/bamboo-memory/src/ledger_store/` (same crate as
`memory_store` — it shares `atomic_fs`, the path-resolver idiom, audit-log style, and
the frontmatter parser; a separate crate adds a dependency edge for no benefit).

```
~/.bamboo/ledger/v1/
└── scopes/
    ├── global/                          # personal life — the default scope
    │   ├── records/<record_id>.md       # YAML frontmatter + markdown body
    │   ├── indexes/
    │   │   ├── by_time.json             # sorted (due|starts|remind) → id; agenda queries
    │   │   ├── by_status.json           # open/in-progress/blocked buckets
    │   │   └── lexical.json             # BM25, same format as memory_store
    │   ├── views/
    │   │   ├── AGENDA.md                # today + overdue + next 7 days (human-readable)
    │   │   └── TODO.md                  # open tree, grouped by root
    │   └── logs/audit.jsonl             # every mutation, append-only
    └── projects/<project_key>/          # same subtree, project-scoped work items
```

Design decisions, mirroring `memory_store` deliberately:

- **One markdown file per record.** Human-readable, greppable, diffable, syncable
  (git / Syncthing / iCloud) — this is the local-first promise. Frontmatter is the
  structured half; the body is free prose/checklists.
- **Indexes and views are derived, rebuildable caches**, refreshed on every scope
  write under a per-scope lock (`refresh_scope_artifacts` pattern). Corruption
  recovery = rebuild from `records/*.md`, exactly like `rebuild_scope`.
- **Atomic writes + audit JSONL** via the existing `atomic_fs` helpers.
- **Why not SQLite?** Volume is human-scale (thousands, not millions), time queries
  are served by a small sorted index, and file-per-record keeps the export/backup/sync
  story identical to durable memory. The `LedgerStore` trait boundary keeps a SQLite
  backend possible later without touching callers.

## 6. Tool surface: one `ledger` overlay tool

An **overlay tool** (like `memory` and `scheduler` — it needs the store and the
schedule manager), registered in `crates/app/bamboo-server/src/app_state/tools.rs`
and named in `SERVER_TOOL_NAMES`. One tool, action-dispatched, mirroring the `memory`
tool's shape so the model transfers its habits:

| Action | Purpose |
|---|---|
| `upsert` | Create/update a record (kind, title, time, priority, parent…) |
| `transition` | `done` / `cancel` / `block` / `reopen` — also reconciles linked schedules |
| `query` | By time window ("due before Friday"), status, kind, tag, scope; agenda shortcut |
| `get` | Full record + children |
| `decompose` | Create child records under a parent in one call |
| `promote` | Lift items from the current session `TaskList` into the ledger |

The system prompt (same place the `memory` tool guidance lives) teaches: *when the
user states a commitment, deadline, or event — write it to the ledger; when asked
"what's on my plate" — query it; never keep user commitments only in the session task
list.*

## 7. Schedule bridge: reminders that actually fire

This is where the ledger stops being a database and becomes an assistant.

1. **Add `ScheduleTrigger::Once { at: DateTime<Utc> }`** to
   `crates/core/bamboo-domain/src/schedule/domain.rs` and `NativeTriggerEngine`
   (`next_after` returns `at` once, then `None`; the store disables the schedule after
   its terminal run). Small, independently useful change.
2. **Ledger-managed schedules.** When a record gains `remind_at`/`recurrence`, the
   `LedgerStore` (via a `ScheduleBridge` trait implemented in the server) upserts
   `ScheduleSpec`s tagged with `run_config` metadata `ledger_record_id`, and stores
   the ids back on `record.schedule_ids`. Transitioning a record to
   `Done`/`Cancelled` deletes/disables its schedules — the invariant is *the schedule
   store never outlives the intent*.
3. **When a reminder fires**, the existing manager path already does everything
   needed: creates a session, `task_message` = "Reminder for ledger record `<id>`:
   <title>. Check status, gather anything helpful, notify the user." With
   `auto_execute`, the headless run resolves context (the record, related memories)
   and the notification relay pushes to desktop/ntfy/bark. A reminder is therefore
   not a dumb ping — it is an agent turn with the record in hand (it can check
   whether the thing already got done, draft the email, summarize what's needed).
4. **A daily agenda schedule** ("every morning at 08:00, query today's agenda and
   send a briefing") ships as a default-off template — it is pure configuration on
   top of the above, which is the point of the design.

## 8. Prompt injection: the agenda layer

Extend `external_memory.rs` with one new layer, inserted between session notes and
relevant durable memories:

```
## External Memory (Persistent)
  [observed state]
  [session memory note]
  [📅 Agenda]            ← NEW: overdue + due-today + next-48h events + top open todos
  [relevant durable memories]
  [project index / dream summaries]
```

- Rendered from `indexes/by_time.json` + `by_status.json` — **no LLM cost, no BM25
  needed**; capped (~1200 chars) and count-limited (e.g. 10 items) like the other layers.
- Lives in the volatile block, so record churn never busts the prompt prefix cache —
  the same reason external memory already avoids the system message.
- Gated by a `PromptMemoryFlags`-style config flag (`ledger_agenda_injection`,
  default on when the ledger has any open records).

This single layer is what makes the assistant *proactive inside conversations*: any
session, on any topic, knows the user has a flight tomorrow.

## 9. Background loops: extractor + ledger gardener

Both piggyback on existing spawn points in `app_state/builder.rs` and follow the
gardener's golden rule — **deterministic prefilter first, LLM only when there is work**.

- **Commitment extractor** — extend the auto-dream pass (it already walks recent
  sessions with the background model). The extraction prompt additionally proposes
  *ledger candidates* ("user said they must renew the passport before August").
  Candidates are written as `status: Open` records with `created_by: agent` and
  `source.excerpt` set, and surfaced in the agenda as `(suggested)` until the user or
  agent confirms — auto-capture without silent authority.
- **Ledger gardener** — a fourth pass in `gardener.rs`:
  - *Expiry:* time-passed events and stale done records → `Expired`/archive (out of
    indexes/views, never deleted — same reversibility contract as memory archiving).
  - *Schedule reconciliation:* repair record↔schedule drift (crash between writes).
  - *Distillation:* completed/recurring patterns become **durable memories** ("takes
    medication daily at 9", "monthly report due first Monday") — this is the ledger
    feeding the long-term memory system, closing the loop the user's question
    intuited: prospective records *become* retrospective knowledge once resolved.

## 10. API + skill (thin layers on top)

- **HTTP:** `/api/v1/ledger` scope in `routes/agent.rs` — `GET/POST /records`,
  `PATCH/DELETE /records/{id}`, `GET /agenda?from=&to=` — so lotus/bodhi can render a
  real todo/calendar UI over the same store the agent uses. SSE change-feed events
  (`ledger.record.updated`) ride the existing `/stream` change feed.
- **Skill:** a built-in `personal-assistant` skill (`builtin_skills/personal-assistant/`)
  carrying the *persona and workflows* — morning-briefing format, GTD-style triage
  guidance, decomposition heuristics — with `allowed-tools` referencing `ledger` +
  `scheduler` + `memory`. Behavior policy lives in the skill (editable, no recompile);
  capability lives in the tool. This is the hybrid pattern the codebase already uses.

## 11. Phased roadmap

| Phase | Deliverable | Depends on | Status |
|---|---|---|---|
| **1** | `bamboo-domain/src/ledger/` types + `ledger_store` (records, indexes, views, audit) + unit tests | — | ✅ done |
| **2** | `ledger` overlay tool + system-prompt guidance + `promote` from session TaskList | 1 | ✅ done |
| **3** | `ScheduleTrigger::Once` + schedule bridge (record↔schedule lifecycle) | 1 | ✅ done |
| **4** | Agenda prompt-injection layer | 1 | ✅ done |
| **5** | Ledger gardener pass (expiry, reconciliation, distillation) | 1, 3 | ✅ done |
| **6** | Commitment extractor in auto-dream | 1, 2 | ✅ done |
| **7** | HTTP API + built-in skill + daily-briefing schedule template | 2–4 | ✅ done (SSE change-feed events deferred) |

Phases 1–4 are the minimum lovable assistant: remember commitments across sessions,
answer "what's due", fire reminders, and mention the agenda proactively. 5–7 make it
self-maintaining and product-visible.

## 12. Open questions

1. **Confirmation policy for extracted records** — `(suggested)` status vs. a
   notification-driven approve flow? Proposal: start with suggested-in-agenda, revisit.
2. **Scope default** — personal records are `Global`; should a session inside a
   project default work-items to `Project` scope? Proposal: yes, kind-dependent
   (Todo→ambient scope, Event/Reminder→Global).
3. **External calendar sync (CalDAV/ics)** — explicitly out of scope here; the
   `LedgerStore` trait and file-per-record layout leave room for an importer later.
4. **Tool name** — `ledger` vs `assistant` vs `todo`. `ledger` is generic and matches
   the store; `todo` may prompt-transfer better from model pretraining. Needs a quick
   eval.
