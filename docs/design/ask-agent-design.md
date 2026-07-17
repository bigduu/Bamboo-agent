# `ask_agent` — durable Mailbox request/reply to a sub-agent

> Status: design + implementation plan. Builds on Change A (actor-only sub-agents).
> Decisions ratified by the user: transport = **file Mailbox** (durable, not WS);
> surface = **`action="ask"` on the `SubAgent` tool**; answer source = **both modes**
> (`query` summarize/extract + `steer` inject/redirect); not-live = **auto-activate**.

## 1. Why the Mailbox, and where it lives

The `Mailbox` type (`crates/infra/bamboo-subagent/src/mailbox.rs`, maildir `new/cur/corrupt`,
`InboxKind::{Task,Ask,Handoff,Reply}`) is fully built but **dormant** — only `Registry` +
tests touch it; the live actor path never drains a mailbox. The live cross-process
coordination point between parent (server) and worker (`bamboo subagent-worker`) is the
shared **`fabric_dir`** (`ProvisionSpec.fabric_dir`, default `$TMP/bamboo-subagents`), where
the worker self-registers discovery records (`Fabric`).

We therefore root ask/reply mailboxes under the shared fabric:

```
<fabric_dir>/mailboxes/<session_id>/{new,cur,corrupt}/    # one mailbox per session
```

- **Ask** → delivered to `<fabric>/mailboxes/<child_id>/` (the target child's inbox).
- **Reply** → delivered to `<fabric>/mailboxes/<parent_id>/` (the asking parent's inbox).

Both sides construct `Mailbox::at(<fabric>/mailboxes/<id>)` — no `SubagentStore` needed.

## 2. Data model (mailbox.rs)

`InboxMessage` gains a correlation id so a Reply can be matched to its Ask:

```rust
pub struct InboxMessage {
    pub id: MsgId,
    pub from: AgentRef,                 // sender (parent for Ask, child for Reply)
    pub kind: InboxKind,                // Ask | Reply (others already exist)
    pub body: serde_json::Value,        // Ask: {question, mode}; Reply: {answer}
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<MsgId>,  // NEW: Reply.correlation_id == Ask.id
}
```

`AgentRef.session_id` already carries the addressing we need (parent vs child id).
Body helpers: `AskBody { question: String, mode: AskMode }`, `ReplyBody { answer: String }`,
`enum AskMode { Query, Steer }` (serde snake_case, default `Query`).

## 3. Provisioning (provision.rs)

The worker must know (a) the fabric (already has it), (b) its own session id, and (c) the
parent's session id (to address replies). `ChildIdentity` already carries the agent/session
identity; add `parent_session_id` to `ProvisionSpec` (`serde(default)`, forward-compatible)
if not already derivable. The worker derives:
- own inbox  = `Mailbox::at(<fabric>/mailboxes/<own_session_id>)`
- reply inbox = `Mailbox::at(<fabric>/mailboxes/<parent_session_id>)`

## 4. Worker side (subagent_worker.rs) — drain at known points (no busy loop)

Add `drain_asks(&own_mailbox, &reply_mailbox, &agent, &session)`:
1. `own_mailbox.drain()` → for each `Ask` `InboxMessage` (dedupe via `AdmittedSet`):
   - `Query` mode: clone the current session, run a short `agent.execute(question)` on the
     clone, harvest the final assistant message (reuse the run-result harvesting at
     `subagent_worker.rs:359-366`). Live task untouched.
   - `Steer` mode: inject `question` into the live session as a user turn and run; the
     resulting final assistant message is the answer (this is the "redirect the goal" path).
   - Deliver `Reply { answer, correlation_id = ask.id, from = child }` to `reply_mailbox`.
   - `ack` the Ask.

Call `drain_asks` at: worker **startup** (before/around the first `Run`; catches asks queued
during auto-activate), each **round boundary** (where `SteerInbox` is already drained), and on
a new **`ParentFrame::DrainAsks`** WS nudge (prompt drain when live). Serialize asks per worker
(handle one at a time) to bound LLM load.

## 5. Parent side

- **Engine**: `ChildSessionPort::ask_child(child_id, question, mode, timeout) -> Result<String>`
  + `ask_child_action` near `send_message_to_child_action` (`child_session/actions.rs:337`):
  `load_child_for_parent` scope guard → resolve resident name if given → deliver Ask to the
  child's fabric mailbox → if `!is_live`, **auto-activate** (enqueue/spawn via the actor
  runner) else send `ParentFrame::DrainAsks` nudge → poll the parent's reply mailbox for a
  `Reply` with `correlation_id == ask.id`, bounded by `timeout` → `ack` + return `answer`.
- **Server adapter** (`child_session_adapter.rs`): implements `ask_child` against the fabric
  mailbox + `external_agents::live` (nudge / is_live) + the spawn scheduler (auto-activate).

## 6. Tool surface (sub_agent.rs)

New `SubAgentArgs::Ask { child_session_id: Option<String>, resident_name: Option<String>,
question: String, mode: Option<AskMode>, timeout_secs: Option<u64> }`. Dispatch near `:849`:
resolve target (bare id, or `find_resident_child` per `:539-557`), call `ask_child_action`,
return `tool_result({ from, answer, mode, status: "answered" })` (synchronous — no
`register_parent_wait_for_child`). Document `action="ask"` in the tool description (mention the
`ask_agent` alias name). No change to `SERVER_TOOL_NAMES`.

## 7. Implementation layers (each compiles + tests before the next)

1. **Data/proto**: `InboxMessage.correlation_id`, `AskBody`/`ReplyBody`/`AskMode`,
   `ParentFrame::DrainAsks`. Unit tests (round-trip).
2. **Worker drain**: `drain_asks` + call sites + `DrainAsks` handling. Echo-executor test.
3. **Engine port + action**: `ask_child` trait method, `ask_child_action`. Fake-port tests.
4. **Server adapter**: fabric mailbox deliver/poll + auto-activate + nudge.
5. **Tool action** + description + resident resolution. `sub_agent_tests` coverage.
6. End-to-end test (worktree integration): ask a live echo child, assert the reply.

## 8. Open/again-confirmable

- Mode default = `query`. Steer mode mutates the child's live conversation.
- Auto-activate spawns the worker if idle/dead; a never-created child still errors
  (not a child of caller).
- Concurrency: asks serialized per worker for v1.
