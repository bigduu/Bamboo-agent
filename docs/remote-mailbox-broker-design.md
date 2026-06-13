# Remote Mailbox Broker + `ask_agent` over the network

> Supersedes the *transport* section of `ask-agent-design.md`. Builds on Change A
> (actor-only sub-agents) and continues `remote-actor-plan.md` (remote actors).
>
> **Ratified decisions:** ask/reply transport = a **standalone network broker**
> (`bamboo broker serve`), **WebSocket push**, durable via the existing **`Mailbox`
> maildir**, **Bearer** auth; also lay the **remote-actor-plan.md P0 seams** now;
> not-live target → **auto-activate**; answer modes = **`query` + `steer`**.

Why not a file mailbox under a shared dir: it is local-filesystem-only and cannot
reach a remote worker. The broker puts the durable `Mailbox` behind a network
endpoint so parent and worker — local or remote — reach it over WS, getting
durability *and* remote reach together.

---

## Phase 0 — remote-worker seams (= remote-actor-plan.md §6 P0, zero behavior change)

Abstract the three local death-knots into traits; default impls replicate today
exactly. (Details + line refs in `remote-actor-plan.md` §2.2/§3.)

1. `WorkerLauncher` trait + `LocalSubprocessLauncher` — wraps `fleet.rs::spawn_worker`.
   Returns `LaunchedWorker { client: ChildClient, kill_handle: Option<…> }`.
2. `Discovery` trait + `FileFabric` — wraps `discovery.rs::Fabric`
   (publish/resolve/discover/withdraw/gc).
3. `WsServer::bind(addr)` / `bind_tls(addr, identity)` — `bind_loopback` stays the default.
4. `Placement` enum (`Local` | `Remote{endpoint}` | `Schedulable{pool}`) into
   `ProvisionSpec` (`serde(default)=Local`, forward-compatible).
- **Acceptance:** `subagent_actor_via_server.rs`, `subagent_worker_e2e.rs`,
  `e2e_subprocess.rs` all green; `cargo test -p bamboo-subagent` passes; no behavior change.

These seams are what let the broker (Phase 1) place/connect workers locally or remotely
without the ask path caring where they run.

---

## Phase 1 — `bamboo broker serve` (standalone WS broker)

### Process & wire
- New hidden subcommand `bamboo broker serve --bind <addr> [--tls] --token <T>`
  (mirror `subagent-worker` registration in `src/bin/bamboo.rs`).
- WebSocket server (`WsServer::bind`/`bind_tls`). One WS connection per client
  (parent or worker). Bearer token in the WS handshake subprotocol / first frame
  (reuse `ProvisionSpec.secrets` scoped-envelope discipline — token never in argv/env).

### Identity & addressing
- Each client identifies on connect: `Hello { agent_ref: AgentRef, token }` where
  `AgentRef.session_id` is the mailbox key. The broker owns one `Mailbox` per
  `session_id` under its own root: `<broker_root>/mailboxes/<session_id>/{new,cur,corrupt}`.

### Broker frames (new `broker::proto`)
```
ClientFrame (client → broker):
  Hello { agent_ref, token }
  Deliver { to: session_id, message: InboxMessage }   // enqueue into to's mailbox
  Subscribe                                            // start receiving my mailbox
  Ack { id: MsgId }                                    // delete from cur/
BrokerFrame (broker → client):
  Welcome { } | Error { reason }
  Message { message: InboxMessage }                    // pushed from my mailbox (new/→cur/)
  Delivered { id }                                     // deliver receipt
```
- **Durability:** `Deliver` does `Mailbox::deliver` (atomic temp+rename) before acking
  the sender — survives broker restart. On `Subscribe`, the broker `recover()`s `cur/`
  then drains `new/`, pushing `Message` frames; consumer `Ack`s to delete. At-least-once;
  dedupe via `AdmittedSet` (already exists).
- **Push:** broker watches each subscribed mailbox (notify on `Deliver` to that key +
  periodic sweep) and pushes promptly — no client polling.

### Reuse
- `InboxMessage` / `InboxKind::{Ask,Reply}` / `AskBody` / `ReplyBody` / `correlation_id`
  (Layer 1, already built) are the broker's message schema verbatim.
- `Mailbox` maildir is the broker's storage verbatim.

---

## Phase 2 — `ask_agent` over the broker

Flow (target = caller's own child / resident; scope guard = send_message's
Root-caller + `load_child_for_parent`):
```
SubAgent action=ask
  └─ ask_child_action(parent, target, question, mode, timeout)
       └─ broker.Deliver { to: child_id, Ask{ question, mode }, from: parent, id: ask_id }
       └─ if target not live → auto-activate:
            Placement::Local  → LocalSubprocessLauncher (spawn worker; on boot it
                                 Subscribes to broker, drains the queued Ask)
            Placement::Remote → ConnectLauncher to the remote endpoint (P1)
       └─ await BrokerFrame::Message{ Reply{answer}, correlation_id==ask_id } on the
          parent's own broker subscription, bounded by timeout → Ack → return {answer}
```
Worker side (connects to broker instead of/in addition to the parent WS):
- On boot: `Subscribe`; for each `Ask`: `query` = ephemeral `agent.execute` on a session
  clone; `steer` = inject as a live user turn; harvest final assistant message; `Deliver`
  a `Reply{answer, correlation_id=ask.id}` to `from.session_id` (the parent); `Ack` the Ask.
- Asks serialized per worker (bound LLM load).

Tool: `SubAgentArgs::Ask { child_session_id?|resident_name?, question, mode?, timeout_secs? }`
→ `tool_result({from, answer, mode, status:"answered"})`. No `SERVER_TOOL_NAMES` change.

---

## Build order (each compiles + tests before the next)

- **P0.1** `WorkerLauncher` + `LocalSubprocessLauncher` (wrap spawn_worker).
- **P0.2** `Discovery` + `FileFabric` (wrap Fabric).
- **P0.3** `WsServer::bind`/`bind_tls`; **P0.4** `Placement` into `ProvisionSpec`.
  → e2e green, no behavior change.
- **B1** broker crate/binary: frames + WS server + Mailbox-backed routing + auth + tests.
- **B2** broker client (used by parent adapter and worker).
- **B3** worker: subscribe + Ask handler (query/steer) + Reply.
- **B4** engine `ask_child_action` + port + auto-activate via launcher.
- **B5** `SubAgent action=ask` + resident resolution + description + tests + e2e.

---

## SHIPPED (what actually landed)

Branch `feat/subagent-actor-only`. All additive over Change A (actor-only) + Phase 0
(remote-worker seams). Everything below is implemented and tested.

**Topology (ratified):** single central broker, hub-and-spoke. The broker is a *pure
message bus* (routes Ask/Reply; never spawns or coordinates brokers). The master deploys
execution environments (local subprocess / Docker / SSH) that dial home to the broker —
push model, not mutual discovery.

**Crate `bamboo-broker`** (`crates/app/bamboo-broker`):
- `proto` — `ClientFrame`(Hello/Deliver/Subscribe/Ack) ↔ `BrokerFrame`(Welcome/Error/Message/Delivered).
- `core::BrokerCore` — durable per-session `Mailbox` routing, push subscriptions, at-least-once.
- `server::BrokerServer` — WS bus + Bearer-token handshake (`bamboo broker serve`).
- `client::BrokerClient` — connect + demux (messages / delivered).
- `serve::serve_executor` — worker loop; answers each Ask by running a `ChildExecutor`;
  **query** = read-only over a context copy, **steer** = persist into context. Works with
  EchoExecutor (no LLM) and the real BambooRuntime executor.
- `ask::ask_agent` / `ask_over` — orchestrator delivers an Ask and awaits the correlated Reply.
- `deploy` — `Deployer` + `LocalProcessDeployer` / `DockerDeployer` / `SshDeployer`
  (all spawn `bamboo broker-agent serve …`; token via env, never argv).

**Deployable agent:** `bamboo broker-agent serve --broker <ws> --token <t> --id <id> [--echo|--model]`
(`src/broker_agent.rs`) — connects to a broker and serves its mailbox, anywhere.

**In-loop command tool:** `ask_agent` (`bamboo-server-tools`), overlaid on the Root surface
only when `subagents.broker { endpoint, token }` is configured. A running root agent calls it
to command another broker-deployed agent (query/steer). No change to the SubAgent tool.
(Note: realized as a dedicated `ask_agent` tool rather than a `SubAgent` action — the broker
ask is a different substrate than SubAgent's child-session ports, and an isolated overlay tool
is lower-risk; `OverlayToolExecutor` routes by tool name so no `SERVER_TOOL_NAMES` change.)

**Transport note:** ask/reply rides the broker's WS-fronted durable mailbox (remote-capable);
the literal file mailbox is the broker's storage substrate. `ParentFrame::DrainAsks` (the
interim file-nudge) was dropped.

**Tests (all green):** broker 14 lib + ws_roundtrip 3 + serve/ask integration; `ask_agent_tool`
2; deploy e2e 3 (real `broker-agent --echo` subprocess via LocalProcessDeployer: single query+steer,
two-agent independent command, gated live-Docker). Regression: config 110, subagent 41 + e2e 2,
engine 789, server 850, server-tools 26, actor e2e 3 (server→real worker), session_history 4.

**Deferred (clearly scoped):** `bind_tls`/`wss://` + remote `ConnectLauncher`/`Placement::Remote`
end-to-end (P1 of `remote-actor-plan.md`; the seams + `Placement` enum are in place); the
real-LLM broker-agent path is wired but its e2e needs a provider (the deterministic path uses
`--echo`); federated broker-to-broker (explicitly out of scope — hub-and-spoke chosen).

## Layer-1 reconciliation
Keep `InboxMessage.correlation_id`, `AskMode`, `AskBody`, `ReplyBody` (broker schema).
Drop `ParentFrame::DrainAsks` (file-nudge, obsolete under the broker) when wiring B3.

## Open/again-confirmable
- Broker as its own crate `crates/app/bamboo-broker` (lib) + subcommand in the `bamboo` binary.
- v1 single broker instance; HA/sharding later.
- Discovery (`RegistryFabric`) can later move onto the broker too (remote-actor-plan.md P2).
