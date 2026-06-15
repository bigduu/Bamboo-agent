# bamboo-broker core routing — deep analysis

Scope: `crates/app/bamboo-broker/src/{proto,core,lib,error}.rs`, with supporting evidence from `bamboo-subagent/src/mailbox.rs` (the underlying maildir store the broker is built on). All line numbers are 1-based and refer to the files in this crate unless a `mailbox.rs:` prefix is given.

---

## 1. `error.rs` — the error surface

A 21-line file. Two items:

| Item | Location | Notes |
|---|---|---|
| `enum BrokerError` | `error.rs:5-19` | `#[derive(Debug)]` + `thiserror::Error`. Four variants. |
| `type BrokerResult<T> = Result<T, BrokerError>` | `error.rs:21` | The crate-wide Result alias. |

Variants:

| Variant | Line | `#[error(...)]` | `#[from]`? | Producer |
|---|---|---|---|---|
| `Store(StoreError)` | `error.rs:9` | `"store: {0}"` | **yes** — `bamboo_subagent::StoreError` | Any `Mailbox` op (`deliver`/`drain`/`recover`/`ack`) via `?`. See `core.rs:47`, `core.rs:68`, `core.rs:71`, `core.rs:85`, `core.rs:107`. |
| `Auth(String)` | `error.rs:11` | `"auth: {0}"` | no | WS auth layer (token check). Not produced inside `core.rs` at all. |
| `Protocol(String)` | `error.rs:14` | `"protocol: {0}"` | no | Out-of-sequence frames (e.g. request before `Hello`). Transport-layer concern. |
| `Transport(String)` | `error.rs:17` | `"transport: {0}"` | no | WebSocket / IO failures. |

**Observations**
- `Store` is the only `#[from]` in the enum, so `?` on a `StoreError` is the only implicit conversion in the crate. `Auth`/`Protocol`/`Transport` must be constructed explicitly with `BrokerError::Auth(..)` etc.
- There is **no `Serialization`/`Json` variant.** `proto.rs:44` and `proto.rs:53` `serde_json::to_string(...).expect(...)` — serialization panics rather than becoming a `BrokerError`. Justified only because the inputs are self-serialized DTOs; a malformed `InboxMessage.body` (arbitrary `serde_json::Value`) cannot fail to re-serialize, so the `expect` is safe in practice but worth noting as a panic surface.
- `from_text` (`proto.rs:46`, `proto.rs:55`) returns `serde_json::Result`, **not** `BrokerResult`. The WS layer must lift `serde_json::Error` into `BrokerError::Protocol(..)` manually; there is no `#[from] serde_json::Error`. This is a minor ergonomic gap.
- `BrokerError` is **not** `Clone`. `Auth`/`Protocol`/`Transport` carry `String`; `Store(#[from] StoreError)` is whatever `StoreError` is (also not `Clone`-likely). Fine for `?`-propagation but means tests can't `assert_eq!` on errors directly.

---

## 2. `proto.rs` — the wire protocol

### 2.1 Type reuse philosophy
`proto.rs:1-5` states it explicitly: the broker is a **transport** for `bamboo_subagent`'s message types; it does not reinterpret them. The only `use` (`proto.rs:7`) is:
```rust
use bamboo_subagent::{AgentRef, InboxMessage, MsgId};
```
So `InboxMessage` (the canonical inbox payload), `MsgId` (its id type), and `AgentRef` (session identity) are re-exported verbatim into the wire format. There is **no broker-private envelope** around a message — `BrokerFrame::Message { message: InboxMessage }` (`proto.rs:37`) ships the exact struct that the receiver's `Mailbox` would have stored. Implication: any schema change to `InboxMessage` is a wire-breaking change for the broker, with no versioning field to gate on.

### 2.2 `ClientFrame` (client → broker), `proto.rs:10-25`
`#[serde(tag = "kind", rename_all = "snake_case")]` on `proto.rs:12` — internally-tagged JSON enum, tag key `"kind"`, variant names lowercased.

| Variant | Line | Fields | Semantics |
|---|---|---|---|
| `Hello` | `proto.rs:16` | `agent: AgentRef`, `token: String` | First frame. Binds the connection to mailbox key `agent.session_id`. `token` is the auth credential. |
| `Deliver` | `proto.rs:18` | `to: String`, `message: InboxMessage` | Durably enqueue into `to`'s mailbox. `to` is a bare session id (no `AgentRef`), so routing is by session id only. |
| `Subscribe` | `proto.rs:21` | (unit) | Start push delivery of the caller's **own** mailbox. |
| `Ack` | `proto.rs:24` | `id: MsgId` | Acknowledge (delete) a pushed message. At-least-once redelivery hinge. |

The tag stability test at `proto.rs:104-106` pins `kind == "subscribe"` for the unit variant — this is the contract that protects against a future `rename_all` change silently breaking the wire.

### 2.3 `BrokerFrame` (broker → client), `proto.rs:27-40`
Same serde tag style (`proto.rs:29`).

| Variant | Line | Fields | Semantics |
|---|---|---|---|
| `Welcome` | `proto.rs:32` | (unit) | Handshake accepted. |
| `Error` | `proto.rs:35` | `reason: String` | Rejection; broker closes the connection after an auth error. |
| `Message` | `proto.rs:37` | `message: InboxMessage` | A pushed mailbox message. |
| `Delivered` | `proto.rs:39` | `id: MsgId` | Receipt for a processed `Deliver` (durably enqueued). |

**Notable asymmetry:** `ClientFrame::Deliver` takes the message by reference in the `BrokerCore` API (`core.rs:46`: `msg: &InboxMessage`), but the frame owns it (`proto.rs:18`). The WS layer must move the owned frame field into the borrow on dispatch.

### 2.4 `to_text` / `from_text`, `proto.rs:42-58`
```rust
pub fn to_text(&self) -> String { serde_json::to_string(self).expect("… serializes") }
pub fn from_text(s: &str) -> serde_json::Result<Self> { serde_json::from_str(s) }
```
Symmetric helpers on both enums. Two observations:
1. `to_text` **panics** on serialization failure (see §1). Round-trip tests at `proto.rs:84-107` and `proto.rs:109-122` prove equality after `from_text(to_text(x))`.
2. `from_text` returns `serde_json::Result`, not `BrokerResult`. There is no `#[from] serde_json::Error` on `BrokerError`, so the WS layer must map this to `BrokerError::Protocol(..)` by hand. This is a real friction point — every call site of `from_text` in `serve`/`server` pays this tax.

### 2.5 `token` in `Hello`, `proto.rs:16`
The auth token is a plain `String` field on `Hello`. Nothing in `proto.rs` validates it — that responsibility is pushed entirely to the WS layer (which would emit `BrokerFrame::Error { reason }` and close). `core.rs` has **no** notion of auth; `BrokerCore::deliver`/`subscribe`/`ack` accept any session id unconditionally. This is a clean separation (core is transport-agnostic and auth-free) but it means the security boundary lives entirely in whatever calls into `BrokerCore`. The `token` is sent in cleartext inside a JSON text frame; TLS is the WS layer's problem.

### 2.6 Test coverage, `proto.rs:60-122`
`ask_msg()` (`proto.rs:66-82`) constructs an `InboxMessage` with `InboxKind::Ask` + `AskBody { question, mode: AskMode::Query }`. Both round-trip tests iterate all variants. The tag-stability assertion (`proto.rs:105-106`) is the only wire-format pin; there is **no** negative test (malformed JSON, unknown `kind` tag, missing fields). An unknown `kind` currently fails `from_str` with a serde error — forward-compatibility (ignoring unknown tags) is **not** provided.

---

## 3. `core.rs` — the routing engine

### 3.1 State, `core.rs:25-29`
```rust
pub struct BrokerCore {
    root: PathBuf,
    subscribers: Mutex<HashMap<String, mpsc::UnboundedSender<InboxMessage>>>,
}
```
- `root` — the maildir root. Per-session mailbox is at `<root>/mailboxes/<session_id>` (`core.rs:40-42`). **No sanitization** of `session_id`: a caller-supplied `to: "../admin"` in `ClientFrame::Deliver` (`proto.rs:18`) becomes a path-traversal vector at the mailbox layer. This is only safe if the WS/auth layer canonicalizes or allowlists session ids before calling `BrokerCore`.
- `subscribers` — `session_id → unbounded sender`. Tokio `Mutex` (not `std`), so holding it across `.await` is allowed (and `subscribe` does exactly that — see §3.4). The use of `mpsc::UnboundedSender` means pushes never block the producer; if the subscriber's receiver is slow, messages accumulate unboundedly in the channel (memory pressure on a stalled consumer).

### 3.2 `mailbox()`, `core.rs:40-42`
```rust
fn mailbox(&self, session_id: &str) -> Mailbox {
    Mailbox::at(self.root.join("mailboxes").join(session_id))
}
```
A new `Mailbox` value is constructed **per call**. `Mailbox` is cheap (just a `PathBuf`, see `mailbox.rs:125-127`); it does not itself hold any state. All durability state lives on disk in `new/`, `cur/`, `corrupt/` subdirs (`mailbox.rs:129-137`). This is what makes the broker stateless across restarts — everything recoverable is on disk.

### 3.3 `deliver`, `core.rs:46-50`
```rust
pub async fn deliver(&self, to: &str, msg: &InboxMessage) -> BrokerResult<MsgId> {
    let id = self.mailbox(to).deliver(msg).await?;   // persist to new/
    self.push_new(to).await?;                          // claim + push if subscribed
    Ok(id)
}
```
Two phases:
1. **Durable enqueue** — `Mailbox::deliver` (`mailbox.rs:151-159`) serializes to JSON, names the file `<20-digit-nanos>-<msgid>.json` (lexicographic == time order, `mailbox.rs:153-155`), and `atomic_write`s it into `new/`. Returns the `MsgId`. After this line returns, the message survives a crash.
2. **Live push** — `push_new(to)` (`core.rs:99-111`) is a no-op if no subscriber is registered; otherwise it `drain()`s `new/` and pushes each claimed message. Critically, `push_new` drains **all** pending messages in `new/`, not just the one delivered in step 1 — so concurrent delivers to the same session collapse into one drain.

**Ordering invariant:** because `Mailbox::deliver` is synchronous-on-disk (atomic write completes before `await` resolves) and the filename prefix is `timestamp_nanos_opt`, messages delivered in wall-clock order on the same broker are drained in wall-clock order. This is a best-effort global ordering, not a causal one — two senders racing will get timestamps in whatever order the OS schedules the `atomic_write` calls.

### 3.4 `subscribe`, `core.rs:55-75` — the heart of the engine
```rust
pub async fn subscribe(&self, session_id: &str)
    -> BrokerResult<mpsc::UnboundedReceiver<InboxMessage>>
{
    let (tx, rx) = mpsc::unbounded_channel();
    self.subscribers.lock().await.insert(session_id.to_string(), tx.clone());  // ← (A)

    let mb = self.mailbox(session_id);
    for d in mb.recover().await? { let _ = tx.send(d.msg); }                   // ← (B) cur/
    for d in mb.drain().await?  { let _ = tx.send(d.msg); }                    // ← (C) new/
    Ok(rx)
}
```
Three sub-phases, in order:

- **(A) Register** the new sender in the map. `HashMap::insert` **replaces** any prior sender for the same `session_id` (documented at `core.rs:54`: "A prior subscriber for the same id is replaced"). The clone (`tx.clone()`) is technically unnecessary — `tx` could be moved — but is harmless. The map is still locked only for the duration of the `insert`; the lock is released before (B) and (C). **This is the central concurrency hazard and is analyzed in §5.1.**
- **(B) Recover** `cur/` — `mailbox.rs:220-...` reads all files already in `cur/` (claimed-but-unacked from a previous connection / crash). Files that fail to parse are moved to `corrupt/` (`mailbox.rs:231-232`). These are pushed to the new subscriber.
- **(C) Drain** `new/` — `mailbox.rs:165-...` atomically renames `new/<name>` → `cur/<name>` (`mailbox.rs:170-175`), reads, and pushes. Lost renames (already-claimed files) are skipped (`mailbox.rs:173-175`), making concurrent drains safe.

After `subscribe` returns, the caller owns `rx`. The sender side lives in the map until `unsubscribe` or a subsequent `subscribe` replaces it.

### 3.5 `unsubscribe`, `core.rs:79-81`
```rust
pub async fn unsubscribe(&self, session_id: &str) {
    self.subscribers.lock().await.remove(session_id);
}
```
Drops the map entry. The `UnboundedSender` is dropped here (if no clone remains — and there is no other clone after the one in `subscribe`), which will eventually cause the receiver's `rx.recv()` to return `None`. **Important:** messages already in `cur/` (claimed but unacked) are **not** deleted here — they stay on disk for the next `subscribe` to `recover`. This is the at-least-once redelivery mechanism, verified by the test at `core.rs:182-196`.

### 3.6 `ack`, `core.rs:84-87`
```rust
pub async fn ack(&self, session_id: &str, id: &MsgId) -> BrokerResult<()> {
    self.mailbox(session_id).ack(id).await?;
    Ok(())
}
```
Delegates to `Mailbox::ack` (`mailbox.rs:199-...`): scans `cur/` for a file whose name ends with `-<msgid>.json` (`mailbox.rs:200, 209`) and removes it. If `cur/` doesn't exist, returns `Ok(())` (`mailbox.rs:204-205`) — idempotent. After `ack`, the message will not be re-pushed by a subsequent `recover` (test at `core.rs:167-180`).

### 3.7 `push_new`, `core.rs:99-111`
```rust
async fn push_new(&self, session_id: &str) -> BrokerResult<()> {
    let tx = {
        let subs = self.subscribers.lock().await;
        match subs.get(session_id) {
            Some(tx) => tx.clone(),
            None => return Ok(()),
        }
    };                                                       // lock dropped here
    for d in self.mailbox(session_id).drain().await? {
        let _ = tx.send(d.msg);
    }
    Ok(())
}
```
- Takes the lock only to **clone the sender**, then releases it before the (slow, async) `drain`. This is deliberate: it minimizes critical-section length and prevents `deliver` from serializing across sessions.
- `let _ = tx.send(...)` silently drops the send if the receiver has been dropped (e.g. the subscriber disconnected between the clone and the send). This is intentional and safe — the message is already durable in `cur/` (drain renamed it), so dropping the in-memory push simply means the next `subscribe` will `recover` it.
- The docstring (`core.rs:94-98`) is precise: `push_new` does **not** `recover`. A live subscriber does not get re-spammed with its own not-yet-acked messages; only a fresh `subscribe` does.

### 3.8 `is_subscribed`, `core.rs:90-92`
`subs.lock().await.contains_key(session_id)`. Used only in tests (`core.rs:147`). Note: a `true` return does **not** guarantee the subscriber's receiver is still alive — the receiver could be dropped while the sender is still in the map. This is a liveness hint, not a correctness primitive.

### 3.9 Tests, `core.rs:114-207`
Five tests, all using `tempfile::TempDir` and `tokio::test`:
| Test | Line | Verifies |
|---|---|---|
| `deliver_then_subscribe_drains_backlog` | `core.rs:142-152` | Deliver-while-unsubscribed persists; subscribe later drains. |
| `subscribe_then_deliver_pushes_live` | `core.rs:154-164` | Subscribe-first then deliver pushes live. |
| `ack_removes_so_resubscribe_does_not_redeliver` | `core.rs:167-180` | Ack + unsubscribe + resubscribe ⇒ no redelivery. |
| `unacked_message_redelivers_on_resubscribe` | `core.rs:182-196` | No-ack + unsubscribe + resubscribe ⇒ redelivery via `recover`. |
| `deliver_to_unsubscribed_is_durable_and_isolated_per_session` | `core.rs:198-207` | Per-session isolation: delivering to "a" and "b", subscriber for "a" sees only "a". |

**Coverage gaps:** no test exercises (a) two subscribers racing on the same `session_id`, (b) `deliver` racing `subscribe`, (c) `push_new`'s silent-drop on a dead receiver, (d) path-traversal `session_id`, (e) concurrent `deliver` from multiple senders to one session. These gaps are exactly where the hazards in §5 live.

---

## 4. `lib.rs` — public API surface

### 4.1 Module graph, `lib.rs:19-28`
```rust
pub mod ask; pub mod client; pub mod core; pub mod deploy;
pub mod mcp;  pub mod proto; pub mod serve; pub mod server;
mod error;   // ← private; only re-exported types leak out
```
Eight public modules, one private (`error`). `error` is private but its types are re-exported (`lib.rs:40`), so downstream can name `BrokerError`/`BrokerResult` without seeing the module.

### 4.2 `ORCHESTRATOR_ID`, `lib.rs:30-32`
```rust
pub const ORCHESTRATOR_ID: &str = "bamboo-orchestrator";
```
The well-known mailbox id for the central orchestrator. Workers address MCP proxy requests here; `serve_mcp_proxy` listens here. The comment emphasizes "single MCP host" — this is a **fixed singleton**, not a per-instance id. Two brokers on the same disk root would collide on this mailbox; the deployment model assumes one broker per root.

### 4.3 Re-exports, `lib.rs:34-45`
| Re-export | Line | Origin |
|---|---|---|
| `ask_agent, ask_over, request_over` | `lib.rs:34` | `crate::ask` |
| `BrokerClient` | `lib.rs:35` | `crate::client` |
| `BrokerCore` | `lib.rs:36` | `crate::core` |
| `AgentDeployment, DeployedAgent, Deployer, DockerDeployer, LocalProcessDeployer, SshDeployer` | `lib.rs:37-39` | `crate::deploy` |
| `BrokerError, BrokerResult` | `lib.rs:40` | `crate::error` |
| `serve_mcp_proxy, McpProxyExecutor, McpReply, McpRequest, ProxiedResult` | `lib.rs:41` | `crate::mcp` |
| `BrokerFrame, ClientFrame` | `lib.rs:42` | `crate::proto` |
| `serve_executor, serve_loop, serve_mailbox, serve_with, Handled` | `lib.rs:43` | `crate::serve` |
| `BrokerServer` | `lib.rs:44` | `crate::server` |
| `AgentRef` | `lib.rs:45` | `bamboo_subagent` (passthrough) |

`lib.rs:45` re-exports `bamboo_subagent::AgentRef` so consumers can `use bamboo_broker::AgentRef` without a direct dep on `bamboo-subagent`. Notably **only `AgentRef` is re-exported**, not `InboxMessage` or `MsgId` — callers must still reach into `bamboo_subagent` for those, despite the proto frames embedding them. Minor inconsistency.

The crate-level doc (`lib.rs:1-17`) is clear about topology: **single central broker, hub-and-spoke, pure message bus, no actor spawning**. Placement lives behind `bamboo_subagent::WorkerLauncher`. This is the design contract that justifies all the single-broker assumptions in `core.rs`.

---

## 5. Concurrency & correctness analysis

### 5.1 Race: `deliver` vs `subscribe` on the same session ⚠️ **message duplication, not loss**

Trace two interleaved tasks on session `"s"`:

| Step | `deliver("s", m)` | `subscribe("s")` |
|---|---|---|
| 1 | `Mailbox::deliver(m)` → file appears in `new/` | |
| 2 | | `insert(tx)` into map (`core.rs:60-63`) |
| 3 | | `recover()` reads `cur/` — `m` not there |
| 4 | | `drain()` renames `new/m` → `cur/m`, pushes `m` to `tx` |
| 5 | `push_new("s")`: clones `tx`, calls `drain()` again | |
| 6 | `drain()` returns **empty** (`m` already in `cur/`) → no push | |

Result: `m` is delivered **once** to the subscriber. The maildir rename is the claiming operation and is atomic, so the two `drain()`s cannot both claim `m` (`mailbox.rs:172-175` guards the lost-rename race). **No loss, no duplication in this interleaving.**

The dangerous interleaving is:

| Step | `deliver("s", m)` | `subscribe("s")` |
|---|---|---|
| 1 | `Mailbox::deliver(m)` → `new/m` | |
| 2 | | `insert(tx)` |
| 3 | | `recover()` (empty) |
| 4 | `push_new("s")`: clones `tx`, `drain()` claims `new/m` → `cur/m`, **pushes `m` to `tx`** | |
| 5 | | `drain()` returns empty (already claimed) |
| 6 | | returns `rx` |

Still one delivery — `push_new` won step 4's rename. But now consider:

| Step | `deliver("s", m)` | `subscribe("s")` |
|---|---|---|
| 1 | `Mailbox::deliver(m)` → `new/m` | |
| 2 | | `insert(tx)` |
| 3 | | `recover()` empty |
| 4 | | `drain()` claims `new/m` → `cur/m`, pushes `m` to `tx` |
| 5 | `push_new("s")`: clones `tx`, `drain()` returns empty, **no push** | |

One delivery. The maildir claim-once semantics make `deliver`/`subscribe` on the same session **safe from both loss and duplication**, *provided* the subscriber actually consumes `rx`. The only residual risk is if `tx.send` in step 4 silently drops (receiver dropped between step 2 and 4): the message is then in `cur/` unacked, and will be redelivered by the **next** `subscribe`'s `recover`. Still no loss.

**Conclusion:** the `deliver`-vs-`subscribe` race is benign thanks to maildir claim-once. At-least-once is preserved; exactly-once is not promised (and the docs don't claim it). Consumers must dedupe by `MsgId` (`core.rs:13-14` says so).

### 5.2 Race: two concurrent `subscribe("s")` ⚠️ **old subscriber is silently orphaned**

This is the most subtle hazard. `core.rs:54` documents "A prior subscriber for the same id is replaced," but the consequences are non-obvious:

| Step | Task A `subscribe("s")` | Task B `subscribe("s")` |
|---|---|---|
| 1 | `insert(tx_A)` | |
| 2 | | `insert(tx_B)` — **drops the map's reference to `tx_A`** |
| 3 | `recover()` pushes to `tx_A` | |
| 4 | `drain()` pushes to `tx_A` | |
| 5 | returns `rx_A` | (B's recover/drain may find `cur/`/`new/` already drained by A) |

After step 2, `tx_A` is **only** held by Task A's local variable — the map no longer references it. So subsequent `deliver("s", …)` calls go to `tx_B` via `push_new`, **never to `tx_A`**. Task A's `rx_A` receives only whatever was pushed in steps 3-4 and then goes quiet forever (the channel stays open because Task A still holds `tx_A`'s sibling via the moved `tx`... actually no — `tx_A` is the local in A; the receiver `rx_A` will return `None` only when **all** senders drop. Task A holds `tx` until `subscribe` returns at which point `tx` goes out of scope — wait, `subscribe` clones `tx` into the map and the local `tx` is dropped at end of function. So after A returns, `rx_A` has zero senders and `recv()` returns `None`.)

Refined: after `subscribe` returns, the **only** sender for `rx_A` is the one in the map (the local `tx` was moved/cloned and dropped). When B's `insert` overwrites it, `rx_A` has **no remaining senders** → `rx_A.recv()` returns `None` immediately. Task A sees stream-end and (if well-written) treats it as "disconnected." **This is the intended semantics, but it is implicit** — there is no `BrokerFrame::Error { reason: "superseded" }`. A naive consumer might interpret the silent `None` as "mailbox empty forever" rather than "you were replaced."

**Message-loss scenario for the orphaned subscriber:** if Task A's `recover()`/`drain()` in steps 3-4 ran **after** B already drained (interleaving where B is fully first), then A's `drain()` finds `new/` empty and A gets nothing — but B already got the messages, so no actual loss system-wide. The loss is only "from A's point of view," which is correct because A was superseded.

**Real concern:** if A's `drain()` ran **before** B's `insert`, A claimed messages into `cur/` and pushed them to `rx_A`. Now B subscribes; B's `recover()` reads those same messages from `cur/` (they're unacked) and pushes them to `rx_B`. **Both A and B see the same messages.** This is at-least-once redelivery across two consumers of the same session — expected if you model it as "the session's mailbox has one reader at a time, but reader handoff can double-deliver in-flight messages." It is **not** a bug if consumers are idempotent by `MsgId`; it is a bug if a deployment assumes two subscribers means two independent consumers.

### 5.3 Race: `unsubscribe` vs `deliver` ⚠️ **benign (no loss)**

| Step | `deliver("s", m)` | `unsubscribe("s")` |
|---|---|---|
| 1 | `Mailbox::deliver(m)` → `new/m` | |
| 2 | | `remove("s")` — `tx` dropped from map |
| 3 | `push_new("s")`: `get("s")` → `None` → returns, **no push** | |

`m` stays in `new/`, durable. Next `subscribe("s")` will `drain()` it. **No loss.** This is the designed behavior for a disconnect mid-deliver.

### 5.4 Race: `push_new`'s cloned sender vs `unsubscribe` — **silent drop, benign**

`push_new` (`core.rs:100-106`) clones `tx` under the lock, then releases the lock, then `drain()`s and `tx.send()`s. If `unsubscribe` runs between the clone and the send, the send target is a **clone** that's still alive (the local `tx` in `push_new`), so the send succeeds into a channel whose receiver may already be gone. `let _ = tx.send(...)` discards the error. The message is durable in `cur/` (drain renamed it), so the next `subscribe` re-covers it. Benign.

### 5.5 Subscriber map concurrency model — **per-map mutex, no per-session lock**

There is exactly **one** `Mutex<HashMap<..>>` (`core.rs:28`) guarding **all** sessions. Consequences:
- **Pro:** atomicity of `insert`/`remove`/`get` is trivial — the mutex serializes them.
- **Con:** a slow `deliver` to session "x" does **not** block `deliver` to "y", because `push_new` releases the lock before the slow `drain()` (`core.rs:100-106`). Good.
- **Con:** `subscribe` holds no lock during its (slow) `recover`/`drain` (`core.rs:65-73`), but it **has already mutated the map** at `core.rs:60-63`. So the map reflects "s is subscribed" before the backlog is pushed. A concurrent `deliver` during A's `drain` will see A's `tx` in the map, `push_new`, and race A's `drain` on `new/` — benign per §5.1.

There is **no per-session locking**, so two operations on the *same* session can run truly concurrently (e.g. two `deliver`s, or `deliver` + `ack`). The maildir rename operations provide the per-message atomicity that the broker layer doesn't. This is a deliberate "lock only the map, let the filesystem serialize the mailbox" design.

### 5.6 Multiple subscribers to the same `session_id` — **not supported, last-writer-wins**

The `HashMap<String, Sender>` (`core.rs:28`) can hold **at most one** sender per session id. There is no `Vec<Sender>` / fan-out. Two clients that `Hello` with the same `agent.session_id` and both `Subscribe` will collide: the second `subscribe` (`core.rs:60-63`) replaces the first. The first client's receiver goes dead (§5.2). **The broker is a unicast-per-session bus, not a pub/sub topic.** Any "multiple workers share one session id" deployment will see one worker starved. This is consistent with the "single central orchestrator + named workers" topology in `lib.rs:10-12`, but it is **not enforced** — two workers can `Hello` with the same id and the broker won't refuse the second (no `BrokerError::Protocol("session in use")`). The first subscriber just silently loses the stream.

### 5.7 Message-loss scenarios — summary

Genuinely losing a message (neither delivered nor recoverable) requires:
1. **Ack-then-crash-before-effect:** `ack` returns `Ok(())` after `tokio::fs::remove_file` (`mailbox.rs:210-211`). If the OS has the file deletion in its page cache and the machine crashes, the file may reappear — at-least-once, not a loss. Real loss here needs a filesystem that loses acknowledged deletes, which is outside the broker's contract.
2. **Path traversal clobbering:** `mailbox("a/../b")` resolves to `<root>/mailboxes/b`. Combined with `..` in `to` or `session_id`, an attacker (or a buggy caller) could route messages into a different session's mailbox, then ack them away. Not "loss" exactly, but mis-routing. There is **no** `session_id` validation in `core.rs` — see `core.rs:40-42` and `core.rs:46`. The WS/auth layer is the only gate.
3. **`corrupt/` quarantine:** `recover` (`mailbox.rs:231-232`) moves unparseable `cur/` files to `corrupt/` and does **not** push them. So a message whose JSON is corrupted on disk (partial write — should be impossible given `atomic_write`, but a non-atomic fs move or external tampering could) is silently swallowed from the subscriber's perspective. It's not deleted (lives in `corrupt/`), so operator recovery is possible, but the consumer never sees it. This is the one in-band "loss" path, and it's by design.
4. **`push_new` silent send-drop on a dead receiver** is **not** loss — the message is in `cur/` and will be re-covered.

No other loss path exists in the analyzed code.

### 5.8 Ordering guarantees

- **Per-session FIFO by created_at:** `Mailbox::deliver` names files `<nanos>-<msgid>.json` (`mailbox.rs:153-155`), and `drain`/`recover` sort lexicographically (`mailbox.rs:167`, `mailbox.rs:222`). So within one session, messages are pushed in `created_at` order, **regardless of which producer delivered them or when.** Ties on nanos are broken by `MsgId`, which is monotonic-ish (assumed ULID/UUID — `MsgId::new()` at `proto.rs:68` etc.).
- **Cross-session ordering:** none. Two sessions are fully independent (separate mailboxes, separate channels).
- **Live-push order:** `push_new` drains in sorted order (`mailbox.rs:167`), so live pushes preserve FIFO. `subscribe` pushes `recover` (all of `cur/`) before `drain` (all of `new/`), which respects time order **only if** every `cur/` file is older than every `new/` file. This holds because `drain` always moves new→cur, so a file in `cur/` was delivered no later than the oldest file in `new/`. The 20-digit-nanos prefix makes it strictly hold unless clocks go backwards across broker restarts.

### 5.9 Liveness / back-pressure

- `mpsc::UnboundedSender` (`core.rs:28`) means **no back-pressure** from a slow consumer. A subscriber that stops calling `recv` will cause its channel to grow unboundedly; the broker never blocks. For a stalled consumer this is an OOM vector. The trade-off is that `deliver` to a slow session never blocks the producer (good for the hub-and-spoke orchestrator pattern). Switching to a bounded channel would add back-pressure but would also make `push_new`'s `let _ = tx.send(...)` lossy in a new way.
- The unbounded channel is also why `subscribe` can do all its backlog pushing synchronously inside the function (`core.rs:68-73`) without deadlock — it can't block on send.

---

## 6. Cross-cutting observations & risks

1. **No session-id validation** (`core.rs:40-42`, `core.rs:46`, `core.rs:55`, `core.rs:84`). Path traversal (`..`), empty string, and overlong ids all reach `Mailbox::at` unsanitized. The broker trusts the WS/auth layer entirely. If `serve`/`server` ever forward a `ClientFrame::Deliver { to, .. }` without validating `to`, an authenticated client can write into any session's mailbox (including `ORCHESTRATOR_ID`). **Recommend:** add a `fn valid_session_id(s: &str) -> bool` in `core.rs` and call it at the top of `deliver`/`subscribe`/`ack`, returning `BrokerError::Protocol("bad session id")`.
2. **No "session in use" rejection** (`core.rs:60-63`). Silent supersession (§5.2, §5.6) is operationally surprising. **Recommend:** on `insert` collision, either return `Err(BrokerError::Protocol("session already subscribed"))` or send `BrokerFrame::Error { reason: "superseded" }` to the old sender before replacing.
3. **`from_text` returns `serde_json::Result`, not `BrokerResult`** (`proto.rs:46`, `proto.rs:55`). Every call site pays a manual `.map_err(|e| BrokerError::Protocol(e.to_string()))?`. **Recommend:** add `#[from] serde_json::Error` to `BrokerError`, or change `from_text` to return `BrokerResult<Self>`.
4. **`to_text` panics on serialization failure** (`proto.rs:43-44`, `proto.rs:52-53`). Currently safe because inputs are self-serializing DTOs, but it's an invariant the type system doesn't enforce. **Recommend:** return `BrokerResult<String>` (after fixing #3) or at least document the invariant on `InboxMessage`.
5. **No forward-compat for unknown `kind` tags.** `serde(tag = "kind")` rejects unknown variants. A rolling deploy where the broker emits a new `BrokerFrame` variant will break old clients hard. **Recommend:** add `#[serde(other)]`-style catch-all or a version field if rolling deploys are planned. (serde doesn't support `#[serde(other)]` on internally-tagged enums with data — would need an untagged fallback or a wrapper.)
6. **`ORCHESTRATOR_ID` is a singleton** (`lib.rs:32`). Two broker processes on one maildir root will corrupt each other's `cur/`/`new/`. The design assumes one broker per root; this is unstated in `core.rs`. **Recommend:** document the one-broker-per-root invariant on `BrokerCore::new`.
7. **`deliver` returns `MsgId` but `Delivered` receipt carries the same id** (`core.rs:49`, `proto.rs:39`). The id is the *sender's* `msg.id` (passed through from `InboxMessage`), not a broker-assigned id. So the receipt is really just "yes, your message id X was enqueued." Fine, but worth noting the broker does no id assignment — dedup is entirely the consumer's job by `MsgId`.
8. **`tx.clone()` in `subscribe` is redundant** (`core.rs:63`). The local `tx` could be moved into the map (the channel is already created, only `rx` is needed by the caller). The clone is harmless but suggests the author may have once intended to keep a local sender. Minor.

---

## 7. File-level summary table

| File | LOC (ex. tests) | Public items | Role |
|---|---|---|---|
| `error.rs` | 21 | `BrokerError` (4 variants), `BrokerResult` | Error surface; only `Store` is `#[from]`. |
| `proto.rs` | 58 (ex. tests) | `ClientFrame` (4 var), `BrokerFrame` (4 var), `to_text`/`from_text` x2 | Wire DTOs; reuses `bamboo_subagent` types verbatim. |
| `core.rs` | 112 (ex. tests) | `BrokerCore`, `new/deliver/subscribe/unsubscribe/ack/is_subscribed`, `push_new` (priv) | Transport-agnostic routing engine; maildir-backed. |
| `lib.rs` | 45 | 8 pub mods, `ORCHESTRATOR_ID`, ~20 re-exports | Facade; documents hub-and-spoke topology. |

End of report.
