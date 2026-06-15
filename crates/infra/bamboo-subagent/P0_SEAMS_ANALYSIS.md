# P0 Seams Analysis — provision.rs / launcher.rs / discovery.rs

Crate: `crates/infra/bamboo-subagent`
Scope: Deep read of the three P0 seam files in the subagent crate. Each section maps types/fields to behavior, then calls out smells, edge cases, races, and specific line numbers.

---

## 1. `provision.rs` — the one-shot bootstrap contract

**Purpose (module doc, lines 1–12):** The parent decides everything (model routing, tool policy, storage, credentials); the worker only executes. The spec is fed over **stdin once, then the pipe closes** — deliberately *not* argv (visible in `ps`) or env (inherited by grandchildren). Secrets are isolated in a dedicated envelope so the security story can evolve (proxy mode, short-lived tokens) without touching bootstrap.

### 1.1 Constants

| Constant | Value | Line | Role |
|---|---|---|---|
| `PROVISION_VERSION` | `1` | 19 | Current schema version written by this crate |
| `MAX_SPEC_BYTES` | `8 MiB` | 23 | Hard cap on a stdin read; real specs are a few KB |

### 1.2 `ProvisionSpec` (lines 26–70) — every field

Derives `Debug, Clone, PartialEq, Serialize, Deserialize`.

| Field | Type | Serde | Default | Purpose / behavior |
|---|---|---|---|---|
| `version` | `u32` | required | `PROVISION_VERSION` via `new()` | Schema version; **written but never validated on read** (see smell S1) |
| `identity` | `ChildIdentity` | required | — | Who this child is |
| `executor` | `ExecutorSpec` | required (internally tagged) | — | Which engine runs |
| `fabric_dir` | `String` | required | — | Tier-1 discovery directory the worker self-registers into |
| `storage_dir` | `Option<String>` | `default, skip_if_none` | `None` | Isolated storage root for session/mailbox files |
| `workspace` | `Option<String>` | `default, skip_if_none` | `None` | Cwd for the actor's file ops |
| `model` | `Option<ModelRefSpec>` | `default, skip_if_none` | `None` | Final parent-resolved model (explicit pin > per-type routing > defaults) |
| `disabled_tools` | `Option<Vec<String>>` | `default, skip_if_none` | `None` | Tool names hidden from the child (profile policy already applied) |
| `limits` | `Limits` | `default` | `Limits::default()` | Time/rounds budget |
| `secrets` | `SecretsEnvelope` | `default` | empty envelope | Scoped credentials |
| `reusable` | `bool` | `default` | `false` | If true, worker serves many runs (warm pool); each run still gets a fresh session rehydrated from `messages` |
| `placement` | `Placement` | `default` | `Local` | Where the actor runs |
| `capabilities` | `Capabilities` | `default` | empty | Orchestrator-synced MCP/skills; empty for plain actor children |

### 1.3 `Placement` (lines 104–118) — forward compatibility via `serde(default)`

```rust
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Placement { #[default] Local, Remote { endpoint }, Schedulable { pool } }
```

- **Tagged enum** (`kind` field, snake_case) → serializes as `{"kind":"local"}`, `{"kind":"remote","endpoint":"…"}`, `{"kind":"schedulable","pool":"…"}`.
- `#[default] Local` + field-level `#[serde(default)]` (line 63) → a spec that predates the field deserializes to `Local`, preserving today's behavior. Verified by `missing_optional_fields_default_backward_compat` (line 278) and `placement_defaults_local_and_remote_round_trips` (line 297).
- **Forward compat gap (S2):** an *unknown* `kind` value from a newer parent (e.g. `{"kind":"k8s"}`) is a hard deserialize error, not a fallback to `Local`. So the "older worker reads newer spec" claim in the module doc holds only for *unknown fields*, not *unknown enum variants*. There is no `#[serde(other)]` escape hatch (and none is possible with struct variants).

### 1.4 `Capabilities` (lines 75–91) — capability sync model

```rust
pub struct Capabilities {
    pub mcp: Option<serde_json::Value>,         // opaque; worker deserializes to domain McpConfig
    pub skills_dir: Option<String>,
    pub mcp_proxy: Option<McpProxyConfig>,
}
```

- **Sync model:** the orchestrator *pushes* a snapshot of the toolset into the spec; the worker loads it verbatim. There is **no incremental/delta sync** — every spec carries the full capability set. Empty `Capabilities` (default) means builtin tools + isolated empty skills dir, i.e. zero behavior change for actor children.
- **`mcp` is opaque** (`serde_json::Value`) deliberately so this leaf crate avoids depending on `bamboo-domain`'s `McpConfig`. Validation is deferred to the worker.
- **Mutual exclusion `mcp` vs `mcp_proxy`** is documented (line 88: "Mutually exclusive with `mcp` direct-sync") but **not enforced anywhere in this crate** (S3). A spec carrying both would be accepted here; only the worker knows to reject it.

### 1.5 `McpProxyConfig` (lines 94–102)

`{ orchestrator, endpoint, token }` — the broker mailbox id, `wss://` endpoint, and bearer token the worker uses to proxy MCP tool calls to the orchestrator. `token` is a plaintext secret riding in the spec (consistent with the stdin-not-argv story, but note it is `Debug`-printable via the derived `Debug` on `McpProxyConfig`/`Capabilities`/`ProvisionSpec` — S4: credential leakage via `Debug`).

### 1.6 `SecretsEnvelope` / `ScopedCredential` (lines 163–183) — scoped credential model

```rust
pub struct SecretsEnvelope { pub provider_credentials: Vec<ScopedCredential> }
pub struct ScopedCredential {
    pub provider: String,            // routing key: legacy name ("anthropic") OR instance uuid
    pub api_key: String,
    pub base_url: Option<String>,
    pub provider_type: Option<String>, // concrete protocol; defaults to `provider` when None
}
```

- **Scope principle (line 163):** credentials are scoped to *exactly what this child needs*, never the whole config. Held in memory only; the worker must not persist them (enforced elsewhere, not in this file).
- **Provider routing:** `provider` is polymorphic — either a legacy name or an instance id. When it is an instance id, `provider_type` disambiguates the concrete protocol; when `None`, it falls back to `provider` itself. This is a clean two-mode routing key.
- **`api_key` is plaintext in the struct** and the struct derives `Debug` (line 171) → **`Debug` prints the key** (S4, same root cause as McpProxyConfig). No `redact` helper anywhere in this file.

### 1.7 `ExecutorSpec` (lines 134–143)

```rust
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutorSpec { Echo, BambooRuntime, CliAdapter { command, args } }
```

- Internally tagged, snake_case → `{"kind":"echo"}`, `{"kind":"bamboo_runtime"}`, `{"kind":"cli_adapter","command":"…","args":[…]}`. Verified by `executor_tags_are_stable` (line 347).
- **Extension story (line 133):** adding an engine = one new variant + one factory arm in the worker. Clean.
- **Same forward-compat gap as `Placement` (S2):** an unknown `kind` from a newer parent is a hard error.

### 1.8 `ChildIdentity` / `ModelRefSpec` / `Limits` (lines 120–161)

- `ChildIdentity { child_id, parent_id?, project_key?, role }` — `role` defaults to `""` (line 128 `#[serde(default)]`), which is a valid but semantically odd "no role" value (S5: consider `Option<String>`).
- `ModelRefSpec { provider, model }` — local mirror of `ProviderModelRef`; this crate stays a leaf (no `bamboo-domain` dep).
- `Limits { run_timeout_secs?, idle_timeout_secs?, max_rounds? }` — all optional, all default to `None` (unbounded). **No enforcement here** — these are advisory values the worker must honor.

### 1.9 `ProvisionSpec::new` and codecs (lines 185–230)

- `new()` (line 186) sets `version = PROVISION_VERSION` and fills every optional/default field explicitly — good defensive style, no `..Default::default()` footgun.
- `to_json` / `from_json` (lines 204–212) are thin wrappers mapping serde errors onto `StoreError::decode`.
- **`read_from_stdin` (lines 219–229) — the bootstrap flow:**
  1. `tokio::io::stdin().take(MAX_SPEC_BYTES).read_to_end(&mut buf)` — caps the read at 8 MiB.
  2. `String::from_utf8_lossy(&buf)` — lossy UTF-8 (S6: silently mutates invalid bytes; a malicious/garbage stdin becomes a valid-but-wrong string, then a serde error rather than a clear "non-UTF8 stdin" error).
  3. `Self::from_json(text.trim())` — trim whitespace, parse.
  - **Defense:** `MAX_SPEC_BYTES` prevents OOM from a runaway writer (the pipe is trusted, but defense-in-depth).
  - **Smell S7:** the read is uncapped on *time* — a writer that opens the pipe but never closes it will hang `read_to_end` forever (no deadline). A real runaway writer is the stated threat; a stalled one is the unaddressed sibling threat.

### 1.10 Version compatibility — forward + backward

| Direction | Mechanism | Evidence |
|---|---|---|
| **Forward** (older worker, newer spec) | serde ignores unknown fields by default | `unknown_fields_are_ignored_forward_compat` (line 268) |
| **Backward** (newer worker, older spec) | every added field has `#[serde(default)]` or `Default` | `missing_optional_fields_default_backward_compat` (line 278) |

- **Caveat (S1):** `version` is *written* but never *checked* on read. There is no `if spec.version > PROVISION_VERSION { warn }` or reject. So a v5 spec read by a v1 worker will silently apply v1 semantics to v5 fields it happens to share names with. The version field is currently documentation-only.

### 1.11 provision.rs — smell / edge-case / race register

| ID | Lines | Kind | Issue |
|---|---|---|---|
| S1 | 19, 209–212 | Smell | `version` written but never validated on read; no compat gate |
| S2 | 108–118, 135–143 | Forward-compat gap | Unknown `kind` on `Placement`/`ExecutorSpec` is a hard error, not a fallback — contradicts the "older worker reads newer spec" claim for variant additions |
| S3 | 80–90 | Smell | `mcp` ⊕ `mcp_proxy` mutual exclusion documented but unenforced in this crate |
| S4 | 26, 94, 171 | Security | `ProvisionSpec`, `McpProxyConfig`, `ScopedCredential` derive `Debug` → `api_key`/`token` printable. No redaction |
| S5 | 128 | Smell | `role: String` defaults to `""`; `Option<String>` would be clearer |
| S6 | 227 | Edge case | `from_utf8_lossy` silently replaces invalid bytes → opaque downstream serde error instead of a clear "non-UTF8 stdin" error |
| S7 | 222–226 | Edge case | `read_to_end` has no time bound; a stalled parent pipe hangs the worker forever |
| S8 | 153–161 | Smell | `Limits` are advisory; no enforcement in this crate (acceptable — worker's job — but worth noting) |

---

## 2. `launcher.rs` — the placement seam

**Purpose (lines 1–8):** abstracts *how* a worker comes to exist so the fleet/runner never branches on placement. Phase 0 ships only `LocalSubprocessLauncher`; remote/schedulable launchers plug in later behind the same trait.

### 2.1 `WorkerLauncher` trait (lines 23–26)

```rust
#[async_trait]
pub trait WorkerLauncher: Send + Sync {
    async fn launch(&self, spec: &ProvisionSpec, wait: Duration) -> TransportResult<SpawnedChild>;
}
```

- **Object safety:** the trait is `Send + Sync` with a single `async fn` (desugared by `async_trait` to `Box<dyn Future + Send>`). The unit test `local_subprocess_launcher_is_a_trait_object` (line 58) explicitly asserts `&dyn WorkerLauncher` compiles, confirming the design intent of holding `Arc<dyn WorkerLauncher>`.
- **`launch` contract:** bring up (or connect to) one worker for `spec`, wait up to `wait` for it to become reachable (self-registered in discovery), return a `SpawnedChild` (which owns the process and kills it on drop via `kill_on_drop`). On timeout, `spawn_worker` kills the process (see `fleet.rs` doc line 41).
- **Return type asymmetry (L1):** `launch` returns `SpawnedChild`, a struct that owns a `tokio::process::Child`. That type only makes sense for *local* subprocesses; a `RemoteLauncher` (connect to `wss://`) has no PID to kill. The trait's return type implicitly bakes in "local process" semantics, which will force a refactor (e.g. an enum return or a `WorkerHandle` abstraction) when remote launchers land. This is the single biggest design risk in this file.
- **`wait: Duration` (L2):** the deadline is a positional `Duration` rather than an `Instant` or a cancellation token. There is no way to cancel a launch early from outside; the caller can only wait for the timeout.

### 2.2 `LocalSubprocessLauncher` (lines 31–50)

```rust
pub struct LocalSubprocessLauncher { pub worker_bin: PathBuf, pub worker_args: Vec<String> }
```

- **It is a literal zero-behavior-change wrapper:** `launch` (line 47) does nothing but forward to `spawn_worker(&self.worker_bin, &self.worker_args, spec, wait)`. All real logic (create `fabric_dir`, encode spec, spawn, feed stdin, close, poll for registration) lives in `fleet::spawn_worker` (`fleet.rs:45`).
- **Public fields (L3):** `worker_bin` and `worker_args` are `pub`, so callers can mutate them after construction. Probably unintentional — a launcher is conceptually configured once. `pub` also weakens invariants (e.g. nothing stops setting `worker_bin` to empty).
- **No validation (L4):** `new` does not check that `worker_bin` exists or is executable; failure is deferred to `launch` → `spawn_worker` → `Command::spawn` → `TransportError::Io`.

### 2.3 launcher.rs — smell / edge-case / race register

| ID | Lines | Kind | Issue |
|---|---|---|---|
| L1 | 25 | Design risk | Return type `SpawnedChild` bakes in local-subprocess semantics; remote launchers have no PID — forces trait refactor |
| L2 | 25 | Smell | `wait: Duration` precludes external cancellation; no `CancellationToken` |
| L3 | 31–34 | Smell | Public mutable fields; no construction-time validation |
| L4 | 37–42 | Smell | No `worker_bin` existence/executable check in `new` |

---

## 3. `discovery.rs` — the file fabric

**Purpose (lines 1–5):** a process-independent, file-based Tier-1 fabric. Each actor `publish`es `<dir>/<agent_id>.json` atomically; others `discover` by scanning and dropping stale (lease-expired) records. Long-running service agents live here; owned children use the Tier-2 registry.

### 3.1 `Discovery` trait (lines 122–129)

```rust
#[async_trait]
pub trait Discovery: Send + Sync {
    async fn publish(&self, rec: &AgentRecord) -> Result<()>;
    async fn resolve(&self, agent_id: &str) -> Result<Option<AgentRecord>>;
    async fn discover(&self) -> Result<Vec<AgentRecord>>;
    async fn withdraw(&self, agent_id: &str) -> Result<()>;
    async fn gc(&self) -> Result<usize>;
}
```

- **Object-safe** (`Send + Sync`, `async_trait`) — `fabric_is_usable_as_dyn_discovery` (line 233) drives it through `&dyn Discovery`.
- **Method set semantics:**
  - `publish` — upsert (atomic write).
  - `resolve(id)` — point lookup, `None` if missing or stale.
  - `discover` — full live list, sorted by `agent_id`.
  - `withdraw(id)` — delete (idempotent).
  - `gc` — sweep stale files, return count removed.
- **Missing operations (D1):** no `list_all` (including stale) and no `watch`/subscribe. Callers wanting change notifications must poll `discover`.

### 3.2 `Fabric` / `FileFabric` (lines 17–156)

`pub type FileFabric = Fabric;` (line 134) — alias names the intent without renaming call sites.

`record_path` (line 26): `dir.join(format!("{agent_id}.json"))`.

**`publish` (lines 32–36):** `serde_json::to_vec_pretty` → `atomic_write`. Atomicity comes from `error::atomic_write` (`error.rs:49`): write to `.<stem>.tmp.<uuid>` in the same dir, `sync_all`, then `rename`. The temp name is `.`-prefixed and unique so directory scanners skip it (the `.`-prefix filter in `discover`/`gc` at lines 66/102 is coordinated with this).

**`withdraw` (lines 39–45):** `remove_file`, treating `NotFound` as success (idempotent).

**`discover` / `discover_as_of` (lines 48–77):**
- Read dir; `NotFound` → empty vec (treats missing fabric as empty, not an error).
- For each entry: skip `.`-prefixed or non-`.json`; parse; keep if `lease_expires_at > now`.
- Sort by `agent_id` (deterministic; `discover_as_of` is the testable form).

**`resolve` (lines 80–85):** point read of `<id>.json`; `None` if missing, corrupt, or stale.

**`gc` (lines 88–114):** scan all `.json`; a record is stale if `lease_expires_at <= now` **or** unreadable/corrupt (line 107: `_ => true`); `remove_file` and count. Unreadable files are treated as stale and reaped — good defensive choice for a local fabric.

**`read_record` (lines 158–167):** `tokio::fs::read` → `serde_json::from_slice`; corrupt JSON → `Ok(None)` (not an error); `NotFound` → `Ok(None)`; other IO → `Err`.

### 3.3 `AgentRecord` (defined in `proto.rs:11–24`, used throughout discovery)

```rust
pub struct AgentRecord {
    pub agent_id: String,
    pub role: String,
    pub labels: Vec<String>,            // #[serde(default)]
    pub endpoint: String,               // ws://127.0.0.1:<port>
    pub pid: u32,
    pub version: String,                // #[serde(default)]
    pub started_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
}
```

- **Liveness key:** `lease_expires_at`. A reader treats the record as stale once `now > lease_expires_at` (strict `>` in `discover`/`resolve`; `<=` in `gc`). The 1-second boundary asymmetry is benign.
- **Renewal = re-publish with a bumped `lease_expires_at`** (line 30–31 doc) — there is no separate `heartbeat`/`renew` method; renewal and update are the same operation.
- **`pid`** is `u32` (no `Option`) — a remote worker has no local PID, yet this field is required. Forward-compat smell (D2).
- **`endpoint`** is a required `String` — fine for service agents (the documented use) but assumes every published actor is network-reachable.

### 3.4 Lease expiry & gc — how liveness works

The model is **soft-state, lease-based** with no cross-process locking:

1. On startup, a worker `publish`es its record with `lease_expires_at = now + lease_ttl`.
2. While alive, it periodically re-`publish`es with a bumped expiry (renewal).
3. Readers (`discover`/`resolve`) filter `lease_expires_at > now` → a crashed worker that stops renewing disappears from results after one TTL.
4. `gc` physically deletes files whose `lease_expires_at <= now` (or that are corrupt). Disk reclaims happen lazily; correctness does not depend on `gc` running.

**This is a fundamentally eventually-consistent, race-tolerant design** — there is no mutex, and that is intentional.

### 3.5 discovery.rs — smell / edge-case / race register

| ID | Lines | Kind | Issue |
|---|---|---|---|
| D1 | 122–129 | Smell | No `watch`/subscribe; change detection requires polling `discover` |
| D2 | proto.rs:18 | Forward-compat | `pid: u32` is required; remote actors have no PID → will need `Option` or a sentinel |
| D3 | 26 | Security/path-injection | `record_path` does `format!("{agent_id}.json")` with no sanitization. `agent_id` containing `/` or `..` escapes `dir`. Today `agent_id` is parent-controlled, but if it ever becomes user-supplied this is path traversal. Cheap to harden (`agent_id.contains('/')` reject, or hash the id). |
| D4 | 32–36, 105–109 | Race (benign) | `publish` (temp+rename) and `gc` (remove stale) race: gc may unlink a file mid-publish-target. The atomic rename means the worst case is gc removing a just-published-but-not-yet-renamed *old* file, or publish's rename landing after gc's unlink. Both converge to a correct state on the next publish/gc cycle. No corruption, but a record can briefly vanish from `discover`. Acceptable for soft-state. |
| D5 | 105–108 | Edge case | `gc` treats *any* read error (not just corrupt JSON) as stale and deletes. A transient permission error would silently delete a live record. `read_record` already distinguishes `NotFound` vs real IO; `gc` collapses the `Err` arm into "stale". Could instead skip-on-error. |
| D6 | 88, 48 | Edge case | `discover` and `gc` are **not atomic** w.r.t. the directory — they `read_dir` then iterate. Files added/removed during the scan may or may not be seen. Fine for liveness, but the count returned by `gc` is only a lower bound of what was stale at any instant. |
| D7 | 60–74, 96–112 | Performance | Both `discover` and `gc` do a full directory scan + per-file read. At scale (thousands of agents) this is O(n) IO per call with no caching. Acceptable for a local fabric today; a network backend would need indexing. |
| D8 | 158–167 | Smell | `read_record` swallows corrupt JSON as `Ok(None)` — silently invisible. No metric/log on corruption (would help debug "why did my agent disappear"). |
| D9 | 26 | Edge case | Two agents with ids differing only by filesystem-unsafe chars on case-insensitive FSes (macOS HFS+/APFS default) could collide (e.g. `Agent` vs `agent`). |

---

## 4. Cross-file observations

1. **Secret handling consistency (S4):** `provision.rs` carries plaintext secrets (`api_key`, `token`) in `Debug`-deriving structs. The bootstrap flow is correct (stdin, not argv/env), but anyone who `dbg!(spec)` leaks credentials. A `redacted` Debug helper or a manual `Debug` impl is the standard fix. This is the most actionable security finding.
2. **Forward-compat story is *field*-level only (S1/S2):** the module doc promises bidirectional compat, and it holds for fields. But it does *not* hold for new enum variants (`Placement`, `ExecutorSpec`) and the `version` field is decorative. Either add `#[serde(other)]`-style fallback (impossible for struct variants — would require an `Unknown { kind: String, rest: Value }` catch-all) or narrow the doc claim.
3. **`SpawnedChild` leak (L1):** `launcher.rs` returns a local-process type from a trait meant for remote backends. This is the seam most likely to need rework when `Placement::Remote` actually ships.
4. **Path safety (D3):** `discovery.rs` trusts `agent_id` to be a single path component. Today safe; document the invariant or sanitize, because it is the cheapest hardening available.
5. **No time bound on bootstrap read (S7):** `read_from_stdin` has a byte cap but no deadline. A parent that stalls mid-write hangs the worker. Pair `take(MAX_SPEC_BYTES)` with a `tokio::time::timeout`.
