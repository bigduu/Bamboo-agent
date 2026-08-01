//! ProvisionSpec — the one-shot bootstrap contract between parent and worker.
//!
//! The parent **decides** (model routing, tool policy, storage layout, credentials) and the
//! worker only **executes** the already-resolved result. The spec is fed to the worker over
//! **stdin, once, then the pipe closes** — never argv (visible in `ps`) or env (inherited by
//! grandchildren). Secrets ride in a dedicated envelope so the security story can evolve
//! (proxy mode, short-lived tokens) without touching the bootstrap flow.
//!
//! Forward compatibility: `version` + serde's default of ignoring unknown fields means an
//! older worker can read a newer spec (new fields are skipped) and a newer worker can read
//! an older spec (missing fields default). Parent and worker binaries need not be upgraded
//! in lockstep.

use serde::{Deserialize, Serialize};

use crate::error::{Result, StoreError};

/// Current spec version written by this crate.
pub const PROVISION_VERSION: u32 = 1;

/// Upper bound for a spec read from stdin (defense in depth against a
/// runaway writer; a real spec is a few KB).
pub const MAX_SPEC_BYTES: u64 = 8 * 1024 * 1024;

/// Everything a worker needs to become a functioning actor. Parent-resolved, flat, complete.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProvisionSpec {
    pub version: u32,
    pub identity: ChildIdentity,
    /// Which execution engine this actor runs (worker maps it via its factory).
    pub executor: ExecutorSpec,
    /// Tier-1 fabric directory the worker self-registers into (legacy direct-WS
    /// path). Ignored when `bus` is set.
    pub fabric_dir: String,
    /// The mailbox bus this actor dials home to instead of listening for a direct
    /// WS connection. When set, the worker serves its mailbox over the bus (the
    /// unified actor+mailbox transport); the parent drives it by mailbox id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bus: Option<BusEndpoint>,
    /// Isolated storage root for this actor's own session/mailbox files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_dir: Option<String>,
    /// Working directory for the actor's file operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    /// Final, parent-resolved model (explicit pin > per-type routing > defaults).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRefSpec>,
    /// Tool names to hide from the child (already resolved from the profile policy).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disabled_tools: Option<Vec<String>>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub secrets: SecretsEnvelope,
    /// When true the worker serves connection-after-connection (a warm, reusable
    /// actor) instead of exiting after one run. The parent pools such workers and
    /// reuses an idle one for the next assignment with a matching fingerprint
    /// (role/provider/model/workspace/tools), so N sibling sub-agents no longer
    /// mean N processes. Each run still gets a fresh session rehydrated from the
    /// run's `messages`, so context stays isolated across reuses.
    ///
    /// (The production actor runner always sets this `true`; the `false` path is
    /// exercised only by one-shot CLI/test workers.)
    #[serde(default)]
    pub reusable: bool,
    /// Where this actor runs. `Local` (default) — the parent spawns a local
    /// subprocess. `Remote{endpoint}` — connect to an already-running `wss://`
    /// worker. `Schedulable{pool}` — a control plane assigns an endpoint.
    /// Forward-compatible: an older spec without this field defaults to `Local`,
    /// so behavior is unchanged until a placement is set.
    #[serde(default)]
    pub placement: Placement,
    /// Capabilities synced from the orchestrator so a deployed worker matches its
    /// toolset (MCP servers + user skills). Empty for plain actor children (no
    /// behavior change); a deployed broker-agent fills these.
    #[serde(default)]
    pub capabilities: Capabilities,
}

/// Orchestrator-synced extras for a worker. Forward-compatible (all optional);
/// an older spec without these leaves the worker on builtin tools + isolated
/// skills exactly as before.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Serialized MCP config — opaque to this leaf crate; the worker deserializes
    /// it into the domain `McpConfig`. Typically the portable (SSE /
    /// streamable-http) subset; host-bound stdio servers are excluded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp: Option<serde_json::Value>,
    /// Directory of user/project skills the worker should load, instead of an
    /// empty isolated dir.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skills_dir: Option<String>,
    /// When set, the worker proxies its MCP tool calls to the orchestrator over
    /// the broker (host-bound servers like nova run only there). Mutually
    /// exclusive with `mcp` direct-sync — proxy covers all MCP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_proxy: Option<McpProxyConfig>,
    /// When `true`, the worker builds its tool executor WITH a permission
    /// checker, so gated tools hit `ConfirmationRequired` and delegate the
    /// decision to the host via the per-run `ApprovalProxy` (Phase 2:
    /// child → parent approval). Default `false` runs all tools unchecked.
    ///
    /// In practice this is effectively fixed per spawn path, not a free knob:
    /// the actor runner hard-sets it `true` (it has the ApprovalProxy bridge),
    /// while the broker-agent path leaves it `false` ON PURPOSE — that path has
    /// no approval delegation over the broker, so a gate would have nothing to
    /// delegate to. Only meaningful when the run has a host bridge to proxy to.
    #[serde(default)]
    pub enforce_permissions: bool,
    /// When `true`, the worker builds its OWN external-child runner, scheduler,
    /// and adapter and runs the REAL `SubAgent` tool directly, so a nested worker
    /// can spawn grandchildren in-process (Phase 6: direct nested execution).
    /// Default `false` — the worker has no `SubAgent` tool (a leaf sub-agent).
    #[serde(default)]
    pub nested_spawn: bool,
    /// Max nesting depth a self-orchestrating worker may spawn to (Phase 6:
    /// direct nested execution). A worker (or the root) refuses to spawn a child
    /// when its own `spawn_depth >= max_spawn_depth`. `None` ⇒ the default cap
    /// (4) applies. Carried down so every level enforces the same bound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_spawn_depth: Option<u32>,
    /// Whether this actor runs in "bypass permissions" mode (propagated from the
    /// parent at spawn). Phase 6: when true, a self-orchestrating worker installs
    /// an OFF-LOOP model-reviewer so its CHILDREN's forced-ask (dangerous) gated
    /// actions — which still fire `ConfirmationRequired` even under bypass — get
    /// an LLM reasonableness check instead of a blind pass.
    #[serde(default)]
    pub bypass: bool,
    /// Stronger zero-prompt permission posture. Kept separate from `bypass`
    /// because legacy bypass still routes forced confirmations to an approver.
    #[serde(default)]
    pub auto_approve_permissions: bool,
    /// Exact requested/effective permission posture provisioned for audit and
    /// rolling warm-worker activation. Empty values denote a legacy spec.
    #[serde(default)]
    pub permission_requested_mode: String,
    #[serde(default)]
    pub permission_effective_mode: String,
    /// Whether this run has NO interactive human approver (headless `-p`,
    /// scheduled jobs, deployed broker-agents — propagated from the unattended
    /// root). #73: when true, the worker's per-run `ApprovalProxy` decides a
    /// gated action with the OFF-LOOP model-reviewer LOCALLY instead of
    /// escalating to a human who will never answer (which would 300s-deny). When
    /// false (an interactive session) the approval escalates to the human as
    /// usual. Independent of `bypass` (an interactive bypass run still has a
    /// human; a headless default-mode run does not).
    #[serde(default)]
    pub no_human_approver: bool,
    /// Whether this worker is a READ-ONLY Guardian reviewer. #71: a guardian
    /// reviewer keeps `Bash` (its mutating tools are stripped by
    /// `guardian_read_only_disabled_tools`) so it can fetch the diff and run
    /// tests — but an unrestricted `Bash` would let it `rm -rf`, `git push`, or
    /// `curl | sh`, making the read-only guarantee nominal. When `true`, the
    /// worker installs a `GuardianReadOnlyChecker` that DENIES any `Bash`/
    /// `execute_command` whose command is not on the read-only allowlist
    /// (`is_read_only_command`) and runs read-only commands without gating.
    /// Default `false` preserves the unrestricted-Bash behavior for ordinary
    /// sub-agents. Set by the host's `build_spec` from the reviewer's session
    /// marker. Mirrors `no_human_approver` above.
    #[serde(default)]
    pub guardian_read_only: bool,
}

impl Capabilities {
    /// Decode the exact provision-time permission posture. Typed fields are
    /// authoritative for new specs; legacy booleans remain readable during a
    /// rolling upgrade, with Auto normalized independently from Bypass.
    pub fn permission_resolution(
        &self,
    ) -> std::result::Result<bamboo_domain::PermissionModeResolution, String> {
        if self.auto_approve_permissions && self.bypass {
            return Err(
                "provisioned auto_approve_permissions and bypass are mutually exclusive"
                    .to_string(),
            );
        }
        let has_requested_mode = !self.permission_requested_mode.is_empty();
        let has_effective_mode = !self.permission_effective_mode.is_empty();
        if has_requested_mode != has_effective_mode {
            return Err(
                "provisioned requested/effective permission modes must be provided together"
                    .to_string(),
            );
        }
        let requested = if self.permission_requested_mode.is_empty() {
            if self.auto_approve_permissions {
                bamboo_domain::SessionPermissionMode::Auto
            } else if self.bypass {
                bamboo_domain::SessionPermissionMode::Bypass
            } else {
                bamboo_domain::SessionPermissionMode::Default
            }
        } else {
            bamboo_domain::SessionPermissionMode::from_audit_str(&self.permission_requested_mode)
                .ok_or_else(|| {
                    format!(
                        "invalid provisioned requested permission mode '{}'",
                        self.permission_requested_mode
                    )
                })?
        };
        let effective = if self.permission_effective_mode.is_empty() {
            bamboo_domain::resolve_permission_mode(
                requested,
                bamboo_domain::PermissionMode::Default,
            )
            .effective
        } else {
            bamboo_domain::PermissionMode::from_audit_str(&self.permission_effective_mode)
                .ok_or_else(|| {
                    format!(
                        "invalid provisioned effective permission mode '{}'",
                        self.permission_effective_mode
                    )
                })?
        };
        let resolution = bamboo_domain::PermissionModeResolution {
            requested,
            effective,
        };
        if !resolution.is_consistent() {
            return Err("inconsistent provisioned permission posture".to_string());
        }
        if has_requested_mode
            && (self.bypass != resolution.bypass_permissions()
                || self.auto_approve_permissions != resolution.suppress_approval_prompts())
        {
            return Err("provisioned permission flags disagree with typed posture".to_string());
        }
        Ok(resolution)
    }
}

/// The mailbox bus an actor dials home to (the unified transport).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BusEndpoint {
    /// Broker WebSocket endpoint, e.g. `ws://127.0.0.1:9600`.
    pub endpoint: String,
    /// Bearer token presented in the broker handshake.
    pub token: String,
}

/// How a worker reaches the orchestrator's MCP proxy over the broker.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct McpProxyConfig {
    /// The orchestrator's broker mailbox id (proxy requests go here).
    pub orchestrator: String,
    /// Broker WebSocket endpoint.
    pub endpoint: String,
    /// Bearer token for the broker.
    pub token: String,
}

/// Where an actor physically runs — a configurable "temperature", not a baked-in
/// property (see `docs/remote-actor-plan.md` §3.4). Default `Local` keeps today's
/// behavior; the launcher picks the matching `WorkerLauncher` per variant.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Placement {
    /// Parent spawns a local subprocess (current behavior).
    #[default]
    Local,
    /// Connect to a resident worker already serving at `endpoint` (e.g.
    /// `wss://gpu-host:8443`).
    Remote { endpoint: String },
    /// Ask a control plane to assign an endpoint from a named pool.
    Schedulable { pool: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChildIdentity {
    pub child_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    /// Role/profile id, e.g. "researcher". Also published in the discovery record.
    #[serde(default)]
    pub role: String,
    /// Nesting depth of THIS actor in the spawn tree (root orchestrator = 0, its
    /// direct worker = 1, …). The worker stamps this onto its run session's
    /// `spawn_depth` so in-process children accumulate depth correctly ACROSS the
    /// actor process boundary (each worker otherwise starts at a fresh root).
    /// Used to enforce the max-depth cap (Phase 6: direct nested execution).
    #[serde(default)]
    pub depth: u32,
}

/// Which engine runs the task. The worker's factory maps each variant to a `ChildExecutor`;
/// adding an engine = one new variant + one factory arm, nothing else changes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutorSpec {
    /// Dependency-free echo stand-in (testing / smoke runs through the full chain).
    Echo,
    /// The real bamboo agent loop.
    BambooRuntime,
    /// Wrap an external CLI agent as the engine.
    CliAdapter { command: String, args: Vec<String> },
    /// Drive the official Claude Code CLI (`claude`) as the engine over its
    /// stream-json wire protocol (see `docs/claude-code-executor.md`). `binary`
    /// overrides the executable (tests point it at a stub script); `None` runs
    /// `claude` from `PATH`. `model` maps to the CLI's `--model` flag; `None`
    /// omits it (CLI default). `permission_mode` maps to `--permission-mode`;
    /// `None` no longer omits the flag — the executor passes an EXPLICIT
    /// `default` (issue #443: the CLI's headless stream-json default is
    /// `auto`, which self-approves every tool and never asks).
    ClaudeCode {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binary: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        permission_mode: Option<String>,
        /// Issue #443: `false`/`None` (the default) isolates the child from
        /// the invoking user's `~/.claude` setup (`--strict-mcp-config` +
        /// `--setting-sources project`) — an e2e run showed 6 MCP servers
        /// (incl. desktop control), all skills, and ~8k cache-creation tokens
        /// leaking in from global config. `Some(true)` opts back into the old
        /// inherit-everything behavior.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherit_user_config: Option<bool>,
        /// Issue #443: extra env var NAMES (verbatim, not values) forwarded
        /// to the child from the parent process env, on top of the fixed
        /// HOME/PATH/SHELL/TERM/LANG/LC_*/TMPDIR/USER/LOGNAME allowlist.
        /// Forwarding `ANTHROPIC_API_KEY` here is an explicit opt-in that
        /// flips billing from the CLI's own subscription auth to the API key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        forward_env: Option<Vec<String>>,
    },
    /// Drive the official Codex CLI in one-shot `exec` mode (default) or as a
    /// long-lived `app-server` JSON-RPC peer with interactive approval relay.
    Codex {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binary: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        /// `exec | app_server`; absent preserves the one-shot exec default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mode: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sandbox: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherit_user_config: Option<bool>,
        /// `inherit | api_key | custom | bamboo`. Unset is interpreted as
        /// `bamboo` unless the legacy `inherit_user_config = true` escape hatch
        /// is present.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        auth_mode: Option<String>,
        /// Custom-provider base URL, or the parent Bamboo loopback endpoint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_url: Option<String>,
        /// Codex custom-provider wire protocol. Codex >= 0.144 accepts
        /// `responses`; kept explicit on the wire for capability validation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wire_api: Option<String>,
        /// Stable credential reference for custom mode. Never contains the key.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_key_ref: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        forward_env: Option<Vec<String>>,
        /// `never | on-failure` in exec mode; `on-request` in app-server mode.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        approval_policy: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        network_access: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        allow_danger_bypass: Option<bool>,
        /// External-agent permission profile used for default sandbox mapping.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        permission_profile: Option<String>,
        /// Whether Bamboo created/owns the workspace. Only owned non-git
        /// workspaces may receive `--skip-git-repo-check`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workspace_owned: Option<bool>,
    },
}

/// Provider+model pair, parent-resolved. (Local mirror of `ProviderModelRef`;
/// this crate stays a leaf and does not depend on `bamboo-domain`.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRefSpec {
    pub provider: String,
    pub model: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rounds: Option<u32>,
}

/// Credentials scoped to exactly what this child needs — never the whole config.
/// Held in memory only; the worker must not persist it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SecretsEnvelope {
    #[serde(default)]
    pub provider_credentials: Vec<ScopedCredential>,
    /// Bearer token a remote resident worker requires on the WS handshake
    /// (remote-actor-plan §3.4 / P1). The parent puts the worker's expected
    /// token here and `ConnectLauncher` reads it to authenticate the connect.
    /// Additive + forward-compatible (older specs deserialize with `None`).
    /// Never published in a discovery `AgentRecord` — tokens stay in the
    /// scoped envelope, not in advertised records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_auth_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScopedCredential {
    /// Routing key as the parent knows it: a legacy provider name
    /// ("anthropic") or a provider-instance id (uuid).
    pub provider: String,
    pub api_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    /// Concrete provider protocol to construct ("anthropic", "openai", …).
    /// Needed when `provider` is an instance id; defaults to `provider`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_type: Option<String>,
    /// Stable reference used to select a custom Codex provider key without
    /// copying plaintext credential material into ordinary configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
}

impl ProvisionSpec {
    pub fn new(identity: ChildIdentity, executor: ExecutorSpec, fabric_dir: String) -> Self {
        Self {
            version: PROVISION_VERSION,
            identity,
            executor,
            fabric_dir,
            bus: None,
            storage_dir: None,
            workspace: None,
            model: None,
            disabled_tools: None,
            limits: Limits::default(),
            secrets: SecretsEnvelope::default(),
            reusable: false,
            placement: Placement::default(),
            capabilities: Capabilities::default(),
        }
    }

    /// Cross-field invariants enforced before a spec is shipped to a worker.
    ///
    /// `mcp` (direct portable servers) and `mcp_proxy` (proxy ALL MCP to the
    /// orchestrator) are mutually exclusive — the proxy already covers every
    /// server, so carrying both is contradictory. The worker would silently
    /// honor only `mcp_proxy`; fail closed here instead (D4 from the drift
    /// audit: this invariant was documented but never guarded).
    pub fn validate(&self) -> Result<()> {
        if self.capabilities.mcp.is_some() && self.capabilities.mcp_proxy.is_some() {
            return Err(StoreError::Invalid(
                "capabilities.mcp and capabilities.mcp_proxy are mutually exclusive \
                 (proxy covers all MCP) — set exactly one"
                    .to_string(),
            ));
        }
        self.capabilities
            .permission_resolution()
            .map_err(StoreError::Invalid)?;
        Ok(())
    }

    pub fn to_json(&self) -> Result<String> {
        // Enforce the invariants on EVERY serialization path (local spawn over
        // stdin, deploy, tests) so an invalid spec can never reach a worker.
        self.validate()?;
        serde_json::to_string(self)
            .map_err(|e| StoreError::decode(std::path::Path::new("<provision>"), e))
    }

    pub fn from_json(s: &str) -> Result<Self> {
        serde_json::from_str(s)
            .map_err(|e| StoreError::decode(std::path::Path::new("<provision>"), e))
    }

    /// Read a spec from the process's stdin (the parent writes one JSON document and
    /// closes the pipe). Used by worker `main`.
    ///
    /// Defense in depth: the read is capped at [`MAX_SPEC_BYTES`] — the pipe is
    /// trusted (our own parent), but a runaway writer must not OOM the worker.
    pub async fn read_from_stdin() -> Result<Self> {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        tokio::io::stdin()
            .take(MAX_SPEC_BYTES)
            .read_to_end(&mut buf)
            .await
            .map_err(|e| StoreError::io("<stdin>", e))?;
        let text = String::from_utf8_lossy(&buf);
        Self::from_json(text.trim())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ProvisionSpec {
        let mut s = ProvisionSpec::new(
            ChildIdentity {
                child_id: "c1".into(),
                parent_id: Some("p1".into()),
                project_key: Some("proj".into()),
                role: "researcher".into(),
                depth: 0,
            },
            ExecutorSpec::Echo,
            "/tmp/fabric".into(),
        );
        s.model = Some(ModelRefSpec {
            provider: "anthropic".into(),
            model: "claude-sonnet-4-6".into(),
        });
        s.secrets.provider_credentials.push(ScopedCredential {
            provider: "anthropic".into(),
            api_key: "sk-test".into(),
            base_url: None,
            provider_type: None,
            credential_ref: None,
        });
        s
    }

    #[test]
    fn validate_rejects_both_mcp_and_mcp_proxy() {
        let mut s = spec();
        s.capabilities.mcp = Some(serde_json::json!({"servers": []}));
        s.capabilities.mcp_proxy = Some(McpProxyConfig {
            orchestrator: "bamboo-orchestrator".into(),
            endpoint: "ws://127.0.0.1:9600".into(),
            token: "t".into(),
        });
        // validate() rejects, and to_json() (the universal ship path) propagates it.
        assert!(matches!(s.validate(), Err(StoreError::Invalid(_))));
        assert!(s.to_json().is_err());

        // Exactly one is fine.
        s.capabilities.mcp = None;
        assert!(s.validate().is_ok());
        assert!(s.to_json().is_ok());
    }

    #[test]
    fn round_trips() {
        let s = spec();
        let parsed = ProvisionSpec::from_json(&s.to_json().unwrap()).unwrap();
        assert_eq!(parsed, s);
    }

    #[test]
    fn unknown_fields_are_ignored_forward_compat() {
        // A "newer" spec with fields this version doesn't know about.
        let mut v: serde_json::Value = serde_json::from_str(&spec().to_json().unwrap()).unwrap();
        v["future_field"] = serde_json::json!({"x": 1});
        v["identity"]["future_sub"] = serde_json::json!(true);
        let parsed = ProvisionSpec::from_json(&v.to_string()).unwrap();
        assert_eq!(parsed.identity.child_id, "c1");
    }

    #[test]
    fn missing_optional_fields_default_backward_compat() {
        // A minimal "older" spec: only required fields.
        let minimal = serde_json::json!({
            "version": 1,
            "identity": { "child_id": "c9" },
            "executor": { "kind": "echo" },
            "fabric_dir": "/tmp/f",
        });
        let parsed = ProvisionSpec::from_json(&minimal.to_string()).unwrap();
        assert_eq!(parsed.identity.child_id, "c9");
        assert_eq!(parsed.executor, ExecutorSpec::Echo);
        assert!(parsed.model.is_none());
        assert!(parsed.secrets.provider_credentials.is_empty());
        assert_eq!(parsed.limits, Limits::default());
        // Placement defaults to Local for a spec that predates the field.
        assert_eq!(parsed.placement, Placement::Local);
    }

    #[test]
    fn placement_defaults_local_and_remote_round_trips() {
        // Default spec is Local, serialized with kind="local".
        let v: serde_json::Value = serde_json::from_str(&spec().to_json().unwrap()).unwrap();
        assert_eq!(v["placement"]["kind"], "local");

        // Remote round-trips with its endpoint.
        let mut s = spec();
        s.placement = Placement::Remote {
            endpoint: "wss://gpu-host:8443".into(),
        };
        let parsed = ProvisionSpec::from_json(&s.to_json().unwrap()).unwrap();
        assert_eq!(
            parsed.placement,
            Placement::Remote {
                endpoint: "wss://gpu-host:8443".into()
            }
        );
    }

    #[test]
    fn worker_auth_token_defaults_none_and_round_trips() {
        // Absent in JSON ⇒ None (forward-compatible: older specs have no token).
        assert!(SecretsEnvelope::default().worker_auth_token.is_none());
        let minimal = serde_json::json!({
            "version": 1,
            "identity": { "child_id": "c" },
            "executor": { "kind": "echo" },
            "fabric_dir": "/tmp/f",
        });
        let parsed = ProvisionSpec::from_json(&minimal.to_string()).unwrap();
        assert!(parsed.secrets.worker_auth_token.is_none());

        // Round-trips when set, and is omitted from JSON when None.
        let mut s = spec();
        assert!(
            !s.to_json().unwrap().contains("worker_auth_token"),
            "None token must be skipped in serialization"
        );
        s.secrets.worker_auth_token = Some("T-secret".into());
        let parsed = ProvisionSpec::from_json(&s.to_json().unwrap()).unwrap();
        assert_eq!(
            parsed.secrets.worker_auth_token.as_deref(),
            Some("T-secret")
        );
    }

    #[test]
    fn capabilities_default_empty_and_round_trip() {
        // Default spec carries no synced capabilities (actor children unaffected).
        assert_eq!(spec().capabilities, Capabilities::default());

        // Round-trips with content.
        let mut s = spec();
        s.capabilities = Capabilities {
            mcp: Some(serde_json::json!({ "version": 1, "servers": [] })),
            skills_dir: Some("/home/u/.bamboo/skills".into()),
            mcp_proxy: None,
            enforce_permissions: false,
            nested_spawn: false,
            max_spawn_depth: None,
            bypass: false,
            auto_approve_permissions: false,
            permission_requested_mode: String::new(),
            permission_effective_mode: String::new(),
            no_human_approver: false,
            guardian_read_only: false,
        };
        let parsed = ProvisionSpec::from_json(&s.to_json().unwrap()).unwrap();
        assert_eq!(
            parsed.capabilities.skills_dir.as_deref(),
            Some("/home/u/.bamboo/skills")
        );
        assert!(parsed.capabilities.mcp.is_some());

        // Backward compat: a spec without `capabilities` defaults to empty.
        let minimal = serde_json::json!({
            "version": 1,
            "identity": { "child_id": "c" },
            "executor": { "kind": "echo" },
            "fabric_dir": "/tmp/f",
        });
        let parsed = ProvisionSpec::from_json(&minimal.to_string()).unwrap();
        assert_eq!(parsed.capabilities, Capabilities::default());
    }

    #[test]
    fn capabilities_reject_partial_typed_permission_mode_pairs() {
        let mut requested_only = Capabilities {
            permission_requested_mode: "auto".to_string(),
            auto_approve_permissions: true,
            ..Capabilities::default()
        };
        assert!(requested_only
            .permission_resolution()
            .unwrap_err()
            .contains("provided together"));
        let mut invalid_spec = spec();
        invalid_spec.capabilities = requested_only.clone();
        assert!(matches!(
            invalid_spec.validate(),
            Err(StoreError::Invalid(_))
        ));
        assert!(invalid_spec.to_json().is_err());

        requested_only.permission_requested_mode.clear();
        requested_only.permission_effective_mode = "auto".to_string();
        assert!(requested_only
            .permission_resolution()
            .unwrap_err()
            .contains("provided together"));

        let legacy_auto = Capabilities {
            auto_approve_permissions: true,
            ..Capabilities::default()
        };
        assert_eq!(
            legacy_auto.permission_resolution().unwrap(),
            bamboo_domain::resolve_permission_mode(
                bamboo_domain::SessionPermissionMode::Auto,
                bamboo_domain::PermissionMode::Default,
            )
        );
    }

    #[test]
    fn enforce_permissions_defaults_false_and_round_trips() {
        // Absent in JSON ⇒ false (backward compatible with older orchestrators).
        assert!(!Capabilities::default().enforce_permissions);
        // Round-trips when opted in.
        let mut s = spec();
        s.capabilities.enforce_permissions = true;
        let parsed = ProvisionSpec::from_json(&s.to_json().unwrap()).unwrap();
        assert!(parsed.capabilities.enforce_permissions);
    }

    #[test]
    fn executor_tags_are_stable() {
        let v: serde_json::Value = serde_json::from_str(&spec().to_json().unwrap()).unwrap();
        assert_eq!(v["executor"]["kind"], "echo");
        let cli = ExecutorSpec::CliAdapter {
            command: "claude".into(),
            args: vec!["-p".into()],
        };
        let vv = serde_json::to_value(&cli).unwrap();
        assert_eq!(vv["kind"], "cli_adapter");
        assert_eq!(
            serde_json::to_value(ExecutorSpec::BambooRuntime).unwrap()["kind"],
            "bamboo_runtime"
        );
        let claude_code = ExecutorSpec::ClaudeCode {
            binary: Some("/usr/local/bin/claude".into()),
            model: Some("claude-sonnet-4-6".into()),
            permission_mode: Some("bypassPermissions".into()),
            inherit_user_config: Some(true),
            forward_env: Some(vec!["ANTHROPIC_API_KEY".into()]),
        };
        let vcc = serde_json::to_value(&claude_code).unwrap();
        assert_eq!(vcc["kind"], "claude_code");
        assert_eq!(vcc["binary"], "/usr/local/bin/claude");
        assert_eq!(vcc["inherit_user_config"], true);
        assert_eq!(vcc["forward_env"][0], "ANTHROPIC_API_KEY");
        // Round-trips.
        assert_eq!(
            serde_json::from_value::<ExecutorSpec>(vcc).unwrap(),
            claude_code
        );
        let minimal = ExecutorSpec::ClaudeCode {
            binary: None,
            model: None,
            permission_mode: None,
            inherit_user_config: None,
            forward_env: None,
        };
        let vmin = serde_json::to_value(&minimal).unwrap();
        assert_eq!(vmin, serde_json::json!({"kind": "claude_code"}));

        let codex = ExecutorSpec::Codex {
            binary: Some("/usr/local/bin/codex".into()),
            model: Some("gpt-5-codex".into()),
            mode: Some("exec".into()),
            sandbox: Some("workspace-write".into()),
            inherit_user_config: Some(true),
            auth_mode: Some("inherit".into()),
            base_url: None,
            wire_api: None,
            provider_key_ref: None,
            forward_env: Some(vec!["OPENAI_API_KEY".into()]),
            approval_policy: Some("never".into()),
            network_access: Some(true),
            allow_danger_bypass: Some(false),
            permission_profile: Some("restricted".into()),
            workspace_owned: Some(true),
        };
        let vc = serde_json::to_value(&codex).unwrap();
        assert_eq!(vc["kind"], "codex");
        assert_eq!(vc["binary"], "/usr/local/bin/codex");
        assert_eq!(vc["sandbox"], "workspace-write");
        assert_eq!(vc["forward_env"][0], "OPENAI_API_KEY");
        assert_eq!(vc["approval_policy"], "never");
        assert_eq!(vc["network_access"], true);
        assert_eq!(vc["permission_profile"], "restricted");
        assert_eq!(serde_json::from_value::<ExecutorSpec>(vc).unwrap(), codex);
        let minimal_codex = ExecutorSpec::Codex {
            binary: None,
            model: None,
            mode: None,
            sandbox: None,
            inherit_user_config: None,
            auth_mode: None,
            base_url: None,
            wire_api: None,
            provider_key_ref: None,
            forward_env: None,
            approval_policy: None,
            network_access: None,
            allow_danger_bypass: None,
            permission_profile: None,
            workspace_owned: None,
        };
        assert_eq!(
            serde_json::to_value(&minimal_codex).unwrap(),
            serde_json::json!({"kind": "codex"})
        );
    }
}
