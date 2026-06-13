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
    /// Tier-1 fabric directory the worker self-registers into.
    pub fabric_dir: String,
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
    #[serde(default)]
    pub reusable: bool,
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
}

impl ProvisionSpec {
    pub fn new(identity: ChildIdentity, executor: ExecutorSpec, fabric_dir: String) -> Self {
        Self {
            version: PROVISION_VERSION,
            identity,
            executor,
            fabric_dir,
            storage_dir: None,
            workspace: None,
            model: None,
            disabled_tools: None,
            limits: Limits::default(),
            secrets: SecretsEnvelope::default(),
            reusable: false,
        }
    }

    pub fn to_json(&self) -> Result<String> {
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
        });
        s
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
    }
}
