//! `bamboo subagent-worker` — the real actor worker.
//!
//! Three-stage shape (same as the demo worker, with the real engine arm):
//!
//! ```text
//! read ProvisionSpec from stdin → executor factory → bind WS / self-register / serve → cleanup
//! ```
//!
//! [`BambooRuntimeExecutor`] maps `ExecutorSpec::BambooRuntime` to the actual agent loop:
//! an isolated `Config` is assembled **in memory** from the spec's `SecretsEnvelope`
//! (credentials never touch argv, env, or disk), storage/skills/metrics live under the
//! spec's isolated `storage_dir`, and `agent.execute()` streams `AgentEvent`s back over
//! the WebSocket verbatim (zero mapping).

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use bamboo_agent_core::{AgentError, AgentEvent, Message, Role, Session};
use bamboo_llm::{create_provider_by_name, Config};
use bamboo_metrics::{MetricsCollector, SqliteMetricsStorage};
use bamboo_skills::{SkillManager, SkillStoreConfig};
use bamboo_storage::{LockedSessionStore, SessionStoreV2};
use bamboo_subagent::discovery::Fabric;
use bamboo_subagent::executor::{ChildExecutor, ChildOutcome, EchoExecutor, EventSink, SteerInbox};
use bamboo_subagent::proto::{AgentRecord, RunSpec};
use bamboo_subagent::provision::{ExecutorSpec, ProvisionSpec};
use bamboo_subagent::transport::WsServer;

/// How long a finished actor's isolated storage is retained for debugging
/// before background GC removes it.
const STORAGE_RETENTION: std::time::Duration = std::time::Duration::from_secs(7 * 24 * 60 * 60);

/// Worker entry point: provision from stdin, build the executor, serve one run, clean up.
pub async fn run() -> std::result::Result<(), String> {
    // Stage 1: provision (one JSON document on stdin; the parent closes the pipe).
    let spec = ProvisionSpec::read_from_stdin()
        .await
        .map_err(|e| format!("read ProvisionSpec from stdin: {e}"))?;

    // Best-effort housekeeping while we boot: expire stale sibling storage
    // dirs (default retention 7 days) and stale fabric records.
    tokio::spawn(gc_stale_storage(
        std::env::temp_dir().join("bamboo-subagents"),
        STORAGE_RETENTION,
    ));
    {
        let fab = Fabric::at(&spec.fabric_dir);
        tokio::spawn(async move {
            let _ = fab.gc().await;
        });
    }

    // Stage 2: executor factory.
    let executor: Arc<dyn ChildExecutor> = match &spec.executor {
        ExecutorSpec::Echo => Arc::new(EchoExecutor),
        ExecutorSpec::BambooRuntime => Arc::new(BambooRuntimeExecutor::build(&spec).await?),
        ExecutorSpec::CliAdapter { .. } => {
            return Err("cli_adapter executor is not implemented yet".to_string());
        }
    };

    // Stage 3: bind, self-register (with lease renewal), serve, cleanup.
    let server = WsServer::bind_loopback()
        .await
        .map_err(|e| format!("bind loopback ws server: {e}"))?;
    let endpoint = server.ws_endpoint();

    let fab = Arc::new(Fabric::at(&spec.fabric_dir));
    let record = AgentRecord {
        agent_id: spec.identity.child_id.clone(),
        role: spec.identity.role.clone(),
        labels: Vec::new(),
        endpoint,
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        started_at: Utc::now(),
        lease_expires_at: Utc::now() + ChronoDuration::seconds(60),
    };
    fab.publish(&record)
        .await
        .map_err(|e| format!("publish discovery record: {e}"))?;

    // Lease renewal: republish with a fresh expiry while we serve.
    let renew_fab = fab.clone();
    let mut renew_record = record.clone();
    let renew = tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(20));
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            renew_record.lease_expires_at = Utc::now() + ChronoDuration::seconds(60);
            if renew_fab.publish(&renew_record).await.is_err() {
                break;
            }
        }
    });

    // Reusable actors serve connection-after-connection so the parent can pool
    // and reuse them; one-shot children serve a single connection then exit. Both
    // exit on their own if left idle (orphan/idle defense) rather than lingering.
    let serve_result = if spec.reusable {
        let idle = std::time::Duration::from_secs(spec.limits.idle_timeout_secs.unwrap_or(300));
        server
            .serve_reusable_with_idle_timeout(executor, idle)
            .await
    } else {
        server
            .serve_one_with_accept_timeout(executor, std::time::Duration::from_secs(120))
            .await
    };
    renew.abort();
    let _ = fab.withdraw(&spec.identity.child_id).await;
    serve_result.map_err(|e| format!("serve: {e}"))
}

/// `ChildExecutor` backed by the real bamboo agent loop, assembled from a `ProvisionSpec`.
pub struct BambooRuntimeExecutor {
    agent: bamboo_engine::Agent,
    /// Same store the agent persists to, kept as the concrete type so steering
    /// can do a LOCKED read-modify-write (`update_runtime_config`) instead of
    /// an unlocked load+save that could revert a concurrent loop save.
    locked_store: Arc<LockedSessionStore>,
    model: Option<String>,
    workspace: Option<String>,
    disabled_tools: Option<BTreeSet<String>>,
    child_id: String,
}

impl BambooRuntimeExecutor {
    /// Assemble the isolated runtime: in-memory config + scoped credentials, provider,
    /// isolated storage/skills/metrics, builtin tools — never touching the user's
    /// `~/.bamboo` or persisting any secret.
    pub async fn build(spec: &ProvisionSpec) -> std::result::Result<Self, String> {
        let storage_dir = spec
            .storage_dir
            .clone()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                std::env::temp_dir()
                    .join("bamboo-subagents")
                    .join(&spec.identity.child_id)
            });
        tokio::fs::create_dir_all(&storage_dir)
            .await
            .map_err(|e| format!("create storage dir: {e}"))?;

        // Routing key: the resolved model's provider (may be a legacy name OR a
        // provider-instance id), else the credential's own key.
        let provider_key = spec
            .model
            .as_ref()
            .map(|m| m.provider.clone())
            .filter(|p| !p.trim().is_empty())
            .or_else(|| {
                spec.secrets
                    .provider_credentials
                    .first()
                    .map(|c| c.provider.clone())
            })
            .ok_or_else(|| {
                "provision spec carries neither model.provider nor a credential".to_string()
            })?;
        let cred = spec
            .secrets
            .provider_credentials
            .iter()
            .find(|c| c.provider == provider_key)
            .or_else(|| spec.secrets.provider_credentials.first());
        // Concrete protocol to construct: the credential's provider_type when the
        // routing key is an instance id; else the key itself ("anthropic", …).
        let factory_name = cred
            .and_then(|c| c.provider_type.clone())
            .filter(|t| !t.trim().is_empty())
            .unwrap_or_else(|| provider_key.clone());

        // In-memory config: exactly one provider slot, built from the envelope.
        // (Provider config structs are deserialized from a minimal JSON shape so this
        // code does not chase their full field lists.)
        let config = build_isolated_config(&factory_name, cred, spec)?;

        let provider = create_provider_by_name(&config, &factory_name, storage_dir.clone())
            .await
            .map_err(|e| format!("create provider '{factory_name}': {e}"))?;

        // Isolated storage / skills / metrics (all under storage_dir).
        let store = Arc::new(
            SessionStoreV2::new(storage_dir.clone())
                .await
                .map_err(|e| format!("init session store: {e}"))?,
        );
        let persistence = Arc::new(LockedSessionStore::new(store.clone()));
        let locked_store = persistence.clone();
        // Synced skills dir (orchestrator's user/project skills) when provided,
        // else the worker's isolated (empty) dir — unchanged for actor children.
        let skills_dir = spec
            .capabilities
            .skills_dir
            .clone()
            .map(PathBuf::from)
            .unwrap_or_else(|| storage_dir.join("skills"));
        let skill_manager = Arc::new(SkillManager::with_config(SkillStoreConfig {
            skills_dir,
            project_dir: spec.workspace.clone().map(PathBuf::from),
            active_mode: None,
        }));
        skill_manager
            .initialize()
            .await
            .map_err(|e| format!("init skill manager: {e}"))?;
        let metrics_storage: Arc<dyn bamboo_metrics::storage::MetricsStorage> =
            Arc::new(SqliteMetricsStorage::new(storage_dir.join("metrics.db")));
        let metrics_collector = MetricsCollector::spawn(metrics_storage, 90);

        let config = Arc::new(tokio::sync::RwLock::new(config));
        let builtin: Arc<dyn bamboo_agent_core::tools::ToolExecutor> = Arc::new(
            bamboo_tools::BuiltinToolExecutor::new_with_config(config.clone()),
        );
        // Compose orchestrator-synced MCP servers (the portable / URL subset) on
        // top of builtin tools when provided. Absent for actor children, so they
        // keep builtin-only behavior. A parse/connect failure degrades to builtin.
        let default_tools: Arc<dyn bamboo_agent_core::tools::ToolExecutor> =
            match spec.capabilities.mcp.as_ref() {
                Some(mcp_value) => {
                    match serde_json::from_value::<bamboo_domain::mcp_config::McpConfig>(
                        mcp_value.clone(),
                    ) {
                        Ok(mcp_config) => {
                            let mcp_manager =
                                Arc::new(bamboo_mcp::manager::McpServerManager::new_with_config(
                                    config.clone(),
                                ));
                            mcp_manager.initialize_from_config(&mcp_config).await;
                            let mcp_tools = Arc::new(bamboo_mcp::executor::McpToolExecutor::new(
                                mcp_manager.clone(),
                                mcp_manager.tool_index(),
                            ));
                            Arc::new(bamboo_mcp::executor::CompositeToolExecutor::new(
                                builtin, mcp_tools,
                            ))
                        }
                        Err(e) => {
                            tracing::warn!("ignoring synced MCP config (parse error): {e}");
                            builtin
                        }
                    }
                }
                None => builtin,
            };

        let agent = bamboo_engine::Agent::builder()
            .storage(store.clone())
            .persistence(persistence)
            .attachment_reader(store)
            .skill_manager(skill_manager)
            .metrics_collector(metrics_collector)
            .config(config)
            .provider(provider)
            .default_tools(default_tools)
            .build()
            .map_err(|e| format!("build agent runtime: {e}"))?;

        Ok(Self {
            agent,
            locked_store,
            model: spec.model.as_ref().map(|m| m.model.clone()),
            workspace: spec.workspace.clone(),
            disabled_tools: spec
                .disabled_tools
                .as_ref()
                .map(|v| v.iter().cloned().collect()),
            child_id: spec.identity.child_id.clone(),
        })
    }
}

#[async_trait]
impl ChildExecutor for BambooRuntimeExecutor {
    async fn run(
        &self,
        run: RunSpec,
        events: EventSink,
        mut steer: SteerInbox,
        cancel: CancellationToken,
    ) -> ChildOutcome {
        // Fresh session per run, in the worker's isolated store. When the parent
        // ships prior conversation (a reactivation: send_message/update/rerun),
        // rehydrate from it — the parent's store is the actor's durable state,
        // this process is just its activation. The run id is unique so a
        // long-running service agent can serve concurrent runs without
        // storage collisions (stateless-RPC semantics: one session per call).
        let mut session = Session::new(
            format!("{}-run-{}", self.child_id, uuid::Uuid::new_v4()),
            self.model.clone().unwrap_or_default(),
        );
        session.workspace = self.workspace.clone();
        let rehydrated: Vec<Message> = run
            .messages
            .iter()
            .filter_map(|v| serde_json::from_value::<Message>(v.clone()).ok())
            .collect();
        if rehydrated.is_empty() {
            session.add_message(Message::user(run.assignment.clone()));
        } else {
            session.messages = rehydrated;
            // Defensive: execution is driven by the last user message; if the
            // shipped history somehow lacks one, append the assignment.
            if !session
                .messages
                .iter()
                .any(|m| matches!(m.role, Role::User))
            {
                session.add_message(Message::user(run.assignment.clone()));
            }
        }
        bamboo_engine::session_app::execution_prep::prepare_session_for_execution(
            &mut session,
            None,
            self.model.as_deref(),
        );

        // Seed the worker's local store so mid-run steering can read-modify-write
        // the session's pending_injected_messages (the engine loop merges that
        // queue from storage at every round boundary).
        {
            let mut seed = session.clone();
            let _ = self
                .agent
                .persistence()
                .save_runtime_session(&mut seed)
                .await;
        }

        // In-band steering: each ParentFrame::Message lands in the local store's
        // pending queue; the running loop admits it at its next round boundary —
        // exactly the in-process mechanism, reused across the process boundary.
        let steer_store = self.locked_store.clone();
        let steer_session_id = session.id.clone();
        let steer_task = tokio::spawn(async move {
            while let Some(text) = steer.recv().await {
                // LOCKED read-modify-write: load + mutate + save all happen
                // under the per-session lock, so a concurrent loop save can
                // neither be reverted by this write nor revert it.
                let queued = steer_store
                    .update_runtime_config(&steer_session_id, |latest| {
                        let mut pending = latest.pending_injected_messages().unwrap_or_default();
                        pending.push(serde_json::json!({
                            "content": text,
                            "created_at": chrono::Utc::now(),
                        }));
                        latest.set_pending_injected_messages(pending);
                    })
                    .await;
                match queued {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        tracing::warn!("steer message dropped: session not found in worker store")
                    }
                    Err(e) => tracing::warn!("steer message could not be queued: {e}"),
                }
            }
        });

        // AgentEvents stream to the parent verbatim (zero mapping).
        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(256);
        let forward = tokio::spawn(async move {
            while let Some(ev) = event_rx.recv().await {
                if let Ok(value) = serde_json::to_value(&ev) {
                    events.emit(value);
                }
            }
        });

        let mut builder = bamboo_engine::ExecuteRequestBuilder::new(
            run.assignment.clone(),
            event_tx,
            cancel.clone(),
        );
        if let Some(model) = self.model.clone() {
            builder = builder.model(model);
        }
        if let Some(disabled) = self.disabled_tools.clone() {
            builder = builder.disabled_tools(disabled);
        }

        let result = self.agent.execute(&mut session, builder.build()).await;
        steer_task.abort();
        let _ = forward.await; // flush remaining events before the terminal frame

        match result {
            Ok(()) => {
                // The result text = the session's final assistant message.
                let text = session
                    .messages
                    .iter()
                    .rev()
                    .find(|m| matches!(m.role, Role::Assistant))
                    .map(|m| m.content.clone())
                    .unwrap_or_default();
                ChildOutcome::completed(text)
            }
            Err(AgentError::Cancelled) => ChildOutcome::cancelled(),
            Err(e) => ChildOutcome::error(e.to_string()),
        }
    }
}

/// Remove sibling actor storage directories whose last modification is older
/// than `retention`. Best-effort: errors are ignored (another worker may be
/// GC'ing concurrently); only directories directly under `root` are touched.
///
/// Liveness guard: a directory whose name matches a LIVE fabric record (lease
/// not expired) is never removed — dir mtime alone would misjudge a long-running
/// actor (>retention) as stale, because file writes inside subdirectories do
/// not bump the top-level directory's mtime.
async fn gc_stale_storage(root: PathBuf, retention: std::time::Duration) {
    let live_ids: std::collections::HashSet<String> = Fabric::at(&root)
        .discover()
        .await
        .map(|records| records.into_iter().map(|r| r.agent_id).collect())
        .unwrap_or_default();

    let Ok(mut rd) = tokio::fs::read_dir(&root).await else {
        return;
    };
    let now = std::time::SystemTime::now();
    while let Ok(Some(entry)) = rd.next_entry().await {
        let Ok(meta) = entry.metadata().await else {
            continue;
        };
        if !meta.is_dir() {
            continue;
        }
        if live_ids.contains(&entry.file_name().to_string_lossy().into_owned()) {
            continue; // live actor (renewing its lease) — never reap
        }
        let stale = meta
            .modified()
            .ok()
            .and_then(|m| now.duration_since(m).ok())
            .is_some_and(|age| age > retention);
        if stale {
            let _ = tokio::fs::remove_dir_all(entry.path()).await;
        }
    }
}

/// Build the worker's isolated, in-memory `Config`: one provider slot keyed by the
/// concrete protocol name (`factory_name`), populated from the scoped credential.
/// Never written to disk.
fn build_isolated_config(
    factory_name: &str,
    cred: Option<&bamboo_subagent::provision::ScopedCredential>,
    spec: &ProvisionSpec,
) -> std::result::Result<Config, String> {
    let mut slot = serde_json::Map::new();
    if let Some(cred) = cred {
        slot.insert("api_key".into(), cred.api_key.clone().into());
        if let Some(base_url) = &cred.base_url {
            slot.insert("base_url".into(), base_url.clone().into());
        }
    }
    if let Some(model) = &spec.model {
        slot.insert("model".into(), model.model.clone().into());
    }

    let value = serde_json::json!({
        "provider": factory_name,
        "providers": { factory_name: slot },
    });
    serde_json::from_value::<Config>(value)
        .map_err(|e| format!("assemble isolated config for '{factory_name}': {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_subagent::provision::{ChildIdentity, ModelRefSpec, ScopedCredential};

    fn spec_with(provider: &str, key: &str, model: Option<(&str, &str)>) -> ProvisionSpec {
        let mut s = ProvisionSpec::new(
            ChildIdentity {
                child_id: "c1".into(),
                parent_id: None,
                project_key: None,
                role: "worker".into(),
            },
            ExecutorSpec::BambooRuntime,
            "/tmp/fabric".into(),
        );
        s.secrets.provider_credentials.push(ScopedCredential {
            provider: provider.into(),
            api_key: key.into(),
            base_url: None,
            provider_type: None,
        });
        s.model = model.map(|(p, m)| ModelRefSpec {
            provider: p.into(),
            model: m.into(),
        });
        s
    }

    #[test]
    fn isolated_config_populates_the_provider_slot() {
        let spec = spec_with("anthropic", "sk-test", Some(("anthropic", "claude-test")));
        let config = build_isolated_config(
            "anthropic",
            spec.secrets.provider_credentials.first(),
            &spec,
        )
        .unwrap();
        assert_eq!(config.provider, "anthropic");
        let slot = config.providers.anthropic.expect("anthropic slot");
        assert_eq!(slot.api_key, "sk-test");
        assert_eq!(slot.model.as_deref(), Some("claude-test"));
    }

    #[test]
    fn isolated_config_works_for_openai_shape_too() {
        let spec = spec_with("openai", "sk-oa", Some(("openai", "gpt-test")));
        let config =
            build_isolated_config("openai", spec.secrets.provider_credentials.first(), &spec)
                .unwrap();
        assert_eq!(config.provider, "openai");
        let slot = config.providers.openai.expect("openai slot");
        assert_eq!(slot.api_key, "sk-oa");
    }
}
