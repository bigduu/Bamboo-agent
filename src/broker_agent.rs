//! `bamboo broker-agent serve`: a long-lived agent that connects to a central
//! message broker and answers Ask/Task for its own mailbox (query/steer modes).
//!
//! Location-independent: deploy it as a local subprocess, in a Docker container,
//! or on a remote host — it only needs `--broker <ws>` + `--token`. This is the
//! "deploy somewhere to do work / be asked" half of the broker topology; the
//! orchestrator reaches it purely by `--id` (its mailbox key), never caring where
//! it runs.

use std::sync::Arc;

use bamboo_subagent::provision::{ChildIdentity, ExecutorSpec, ModelRefSpec, ProvisionSpec};
use bamboo_subagent::{AgentRef, EchoExecutor};
use tokio_util::sync::CancellationToken;

use crate::subagent_worker::BambooRuntimeExecutor;

/// Install a graceful-stop signal handler and return the [`CancellationToken`]
/// it trips: SIGTERM (or ctrl_c, if SIGTERM can't be installed / on non-unix)
/// wired into [`bamboo_broker::serve_executor_with_shutdown`] so the worker
/// stops pulling new Ask/Task/Run work and drains whatever's in flight instead
/// of being hard-killed mid-answer when `deploy_agent action=stop` (or an
/// orchestrator exit) sends SIGTERM. #49.
fn install_shutdown_signal() -> CancellationToken {
    let token = CancellationToken::new();
    let tripped = token.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            match signal(SignalKind::terminate()) {
                Ok(mut term) => {
                    tokio::select! {
                        _ = term.recv() => {
                            tracing::info!("broker-agent: SIGTERM received — draining in-flight work before exit");
                        }
                        r = tokio::signal::ctrl_c() => {
                            if r.is_ok() {
                                tracing::info!("broker-agent: ctrl-c received — draining in-flight work before exit");
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "broker-agent: failed to install SIGTERM handler ({e}); falling back to ctrl-c only"
                    );
                    if tokio::signal::ctrl_c().await.is_ok() {
                        tracing::info!(
                            "broker-agent: ctrl-c received — draining in-flight work before exit"
                        );
                    }
                }
            }
        }
        #[cfg(not(unix))]
        {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::info!(
                    "broker-agent: ctrl-c received — draining in-flight work before exit"
                );
            }
        }
        tripped.cancel();
    });
    token
}

/// Parameters for `broker-agent serve`.
pub struct BrokerAgentArgs {
    /// Broker WebSocket endpoint, e.g. `ws://broker-host:9600`.
    pub broker: String,
    /// Bearer token presented in the broker handshake.
    pub token: String,
    /// This agent's mailbox key / session id — how the orchestrator addresses it.
    pub id: String,
    /// Optional role/profile label (published in the agent ref).
    pub role: Option<String>,
    /// Optional model `provider:model` (real mode only).
    pub model: Option<String>,
    /// Optional workspace dir for the agent's file tools (real mode only).
    pub workspace: Option<String>,
    /// Use the dependency-free `EchoExecutor` (no LLM) — smoke tests / wiring.
    pub echo: bool,
    /// When set, proxy all MCP tool calls to this orchestrator id over the broker
    /// (host-bound servers run only there) instead of syncing servers directly.
    pub mcp_proxy: Option<String>,
    /// Read a parent-resolved `ProvisionSpec` from stdin instead of self-resolving
    /// from local config — the orchestrator ships the same authoritative bootstrap
    /// a local subprocess worker gets (model/creds/MCP/identity/bus all decided by
    /// the parent). Unifies the deployed-actor bootstrap with the local one.
    pub spec_stdin: bool,
    /// Like `spec_stdin`, but read the spec from a FILE the orchestrator uploaded
    /// (the delivery a remote deployer uses — it SFTP/scp-uploads the spec next to
    /// the binary rather than piping a TTY'd stdin).
    pub spec_file: Option<String>,
}

/// Connect to the broker and serve until the connection drops or a graceful
/// stop signal (SIGTERM/ctrl_c) has been received and in-flight work drained.
pub async fn run(args: BrokerAgentArgs) -> Result<(), String> {
    // Graceful stop (#49): SIGTERM/ctrl_c trips this token; the serve loop then
    // stops pulling new work, finishes + replies to the in-flight Ask/Task/Run,
    // and returns — instead of the process dying mid-answer.
    let shutdown = install_shutdown_signal();

    // Parent-shipped bootstrap: read the authoritative ProvisionSpec the
    // orchestrator resolved (identity, bus, model, creds, MCP) — from an uploaded
    // file (remote deploy) or stdin (local). Unifies the deployed-actor path with
    // the local one; no self-resolution from this host's config.
    let piped_spec = if let Some(path) = &args.spec_file {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| format!("broker-agent: read spec file '{path}': {e}"))?;
        Some(
            serde_json::from_slice::<bamboo_subagent::ProvisionSpec>(&bytes)
                .map_err(|e| format!("broker-agent: parse spec file '{path}': {e}"))?,
        )
    } else if args.spec_stdin {
        Some(
            bamboo_subagent::ProvisionSpec::read_from_stdin()
                .await
                .map_err(|e| format!("broker-agent: read ProvisionSpec from stdin: {e}"))?,
        )
    } else {
        None
    };
    if let Some(spec) = piped_spec {
        let me = AgentRef {
            session_id: spec.identity.child_id.clone(),
            role: Some(spec.identity.role.clone()),
        };
        // The spec's bus is authoritative when present; fall back to the CLI flags.
        let (endpoint, token) = match &spec.bus {
            Some(bus) => (bus.endpoint.clone(), bus.token.clone()),
            None => (args.broker.clone(), args.token.clone()),
        };
        tracing::info!(id = %me.session_id, broker = %endpoint, "broker-agent serving from piped spec");
        let executor: Arc<dyn bamboo_subagent::ChildExecutor> = match spec.executor {
            ExecutorSpec::Echo => Arc::new(EchoExecutor),
            ExecutorSpec::ClaudeCode {
                ref binary,
                ref model,
                ref permission_mode,
                ref inherit_user_config,
                ref forward_env,
            } => Arc::new(crate::claude_code_executor::ClaudeCodeExecutor::new(
                binary.clone(),
                model.clone(),
                permission_mode.clone(),
                spec.workspace.clone(),
                Some(crate::claude_code_executor::resolve_claude_code_state_dir(
                    &spec.storage_dir,
                    &spec.identity.child_id,
                )),
                inherit_user_config.unwrap_or(false),
                forward_env.clone().unwrap_or_default(),
            )),
            _ => Arc::new(BambooRuntimeExecutor::build(&spec).await?),
        };
        return bamboo_broker::serve_executor_with_shutdown(
            &endpoint, me, &token, executor, shutdown,
        )
        .await
        .map_err(|e| format!("broker-agent (spec) failed: {e}"));
    }

    // Legacy self-resolve path: build the spec from local config + CLI args.
    let me = AgentRef {
        session_id: args.id.clone(),
        role: args.role.clone(),
    };
    tracing::info!(id = %args.id, broker = %args.broker, echo = args.echo, "broker-agent connecting");

    if args.echo {
        return bamboo_broker::serve_executor_with_shutdown(
            &args.broker,
            me,
            &args.token,
            Arc::new(EchoExecutor),
            shutdown,
        )
        .await
        .map_err(|e| format!("broker-agent (echo) failed: {e}"));
    }

    let spec = build_spec(&args)?;
    let executor = BambooRuntimeExecutor::build(&spec).await?;
    bamboo_broker::serve_executor_with_shutdown(
        &args.broker,
        me,
        &args.token,
        Arc::new(executor),
        shutdown,
    )
    .await
    .map_err(|e| format!("broker-agent failed: {e}"))
}

/// Build a `ProvisionSpec` from the local config + CLI args — the standalone
/// counterpart to the spec a subprocess worker is handed over stdin by a parent.
fn build_spec(args: &BrokerAgentArgs) -> Result<ProvisionSpec, String> {
    let config = bamboo_config::Config::new();
    let credentials =
        bamboo_engine::external_agents::runtime::extract_provider_credentials(&config);
    if credentials.is_empty() {
        return Err(
            "no provider credentials in local config (configure a provider, or use --echo)".into(),
        );
    }

    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: args.id.clone(),
            parent_id: None,
            project_key: None,
            role: args
                .role
                .clone()
                .unwrap_or_else(|| "general-purpose".into()),
            depth: 0,
        },
        ExecutorSpec::BambooRuntime,
        // #217: the persistent data-dir subagents home, not `env::temp_dir()`.
        bamboo_config::paths::subagents_dir()
            .join("broker-agents")
            .join(&args.id)
            .to_string_lossy()
            .into_owned(),
    );
    // Model precedence: explicit --model > config defaults.sub_agent > defaults.chat.
    // A deployed worker given no --model must still inherit the configured default,
    // otherwise its AgentLoopConfig has no model_name and every LLM call fails.
    let explicit_model = match args.model.as_deref() {
        Some(raw) => parse_model(raw)?,
        None => None,
    };
    spec.model = explicit_model.or_else(|| {
        config.defaults.as_ref().and_then(|defaults| {
            defaults
                .sub_agent
                .as_ref()
                .or(Some(&defaults.chat))
                .map(|r| ModelRefSpec {
                    provider: r.provider.clone(),
                    model: r.model.clone(),
                })
        })
    });
    spec.workspace = args.workspace.clone();
    spec.secrets.provider_credentials = credentials;

    // Sync the orchestrator's capabilities so the deployed worker matches its
    // toolset: the portable (URL-based) MCP servers + the user skills dir. This
    // reads THIS host's config — the same machine for a local deploy, or a
    // mounted bamboo home for Docker. (Host-bound stdio MCP is excluded here;
    // P2 will proxy it over the broker.)
    // MCP: proxy to the orchestrator (covers ALL MCP, incl. host-bound stdio)
    // when configured; otherwise sync the portable (URL) subset for direct use.
    if let Some(orchestrator) = &args.mcp_proxy {
        spec.capabilities.mcp_proxy = Some(bamboo_subagent::McpProxyConfig {
            orchestrator: orchestrator.clone(),
            endpoint: args.broker.clone(),
            token: args.token.clone(),
        });
    } else {
        let portable = portable_mcp(&config.mcp);
        if !portable.servers.is_empty() {
            spec.capabilities.mcp = serde_json::to_value(&portable).ok();
        }
    }
    let skills_dir = bamboo_config::paths::resolve_bamboo_dir().join("skills");
    if skills_dir.is_dir() {
        spec.capabilities.skills_dir = Some(skills_dir.to_string_lossy().into_owned());
    }

    // #73: a deployed broker-agent is definitionally unattended — no interactive
    // human to answer approvals. Mark it so that IF gating is ever enabled for
    // it, its (and its sub-agents') gated actions are model-reviewed locally
    // rather than hard-denied (host=None, reviewer=None).
    spec.capabilities.no_human_approver = true;

    // DELIBERATE DIVERGENCE (audited): unlike the actor child runner — which
    // hard-sets `enforce_permissions = true` because it owns the per-run
    // ApprovalProxy bridge — this path leaves it `false`. A broker-agent has NO
    // approval-delegation channel over the broker, so a permission gate here
    // would have nowhere to escalate (it would fail-closed and break legitimate
    // dangerous-but-needed ops). Flipping this on REQUIRES first wiring
    // approval-delegation over the broker; until then this is the intentional
    // (trusted-infra) posture, not an oversight.
    // spec.capabilities.enforce_permissions stays false — see provision.rs.

    Ok(spec)
}

/// The portable (URL-based: SSE / streamable-http) enabled MCP servers — the ones
/// a worker can connect to directly wherever it runs. Host-bound `stdio` servers
/// (a local binary, e.g. nova) are excluded; P2 proxies those over the broker.
fn portable_mcp(
    mcp: &bamboo_domain::mcp_config::McpConfig,
) -> bamboo_domain::mcp_config::McpConfig {
    use bamboo_domain::mcp_config::TransportConfig;
    let servers = mcp
        .servers
        .iter()
        .filter(|s| s.enabled && !matches!(s.transport, TransportConfig::Stdio(_)))
        .cloned()
        .collect();
    bamboo_domain::mcp_config::McpConfig {
        version: mcp.version,
        servers,
    }
}

/// Parse `--model provider:model`; a bare value leaves the provider empty
/// (resolved by the runtime against the configured default). Shared grammar
/// (#246): the `provider:model` / bare-model split and malformed-colon
/// validation live in [`crate::model_spec`], same as `-p -m` and
/// `actor run|serve -m`.
fn parse_model(s: &str) -> Result<Option<ModelRefSpec>, String> {
    let parsed = crate::model_spec::parse_model_spec(s).map_err(|e| format!("--model {e}"))?;
    Ok(parsed.map(|p| ModelRefSpec {
        provider: p.provider.unwrap_or_default(),
        model: p.model,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_model_handles_provider_pair_and_bare() {
        assert_eq!(
            parse_model("anthropic:claude-sonnet-4-6").unwrap(),
            Some(ModelRefSpec {
                provider: "anthropic".into(),
                model: "claude-sonnet-4-6".into()
            })
        );
        assert_eq!(
            parse_model("gpt-5").unwrap(),
            Some(ModelRefSpec {
                provider: String::new(),
                model: "gpt-5".into()
            })
        );
        assert_eq!(parse_model("   ").unwrap(), None);
    }

    /// A malformed `provider:` (empty half) is rejected — previously this
    /// silently produced a bogus empty-provider or empty-model spec; now it
    /// fails fast like `-p -m` / `actor run -m` already do (#246).
    #[test]
    fn parse_model_rejects_malformed_colon() {
        assert!(parse_model("openai:").is_err());
        assert!(parse_model(":gpt-4o").is_err());
    }

    #[test]
    fn portable_mcp_keeps_url_servers_drops_stdio_and_disabled() {
        // The security-relevant filter: never sync host-bound stdio servers (a
        // local binary) to a worker; keep only enabled URL-based servers.
        let mcp: bamboo_domain::mcp_config::McpConfig = serde_json::from_value(serde_json::json!({
            "version": 1,
            "servers": [
                { "id": "web",  "enabled": true,  "transport": { "type": "sse", "url": "https://w/sse" } },
                { "id": "nova", "enabled": true,  "transport": { "type": "stdio", "command": "nova" } },
                { "id": "off",  "enabled": false, "transport": { "type": "sse", "url": "https://o/sse" } },
            ]
        }))
        .expect("mcp config deserializes");

        let portable = portable_mcp(&mcp);
        let ids: Vec<_> = portable.servers.iter().map(|s| s.id.clone()).collect();
        assert_eq!(ids, vec!["web"]); // stdio (nova) and disabled (off) dropped
    }
}
