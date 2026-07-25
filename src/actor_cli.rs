//! `bamboo actor …` / `bamboo -p` — drive actors from the terminal.
//!
//! - `run`:   spawn an owned one-shot actor, give it a task, stream the output.
//! - `serve`: become a long-running Tier-1 **service agent** — announce into the
//!   discovery fabric and serve calls forever (stateless RPC: one isolated
//!   session per call, design §8).
//! - `list`:  show live fabric records (who is discoverable right now).
//! - `call`:  discover a service agent by id or role and send it a task.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};

use bamboo_llm::Config;
use bamboo_subagent::discovery::Fabric;
use bamboo_subagent::executor::{ChildExecutor, EchoExecutor};
use bamboo_subagent::fleet::spawn_worker;
use bamboo_subagent::proto::{AgentRecord, ChildFrame, ParentFrame, RunSpec, TerminalStatus};
use bamboo_subagent::provision::{
    ChildIdentity, ExecutorSpec, ModelRefSpec, ProvisionSpec, ScopedCredential,
};
use bamboo_subagent::transport::{ChildClient, WsServer};

use crate::subagent_worker::BambooRuntimeExecutor;

/// Default fabric directory shared by all local actors.
///
/// #217: lives under the persistent data dir (`~/.bamboo/subagents` by
/// default, or `BAMBOO_DATA_DIR`/`BAMBOO_WORKSPACE_ROOT`'s sibling) instead
/// of `env::temp_dir()`, so actor discovery/storage state survives reboots.
pub fn default_fabric_dir() -> PathBuf {
    bamboo_config::paths::subagents_dir()
}

pub struct ActorRunArgs {
    pub prompt: String,
    pub model: Option<String>,
    pub role: String,
    pub workspace: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub echo: bool,
    /// Print raw event JSON instead of pretty streaming.
    pub raw: bool,
}

pub struct ActorServeArgs {
    pub role: String,
    /// Stable agent id; defaults to `<role>-<short-uuid>`.
    pub id: Option<String>,
    pub model: Option<String>,
    pub workspace: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub echo: bool,
    /// Address to bind. `None` ⇒ loopback ephemeral port (current behavior).
    /// Pass `0.0.0.0:PORT` for a remotely-reachable worker
    /// (remote-actor-plan P1, #181).
    pub bind: Option<SocketAddr>,
    /// Terminate TLS (`wss://`). Requires `cert_file` + `key_file`.
    pub tls: bool,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    /// Bearer token the worker requires on the WS handshake. `None` ⇒ accept any
    /// (loopback default).
    pub token: Option<String>,
}

pub struct ActorCallArgs {
    /// Agent id (exact) or role (first live match) to call.
    pub agent: String,
    pub prompt: String,
    pub raw: bool,
}

// ---------------------------------------------------------------------------
// run — spawn an owned one-shot actor
// ---------------------------------------------------------------------------

pub async fn run(args: ActorRunArgs) -> Result<(), String> {
    let child_id = format!("cli-{}", uuid::Uuid::new_v4());
    let spec = prepare_spec(
        &child_id,
        &args.role,
        &args.model,
        &args.workspace,
        &args.data_dir,
        args.echo,
    )?;

    let worker_bin =
        std::env::current_exe().map_err(|e| format!("cannot locate own executable: {e}"))?;
    eprintln!(
        "▶ spawning actor {child_id} (model: {}, executor: {})",
        describe_model(&spec),
        if args.echo { "echo" } else { "bamboo_runtime" },
    );

    let spawned = spawn_worker(
        &worker_bin,
        &["subagent-worker".to_string()],
        &spec,
        Duration::from_secs(30),
    )
    .await
    .map_err(|e| format!("spawn/register failed: {e}"))?;
    eprintln!(
        "✔ actor registered (pid {}, endpoint {})",
        spawned.record.pid, spawned.record.endpoint
    );

    let exit = connect_and_stream(&spawned.record.endpoint, &args.prompt, args.raw).await;
    spawned.kill().await;
    exit
}

// ---------------------------------------------------------------------------
// serve — long-running Tier-1 service agent
// ---------------------------------------------------------------------------

pub async fn serve(args: ActorServeArgs) -> Result<(), String> {
    let agent_id = args
        .id
        .clone()
        .unwrap_or_else(|| format!("{}-{}", args.role, &uuid::Uuid::new_v4().to_string()[..8]));
    let spec = prepare_spec(
        &agent_id,
        &args.role,
        &args.model,
        &args.workspace,
        &args.data_dir,
        args.echo,
    )?;

    let executor: std::sync::Arc<dyn ChildExecutor> = if args.echo {
        std::sync::Arc::new(EchoExecutor)
    } else {
        std::sync::Arc::new(BambooRuntimeExecutor::build(&spec).await?)
    };

    // Bind: TLS > explicit --bind (+ optional token) > loopback default.
    // Default (no flags) is byte-for-byte the historical loopback behavior.
    let server = if args.tls {
        let (cert, key) = match (&args.cert_file, &args.key_file) {
            (Some(c), Some(k)) => (c, k),
            _ => return Err("--tls requires both --cert-file and --key-file".to_string()),
        };
        let bind_addr = args
            .bind
            .unwrap_or_else(|| (std::net::Ipv4Addr::UNSPECIFIED, 8443).into());
        WsServer::bind_tls(bind_addr, cert, key, args.token.clone())
            .await
            .map_err(|e| format!("bind_tls: {e}"))?
    } else if let Some(bind_addr) = args.bind {
        // A bearer token on a non-loopback PLAINTEXT bind would cross the wire in
        // the clear, defeating its purpose. Refuse it: tokens require --tls on a
        // public bind. (A loopback plaintext bind with a token is allowed for
        // local/testing use.)
        if args.token.is_some() && !bind_addr.ip().is_loopback() {
            return Err(format!(
                "refusing --token on a non-loopback plaintext bind ({bind_addr}): the token would \
                 be sent in cleartext. Use --tls (with --cert-file/--key-file) for a public bind."
            ));
        }
        WsServer::bind_with_token(bind_addr, args.token.clone())
            .await
            .map_err(|e| format!("bind: {e}"))?
    } else {
        // Unchanged loopback default (no TLS, no token).
        WsServer::bind_loopback()
            .await
            .map_err(|e| format!("bind: {e}"))?
    };
    let endpoint = server.ws_endpoint();

    let fab = std::sync::Arc::new(Fabric::at(&spec.fabric_dir));
    let _ = fab.gc().await; // housekeeping: drop expired records
    let record = AgentRecord {
        agent_id: agent_id.clone(),
        role: args.role.clone(),
        labels: Vec::new(),
        endpoint: endpoint.clone(),
        pid: std::process::id(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        started_at: Utc::now(),
        lease_expires_at: Utc::now() + ChronoDuration::seconds(60),
    };
    fab.publish(&record)
        .await
        .map_err(|e| format!("announce: {e}"))?;

    // Lease renewal while serving.
    let renew_fab = fab.clone();
    let mut renew_record = record.clone();
    let renew = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(20));
        tick.tick().await;
        loop {
            tick.tick().await;
            renew_record.lease_expires_at = Utc::now() + ChronoDuration::seconds(60);
            if renew_fab.publish(&renew_record).await.is_err() {
                break;
            }
        }
    });

    eprintln!(
        "✔ service agent '{agent_id}' (role: {}) announced at {endpoint}",
        args.role
    );
    eprintln!("  serving until Ctrl-C — call it with: bamboo actor call {agent_id} \"<task>\"");

    // Serve forever; Ctrl-C withdraws the record and exits cleanly.
    let result = tokio::select! {
        r = server.serve(executor) => r.map_err(|e| format!("serve: {e}")),
        _ = tokio::signal::ctrl_c() => Ok(()),
    };
    renew.abort();
    let _ = fab.withdraw(&agent_id).await;
    eprintln!("⏹ service agent '{agent_id}' withdrawn");
    result
}

// ---------------------------------------------------------------------------
// list — discoverable actors right now
// ---------------------------------------------------------------------------

pub async fn list() -> Result<(), String> {
    let fab = Fabric::at(default_fabric_dir());
    let _ = fab.gc().await;
    let records = fab.discover().await.map_err(|e| format!("discover: {e}"))?;
    if records.is_empty() {
        println!(
            "no live actors (fabric: {})",
            default_fabric_dir().display()
        );
        return Ok(());
    }
    println!("{:<28} {:<12} {:<8} ENDPOINT", "AGENT", "ROLE", "PID");
    for r in records {
        println!(
            "{:<28} {:<12} {:<8} {}",
            r.agent_id, r.role, r.pid, r.endpoint
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// call — discover + invoke a service agent
// ---------------------------------------------------------------------------

pub async fn call(args: ActorCallArgs) -> Result<(), String> {
    let fab = Fabric::at(default_fabric_dir());
    let record = match fab
        .resolve(&args.agent)
        .await
        .map_err(|e| format!("resolve: {e}"))?
    {
        Some(r) => r,
        None => {
            // Fall back to role match: first live agent with this role.
            fab.discover()
                .await
                .map_err(|e| format!("discover: {e}"))?
                .into_iter()
                .find(|r| r.role == args.agent)
                .ok_or_else(|| {
                    format!(
                        "no live actor with id or role '{}'; see `bamboo actor list`",
                        args.agent
                    )
                })?
        }
    };
    eprintln!(
        "▶ calling {} (role: {}, endpoint {})",
        record.agent_id, record.role, record.endpoint
    );
    connect_and_stream(&record.endpoint, &args.prompt, args.raw).await
}

// ---------------------------------------------------------------------------
// shared plumbing
// ---------------------------------------------------------------------------

/// Connect to an actor endpoint, dispatch a run, stream until terminal.
/// Ctrl-C sends the out-of-band cancel.
async fn connect_and_stream(endpoint: &str, prompt: &str, raw: bool) -> Result<(), String> {
    let mut client = ChildClient::connect(endpoint)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    client
        .send(ParentFrame::Run(RunSpec {
            assignment: prompt.to_string(),
            logical_session: None,
            project_id: None,
            reasoning_effort: None,
            permission_policy: None,
            messages: Vec::new(),
            activation_run_id: None,
            initial_session_messages: Vec::new(),
            secrets: Default::default(),
        }))
        .await
        .map_err(|e| format!("dispatch failed: {e}"))?;

    let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = cancel_tx.send(()).await;
        }
    });

    let mut exit: Result<(), String> = Ok(());
    let mut streamed_tokens = false;
    loop {
        tokio::select! {
            _ = cancel_rx.recv() => {
                eprintln!("\n⏹ cancelling…");
                let _ = client.send(ParentFrame::Cancel).await;
            }
            frame = client.next_frame() => {
                match frame {
                    Ok(Some(ChildFrame::Event { event })) => {
                        if event["type"] == "token" {
                            streamed_tokens = true;
                        }
                        print_event(&event, raw);
                    }
                    Ok(Some(ChildFrame::ApprovalRequest { .. })) => {
                        // This CLI does not route gated-tool approvals; ignore.
                        // (The production host in actor_adapter answers these.)
                    }
                    Ok(Some(ChildFrame::SessionMessageAdmitted { .. })) => {
                        // The standalone CLI never forwards canonical SessionInbox
                        // claims, so no confirmation is expected here.
                    }
                    Ok(Some(ChildFrame::Terminal { status, result, error, .. })) => {
                        println!();
                        match status {
                            TerminalStatus::Completed => {
                                eprintln!("✔ completed");
                                if !streamed_tokens {
                                    if let Some(r) = result {
                                        println!("{r}");
                                    }
                                }
                            }
                            TerminalStatus::Cancelled => eprintln!("⏹ cancelled"),
                            TerminalStatus::Suspended => eprintln!("⏸ suspended (waiting on sub-agents)"),
                            TerminalStatus::Error => {
                                exit = Err(error.unwrap_or_else(|| "actor errored".into()));
                            }
                        }
                        break;
                    }
                    Ok(None) => {
                        exit = Err("connection closed before terminal".into());
                        break;
                    }
                    Err(e) => {
                        exit = Err(format!("transport error: {e}"));
                        break;
                    }
                }
            }
        }
    }

    let _ = client.close().await;
    exit
}

/// Resolve config + model + credential into a ProvisionSpec for a local actor.
fn prepare_spec(
    child_id: &str,
    role: &str,
    model_arg: &Option<String>,
    workspace: &Option<PathBuf>,
    data_dir: &Option<PathBuf>,
    echo: bool,
) -> Result<ProvisionSpec, String> {
    let data_dir = data_dir
        .clone()
        .unwrap_or_else(bamboo_config::paths::resolve_bamboo_dir);
    // Loads config.json and hydrates encrypted api keys into memory.
    let config = Config::from_data_dir(Some(data_dir.clone()));
    let credentials =
        bamboo_engine::external_agents::runtime::extract_provider_credentials(&config);

    let model = resolve_model(model_arg, &config)?;
    if !echo && model.is_none() {
        return Err(
            "no model resolved: pass --model provider:model or configure defaults.sub_agent/chat"
                .to_string(),
        );
    }

    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: child_id.to_string(),
            parent_id: None,
            project_key: None,
            role: role.to_string(),
            depth: 0,
        },
        if echo {
            ExecutorSpec::Echo
        } else {
            ExecutorSpec::BambooRuntime
        },
        default_fabric_dir().to_string_lossy().into_owned(),
    );
    spec.workspace = workspace
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .map(|w| w.to_string_lossy().into_owned());
    spec.model = model.clone();
    if let Some(m) = &model {
        if let Some(cred) = pick_credential(&credentials, &m.provider) {
            spec.secrets.provider_credentials.push(cred);
        } else if !echo {
            return Err(format!(
                "no credential found for provider '{}' in {}",
                m.provider,
                data_dir.display()
            ));
        }
    }
    Ok(spec)
}

fn describe_model(spec: &ProvisionSpec) -> String {
    spec.model
        .as_ref()
        .map(|m| format!("{}:{}", m.provider, m.model))
        .unwrap_or_else(|| "-".into())
}

fn print_event(event: &serde_json::Value, raw: bool) {
    use std::io::Write;
    if raw {
        println!("{event}");
        return;
    }
    match event["type"].as_str().unwrap_or("") {
        "token" => {
            print!("{}", event["content"].as_str().unwrap_or(""));
            let _ = std::io::stdout().flush();
        }
        "reasoning_token" => { /* keep terse */ }
        "tool_start" => {
            eprintln!("\n⚙ {}", event["tool_name"].as_str().unwrap_or("tool"));
        }
        "tool_complete" => eprintln!("✔ tool done"),
        "tool_error" => eprintln!("✘ tool error: {}", event["error"].as_str().unwrap_or("")),
        "error" => eprintln!("✘ {}", event["message"].as_str().unwrap_or("")),
        _ => {}
    }
}

/// `--model provider:model` (or bare model on the default provider) >
/// `defaults.sub_agent` > `defaults.chat`. Shared grammar (#246): the
/// `provider:model` / bare-model split lives in [`crate::model_spec`], same
/// as `-p -m` and `broker-agent spawn --model`.
fn resolve_model(
    explicit: &Option<String>,
    config: &Config,
) -> Result<Option<ModelRefSpec>, String> {
    if let Some(raw) = explicit {
        if let Some(parsed) =
            crate::model_spec::parse_model_spec(raw).map_err(|e| format!("--model {e}"))?
        {
            let provider = parsed.provider.unwrap_or_else(|| config.provider.clone());
            return Ok(Some(ModelRefSpec {
                provider,
                model: parsed.model,
            }));
        }
    }
    if let Some(defaults) = &config.defaults {
        let pick = defaults.sub_agent.as_ref().or(Some(&defaults.chat));
        if let Some(r) = pick {
            return Ok(Some(ModelRefSpec {
                provider: r.provider.clone(),
                model: r.model.clone(),
            }));
        }
    }
    Ok(None)
}

fn pick_credential(creds: &[ScopedCredential], provider: &str) -> Option<ScopedCredential> {
    creds.iter().find(|c| c.provider == provider).cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn some(s: &str) -> Option<String> {
        Some(s.to_string())
    }

    /// `--model provider:model` resolves to that exact pair, independent of
    /// the configured default provider.
    #[test]
    fn resolve_model_colon_form() {
        let mut config = Config::default();
        config.provider = "anthropic".into();
        let m = resolve_model(&some("openai:gpt-4o"), &config)
            .unwrap()
            .unwrap();
        assert_eq!(m.provider, "openai");
        assert_eq!(m.model, "gpt-4o");
    }

    /// A bare `--model <id>` binds to the config's default provider — same
    /// grammar `bamboo -p -m` uses (#246).
    #[test]
    fn resolve_model_bare_uses_config_default_provider() {
        let mut config = Config::default();
        config.provider = "openai".into();
        let m = resolve_model(&some("gpt-4o"), &config).unwrap().unwrap();
        assert_eq!(m.provider, "openai");
        assert_eq!(m.model, "gpt-4o");
    }

    fn defaults_config(
        chat: (&str, &str),
        sub_agent: Option<(&str, &str)>,
    ) -> bamboo_config::DefaultsConfig {
        bamboo_config::DefaultsConfig {
            chat: bamboo_domain::ProviderModelRef::new(chat.0, chat.1),
            fast: None,
            task_summary: None,
            vision: None,
            memory_background: None,
            planning: None,
            search: None,
            code_review: None,
            sub_agent: sub_agent.map(|(p, m)| bamboo_domain::ProviderModelRef::new(p, m)),
            subagent_models: std::collections::HashMap::new(),
        }
    }

    /// No `--model` falls back to `defaults.sub_agent`, then `defaults.chat`.
    #[test]
    fn resolve_model_falls_back_to_defaults_sub_agent() {
        let mut config = Config::default();
        config.defaults = Some(defaults_config(
            ("anthropic", "claude-x"),
            Some(("openai", "gpt-sub")),
        ));
        let m = resolve_model(&None, &config).unwrap().unwrap();
        assert_eq!(m.provider, "openai");
        assert_eq!(m.model, "gpt-sub");
    }

    /// No `--model` and no `defaults.sub_agent` falls back to `defaults.chat`.
    #[test]
    fn resolve_model_falls_back_to_defaults_chat() {
        let mut config = Config::default();
        config.defaults = Some(defaults_config(("anthropic", "claude-x"), None));
        let m = resolve_model(&None, &config).unwrap().unwrap();
        assert_eq!(m.provider, "anthropic");
        assert_eq!(m.model, "claude-x");
    }

    /// Neither `--model` nor `defaults` configured → `None` (caller decides
    /// whether that's fatal).
    #[test]
    fn resolve_model_none_when_nothing_configured() {
        let mut config = Config::default();
        config.defaults = None;
        assert_eq!(resolve_model(&None, &config).unwrap(), None);
    }

    /// A malformed `provider:` (empty half) is rejected — same grammar
    /// `bamboo -p -m` enforces (#246), not silently treated as a bare model.
    #[test]
    fn resolve_model_malformed_colon_errors() {
        let config = Config::default();
        assert!(resolve_model(&some("openai:"), &config).is_err());
        assert!(resolve_model(&some(":gpt-4o"), &config).is_err());
    }
}
