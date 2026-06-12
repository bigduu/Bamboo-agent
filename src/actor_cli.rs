//! `bamboo actor run` — drive a sub-agent actor from the terminal.
//!
//! Spawns the exact production chain (worker process + stdin ProvisionSpec + fabric
//! self-register + WebSocket) against the user's real config, streaming events live:
//!
//! ```text
//! bamboo actor run "Summarize this repo"            # real LLM via your config
//! bamboo actor run --echo "ping"                    # dependency-free smoke run
//! bamboo actor run --model <provider:model> "..."   # pin the model
//! ```

use std::path::PathBuf;
use std::time::Duration;

use bamboo_llm::Config;
use bamboo_subagent::fleet::spawn_worker;
use bamboo_subagent::proto::{ChildFrame, ParentFrame, RunSpec, TerminalStatus};
use bamboo_subagent::provision::{
    ChildIdentity, ExecutorSpec, ModelRefSpec, ProvisionSpec, ScopedCredential,
};
use bamboo_subagent::transport::ChildClient;

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

pub async fn run(args: ActorRunArgs) -> Result<(), String> {
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(bamboo_config::paths::resolve_bamboo_dir);
    // Loads config.json and hydrates encrypted api keys into memory.
    let config = Config::from_data_dir(Some(data_dir.clone()));

    let credentials = bamboo_engine::external_agents::runtime::extract_provider_credentials(&config);

    // Resolve the model: explicit --model > defaults.sub_agent > defaults.chat.
    let model = resolve_model(&args.model, &config)?;

    let executor = if args.echo {
        ExecutorSpec::Echo
    } else {
        ExecutorSpec::BambooRuntime
    };
    if !args.echo && model.is_none() {
        return Err(
            "no model resolved: pass --model provider:model or configure defaults.sub_agent/chat"
                .to_string(),
        );
    }

    let child_id = format!("cli-{}", uuid::Uuid::new_v4());
    let fabric_dir = std::env::temp_dir().join("bamboo-subagents");
    let workspace = args
        .workspace
        .clone()
        .or_else(|| std::env::current_dir().ok());

    let mut spec = ProvisionSpec::new(
        ChildIdentity {
            child_id: child_id.clone(),
            parent_id: None,
            project_key: None,
            role: args.role.clone(),
        },
        executor,
        fabric_dir.to_string_lossy().into_owned(),
    );
    spec.workspace = workspace.map(|w| w.to_string_lossy().into_owned());
    spec.model = model.clone();
    // Least-privilege: ship only the credential for the resolved provider.
    if let Some(m) = &model {
        if let Some(cred) = pick_credential(&credentials, &m.provider) {
            spec.secrets.provider_credentials.push(cred);
        } else if !args.echo {
            return Err(format!(
                "no credential found for provider '{}' in {}",
                m.provider,
                data_dir.display()
            ));
        }
    }

    let worker_bin =
        std::env::current_exe().map_err(|e| format!("cannot locate own executable: {e}"))?;
    eprintln!(
        "▶ spawning actor {child_id} (model: {}, executor: {})",
        model
            .as_ref()
            .map(|m| format!("{}:{}", m.provider, m.model))
            .unwrap_or_else(|| "-".into()),
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

    let mut client = ChildClient::connect(&spawned.record.endpoint)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    client
        .send(ParentFrame::Run(RunSpec {
            assignment: args.prompt.clone(),
            reasoning_effort: None,
            messages: Vec::new(),
        }))
        .await
        .map_err(|e| format!("dispatch failed: {e}"))?;

    // Ctrl-C -> out-of-band cancel frame.
    let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel::<()>(1);
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = cancel_tx.send(()).await;
        }
    });

    let mut exit: Result<(), String> = Ok(());
    loop {
        tokio::select! {
            _ = cancel_rx.recv() => {
                eprintln!("\n⏹ cancelling…");
                let _ = client.send(ParentFrame::Cancel).await;
            }
            frame = client.next_frame() => {
                match frame {
                    Ok(Some(ChildFrame::Event { event })) => print_event(&event, args.raw),
                    Ok(Some(ChildFrame::Terminal { status, result, error })) => {
                        println!();
                        match status {
                            TerminalStatus::Completed => {
                                eprintln!("✔ completed");
                                if let Some(r) = result {
                                    println!("{r}");
                                }
                            }
                            TerminalStatus::Cancelled => {
                                eprintln!("⏹ cancelled");
                            }
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
    spawned.kill().await;
    exit
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
/// `defaults.sub_agent` > `defaults.chat`.
fn resolve_model(
    explicit: &Option<String>,
    config: &Config,
) -> Result<Option<ModelRefSpec>, String> {
    if let Some(spec) = explicit {
        let spec = spec.trim();
        if let Some((p, m)) = spec.split_once(':') {
            if p.trim().is_empty() || m.trim().is_empty() {
                return Err(format!("--model '{spec}' must be provider:model"));
            }
            return Ok(Some(ModelRefSpec {
                provider: p.trim().into(),
                model: m.trim().into(),
            }));
        }
        return Ok(Some(ModelRefSpec {
            provider: config.provider.clone(),
            model: spec.into(),
        }));
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
