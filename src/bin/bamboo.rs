//! Bamboo binary entry point
//!
//! Standalone HTTP server for Bamboo

use bamboo_llm::Config;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bamboo")]
#[command(about = "A fully self-contained AI agent backend framework", long_about = None)]
#[command(
    after_help = "QUICK RUN:\n  bamboo -p \"your task\"          run an agent on the prompt and print the result\n  bamboo -p \"ping\" --echo        no-key smoke test of the actor chain\n  bamboo -p \"...\" -m provider:model   pin the model"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Run an agent on this prompt and print the result (headless one-shot;
    /// spawns an isolated actor process and streams its output).
    #[arg(short = 'p', long = "prompt", global = false)]
    prompt: Option<String>,

    /// With -p: model as 'provider:model' (or a bare model id on the default
    /// provider). Defaults to defaults.sub_agent, then defaults.chat.
    #[arg(short = 'm', long)]
    model: Option<String>,

    /// With -p: working directory for the agent (defaults to the current dir).
    #[arg(long)]
    workspace: Option<PathBuf>,

    /// With -p: data directory holding config.json (defaults to ~/.bamboo).
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// With -p: use the dependency-free echo executor (no LLM, no key) to
    /// smoke-test the actor chain.
    #[arg(long)]
    echo: bool,

    /// With -p: print raw event JSON instead of pretty streaming.
    #[arg(long)]
    raw: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the Bamboo HTTP server
    Serve {
        /// Port to listen on (overrides config file)
        #[arg(short, long)]
        port: Option<u16>,

        /// Bind address (overrides config file)
        #[arg(short, long)]
        bind: Option<String>,

        /// Data directory (overrides config file)
        #[arg(short, long)]
        data_dir: Option<PathBuf>,

        /// Static files directory (for Docker mode)
        #[arg(short, long)]
        static_dir: Option<PathBuf>,

        /// Number of worker threads (overrides config file)
        #[arg(short, long)]
        workers: Option<usize>,
    },

    /// Show Bamboo configuration
    Config {
        /// Show config file path
        #[arg(short, long)]
        path: bool,

        /// Show sensitive values (API keys, etc.)
        #[arg(long)]
        show_secrets: bool,
    },

    /// Run as a sub-agent worker process (spawned by a parent bamboo server).
    ///
    /// Reads a ProvisionSpec JSON document from stdin (the parent writes it and
    /// closes the pipe), self-registers into the discovery fabric, and serves
    /// one run over a loopback WebSocket. Not intended for interactive use.
    #[command(name = "subagent-worker", hide = true)]
    SubagentWorker,

    /// Run a sub-agent actor from the terminal (spawns the real worker
    /// process + WebSocket chain against your configured providers).
    Actor {
        #[command(subcommand)]
        command: ActorCommands,
    },
}

#[derive(Subcommand)]
enum ActorCommands {
    /// Spawn an actor, give it a task, and stream its events live.
    Run {
        /// The task / prompt for the actor.
        prompt: String,

        /// Model as 'provider:model' (or a bare model id on the default
        /// provider). Defaults to defaults.sub_agent, then defaults.chat.
        #[arg(short, long)]
        model: Option<String>,

        /// Role label published in the discovery record.
        #[arg(long, default_value = "cli")]
        role: String,

        /// Working directory for the actor (defaults to the current dir).
        #[arg(short, long)]
        workspace: Option<PathBuf>,

        /// Data directory holding config.json (defaults to ~/.bamboo).
        #[arg(short, long)]
        data_dir: Option<PathBuf>,

        /// Use the dependency-free echo executor (no LLM, no key needed) to
        /// smoke-test the whole actor chain.
        #[arg(long)]
        echo: bool,

        /// Print raw event JSON instead of pretty streaming.
        #[arg(long)]
        raw: bool,
    },

    /// Become a long-running service agent: announce into the local discovery
    /// fabric and serve calls until Ctrl-C (one isolated session per call).
    Serve {
        /// Role to announce (how others find you), e.g. "summarizer".
        #[arg(long, default_value = "service")]
        role: String,

        /// Stable agent id; defaults to '<role>-<short-uuid>'.
        #[arg(long)]
        id: Option<String>,

        /// Model as 'provider:model'; defaults to defaults.sub_agent/chat.
        #[arg(short, long)]
        model: Option<String>,

        /// Working directory for the agent (defaults to the current dir).
        #[arg(short, long)]
        workspace: Option<PathBuf>,

        /// Data directory holding config.json (defaults to ~/.bamboo).
        #[arg(short, long)]
        data_dir: Option<PathBuf>,

        /// Serve the echo executor (no LLM) — for smoke tests.
        #[arg(long)]
        echo: bool,
    },

    /// List actors currently discoverable in the local fabric.
    List,

    /// Discover a service agent (by id, or first match by role) and send it a task.
    Call {
        /// Agent id (exact) or role (first live match).
        agent: String,

        /// The task / prompt to send.
        prompt: String,

        /// Print raw event JSON instead of pretty streaming.
        #[arg(long)]
        raw: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logging (file + stdout, with rotation).
    // Use debug level in debug builds, info in release.
    let debug = cfg!(debug_assertions);
    match &cli.command {
        Some(Commands::Serve { data_dir, .. }) => {
            let home = data_dir
                .clone()
                .unwrap_or_else(bamboo_config::paths::resolve_bamboo_dir);
            bamboo_agent::server::logging::init_logging_with_home(&home, debug);
        }
        Some(Commands::Config { .. }) => {
            // No file logging for config subcommand; stdout only.
            tracing_subscriber::fmt()
                .with_target(true)
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                )
                .init();
        }
        Some(Commands::SubagentWorker) | Some(Commands::Actor { .. }) | None => {
            // Worker/CLI logs go to stderr only: stdin/stdout are part of the
            // bootstrap & streaming protocol and must stay clean. (`None` is
            // the top-level `-p` quick run, or bare `bamboo` -> help.)
            tracing_subscriber::fmt()
                .with_writer(std::io::stderr)
                .with_target(true)
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
                )
                .init();
        }
    }

    // Top-level quick run: `bamboo -p "<prompt>"` — headless one-shot agent.
    let command = match cli.command {
        Some(command) => command,
        None => {
            let Some(prompt) = cli.prompt else {
                use clap::CommandFactory;
                let _ = Cli::command().print_help();
                std::process::exit(2);
            };
            let args = bamboo_agent::actor_cli::ActorRunArgs {
                prompt,
                model: cli.model,
                role: "cli".to_string(),
                workspace: cli.workspace,
                data_dir: cli.data_dir,
                echo: cli.echo,
                raw: cli.raw,
            };
            if let Err(e) = bamboo_agent::actor_cli::run(args).await {
                eprintln!("run failed: {e}");
                std::process::exit(1);
            }
            return;
        }
    };

    match command {
        Commands::Serve {
            port,
            bind,
            data_dir,
            static_dir,
            workers,
        } => {
            let bamboo_home_dir = data_dir
                .clone()
                .unwrap_or_else(bamboo_config::paths::resolve_bamboo_dir);
            // Stabilize the data dir for the lifetime of this process.
            bamboo_config::paths::init_bamboo_dir(bamboo_home_dir.clone());
            // Keep runtime path resolution consistent: most helpers derive their base dir from
            // BAMBOO_DATA_DIR / `${HOME}/.bamboo` via `core::paths::bamboo_dir()`.
            // SAFETY: Called on the main thread before any async runtime work begins,
            // so no concurrent reads of the env are possible.
            unsafe {
                std::env::set_var("BAMBOO_DATA_DIR", bamboo_home_dir.as_os_str());
            }

            // Load config (with env var overrides already applied)
            // If --data-dir is specified, load from that directory.
            let mut config =
                bamboo_llm::Config::from_data_dir(Some(bamboo_home_dir.clone()));

            // Apply CLI argument overrides (highest priority)
            if let Some(p) = port {
                config.server.port = p;
            }
            if let Some(b) = bind {
                config.server.bind = b;
            }
            if let Some(s) = static_dir {
                config.server.static_dir = Some(s);
            }
            if let Some(w) = workers {
                config.server.workers = w;
            }

            // Map config-level worker count into an env var that the server entrypoints can
            // consume without requiring breaking signature changes.
            // SAFETY: Still on the main thread before async work begins.
            if workers.is_some() || std::env::var("BAMBOO_WORKERS").is_err() {
                unsafe {
                    std::env::set_var("BAMBOO_WORKERS", config.server.workers.to_string());
                }
            }

            // Start server using the unified config
            println!("Starting Bamboo server at {}", config.server_addr());
            let result = if config.server.static_dir.is_some() {
                bamboo_agent::server::run_with_bind_and_static(
                    bamboo_home_dir,
                    config.server.port,
                    &config.server.bind,
                    config.server.static_dir.clone(),
                )
                .await
            } else {
                bamboo_agent::server::run_with_bind(
                    bamboo_home_dir,
                    config.server.port,
                    &config.server.bind,
                )
                .await
            };

            if let Err(e) = result {
                eprintln!("Failed to start server: {}", e);
                std::process::exit(1);
            }
        }

        Commands::SubagentWorker => {
            if let Err(e) = bamboo_agent::subagent_worker::run().await {
                eprintln!("subagent-worker failed: {e}");
                std::process::exit(1);
            }
        }

        Commands::Actor { command } => {
            let result = match command {
                ActorCommands::Run {
                    prompt,
                    model,
                    role,
                    workspace,
                    data_dir,
                    echo,
                    raw,
                } => {
                    bamboo_agent::actor_cli::run(bamboo_agent::actor_cli::ActorRunArgs {
                        prompt,
                        model,
                        role,
                        workspace,
                        data_dir,
                        echo,
                        raw,
                    })
                    .await
                }
                ActorCommands::Serve {
                    role,
                    id,
                    model,
                    workspace,
                    data_dir,
                    echo,
                } => {
                    bamboo_agent::actor_cli::serve(bamboo_agent::actor_cli::ActorServeArgs {
                        role,
                        id,
                        model,
                        workspace,
                        data_dir,
                        echo,
                    })
                    .await
                }
                ActorCommands::List => bamboo_agent::actor_cli::list().await,
                ActorCommands::Call { agent, prompt, raw } => {
                    bamboo_agent::actor_cli::call(bamboo_agent::actor_cli::ActorCallArgs {
                        agent,
                        prompt,
                        raw,
                    })
                    .await
                }
            };
            if let Err(e) = result {
                eprintln!("actor command failed: {e}");
                std::process::exit(1);
            }
        }

        Commands::Config { path, show_secrets } => {
            if path {
                println!(
                    "{}",
                    bamboo_config::paths::config_json_path().display()
                );
            } else {
                let mut config = Config::new();
                config.normalize_tool_settings();
                let config_value = match serialize_config_for_cli(config, show_secrets) {
                    Ok(value) => value,
                    Err(e) => {
                        eprintln!("Failed to serialize config: {}", e);
                        std::process::exit(1);
                    }
                };

                match serde_json::to_string_pretty(&config_value) {
                    Ok(json) => println!("{}", json),
                    Err(e) => {
                        eprintln!("Failed to render config as JSON: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        }
    }
}

fn serialize_config_for_cli(
    mut config: Config,
    show_secrets: bool,
) -> bamboo_agent::Result<serde_json::Value> {
    config.refresh_proxy_auth_encrypted()?;
    config.refresh_provider_api_keys_encrypted()?;
    config.refresh_mcp_secrets_encrypted()?;
    config.normalize_tool_settings();

    let mut value = serde_json::to_value(&config)?;
    if !show_secrets {
        value = bamboo_agent::server::handlers::settings::redact_config_for_api(value, &config);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::serialize_config_for_cli;
    use bamboo_mcp::{McpServerConfig, StdioConfig, TransportConfig};
    use bamboo_config::{Config, OpenAIConfig, ProviderConfigs, ProxyAuth};
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};

    // fields set conditionally below
    #[allow(clippy::field_reassign_with_default)]
    fn configured_config() -> Config {
        let mut config = Config::default();
        config.proxy_auth = Some(ProxyAuth {
            username: "alice".to_string(),
            password: "secret".to_string(),
        });
        config.providers = ProviderConfigs {
            openai: Some(OpenAIConfig {
                api_key: "sk-cli-secret".to_string(),
                api_key_encrypted: None,
                base_url: Some("https://api.openai.com/v1".to_string()),
                model: Some("gpt-4o".to_string()),
                fast_model: None,
                vision_model: None,
                reasoning_effort: None,
                responses_only_models: vec![],
                request_overrides: None,
                extra: BTreeMap::new(),
            }),
            ..ProviderConfigs::default()
        };
        config.tools.disabled = vec![" bash ".to_string(), "read_file".to_string()];
        config.mcp.servers.push(McpServerConfig {
            id: "stdio-server".to_string(),
            name: None,
            enabled: true,
            transport: TransportConfig::Stdio(StdioConfig {
                command: "node".to_string(),
                args: vec!["server.js".to_string()],
                cwd: None,
                env: HashMap::from([("TOKEN".to_string(), "super-secret".to_string())]),
                env_encrypted: HashMap::new(),
                startup_timeout_ms: 5_000,
            }),
            request_timeout_ms: 5_000,
            healthcheck_interval_ms: 1_000,
            reconnect: Default::default(),
            allowed_tools: vec![],
            denied_tools: vec![],
        });
        config
    }

    #[test]
    fn serialize_config_for_cli_redacts_sensitive_fields_by_default() {
        let value = serialize_config_for_cli(configured_config(), false)
            .expect("CLI config should serialize");

        assert_eq!(value["providers"]["openai"]["api_key"], "****...****");
        assert!(value["providers"]["openai"]
            .as_object()
            .is_some_and(|obj| !obj.contains_key("api_key_encrypted")));
        assert!(value.get("proxy_auth_encrypted").is_none());
        assert_eq!(
            value["mcpServers"]["stdio-server"]["env"]["TOKEN"],
            "****...****"
        );
        assert_eq!(value["tools"]["disabled"], json!(["Bash", "Read"]));
    }

    #[test]
    fn serialize_config_for_cli_can_include_secrets_when_requested() {
        let value = serialize_config_for_cli(configured_config(), true)
            .expect("CLI config should serialize");

        assert!(value["providers"]["openai"]["api_key_encrypted"]
            .as_str()
            .is_some());
        assert!(value.get("proxy_auth_encrypted").is_some());
        assert_eq!(
            value["mcpServers"]["stdio-server"]["env"]["TOKEN"],
            "super-secret"
        );
        assert_eq!(value["tools"]["disabled"], json!(["Bash", "Read"]));
    }
}
