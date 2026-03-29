//! Bamboo binary entry point
//!
//! Standalone HTTP server for Bamboo

use bamboo_agent::core::Config;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bamboo")]
#[command(about = "A fully self-contained AI agent backend framework", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logging.
    tracing_subscriber::fmt()
        .with_target(true)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match cli.command {
        Commands::Serve {
            port,
            bind,
            data_dir,
            static_dir,
            workers,
        } => {
            let bamboo_home_dir = data_dir
                .clone()
                .unwrap_or_else(bamboo_agent::core::paths::resolve_bamboo_dir);
            // Stabilize the data dir for the lifetime of this process.
            bamboo_agent::core::paths::init_bamboo_dir(bamboo_home_dir.clone());
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
                bamboo_agent::core::Config::from_data_dir(Some(bamboo_home_dir.clone()));

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

        Commands::Config { path, show_secrets } => {
            if path {
                println!(
                    "{}",
                    bamboo_agent::core::paths::config_json_path().display()
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
    use bamboo_agent::{
        agent::mcp::{McpServerConfig, StdioConfig, TransportConfig},
        core::{Config, OpenAIConfig, ProviderConfigs, ProxyAuth},
    };
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};

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
