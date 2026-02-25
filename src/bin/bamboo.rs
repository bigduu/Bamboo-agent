//! Bamboo binary entry point
//!
//! Standalone HTTP server for Bamboo

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

    // Initialize logging
    tracing_subscriber::fmt::init();

    match cli.command {
        Commands::Serve {
            port,
            bind,
            data_dir,
            static_dir,
            workers,
        } => {
            // Load config (with env var overrides already applied)
            // If --data-dir is specified, load from that directory
            let mut config = if let Some(ref d) = data_dir {
                bamboo_agent::core::Config::from_data_dir(Some(d.clone()))
            } else {
                bamboo_agent::core::Config::new()
            };

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

            // Start server using the unified config
            println!("Starting Bamboo server at {}", config.server_addr());
            if let Err(e) = bamboo_agent::server::run_with_bind(
                config.data_dir.clone(),
                config.server.port,
                &config.server.bind,
            )
            .await
            {
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
                let config = bamboo_agent::core::Config::new();
                let mut config_value = serde_json::to_value(&config).unwrap();

                if !show_secrets {
                    // Redact sensitive fields
                    if let Some(providers) = config_value.get_mut("providers") {
                        if let Some(providers_obj) = providers.as_object_mut() {
                            for (_, provider) in providers_obj.iter_mut() {
                                if let Some(provider_obj) = provider.as_object_mut() {
                                    if provider_obj.contains_key("api_key") {
                                        provider_obj.insert(
                                            "api_key".to_string(),
                                            serde_json::json!("***REDACTED***"),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                println!("{}", serde_json::to_string_pretty(&config_value).unwrap());
            }
        }
    }
}
