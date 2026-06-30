//! Bamboo binary entry point
//!
//! Standalone HTTP server for Bamboo

use bamboo_llm::Config;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "bamboo")]
#[command(version)]
#[command(about = "A fully self-contained AI agent backend framework", long_about = None)]
#[command(
    after_help = "QUICK RUN (headless server):\n  bamboo -p \"your task\"               full agent run (incl. sub-agents), print result, exit\n  bamboo -p \"next step\" -s <session>  continue an existing session's loop\n  bamboo -p \"...\" -m provider:model   pin the model\n  bamboo -p \"ping\" --echo             no-key smoke of the actor chain (no server)"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Run a full headless agent on this prompt and print the result. Boots
    /// the complete server runtime (root tool surface incl. SubAgent, so it
    /// CAN spawn actor children), finishes when the whole tree finishes.
    #[arg(short = 'p', long = "prompt", global = false)]
    prompt: Option<String>,

    /// With -p: continue this existing session instead of creating a new one.
    #[arg(short = 's', long)]
    session: Option<String>,

    /// With -p: model as 'provider:model'. Defaults to the session/config default.
    #[arg(short = 'm', long)]
    model: Option<String>,

    /// With -p: working directory for a NEW session (defaults to the current dir).
    #[arg(long)]
    workspace: Option<PathBuf>,

    /// With -p: data directory holding config.json (defaults to ~/.bamboo).
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// With -p: skip the server and run the bare actor chain with the echo
    /// executor (no LLM, no key) — transport smoke test only.
    #[arg(long)]
    echo: bool,

    /// With -p: NDJSON streaming on stdout — one JSON object per line:
    /// {"type":"session_started",...}, every agent event verbatim, then a
    /// final {"type":"result",...} envelope. Pipe-safe (logs go to stderr).
    #[arg(long = "stream-json", alias = "raw")]
    stream_json: bool,

    /// With -p: permission mode for the headless run, which has no interactive
    /// approver. One of: default | plan | accept-edits | dont-ask | bypass.
    /// Use `bypass` to let a tool-using agent run unattended (otherwise the run
    /// stalls at the first permission-gated tool such as Bash).
    #[arg(long = "permission-mode")]
    permission_mode: Option<String>,
}

/// Spawn the sidecar orphan guard: a dedicated OS thread that exits the process
/// when the shell that spawned us goes away.
///
/// The primary signal is `getppid()`: when our parent terminates — even via
/// SIGKILL / force-quit, even while it lingers as an unreaped zombie — the
/// kernel reparents us to init/launchd *at the moment of termination*, so
/// `getppid()` changes. That is reap-independent, unlike `kill(pid, 0)` (which
/// still reports a zombie as alive). The recorded shell PID fully disappearing
/// is kept as a secondary trigger.
#[cfg(unix)]
fn spawn_orphan_guard(shell_pid: u32) {
    std::thread::spawn(move || {
        let initial_parent = unsafe { libc::getppid() };
        loop {
            let current_parent = unsafe { libc::getppid() };
            // Reparented away from our original parent (it terminated → the kernel
            // handed us to init/launchd). This is immediate at the parent's exit and
            // independent of when its zombie is reaped — unlike `kill(pid, 0)`, which
            // still reports a not-yet-reaped zombie as alive. The shell PID fully
            // disappearing is kept as a secondary trigger.
            let reparented = current_parent != initial_parent || current_parent <= 1;
            let shell_gone = {
                let r = unsafe { libc::kill(shell_pid as libc::pid_t, 0) };
                r != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            };
            if reparented || shell_gone {
                std::process::exit(0);
            }
            std::thread::sleep(std::time::Duration::from_millis(300));
        }
    });
}

#[cfg(windows)]
fn spawn_orphan_guard(shell_pid: u32) {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
    };
    // Open a synchronizable handle to the shell and block until it exits, then take
    // the sidecar down with it. Covers normal quit AND hard kills (the shell runs no
    // cleanup). If the shell is already gone, OpenProcess fails and we exit at once.
    std::thread::spawn(move || unsafe {
        let handle = OpenProcess(PROCESS_SYNCHRONIZE, 0, shell_pid);
        if handle.is_null() {
            std::process::exit(0);
        }
        // INFINITE (0xFFFF_FFFF): wait until the shell process terminates.
        WaitForSingleObject(handle, u32::MAX);
        let _ = CloseHandle(handle);
        std::process::exit(0);
    });
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

        /// Exit when this parent process id no longer exists. The Bodhi desktop
        /// shell spawns `bamboo serve` as a sidecar and passes its own PID here, so
        /// the backend shuts down if the shell dies — including via SIGKILL /
        /// force-quit, which run no cleanup. Polled on a dedicated thread, so it is
        /// independent of the async runtime and of how the parent wired our stdio.
        #[arg(long)]
        parent_pid: Option<u32>,
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

    /// Run the standalone sub-agent message broker: a WebSocket bus over durable
    /// per-session mailboxes. The orchestrator and its workers (local subprocess,
    /// Docker, or SSH/remote) connect here to exchange ask/reply traffic.
    Broker {
        #[command(subcommand)]
        command: BrokerCommands,
    },

    /// Run a broker-connected agent: connect to a central broker and answer
    /// Ask/Task (query/steer) for this agent's mailbox. Deploy locally, in
    /// Docker, or on a remote host — it only needs `--broker` + `--token`.
    #[command(name = "broker-agent")]
    BrokerAgent {
        #[command(subcommand)]
        command: BrokerAgentCommands,
    },

    /// Probe a running server's health endpoint. Exits non-zero if it is
    /// unreachable or reports unhealthy — usable as a readiness check.
    Health {
        #[command(flatten)]
        conn: ConnArgs,
    },

    /// One-screen overview of a running server: address, health, session counts.
    Status {
        #[command(flatten)]
        conn: ConnArgs,
    },

    /// List sessions on a running server (stop one with `bamboo stop <id>`).
    Sessions {
        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Stop a running agent session's loop by id (POST /api/v1/stop/{id}).
    Stop {
        /// Session id to stop.
        session_id: String,
        #[command(flatten)]
        conn: ConnArgs,
    },
}

/// Connection options shared by the admin subcommands (`health` / `status` /
/// `sessions` / `stop`). Resolves which running server to talk to.
#[derive(clap::Args, Clone)]
struct ConnArgs {
    /// Full base URL of the server, e.g. `http://127.0.0.1:9562`.
    /// Overrides `--port` / `--data-dir`.
    #[arg(long)]
    server_url: Option<String>,

    /// Server port (defaults to the configured port, normally 9562).
    #[arg(long)]
    port: Option<u16>,

    /// Data dir holding config.json, to resolve the port/bind (defaults to ~/.bamboo).
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

impl From<ConnArgs> for bamboo_agent::admin_cli::ConnArgs {
    fn from(c: ConnArgs) -> Self {
        bamboo_agent::admin_cli::ConnArgs {
            server_url: c.server_url,
            port: c.port,
            data_dir: c.data_dir,
        }
    }
}

#[derive(Subcommand)]
enum BrokerCommands {
    /// Serve the broker on a WebSocket endpoint until terminated.
    Serve {
        /// Bind address. Use `0.0.0.0:9600` to accept connections from remote /
        /// containerized workers; `127.0.0.1:9600` for local-only.
        #[arg(long, default_value = "127.0.0.1:9600")]
        bind: String,

        /// Bearer token every client must present in its `Hello` frame. Falls
        /// back to the `BAMBOO_BROKER_TOKEN` env var; required (no default, so a
        /// broker is never accidentally unauthenticated).
        #[arg(long)]
        token: Option<String>,

        /// Durable mailbox storage root. Defaults to `<bamboo_dir>/broker`.
        #[arg(long)]
        root: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum BrokerAgentCommands {
    /// Connect to a broker and serve this agent's mailbox until terminated.
    Serve {
        /// Broker WebSocket endpoint, e.g. `ws://broker-host:9600`.
        #[arg(long)]
        broker: String,

        /// Bearer token (falls back to the `BAMBOO_BROKER_TOKEN` env var).
        #[arg(long)]
        token: Option<String>,

        /// This agent's mailbox key / session id — how it is addressed.
        #[arg(long)]
        id: String,

        /// Optional role/profile label.
        #[arg(long)]
        role: Option<String>,

        /// Optional model `provider:model` (real mode).
        #[arg(long)]
        model: Option<String>,

        /// Optional workspace directory for file tools (real mode).
        #[arg(long)]
        workspace: Option<String>,

        /// Use the dependency-free echo executor (no LLM) — for smoke tests.
        #[arg(long)]
        echo: bool,

        /// Proxy all MCP tool calls to this orchestrator id over the broker
        /// (host-bound MCP servers run only there).
        #[arg(long = "mcp-proxy")]
        mcp_proxy: Option<String>,

        /// Read a parent-resolved `ProvisionSpec` (model/creds/MCP/identity/bus)
        /// from stdin instead of self-resolving from this host's local config.
        /// The orchestrator pipes it on deploy — the same bootstrap a local
        /// subprocess worker already gets. When set, --model/--workspace/--mcp-proxy
        /// are ignored (the spec is authoritative).
        #[arg(long = "spec-stdin")]
        spec_stdin: bool,

        /// Like --spec-stdin, but read the spec from this FILE (a remote deployer
        /// uploads it next to the binary). Takes precedence over --spec-stdin.
        #[arg(long = "spec-file")]
        spec_file: Option<String>,
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

        /// Address to bind for a remotely-reachable worker, e.g.
        /// '0.0.0.0:8443'. Omit for the loopback ephemeral-port default
        /// (remote-actor-plan P1, #181).
        #[arg(long)]
        bind: Option<std::net::SocketAddr>,

        /// Terminate TLS ('wss://'). Requires --cert-file and --key-file.
        #[arg(long)]
        tls: bool,

        /// PEM certificate chain for --tls.
        #[arg(long)]
        cert_file: Option<PathBuf>,

        /// PEM private key for --tls.
        #[arg(long)]
        key_file: Option<PathBuf>,

        /// Bearer token a connecting parent must present on the WS handshake.
        /// Omit to accept any client (loopback default).
        #[arg(long)]
        token: Option<String>,
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
        Some(Commands::SubagentWorker)
        | Some(Commands::Actor { .. })
        | Some(Commands::Broker { .. })
        | Some(Commands::BrokerAgent { .. })
        | Some(Commands::Health { .. })
        | Some(Commands::Status { .. })
        | Some(Commands::Sessions { .. })
        | Some(Commands::Stop { .. })
        | None => {
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

    // Top-level quick run: `bamboo -p "<prompt>"` — a complete headless server.
    let command = match cli.command {
        Some(command) => command,
        None => {
            let Some(prompt) = cli.prompt else {
                use clap::CommandFactory;
                let _ = Cli::command().print_help();
                std::process::exit(2);
            };

            // --echo stays a bare actor-chain smoke (no server, no key).
            if cli.echo {
                let args = bamboo_agent::actor_cli::ActorRunArgs {
                    prompt,
                    model: cli.model,
                    role: "cli".to_string(),
                    workspace: cli.workspace,
                    data_dir: cli.data_dir,
                    echo: true,
                    raw: cli.stream_json,
                };
                if let Err(e) = bamboo_agent::actor_cli::run(args).await {
                    eprintln!("run failed: {e}");
                    std::process::exit(1);
                }
                return;
            }

            // Full headless server: same data-dir conventions as `serve`.
            let bamboo_home_dir = cli
                .data_dir
                .clone()
                .unwrap_or_else(bamboo_config::paths::resolve_bamboo_dir);
            bamboo_config::paths::init_bamboo_dir(bamboo_home_dir.clone());
            // SAFETY: main thread, before any async runtime work reads the env.
            unsafe {
                std::env::set_var("BAMBOO_DATA_DIR", bamboo_home_dir.as_os_str());
            }

            let args = bamboo_agent::headless::HeadlessArgs {
                prompt,
                session: cli.session,
                model: cli.model,
                workspace: cli.workspace,
                data_dir: bamboo_home_dir,
                stream_json: cli.stream_json,
                permission_mode: cli.permission_mode,
            };
            if let Err(e) = bamboo_agent::headless::run(args).await {
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
            parent_pid,
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
            let mut config = bamboo_llm::Config::from_data_dir(Some(bamboo_home_dir.clone()));

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

            // Orphan guard: when spawned as a sidecar, watch the parent process and
            // exit if it goes away — normal exit OR SIGKILL/force-quit, which run no
            // cleanup. Polled on a dedicated OS thread so it is independent of the
            // async runtime (once actix-web is serving it owns the runtime) and of
            // how the parent wired our stdio — a Tauri sidecar does not hand us a
            // pipe whose EOF we could watch, so a PID poll is the portable signal.
            if let Some(ppid) = parent_pid {
                spawn_orphan_guard(ppid);
            }

            // Start server using the unified config
            println!("Starting Bamboo server at {}", config.server_addr());
            // v2-P1 (#181): honor `server.tls` for in-process TLS termination
            // (fail-fast on bad/missing certs); absent → unchanged plaintext.
            let tls = config.server.tls.clone();
            let result = if config.server.static_dir.is_some() {
                bamboo_agent::server::run_with_bind_and_static_tls(
                    bamboo_home_dir,
                    config.server.port,
                    &config.server.bind,
                    config.server.static_dir.clone(),
                    tls,
                )
                .await
            } else {
                bamboo_agent::server::run_with_bind_tls(
                    bamboo_home_dir,
                    config.server.port,
                    &config.server.bind,
                    tls,
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
                    bind,
                    tls,
                    cert_file,
                    key_file,
                    token,
                } => {
                    bamboo_agent::actor_cli::serve(bamboo_agent::actor_cli::ActorServeArgs {
                        role,
                        id,
                        model,
                        workspace,
                        data_dir,
                        echo,
                        bind,
                        tls,
                        cert_file,
                        key_file,
                        token,
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

        Commands::Broker { command } => {
            let BrokerCommands::Serve { bind, token, root } = command;
            let token = match token
                .or_else(|| std::env::var("BAMBOO_BROKER_TOKEN").ok())
                .filter(|t| !t.is_empty())
            {
                Some(t) => t,
                None => {
                    eprintln!(
                        "broker: a Bearer token is required (pass --token or set BAMBOO_BROKER_TOKEN)"
                    );
                    std::process::exit(1);
                }
            };
            let root =
                root.unwrap_or_else(|| bamboo_config::paths::resolve_bamboo_dir().join("broker"));
            let listener = match tokio::net::TcpListener::bind(&bind).await {
                Ok(l) => l,
                Err(e) => {
                    eprintln!("broker: failed to bind {bind}: {e}");
                    std::process::exit(1);
                }
            };
            let addr = listener
                .local_addr()
                .map(|a| a.to_string())
                .unwrap_or_else(|_| bind.clone());
            tracing::info!(%addr, root = %root.display(), "bamboo broker serving");
            let core = std::sync::Arc::new(bamboo_broker::BrokerCore::new(root));
            let server = std::sync::Arc::new(bamboo_broker::BrokerServer::new(core, token));
            if let Err(e) = server.serve(listener).await {
                eprintln!("broker server failed: {e}");
                std::process::exit(1);
            }
        }

        Commands::BrokerAgent { command } => {
            let BrokerAgentCommands::Serve {
                broker,
                token,
                id,
                role,
                model,
                workspace,
                echo,
                mcp_proxy,
                spec_stdin,
                spec_file,
            } = command;
            let token = match token
                .or_else(|| std::env::var("BAMBOO_BROKER_TOKEN").ok())
                .filter(|t| !t.is_empty())
            {
                Some(t) => t,
                None => {
                    eprintln!(
                        "broker-agent: a Bearer token is required (pass --token or set BAMBOO_BROKER_TOKEN)"
                    );
                    std::process::exit(1);
                }
            };
            let result =
                bamboo_agent::broker_agent::run(bamboo_agent::broker_agent::BrokerAgentArgs {
                    broker,
                    token,
                    id,
                    role,
                    model,
                    workspace,
                    echo,
                    mcp_proxy,
                    spec_stdin,
                    spec_file,
                })
                .await;
            if let Err(e) = result {
                eprintln!("broker-agent failed: {e}");
                std::process::exit(1);
            }
        }

        Commands::Config { path, show_secrets } => {
            if path {
                println!("{}", bamboo_config::paths::config_json_path().display());
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

        Commands::Health { conn } => {
            if let Err(e) = bamboo_agent::admin_cli::health(conn.into()).await {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::Status { conn } => {
            if let Err(e) = bamboo_agent::admin_cli::status(conn.into()).await {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::Sessions { conn } => {
            if let Err(e) = bamboo_agent::admin_cli::sessions_list(conn.into()).await {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::Stop { session_id, conn } => {
            if let Err(e) = bamboo_agent::admin_cli::stop(conn.into(), &session_id).await {
                eprintln!("{e}");
                std::process::exit(1);
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
    use bamboo_config::{Config, OpenAIConfig, ProviderConfigs, ProxyAuth};
    use bamboo_mcp::{McpServerConfig, StdioConfig, TransportConfig};
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
