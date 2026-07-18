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
    after_help = "QUICK RUN (headless server):\n  bamboo -p \"your task\"               full agent run (incl. sub-agents), print result, exit\n  bamboo -p \"next step\" -s <session>  continue an existing session's loop\n  bamboo -p \"...\" -m provider:model   pin the model\n  bamboo -p \"ping\" --echo             no-key smoke of the actor chain (no server)\n  echo \"your task\" | bamboo -p -      read the prompt from stdin\n  bamboo completions zsh > ~/.zfunc/_bamboo   shell completions"
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

    /// With -p: model as 'provider:model' OR a bare model id (bound to
    /// --provider, else the configured default provider). Defaults to the
    /// session/config default.
    #[arg(short = 'm', long)]
    model: Option<String>,

    /// With -p: provider name (e.g. `anthropic`, `openai`, `gemini`). Combine
    /// with a bare `-m <model>` to pin `provider:model`, or use alone to select
    /// that provider (its configured default model). Conflicts with the
    /// `provider:model` form of `-m` only when the providers differ.
    #[arg(long)]
    provider: Option<String>,

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

    /// With -p: reasoning effort override for this run. One of:
    /// low | medium | high | xhigh | max. Defaults to the active provider/config value.
    #[arg(long = "reasoning-effort")]
    reasoning_effort: Option<String>,

    /// With -p: per-run skill mode (e.g. `code`, `ask`); skill discovery then
    /// prefers `skills-<mode>` directories. Defaults to the session/config value.
    #[arg(long = "skill-mode")]
    skill_mode: Option<String>,

    /// With -p: cancel the run if it hasn't finished within this many seconds
    /// (wall clock; counts any permission-gate round trips). Cancels the same
    /// way Ctrl-C does, then exits non-zero. Client-side only — there is no
    /// server-side per-run deadline in `ExecuteRequest` (yet) to delegate to.
    #[arg(long = "timeout", value_name = "SECONDS")]
    timeout: Option<u64>,

    /// Default log level when `RUST_LOG` is unset: error | warn | info | debug | trace.
    /// `RUST_LOG` still takes precedence when set. Takes priority over `-v`/`--verbose`.
    #[arg(long = "log-level", global = true)]
    log_level: Option<String>,

    /// Increase log verbosity when `--log-level`/`RUST_LOG` are unset: `-v` = debug,
    /// `-vv` (or more) = trace. Ignored if `--log-level` is given (more specific);
    /// `RUST_LOG` still wins over both when set.
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    /// Path to a `config.json` file, or its containing directory, to use
    /// instead of the default `<data-dir>/config.json`. Internally this
    /// resolves to a data directory exactly like `--data-dir` does, and is
    /// applied by seeding `BAMBOO_DATA_DIR` for this process (only when that
    /// env var isn't already set — same "explicit env wins" precedent as
    /// `--log-level`/`RUST_LOG`). Boundary: an explicit `--data-dir` (or
    /// `--conn.data-dir`) on a specific subcommand still wins over this,
    /// since it is passed directly rather than through the env fallback.
    #[arg(long = "config", value_name = "PATH", global = true)]
    config: Option<PathBuf>,
}

/// Resolve `--config <PATH>` to a data directory. A directory is used as-is
/// (matches `--data-dir` semantics: the dir that holds `config.json`). A file
/// path (existing or not-yet-created) is anchored on its parent directory,
/// since the data directory — not a bare file path — is what actually gates
/// `config.json` resolution throughout the codebase (see
/// `bamboo_config::paths::resolve_bamboo_dir`).
///
/// A path that doesn't exist is treated as a FILE only when it looks like one
/// (a `.json` extension); anything else is taken as a not-yet-created data
/// DIRECTORY and used as-is — `--data-dir` doesn't require its directory to
/// exist either, and anchoring a nonexistent bare directory on its parent
/// would land one level too high.
fn resolve_config_data_dir(path: &std::path::Path) -> Result<PathBuf, String> {
    if path.is_dir() {
        return Ok(path.to_path_buf());
    }
    let looks_like_file = path.is_file()
        || path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"));
    if !looks_like_file {
        return Ok(path.to_path_buf());
    }
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => Ok(parent.to_path_buf()),
        // A bare filename with no parent component: anchor on the current dir.
        _ => Ok(PathBuf::from(".")),
    }
}

/// `clap` `value_parser` for `broker serve --messages-per-second` /
/// `--message-burst`: rejects `0` with a clear CLI error instead of letting
/// it through and silently falling back to the default deep inside
/// `BrokerLimits` construction (review finding on #491/#53) — an operator
/// passing `0` almost certainly wants a hard error, not a quiet no-op.
fn parse_nonzero_u32(s: &str) -> Result<u32, String> {
    let n: u32 = s.parse().map_err(|e| format!("invalid number: {e}"))?;
    if n == 0 {
        return Err(
            "must be greater than 0 (0 would silently fall back to the default, not enforce \
             a stricter limit)"
                .to_string(),
        );
    }
    Ok(n)
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

    /// Full-screen terminal client (TUI) over a running server: chat,
    /// sessions, MCP, schedules, skills and config in one keyboard-driven
    /// interface. If a loopback --server-url is unreachable it offers to
    /// start a local `bamboo serve` (y/n) — see --auto-serve/--no-auto-serve.
    #[command(after_help = "EXAMPLES:\n  \
        # Connect to the default local server (offers to start one if absent)\n  \
        bamboo tui\n\n  \
        # Resume a session, headless-server URL pinned\n  \
        bamboo tui --server-url http://127.0.0.1:9562 --session-id <id>\n\n  \
        # Never offer/auto-start a local server (just warn when unreachable)\n  \
        bamboo tui --no-auto-serve")]
    Tui {
        /// Bamboo server URL. Defaults to the concrete loopback IPv4 (not
        /// `localhost`, which resolves to `::1` first on dual-stack hosts
        /// while the server default-binds `127.0.0.1` only → ECONNREFUSED).
        #[arg(long, default_value = bamboo_tui::DEFAULT_SERVER_URL)]
        server_url: String,

        /// Session ID to resume (optional)
        #[arg(long)]
        session_id: Option<String>,

        /// Model to use
        #[arg(short, long)]
        model: Option<String>,

        /// If `--server-url` is unreachable and loopback, start a local
        /// `bamboo serve` automatically instead of asking (y/n). No effect for
        /// a remote (non-loopback) `--server-url` — that always just warns.
        #[arg(long, conflicts_with = "no_auto_serve")]
        auto_serve: bool,

        /// Never offer/auto-start a local server, even for an unreachable
        /// loopback `--server-url` — just warn, like a remote URL always does.
        #[arg(long, conflicts_with = "auto_serve")]
        no_auto_serve: bool,
    },

    /// First-run setup: write `config.json` with a provider + API key.
    ///
    /// Interactive by default (prompts for anything not given as a flag). Pass
    /// `--non-interactive` for CI/scripts (then `--provider` + `--api-key` are
    /// required). The key is stored encrypted at rest.
    Init {
        /// Data directory holding config.json (defaults to ~/.bamboo).
        #[arg(long)]
        data_dir: Option<PathBuf>,

        /// Provider to configure: anthropic | openai | gemini. Prompted if omitted.
        #[arg(long)]
        provider: Option<String>,

        /// API key. Prompted (visible) if omitted in interactive mode.
        #[arg(long)]
        api_key: Option<String>,

        /// Default chat model. A sensible provider default is used if omitted.
        #[arg(long)]
        model: Option<String>,

        /// Overwrite an existing provider key without prompting.
        #[arg(long)]
        force: bool,

        /// Never prompt; fail if a required value is missing (CI-safe).
        #[arg(long)]
        non_interactive: bool,
    },

    /// Diagnose the local install: config presence, provider credential, and
    /// whether a server is reachable. Exits non-zero if a blocking problem is found.
    Doctor {
        /// Data directory holding config.json (defaults to ~/.bamboo).
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },

    /// Show (or set) Bamboo configuration.
    Config {
        /// Set a single config value, e.g.
        /// `config set providers.anthropic.api_key sk-ant-...`.
        #[command(subcommand)]
        action: Option<ConfigAction>,

        /// Show config file path
        #[arg(short, long)]
        path: bool,

        /// Show sensitive values (API keys, etc.)
        #[arg(long)]
        show_secrets: bool,
    },

    /// Generate a shell completion script. Pipe it to your shell's completion dir,
    /// e.g. `bamboo completions zsh > ~/.zfunc/_bamboo`.
    Completions {
        /// Target shell.
        #[arg(value_enum)]
        shell: clap_complete::Shell,
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

    /// Answer a session's pending question (permission gate / clarification)
    /// from the terminal (POST /api/v1/respond/{id}). Answering resumes the
    /// blocked run server-side — handy for a headless/scheduled run that is
    /// waiting on input. Use --pending to view the question first.
    #[command(after_help = "EXAMPLES:\n  \
        # See what a blocked session is asking\n  \
        bamboo respond <session-id> --pending\n\n  \
        # Answer it (the run resumes server-side)\n  \
        bamboo respond <session-id> \"Yes\"")]
    Respond {
        /// Session id with the pending question.
        session_id: String,

        /// The answer — one of the offered options, or free text when the
        /// question allows custom input. Omit it with --pending to only view
        /// the question.
        #[arg(required_unless_present = "pending", conflicts_with = "pending")]
        answer: Option<String>,

        /// Show the pending question (text + options) instead of answering.
        #[arg(long)]
        pending: bool,

        /// With --pending: print the raw JSON response instead of pretty text.
        #[arg(long, requires = "pending")]
        json: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Per-session lifecycle verbs on a running server (list with
    /// `bamboo sessions`).
    Session {
        #[command(subcommand)]
        command: SessionCommands,
    },

    /// Print a session's message transcript from a running server
    /// (GET /api/v1/history/{id}). Handy to review a headless `-p` run's log.
    History {
        /// Session id to show.
        session_id: String,
        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Manage schedules (timed tasks) on a running server (/api/v1/schedules).
    /// The scheduler creates a fresh session per fire and auto-executes its
    /// task prompt.
    Schedules {
        #[command(subcommand)]
        command: SchedulesCommands,
    },

    /// Inspect the skill surface the agent would load (offline read of
    /// `<data_dir>/skills`; no running server required).
    Skills {
        #[command(subcommand)]
        command: SkillsCommands,
    },

    /// Manage MCP servers: `list` is an offline read of `config.json`; the
    /// other verbs (`status`/`connect`/`disconnect`/`refresh`/`tools`/`add`/
    /// `remove`) talk to a running `bamboo serve` instance.
    Mcp {
        #[command(subcommand)]
        command: McpCommands,
    },

    /// Manage plugins (bundled MCP servers/prompt presets/skills/workflows)
    /// on a running server (`/api/v1/plugins`): `install`/`list`/`remove`/
    /// `update`.
    Plugin {
        #[command(subcommand)]
        command: PluginCommands,
    },
}

#[derive(Subcommand)]
enum SessionCommands {
    /// Show one session's detail (GET /api/v1/sessions/{id}): title, model,
    /// running state, pending question, message count, placement.
    Show {
        /// Session id to show.
        session_id: String,

        /// Print the raw JSON response instead of the pretty summary.
        #[arg(long)]
        json: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Delete a session permanently (DELETE /api/v1/sessions/{id}). Cancels
    /// any running execution first. Asks for confirmation unless --yes.
    Delete {
        /// Session id to delete.
        session_id: String,

        /// Skip the confirmation prompt (required for scripts / non-TTY use).
        #[arg(short = 'y', long)]
        yes: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },
}

#[derive(Subcommand)]
enum SkillsCommands {
    /// List discovered skills (id, name, description).
    List {
        /// Data directory holding config.json + skills/ (defaults to ~/.bamboo).
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum McpCommands {
    /// List configured MCP servers (id, enabled, transport, name) — offline
    /// read of `config.json`; no running server required.
    List {
        /// Data directory holding config.json (defaults to ~/.bamboo).
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },

    /// Live status from a running server: connection state, tool counts and
    /// last errors per server (GET /api/v1/mcp/servers).
    Status {
        #[command(flatten)]
        conn: ConnArgs,

        /// Print the raw JSON response instead of a table.
        #[arg(long)]
        json: bool,
    },

    /// Enable and (re)connect a configured server on a running instance
    /// (POST /api/v1/mcp/servers/{id}/connect).
    Connect {
        /// Server id (see `bamboo mcp status`).
        name: String,
        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Disable and disconnect a server on a running instance
    /// (POST /api/v1/mcp/servers/{id}/disconnect).
    Disconnect {
        /// Server id (see `bamboo mcp status`).
        name: String,
        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Re-list tools from one server, or from every enabled server when no id
    /// is given (POST /api/v1/mcp/servers/{id}/refresh).
    Refresh {
        /// Server id; omit to refresh every enabled server.
        name: Option<String>,
        #[command(flatten)]
        conn: ConnArgs,
    },

    /// List the tools a server exposes, or every server's tools when no id is
    /// given (GET /api/v1/mcp/servers/{id}/tools, GET /api/v1/mcp/tools).
    Tools {
        /// Server id; omit for all servers.
        name: Option<String>,
        #[command(flatten)]
        conn: ConnArgs,

        /// Print the raw JSON response instead of a table.
        #[arg(long)]
        json: bool,
    },

    /// Add (or overwrite) a server from a raw JSON payload passed through to
    /// POST /api/v1/mcp/servers. Accepts the mainstream flat shape
    /// (`{"id": "...", "command": "..."}` / `{"id": "...", "url": "..."}`)
    /// or Bamboo's internal shape, exactly like the HTTP API.
    Add {
        /// Path to a JSON file with the server config, or `-` to read stdin.
        #[arg(long = "json", value_name = "FILE|-")]
        json: String,
        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Stop and delete a server (DELETE /api/v1/mcp/servers/{id}). Asks for
    /// confirmation unless --yes (a removed server can be re-added with
    /// `bamboo mcp add`).
    Remove {
        /// Server id (see `bamboo mcp status`).
        name: String,

        /// Skip the confirmation prompt.
        #[arg(long, short = 'y')]
        yes: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },
}

#[derive(Subcommand)]
enum PluginCommands {
    /// Install a plugin from a local directory, a local `.tar.gz`/`.tgz`/
    /// `.zip` archive, or an `http(s)://` URL (source kind auto-detected from
    /// the argument) — `POST /api/v1/plugins/install`. Fails if the plugin id
    /// is already installed; use `bamboo plugin update` for that case.
    ///
    /// A URL source is checked against three trust layers: a host allowlist
    /// (`--allow-untrusted-host` opts out), an ed25519 publisher signature
    /// (`--allow-unsigned` opts out), and a checksum (`--sha256` pins it, or
    /// `--allow-unverified` opts out — waived automatically when the bundle
    /// is signature-verified). The defaults trust nova's official plugin
    /// from its GitHub release, so installing it needs NO flags at all once
    /// it's signed; any other host/publisher needs the matching opt-out (or
    /// `--insecure` to waive all three at once).
    #[command(after_help = "EXAMPLES:\n  \
        # From a local directory (development)\n  \
        bamboo plugin install ./my-plugin\n\n  \
        # From a packaged archive\n  \
        bamboo plugin install ./my-plugin.tar.gz\n\n  \
        # Official nova plugin from its trusted GitHub release — no flags\n  \
        # needed once it's signed by nova's official key (both trusted by\n  \
        # default; see `plugin_trust` in config.json)\n  \
        bamboo plugin install https://github.com/bigduu/Nova/releases/download/v0.2.0/nova-plugin-v0.2.0.tar.gz\n\n  \
        # From a URL, pinned by the bundle's sha256 (checksum layer only)\n  \
        bamboo plugin install https://example.com/my-plugin.tar.gz \\\n    \
        --sha256 3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1 \\\n    \
        --allow-untrusted-host --allow-unsigned\n\n  \
        # From an untrusted host, explicitly accepting the risk\n  \
        bamboo plugin install https://example.com/my-plugin.tar.gz \\\n    \
        --allow-untrusted-host --allow-unsigned --allow-unverified\n\n  \
        # Same as above, all at once (dev / self-hosted / custom setups only)\n  \
        bamboo plugin install https://example.com/my-plugin.tar.gz --insecure")]
    Install {
        /// A local directory, a local `.tar.gz`/`.tgz`/`.zip` archive, or an
        /// `http(s)://` URL — the source kind is auto-detected.
        source: String,

        /// Expected sha256 (hex) of the downloaded plugin BUNDLE (the
        /// `plugin.json`, or the archive containing it) fetched from a URL
        /// source. Only valid with a URL source; the server rejects a
        /// mismatch before unpacking anything. Without this, a URL install
        /// also needs `--allow-unverified` (unless the bundle is
        /// signature-verified, which satisfies this on its own). Honored even
        /// alongside `--insecure`: a supplied sha256 is a check you opted
        /// INTO, so it is still verified (a mismatch still refuses the
        /// install) — `--insecure` only turns default-required checks off.
        #[arg(long, value_name = "HEX")]
        sha256: Option<String>,

        /// Explicit opt-out: install from a URL with no bundle checksum
        /// verification. Only valid with a URL source. Use only when you
        /// trust the URL/host and have no sha256 to pin — the server logs a
        /// warning for every unverified install.
        #[arg(long)]
        allow_unverified: bool,

        /// Explicit opt-out: install from a URL whose host is not in
        /// `plugin_trust.trusted_hosts` (config.json; defaults to nova's
        /// official GitHub org). Only valid with a URL source. The server
        /// refuses BEFORE fetching unless this is set.
        #[arg(long)]
        allow_untrusted_host: bool,

        /// Explicit opt-out: install a bundle that is unsigned, or whose
        /// `.sig` does not verify against any key in
        /// `plugin_trust.trusted_keys` (config.json; defaults to nova's
        /// official signing key). Only valid with a URL source.
        #[arg(long)]
        allow_unsigned: bool,

        /// Skip ALL plugin trust checks (host allowlist, signature,
        /// checksum) for this install — use only for sources you fully
        /// trust; equivalent to --allow-untrusted-host --allow-unsigned
        /// --allow-unverified. Only valid with a URL source. A `--sha256`
        /// passed alongside this is still verified (this flag only turns
        /// checks OFF, never off a check you opted into). The server logs a
        /// prominent warning naming the source for every insecure install and
        /// records it in provenance (see `bamboo plugin list --json`).
        #[arg(long)]
        insecure: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },

    /// List installed plugins (id, version, status, registered capability
    /// counts, source) — `GET /api/v1/plugins`.
    List {
        #[command(flatten)]
        conn: ConnArgs,

        /// Print the raw JSON response instead of a table.
        #[arg(long)]
        json: bool,
    },

    /// Uninstall a plugin: stops/removes its registered MCP servers, prompt
    /// presets and workflow files, then deletes its plugin directory
    /// (`DELETE /api/v1/plugins/{id}`). Asks for confirmation unless --yes.
    Remove {
        /// Plugin id (see `bamboo plugin list`).
        id: String,

        /// Skip the confirmation prompt.
        #[arg(long, short = 'y')]
        yes: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Upgrade an installed plugin to a new source (same auto-detect as
    /// `install`) — `POST /api/v1/plugins/{id}/update`. Drops capabilities the
    /// new version no longer declares before registering the new set.
    Update {
        /// Plugin id (see `bamboo plugin list`).
        id: String,

        /// A local directory, a local `.tar.gz`/`.tgz`/`.zip` archive, or an
        /// `http(s)://` URL — the source kind is auto-detected.
        source: String,

        /// Expected sha256 (hex) of the downloaded plugin BUNDLE (the
        /// `plugin.json`, or the archive containing it) fetched from a URL
        /// source. Only valid with a URL source; the server rejects a
        /// mismatch before unpacking anything. Without this, a URL source
        /// also needs `--allow-unverified` (unless the bundle is
        /// signature-verified, which satisfies this on its own). Honored even
        /// alongside `--insecure` — see that flag's doc.
        #[arg(long, value_name = "HEX")]
        sha256: Option<String>,

        /// Explicit opt-out: update from a URL with no bundle checksum
        /// verification. Only valid with a URL source.
        #[arg(long)]
        allow_unverified: bool,

        /// Explicit opt-out: update from a URL whose host is not in
        /// `plugin_trust.trusted_hosts`. Only valid with a URL source.
        #[arg(long)]
        allow_untrusted_host: bool,

        /// Explicit opt-out: update from a bundle that is unsigned or not
        /// signed by a key in `plugin_trust.trusted_keys`. Only valid with a
        /// URL source.
        #[arg(long)]
        allow_unsigned: bool,

        /// Skip ALL plugin trust checks (host allowlist, signature,
        /// checksum) for this update — use only for sources you fully trust;
        /// equivalent to --allow-untrusted-host --allow-unsigned
        /// --allow-unverified. Only valid with a URL source. A `--sha256`
        /// passed alongside this is still verified. The server logs a
        /// prominent warning naming the source and records it in provenance.
        #[arg(long)]
        insecure: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },
}

#[derive(Subcommand)]
enum SchedulesCommands {
    /// List schedules (id, name, trigger, enabled, next/last run).
    List {
        /// Print the raw server response as pretty JSON instead of a table.
        #[arg(long)]
        json: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Show one schedule in detail (definition, state counters, run config).
    Show {
        /// Schedule id to show.
        schedule_id: String,

        /// Print the schedule as pretty JSON instead of the detail view.
        #[arg(long)]
        json: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Create a schedule. Give exactly one trigger: --cron, --every, --daily —
    /// or --json for a raw create payload (full schema: weekly/monthly
    /// triggers, misfire/overlap policies, start/end window, ...).
    #[command(group(
        clap::ArgGroup::new("trigger")
            .required(true)
            .args(["cron", "every", "daily", "json"])
    ))]
    #[command(after_help = "EXAMPLES:\n  \
        # Every day at 09:00 (server timezone unless --timezone)\n  \
        bamboo schedules create --name standup --daily 09:00 \\\n    \
        --prompt \"summarize yesterday's commits\" --timezone Asia/Shanghai\n\n  \
        # Cron trigger (seconds-first expression), pinned model + workspace\n  \
        bamboo schedules create --name nightly --cron '0 0 2 * * *' \\\n    \
        --prompt \"run the test suite and report\" \\\n    \
        --model anthropic:claude-sonnet-4 --workspace /path/to/repo\n\n  \
        # Full-fidelity raw payload (POST /api/v1/schedules body)\n  \
        bamboo schedules create --json schedule.json\n  \
        cat schedule.json | bamboo schedules create --json -")]
    Create {
        /// Schedule name.
        #[arg(long, required_unless_present = "json")]
        name: Option<String>,

        /// Cron trigger: a seconds-first cron expression,
        /// e.g. '0 30 9 * * *' = every day at 09:30:00.
        #[arg(long, value_name = "EXPR")]
        cron: Option<String>,

        /// Interval trigger: fire every N seconds.
        #[arg(long, value_name = "SECONDS")]
        every: Option<u64>,

        /// Daily trigger at a wall-clock time, e.g. '09:30' or '09:30:15'.
        #[arg(long, value_name = "HH:MM[:SS]")]
        daily: Option<String>,

        /// Task prompt: each fire creates a fresh session with this user
        /// message and auto-executes it.
        #[arg(long, required_unless_present = "json")]
        prompt: Option<String>,

        /// Model as 'provider:model' for the fired sessions (defaults to the
        /// server's configured schedule model).
        #[arg(long)]
        model: Option<String>,

        /// Working directory for the fired sessions' file tools.
        #[arg(long)]
        workspace: Option<String>,

        /// IANA timezone for wall-clock triggers, e.g. 'Asia/Shanghai'
        /// (defaults to the server's timezone handling).
        #[arg(long)]
        timezone: Option<String>,

        /// Create the schedule disabled (it won't fire until enabled).
        #[arg(long)]
        disabled: bool,

        /// Raw CreateScheduleRequest JSON: a FILE path or '-' for stdin,
        /// POSTed verbatim. Conflicts with the flag-based fields.
        #[arg(
            long,
            value_name = "FILE|-",
            conflicts_with_all = ["name", "prompt", "model", "workspace", "timezone", "disabled"]
        )]
        json: Option<String>,

        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Delete a schedule (asks for confirmation unless --yes).
    Delete {
        /// Schedule id to delete.
        schedule_id: String,

        /// Skip the confirmation prompt.
        #[arg(long, short = 'y')]
        yes: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Trigger a schedule to run now (POST /api/v1/schedules/{id}/run).
    Run {
        /// Schedule id to run.
        schedule_id: String,

        #[command(flatten)]
        conn: ConnArgs,
    },

    /// Show a schedule's run history (status, timings, session ids).
    Runs {
        /// Schedule id whose runs to list.
        schedule_id: String,

        /// Print the raw server response as pretty JSON instead of a table.
        #[arg(long)]
        json: bool,

        #[command(flatten)]
        conn: ConnArgs,
    },
}

/// Connection options shared by the admin subcommands (`health` / `status` /
/// `sessions` / `session` / `stop` / `respond` / `history`). Resolves which
/// running server to talk to.
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
enum ConfigAction {
    /// Set a single config value by dotted key.
    ///
    /// Secret-aware keys (stored encrypted at rest):
    ///   provider
    ///   providers.<anthropic|openai|gemini|bodhi>.api_key
    ///   providers.<anthropic|openai|gemini>.model
    ///   provider_instances.<id>.api_key
    ///   notifications.ntfy.token / notifications.bark.device_key
    ///
    /// Any other key is a generic validated dot-path into config.json
    /// (e.g. `server.port 9563`, `features.provider_model_ref true`,
    /// `tools.disabled '["Bash"]'`). The value is parsed as JSON when it
    /// parses, else taken as a string; unknown keys and type mismatches are
    /// rejected before anything is written. `proxy_auth.*` and `*_encrypted`
    /// keys cannot be set here.
    Set {
        /// Dotted config key, e.g. `providers.anthropic.api_key`.
        key: String,
        /// Value to store (JSON or plain string).
        value: String,
        /// Data directory holding config.json (defaults to ~/.bamboo).
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Validate and preview the resulting change without writing.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum BrokerCommands {
    /// Serve the broker on a WebSocket endpoint until terminated.
    #[command(after_help = "EXAMPLES:\n  \
        # Local-only broker (loopback), token from the env var\n  \
        BAMBOO_BROKER_TOKEN=secret bamboo broker serve\n\n  \
        # Accept remote / containerized workers on all interfaces\n  \
        bamboo broker serve --bind 0.0.0.0:9600 --token secret\n\n  \
        # wss:// for cross-network deployment (#48) — PEM cert/key pair\n  \
        bamboo broker serve --bind 0.0.0.0:9600 --token secret \\\n    \
        --cert /etc/bamboo/broker.crt --key /etc/bamboo/broker.key")]
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

        /// PEM certificate for `wss://` (#48). Requires `--key`; both present
        /// switches the listener from plaintext `ws://` to TLS-terminated
        /// `wss://` — the bearer token and all mailbox traffic (including
        /// proxied MCP tool arguments) are otherwise sent in the clear, which
        /// is fine on loopback but unsafe across a network. Omit both for the
        /// unchanged plaintext default.
        #[arg(long, requires = "key")]
        cert: Option<PathBuf>,

        /// PEM private key for `wss://` (#48). Requires `--cert`.
        #[arg(long, requires = "cert")]
        key: Option<PathBuf>,

        /// Max concurrent WebSocket connections (#53 DoS defense). Beyond
        /// this, new connections are dropped immediately. Default is
        /// generous — sized well above a normal multi-worker fabric.
        #[arg(long, default_value_t = bamboo_broker::BrokerLimits::default().max_connections)]
        max_connections: usize,

        /// Sustained per-connection `Deliver`-frame rate, frames/sec (#53).
        /// Exceeding it delays (not disconnects) the connection. Default is
        /// well above a single live event-streaming Run's normal rate. Must
        /// be nonzero — `0` is rejected outright rather than silently
        /// falling back to the default, since a `0` is almost certainly a
        /// misconfiguration (an operator trying to be maximally strict, or a
        /// scripting bug), not an intentional "block everything".
        #[arg(long, default_value_t = bamboo_broker::BrokerLimits::default().messages_per_second.get(), value_parser = parse_nonzero_u32)]
        messages_per_second: u32,

        /// Burst allowance layered on `--messages-per-second` (#53). Must be
        /// nonzero (see `--messages-per-second`).
        #[arg(long, default_value_t = bamboo_broker::BrokerLimits::default().message_burst.get(), value_parser = parse_nonzero_u32)]
        message_burst: u32,

        /// Max pending (undelivered-or-unacked) messages a single session's
        /// mailbox may hold before `deliver` starts refusing more (#53) — a
        /// backlog cap against a flood aimed at an offline/never-draining
        /// mailbox.
        #[arg(long, default_value_t = bamboo_broker::DEFAULT_MAX_PENDING_PER_MAILBOX)]
        max_pending_per_mailbox: usize,
    },
}

#[derive(Subcommand)]
enum BrokerAgentCommands {
    /// Connect to a broker and serve this agent's mailbox until terminated.
    #[command(after_help = "EXAMPLES:\n  \
        # Self-resolving worker: connect, answer Ask/Task for its mailbox\n  \
        bamboo broker-agent serve --broker ws://broker-host:9600 --token secret \\\n    \
        --id summarizer-1 --role summarizer --model anthropic:claude-sonnet-4\n\n  \
        # Boot from a parent-provided ProvisionSpec (deploy path)\n  \
        bamboo broker-agent serve --broker ws://host:9600 --token secret --id w1 --spec-stdin")]
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

        /// PEM CA cert to trust for a `wss://` broker with a self-signed cert
        /// (#48), instead of the OS native root store. Omit for a CA-signed
        /// cert (Let's Encrypt, etc.) or a self-signed cert whose CA is
        /// already installed in the OS trust store — both work with no flag.
        #[arg(long = "tls-ca-cert")]
        tls_ca_cert: Option<String>,
    },
}

#[derive(Subcommand)]
enum ActorCommands {
    /// Spawn an actor, give it a task, and stream its events live.
    #[command(after_help = "EXAMPLES:\n  \
        # Spawn an actor on a specific model and stream its events\n  \
        bamboo actor run \"summarize ./notes.md\" -m anthropic:claude-sonnet-4\n\n  \
        # No-key transport smoke test (echo executor)\n  \
        bamboo actor run \"ping\" --echo")]
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
    #[command(after_help = "EXAMPLES:\n  \
        # Announce a local service agent others can discover by role\n  \
        bamboo actor serve --role summarizer\n\n  \
        # Remotely reachable over wss:// with a bearer token\n  \
        bamboo actor serve --role summarizer --bind 0.0.0.0:8443 \\\n    \
        --tls --cert-file cert.pem --key-file key.pem --token secret")]
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
    #[command(after_help = "EXAMPLES:\n  \
        # Send a task to a discovered service agent (by role or exact id)\n  \
        bamboo actor call summarizer \"summarize ./notes.md\"")]
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

    // `--config <path>` seeds `BAMBOO_DATA_DIR` (only when unset) with the data
    // directory it resolves to, so every flow that falls back to
    // `bamboo_config::paths::resolve_bamboo_dir()` (i.e. did not receive its own
    // explicit `--data-dir` / `ConnArgs::data_dir`) picks it up — that covers
    // `-p`, `serve`, `init`, `doctor`, `config`, `actor`, `mcp list`/`skills list`,
    // and every admin verb's `ConnArgs`. A subcommand's own explicit `--data-dir`
    // still wins (it's passed directly, not through the env fallback), and an
    // already-set `BAMBOO_DATA_DIR` wins over `--config` too — same
    // "explicit env wins" precedent as `--log-level`/`RUST_LOG` below.
    if let Some(path) = cli.config.as_deref() {
        match resolve_config_data_dir(path) {
            Ok(dir) => {
                if std::env::var_os("BAMBOO_DATA_DIR").is_none() {
                    // SAFETY: main thread, before any async runtime work reads the env.
                    unsafe {
                        std::env::set_var("BAMBOO_DATA_DIR", dir.as_os_str());
                    }
                }
            }
            Err(e) => {
                eprintln!("invalid --config '{}': {e}", path.display());
                std::process::exit(2);
            }
        }
    }

    // `--log-level` (or `-v`/`-vv` when `--log-level` is absent) seeds `RUST_LOG`
    // (only when unset) so every logging path — the fmt subscribers below AND
    // `serve`'s file logging — honors it uniformly. An explicit `RUST_LOG` still
    // wins. Validated to a plain level here (use `RUST_LOG` directly for
    // target-scoped directives).
    let verbose_level = match cli.verbose {
        0 => None,
        1 => Some("debug"),
        _ => Some("trace"),
    };
    if let Some(level) = cli.log_level.as_deref().or(verbose_level) {
        const LEVELS: [&str; 5] = ["error", "warn", "info", "debug", "trace"];
        if !LEVELS.contains(&level.to_ascii_lowercase().as_str()) {
            eprintln!(
                "invalid --log-level '{level}' (expected: {})",
                LEVELS.join(" | ")
            );
            std::process::exit(2);
        }
        if std::env::var_os("RUST_LOG").is_none() {
            // SAFETY: consistent with the other env seeding in this file. We run
            // before any logging subscriber is installed and while the tokio
            // worker threads (already spawned by `#[tokio::main]`) are parked and
            // read no env, so this write races nothing in practice.
            unsafe {
                std::env::set_var("RUST_LOG", level);
            }
        }
    }

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
        Some(Commands::Tui { .. }) => {
            // No logging subscriber: the TUI owns the terminal (raw mode +
            // alternate screen), so a stdout/stderr fmt layer would garble the
            // display. Matches the standalone `bamboo-tui` binary, which
            // installs none.
        }
        Some(Commands::SubagentWorker)
        | Some(Commands::Actor { .. })
        | Some(Commands::Broker { .. })
        | Some(Commands::BrokerAgent { .. })
        | Some(Commands::Health { .. })
        | Some(Commands::Status { .. })
        | Some(Commands::Sessions { .. })
        | Some(Commands::Stop { .. })
        | Some(Commands::Respond { .. })
        | Some(Commands::Session { .. })
        | Some(Commands::History { .. })
        | Some(Commands::Schedules { .. })
        | Some(Commands::Skills { .. })
        | Some(Commands::Mcp { .. })
        | Some(Commands::Plugin { .. })
        | Some(Commands::Init { .. })
        | Some(Commands::Doctor { .. })
        | Some(Commands::Completions { .. })
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

            // `bamboo -p -` reads the prompt from stdin (pipe-friendly).
            let prompt = if prompt == "-" {
                use std::io::Read as _;
                let mut buf = String::new();
                if std::io::stdin().read_to_string(&mut buf).is_err() {
                    eprintln!("failed to read prompt from stdin");
                    std::process::exit(1);
                }
                let trimmed = buf.trim().to_string();
                if trimmed.is_empty() {
                    eprintln!("empty prompt on stdin");
                    std::process::exit(1);
                }
                trimmed
            } else {
                prompt
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
                reasoning_effort: cli.reasoning_effort,
                skill_mode: cli.skill_mode,
                provider: cli.provider,
                timeout_secs: cli.timeout,
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
            // Desktop notification sink default posture (see
            // `notify_sinks::desktop::desktop_enabled`): a sidecar runs under a
            // native shell (e.g. Bodhi) that owns notification UX, so the
            // desktop sink's "auto" default flips off in that mode. Set once,
            // here, before the Actix runtime starts.
            bamboo_agent::server::notify_sinks::set_sidecar_mode(parent_pid.is_some());

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

        Commands::Tui {
            server_url,
            session_id,
            model,
            auto_serve,
            no_auto_serve,
        } => {
            let auto_serve = if auto_serve {
                bamboo_tui::AutoServeMode::Auto
            } else if no_auto_serve {
                bamboo_tui::AutoServeMode::Off
            } else {
                bamboo_tui::AutoServeMode::Prompt
            };
            // `bamboo_tui::run` fails closed on a non-TTY stdin/stdout before
            // touching terminal state (raw mode / alternate screen), so a
            // scripted `bamboo tui` errors cleanly instead of garbling output.
            let result = bamboo_tui::run(bamboo_tui::TuiOptions {
                server_url,
                session_id,
                model,
                auto_serve,
            })
            .await;
            if let Err(e) = result {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::Completions { shell } => {
            use clap::CommandFactory;
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_string();
            clap_complete::generate(shell, &mut cmd, name, &mut std::io::stdout());
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
            let BrokerCommands::Serve {
                bind,
                token,
                root,
                cert,
                key,
                max_connections,
                messages_per_second,
                message_burst,
                max_pending_per_mailbox,
            } = command;
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
            // #48: `--cert`/`--key` (clap `requires` enforces both-or-neither)
            // switch the listener to `wss://`. `is_tls` only for the log line
            // below — the boolean itself never gates anything security-relevant.
            let is_tls = cert.is_some();
            tracing::info!(
                %addr,
                root = %root.display(),
                tls = is_tls,
                max_connections,
                messages_per_second,
                message_burst,
                max_pending_per_mailbox,
                "bamboo broker serving"
            );
            let core = std::sync::Arc::new(
                bamboo_broker::BrokerCore::new(root)
                    .with_max_pending_per_mailbox(max_pending_per_mailbox),
            );
            // Reclaim empty, unsubscribed mailbox dirs every 5 minutes.
            let _gc = core
                .clone()
                .spawn_mailbox_gc(std::time::Duration::from_secs(300));
            let limits = bamboo_broker::BrokerLimits {
                max_connections,
                // `parse_nonzero_u32` (the clap `value_parser` on both flags)
                // already rejected `0` at CLI-parse time, so these are
                // infallible here — no silent fallback needed.
                messages_per_second: std::num::NonZeroU32::new(messages_per_second)
                    .expect("clap value_parser rejects 0"),
                message_burst: std::num::NonZeroU32::new(message_burst)
                    .expect("clap value_parser rejects 0"),
            };
            let mut server = bamboo_broker::BrokerServer::with_limits(core, token, limits);
            // Fail-fast (#48): a bad/missing cert or key must abort startup, never
            // silently downgrade to plaintext.
            if let (Some(cert), Some(key)) = (&cert, &key) {
                server = match server.with_tls(cert, key) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("broker: failed to load TLS cert/key: {e}");
                        std::process::exit(1);
                    }
                };
            }
            let server = std::sync::Arc::new(server);
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
                tls_ca_cert,
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
                    tls_ca_cert,
                })
                .await;
            if let Err(e) = result {
                eprintln!("broker-agent failed: {e}");
                std::process::exit(1);
            }
        }

        Commands::Init {
            data_dir,
            provider,
            api_key,
            model,
            force,
            non_interactive,
        } => {
            let res = bamboo_agent::setup_cli::run_init(bamboo_agent::setup_cli::InitArgs {
                data_dir,
                provider,
                api_key,
                model,
                force,
                non_interactive,
            });
            if let Err(e) = res {
                eprintln!("init failed: {e:#}");
                std::process::exit(1);
            }
        }

        Commands::Doctor { data_dir } => {
            match bamboo_agent::setup_cli::run_doctor(data_dir).await {
                Ok(true) => {}
                Ok(false) => std::process::exit(1),
                Err(e) => {
                    eprintln!("doctor failed: {e:#}");
                    std::process::exit(1);
                }
            }
        }

        Commands::Config {
            action,
            path,
            show_secrets,
        } => {
            if let Some(ConfigAction::Set {
                key,
                value,
                data_dir,
                dry_run,
            }) = action
            {
                if let Err(e) =
                    bamboo_agent::setup_cli::run_config_set(&key, &value, data_dir, dry_run)
                {
                    eprintln!("config set failed: {e:#}");
                    std::process::exit(1);
                }
            } else if path {
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

        Commands::Respond {
            session_id,
            answer,
            pending,
            json,
            conn,
        } => {
            let result = if pending {
                bamboo_agent::admin_cli::respond_pending(conn.into(), &session_id, json).await
            } else {
                // clap enforces `answer` is present when --pending is absent.
                let answer = answer.unwrap_or_default();
                bamboo_agent::admin_cli::respond(conn.into(), &session_id, &answer).await
            };
            if let Err(e) = result {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::Session { command } => {
            let result = match command {
                SessionCommands::Show {
                    session_id,
                    json,
                    conn,
                } => bamboo_agent::admin_cli::session_show(conn.into(), &session_id, json).await,
                SessionCommands::Delete {
                    session_id,
                    yes,
                    conn,
                } => bamboo_agent::admin_cli::session_delete(conn.into(), &session_id, yes).await,
            };
            if let Err(e) = result {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::History { session_id, conn } => {
            if let Err(e) = bamboo_agent::admin_cli::history(conn.into(), &session_id).await {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::Schedules { command } => {
            use bamboo_agent::admin_cli;
            let result = match command {
                SchedulesCommands::List { json, conn } => {
                    admin_cli::schedules_list(conn.into(), json).await
                }
                SchedulesCommands::Show {
                    schedule_id,
                    json,
                    conn,
                } => admin_cli::schedules_show(conn.into(), &schedule_id, json).await,
                SchedulesCommands::Create {
                    name,
                    cron,
                    every,
                    daily,
                    prompt,
                    model,
                    workspace,
                    timezone,
                    disabled,
                    json,
                    conn,
                } => {
                    admin_cli::schedules_create(
                        conn.into(),
                        admin_cli::ScheduleCreateArgs {
                            name,
                            cron,
                            every,
                            daily,
                            prompt,
                            model,
                            workspace,
                            timezone,
                            disabled,
                            json,
                        },
                    )
                    .await
                }
                SchedulesCommands::Delete {
                    schedule_id,
                    yes,
                    conn,
                } => admin_cli::schedules_delete(conn.into(), &schedule_id, yes).await,
                SchedulesCommands::Run { schedule_id, conn } => {
                    admin_cli::schedules_run(conn.into(), &schedule_id).await
                }
                SchedulesCommands::Runs {
                    schedule_id,
                    json,
                    conn,
                } => admin_cli::schedules_runs(conn.into(), &schedule_id, json).await,
            };
            if let Err(e) = result {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::Skills { command } => {
            let SkillsCommands::List { data_dir } = command;
            if let Err(e) = bamboo_agent::read_cli::skills_list(data_dir).await {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::Mcp { command } => {
            let result = match command {
                // Offline read (no server) — unchanged.
                McpCommands::List { data_dir } => bamboo_agent::read_cli::mcp_list(data_dir).await,
                // Server-backed verbs over the /api/v1/mcp routes.
                McpCommands::Status { conn, json } => {
                    bamboo_agent::admin_cli::mcp_status(conn.into(), json).await
                }
                McpCommands::Connect { name, conn } => {
                    bamboo_agent::admin_cli::mcp_connect(conn.into(), &name).await
                }
                McpCommands::Disconnect { name, conn } => {
                    bamboo_agent::admin_cli::mcp_disconnect(conn.into(), &name).await
                }
                McpCommands::Refresh { name, conn } => {
                    bamboo_agent::admin_cli::mcp_refresh(conn.into(), name.as_deref()).await
                }
                McpCommands::Tools { name, conn, json } => {
                    bamboo_agent::admin_cli::mcp_tools(conn.into(), name.as_deref(), json).await
                }
                McpCommands::Add { json, conn } => {
                    bamboo_agent::admin_cli::mcp_add(conn.into(), &json).await
                }
                McpCommands::Remove { name, yes, conn } => {
                    bamboo_agent::admin_cli::mcp_remove(conn.into(), &name, yes).await
                }
            };
            if let Err(e) = result {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }

        Commands::Plugin { command } => {
            let result = match command {
                PluginCommands::Install {
                    source,
                    sha256,
                    allow_unverified,
                    allow_untrusted_host,
                    allow_unsigned,
                    insecure,
                    conn,
                } => {
                    bamboo_agent::plugin_cli::install(
                        conn.into(),
                        &source,
                        sha256.as_deref(),
                        allow_unverified,
                        allow_untrusted_host,
                        allow_unsigned,
                        insecure,
                    )
                    .await
                }
                PluginCommands::List { conn, json } => {
                    bamboo_agent::plugin_cli::list(conn.into(), json).await
                }
                PluginCommands::Remove { id, yes, conn } => {
                    bamboo_agent::plugin_cli::remove(conn.into(), &id, yes).await
                }
                PluginCommands::Update {
                    id,
                    source,
                    sha256,
                    allow_unverified,
                    allow_untrusted_host,
                    allow_unsigned,
                    insecure,
                    conn,
                } => {
                    bamboo_agent::plugin_cli::update(
                        conn.into(),
                        &id,
                        &source,
                        sha256.as_deref(),
                        allow_unverified,
                        allow_untrusted_host,
                        allow_unsigned,
                        insecure,
                    )
                    .await
                }
            };
            if let Err(e) = result {
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

    // CLI output and public Config serde retain the full compatibility view.
    // Only save_to_dir persists a root DTO that excludes sidecar domains.
    let mut value = config.to_compatibility_value()?;
    if !show_secrets {
        value = bamboo_agent::server::handlers::settings::redact_config_for_api(value, &config);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{resolve_config_data_dir, serialize_config_for_cli};
    use bamboo_config::{Config, OpenAIConfig, ProviderConfigs, ProxyAuth};
    use bamboo_mcp::{McpServerConfig, StdioConfig, TransportConfig};
    use serde_json::json;
    use std::collections::{BTreeMap, HashMap};

    /// `--config <dir>` pointing at an existing directory is used as-is (same
    /// semantics as `--data-dir`).
    #[test]
    fn resolve_config_data_dir_existing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let resolved = resolve_config_data_dir(dir.path()).unwrap();
        assert_eq!(resolved, dir.path());
    }

    /// `--config <dir>/config.json` (an existing file) resolves to the file's
    /// parent directory — the data dir is what actually gates `config.json`
    /// resolution downstream.
    #[test]
    fn resolve_config_data_dir_existing_file_uses_parent() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.json");
        std::fs::write(&config_path, "{}").unwrap();
        let resolved = resolve_config_data_dir(&config_path).unwrap();
        assert_eq!(resolved, dir.path());
    }

    /// A not-yet-created file path (common for a fresh `bamboo init --config
    /// <path>`-style flow) still resolves via its parent — existence is not
    /// required.
    #[test]
    fn resolve_config_data_dir_nonexistent_file_uses_parent() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("nested").join("config.json");
        let resolved = resolve_config_data_dir(&config_path).unwrap();
        assert_eq!(resolved, dir.path().join("nested"));
    }

    /// A bare filename with no parent component anchors on the current dir
    /// rather than erroring.
    #[test]
    fn resolve_config_data_dir_bare_filename_anchors_on_current_dir() {
        let resolved = resolve_config_data_dir(std::path::Path::new("config.json")).unwrap();
        assert_eq!(resolved, std::path::PathBuf::from("."));
    }

    /// A not-yet-created path that does NOT look like a config file (no
    /// `.json` extension) is a data DIRECTORY and is used as-is — anchoring it
    /// on its parent would land one level too high (`--data-dir` doesn't
    /// require the directory to exist either).
    #[test]
    fn resolve_config_data_dir_nonexistent_bare_directory_used_as_is() {
        let dir = tempfile::tempdir().unwrap();
        let datadir_path = dir.path().join("brand-new-datadir");
        let resolved = resolve_config_data_dir(&datadir_path).unwrap();
        assert_eq!(resolved, datadir_path);
    }

    // fields set conditionally below
    #[allow(clippy::field_reassign_with_default)]
    fn configured_config() -> Config {
        let mut config = Config::default();
        config.proxy_auth = Some(ProxyAuth {
            username: "alice".to_string(),
            password: "secret".to_string(),
        });
        *config.providers_mut() = ProviderConfigs {
            openai: Some(OpenAIConfig {
                api_key: "sk-cli-secret".to_string(),
                api_key_from_env: false,
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

    #[test]
    fn cli_tree_includes_tui_subcommand_with_standalone_flag_surface() {
        use clap::CommandFactory;

        let cmd = super::Cli::command();
        // Full-tree consistency check (what `--help` generation relies on).
        cmd.clone().debug_assert();

        let tui = cmd
            .find_subcommand("tui")
            .expect("`tui` must be in the subcommand tree");
        let args: Vec<&str> = tui.get_arguments().map(|a| a.get_id().as_str()).collect();
        // Same flag surface as the standalone `bamboo-tui` binary.
        for expected in [
            "server_url",
            "session_id",
            "model",
            "auto_serve",
            "no_auto_serve",
        ] {
            assert!(args.contains(&expected), "missing --{expected} on `tui`");
        }
    }

    #[test]
    fn tui_help_parses_and_auto_serve_flags_conflict() {
        use clap::Parser;

        // `bamboo tui --help` parses down the tree (clap reports help as an
        // "error" of kind DisplayHelp).
        let Err(help) = super::Cli::try_parse_from(["bamboo", "tui", "--help"]) else {
            panic!("--help must surface as a DisplayHelp error");
        };
        assert_eq!(help.kind(), clap::error::ErrorKind::DisplayHelp);

        // The flags parse.
        assert!(super::Cli::try_parse_from(["bamboo", "tui", "--auto-serve"]).is_ok());
        assert!(super::Cli::try_parse_from([
            "bamboo",
            "tui",
            "--server-url",
            "http://127.0.0.1:9999",
            "--session-id",
            "abc",
            "-m",
            "anthropic:claude-sonnet-4",
            "--no-auto-serve",
        ])
        .is_ok());

        // --auto-serve and --no-auto-serve are mutually exclusive.
        let Err(conflict) =
            super::Cli::try_parse_from(["bamboo", "tui", "--auto-serve", "--no-auto-serve"])
        else {
            panic!("conflicting flags must be rejected");
        };
        assert_eq!(conflict.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn cli_tree_includes_plugin_subcommand_with_install_list_remove_update() {
        use clap::CommandFactory;

        let cmd = super::Cli::command();
        let plugin = cmd
            .find_subcommand("plugin")
            .expect("`plugin` must be in the subcommand tree");
        let verbs: Vec<&str> = plugin.get_subcommands().map(|c| c.get_name()).collect();
        for expected in ["install", "list", "remove", "update"] {
            assert!(verbs.contains(&expected), "missing `plugin {expected}`");
        }
    }

    #[test]
    fn plugin_help_parses_for_top_level_and_each_verb() {
        use clap::error::ErrorKind;
        use clap::Parser;

        for args in [
            vec!["bamboo", "plugin", "--help"],
            vec!["bamboo", "plugin", "install", "--help"],
            vec!["bamboo", "plugin", "list", "--help"],
            vec!["bamboo", "plugin", "remove", "--help"],
            vec!["bamboo", "plugin", "update", "--help"],
        ] {
            let Err(help) = super::Cli::try_parse_from(&args) else {
                panic!("{args:?} must surface as a DisplayHelp error");
            };
            assert_eq!(help.kind(), ErrorKind::DisplayHelp, "{args:?}");
        }
    }

    #[test]
    fn plugin_install_parses_source_and_optional_sha256() {
        use clap::Parser;

        assert!(super::Cli::try_parse_from(["bamboo", "plugin", "install", "./my-plugin"]).is_ok());
        assert!(super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "install",
            "https://example.com/my-plugin.tar.gz",
            "--sha256",
            "deadbeef",
        ])
        .is_ok());
        // `source` is required.
        assert!(super::Cli::try_parse_from(["bamboo", "plugin", "install"]).is_err());
    }

    #[test]
    fn plugin_install_parses_allow_unverified_flag() {
        use clap::Parser;

        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "install",
            "https://example.com/my-plugin.tar.gz",
            "--allow-unverified",
        ])
        .expect("--allow-unverified should parse");
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Install {
                    allow_unverified,
                    sha256,
                    ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Install");
        };
        assert!(allow_unverified);
        assert!(sha256.is_none());

        // Both flags can be given together (redundant, not rejected client-side —
        // the server treats a supplied sha256 as authoritative regardless).
        assert!(super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "install",
            "https://example.com/my-plugin.tar.gz",
            "--sha256",
            "deadbeef",
            "--allow-unverified",
        ])
        .is_ok());

        // Defaults to false when omitted.
        let parsed =
            super::Cli::try_parse_from(["bamboo", "plugin", "install", "./my-plugin"]).unwrap();
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Install {
                    allow_unverified, ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Install");
        };
        assert!(!allow_unverified);
    }

    #[test]
    fn plugin_install_parses_allow_untrusted_host_flag() {
        use clap::Parser;

        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "install",
            "https://example.com/my-plugin.tar.gz",
            "--allow-untrusted-host",
        ])
        .expect("--allow-untrusted-host should parse");
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Install {
                    allow_untrusted_host,
                    allow_unsigned,
                    ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Install");
        };
        assert!(allow_untrusted_host);
        assert!(!allow_unsigned);

        // Defaults to false when omitted.
        let parsed =
            super::Cli::try_parse_from(["bamboo", "plugin", "install", "./my-plugin"]).unwrap();
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Install {
                    allow_untrusted_host,
                    ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Install");
        };
        assert!(!allow_untrusted_host);
    }

    #[test]
    fn plugin_install_parses_allow_unsigned_flag() {
        use clap::Parser;

        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "install",
            "https://example.com/my-plugin.tar.gz",
            "--allow-unsigned",
        ])
        .expect("--allow-unsigned should parse");
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Install {
                    allow_unsigned,
                    allow_untrusted_host,
                    ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Install");
        };
        assert!(allow_unsigned);
        assert!(!allow_untrusted_host);

        // All four flags/opts can be combined.
        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "install",
            "https://example.com/my-plugin.tar.gz",
            "--sha256",
            "deadbeef",
            "--allow-unverified",
            "--allow-untrusted-host",
            "--allow-unsigned",
        ])
        .expect("all four trust flags together should parse");
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Install {
                    sha256,
                    allow_unverified,
                    allow_untrusted_host,
                    allow_unsigned,
                    ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Install");
        };
        assert_eq!(sha256.as_deref(), Some("deadbeef"));
        assert!(allow_unverified);
        assert!(allow_untrusted_host);
        assert!(allow_unsigned);

        // Defaults to false when omitted.
        let parsed =
            super::Cli::try_parse_from(["bamboo", "plugin", "install", "./my-plugin"]).unwrap();
        let super::Commands::Plugin {
            command: super::PluginCommands::Install { allow_unsigned, .. },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Install");
        };
        assert!(!allow_unsigned);
    }

    #[test]
    fn plugin_install_parses_insecure_flag() {
        use clap::Parser;

        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "install",
            "https://example.com/my-plugin.tar.gz",
            "--insecure",
        ])
        .expect("--insecure should parse");
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Install {
                    insecure,
                    allow_unverified,
                    allow_untrusted_host,
                    allow_unsigned,
                    ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Install");
        };
        assert!(insecure);
        // `--insecure` alone does NOT flip the individual `allow_*` clap
        // fields themselves (those stay whatever was literally passed on the
        // command line) — the aggregate is applied by `detect_source` when
        // building the JSON request body, exercised in `plugin_cli`'s own
        // tests (`detect_source_insecure_...`).
        assert!(!allow_unverified);
        assert!(!allow_untrusted_host);
        assert!(!allow_unsigned);

        // `--insecure` combined with an explicit `--sha256` parses too (the
        // checksum stays honored — see `plugin_cli::detect_source`'s docs).
        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "install",
            "https://example.com/my-plugin.tar.gz",
            "--insecure",
            "--sha256",
            "deadbeef",
        ])
        .expect("--insecure with --sha256 should parse");
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Install {
                    insecure, sha256, ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Install");
        };
        assert!(insecure);
        assert_eq!(sha256.as_deref(), Some("deadbeef"));

        // Defaults to false when omitted.
        let parsed =
            super::Cli::try_parse_from(["bamboo", "plugin", "install", "./my-plugin"]).unwrap();
        let super::Commands::Plugin {
            command: super::PluginCommands::Install { insecure, .. },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Install");
        };
        assert!(!insecure);
    }

    #[test]
    fn plugin_update_parses_id_and_source() {
        use clap::Parser;

        assert!(super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "update",
            "hello-plugin",
            "./my-plugin-v2",
        ])
        .is_ok());
        // Both `id` and `source` are required.
        assert!(
            super::Cli::try_parse_from(["bamboo", "plugin", "update", "hello-plugin"]).is_err()
        );
    }

    #[test]
    fn plugin_update_parses_allow_unverified_flag() {
        use clap::Parser;

        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "update",
            "hello-plugin",
            "https://example.com/my-plugin.tar.gz",
            "--allow-unverified",
        ])
        .expect("--allow-unverified should parse on update too");
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Update {
                    allow_unverified, ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Update");
        };
        assert!(allow_unverified);
    }

    #[test]
    fn plugin_update_parses_allow_untrusted_host_and_allow_unsigned_flags() {
        use clap::Parser;

        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "update",
            "hello-plugin",
            "https://example.com/my-plugin.tar.gz",
            "--allow-untrusted-host",
            "--allow-unsigned",
        ])
        .expect("--allow-untrusted-host and --allow-unsigned should parse on update too");
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Update {
                    allow_untrusted_host,
                    allow_unsigned,
                    ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Update");
        };
        assert!(allow_untrusted_host);
        assert!(allow_unsigned);

        // Default to false when omitted.
        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "update",
            "hello-plugin",
            "./my-plugin-v2",
        ])
        .unwrap();
        let super::Commands::Plugin {
            command:
                super::PluginCommands::Update {
                    allow_untrusted_host,
                    allow_unsigned,
                    ..
                },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Update");
        };
        assert!(!allow_untrusted_host);
        assert!(!allow_unsigned);
    }

    #[test]
    fn plugin_update_parses_insecure_flag() {
        use clap::Parser;

        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "update",
            "hello-plugin",
            "https://example.com/my-plugin.tar.gz",
            "--insecure",
        ])
        .expect("--insecure should parse on update too");
        let super::Commands::Plugin {
            command: super::PluginCommands::Update { insecure, .. },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Update");
        };
        assert!(insecure);

        // Defaults to false when omitted.
        let parsed = super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "update",
            "hello-plugin",
            "./my-plugin-v2",
        ])
        .unwrap();
        let super::Commands::Plugin {
            command: super::PluginCommands::Update { insecure, .. },
        } = parsed.command.expect("a subcommand was given")
        else {
            panic!("expected Plugin::Update");
        };
        assert!(!insecure);
    }

    #[test]
    fn plugin_remove_parses_id_and_yes_flag() {
        use clap::Parser;

        assert!(super::Cli::try_parse_from(["bamboo", "plugin", "remove", "hello-plugin"]).is_ok());
        assert!(super::Cli::try_parse_from([
            "bamboo",
            "plugin",
            "remove",
            "hello-plugin",
            "--yes",
        ])
        .is_ok());
        assert!(super::Cli::try_parse_from(["bamboo", "plugin", "remove"]).is_err());
    }

    #[test]
    fn plugin_list_parses_json_flag() {
        use clap::Parser;

        assert!(super::Cli::try_parse_from(["bamboo", "plugin", "list"]).is_ok());
        assert!(super::Cli::try_parse_from(["bamboo", "plugin", "list", "--json"]).is_ok());
    }
}
