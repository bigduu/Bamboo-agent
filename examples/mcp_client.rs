//! Connecting an external MCP (Model Context Protocol) server programmatically
//! and merging its tools into the agent's tool surface — the "MCP" headline
//! capability the SDK advertises but has no example of.
//!
//! `.mcp_server(config)` / `.mcp_servers([...])` on [`AgentBuilder`], applied
//! by `with_defaults_for_data_dir`, start each configured server (in call
//! order) and compose its tools with the built-in surface via
//! `CompositeToolExecutor`: built-ins are tried first, falling back to MCP on
//! `NotFound`. Each server's `initialize` `instructions` (if any) are folded
//! into the tool guidance injected into the system prompt automatically.
//!
//! This is the equivalent of adding a server entry to `~/.bamboo/config.json`
//! (`bamboo mcp add`) or the `/api/v1/mcp` HTTP route, but wired directly in
//! process — useful when the set of MCP servers is decided by your embedding
//! app's own config rather than bamboo's.
//!
//! ```bash
//! cargo run --example mcp_client
//! ```
//!
//! Needs `~/.bamboo/config.json` with a provider + API key (`bamboo init`),
//! AND `npx` on PATH to actually run (this example wires up the reference
//! `@modelcontextprotocol/server-everything` stdio server as a stand-in for
//! whatever MCP server your app wants to connect). Swap `command`/`args` for
//! any other MCP server binary.

use std::collections::HashMap;

use bamboo_mcp::{McpServerConfig, ReconnectConfig, StdioConfig, TransportConfig};
use bamboo_sdk::agent::{Agent, AgentEvent, Session};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let home = dirs::home_dir().unwrap().join(".bamboo");

    let mcp_config = McpServerConfig {
        id: "everything".to_string(),
        name: Some("MCP reference server".to_string()),
        enabled: true,
        transport: TransportConfig::Stdio(StdioConfig {
            command: "npx".to_string(),
            args: vec![
                "-y".to_string(),
                "@modelcontextprotocol/server-everything".to_string(),
            ],
            cwd: None,
            env: HashMap::new(),
            env_encrypted: HashMap::new(),
            env_credential_refs: HashMap::new(),
            startup_timeout_ms: 20_000,
        }),
        request_timeout_ms: 60_000,
        healthcheck_interval_ms: 30_000,
        reconnect: ReconnectConfig::default(),
        // Empty = every tool the server advertises is allowed. Restrict to a
        // subset with e.g. `vec!["echo".to_string()]`.
        allowed_tools: Vec::new(),
        denied_tools: Vec::new(),
    };

    let agent = Agent::builder()
        .model("claude-sonnet-4-6")
        .instruction("You are a helpful assistant with access to external MCP tools.")
        .mcp_server(mcp_config)
        .with_defaults_for_data_dir(home)
        .await
        .expect("wire runtime deps (connects configured MCP servers)")
        .build()
        .expect("agent fully configured");

    let session = Session::new("mcp-client-demo", "claude-sonnet-4-6");
    let mut rx = agent.run_stream(session, "What tools do you have available? List them.");

    while let Some(event) = rx.recv().await {
        match event {
            AgentEvent::Token { content } => print!("{content}"),
            AgentEvent::ToolStart { tool_name, .. } => println!("\n[tool] {tool_name}"),
            AgentEvent::Complete { .. } => break,
            AgentEvent::Error { message } => {
                println!("\n[error] {message}");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}
