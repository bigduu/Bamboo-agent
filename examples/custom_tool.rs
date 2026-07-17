//! Implementing a custom tool (`impl Tool`) and registering it on the agent
//! builder — the "custom tools" capability the SDK advertises but has no
//! example of.
//!
//! A tool is: a name, a description, a JSON Schema for its arguments, and an
//! async `invoke`. Register it with `.tool(MyTool)` (owned) or
//! `.tool_shared(Arc::new(MyTool))` (pre-built `Arc<dyn Tool>`, e.g. shared
//! across multiple agents) on [`AgentBuilder`]. `.tools([...])` REPLACES the
//! whole tool set — mix built-ins in with your custom tool via
//! [`BuiltinTool`] if you want the agent to keep them:
//!
//! ```bash
//! cargo run --example custom_tool
//! ```
//!
//! Needs `~/.bamboo/config.json` with a provider + API key (`bamboo init`).

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use bamboo_agent_core::tools::{Tool, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_sdk::agent::{Agent, AgentEvent, BuiltinTool, Session};

/// A trivial weather lookup tool. Real tools would call out to an API, read
/// a file, run a subprocess, etc. — `invoke` is just an async function.
struct WeatherTool;

#[derive(Debug, Deserialize)]
struct WeatherArgs {
    city: String,
}

#[async_trait]
impl Tool for WeatherTool {
    /// Unique name — this is what the model sees and calls by.
    fn name(&self) -> &str {
        "get_weather"
    }

    /// Human/model-readable description. This is the primary signal the
    /// model uses to decide when to call the tool, so be specific about
    /// what it does and does not do.
    fn description(&self) -> &str {
        "Look up the current weather for a named city. Returns a short \
         plain-text summary (temperature + conditions). Does not accept \
         coordinates or postal codes — city name only."
    }

    /// JSON Schema for the tool's arguments (standard function-calling
    /// schema — `type`/`properties`/`required`).
    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "city": {
                    "type": "string",
                    "description": "City name, e.g. \"Tokyo\" or \"San Francisco\"."
                }
            },
            "required": ["city"]
        })
    }

    /// The actual execution. `args` is the raw JSON the model produced (parse
    /// it, don't trust it blindly); `ctx` carries session/tool-call metadata
    /// and an optional streaming sender for tools that want to emit
    /// `ToolToken` progress while they run — unused by this simple example.
    ///
    /// Return `Ok(ToolOutcome::Completed(result))` for the common
    /// synchronous case. `ToolOutcome` also has `Running` (detach and
    /// complete later — for long-lived background work) and `NeedsHuman`
    /// (suspend the turn for approval/clarification, like the built-in
    /// `RequestPermissions`/`ConclusionWithOptions` tools) — most custom
    /// tools only ever need `Completed`.
    async fn invoke(
        &self,
        args: serde_json::Value,
        _ctx: ToolCtx,
    ) -> Result<ToolOutcome, ToolError> {
        let args: WeatherArgs =
            serde_json::from_value(args).map_err(|e| ToolError::InvalidArguments(e.to_string()))?;

        // Stand-in for a real lookup (HTTP call, cached dataset, …).
        let summary = format!("{}: 21C, partly cloudy (mock data)", args.city);

        Ok(ToolOutcome::Completed(ToolResult::text(true, summary)))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let home = dirs::home_dir().unwrap().join(".bamboo");

    let agent = Agent::builder()
        .model("claude-sonnet-4-6")
        .instruction("You are a helpful assistant with access to a weather lookup tool.")
        // Mix a built-in (Read) with the custom WeatherTool. `.tools([...])`
        // replaces the whole tool set, so list everything you want the agent
        // to have here.
        .tools([BuiltinTool::Read.tool()])
        .tool(WeatherTool)
        .with_defaults_for_data_dir(home)
        .await
        .expect("wire runtime deps")
        .build()
        .expect("agent fully configured");

    let session = Session::new("custom-tool-demo", "claude-sonnet-4-6");
    let mut rx = agent.run_stream(session, "What's the weather like in Tokyo right now?");

    while let Some(event) = rx.recv().await {
        match event {
            AgentEvent::ToolStart {
                tool_name,
                arguments,
                ..
            } => println!("[tool] {tool_name} args={arguments}"),
            AgentEvent::ToolComplete { result, .. } => {
                println!("[tool result] {}", result.result)
            }
            AgentEvent::Complete { .. } => break,
            AgentEvent::Error { message } => {
                println!("[error] {message}");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}
