//! The [`ExecuteRequest`]/[`ExecuteRequestBuilder`] escape hatch — full
//! per-request control beyond what `Agent::run`/`run_stream` expose: split
//! fast/background/summarization models, per-request reasoning effort,
//! skill selection, and a disabled-tools override.
//!
//! `Agent::run`/`run_stream` apply the builder's configured instruction +
//! model to every call. `Agent::execute` instead takes a fully-specified
//! `ExecuteRequest` you assemble yourself — useful when different turns in
//! the same app need different per-call overrides (e.g. a cheaper model for
//! background/batch turns) without rebuilding the `Agent`. It funnels into
//! the exact same canonical engine execution path as `run`/`run_stream`.
//!
//! ```bash
//! cargo run --example execute_request
//! ```
//!
//! Needs `~/.bamboo/config.json` with a provider + API key (`bamboo init`).

use std::collections::BTreeSet;

use bamboo_domain::ReasoningEffort;
use bamboo_sdk::agent::{Agent, AgentEvent, ExecuteRequestBuilder, Message, Session};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let home = dirs::home_dir().unwrap().join(".bamboo");

    // Note: `.model(...)`/`.instruction(...)` on the builder are NOT applied
    // by `execute` (unlike `run`/`run_stream`) — the request below owns the
    // model/skill/tool selection entirely, so set the system prompt directly
    // on the session instead (see `session.add_message(Message::system(...))`
    // below).
    let agent = Agent::builder()
        .with_defaults_for_data_dir(home)
        .await
        .expect("wire runtime deps")
        .build()
        .expect("agent fully configured");

    let mut session = Session::new("execute-request-demo", "claude-sonnet-4-6");
    session.add_message(Message::system("You are a terse, efficient coding agent."));
    session.add_message(Message::user(
        "Summarize what Cargo.toml declares in this repo.",
    ));

    let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(256);
    let cancel_token = CancellationToken::new();

    let mut disabled_tools = BTreeSet::new();
    disabled_tools.insert("Bash".to_string()); // read-only pass: no shell access

    let request = ExecuteRequestBuilder::new(
        // `initial_message` is only used for logging/telemetry — the engine
        // drives off the last `User` message already in `session.messages`
        // (matches `Agent::run_session`'s contract).
        "Summarize what Cargo.toml declares in this repo.",
        event_tx,
        cancel_token,
    )
    .model("claude-sonnet-4-6")
    .provider_name("anthropic")
    // A cheaper/faster model for auxiliary work (title generation, summaries)
    // within this same run, independent of the primary model above.
    .background_model("claude-haiku-4-5")
    .reasoning_effort(ReasoningEffort::Low)
    .disabled_tools(disabled_tools)
    .selected_skill_ids(vec![]) // no optional skills injected for this turn
    .build();

    // Drain events on a background task (mirrors how `Agent::run_session`
    // internally drains its own channel) so the bounded channel never blocks
    // the execution.
    let drain = tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            if let AgentEvent::Token { content } = event {
                print!("{content}");
            }
        }
    });

    agent.execute(&mut session, request).await.expect("execute");
    drain.await.ok();

    println!(
        "\n\n[final message] {}",
        session
            .messages
            .last()
            .map(|m| m.content.as_str())
            .unwrap_or("")
    );

    Ok(())
}
