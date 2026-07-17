# Getting started

A first-run walkthrough: install, configure a provider, run one agent turn
three different ways (CLI, HTTP, in-process SDK), then where to go next.

## 1. Install

```bash
cargo install --path .        # from a checkout, or: cargo install bamboo-agent
```

Or build/run straight from the workspace without installing:
`cargo run --bin bamboo -- <subcommand>`.

## 2. Configure a provider

```bash
bamboo init
```

Interactive: picks a provider (`anthropic`/`openai`/`gemini`/`copilot`/
`bodhi`) and prompts for an API key. Non-interactive form for scripts/CI:

```bash
bamboo init --non-interactive --provider anthropic --api-key "sk-ant-..."
```

This writes `~/.bamboo/config.json` (override with `--data-dir`) and stores
the key **encrypted at rest** (see [encryption at
rest](../config-reference.md#encryption-at-rest)). Verify the install is
sound at any point with:

```bash
bamboo doctor    # config present, provider keyed, server reachable — exits non-zero on a blocking problem
```

## 3. Your first agent turn — three ways

### a) Headless one-shot (fastest way to see it work)

```bash
bamboo -p "List the files here and tell me what this project does."
```

Boots the full runtime (including sub-agent support), runs one turn, prints
the result, exits. Add `-s <session-id>` on a later call to continue the same
conversation.

### b) HTTP server + curl

```bash
bamboo serve &

SID=$(curl -s http://127.0.0.1:9562/api/v1/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"List the files here and tell me what this project does.","model":"claude-sonnet-4-6"}' \
  | jq -r .session_id)

curl -s -X POST "http://127.0.0.1:9562/api/v1/execute/$SID" \
  -H 'Content-Type: application/json' -d '{}'

curl -N "http://127.0.0.1:9562/api/v1/events/$SID"   # watch the run live (SSE)
```

`chat` only **persists** the turn; `execute` is what actually **runs** the
loop; `events` streams it. See [`docs/guides/API.md`](../guides/API.md) for
the full HTTP/SSE surface.

### c) In-process Rust SDK (no server)

```rust
use bamboo_sdk::agent::{Agent, Session};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let home = dirs::home_dir().unwrap().join(".bamboo");
    let agent = Agent::builder()
        .model("claude-sonnet-4-6")
        .instruction("You are a helpful coding agent.")
        .with_defaults_for_data_dir(home)
        .await?
        .build()?;

    let session = Session::new("demo-session", "claude-sonnet-4-6");
    let mut rx = agent.run_stream(session, "List the files here and tell me what this project does.");
    while let Some(event) = rx.recv().await {
        println!("{event:?}");
    }
    Ok(())
}
```

Runs the exact same agent loop as `bamboo serve`, in your own process — see
[`examples/`](../../examples/) for compiling, runnable versions of this and
several other patterns (streaming real event types instead of `Debug`-printing,
a custom tool, resuming a session, the `ExecuteRequest` escape hatch,
connecting an MCP server).

## 4. What's next

| Want to... | Read |
|---|---|
| Know every `config.json` key | [Configuration reference](../config-reference.md) |
| Drive Bamboo from Telegram/Feishu | [Connect / IM bridge how-to](./CONNECT.md) |
| Install/trust a plugin (e.g. Nova) | [Plugins how-to](./PLUGINS.md) |
| Run it as a long-lived server | [Deploy how-to](./DEPLOY.md) |
| Embed the agent loop in your own Rust app | [`examples/`](../../examples/), the SDK section of the [README](../../README.md#use-it-as-a-rust-sdk-in-process) |
| See the full HTTP/SSE API | [`docs/guides/API.md`](./API.md) |
| Upgrade across a breaking change | [`docs/guides/MIGRATION_GUIDE.md`](./MIGRATION_GUIDE.md) |
| Understand the crate layout / architecture | [`docs/README.md`](../README.md) |
