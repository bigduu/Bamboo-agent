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

The interactive wizard prompts for a provider (`anthropic`, `openai`, or
`gemini`) and its API key. Scripts can run it non-interactively.

```bash
bamboo init --non-interactive --provider anthropic --api-key "sk-ant-..."
```

This writes `~/.bamboo/config.json` (override with `--data-dir`) and stores
the key **encrypted at rest** (see [encryption at
rest](../config-reference.md#encryption-at-rest)). Verify the install is
sound at any point with:

```bash
bamboo doctor    # checks the selected provider, model, and credential; reports local server reachability
```

### Use a custom gateway

To try a model from a shell or CI job, supply the gateway key, base URL, and
exact model ID in the process environment.

```bash
BAMBOO_OPENAI_API_KEY="$GATEWAY_API_KEY" \
BAMBOO_OPENAI_BASE_URL="https://gateway.example/v1" \
BAMBOO_OPENAI_MODEL="your-model-id" \
  bamboo -p "Reply with OK." --provider openai
```

Switch both the prefix and `--provider` for Anthropic Messages
(`BAMBOO_ANTHROPIC`, `anthropic`) or Gemini
(`BAMBOO_GEMINI`, `gemini`). An env-only setup can be checked with
`BAMBOO_PROVIDER=openai bamboo doctor` after exporting the matching variables.

For repeated local use, save the address and model with `init`. It prompts for
the API key and stores it encrypted.

```bash
bamboo init --provider openai --base-url https://gateway.example/v1 --model your-model-id
bamboo -p "Reply with OK."
```

For several accounts, save each under its own provider instance ID. Select
an admitted model with `bamboo -p "Reply with OK." -m work:your-model-id`.
[Provider configuration and precedence](../config-reference.md#choosing-a-provider-and-model).

## 3. Your first agent turn — three ways

### a) Headless one-shot (fastest way to see it work)

```bash
bamboo -p "List the files here and tell me what this project does."
```

Boots the full runtime (including sub-agent support), runs one turn, prints
the result, exits. Add `-s <session-id>` on a later call to continue the same
conversation. Plain mode reports the number of reasoning characters received
on stderr.

Use `--json` with the full headless command for one terminal `result` object on
stdout.

```bash
bamboo -p "Check the build." --json
printf '%s\n' 'Check the build.' | bamboo -p - --json
```

Use `--stream-json` (or `--raw`) for an NDJSON event stream that ends with the
same `result` object. Choose one output flag per invocation. Both modes write
logs to stderr and preserve the process exit code.

After argument parsing, startup failures also produce a `result` with
`status: "error"` and a null `session_id` when no session was opened. For success
and failure, `result.usage` contains `input_tokens`, `output_tokens` and
`total_tokens` from the engine's run-budget accounting for the root execution
started by this command. The counters include accounted retries and can use
engine estimates if the provider omits usage. Child executions and background
services have separate accounting.
Known pre-execution failures report zero, and a missing execution snapshot
reports `usage: null`. Each `-s` invocation reports its own execution.

### b) HTTP server + curl

With the server running, create the session and subscribe in terminal A:

```bash
SID=$(curl -s http://127.0.0.1:9562/api/v1/chat \
  -H 'Content-Type: application/json' \
  -d '{"message":"List the files here and tell me what this project does.","model":"claude-sonnet-4-6"}' \
  | jq -r .session_id)

printf 'Session: %s\n' "$SID"
curl -iNfsS "http://127.0.0.1:9562/api/v1/events/$SID"
```

Keep terminal A open. Wait for its HTTP 200 response headers, then copy the
printed session ID into terminal B and start execution:

```bash
SID="<session-id printed in terminal A>"
curl -fsS -X POST "http://127.0.0.1:9562/api/v1/execute/$SID" \
  -H 'Content-Type: application/json' -d '{}'
```

`chat` only **persists** the turn; `execute` is what actually **runs** the
loop; `events` streams it live. Subscribe before executing to avoid missing
response tokens. See [`docs/guides/API.md`](../guides/API.md) for
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
