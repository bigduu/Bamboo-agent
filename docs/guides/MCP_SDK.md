# MCP client ownership and migration

Bamboo uses the official [Rust MCP SDK (`rmcp`)](https://github.com/modelcontextprotocol/rust-sdk). The dependency starts at 3.5.0, and `Cargo.lock` fixes the exact tested version. Protocol upgrades should update that dependency and run the interop/regression gates; do not add a second Bamboo protocol implementation.

`rmcp` owns JSON-RPC framing and dispatch, protocol versions and discovery with legacy initialization fallback, subscriptions and acknowledgement checks, cancellation messages, child-process transport, and Streamable HTTP routing/session/stream behavior.

Bamboo owns configuration, decrypted credentials and proxy policy, permission filtering, tool aliases and schema validation, runtime publication/fencing, operation deadlines, reconnect/health policy, and conversion into agent-facing results. These are application contracts rather than an MCP implementation. The SDK response cache is disabled so health probes and catalog refreshes observe current server failures.

## Supported connections

- `stdio`: uses the SDK child-process transport, with Bamboo's configured command, arguments, environment and working directory.
- `streamable_http`: uses the SDK Streamable HTTP transport, with Bamboo's configured HTTP client and headers. SSE response streams within this transport are managed by the SDK.

The retired two-endpoint `sse` transport has no runtime or compatibility adapter. Its configuration variant remains readable solely to preserve existing entries and return actionable migration guidance. It does not start a connection. Do not automatically replace an SSE URL: obtain the server's Streamable HTTP endpoint first.

For a remote server, use an explicit transport in the JSON configuration:

```json
{
  "mcpServers": {
    "remote-tools": {
      "url": "http://localhost:3000/mcp",
      "transport_kind": "streamable_http"
    }
  }
}
```

Embedded consumers constructing transports directly now use SDK transports with `McpProtocolClient::new(transport)` followed by `initialize(timeout_ms)`. The old `McpTransport`, `StdioTransport`, `SseTransport`, `StreamableHttpTransport` and hand-written wire-model exports are removed. `bamboo_mcp::rmcp` re-exports the SDK for those consumers. `McpServerManager` and its configuration-facing API remain the integration boundary for normal applications.

## Validation

Run `cargo test --locked -p bamboo-mcp --all-targets`, locked dependency metadata, formatting and Clippy, then the affected workspace suite. The interop suite exercises a real SDK server over byte-stream and Streamable HTTP transports, a historical initialization-only wire fixture, pagination, subscriptions, structured arrays/null, remote errors, and cancellation reaching the server before disconnect. Existing manager tests also run a real legacy stdio subprocess and preserve runtime publication/authority contracts.
