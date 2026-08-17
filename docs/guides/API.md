# Bamboo API Documentation

Welcome to the Bamboo AI Agent API documentation. Bamboo provides a fully self-contained AI agent backend framework with built-in HTTP/HTTPS server capabilities.

## Overview

Bamboo offers RESTful API endpoints for creating and managing AI agent conversations, executing agent loops, and streaming real-time events.

## Base URL

```
http://localhost:9562/api/v1
```

## API Endpoints

### Chat Operations

#### Create Chat Message

```http
POST /api/v1/chat
```

Create a new chat session or add a message to an existing session.

Supply an optional `Idempotency-Key` header when a client may retry after an
ambiguous timeout. For 10 minutes, an equivalent retry returns the first
response without appending the message again. Reusing the key with another
payload returns `409 idempotency_key_conflict`. Receipts are process-local and
bounded; omitting the header preserves the normal behavior.

**Request Body:**

```json
{
  "message": "Help me write a function",
  "session_id": "optional-session-id",
  "model": "claude-sonnet-4-6",
  "system_prompt": "You are a helpful assistant",
  "enhance_prompt": "Additional instructions",
  "workspace_path": "/path/to/workspace"
}
```

**Response:** `201 Created`

```json
{
  "session_id": "uuid-string",
  "stream_url": "/api/v1/events/session-id",
  "status": "streaming"
}
```

**Next Steps:** After creating a chat, call `POST /api/v1/execute/{session_id}` to start the agent.

---

### Agent Execution

#### Execute Agent

```http
POST /api/v1/execute/{session_id}
```

Start the agent execution loop for a session.

`Idempotency-Key` has the same optional 10-minute replay contract as chat. An
equivalent retry returns the original status, body, and `run_id` without
starting another run. The canonical nested route
`POST /api/v1/sessions/{session_id}/execute` shares the same receipt.

**Path Parameters:**

- `session_id` - Session identifier from `/api/v1/chat`

**Request Body:**

```json
{
  "model": "claude-sonnet-4-6"
}
```

**Response:** `202 Accepted`

```json
{
  "session_id": "session-id",
  "status": "started",
  "events_url": "/api/v1/events/session-id"
}
```

**Note:** The `model` parameter is **required** and must be provided in every request.

---

### Event Streaming

#### Subscribe to Events (Recommended)

```http
GET /api/v1/events/{session_id}
```

Subscribe to real-time agent events via Server-Sent Events (SSE).

**Path Parameters:**

- `session_id` - Session identifier

**Response:** `200 OK` (text/event-stream)

**Event Format:**

```
data: {"type":"token","content":"Hello"}
data: {"type":"tool_start","tool_call_id":"call_1","tool_name":"Read","arguments":{"file_path":"README.md"}}
data: {"type":"tool_complete","tool_call_id":"call_1","result":{ /* ToolResult */ }}
data: {"type":"complete","usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}
```

The `type` discriminant is the snake_case form of the `AgentEvent` variant (the enum is `#[serde(tag = "type", rename_all = "snake_case")]`).

**Terminal Events:**

- `complete` - Agent finished successfully
- `cancelled` - Run cancelled by the user
- `error` - Agent encountered an error

**Example (JavaScript):**

```javascript
const eventSource = new EventSource('/api/v1/events/session-123');

eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Event:', data);

  if (data.type === 'Complete' || data.type === 'Error') {
    eventSource.close();
  }
};
```

> **Other event transports.** Besides the per-session `GET /api/v1/events/{session_id}` feed above, there is an **account-wide, resumable** change feed `GET /api/v1/stream` (SSE) that multiplexes events across all sessions — resume with `?since=<seq>` or the `Last-Event-ID` header. There is also a **live WebSocket** transport at `/v2/stream` (per-device token auth) which is the primary transport used by the web/desktop clients; the SSE feeds remain available for simple/curl clients.

---

### Session Management

#### Copy Session

```http
POST /api/v1/sessions/{session_id}/copy
```

Create an independent root session from an existing session. The copy receives
a new id and preserves the complete transcript, durable configuration,
permission mode, Project assignment, Workspace assignment, and attachments.
Attachment URLs are rewritten to the copied session id. Durable workflow
selection/activation snapshots are retained so the conversation can be
reconstructed; workflow run ids, lifecycle outbox/cache data, and other
transient execution, pending approval/question, child identity, schedule,
placement, and run-status state are not copied. Copying a child session always
produces an independent root with no parent chain.

The operation is failure-atomic: storage or attachment-copy failure leaves no
target session or index entry. Concurrent requests intentionally create
different copies.

**Response:** `201 Created` with `{ "session": SessionSummary }`, `404 Not
Found` when the source does not exist, or `500 Internal Server Error` when the
copy could not be committed.

---

#### Delete Session

```http
DELETE /api/v1/sessions/{session_id}
```

Delete a session and cancel any running execution.

**Path Parameters:**

- `session_id` - Session identifier

**Response:** `200 OK` (no body) or `404 Not Found`

**Side Effects:**

- Session removed from storage
- Session removed from memory
- Running execution cancelled

---

#### Get Session History

```http
GET /api/v1/sessions/{session_id}/history
```

Retrieve message history for a session.

**Path Parameters:**

- `session_id` - Session identifier

**Response:** `200 OK`

```json
{
  "session_id": "session-id",
  "messages": []
}
```

**Note:** Currently returns empty messages array. Full implementation planned.

---

### Execution Control

#### Stop Agent Execution

```http
POST /api/v1/stop/{session_id}
```

Cancel a running agent execution.

**Path Parameters:**

- `session_id` - Session identifier

**Response:** `200 OK`

```json
{
  "success": true,
  "message": "Agent execution stopped"
}
```

**Behavior:**

- Completes current LLM request
- Cancels pending tool executions
- Saves session state
- Updates status to `Cancelled`

---

### Interactive Questions

#### Get Pending Question

```http
GET /api/v1/sessions/{session_id}/question
```

Check if the agent is waiting for user input.

**Path Parameters:**

- `session_id` - Session identifier

**Response (Pending Question):** `200 OK`

```json
{
  "has_pending_question": true,
  "question": "Which language should I use?",
  "options": ["TypeScript", "JavaScript", "Python"],
  "allow_custom": false,
  "tool_call_id": "call_123"
}
```

**Response (No Question):** `200 OK`

```json
{
  "has_pending_question": false
}
```

---

#### Submit User Response

```http
POST /api/v1/sessions/{session_id}/respond
```

Submit a response to a pending question from the `conclusion_with_options` tool.

**Path Parameters:**

- `session_id` - Session identifier

**Request Body:**

```json
{
  "response": "TypeScript"
}
```

**Response:** `200 OK`

```json
{
  "success": true,
  "message": "Response recorded. Agent loop will continue.",
  "response": "TypeScript"
}
```

**Validation:** If `allow_custom` is false, response must match one of the provided options.

---

### Tool Execution

#### Execute Tool Directly

```http
POST /api/v1/tools/execute
```

Execute a built-in tool without running the full agent loop.

**Request Body:**

```json
{
  "tool_name": "read_file",
  "parameters": [
    {"name": "path", "value": "/path/to/file"}
  ]
}
```

**Response:** `200 OK`

```json
{
  "result": "{\"tool_name\":\"read_file\",\"result\":\"file contents\",\"display_preference\":\"Default\"}"
}
```

**Available Tools:**

- `read_file` - Read file contents
- `write_file` - Write file contents
- `execute_command` - Execute shell command
- `list_directory` - List directory contents
- `file_exists` - Check if file exists
- `get_file_info` - Get file metadata
- `git_status` - Get git repository status
- `git_diff` - Get git diff
- And more...

---

### Health Check

#### Health Check Endpoint

```http
GET /health
```

Simple health check for load balancers and monitoring.

**Response:** `200 OK` (plain text "OK")

**Usage:**

- Load balancer health probes
- Monitoring systems
- Kubernetes liveness/readiness probes

---

## Typical Workflow

### 1. Create a Chat Session

```bash
curl -X POST http://localhost:9562/api/v1/chat \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Help me write a Rust function",
    "model": "claude-sonnet-4-6"
  }'
```

Response includes `session_id` and `stream_url`.

### 2. Start Agent Execution

```bash
curl -X POST http://localhost:9562/api/v1/execute/{session_id} \
  -H "Content-Type: application/json" \
  -d '{"model": "claude-sonnet-4-6"}'
```

Response includes `events_url`.

### 3. Subscribe to Events

```javascript
const eventSource = new EventSource('/api/v1/events/{session_id}');
eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log(data);
  if (data.type === 'Complete' || data.type === 'Error') {
    eventSource.close();
  }
};
```

### 4. Handle Interactive Questions (Optional)

If the agent asks a question:

```bash
# Check for pending question
curl http://localhost:9562/api/v1/sessions/{session_id}/question

# Submit response
curl -X POST http://localhost:9562/api/v1/sessions/{session_id}/respond \
  -H "Content-Type: application/json" \
  -d '{"response": "Use async/await"}'
```

### 5. Stop or Delete (Optional)

```bash
# Stop execution
curl -X POST http://localhost:9562/api/v1/stop/{session_id}

# Delete session
curl -X DELETE http://localhost:9562/api/v1/sessions/{session_id}
```

---

## Error Handling

All endpoints return consistent error responses:

```json
{
  "error": "Error message",
  "session_id": "session-id"  // When applicable
}
```

**Common HTTP Status Codes:**

- `200 OK` - Success
- `201 Created` - Resource created
- `202 Accepted` - Request accepted for processing
- `400 Bad Request` - Invalid request or missing parameters
- `404 Not Found` - Resource not found
- `500 Internal Server Error` - Server error

---

## Event Types

### AgentEvent Types

The `type` field is the snake_case name of the `AgentEvent` variant
(`crates/core/bamboo-agent-core/src/agent/events.rs`). The most common
streaming variants:

| `type` | Description | Fields |
|--------|-------------|--------|
| `token` | Assistant text token | `content` |
| `reasoning_token` | Reasoning/thinking token | `content` |
| `tool_token` | Live output from a running tool | `tool_call_id`, `content` |
| `tool_start` | Tool execution started | `tool_call_id`, `tool_name`, `arguments` |
| `tool_complete` | Tool finished successfully | `tool_call_id`, `result` |
| `tool_error` | Tool failed | `tool_call_id`, `error` |
| `token_budget_updated` | Token usage / budget update | `usage` |
| `complete` | Execution finished | `usage` |
| `cancelled` | Run cancelled by the user | `message` |
| `error` | Execution failed | `message` |

The enum also carries session-, task-, plan-, and sub-agent-lifecycle variants
(e.g. `tool_lifecycle`, `need_clarification`, `task_list_updated`,
`sub_agent_started`, `plan_mode_entered`, `message_appended`); consult
`events.rs` for the exhaustive list and exact field shapes.

---

## Configuration

Bamboo can be configured via command-line flags or environment variables:

```bash
bamboo serve --port 9562 --data-dir ~/.local/share/bamboo
```

**Environment Variables:**

- `BAMBOO_PORT` - Server port (default: 9562)
- `BAMBOO_DATA_DIR` - Data directory
- `BAMBOO_BIND` - Bind address (default: 127.0.0.1)
- `BAMBOO_WORKSPACE_ROOT` - Root directory for session workspaces that have no
  explicit path (default: `<data-dir>/workspaces`). A session with no
  configured/explicit workspace gets `<workspace-root>/<session-id>` instead
  of the server process's working directory.
- `BAMBOO_WORKSPACE_CONFINE` - Set to `1`/`true` to require every explicit
  workspace path to be canonicalized and confined under
  `BAMBOO_WORKSPACE_ROOT` (escapes via `..`, a symlink, or an absolute path
  elsewhere are relocated under the root instead of honored as-is). Off by
  default for local single-user use, where pointing bamboo at an existing
  project directory anywhere on disk must keep working; implicitly enabled
  when `BAMBOO_WORKSPACE_ROOT` is set explicitly. Intended for orchestrated /
  multi-tenant deployments that want "one folder = one tenant's entire state".

### Runtime Config Patching (`POST /v1/bamboo/config`)

The live `config.json` (providers, subagents, notifications, MCP servers,
bamboo-connect platforms, ...) is updated with a partial JSON PATCH — you
only send the fields you want to change. Two rules:

- **An omitted key leaves the existing value unchanged.** This is
  unconditional back-compat: a patch that doesn't mention a field never
  touches it.
- **An explicit JSON `null` deletes that field**, opt-in per value
  ([RFC 7386](https://www.rfc-editor.org/rfc/rfc7386) JSON Merge Patch
  semantics). What "deleted" means depends on what's there:
  - an optional field (e.g. `subagents.claude_code_binary`) → cleared back
    to unset.
  - a whole object subtree (e.g. `notifications: null`) → reset to defaults.
  - one entry of a dynamic map (e.g. `provider_instances: {"<id>": null}`,
    `mcpServers: {"<name>": null}`) → that one entry removed, siblings
    untouched.
  - a whole array (e.g. `connect.platforms: null`) → emptied. A `null`
    *inside* an array element is never a delete marker — arrays are always
    replaced wholesale, not merged element-by-element.

  Secret fields (`api_key`, `token`, `device_key`, `app_secret`) treat
  `null` as an explicit clear, equivalent to sending `""` — both are
  distinct from a masked placeholder (`****...****`, which means "keep the
  existing secret").

  Choose your blast radius by choosing which level you null out: nulling a
  single leaf (`providers.openai.api_key: null`) clears just that field;
  nulling an enclosing object (`providers.openai: null`) wipes the whole
  provider's config.

See `bamboo_config::patch::deep_merge_json`'s doc comment for the full
semantics table and the precedence rules against masked-placeholder
resolution.

---

## Architecture

Bamboo follows a session-based architecture with unified server implementation:

1. **Session**: Contains conversation history and state
2. **Agent Loop**: Processes messages and executes tools
3. **LLM Provider**: Communicates with AI model APIs (OpenAI, Anthropic, Gemini, Copilot)
4. **Tool Executor**: Runs built-in tools (read, write, execute, etc.)
5. **Event Broadcaster**: Streams real-time events via Server-Sent Events
6. **Unified Server**: Single HTTP server with explicit routing (~120 routes)

### Server Architecture

- **`bamboo-server` crate**: Unified HTTP server with explicit routing
- **Explicit routing**: All routes registered in `crates/bamboo-server/src/routes/`
- **Direct provider access**: No HTTP callbacks to self (eliminates proxy pattern)
- **Handler organization** (`crates/bamboo-server/src/handlers/`):
  - Core agent handlers in `handlers/agent/` (chat, execute, events, stop, history, respond, etc.)
  - Provider handlers in `handlers/` (openai/, anthropic/, gemini/, copilot_auth/, agent_api.rs)
  - Feature handlers in `handlers/` (settings/, tools/, workspace/, skill/, command/)

---

## License

MIT License

---

## Support

- GitHub Issues: https://github.com/bigduu/Bamboo-agent/issues
- Documentation: https://docs.rs/bamboo-agent
