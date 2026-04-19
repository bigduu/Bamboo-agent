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

**Request Body:**

```json
{
  "message": "Help me write a function",
  "session_id": "optional-session-id",
  "model": "gpt-4o-mini",
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

**Path Parameters:**

- `session_id` - Session identifier from `/api/v1/chat`

**Request Body:**

```json
{
  "model": "claude-3-opus"
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
data: {"type":"TextDelta","delta":"Hello"}
data: {"type":"Complete","usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}
```

**Terminal Events:**

- `Complete` - Agent finished successfully
- `Error` - Agent encountered an error

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

---

### Session Management

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
    "model": "gpt-4o-mini"
  }'
```

Response includes `session_id` and `stream_url`.

### 2. Start Agent Execution

```bash
curl -X POST http://localhost:9562/api/v1/execute/{session_id} \
  -H "Content-Type: application/json" \
  -d '{"model": "gpt-4o-mini"}'
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

| Type | Description | Fields |
|------|-------------|--------|
| `TextDelta` | Partial text generation | `delta` |
| `ToolCall` | Agent calling a tool | `tool_call` |
| `ToolResult` | Tool execution completed | `tool_result` |
| `TokenBudgetUpdated` | Token usage updated | `usage` |
| `Complete` | Execution completed | `usage` |
| `Error` | Execution failed | `message` |

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
