# E2E Tests for Bamboo Agent

This directory contains comprehensive end-to-end tests for all Bamboo Agent API endpoints.

## Test Structure

```
tests/e2e/
├── mod.rs                  # Main module file
├── common/
│   └── mod.rs             # Common test utilities and helpers
├── health.rs              # Tests for /api/v1/health
├── chat.rs                # Tests for /api/v1/chat
├── execute.rs             # Tests for /api/v1/execute/{session_id}
├── events.rs              # Tests for /api/v1/events/{session_id}
├── history.rs             # Tests for /api/v1/history/{session_id}
├── task/                  # Tests for /api/v1/task/* endpoints
├── respond.rs             # Tests for /api/v1/respond/* endpoints
├── stop.rs                # Tests for /api/v1/stop/{session_id}
├── delete.rs              # Tests for /api/v1/sessions/{session_id}
├── metrics.rs             # Tests for /api/v1/metrics/* endpoints
├── mcp.rs                 # Tests for /api/v1/mcp/* endpoints
└── integration_tests.rs   # Full API integration tests
```

## Running Tests

### Run all e2e tests:
```bash
cargo test --test e2e
```

### Run specific test file:
```bash
cargo test --test e2e --test-threads=1 health
```

### Run specific test:
```bash
cargo test --test e2e test_health_endpoint
```

### Run with verbose output:
```bash
cargo test --test e2e -- --nocapture
```

## Test Coverage

The e2e tests cover the following API endpoints:

### Core Endpoints
- `POST /api/v1/chat` - Chat with the agent
- `POST /api/v1/execute/{session_id}` - Execute a task
- `GET /api/v1/events/{session_id}` - Stream events (SSE)
- `POST /api/v1/stop/{session_id}` - Stop execution
- `GET /api/v1/history/{session_id}` - Get session history

### Task Endpoints
- `GET /api/v1/task/{session_id}` - Get task list
- `GET /api/v1/task/{session_id}/exists` - Check if task list exists

### Respond Endpoints
- `POST /api/v1/respond/{session_id}` - Submit user response
- `GET /api/v1/respond/{session_id}/pending` - Get pending question

### Session Management
- `DELETE /api/v1/sessions/{session_id}` - Delete a session

### Metrics Endpoints
- `GET /api/v1/metrics/summary` - Get metrics summary
- `GET /api/v1/metrics/by-model` - Get metrics by model
- `GET /api/v1/metrics/sessions` - List all sessions
- `GET /api/v1/metrics/sessions/{session_id}` - Get session details
- `GET /api/v1/metrics/daily` - Get daily metrics
- `GET /api/v1/metrics/v2/summary` - V2 unified summary
- `GET /api/v1/metrics/v2/timeline` - V2 unified timeline

### MCP (Model Context Protocol) Endpoints
- `GET /api/v1/mcp/servers` - List MCP servers
- `POST /api/v1/mcp/servers` - Add MCP server
- `GET /api/v1/mcp/servers/{id}` - Get server details
- `PUT /api/v1/mcp/servers/{id}` - Update server
- `DELETE /api/v1/mcp/servers/{id}` - Delete server
- `POST /api/v1/mcp/servers/{id}/connect` - Connect to server
- `POST /api/v1/mcp/servers/{id}/disconnect` - Disconnect from server
- `POST /api/v1/mcp/servers/{id}/refresh` - Refresh server tools
- `GET /api/v1/mcp/servers/{id}/tools` - Get server tools
- `GET /api/v1/mcp/tools` - List all tools

### Health Check
- `GET /api/v1/health` - Health check endpoint

## Test Design Principles

1. **Isolation**: Each test is independent and uses isolated test data
2. **Fast**: Tests run quickly without requiring external services
3. **Deterministic**: Tests produce consistent results
4. **Clear**: Test names and assertions are self-documenting

## Adding New Tests

When adding new endpoints, create a corresponding test file:

1. Create `tests/e2e/new_endpoint.rs`
2. Import necessary modules:
   ```rust
   use actix_web::{test, web, App};
   use bamboo_agent::server::handlers;
   ```
3. Use the `create_test_app()` helper from `common`
4. Add the module to `tests/e2e/mod.rs`

## Test Utilities

The `common` module provides:

- `create_test_app()` - Creates a test AppState with temporary directory

## Notes

- Tests use actix-web's testing framework for fast in-memory testing
- No actual HTTP server is started for most tests
- Tests verify endpoint existence, routing, and basic response codes
- For tests requiring LLM interaction, proper mocking is needed (future work)
