# 完整的 Endpoint 测试覆盖分析

## 1. 所有定义的 Routes

### `/api/v1/*` Scope (30 endpoints) - Agent API Routes

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/api/v1/chat` | POST | `agent::chat::handler` | ✅ `tests/e2e/chat.rs` | **覆盖** |
| `/api/v1/execute/{session_id}` | POST | `agent::execute::handler` | ✅ `tests/e2e/execute.rs` | **覆盖** |
| `/api/v1/events/{session_id}` | GET | `agent::events::handler` | ✅ `tests/e2e/events.rs` | **覆盖** |
| `/api/v1/stream/{session_id}` | GET | `agent::stream::handler` | ✅ `tests/e2e/stream.rs` | **覆盖** (deprecated) |
| `/api/v1/stop/{session_id}` | POST | `agent::stop::handler` | ✅ `tests/e2e/stop.rs` | **覆盖** |
| `/api/v1/history/{session_id}` | GET | `agent::history::handler` | ✅ `tests/e2e/history.rs` | **覆盖** |
| `/api/v1/todo/{session_id}` | GET | `agent::todo::get_todo_list` | ✅ `tests/e2e/todo.rs` | **覆盖** |
| `/api/v1/todo/{session_id}/exists` | GET | `agent::todo::has_todo_list` | ✅ `tests/e2e/todo.rs` | **覆盖** |
| `/api/v1/respond/{session_id}` | POST | `agent::respond::submit_response` | ✅ `tests/e2e/respond.rs` | **覆盖** |
| `/api/v1/respond/{session_id}/pending` | GET | `agent::respond::get_pending_question` | ✅ `tests/e2e/respond.rs` | **覆盖** |
| `/api/v1/sessions/{session_id}` | DELETE | `agent::delete::handler` | ✅ `tests/e2e/delete.rs` | **覆盖** |
| `/api/v1/health` | GET | `agent::health::handler` | ✅ `tests/e2e/health.rs` | **覆盖** |
| `/api/v1/metrics/summary` | GET | `agent::metrics::summary` | ✅ `tests/e2e/metrics.rs` | **覆盖** |
| `/api/v1/metrics/by-model` | GET | `agent::metrics::by_model` | ✅ `tests/e2e/metrics.rs` | **覆盖** |
| `/api/v1/metrics/sessions` | GET | `agent::metrics::sessions` | ✅ `tests/e2e/metrics.rs` | **覆盖** |
| `/api/v1/metrics/sessions/{session_id}` | GET | `agent::metrics::session_detail` | ✅ `tests/e2e/metrics.rs` | **覆盖** |
| `/api/v1/metrics/daily` | GET | `agent::metrics::daily` | ✅ `tests/e2e/metrics.rs` | **覆盖** |
| `/api/v1/metrics/forward/summary` | GET | `agent::metrics::forward_summary` | ✅ `tests/e2e/metrics_forward.rs` | **覆盖** |
| `/api/v1/metrics/forward/by-endpoint` | GET | `agent::metrics::forward_by_endpoint` | ✅ `tests/e2e/metrics_forward.rs` | **覆盖** |
| `/api/v1/metrics/forward/requests` | GET | `agent::metrics::forward_requests` | ✅ `tests/e2e/metrics_forward.rs` | **覆盖** |
| `/api/v1/mcp/servers` | GET | `agent::mcp::list_servers` | ✅ `tests/e2e/mcp.rs` | **覆盖** |
| `/api/v1/mcp/servers` | POST | `agent::mcp::add_server` | ✅ `tests/e2e/mcp.rs` | **覆盖** |
| `/api/v1/mcp/servers/{id}` | GET | `agent::mcp::get_server` | ✅ `tests/e2e/mcp.rs` | **覆盖** |
| `/api/v1/mcp/servers/{id}` | PUT | `agent::mcp::update_server` | ✅ `tests/e2e/mcp.rs` | **覆盖** |
| `/api/v1/mcp/servers/{id}` | DELETE | `agent::mcp::delete_server` | ✅ `tests/e2e/mcp.rs` | **覆盖** |
| `/api/v1/mcp/servers/{id}/connect` | POST | `agent::mcp::connect_server` | ✅ `tests/e2e/mcp.rs` | **覆盖** |
| `/api/v1/mcp/servers/{id}/disconnect` | POST | `agent::mcp::disconnect_server` | ✅ `tests/e2e/mcp.rs` | **覆盖** |
| `/api/v1/mcp/servers/{id}/refresh` | POST | `agent::mcp::refresh_tools` | ✅ `tests/e2e/mcp.rs` | **覆盖** |
| `/api/v1/mcp/servers/{id}/tools` | GET | `agent::mcp::get_server_tools` | ✅ `tests/e2e/mcp.rs` | **覆盖** |
| `/api/v1/mcp/tools` | GET | `agent::mcp::list_tools` | ✅ `tests/e2e/mcp.rs` | **覆盖** |

**覆盖率：30/30 (100%) ✅**

---

### `/v1/*` Scope - OpenAI Compatible Routes

#### `/v1/agent/*` - Claude Code Integration (11 endpoints)

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/v1/agent/projects` | GET | `agent_api::list_projects` | ❌ | **缺失** |
| `/v1/agent/projects` | POST | `agent_api::create_project` | ❌ | **缺失** |
| `/v1/agent/projects/{project_id}/sessions` | GET | `agent_api::get_project_sessions` | ❌ | **缺失** |
| `/v1/agent/settings` | GET | `agent_api::get_claude_settings` | ❌ | **缺失** |
| `/v1/agent/settings` | POST | `agent_api::save_claude_settings` | ❌ | **缺失** |
| `/v1/agent/system-prompt` | GET | `agent_api::get_system_prompt` | ❌ | **缺失** |
| `/v1/agent/system-prompt` | POST | `agent_api::save_system_prompt` | ❌ | **缺失** |
| `/v1/agent/sessions/running` | GET | `agent_api::list_running_claude_sessions` | ❌ | **缺失** |
| `/v1/agent/sessions/execute` | POST | `agent_api::execute_claude_code` | ❌ | **缺失** |
| `/v1/agent/sessions/cancel` | POST | `agent_api::cancel_claude_execution` | ❌ | **缺失** |
| `/v1/agent/sessions/{session_id}/jsonl` | GET | `agent_api::get_session_jsonl` | ❌ | **缺失** |

**覆盖率：0/11 (0%) ❌**

#### `/v1/commands/*` (2 endpoints)

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/v1/commands` | GET | `command::list_commands` | ❌ | **缺失** |
| `/v1/commands/{command_type}/{id}` | GET | `command::get_command` | ❌ | **缺失** |

**覆盖率：0/2 (0%) ❌**

#### `/v1/bamboo/*` - Settings & Configuration (20 endpoints)

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/v1/bamboo/workflows` | GET | `settings::list_workflows` | ❌ | **缺失** |
| `/v1/bamboo/workflows/{name}` | GET | `settings::get_workflow` | ❌ | **缺失** |
| `/v1/bamboo/workflows` | POST | `settings::save_workflow` | ❌ | **缺失** |
| `/v1/bamboo/workflows/{name}` | DELETE | `settings::delete_workflow` | ❌ | **缺失** |
| `/v1/bamboo/setup/status` | GET | `settings::get_setup_status` | ❌ | **缺失** |
| `/v1/bamboo/setup/complete` | POST | `settings::mark_setup_complete` | ❌ | **缺失** |
| `/v1/bamboo/setup/incomplete` | POST | `settings::mark_setup_incomplete` | ❌ | **缺失** |
| `/v1/bamboo/config` | GET | `settings::get_bamboo_config` | ❌ | **缺失** |
| `/v1/bamboo/config` | POST | `settings::set_bamboo_config` | ❌ | **缺失** |
| `/v1/bamboo/config/reset` | POST | `settings::reset_bamboo_config` | ❌ | **缺失** |
| `/v1/bamboo/proxy-auth` | POST | `settings::set_proxy_auth` | ❌ | **缺失** |
| `/v1/bamboo/proxy-auth/status` | GET | `settings::get_proxy_auth_status` | ❌ | **缺失** |
| `/v1/bamboo/keyword-masking` | GET | `settings::get_keyword_masking_config` | ❌ | **缺失** |
| `/v1/bamboo/keyword-masking` | POST | `settings::update_keyword_masking_config` | ❌ | **缺失** |
| `/v1/bamboo/keyword-masking/validate` | POST | `settings::validate_keyword_entries` | ❌ | **缺失** |
| `/v1/bamboo/settings/provider` | GET | `settings::get_provider_config` | ❌ | **缺失** |
| `/v1/bamboo/settings/provider` | POST | `settings::update_provider_config` | ❌ | **缺失** |
| `/v1/bamboo/settings/provider/models` | POST | `settings::fetch_provider_models` | ❌ | **缺失** |
| `/v1/bamboo/settings/reload` | POST | `settings::reload_provider_config` | ❌ | **缺失** |
| `/v1/bamboo/anthropic-model-mapping` | GET | `settings::get_anthropic_model_mapping` | ❌ | **缺失** |
| `/v1/bamboo/anthropic-model-mapping` | POST | `settings::set_anthropic_model_mapping` | ❌ | **缺失** |

**覆盖率：0/21 (0%) ❌**

#### `/v1/skills/*` (5 endpoints)

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/v1/skills` | GET | `skill::list_skills` | ✅ `tests/e2e/skills.rs` | **覆盖** |
| `/v1/skills/available-tools` | GET | `skill::get_available_tools` | ✅ `tests/e2e/skills.rs` | **覆盖** |
| `/v1/skills/filtered-tools` | GET | `skill::get_filtered_tools` | ✅ `tests/e2e/skills.rs` | **覆盖** |
| `/v1/skills/available-workflows` | GET | `skill::get_available_workflows` | ✅ `tests/e2e/skills.rs` | **覆盖** |
| `/v1/skills/{id}` | GET | `skill::get_skill` | ✅ `tests/e2e/skills.rs` | **覆盖** |

**覆盖率：5/5 (100%) ✅**

#### `/v1/tools/*` (1 endpoint)

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/v1/tools/execute` | POST | `tools::execute_tool` | ❌ | **缺失** |

**覆盖率：0/1 (0%) ❌**

#### `/v1/workspace/*` (6 endpoints)

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/v1/workspace/validate` | POST | `workspace::validate_workspace` | ❌ | **缺失** |
| `/v1/workspace/recent` | GET | `workspace::get_recent_workspaces` | ❌ | **缺失** |
| `/v1/workspace/recent` | POST | `workspace::add_recent_workspace` | ❌ | **缺失** |
| `/v1/workspace/suggestions` | GET | `workspace::get_workspace_suggestions` | ❌ | **缺失** |
| `/v1/workspace/browse-folder` | POST | `workspace::browse_folder` | ❌ | **缺失** |
| `/v1/workspace/files` | POST | `workspace::list_workspace_files` | ❌ | **缺失** |

**覆盖率：0/6 (0%) ❌**

#### `/v1/bamboo/copilot/*` (5 endpoints)

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/v1/bamboo/copilot/auth/start` | POST | `copilot_auth::start_copilot_auth` | ❌ | **缺失** |
| `/v1/bamboo/copilot/auth/complete` | POST | `copilot_auth::complete_copilot_auth` | ❌ | **缺失** |
| `/v1/bamboo/copilot/authenticate` | POST | `copilot_auth::authenticate_copilot` | ❌ | **缺失** |
| `/v1/bamboo/copilot/auth/status` | POST | `copilot_auth::get_copilot_auth_status` | ❌ | **缺失** |
| `/v1/bamboo/copilot/logout` | POST | `copilot_auth::logout_copilot` | ❌ | **缺失** |

**覆盖率：0/5 (0%) ❌**

#### OpenAI Core (2 endpoints)

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/v1/chat/completions` | POST | `openai::chat_completions` | ❌ | **缺失** |
| `/v1/models` | GET | `openai::get_models` | ❌ | **缺失** |

**覆盖率：0/2 (0%) ❌**

**`/v1/*` 总覆盖率：5/53 (9.4%) ❌**

---

### `/anthropic/v1/*` Scope (3 endpoints)

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/anthropic/v1/messages` | POST | `anthropic::messages` | ❌ | **缺失** |
| `/anthropic/v1/complete` | POST | `anthropic::complete` | ❌ | **缺失** |
| `/anthropic/v1/models` | GET | `anthropic::get_models` | ❌ | **缺失** |

**覆盖率：0/3 (0%) ❌**

---

### `/gemini/v1beta/*` Scope (3 endpoints)

| Endpoint | Method | Handler | E2E Test | Status |
|----------|--------|---------|----------|--------|
| `/gemini/v1beta/models` | GET | `gemini::list_models` | ❌ | **缺失** |
| `/gemini/v1beta/models/{model}:generateContent` | POST | `gemini::generate_content` | ❌ | **缺失** |
| `/gemini/v1beta/models/{model}:streamGenerateContent` | POST | `gemini::stream_generate_content` | ❌ | **缺失** |

**覆盖率：0/3 (0%) ❌**

---

## 总结

### 总体覆盖率统计

| Scope | 总数 | 已覆盖 | 覆盖率 |
|-------|------|--------|--------|
| `/api/v1/*` | 30 | 30 | 100% ✅ |
| `/v1/agent/*` | 11 | 0 | 0% ❌ |
| `/v1/commands/*` | 2 | 0 | 0% ❌ |
| `/v1/bamboo/*` | 21 | 0 | 0% ❌ |
| `/v1/skills/*` | 5 | 5 | 100% ✅ |
| `/v1/tools/*` | 1 | 0 | 0% ❌ |
| `/v1/workspace/*` | 6 | 0 | 0% ❌ |
| `/v1/bamboo/copilot/*` | 5 | 0 | 0% ❌ |
| `/v1/chat/completions + models` | 2 | 0 | 0% ❌ |
| `/anthropic/v1/*` | 3 | 0 | 0% ❌ |
| `/gemini/v1beta/*` | 3 | 0 | 0% ❌ |
| **总计** | **89** | **35** | **39.3%** |

---

## 优先级建议

### 🔴 高优先级（核心功能，立即补充）

1. **`/v1/chat/completions`** - OpenAI 兼容的聊天 endpoint，核心功能
2. **`/v1/models`** - 模型列表，基础查询功能
3. **`/v1/workspace/*` (6 endpoints)** - 工作区管理，前端依赖度高
4. **`/v1/bamboo/config` (GET/POST)** - 配置管理，核心功能
5. **`/v1/bamboo/settings/provider` (GET/POST)** - Provider 配置，核心功能

### 🟡 中优先级（重要功能，近期补充）

6. **`/v1/commands/*` (2 endpoints)** - 命令系统
7. **`/v1/tools/execute`** - 工具执行
8. **`/v1/bamboo/workflows/*` (4 endpoints)** - Workflow 管理
9. **`/anthropic/v1/*` (3 endpoints)** - Anthropic 兼容 API
10. **`/gemini/v1beta/*` (3 endpoints)** - Gemini 兼容 API

### 🟢 低优先级（可选功能，后续补充）

11. **`/v1/agent/*` (11 endpoints)** - Claude Code 集成（特定场景）
12. **`/v1/bamboo/copilot/*` (5 endpoints)** - Copilot 认证（特定场景）
13. **`/v1/bamboo/keyword-masking/*` (3 endpoints)** - 关键词屏蔽配置
14. **`/v1/bamboo/setup/*` (3 endpoints)** - 安装向导状态
15. **`/v1/bamboo/proxy-auth/*` (2 endpoints)** - 代理认证

---

## 建议行动计划

### Phase 1: 核心功能测试（本周）
- [ ] `/v1/chat/completions` + `/v1/models`
- [ ] `/v1/workspace/*` (6 endpoints)
- [ ] `/v1/bamboo/config` + `/v1/bamboo/settings/provider`

### Phase 2: 兼容性 API 测试（下周）
- [ ] `/anthropic/v1/*` (3 endpoints)
- [ ] `/gemini/v1beta/*` (3 endpoints)
- [ ] `/v1/commands/*` (2 endpoints)
- [ ] `/v1/tools/execute`

### Phase 3: 管理功能测试（第三周）
- [ ] `/v1/bamboo/workflows/*` (4 endpoints)
- [ ] `/v1/agent/*` (11 endpoints)

### Phase 4: 特定场景测试（后续）
- [ ] `/v1/bamboo/copilot/*` (5 endpoints)
- [ ] 其他 settings endpoints

---

## 关键发现

1. **测试覆盖极不均衡**：
   - `/api/v1/*` scope 100% 覆盖
   - `/v1/*` scope 只有 9.4% 覆盖（仅 skills）

2. **高风险 endpoints**：
   - `/v1/chat/completions` - 最常用的 endpoint，**零测试**
   - `/v1/workspace/*` - 前端重度依赖，**零测试**
   - `/anthropic/v1/*` 和 `/gemini/v1beta/*` - 多 provider 支持，**零测试**

3. **架构盲点**：
   - 之前的问题（`/v1/skills` 类型错误）就是这个盲区的直接后果
   - 需要建立机制确保每个新 route 都有对应测试

4. **CI/CD 改进**：
   - 添加测试覆盖率检查
   - 新增 route 必须有对应测试才能合并
   - 定期生成覆盖率报告
