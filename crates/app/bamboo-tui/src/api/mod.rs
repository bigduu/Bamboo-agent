pub mod sse;
pub mod types;

use anyhow::Result;
use reqwest::Client;

use types::*;

#[derive(Clone)]
pub struct BambooClient {
    pub base_url: String,
    client: Client,
}

impl BambooClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    // ── Chat ──

    pub async fn chat(&self, req: ChatRequest) -> Result<ChatResponse> {
        let resp = self
            .client
            .post(self.url("/api/v1/chat"))
            .json(&req)
            .send()
            .await?;
        let chat_resp = resp.json().await?;
        Ok(chat_resp)
    }

    pub async fn execute(&self, session_id: &str, model: Option<&str>) -> Result<ExecuteResponse> {
        let resp = self
            .client
            .post(self.url(&format!("/api/v1/execute/{}", session_id)))
            .json(&ExecuteRequest {
                model: model.map(|m| m.to_string()),
            })
            .send()
            .await?;
        // Without this check, a non-2xx (server down mid-request, 4xx/5xx, or a
        // JSON error body that fails to parse as `ExecuteResponse`) surfaces as
        // an opaque decode error at best — and at worst, if the body happens to
        // parse, silently pretends the run started. Either way the caller must
        // be able to tell "the run never started" apart from a real response so
        // it can stop waiting for SSE events that will never arrive.
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("execute failed ({status}): {}", body.trim());
        }
        let exec_resp = resp.json().await?;
        Ok(exec_resp)
    }

    pub async fn stop(&self, session_id: &str) -> Result<()> {
        self.client
            .post(self.url(&format!("/api/v1/stop/{}", session_id)))
            .send()
            .await?;
        Ok(())
    }

    /// Submit an answer to a pending question. Returns the server's
    /// `auto_resume_status` (`started` / `already_running` / `completed` /
    /// `error: …`) so the caller knows whether a run is actually resuming.
    pub async fn respond(&self, session_id: &str, response: &str) -> Result<String> {
        let resp = self
            .client
            .post(self.url(&format!("/api/v1/respond/{}", session_id)))
            .json(&RespondRequest {
                response: response.to_string(),
            })
            .send()
            .await?;
        // The respond validator rejects an answer that doesn't match an option
        // (when custom input is not allowed) with a non-2xx status. Surface that
        // so the UI can keep the question open instead of silently swallowing it.
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("server rejected the answer ({status}): {}", body.trim());
        }
        // Extract auto_resume_status; the run only actually resumes for
        // `started` / `already_running`.
        let auto_resume = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| {
                v.get("auto_resume_status")
                    .and_then(|s| s.as_str())
                    .map(String::from)
            })
            .unwrap_or_default();
        Ok(auto_resume)
    }

    // ── Session resume ──

    /// `GET /api/v1/history/{session_id}` — full message history, used to
    /// replay a resumed session's transcript into the Chat tab.
    pub async fn get_history(&self, session_id: &str) -> Result<HistoryResponse> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/history/{}", session_id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("get history failed ({status}): {body}");
        }
        let history: HistoryResponse = resp.json().await?;
        Ok(history)
    }

    /// `GET /api/v1/respond/{session_id}/pending` — the agent's currently
    /// pending question, if any. Used both by session resume (to pre-populate
    /// the question modal) and by the Ctrl+Q recovery path when no dismissed
    /// question is cached locally.
    pub async fn get_pending_question(&self, session_id: &str) -> Result<PendingQuestion> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/respond/{}/pending", session_id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("get pending question failed ({status}): {body}");
        }
        let pending: PendingQuestion = resp.json().await?;
        Ok(pending)
    }

    // ── Sessions ──

    /// `GET /api/v1/sessions`. The server wraps the page in an envelope
    /// (`total`/`limit`/`offset`/`next_offset`, #421/#252) rather than a bare
    /// array, so the caller can page through a large session list. Both
    /// params are optional — omitted, the server applies its own bounded
    /// default page.
    pub async fn list_sessions(
        &self,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<ListSessionsEnvelope> {
        let mut query: Vec<(&str, String)> = Vec::new();
        if let Some(limit) = limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(offset) = offset {
            query.push(("offset", offset.to_string()));
        }
        let resp = self
            .client
            .get(self.url("/api/v1/sessions"))
            .query(&query)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("list sessions failed ({status}): {body}");
        }
        let envelope: ListSessionsEnvelope = resp.json().await?;
        Ok(envelope)
    }

    /// `GET /api/v1/sessions/{session_id}` — unwraps the `{ "session": ... }`
    /// envelope. Used by session resume to get the model + `is_running` /
    /// `has_pending_question` flags the history endpoint doesn't carry.
    pub async fn get_session(&self, session_id: &str) -> Result<SessionSummary> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/sessions/{}", session_id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("get session failed ({status}): {body}");
        }
        let envelope: GetSessionEnvelope = resp.json().await?;
        Ok(envelope.session)
    }

    pub async fn delete_session(&self, id: &str) -> Result<()> {
        let resp = self
            .client
            .delete(self.url(&format!("/api/v1/sessions/{}", id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("delete session failed ({status}): {body}");
        }
        Ok(())
    }

    // ── Provider catalog (model picker, Ctrl+O) ──

    /// `GET /v1/bamboo/provider-catalog` — note the `/v1` prefix (like
    /// `get_config`/`set_config` below), not `/api/v1`.
    pub async fn get_provider_catalog(&self) -> Result<ProviderCatalog> {
        let resp = self
            .client
            .get(self.url("/v1/bamboo/provider-catalog"))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("get provider catalog failed ({status}): {}", body.trim());
        }
        let catalog: ProviderCatalog = resp.json().await?;
        Ok(catalog)
    }

    /// PATCH the active session's model after the picker applies a selection.
    /// Fire-and-forget from the caller's point of view (the model already
    /// took effect locally via `chat.model`) — this just keeps the server's
    /// session record from drifting, so a failure is reported via `notify`,
    /// never treated as fatal to the model change itself.
    pub async fn patch_session_model(&self, session_id: &str, model: &str) -> Result<()> {
        let resp = self
            .client
            .patch(self.url(&format!("/api/v1/sessions/{}", session_id)))
            .json(&PatchSessionModelRequest {
                model: model.to_string(),
            })
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("patch session model failed ({status}): {}", body.trim());
        }
        Ok(())
    }

    // ── MCP ──

    pub async fn list_mcp_servers(&self) -> Result<Vec<McpServer>> {
        let resp = self
            .client
            .get(self.url("/api/v1/mcp/servers"))
            .send()
            .await?;
        let servers = resp.json().await?;
        Ok(servers)
    }

    pub async fn connect_mcp(&self, id: &str) -> Result<()> {
        self.client
            .post(self.url(&format!("/api/v1/mcp/servers/{}/connect", id)))
            .send()
            .await?;
        Ok(())
    }

    pub async fn disconnect_mcp(&self, id: &str) -> Result<()> {
        self.client
            .post(self.url(&format!("/api/v1/mcp/servers/{}/disconnect", id)))
            .send()
            .await?;
        Ok(())
    }

    pub async fn get_mcp_tools(&self, id: &str) -> Result<Vec<ToolInfo>> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/mcp/servers/{}/tools", id)))
            .send()
            .await?;
        let tools = resp.json().await?;
        Ok(tools)
    }

    // ── Schedules ──

    pub async fn list_schedules(&self) -> Result<Vec<Schedule>> {
        let resp = self
            .client
            .get(self.url("/api/v1/schedules"))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("list schedules failed ({status}): {body}");
        }
        let parsed: ListSchedulesResponse = resp.json().await?;
        Ok(parsed
            .schedules
            .into_iter()
            .map(Schedule::from_view)
            .collect())
    }

    pub async fn create_schedule(&self, req: CreateScheduleRequest) -> Result<()> {
        let resp = self
            .client
            .post(self.url("/api/v1/schedules"))
            .json(&req)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("create schedule failed ({status}): {body}");
        }
        Ok(())
    }

    pub async fn delete_schedule(&self, id: &str) -> Result<()> {
        self.client
            .delete(self.url(&format!("/api/v1/schedules/{}", id)))
            .send()
            .await?;
        Ok(())
    }

    pub async fn run_schedule_now(&self, id: &str) -> Result<()> {
        self.client
            .post(self.url(&format!("/api/v1/schedules/{}/run", id)))
            .send()
            .await?;
        Ok(())
    }

    // ── Skills ──

    pub async fn list_skills(&self) -> Result<Vec<Skill>> {
        let resp = self.client.get(self.url("/v1/skills")).send().await?;
        let skills = resp.json().await?;
        Ok(skills)
    }

    pub async fn get_skill(&self, id: &str) -> Result<SkillDetail> {
        let resp = self
            .client
            .get(self.url(&format!("/v1/skills/{}", id)))
            .send()
            .await?;
        let detail = resp.json().await?;
        Ok(detail)
    }

    // ── Config ──

    pub async fn get_config(&self) -> Result<serde_json::Value> {
        let resp = self
            .client
            .get(self.url("/v1/bamboo/config"))
            .send()
            .await?;
        let val = resp.json().await?;
        Ok(val)
    }

    pub async fn set_config(&self, config: &serde_json::Value) -> Result<()> {
        let resp = self
            .client
            .post(self.url("/v1/bamboo/config"))
            .json(config)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("set config failed ({status}): {body}");
        }
        Ok(())
    }

    // ── Health ──

    pub async fn health(&self) -> Result<bool> {
        let resp = self.client.get(self.url("/api/v1/health")).send().await?;
        Ok(resp.status().is_success())
    }
}
