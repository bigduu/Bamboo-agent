pub mod sse;
pub mod types;

use anyhow::Result;
use reqwest::Client;

use types::*;

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
        let resp = self.client.post(self.url("/api/v1/chat")).json(&req).send().await?;
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

    pub async fn respond(&self, session_id: &str, response: &str) -> Result<()> {
        self.client
            .post(self.url(&format!("/api/v1/respond/{}", session_id)))
            .json(&RespondRequest {
                response: response.to_string(),
            })
            .send()
            .await?;
        Ok(())
    }

    pub async fn pending_question(&self, session_id: &str) -> Result<PendingQuestion> {
        let resp = self
            .client
            .get(self.url(&format!(
                "/api/v1/respond/{}/pending",
                session_id
            )))
            .send()
            .await?;
        let pq = resp.json().await?;
        Ok(pq)
    }

    // ── Sessions ──

    pub async fn list_sessions(&self) -> Result<Vec<SessionSummary>> {
        let resp = self
            .client
            .get(self.url("/api/v1/sessions"))
            .send()
            .await?;
        let sessions = resp.json().await?;
        Ok(sessions)
    }

    pub async fn create_session(&self, req: CreateSessionRequest) -> Result<serde_json::Value> {
        let resp = self
            .client
            .post(self.url("/api/v1/sessions"))
            .json(&req)
            .send()
            .await?;
        let val = resp.json().await?;
        Ok(val)
    }

    pub async fn delete_session(&self, id: &str) -> Result<()> {
        self.client
            .delete(self.url(&format!("/api/v1/sessions/{}", id)))
            .send()
            .await?;
        Ok(())
    }

    pub async fn clear_session(&self, id: &str) -> Result<()> {
        self.client
            .post(self.url(&format!("/api/v1/sessions/{}/clear", id)))
            .send()
            .await?;
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
            .post(self.url(&format!(
                "/api/v1/mcp/servers/{}/disconnect",
                id
            )))
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

    pub async fn delete_mcp_server(&self, id: &str) -> Result<()> {
        self.client
            .delete(self.url(&format!("/api/v1/mcp/servers/{}", id)))
            .send()
            .await?;
        Ok(())
    }

    // ── Schedules ──

    pub async fn list_schedules(&self) -> Result<Vec<Schedule>> {
        let resp = self
            .client
            .get(self.url("/api/v1/schedules"))
            .send()
            .await?;
        let schedules = resp.json().await?;
        Ok(schedules)
    }

    pub async fn create_schedule(&self, req: CreateScheduleRequest) -> Result<Schedule> {
        let resp = self
            .client
            .post(self.url("/api/v1/schedules"))
            .json(&req)
            .send()
            .await?;
        let schedule = resp.json().await?;
        Ok(schedule)
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
        let resp = self.client.get(self.url("/v1/bamboo/config")).send().await?;
        let val = resp.json().await?;
        Ok(val)
    }

    pub async fn set_config(&self, config: &serde_json::Value) -> Result<()> {
        self.client
            .post(self.url("/v1/bamboo/config"))
            .json(config)
            .send()
            .await?;
        Ok(())
    }

    pub async fn fetch_models(&self) -> Result<Vec<ModelInfo>> {
        let resp = self
            .client
            .post(self.url("/v1/bamboo/settings/provider/models"))
            .send()
            .await?;
        let models = resp.json().await?;
        Ok(models)
    }

    // ── Health ──

    pub async fn health(&self) -> Result<bool> {
        let resp = self.client.get(self.url("/api/v1/health")).send().await?;
        Ok(resp.status().is_success())
    }
}
