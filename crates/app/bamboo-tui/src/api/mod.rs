pub mod sse;
pub mod types;

use anyhow::Result;
use reqwest::Client;
use std::fmt;

use types::*;

#[derive(Debug, Clone)]
pub struct VersionedSession {
    pub summary: SessionSummary,
    pub metadata_version: u64,
}

/// Recoverable failure from a session metadata PATCH. The picker keeps its
/// query, selected row, and rename draft open for every variant; `conflict`
/// specifically identifies a 412 so the UI can explain the refetch/retry path.
#[derive(Debug, Clone)]
pub struct SessionMutationFailure {
    pub conflict: bool,
    pub current_version: Option<u64>,
    message: String,
}

impl SessionMutationFailure {
    fn transport(error: reqwest::Error) -> Self {
        Self {
            conflict: false,
            current_version: None,
            message: format!("session update transport failed: {error}"),
        }
    }

    fn protocol(message: impl Into<String>) -> Self {
        Self {
            conflict: false,
            current_version: None,
            message: message.into(),
        }
    }

    fn rejected(status: reqwest::StatusCode, body: String, etag: Option<u64>) -> Self {
        let body = body.trim();
        Self {
            conflict: status == reqwest::StatusCode::PRECONDITION_FAILED,
            current_version: etag,
            message: if body.is_empty() {
                format!("session update failed ({status})")
            } else {
                format!("session update failed ({status}): {body}")
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn test_conflict(current_version: u64) -> Self {
        Self {
            conflict: true,
            current_version: Some(current_version),
            message: "version conflict".to_string(),
        }
    }
}

impl fmt::Display for SessionMutationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SessionMutationFailure {}

fn parse_etag(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    let raw = headers.get(reqwest::header::ETAG)?.to_str().ok()?.trim();
    raw.strip_prefix("W/")
        .unwrap_or(raw)
        .trim()
        .trim_matches('"')
        .parse()
        .ok()
}

#[derive(Debug, Clone)]
pub struct RespondFailure {
    refresh_question: bool,
    message: String,
}

impl RespondFailure {
    fn transport(error: reqwest::Error) -> Self {
        Self {
            // Once the POST has been sent, a transport failure leaves its
            // outcome unknown: the server may have consumed the question and
            // resumed before the response was lost. Reconcile authoritative
            // state instead of enabling a blind duplicate submission.
            refresh_question: true,
            message: format!("answer response transport failed: {error}"),
        }
    }

    fn protocol(message: impl Into<String>) -> Self {
        Self {
            // A successful status with an unreadable/invalid body is the same
            // unknown-outcome class as a lost response body.
            refresh_question: true,
            message: message.into(),
        }
    }

    pub(crate) fn rejected(status: reqwest::StatusCode, body: String) -> Self {
        let refresh_question = status == reqwest::StatusCode::CONFLICT
            || (status == reqwest::StatusCode::BAD_REQUEST && body.contains("No pending question"));
        Self {
            refresh_question,
            message: format!("server rejected the answer ({status}): {}", body.trim()),
        }
    }

    /// A conflict means the tool identity changed; a bad request can mean the
    /// pending question was already consumed. In both cases the modal must be
    /// reconciled with the server instead of blindly retrying stale state.
    pub fn should_refresh_question(&self) -> bool {
        self.refresh_question
    }

    pub(crate) fn unavailable(message: impl Into<String>) -> Self {
        Self {
            refresh_question: false,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RespondFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RespondFailure {}

fn parse_auto_resume_status(body: &str) -> std::result::Result<String, RespondFailure> {
    let value: serde_json::Value = serde_json::from_str(body).map_err(|error| {
        RespondFailure::protocol(format!("invalid answer response JSON: {error}"))
    })?;
    let status = value
        .get("auto_resume_status")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            RespondFailure::protocol("answer response is missing string field 'auto_resume_status'")
        })?;

    match status {
        "started" | "already_running" | "completed" | "error: session not found" => {
            Ok(status.to_string())
        }
        other => Err(RespondFailure::protocol(format!(
            "answer response returned unsupported auto_resume_status '{other}'"
        ))),
    }
}

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
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("chat failed ({status}): {}", body.trim());
        }
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
        let resp = self
            .client
            .post(self.url(&format!("/api/v1/stop/{}", session_id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("stop failed ({status}): {}", body.trim());
        }
        Ok(())
    }

    /// Submit an answer to a pending question. Returns the server's
    /// `auto_resume_status` (`started` / `already_running` / `completed` /
    /// `error: …`) so the caller knows whether a run is actually resuming.
    pub async fn respond(
        &self,
        session_id: &str,
        response: &str,
        expected_tool_call_id: Option<&str>,
    ) -> std::result::Result<String, RespondFailure> {
        let resp = self
            .client
            .post(self.url(&format!("/api/v1/respond/{}", session_id)))
            .json(&RespondRequest {
                response: response.to_string(),
                expected_tool_call_id: expected_tool_call_id.map(str::to_string),
            })
            .send()
            .await
            .map_err(RespondFailure::transport)?;
        // The respond validator rejects an answer that doesn't match an option
        // (when custom input is not allowed) with a non-2xx status. Surface that
        // so the UI can keep the question open instead of silently swallowing it.
        let status = resp.status();
        let body = resp.text().await.map_err(|error| {
            RespondFailure::protocol(format!("failed to read answer response body: {error}"))
        })?;
        if !status.is_success() {
            return Err(RespondFailure::rejected(status, body));
        }
        // Do not turn an unknown successful response into an empty status:
        // that would clear the modal and detach the fresh successor stream.
        parse_auto_resume_status(&body)
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

    /// Fetch a session together with its metadata ETag. Rename and pin flows
    /// retain this version while the operator edits, then send it back via
    /// `If-Match` so concurrent updates become a recoverable 412 instead of a
    /// silent last-writer-wins overwrite.
    pub async fn get_session_versioned(&self, session_id: &str) -> Result<VersionedSession> {
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
        let metadata_version = parse_etag(resp.headers())
            .ok_or_else(|| anyhow::anyhow!("get session response omitted metadata ETag"))?;
        let envelope: GetSessionEnvelope = resp.json().await?;
        Ok(VersionedSession {
            summary: envelope.session,
            metadata_version,
        })
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

    pub async fn patch_session_metadata(
        &self,
        session_id: &str,
        expected_version: u64,
        patch: &PatchSessionMetadataRequest,
    ) -> std::result::Result<VersionedSession, SessionMutationFailure> {
        let resp = self
            .client
            .patch(self.url(&format!("/api/v1/sessions/{}", session_id)))
            .header(reqwest::header::IF_MATCH, format!("\"{expected_version}\""))
            .json(patch)
            .send()
            .await
            .map_err(SessionMutationFailure::transport)?;
        let status = resp.status();
        let metadata_version = parse_etag(resp.headers());
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(SessionMutationFailure::rejected(
                status,
                body,
                metadata_version,
            ));
        }
        let metadata_version = metadata_version.ok_or_else(|| {
            SessionMutationFailure::protocol("session PATCH response omitted metadata ETag")
        })?;
        let envelope: GetSessionEnvelope = resp.json().await.map_err(|error| {
            SessionMutationFailure::protocol(format!(
                "session PATCH returned an unreadable response: {error}"
            ))
        })?;
        Ok(VersionedSession {
            summary: envelope.session,
            metadata_version,
        })
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

    /// PATCH the active session's model before the picker commits a selection
    /// locally. The caller keeps the overlay open on failure so the visible
    /// model badge can never drift from the persisted session record.
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
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("list mcp servers failed ({status}): {body}");
        }
        let servers = resp.json().await?;
        Ok(servers)
    }

    pub async fn connect_mcp(&self, id: &str) -> Result<()> {
        let resp = self
            .client
            .post(self.url(&format!("/api/v1/mcp/servers/{}/connect", id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("connect mcp failed ({status}): {}", body.trim());
        }
        Ok(())
    }

    pub async fn disconnect_mcp(&self, id: &str) -> Result<()> {
        let resp = self
            .client
            .post(self.url(&format!("/api/v1/mcp/servers/{}/disconnect", id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("disconnect mcp failed ({status}): {}", body.trim());
        }
        Ok(())
    }

    pub async fn get_mcp_tools(&self, id: &str) -> Result<Vec<ToolInfo>> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/mcp/servers/{}/tools", id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("get mcp tools failed ({status}): {body}");
        }
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
        let resp = self
            .client
            .delete(self.url(&format!("/api/v1/schedules/{}", id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("delete schedule failed ({status}): {body}");
        }
        Ok(())
    }

    pub async fn run_schedule_now(&self, id: &str) -> Result<()> {
        let resp = self
            .client
            .post(self.url(&format!("/api/v1/schedules/{}/run", id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("run schedule failed ({status}): {body}");
        }
        Ok(())
    }

    // ── Skills ──

    pub async fn list_skills(&self) -> Result<Vec<Skill>> {
        let resp = self.client.get(self.url("/v1/skills")).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("list skills failed ({status}): {body}");
        }
        let skills = resp.json().await?;
        Ok(skills)
    }

    pub async fn get_skill(&self, id: &str) -> Result<SkillDetail> {
        let resp = self
            .client
            .get(self.url(&format!("/v1/skills/{}", id)))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("get skill failed ({status}): {body}");
        }
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
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("get config failed ({status}): {body}");
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
        use tokio::io::AsyncReadExt;

        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        let header_end = loop {
            if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
            let read = stream.read(&mut chunk).await.unwrap();
            if read == 0 {
                break request.len();
            }
            request.extend_from_slice(&chunk[..read]);
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        while request.len() < header_end + content_length {
            let read = stream.read(&mut chunk).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    async fn respond(stream: &mut tokio::net::TcpStream, status: &str, etag: &str, body: &str) {
        use tokio::io::AsyncWriteExt;

        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nETag: {etag}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
    }

    #[test]
    fn auto_resume_status_accepts_only_server_contract_values() {
        for status in [
            "started",
            "already_running",
            "completed",
            "error: session not found",
        ] {
            let body = format!(r#"{{"auto_resume_status":"{status}"}}"#);
            assert_eq!(parse_auto_resume_status(&body).unwrap(), status);
        }
    }

    #[test]
    fn malformed_missing_and_unknown_resume_status_require_reconciliation() {
        for body in [
            "not-json",
            r#"{"success":true}"#,
            r#"{"auto_resume_status":7}"#,
            r#"{"auto_resume_status":"future_status"}"#,
        ] {
            let error = parse_auto_resume_status(body).unwrap_err();
            assert!(error.should_refresh_question(), "body: {body}");
        }
    }

    #[tokio::test]
    async fn metadata_patch_round_trips_get_etag_and_if_match() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut get_socket, _) = listener.accept().await.unwrap();
            let get = read_request(&mut get_socket).await;
            assert!(get.starts_with("GET /api/v1/sessions/s1 HTTP/1.1"));
            respond(
                &mut get_socket,
                "200 OK",
                "W/\"7\"",
                r#"{"session":{"id":"s1","title":"Before"}}"#,
            )
            .await;

            let (mut patch_socket, _) = listener.accept().await.unwrap();
            let patch = read_request(&mut patch_socket).await;
            assert!(patch.starts_with("PATCH /api/v1/sessions/s1 HTTP/1.1"));
            assert!(patch.to_ascii_lowercase().contains("if-match: \"7\""));
            assert!(patch.ends_with(r#"{"title":"After"}"#));
            respond(
                &mut patch_socket,
                "200 OK",
                "\"8\"",
                r#"{"session":{"id":"s1","title":"After"}}"#,
            )
            .await;
        });

        let client = BambooClient::new(&base_url);
        let current = client.get_session_versioned("s1").await.unwrap();
        assert_eq!(current.metadata_version, 7);
        assert_eq!(current.summary.title, "Before");

        let updated = client
            .patch_session_metadata(
                "s1",
                current.metadata_version,
                &PatchSessionMetadataRequest {
                    title: Some("After".to_string()),
                    pinned: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.metadata_version, 8);
        assert_eq!(updated.summary.title, "After");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn metadata_patch_surfaces_412_and_current_etag() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let patch = read_request(&mut socket).await;
            assert!(patch.to_ascii_lowercase().contains("if-match: \"4\""));
            respond(
                &mut socket,
                "412 Precondition Failed",
                "\"5\"",
                r#"{"error":"metadata changed"}"#,
            )
            .await;
        });

        let failure = BambooClient::new(&base_url)
            .patch_session_metadata(
                "s1",
                4,
                &PatchSessionMetadataRequest {
                    title: None,
                    pinned: Some(true),
                },
            )
            .await
            .unwrap_err();
        assert!(failure.conflict);
        assert_eq!(failure.current_version, Some(5));
        assert!(failure.to_string().contains("metadata changed"));
        server.await.unwrap();
    }
}
