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

    pub(crate) fn protocol(message: impl Into<String>) -> Self {
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

/// Recoverable failure from a revisioned permission-policy mutation. A 409 is
/// kept typed so the editor can retain its draft, refresh the authoritative
/// revision, and require an explicit retry instead of replaying blindly.
#[derive(Debug, Clone)]
pub struct PermissionMutationFailure {
    pub conflict: bool,
    message: String,
}

impl PermissionMutationFailure {
    fn transport(error: reqwest::Error) -> Self {
        Self {
            conflict: false,
            message: format!("permission policy transport failed: {error}"),
        }
    }

    fn protocol(message: impl Into<String>) -> Self {
        Self {
            conflict: false,
            message: message.into(),
        }
    }

    fn rejected(status: reqwest::StatusCode, body: String) -> Self {
        let body = body.trim();
        Self {
            conflict: status == reqwest::StatusCode::CONFLICT,
            message: if body.is_empty() {
                format!("permission policy update failed ({status})")
            } else {
                format!("permission policy update failed ({status}): {body}")
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn test_conflict() -> Self {
        Self {
            conflict: true,
            message: "revision conflict".to_string(),
        }
    }
}

impl fmt::Display for PermissionMutationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PermissionMutationFailure {}

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
            || status == reqwest::StatusCode::NOT_FOUND
            || status == reqwest::StatusCode::GONE
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

    // ── Command catalog ──

    /// List Bamboo's authoritative command catalog in the active Session's
    /// Project/workspace scope. Omitting `session_id` intentionally requests
    /// only the global catalog for a not-yet-created chat.
    pub async fn list_commands(&self, session_id: Option<&str>) -> Result<CommandListResponse> {
        let mut request = self.client.get(self.url("/api/v1/commands"));
        if let Some(session_id) = session_id.filter(|value| !value.trim().is_empty()) {
            request = request.query(&[("session_id", session_id)]);
        }
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("list commands failed ({status}): {}", body.trim());
        }
        Ok(response.json().await?)
    }

    /// Resolve a prompt/workflow command into preview content. Path segments
    /// are appended through `Url::path_segments_mut`, so namespaced prompt
    /// names such as `db/migrate` remain one percent-encoded route parameter.
    pub async fn get_command(
        &self,
        command_type: &str,
        name: &str,
        session_id: Option<&str>,
        arguments: Option<&str>,
    ) -> Result<CommandDetail> {
        let mut url = reqwest::Url::parse(&self.base_url)?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("Bamboo base URL cannot contain command paths"))?;
            segments.pop_if_empty();
            segments.extend(["api", "v1", "commands", command_type, name]);
        }
        {
            let mut query = url.query_pairs_mut();
            if let Some(session_id) = session_id.filter(|value| !value.trim().is_empty()) {
                query.append_pair("session_id", session_id);
            }
            if let Some(arguments) = arguments.filter(|value| !value.is_empty()) {
                query.append_pair("arguments", arguments);
            }
        }
        let response = self.client.get(url).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("resolve command failed ({status}): {}", body.trim());
        }
        Ok(response.json().await?)
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

    pub async fn execute(
        &self,
        session_id: &str,
        model: Option<&str>,
        provider: Option<&str>,
        reasoning_effort: Option<ReasoningEffort>,
    ) -> Result<ExecuteResponse> {
        let resp = self
            .client
            .post(self.url(&format!("/api/v1/execute/{}", session_id)))
            .json(&ExecuteRequest {
                model: model.map(|m| m.to_string()),
                provider: provider.map(str::to_string),
                reasoning_effort,
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

    /// Submit one machine-readable decision to the typed permission endpoint.
    /// Request id, allowed scope, matcher, policy revision, and global
    /// confirmation remain structured all the way to the server.
    pub async fn submit_permission_decision(
        &self,
        session_id: &str,
        decision: &PermissionDecision,
    ) -> std::result::Result<PermissionDecisionResponse, RespondFailure> {
        let resp = self
            .client
            .post(self.url(&format!(
                "/api/v1/sessions/{session_id}/permission-decisions"
            )))
            .json(decision)
            .send()
            .await
            .map_err(RespondFailure::transport)?;
        let status = resp.status();
        let body = resp.text().await.map_err(|error| {
            RespondFailure::protocol(format!(
                "failed to read permission decision response: {error}"
            ))
        })?;
        if !status.is_success() {
            return Err(RespondFailure::rejected(status, body));
        }
        let response: PermissionDecisionResponse =
            serde_json::from_str(&body).map_err(|error| {
                RespondFailure::protocol(format!(
                    "invalid permission decision response JSON: {error}"
                ))
            })?;
        if !response.success {
            return Err(RespondFailure::protocol(
                "permission decision response did not confirm success",
            ));
        }
        Ok(response)
    }

    /// Deliver a checked, one-shot decision to a blocked child agent. The route
    /// consumes the exact child/request tuple; a replay is a safe 404.
    pub async fn submit_child_approval(
        &self,
        child_session_id: &str,
        decision: &ChildApprovalDecision,
    ) -> std::result::Result<ChildApprovalResponse, RespondFailure> {
        let resp = self
            .client
            .post(self.url(&format!(
                "/api/v1/sessions/{child_session_id}/child-approval"
            )))
            .json(decision)
            .send()
            .await
            .map_err(RespondFailure::transport)?;
        let status = resp.status();
        let body = resp.text().await.map_err(|error| {
            RespondFailure::protocol(format!("failed to read child approval response: {error}"))
        })?;
        if !status.is_success() {
            return Err(RespondFailure::rejected(status, body));
        }
        let response: ChildApprovalResponse = serde_json::from_str(&body).map_err(|error| {
            RespondFailure::protocol(format!("invalid child approval response JSON: {error}"))
        })?;
        if !response.delivered {
            return Err(RespondFailure::protocol("child approval was not delivered"));
        }
        Ok(response)
    }

    pub async fn get_subagent_snapshot(&self) -> Result<SubagentSnapshotResponse> {
        let resp = self
            .client
            .get(self.url("/api/v1/subagents/snapshot"))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("sub-agent snapshot failed ({status}): {}", body.trim());
        }
        Ok(resp.json().await?)
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

    /// Authoritative read-only task snapshot used on open, reconnect, and
    /// explicit refresh. Child session IDs are resolved to their shared root
    /// list by the server.
    pub async fn get_task_list(&self, session_id: &str) -> Result<TaskListResponse> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/sessions/{session_id}/task")))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("get task list failed ({status}): {body}");
        }
        Ok(resp.json().await?)
    }

    /// Same paginated session endpoint as [`Self::list_sessions`], decoded
    /// into the relationship-rich projection used by the child-session tree.
    pub async fn list_session_tree(
        &self,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<ListSessionTreeEnvelope> {
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
            anyhow::bail!("list child-session tree failed ({status}): {body}");
        }
        Ok(resp.json().await?)
    }

    /// Relationship-rich detail lookup used to discover the active session's
    /// durable root before the paginated tree is assembled.
    pub async fn get_session_tree(&self, session_id: &str) -> Result<SessionTreeSummary> {
        let resp = self
            .client
            .get(self.url(&format!("/api/v1/sessions/{session_id}")))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("get child-session root failed ({status}): {body}");
        }
        let envelope: GetSessionTreeEnvelope = resp.json().await?;
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

    /// Change the per-session permission posture with the same metadata CAS as
    /// rename/pin. The TUI always fetches the current ETag before opening its
    /// conspicuous confirmation dialog.
    pub async fn patch_session_permission_mode(
        &self,
        session_id: &str,
        expected_version: u64,
        permission_mode: SessionPermissionMode,
    ) -> std::result::Result<VersionedSession, SessionMutationFailure> {
        let resp = self
            .client
            .patch(self.url(&format!("/api/v1/sessions/{session_id}")))
            .header(reqwest::header::IF_MATCH, format!("\"{expected_version}\""))
            .json(&PatchSessionPermissionModeRequest { permission_mode })
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

    /// PATCH the active session's model and reasoning choice as one CAS-guarded
    /// operation before the picker commits either value locally. Returning the
    /// authoritative summary + ETag lets the UI distinguish validation errors
    /// from a stale-session conflict without guessing from display text.
    pub async fn patch_session_execution_profile(
        &self,
        session_id: &str,
        expected_version: u64,
        model_ref: &CatalogModelRef,
        reasoning_effort: Option<ReasoningEffort>,
    ) -> std::result::Result<VersionedSession, SessionMutationFailure> {
        let resp = self
            .client
            .patch(self.url(&format!("/api/v1/sessions/{}", session_id)))
            .header(reqwest::header::IF_MATCH, format!("\"{expected_version}\""))
            .json(&PatchSessionExecutionProfileRequest::new(
                model_ref,
                reasoning_effort,
            ))
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
            SessionMutationFailure::protocol(
                "session execution-profile PATCH response omitted metadata ETag",
            )
        })?;
        let envelope: GetSessionEnvelope = resp.json().await.map_err(|error| {
            SessionMutationFailure::protocol(format!(
                "session execution-profile PATCH returned an unreadable response: {error}"
            ))
        })?;
        Ok(VersionedSession {
            summary: envelope.session,
            metadata_version,
        })
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

    // ── Permission policy ──

    pub async fn get_permission_policy(&self) -> Result<PermissionPolicyResponse> {
        let resp = self
            .client
            .get(self.url("/v1/bamboo/permission/policy"))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("get permission policy failed ({status}): {}", body.trim());
        }
        Ok(resp.json().await?)
    }

    pub async fn create_permission_rule(
        &self,
        expected_revision: u64,
        rule: &DurablePermissionRule,
    ) -> std::result::Result<PermissionPolicyResponse, PermissionMutationFailure> {
        self.mutate_permission_rule(
            reqwest::Method::POST,
            "/v1/bamboo/permission/rules".to_string(),
            expected_revision,
            rule,
        )
        .await
    }

    pub async fn update_permission_rule(
        &self,
        expected_revision: u64,
        rule: &DurablePermissionRule,
    ) -> std::result::Result<PermissionPolicyResponse, PermissionMutationFailure> {
        let mut url = reqwest::Url::parse(&self.base_url)
            .map_err(|error| PermissionMutationFailure::protocol(error.to_string()))?;
        url.path_segments_mut()
            .map_err(|_| PermissionMutationFailure::protocol("invalid Bamboo base URL"))?
            .pop_if_empty()
            .extend(["v1", "bamboo", "permission", "rules", rule.id.as_str()]);
        self.mutate_permission_rule(
            reqwest::Method::PUT,
            url.to_string(),
            expected_revision,
            rule,
        )
        .await
    }

    async fn mutate_permission_rule(
        &self,
        method: reqwest::Method,
        url_or_path: String,
        expected_revision: u64,
        rule: &DurablePermissionRule,
    ) -> std::result::Result<PermissionPolicyResponse, PermissionMutationFailure> {
        let url = if url_or_path.starts_with('/') {
            self.url(&url_or_path)
        } else {
            url_or_path
        };
        let resp = self
            .client
            .request(method, url)
            .json(&PutPermissionRuleRequest {
                expected_revision,
                rule: rule.clone(),
            })
            .send()
            .await
            .map_err(PermissionMutationFailure::transport)?;
        let status = resp.status();
        let body = resp.text().await.map_err(|error| {
            PermissionMutationFailure::protocol(format!(
                "failed to read permission policy response: {error}"
            ))
        })?;
        if !status.is_success() {
            return Err(PermissionMutationFailure::rejected(status, body));
        }
        serde_json::from_str(&body).map_err(|error| {
            PermissionMutationFailure::protocol(format!(
                "invalid permission policy response JSON: {error}"
            ))
        })
    }

    pub async fn delete_permission_rule(
        &self,
        rule_id: &str,
        expected_revision: u64,
    ) -> std::result::Result<PermissionPolicyResponse, PermissionMutationFailure> {
        let mut url = reqwest::Url::parse(&self.base_url)
            .map_err(|error| PermissionMutationFailure::protocol(error.to_string()))?;
        url.path_segments_mut()
            .map_err(|_| PermissionMutationFailure::protocol("invalid Bamboo base URL"))?
            .pop_if_empty()
            .extend(["v1", "bamboo", "permission", "rules", rule_id]);
        let resp = self
            .client
            .delete(url)
            .query(&[("expected_revision", expected_revision)])
            .send()
            .await
            .map_err(PermissionMutationFailure::transport)?;
        let status = resp.status();
        let body = resp.text().await.map_err(|error| {
            PermissionMutationFailure::protocol(format!(
                "failed to read permission policy response: {error}"
            ))
        })?;
        if !status.is_success() {
            return Err(PermissionMutationFailure::rejected(status, body));
        }
        serde_json::from_str(&body).map_err(|error| {
            PermissionMutationFailure::protocol(format!(
                "invalid permission policy response JSON: {error}"
            ))
        })
    }

    pub async fn diagnose_permission(
        &self,
        request: &DiagnosePermissionRequest,
    ) -> Result<serde_json::Value> {
        let resp = self
            .client
            .post(self.url("/v1/bamboo/permission/diagnose"))
            .json(request)
            .send()
            .await?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("permission diagnosis failed ({status}): {}", body.trim());
        }
        Ok(serde_json::from_str(&body)?)
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

    fn request_json(request: &str) -> serde_json::Value {
        let (_, body) = request
            .split_once("\r\n\r\n")
            .expect("HTTP request must contain a header terminator");
        serde_json::from_str(body).expect("request body must be JSON")
    }

    fn sample_permission_rule() -> DurablePermissionRule {
        DurablePermissionRule {
            id: "rule-1".to_string(),
            permission_type: PermissionType::ExecuteCommand,
            effect: PermissionRuleEffect::Allow,
            scope: PermissionRuleScope::Global,
            workspace_path: None,
            matcher: PermissionMatcher {
                id: "command_prefix".to_string(),
                kind: PermissionMatcherKind::CommandPrefix,
                value: "cargo test".to_string(),
            },
            source: PermissionRuleSource::User,
            expires_at: None,
        }
    }

    fn permission_policy_json(revision: u64) -> String {
        format!(
            r#"{{"revision":{revision},"policy":{{"enabled":true,"durable_rules":[]}},"temporary_grants":[]}}"#
        )
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
    async fn typed_permission_decision_uses_canonical_path_and_structured_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request
                .starts_with("POST /api/v1/sessions/session-1/permission-decisions HTTP/1.1"));
            assert_eq!(
                request_json(&request),
                serde_json::json!({
                    "request_id": "request-7",
                    "request_generation": "generation-7",
                    "decision": "allow_global",
                    "matcher_id": "command_prefix",
                    "expected_policy_revision": 23,
                    "confirm_global": true
                })
            );
            respond(
                &mut socket,
                "200 OK",
                "\"1\"",
                r#"{"success":true,"replayed":false,"auto_resume_status":"started"}"#,
            )
            .await;
        });

        let response = BambooClient::new(&base_url)
            .submit_permission_decision(
                "session-1",
                &PermissionDecision {
                    request_id: "request-7".to_string(),
                    request_generation: "generation-7".to_string(),
                    decision: PermissionDecisionKind::AllowGlobal,
                    matcher_id: Some("command_prefix".to_string()),
                    expected_policy_revision: Some(23),
                    confirm_global: true,
                },
            )
            .await
            .unwrap();
        assert!(response.success);
        assert!(!response.replayed);
        assert_eq!(response.auto_resume_status.as_deref(), Some("started"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn typed_permission_decision_409_requires_pending_reconciliation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request
                .starts_with("POST /api/v1/sessions/session-1/permission-decisions HTTP/1.1"));
            respond(
                &mut socket,
                "409 Conflict",
                "\"1\"",
                r#"{"error":{"type":"config_revision_conflict","message":"policy changed"}}"#,
            )
            .await;
        });

        let error = BambooClient::new(&base_url)
            .submit_permission_decision(
                "session-1",
                &PermissionDecision {
                    request_id: "request-7".to_string(),
                    request_generation: "generation-7".to_string(),
                    decision: PermissionDecisionKind::AllowWorkspace,
                    matcher_id: Some("path_subtree".to_string()),
                    expected_policy_revision: Some(22),
                    confirm_global: false,
                },
            )
            .await
            .unwrap_err();
        assert!(error.should_refresh_question());
        assert!(error.to_string().contains("policy changed"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn child_approval_uses_child_scoped_path_and_request_identity() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request.starts_with("POST /api/v1/sessions/child-3/child-approval HTTP/1.1"));
            assert_eq!(
                request_json(&request),
                serde_json::json!({
                    "parent_session_id": "parent-1",
                    "child_attempt": 3,
                    "request_id": "child-request-5",
                    "expected_version": 7,
                    "approved": false
                })
            );
            respond(&mut socket, "200 OK", "\"1\"", r#"{"delivered":true}"#).await;
        });

        let response = BambooClient::new(&base_url)
            .submit_child_approval(
                "child-3",
                &ChildApprovalDecision {
                    parent_session_id: "parent-1".to_string(),
                    child_attempt: 3,
                    request_id: "child-request-5".to_string(),
                    expected_version: 7,
                    approved: false,
                },
            )
            .await
            .unwrap();
        assert!(response.delivered);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn child_approval_404_requires_authoritative_reconciliation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request.starts_with("POST /api/v1/sessions/child-3/child-approval HTTP/1.1"));
            respond(
                &mut socket,
                "404 Not Found",
                "\"2\"",
                r#"{"delivered":false}"#,
            )
            .await;
        });

        let failure = BambooClient::new(&base_url)
            .submit_child_approval(
                "child-3",
                &ChildApprovalDecision {
                    parent_session_id: "parent-1".to_string(),
                    child_attempt: 3,
                    request_id: "already-consumed".to_string(),
                    expected_version: 7,
                    approved: true,
                },
            )
            .await
            .unwrap_err();
        assert!(failure.should_refresh_question());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn subagent_snapshot_preserves_child_attempt_version_and_state() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request.starts_with("GET /api/v1/subagents/snapshot HTTP/1.1"));
            respond(
                &mut socket,
                "200 OK",
                "\"7\"",
                r#"{"schema_version":1,"snapshot_seq":42,"approvals_revision":7,"generated_at":"2026-08-14T00:00:00Z","approvals":[{"parent_session_id":"parent-1","child_session_id":"child-3","child_attempt":4,"request_id":"request-9","tool_name":"Write","permission":"write_file","resource":"/workspace/file","created_at":"2026-08-14T00:00:00Z","updated_at":"2026-08-14T00:00:01Z","version":6,"state":"decision_recorded","approved":false}],"children":[]}"#,
            )
            .await;
        });

        let snapshot = BambooClient::new(&base_url)
            .get_subagent_snapshot()
            .await
            .unwrap();
        assert_eq!(snapshot.snapshot_seq, 42);
        assert_eq!(snapshot.approvals_revision, 7);
        assert_eq!(snapshot.approvals.len(), 1);
        let approval = &snapshot.approvals[0];
        assert_eq!(approval.child_attempt, 4);
        assert_eq!(approval.version, 6);
        assert_eq!(approval.state, ChildApprovalState::DecisionRecorded);
        assert_eq!(approval.approved, Some(false));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn child_tree_uses_relationship_detail_and_paginated_session_contracts() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut detail_socket, _) = listener.accept().await.unwrap();
            let detail = read_request(&mut detail_socket).await;
            assert!(detail.starts_with("GET /api/v1/sessions/child-2 HTTP/1.1"));
            respond(
                &mut detail_socket,
                "200 OK",
                "\"1\"",
                r#"{"session":{"id":"child-2","kind":"child","parent_session_id":"child-1","root_session_id":"root","spawn_depth":2,"placement":{"kind":"ssh","host":"worker"}}}"#,
            )
            .await;

            let (mut page_socket, _) = listener.accept().await.unwrap();
            let page = read_request(&mut page_socket).await;
            assert!(page.starts_with("GET /api/v1/sessions?limit=2&offset=2 HTTP/1.1"));
            respond(
                &mut page_socket,
                "200 OK",
                "\"1\"",
                r#"{"sessions":[{"id":"child-2","kind":"child","parent_session_id":"child-1","root_session_id":"root","spawn_depth":2,"placement":{"kind":"ssh","host":"worker"}}],"total":3,"limit":2,"offset":2}"#,
            )
            .await;
        });

        let client = BambooClient::new(&base_url);
        let detail = client.get_session_tree("child-2").await.unwrap();
        assert_eq!(detail.parent_session_id.as_deref(), Some("child-1"));
        assert_eq!(detail.root_session_id, "root");
        assert_eq!(detail.placement.host, "worker");

        let page = client.list_session_tree(Some(2), Some(2)).await.unwrap();
        assert_eq!(page.total, 3);
        assert_eq!(page.offset, 2);
        assert_eq!(page.sessions[0].spawn_depth, 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn task_snapshot_preserves_version_hierarchy_and_blockers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request.starts_with("GET /api/v1/sessions/child-2/task HTTP/1.1"));
            respond(
                &mut socket,
                "200 OK",
                "\"1\"",
                r#"{"session_id":"root","title":"Release","items":[{"id":"child","description":"Deploy","status":"blocked","parent_id":"parent","depends_on":["build"],"blockers":[{"kind":"dependency","summary":"Build pending","waiting_on":"build"}]}],"progress":{"completed":0,"total":1,"percentage":0},"version":7,"created_at":"2026-08-16T00:00:00Z","updated_at":"2026-08-16T00:00:01Z"}"#,
            )
            .await;
        });

        let snapshot = BambooClient::new(&base_url)
            .get_task_list("child-2")
            .await
            .unwrap();
        assert_eq!(snapshot.session_id, "root");
        assert_eq!(snapshot.version, 7);
        assert_eq!(snapshot.items[0].parent_id.as_deref(), Some("parent"));
        assert_eq!(
            snapshot.items[0].blockers[0].waiting_on.as_deref(),
            Some("build")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn command_catalog_is_session_scoped() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request.starts_with("GET /api/v1/commands?session_id=session-1 HTTP/1.1"));
            respond(
                &mut socket,
                "200 OK",
                "\"1\"",
                r#"{"commands":[{"id":"command-workspace-review","name":"review","display_name":"Review","description":"Review changes","type":"prompt","metadata":{"source":"workspace"}}],"total":1}"#,
            )
            .await;
        });

        let catalog = BambooClient::new(&base_url)
            .list_commands(Some("session-1"))
            .await
            .unwrap();

        assert_eq!(catalog.total, 1);
        assert_eq!(catalog.commands[0].name, "review");
        assert_eq!(catalog.commands[0].metadata["source"], "workspace");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn command_resolution_encodes_namespaces_and_arguments() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            let path = request
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap();
            assert!(path.starts_with("/api/v1/commands/prompt/db%2Fmigrate?"));
            assert!(path.contains("session_id=session-1"));
            assert!(path.contains("arguments=production+now"));
            respond(
                &mut socket,
                "200 OK",
                "\"1\"",
                r#"{"id":"command-workspace-db-migrate","name":"db/migrate","content":"Migrate production now","type":"prompt"}"#,
            )
            .await;
        });

        let detail = BambooClient::new(&base_url)
            .get_command(
                "prompt",
                "db/migrate",
                Some("session-1"),
                Some("production now"),
            )
            .await
            .unwrap();

        assert_eq!(detail.content, "Migrate production now");
        server.await.unwrap();
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

    #[tokio::test]
    async fn permission_mode_patch_sends_if_match_and_typed_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request.starts_with("PATCH /api/v1/sessions/session-1 HTTP/1.1"));
            assert!(request.to_ascii_lowercase().contains("if-match: \"11\""));
            assert_eq!(
                request_json(&request),
                serde_json::json!({"permission_mode": "bypass"})
            );
            respond(
                &mut socket,
                "200 OK",
                "W/\"12\"",
                r#"{"session":{"id":"session-1","permission_mode":"bypass","bypass_permissions":true}}"#,
            )
            .await;
        });

        let updated = BambooClient::new(&base_url)
            .patch_session_permission_mode("session-1", 11, SessionPermissionMode::Bypass)
            .await
            .unwrap();
        assert_eq!(updated.metadata_version, 12);
        assert_eq!(
            updated.summary.permission_mode,
            SessionPermissionMode::Bypass
        );
        assert!(updated.summary.bypass_permissions);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn permission_mode_patch_surfaces_412_and_current_etag() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request.to_ascii_lowercase().contains("if-match: \"11\""));
            assert_eq!(
                request_json(&request),
                serde_json::json!({"permission_mode": "auto"})
            );
            respond(
                &mut socket,
                "412 Precondition Failed",
                "\"13\"",
                r#"{"error":{"message":"session changed"}}"#,
            )
            .await;
        });

        let error = BambooClient::new(&base_url)
            .patch_session_permission_mode("session-1", 11, SessionPermissionMode::Auto)
            .await
            .unwrap_err();
        assert!(error.conflict);
        assert_eq!(error.current_version, Some(13));
        assert!(error.to_string().contains("session changed"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn permission_policy_crud_and_diagnose_use_revisioned_contracts() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut create_socket, _) = listener.accept().await.unwrap();
            let create = read_request(&mut create_socket).await;
            assert!(create.starts_with("POST /v1/bamboo/permission/rules HTTP/1.1"));
            assert_eq!(request_json(&create)["expected_revision"], 7);
            assert_eq!(request_json(&create)["rule"]["id"], "rule-1");
            assert_eq!(
                request_json(&create)["rule"]["matcher"],
                serde_json::json!({
                    "id": "command_prefix",
                    "kind": "command_prefix",
                    "value": "cargo test"
                })
            );
            let create_body = permission_policy_json(8);
            respond(&mut create_socket, "201 Created", "\"8\"", &create_body).await;

            let (mut update_socket, _) = listener.accept().await.unwrap();
            let update = read_request(&mut update_socket).await;
            assert!(update.starts_with("PUT /v1/bamboo/permission/rules/rule-1 HTTP/1.1"));
            assert_eq!(request_json(&update)["expected_revision"], 8);
            assert_eq!(request_json(&update)["rule"]["id"], "rule-1");
            let update_body = permission_policy_json(9);
            respond(&mut update_socket, "200 OK", "\"9\"", &update_body).await;

            let (mut delete_socket, _) = listener.accept().await.unwrap();
            let delete = read_request(&mut delete_socket).await;
            assert!(delete.starts_with(
                "DELETE /v1/bamboo/permission/rules/rule-1?expected_revision=9 HTTP/1.1"
            ));
            assert_eq!(delete.split_once("\r\n\r\n").unwrap().1, "");
            let delete_body = permission_policy_json(10);
            respond(&mut delete_socket, "200 OK", "\"10\"", &delete_body).await;

            let (mut diagnose_socket, _) = listener.accept().await.unwrap();
            let diagnose = read_request(&mut diagnose_socket).await;
            assert!(diagnose.starts_with("POST /v1/bamboo/permission/diagnose HTTP/1.1"));
            assert_eq!(
                request_json(&diagnose),
                serde_json::json!({
                    "request_id": "diagnose-1",
                    "session_id": "session-1",
                    "workspace_path": "/workspace/repo",
                    "tool_name": "Bash",
                    "tool_args": {"command": "cargo test"},
                    "permission_type": "execute_command",
                    "resource": "cargo test",
                    "operation_summary": "Run tests",
                    "bypass_requested": false,
                    "auto_approve_requested": false
                })
            );
            respond(
                &mut diagnose_socket,
                "200 OK",
                "\"10\"",
                r#"{"outcome":"deny","reason":{"code":"risk_threshold"}}"#,
            )
            .await;
        });

        let client = BambooClient::new(&base_url);
        let rule = sample_permission_rule();
        assert_eq!(
            client
                .create_permission_rule(7, &rule)
                .await
                .unwrap()
                .revision,
            8
        );
        assert_eq!(
            client
                .update_permission_rule(8, &rule)
                .await
                .unwrap()
                .revision,
            9
        );
        assert_eq!(
            client
                .delete_permission_rule("rule-1", 9)
                .await
                .unwrap()
                .revision,
            10
        );
        let diagnosis = client
            .diagnose_permission(&DiagnosePermissionRequest {
                request_id: "diagnose-1".to_string(),
                session_id: "session-1".to_string(),
                workspace_path: Some("/workspace/repo".to_string()),
                tool_name: "Bash".to_string(),
                tool_args: serde_json::json!({"command": "cargo test"}),
                permission_type: PermissionType::ExecuteCommand,
                resource: "cargo test".to_string(),
                operation_summary: "Run tests".to_string(),
                bypass_requested: false,
                auto_approve_requested: false,
                platform_hard_deny: None,
            })
            .await
            .unwrap();
        assert_eq!(diagnosis["outcome"], "deny");
        assert_eq!(diagnosis["reason"]["code"], "risk_threshold");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn permission_policy_409_is_a_typed_revision_conflict() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request.starts_with("POST /v1/bamboo/permission/rules HTTP/1.1"));
            respond(
                &mut socket,
                "409 Conflict",
                "\"9\"",
                r#"{"error":{"type":"config_revision_conflict","message":"expected 7, actual 9"}}"#,
            )
            .await;
        });

        let error = BambooClient::new(&base_url)
            .create_permission_rule(7, &sample_permission_rule())
            .await
            .unwrap_err();
        assert!(error.conflict);
        assert!(error.to_string().contains("expected 7, actual 9"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn execution_profile_patch_is_atomic_and_cas_guarded() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let patch = read_request(&mut socket).await;
            assert!(patch.starts_with("PATCH /api/v1/sessions/s1 HTTP/1.1"));
            assert!(patch.to_ascii_lowercase().contains("if-match: \"7\""));
            assert_eq!(
                request_json(&patch),
                serde_json::json!({
                    "model": "shared",
                    "provider": "provider-b",
                    "reasoning_effort": "max"
                })
            );
            respond(
                &mut socket,
                "200 OK",
                "\"8\"",
                r#"{"session":{"id":"s1","title":"Session","model":"shared","provider":"provider-b","reasoning_effort":"max"}}"#,
            )
            .await;
        });

        let updated = BambooClient::new(&base_url)
            .patch_session_execution_profile(
                "s1",
                7,
                &CatalogModelRef {
                    provider: "provider-b".to_string(),
                    model: "shared".to_string(),
                },
                Some(ReasoningEffort::Max),
            )
            .await
            .unwrap();
        assert_eq!(updated.metadata_version, 8);
        assert_eq!(updated.summary.reasoning_effort, Some(ReasoningEffort::Max));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn execution_profile_patch_reports_stale_session_conflict() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let patch = read_request(&mut socket).await;
            assert_eq!(
                request_json(&patch),
                serde_json::json!({
                    "model": "plain",
                    "provider": "provider-b",
                    "clear_reasoning_effort": true
                })
            );
            respond(
                &mut socket,
                "412 Precondition Failed",
                "\"11\"",
                r#"{"error":{"message":"Version conflict"},"current_version":11}"#,
            )
            .await;
        });

        let error = BambooClient::new(&base_url)
            .patch_session_execution_profile(
                "s1",
                7,
                &CatalogModelRef {
                    provider: "provider-b".to_string(),
                    model: "plain".to_string(),
                },
                None,
            )
            .await
            .unwrap_err();
        assert!(error.conflict);
        assert_eq!(error.current_version, Some(11));
        server.await.unwrap();
    }
}
