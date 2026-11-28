pub mod sse;
pub mod types;

use anyhow::{Context, Result};
use reqwest::Client;
use std::fmt;
use std::time::Duration;

pub use types::CancelScheduleRunOutcome;
use types::*;

#[derive(Debug, serde::Deserialize)]
pub(crate) struct ChatSubmission {
    #[serde(flatten)]
    pub(crate) response: ChatResponse,
    pub(crate) loop_command: Option<LoopChatAcknowledgment>,
}

#[derive(Debug)]
pub(crate) struct ChatStart {
    pub(crate) session_id: String,
    pub(crate) project_id: Option<String>,
}

#[derive(Debug)]
struct ChatRejected {
    status: reqwest::StatusCode,
    body: String,
}

impl ChatRejected {
    fn unassigned_workspace_owner(&self) -> Option<String> {
        if self.status != reqwest::StatusCode::CONFLICT {
            return None;
        }
        let body: serde_json::Value = serde_json::from_str(&self.body).ok()?;
        if body["error"]["code"].as_str() != Some("project_workspace_conflict")
            || body["session_project_id"].as_str() != Some("unassigned")
        {
            return None;
        }
        body["owner_project_id"]
            .as_str()
            .filter(|id| !id.trim().is_empty())
            .map(str::to_owned)
    }
}

impl fmt::Display for ChatRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "chat failed ({}): {}", self.status, self.body.trim())
    }
}

impl std::error::Error for ChatRejected {}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct LoopChatAcknowledgment {
    pub(crate) should_execute: bool,
    #[serde(rename = "loop")]
    pub(crate) record: Option<LoopChatRecord>,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct LoopChatRecord {
    pub(crate) id: Option<String>,
    pub(crate) state: String,
    next_fire_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) last_run_id: Option<String>,
    pub(crate) last_run_status: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(bound(deserialize = "T: serde::Deserialize<'de>"))]
struct SessionLoopStatus<T> {
    session_id: String,
    #[serde(rename = "loop", deserialize_with = "serde::Deserialize::deserialize")]
    record: Option<T>,
}

#[derive(serde::Deserialize)]
struct SelectedSessionLoopRecord {
    #[serde(flatten)]
    core: LoopChatRecord,
    mode: Option<String>,
    interval_seconds: Option<u64>,
    iteration: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionLoopPreview {
    pub state: SessionLoopPreviewState,
    pub next_fire_at: Option<chrono::DateTime<chrono::Utc>>,
    pub mode: Option<SessionLoopPreviewMode>,
    pub iteration: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionLoopPreviewMode {
    Fixed { interval_seconds: Option<u64> },
    Adaptive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionLoopPreviewState {
    Scheduled,
    Paused,
    Stopped,
    Expired,
}

impl SessionLoopPreviewState {
    fn from_wire(state: &str) -> Option<Self> {
        match state {
            "scheduled" => Some(Self::Scheduled),
            "paused" => Some(Self::Paused),
            "stopped" => Some(Self::Stopped),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }
}

impl LoopChatAcknowledgment {
    pub(crate) fn status_text(&self) -> String {
        let Some(record) = &self.record else {
            return "No session loop".to_string();
        };
        let mut text = format!("Loop {}", record.state);
        if let Some(next) = record.next_fire_at {
            let next = next
                .with_timezone(&chrono::Local)
                .format("%Y-%m-%d %H:%M:%S");
            text.push_str(&format!(" · next {next}"));
        }
        if let Some(last) = &record.last_run_status {
            text.push_str(&format!(" · last run {last}"));
        }
        text
    }
}

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

const MAX_SCHEDULE_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const MAX_SESSION_LOOP_PREVIEW_BYTES: usize = 128 * 1024;
const SCHEDULE_LIST_UNAVAILABLE: &str = "Could not load schedules. Reopen Schedules to try again.";
const SCHEDULE_UNAVAILABLE_OR_CHANGED: &str =
    "Schedule is unavailable or changed. Reopen Schedules.";
const SCHEDULE_CREATE_UNCONFIRMED: &str =
    "Could not confirm creation. Check Schedules before trying again.";
const SCHEDULE_DELETE_UNCONFIRMED: &str =
    "Could not confirm deletion. Check the schedule before trying again.";
const SCHEDULE_RUN_UNCONFIRMED: &str =
    "Could not confirm Run Now. Check Recent runs before trying again.";

enum BoundedBodyError {
    TooLarge,
    Read(reqwest::Error),
}

async fn read_bounded_body(
    mut response: reqwest::Response,
    limit: usize,
) -> std::result::Result<Vec<u8>, BoundedBodyError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(BoundedBodyError::TooLarge);
    }

    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(BoundedBodyError::Read)? {
        if chunk.len() > limit - body.len() {
            return Err(BoundedBodyError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn schedule_response_code(response: reqwest::Response) -> Option<String> {
    const MAX_ERROR_BYTES: usize = 8 * 1024;
    let body = read_bounded_body(response, MAX_ERROR_BYTES).await.ok()?;
    serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|error| {
            error
                .pointer("/error/code")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
}

async fn schedule_response_has_code(response: reqwest::Response, code: &str) -> bool {
    schedule_response_code(response).await.as_deref() == Some(code)
}

async fn schedule_project_conflict(response: reqwest::Response) -> Option<&'static str> {
    // A slow error body should not delay feedback in the Schedule views.
    let code = tokio::time::timeout(Duration::from_millis(500), schedule_response_code(response))
        .await
        .ok()
        .flatten()?;
    match code.as_str() {
        "project_archived" => Some("The schedule needs an active Project."),
        "project_workspace_conflict" => {
            Some("The workspace belongs to another Project. Check the Project settings.")
        }
        "project_path_missing" => Some("Set a workspace path for the schedule's Project."),
        "project_path_unavailable" => {
            Some("The Project workspace is unavailable. Check its path on the Host.")
        }
        _ => None,
    }
}

fn schedule_action_unconfirmed(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::REQUEST_TIMEOUT
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

    pub(crate) async fn chat(&self, mut req: ChatRequest) -> Result<ChatStart> {
        let from_launch_directory =
            req.session_id.is_none() && req.project_id.is_none() && req.workspace_path.is_some();
        let submission = match self.submit_chat(&req).await {
            Err(error) if from_launch_directory => {
                // This conflict rejects the turn before admission. Use the
                // server's canonical workspace owner and retry only once.
                let Some(project_id) = error
                    .downcast_ref::<ChatRejected>()
                    .and_then(ChatRejected::unassigned_workspace_owner)
                else {
                    return Err(error);
                };
                req.project_id = Some(project_id);
                self.submit_chat(&req).await?
            }
            result => result?,
        };
        Ok(ChatStart {
            session_id: submission.response.session_id,
            project_id: req.project_id,
        })
    }

    /// A backend without /loop support could accept a control as normal chat.
    /// Confirm the existing Session's dedicated endpoint before posting it.
    pub(crate) async fn submit_loop_chat(&self, req: ChatRequest) -> Result<ChatSubmission> {
        let session_id = req
            .session_id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("Open a Session before using /loop"))?;
        self.session_loop_status(session_id).await?;
        self.submit_chat(&req).await
    }

    fn session_loop_url(&self, session_id: &str) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(&self.base_url)?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Bamboo base URL cannot contain Session paths"))?
            .pop_if_empty()
            .extend(["api", "v1", "sessions", session_id, "loop"]);
        Ok(url)
    }

    pub(crate) async fn session_loop_status(
        &self,
        session_id: &str,
    ) -> Result<Option<LoopChatRecord>> {
        let response = self
            .client
            .get(self.session_loop_url(session_id)?)
            .header(reqwest::header::CACHE_CONTROL, "no-store")
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Session loop status failed ({status}): {}", body.trim());
        }
        let status: SessionLoopStatus<LoopChatRecord> = response.json().await?;
        anyhow::ensure!(
            status.session_id == session_id
                && status
                    .record
                    .as_ref()
                    .is_none_or(
                        |record| SessionLoopPreviewState::from_wire(&record.state).is_some()
                    ),
            "Invalid Session loop status"
        );
        Ok(status.record)
    }

    /// Read the selected Session's paced loop for display in the Session list.
    pub(crate) async fn selected_session_loop_status(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionLoopPreview>> {
        anyhow::ensure!(
            !session_id.trim().is_empty() && !matches!(session_id, "." | ".."),
            "Invalid Session ID"
        );
        let url = self
            .session_loop_url(session_id)
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| anyhow::anyhow!("Could not contact the Host for Session loop status"))?;
        let response = client
            .get(url)
            .header(reqwest::header::CACHE_CONTROL, "no-store")
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    anyhow::anyhow!("Timed out waiting for Session loop status")
                } else {
                    anyhow::anyhow!("Could not contact the Host for Session loop status")
                }
            })?;
        let http_status = response.status();
        anyhow::ensure!(
            http_status == reqwest::StatusCode::OK,
            "Could not fetch Session loop status (HTTP {})",
            http_status.as_u16()
        );
        let body = read_bounded_body(response, MAX_SESSION_LOOP_PREVIEW_BYTES)
            .await
            .map_err(|error| match error {
                BoundedBodyError::TooLarge => {
                    anyhow::anyhow!("Session loop status response is too large (128 KiB limit)")
                }
                BoundedBodyError::Read(error) if error.is_timeout() => {
                    anyhow::anyhow!("Timed out waiting for Session loop status")
                }
                BoundedBodyError::Read(_) => anyhow::anyhow!("Invalid Session loop status"),
            })?;
        // Serde structs also accept sequences. Check the object shape, then
        // decode the original bytes so duplicate known fields are rejected.
        let shape: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| anyhow::anyhow!("Invalid Session loop status"))?;
        anyhow::ensure!(
            matches!(
                shape.get("loop"),
                Some(serde_json::Value::Null | serde_json::Value::Object(_))
            ),
            "Invalid Session loop status"
        );
        let status: SessionLoopStatus<SelectedSessionLoopRecord> = serde_json::from_slice(&body)
            .map_err(|_| anyhow::anyhow!("Invalid Session loop status"))?;
        anyhow::ensure!(
            status.session_id == session_id,
            "Invalid Session loop status"
        );
        let Some(record) = status.record else {
            return Ok(None);
        };
        let state = SessionLoopPreviewState::from_wire(&record.core.state)
            .ok_or_else(|| anyhow::anyhow!("Invalid Session loop status"))?;
        let mode = match record.mode.as_deref() {
            Some("fixed") => Some(SessionLoopPreviewMode::Fixed {
                interval_seconds: record.interval_seconds.filter(|&seconds| seconds > 0),
            }),
            Some("adaptive") => Some(SessionLoopPreviewMode::Adaptive),
            _ => None,
        };
        Ok(Some(SessionLoopPreview {
            state,
            next_fire_at: record.core.next_fire_at,
            mode,
            iteration: record.iteration,
        }))
    }

    async fn submit_chat(&self, req: &ChatRequest) -> Result<ChatSubmission> {
        let resp = self
            .client
            .post(self.url("/api/v1/chat"))
            .json(req)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ChatRejected { status, body }.into());
        }
        Ok(resp.json().await?)
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
            .await
            .map_err(|_| anyhow::anyhow!(SCHEDULE_LIST_UNAVAILABLE))?;
        let status = resp.status();
        anyhow::ensure!(
            !schedule_action_unconfirmed(status),
            SCHEDULE_LIST_UNAVAILABLE
        );
        anyhow::ensure!(
            status.is_success(),
            "The Host refused to load schedules. Reopen Schedules to try again."
        );
        let body = resp
            .bytes()
            .await
            .map_err(|_| anyhow::anyhow!(SCHEDULE_LIST_UNAVAILABLE))?;
        let parsed: ListSchedulesResponse = serde_json::from_slice(&body).map_err(|_| {
            anyhow::anyhow!(
                "The Host returned an invalid schedule list. Reopen Schedules to try again."
            )
        })?;
        Ok(parsed
            .schedules
            .into_iter()
            .map(Schedule::from_view)
            .collect())
    }

    pub async fn set_schedule_enabled(&self, schedule_id: &str, enabled: bool) -> Result<Schedule> {
        self.set_schedule_enabled_with_timeout(schedule_id, enabled, Duration::from_secs(30))
            .await
    }

    async fn set_schedule_enabled_with_timeout(
        &self,
        schedule_id: &str,
        enabled: bool,
        timeout: Duration,
    ) -> Result<Schedule> {
        use reqwest::StatusCode;

        #[derive(serde::Deserialize)]
        struct ToggleReply {
            // The list projection permits an omitted enabled flag. A mutation
            // receipt must state the saved value explicitly.
            enabled: bool,
            #[serde(flatten)]
            view: ScheduleView,
        }

        anyhow::ensure!(
            !matches!(schedule_id, "" | "." | ".."),
            "Invalid schedule ID"
        );
        let mut url = reqwest::Url::parse(&self.base_url).context("Invalid Bamboo URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?
            .pop_if_empty()
            .extend(["api", "v1", "schedules", schedule_id]);
        let response = self
            .client
            .patch(url)
            .timeout(timeout)
            .json(&serde_json::json!({ "enabled": enabled }))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    anyhow::anyhow!("Timed out contacting the Host to update the schedule")
                } else {
                    anyhow::anyhow!("Could not contact the Host to update the schedule")
                }
            })?;
        let status = response.status();
        if status == StatusCode::OK {
            let mut reply: ToggleReply = response.json().await.map_err(|error| {
                if error.is_timeout() {
                    anyhow::anyhow!("Timed out waiting for the schedule update response")
                } else {
                    anyhow::anyhow!("Invalid schedule update response")
                }
            })?;
            anyhow::ensure!(
                reply.view.id == schedule_id,
                "Schedule update response has a different schedule ID"
            );
            anyhow::ensure!(
                reply.enabled == enabled,
                "Schedule update response has a different enabled state"
            );
            reply.view.enabled = reply.enabled;
            return Ok(Schedule::from_view(reply.view));
        }
        if status == StatusCode::CONFLICT
            && schedule_response_has_code(response, "schedule_no_future_occurrence").await
        {
            anyhow::bail!("Schedule has no future occurrence");
        }
        anyhow::bail!("Could not update schedule (HTTP {})", status.as_u16());
    }

    pub(crate) async fn rename_schedule(&self, schedule_id: &str, name: &str) -> Result<String> {
        use reqwest::StatusCode;

        #[derive(serde::Deserialize)]
        struct RenameReply {
            id: String,
            name: String,
        }

        anyhow::ensure!(
            !matches!(schedule_id, "" | "." | ".."),
            "Invalid schedule ID"
        );
        let name = name.trim();
        anyhow::ensure!(!name.is_empty(), "Enter a schedule name");
        let mut url = reqwest::Url::parse(&self.base_url).context("Invalid Bamboo URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?
            .pop_if_empty()
            .extend(["api", "v1", "schedules", schedule_id, "name"]);
        // The dedicated route guarantees that a rename preserves saved cadence.
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| anyhow::anyhow!("Could not connect to the Host to rename the schedule"))?;
        let response = client
            .patch(url)
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({ "name": name }))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    anyhow::anyhow!("Timed out contacting the Host to rename the schedule")
                } else {
                    anyhow::anyhow!("Could not contact the Host to rename the schedule")
                }
            })?;
        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            if schedule_response_has_code(response, "schedule_not_found").await {
                anyhow::bail!("Schedule no longer exists");
            }
            anyhow::bail!("Renaming requires a newer Host");
        }
        if status == StatusCode::METHOD_NOT_ALLOWED {
            anyhow::bail!("Renaming requires a newer Host");
        }
        anyhow::ensure!(
            status == StatusCode::OK,
            "Could not rename schedule (HTTP {})",
            status.as_u16()
        );

        let body = read_bounded_body(response, MAX_SCHEDULE_RESPONSE_BYTES)
            .await
            .map_err(|error| match error {
                BoundedBodyError::TooLarge => {
                    anyhow::anyhow!("Schedule rename response is too large (2 MiB limit)")
                }
                BoundedBodyError::Read(error) if error.is_timeout() => {
                    anyhow::anyhow!("Timed out waiting for the schedule rename response")
                }
                BoundedBodyError::Read(_) => anyhow::anyhow!("Invalid schedule rename response"),
            })?;
        let reply: RenameReply = serde_json::from_slice(&body)
            .map_err(|_| anyhow::anyhow!("Invalid schedule rename response"))?;
        anyhow::ensure!(
            reply.id == schedule_id && reply.name == name,
            "Schedule rename response does not match the request"
        );
        Ok(reply.name)
    }

    pub(crate) async fn update_schedule_interval(
        &self,
        schedule_id: &str,
        every_seconds: u64,
    ) -> Result<Schedule> {
        use reqwest::StatusCode;

        const INVALID_PERIOD: &str =
            "Schedule interval must be a positive representable number of seconds";

        anyhow::ensure!(
            !matches!(schedule_id, "" | "." | ".."),
            "Invalid schedule ID"
        );
        anyhow::ensure!(
            is_valid_schedule_interval(every_seconds),
            "{INVALID_PERIOD}"
        );
        let mut url = reqwest::Url::parse(&self.base_url).context("Invalid Bamboo URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?
            .pop_if_empty()
            .extend(["api", "v1", "schedules", schedule_id, "interval"]);
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| {
                anyhow::anyhow!("Could not contact the Host to update the schedule interval")
            })?;
        let response = client
            .patch(url)
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({ "every_seconds": every_seconds }))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    anyhow::anyhow!("Timed out contacting the Host to update the schedule interval")
                } else {
                    anyhow::anyhow!("Could not contact the Host to update the schedule interval")
                }
            })?;
        let status = response.status();
        if status != StatusCode::OK {
            let code = match status {
                StatusCode::BAD_REQUEST | StatusCode::CONFLICT | StatusCode::NOT_FOUND => {
                    schedule_response_code(response).await
                }
                _ => None,
            };
            let message = match (status, code.as_deref()) {
                (StatusCode::BAD_REQUEST, Some("invalid_schedule_interval")) => INVALID_PERIOD,
                (StatusCode::CONFLICT, Some("schedule_trigger_changed")) => {
                    "Schedule is no longer an Interval schedule"
                }
                (StatusCode::CONFLICT, Some("schedule_no_future_occurrence")) => {
                    "Schedule has no future occurrence"
                }
                (StatusCode::NOT_FOUND, Some("schedule_not_found")) => "Schedule no longer exists",
                (StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED, _) => {
                    "Editing the interval requires a newer Host"
                }
                _ => anyhow::bail!(
                    "Could not update schedule interval (HTTP {})",
                    status.as_u16()
                ),
            };
            anyhow::bail!("{message}");
        }

        let body = read_bounded_body(response, MAX_SCHEDULE_RESPONSE_BYTES)
            .await
            .map_err(|error| match error {
                BoundedBodyError::TooLarge => {
                    anyhow::anyhow!("Schedule interval response is too large (2 MiB limit)")
                }
                BoundedBodyError::Read(error) if error.is_timeout() => {
                    anyhow::anyhow!("Timed out waiting for the schedule interval response")
                }
                BoundedBodyError::Read(_) => anyhow::anyhow!("Invalid schedule interval response"),
            })?;
        let view: ScheduleView = serde_json::from_slice(&body)
            .map_err(|_| anyhow::anyhow!("Invalid schedule interval response"))?;
        let schedule = Schedule::from_view(view);
        anyhow::ensure!(
            schedule.id == schedule_id && schedule.interval_seconds == Some(every_seconds),
            "Schedule interval response does not match the request"
        );
        Ok(schedule)
    }

    pub(crate) async fn update_schedule_clock_time(
        &self,
        schedule_id: &str,
        kind: ScheduleClockKind,
        time: ScheduleClockTime,
    ) -> Result<Schedule> {
        use reqwest::StatusCode;

        let label = kind.label();
        let (route, invalid_code) = match kind {
            ScheduleClockKind::Daily => ("daily-time", "invalid_schedule_daily_time"),
            ScheduleClockKind::Weekly => ("weekly-time", "invalid_schedule_weekly_time"),
            ScheduleClockKind::Monthly => ("monthly-time", "invalid_schedule_monthly_time"),
        };

        anyhow::ensure!(
            !matches!(schedule_id, "" | "." | ".."),
            "Invalid schedule ID"
        );
        anyhow::ensure!(
            ScheduleClockTime::new(time.hour, time.minute, time.second).is_some(),
            "Enter a {label} time in HH:MM or HH:MM:SS format"
        );
        let mut url = reqwest::Url::parse(&self.base_url).context("Invalid Bamboo URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?
            .pop_if_empty()
            .extend(["api", "v1", "schedules", schedule_id, route]);
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| anyhow::anyhow!("Could not contact the Host to update {label} time"))?;
        let response = client
            .patch(url)
            .timeout(Duration::from_secs(30))
            .json(&time)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    anyhow::anyhow!("Timed out contacting the Host to update {label} time")
                } else {
                    anyhow::anyhow!("Could not contact the Host to update {label} time")
                }
            })?;
        let status = response.status();
        if status != StatusCode::OK {
            let code = match status {
                StatusCode::BAD_REQUEST | StatusCode::CONFLICT | StatusCode::NOT_FOUND => {
                    schedule_response_code(response).await
                }
                _ => None,
            };
            match (status, code.as_deref()) {
                (StatusCode::BAD_REQUEST, Some(code)) if code == invalid_code => {
                    anyhow::bail!("Enter a {label} time in HH:MM or HH:MM:SS format")
                }
                (StatusCode::CONFLICT, Some("schedule_trigger_changed")) => {
                    anyhow::bail!("Schedule is no longer a {label} schedule")
                }
                (StatusCode::CONFLICT, Some("schedule_no_future_occurrence")) => {
                    anyhow::bail!("Schedule has no future occurrence")
                }
                (StatusCode::NOT_FOUND, Some("schedule_not_found")) => {
                    anyhow::bail!("Schedule no longer exists")
                }
                (StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED, _) => {
                    anyhow::bail!("Editing {label} time requires a newer Host")
                }
                _ => anyhow::bail!("Could not update {label} time (HTTP {})", status.as_u16()),
            }
        }

        let body = read_bounded_body(response, MAX_SCHEDULE_RESPONSE_BYTES)
            .await
            .map_err(|error| match error {
                BoundedBodyError::TooLarge => {
                    anyhow::anyhow!("{label} time response is too large (2 MiB limit)")
                }
                BoundedBodyError::Read(error) if error.is_timeout() => {
                    anyhow::anyhow!("Timed out waiting for the {label} time response")
                }
                BoundedBodyError::Read(_) => anyhow::anyhow!("Invalid {label} time response"),
            })?;
        let receipt: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| anyhow::anyhow!("Invalid {label} time response"))?;
        // Save acknowledgements carry the state and run configuration. Keep
        // their presence check separate from compatibility decoding of lists.
        anyhow::ensure!(
            receipt
                .get("state")
                .is_some_and(serde_json::Value::is_object)
                && receipt
                    .get("run_config")
                    .is_some_and(serde_json::Value::is_object),
            "Invalid {label} time response"
        );
        let view: ScheduleView = serde_json::from_value(receipt)
            .map_err(|_| anyhow::anyhow!("Invalid {label} time response"))?;
        // Canonical mutation replies include seconds. Older list rows may omit them.
        let exact_time = <ScheduleClockTime as serde::Deserialize>::deserialize(&view.trigger).ok()
            == Some(time);
        let schedule = Schedule::from_view(view);
        anyhow::ensure!(
            schedule.id == schedule_id && schedule.clock_time(kind) == Some(time) && exact_time,
            "{label} time response does not match the request"
        );
        Ok(schedule)
    }

    pub(crate) async fn update_schedule_cron_expression(
        &self,
        schedule_id: &str,
        expression: &str,
    ) -> Result<Schedule> {
        use reqwest::StatusCode;

        #[derive(serde::Deserialize)]
        struct CronExpressionReceipt {
            trigger: ScheduleTriggerReq,
            #[serde(flatten)]
            view: ScheduleView,
        }

        anyhow::ensure!(
            !matches!(schedule_id, "" | "." | ".."),
            "Invalid schedule ID"
        );
        let expression = expression.trim();
        anyhow::ensure!(!expression.is_empty(), "Enter a valid Cron expression");
        let mut url = reqwest::Url::parse(&self.base_url).context("Invalid Bamboo URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?
            .pop_if_empty()
            .extend(["api", "v1", "schedules", schedule_id, "cron-expression"]);
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| {
                anyhow::anyhow!("Could not contact the Host to update the Cron expression")
            })?;
        let response = client
            .patch(url)
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({ "expr": expression }))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    anyhow::anyhow!("Timed out contacting the Host to update the Cron expression")
                } else {
                    anyhow::anyhow!("Could not contact the Host to update the Cron expression")
                }
            })?;
        let status = response.status();
        if status != StatusCode::OK {
            let code = match status {
                StatusCode::BAD_REQUEST | StatusCode::CONFLICT | StatusCode::NOT_FOUND => {
                    schedule_response_code(response).await
                }
                _ => None,
            };
            match (status, code.as_deref()) {
                (StatusCode::BAD_REQUEST, Some("invalid_schedule_cron")) => {
                    anyhow::bail!("Enter a valid Cron expression")
                }
                (StatusCode::CONFLICT, Some("schedule_trigger_changed")) => {
                    anyhow::bail!("Schedule is no longer an editable Cron Task schedule")
                }
                (StatusCode::CONFLICT, Some("schedule_no_future_occurrence")) => {
                    anyhow::bail!("Schedule has no future occurrence")
                }
                (StatusCode::NOT_FOUND, Some("schedule_not_found")) => {
                    anyhow::bail!("Schedule no longer exists")
                }
                (StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED, _) => {
                    anyhow::bail!("Editing a Cron expression requires a newer Host")
                }
                _ => anyhow::bail!(
                    "Could not update the Cron expression (HTTP {})",
                    status.as_u16()
                ),
            }
        }

        let body = read_bounded_body(response, MAX_SCHEDULE_RESPONSE_BYTES)
            .await
            .map_err(|error| match error {
                BoundedBodyError::TooLarge => {
                    anyhow::anyhow!("Cron expression response is too large (2 MiB limit)")
                }
                BoundedBodyError::Read(error) if error.is_timeout() => {
                    anyhow::anyhow!("Timed out waiting for the Cron expression response")
                }
                BoundedBodyError::Read(_) => anyhow::anyhow!("Invalid Cron expression response"),
            })?;
        let shape: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| anyhow::anyhow!("Invalid Cron expression response"))?;
        // Keep list-row defaults separate from a receipt that will replace the
        // row. An exhausted no-op can omit the next timestamp.
        anyhow::ensure!(
            shape.get("name").is_some_and(serde_json::Value::is_string)
                && shape
                    .get("enabled")
                    .is_some_and(serde_json::Value::is_boolean)
                && shape.get("state").is_some_and(serde_json::Value::is_object)
                && shape
                    .get("run_config")
                    .is_some_and(serde_json::Value::is_object),
            "Invalid Cron expression response"
        );
        // Decode the original bytes with a typed trigger so conflicting
        // known keys cannot be collapsed by the object shape check.
        let CronExpressionReceipt { trigger, mut view } = serde_json::from_slice(&body)
            .map_err(|_| anyhow::anyhow!("Invalid Cron expression response"))?;
        let ScheduleTriggerReq::Cron { expr } = trigger else {
            anyhow::bail!("Cron expression response does not match the request");
        };
        anyhow::ensure!(
            view.id == schedule_id
                && view.run_config.workflow_target.is_none()
                && expr.trim() == expression,
            "Cron expression response does not match the request"
        );
        view.trigger = serde_json::json!({ "type": "cron", "expr": expr });
        Ok(Schedule::from_view(view))
    }

    pub(crate) async fn update_schedule_once_at(
        &self,
        schedule_id: &str,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Schedule> {
        use reqwest::StatusCode;

        anyhow::ensure!(
            !matches!(schedule_id, "" | "." | ".."),
            "Invalid schedule ID"
        );
        let timestamp = format_schedule_once_at(at)
            .context("Enter a future RFC3339 timestamp for Once time")?;
        let mut url = reqwest::Url::parse(&self.base_url).context("Invalid Bamboo URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?
            .pop_if_empty()
            .extend(["api", "v1", "schedules", schedule_id, "once-at"]);
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| anyhow::anyhow!("Could not contact the Host to update Once time"))?;
        let response = client
            .patch(url)
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({ "at": timestamp }))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    anyhow::anyhow!("Timed out contacting the Host to update Once time")
                } else {
                    anyhow::anyhow!("Could not contact the Host to update Once time")
                }
            })?;
        let status = response.status();
        if status != StatusCode::OK {
            let code = match status {
                StatusCode::BAD_REQUEST | StatusCode::CONFLICT | StatusCode::NOT_FOUND => {
                    schedule_response_code(response).await
                }
                _ => None,
            };
            match (status, code.as_deref()) {
                (StatusCode::BAD_REQUEST, Some("invalid_schedule_once_at")) => {
                    anyhow::bail!("Enter a future RFC3339 timestamp for Once time")
                }
                (StatusCode::CONFLICT, Some("schedule_trigger_changed")) => {
                    anyhow::bail!("Schedule is no longer a Once schedule")
                }
                (StatusCode::CONFLICT, Some("schedule_once_not_pending")) => {
                    anyhow::bail!("Once schedule no longer has a pending occurrence")
                }
                (StatusCode::CONFLICT, Some("schedule_no_future_occurrence")) => {
                    anyhow::bail!("Schedule has no future occurrence")
                }
                (StatusCode::NOT_FOUND, Some("schedule_not_found")) => {
                    anyhow::bail!("Schedule no longer exists")
                }
                (StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED, _) => {
                    anyhow::bail!("Editing Once time requires a newer Host")
                }
                _ => anyhow::bail!("Could not update Once time (HTTP {})", status.as_u16()),
            }
        }

        let body = read_bounded_body(response, MAX_SCHEDULE_RESPONSE_BYTES)
            .await
            .map_err(|error| match error {
                BoundedBodyError::TooLarge => {
                    anyhow::anyhow!("Once time response is too large (2 MiB limit)")
                }
                BoundedBodyError::Read(error) if error.is_timeout() => {
                    anyhow::anyhow!("Timed out waiting for the Once time response")
                }
                BoundedBodyError::Read(_) => anyhow::anyhow!("Invalid Once time response"),
            })?;
        let receipt: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| anyhow::anyhow!("Invalid Once time response"))?;
        anyhow::ensure!(
            receipt
                .get("state")
                .is_some_and(serde_json::Value::is_object)
                && receipt
                    .get("run_config")
                    .is_some_and(serde_json::Value::is_object),
            "Invalid Once time response"
        );
        let view: ScheduleView = serde_json::from_value(receipt)
            .map_err(|_| anyhow::anyhow!("Invalid Once time response"))?;
        let schedule = Schedule::from_view(view);
        anyhow::ensure!(
            schedule.id == schedule_id
                && schedule.once_at == Some(at)
                && schedule.next_run == Some(at),
            "Once time response does not match the request"
        );
        Ok(schedule)
    }

    pub(crate) async fn set_schedule_prompt(
        &self,
        schedule_id: &str,
        task_message: &str,
    ) -> Result<String> {
        use reqwest::StatusCode;

        #[derive(serde::Deserialize)]
        struct PromptReply {
            id: String,
            run_config: PromptRunConfig,
        }

        #[derive(serde::Deserialize)]
        struct PromptRunConfig {
            task_message: String,
        }

        anyhow::ensure!(
            !matches!(schedule_id, "" | "." | ".."),
            "Invalid schedule ID"
        );
        anyhow::ensure!(!task_message.trim().is_empty(), "Enter a task prompt");
        let mut url = reqwest::Url::parse(&self.base_url).context("Invalid Bamboo URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?
            .pop_if_empty()
            .extend(["api", "v1", "schedules", schedule_id, "prompt"]);
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|_| anyhow::anyhow!("Could not contact the Host to update the task prompt"))?;
        let response = client
            .patch(url)
            .timeout(Duration::from_secs(30))
            .json(&serde_json::json!({ "task_message": task_message }))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    anyhow::anyhow!("Timed out contacting the Host to update the task prompt")
                } else {
                    anyhow::anyhow!("Could not contact the Host to update the task prompt")
                }
            })?;
        let status = response.status();
        if status != StatusCode::OK {
            let code = match status {
                StatusCode::BAD_REQUEST | StatusCode::CONFLICT | StatusCode::NOT_FOUND => {
                    schedule_response_code(response).await
                }
                _ => None,
            };
            let message = match (status, code.as_deref()) {
                (StatusCode::BAD_REQUEST, Some("invalid_schedule_prompt")) => "Enter a task prompt",
                (StatusCode::CONFLICT, Some("schedule_prompt_changed")) => {
                    "Schedule task prompt can no longer be edited"
                }
                (StatusCode::NOT_FOUND, Some("schedule_not_found")) => "Schedule no longer exists",
                (StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED, _) => {
                    "Editing the task prompt requires a newer Host"
                }
                _ => anyhow::bail!("Could not update task prompt (HTTP {})", status.as_u16()),
            };
            anyhow::bail!("{message}");
        }

        let reply: PromptReply = response.json().await.map_err(|error| {
            if error.is_timeout() {
                anyhow::anyhow!("Timed out waiting for the task prompt response")
            } else {
                anyhow::anyhow!("Invalid schedule task prompt response")
            }
        })?;
        anyhow::ensure!(
            reply.id == schedule_id && reply.run_config.task_message == task_message,
            "Schedule task prompt response does not match the request"
        );
        Ok(reply.run_config.task_message)
    }

    pub async fn list_schedule_sessions(&self, schedule_id: &str) -> Result<Vec<SessionSummary>> {
        anyhow::ensure!(
            !matches!(schedule_id, "" | "." | ".."),
            "Invalid schedule ID"
        );
        let mut url = reqwest::Url::parse(&self.base_url).context("Invalid Bamboo URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?
            .pop_if_empty()
            .extend(["api", "v1", "schedules", schedule_id, "sessions"]);
        let response = self
            .client
            .get(url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    anyhow::anyhow!("Timed out fetching schedule sessions")
                } else {
                    anyhow::anyhow!("Could not contact the Host to fetch schedule sessions")
                }
            })?;
        let status = response.status();
        anyhow::ensure!(
            status == reqwest::StatusCode::OK,
            "Could not fetch schedule sessions (HTTP {})",
            status.as_u16()
        );
        let body = read_bounded_body(response, MAX_SCHEDULE_RESPONSE_BYTES)
            .await
            .map_err(|error| match error {
                BoundedBodyError::TooLarge => {
                    anyhow::anyhow!("Schedule sessions response is too large (2 MiB limit)")
                }
                BoundedBodyError::Read(error) if error.is_timeout() => {
                    anyhow::anyhow!("Timed out fetching schedule sessions")
                }
                BoundedBodyError::Read(_) => anyhow::anyhow!("Invalid schedule sessions response"),
            })?;
        let reply: ListScheduleSessionsResponse = serde_json::from_slice(&body)
            .map_err(|_| anyhow::anyhow!("Invalid schedule sessions response"))?;
        anyhow::ensure!(
            reply.schedule_id == schedule_id
                && reply.sessions.iter().all(|row| {
                    row.created_by_schedule_id == schedule_id && !row.session.id.trim().is_empty()
                }),
            "Invalid schedule sessions response"
        );
        Ok(reply.sessions.into_iter().map(|row| row.session).collect())
    }

    pub async fn list_schedule_runs(&self, schedule_id: &str) -> Result<Vec<ScheduleRunView>> {
        let mut url = reqwest::Url::parse(&self.base_url).context("Invalid Bamboo URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?
            .pop_if_empty()
            .extend(["api", "v1", "schedules", schedule_id, "runs"]);
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("Could not fetch schedule runs")?;
        let status = response.status();
        anyhow::ensure!(
            status.is_success(),
            "Could not fetch schedule runs ({status})"
        );
        let parsed: ListScheduleRunsResponse = response
            .json()
            .await
            .context("Invalid schedule runs response")?;
        anyhow::ensure!(
            parsed.schedule_id == schedule_id,
            "Schedule runs response has a different schedule ID"
        );
        Ok(parsed.runs)
    }

    pub async fn cancel_schedule_run(
        &self,
        schedule_id: &str,
        run_id: &str,
    ) -> Result<CancelScheduleRunOutcome> {
        use reqwest::StatusCode;

        #[derive(serde::Deserialize)]
        struct Acknowledgment {
            schedule_id: String,
            run_id: String,
            status: String,
        }

        #[derive(serde::Deserialize)]
        struct TerminalRecord {
            schedule_id: String,
            #[serde(flatten)]
            run: ScheduleRunView,
        }

        let mut url = reqwest::Url::parse(&self.base_url).context("Invalid Bamboo URL")?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Invalid Bamboo URL"))?
            .pop_if_empty()
            .extend([
                "api",
                "v1",
                "schedules",
                schedule_id,
                "runs",
                run_id,
                "cancel",
            ]);
        let response = self.client.post(url).send().await.map_err(|_| {
            anyhow::anyhow!("Could not contact the Host to cancel the schedule run")
        })?;
        let status = response.status();
        let check_identity = |response_schedule: &str, response_run: &str| -> Result<()> {
            anyhow::ensure!(
                response_schedule == schedule_id && response_run == run_id,
                "Schedule cancellation response has a different schedule or run ID"
            );
            Ok(())
        };

        match status {
            StatusCode::ACCEPTED => {
                let reply: Acknowledgment = response
                    .json()
                    .await
                    .map_err(|_| anyhow::anyhow!("Invalid schedule cancellation response"))?;
                check_identity(&reply.schedule_id, &reply.run_id)?;
                anyhow::ensure!(
                    reply.status == "cancel_requested",
                    "Invalid schedule cancellation acknowledgment"
                );
                Ok(CancelScheduleRunOutcome::Requested)
            }
            StatusCode::OK => {
                let reply: TerminalRecord = response
                    .json()
                    .await
                    .map_err(|_| anyhow::anyhow!("Invalid schedule cancellation response"))?;
                check_identity(&reply.schedule_id, &reply.run.run_id)?;
                anyhow::ensure!(
                    reply.run.is_terminal(),
                    "Schedule cancellation response is not a terminal result"
                );
                Ok(CancelScheduleRunOutcome::Terminal(reply.run))
            }
            StatusCode::NOT_FOUND | StatusCode::CONFLICT => {
                let error = response.json::<serde_json::Value>().await.ok();
                let code = error
                    .as_ref()
                    .and_then(|value| value.pointer("/error/code"))
                    .and_then(serde_json::Value::as_str);
                match (status, code) {
                    (StatusCode::NOT_FOUND, Some("schedule_run_not_found")) => {
                        anyhow::bail!("Schedule run not found")
                    }
                    (StatusCode::NOT_FOUND, _) => {
                        anyhow::bail!("This Host does not support schedule-run cancellation")
                    }
                    (StatusCode::CONFLICT, Some("schedule_run_not_cancellable")) => {
                        anyhow::bail!("Schedule run has no cancellable live execution")
                    }
                    _ => anyhow::bail!("Could not cancel schedule run (HTTP {})", status.as_u16()),
                }
            }
            _ => anyhow::bail!("Could not cancel schedule run (HTTP {})", status.as_u16()),
        }
    }

    pub async fn create_schedule(&self, req: CreateScheduleRequest) -> Result<()> {
        use reqwest::StatusCode;

        let resp = self
            .client
            .post(self.url("/api/v1/schedules"))
            .json(&req)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!(SCHEDULE_CREATE_UNCONFIRMED))?;
        let status = resp.status();
        let refused = "The Host refused to create the schedule.";
        let message = match status {
            status if status.is_success() => return Ok(()),
            status if schedule_action_unconfirmed(status) => SCHEDULE_CREATE_UNCONFIRMED,
            StatusCode::BAD_REQUEST => {
                "Could not create the schedule. Check its details and Host settings."
            }
            StatusCode::CONFLICT => schedule_project_conflict(resp).await.unwrap_or(refused),
            _ => refused,
        };
        anyhow::bail!("{message}")
    }

    pub async fn delete_schedule(&self, id: &str) -> Result<()> {
        let resp = self
            .client
            .delete(self.url(&format!("/api/v1/schedules/{}", id)))
            .send()
            .await
            .map_err(|_| anyhow::anyhow!(SCHEDULE_DELETE_UNCONFIRMED))?;
        let status = resp.status();
        anyhow::ensure!(
            !schedule_action_unconfirmed(status),
            SCHEDULE_DELETE_UNCONFIRMED
        );
        anyhow::ensure!(
            status != reqwest::StatusCode::NOT_FOUND,
            SCHEDULE_UNAVAILABLE_OR_CHANGED
        );
        anyhow::ensure!(
            status.is_success(),
            "The Host refused to delete the schedule."
        );
        Ok(())
    }

    pub async fn run_schedule_now(&self, id: &str) -> Result<()> {
        use reqwest::StatusCode;

        let resp = self
            .client
            .post(self.url(&format!("/api/v1/schedules/{}/run", id)))
            .send()
            .await
            .map_err(|_| anyhow::anyhow!(SCHEDULE_RUN_UNCONFIRMED))?;
        let status = resp.status();
        let refused = "The Host refused Run Now. Check the saved schedule and Host settings.";
        let message = match status {
            status if status.is_success() => return Ok(()),
            status if schedule_action_unconfirmed(status) => SCHEDULE_RUN_UNCONFIRMED,
            StatusCode::NOT_FOUND => SCHEDULE_UNAVAILABLE_OR_CHANGED,
            StatusCode::CONFLICT => schedule_project_conflict(resp).await.unwrap_or(refused),
            _ => refused,
        };
        anyhow::bail!("{message}")
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

    #[test]
    fn chat_submission_accepts_both_ordinary_and_loop_control_replies() {
        let mut reply = serde_json::json!({
            "message_id": "accepted-message",
            "session_id": "chat",
            "stream_url": "/api/v1/events/chat",
            "status": "accepted",
        });
        let normal: ChatSubmission = serde_json::from_value(reply.clone()).unwrap();
        assert_eq!(
            normal.response.message_id.as_deref(),
            Some("accepted-message")
        );
        assert!(normal.loop_command.is_none());

        reply["loop_command"] = serde_json::json!({
            "action": "status", "should_execute": false, "loop": null,
        });
        let control: ChatSubmission = serde_json::from_value(reply).unwrap();
        let acknowledgment = control.loop_command.unwrap();
        assert!(!acknowledgment.should_execute);
        assert_eq!(acknowledgment.status_text(), "No session loop");
    }

    #[tokio::test]
    async fn loop_chat_posts_only_after_valid_session_loop_status() {
        use std::time::Duration;
        use tokio::{net::TcpListener, time::timeout};

        for (http_status, body, supported) in [
            ("404 Not Found", "{}", false),
            ("200 OK", r#"{"session_id":"chat"}"#, false),
            ("200 OK", r#"{"session_id":"other","loop":null}"#, false),
            ("200 OK", r#"{"session_id":"chat","loop":null}"#, true),
            (
                "200 OK",
                r#"{"session_id":"chat","loop":{"state":"scheduled","mode":"future","interval_seconds":"unrecognized","iteration":null}}"#,
                true,
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
            let backend = tokio::spawn(async move {
                let (mut status, _) = listener.accept().await.unwrap();
                let request = read_request(&mut status).await;
                assert!(request.starts_with("GET /api/v1/sessions/chat/loop HTTP/1.1"));
                assert!(request
                    .to_ascii_lowercase()
                    .contains("cache-control: no-store"));
                respond(&mut status, http_status, "1", body).await;
                if supported {
                    let (mut chat, _) = listener.accept().await.unwrap();
                    let request = read_request(&mut chat).await;
                    assert!(request.starts_with("POST /api/v1/chat HTTP/1.1"));
                    let body = request_json(&request);
                    assert_eq!(body["session_id"], "chat");
                    assert_eq!(body["message"], "/loop stop");
                    respond(
                        &mut chat,
                        "200 OK",
                        "1",
                        r#"{"session_id":"chat","stream_url":"/events","status":"accepted","loop_command":{"action":"stop","should_execute":false,"loop":null}}"#,
                    )
                    .await;
                } else {
                    assert!(timeout(Duration::from_millis(100), listener.accept())
                        .await
                        .is_err());
                }
            });

            let result = timeout(
                Duration::from_secs(5),
                client.submit_loop_chat(ChatRequest {
                    message_id: None,
                    thread_id: None,
                    in_reply_to: None,
                    correlation_id: None,
                    message: "/loop stop".into(),
                    images: Vec::new(),
                    session_id: Some("chat".into()),
                    project_id: None,
                    workspace_path: None,
                    model: None,
                    provider: None,
                    reasoning_effort: None,
                }),
            )
            .await
            .unwrap();
            backend.await.unwrap();
            assert_eq!(result.is_ok(), supported, "{http_status} {body}");
        }
    }

    const LOOP_PREVIEW_SESSION: &str = "session/one ?#%";

    async fn read_loop_preview_request(socket: &mut tokio::net::TcpStream) {
        let request = read_request(socket).await;
        let (headers, body) = request.split_once("\r\n\r\n").unwrap();
        assert_eq!(
            headers.lines().next(),
            Some("GET /api/v1/sessions/session%2Fone%20%3F%23%25/loop HTTP/1.1")
        );
        assert!(headers
            .lines()
            .any(|line| line.eq_ignore_ascii_case("cache-control: no-store")));
        assert!(body.is_empty());
    }

    async fn loop_preview_reply(status: &str, body: &str) -> Result<Option<SessionLoopPreview>> {
        use tokio::{net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_loop_preview_request(&mut socket).await;
            respond(&mut socket, status, "1", body).await;
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                client.selected_session_loop_status(LOOP_PREVIEW_SESSION),
                serve
            )
        })
        .await
        .unwrap();
        result
    }

    #[tokio::test]
    async fn selected_session_loop_status_reads_the_exact_session_and_known_states() {
        use SessionLoopPreviewState::{Expired, Paused, Scheduled, Stopped};

        for (wire, state, next_fire_at) in [
            ("scheduled", Scheduled, Some("2026-11-28T02:03:04.125Z")),
            ("scheduled", Scheduled, None),
            ("paused", Paused, None),
            ("stopped", Stopped, None),
            ("expired", Expired, None),
        ] {
            let body = serde_json::json!({
                "session_id": LOOP_PREVIEW_SESSION,
                "loop": {
                    "state": wire, "next_fire_at": next_fire_at,
                    "prompt": "Host prompt does not belong to the list preview",
                    "last_run_id": "host-run",
                },
            });
            assert_eq!(
                loop_preview_reply("200 OK", &body.to_string())
                    .await
                    .unwrap(),
                Some(SessionLoopPreview {
                    state,
                    next_fire_at: next_fire_at.map(|value| value.parse().unwrap()),
                    mode: None,
                    iteration: None,
                })
            );
        }
        let body = serde_json::json!({ "session_id": LOOP_PREVIEW_SESSION, "loop": null });
        assert_eq!(
            loop_preview_reply("200 OK", &body.to_string())
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn selected_session_loop_status_projects_optional_paced_details() {
        use serde_json::json;
        use SessionLoopPreviewMode::{Adaptive, Fixed};
        use SessionLoopPreviewState::Scheduled;

        for (details, mode, iteration) in [
            (
                json!({ "mode": "fixed", "interval_seconds": 300, "iteration": 7 }),
                Some(Fixed {
                    interval_seconds: Some(300),
                }),
                Some(7),
            ),
            (
                json!({ "mode": "adaptive", "interval_seconds": 91, "iteration": 2 }),
                Some(Adaptive),
                Some(2),
            ),
            (
                json!({ "mode": "fixed", "interval_seconds": 0, "iteration": 0 }),
                Some(Fixed {
                    interval_seconds: None,
                }),
                Some(0),
            ),
            (
                json!({ "mode": "fixed", "interval_seconds": null, "iteration": null }),
                Some(Fixed {
                    interval_seconds: None,
                }),
                None,
            ),
            (
                json!({ "mode": "fixed" }),
                Some(Fixed {
                    interval_seconds: None,
                }),
                None,
            ),
            (
                json!({ "mode": "future", "interval_seconds": 60, "iteration": 1 }),
                None,
                Some(1),
            ),
            (
                json!({ "mode": null, "interval_seconds": null, "iteration": null }),
                None,
                None,
            ),
            (
                json!({ "interval_seconds": 300, "iteration": 3 }),
                None,
                Some(3),
            ),
        ] {
            let mut record = details;
            record["state"] = json!("scheduled");
            record["next_fire_at"] = json!("2026-11-28T02:03:04Z");
            let body = json!({ "session_id": LOOP_PREVIEW_SESSION, "loop": record });
            assert_eq!(
                loop_preview_reply("200 OK", &body.to_string())
                    .await
                    .unwrap(),
                Some(SessionLoopPreview {
                    state: Scheduled,
                    next_fire_at: Some("2026-11-28T02:03:04Z".parse().unwrap()),
                    mode,
                    iteration,
                })
            );
        }
    }

    #[tokio::test]
    async fn selected_session_loop_status_rejects_invalid_receipts_and_sanitizes_errors() {
        for body in [
            r#"{"session_id":"other","loop":null}"#,
            r#"{"session_id":"session/one ?#%"}"#,
            r#"{"session_id":"other","session_id":"session/one ?#%","loop":null}"#,
            r#"{"session_id":"session/one ?#%","loop":null,"loop":{"state":"scheduled"}}"#,
            r#"{"session_id":"session/one ?#%","loop":{"state":"stopped","state":"scheduled"}}"#,
            r#"{"session_id":"session/one ?#%","loop":{"state":"scheduled","id":null,"id":"loop-one"}}"#,
            r#"{"session_id":"session/one ?#%","loop":{"state":"scheduled","next_fire_at":null,"next_fire_at":null}}"#,
            r#"{"session_id":"session/one ?#%","loop":{"state":"scheduled","mode":"future","mode":"fixed"}}"#,
            r#"{"session_id":"session/one ?#%","loop":{"state":"scheduled","interval_seconds":null,"interval_seconds":60}}"#,
            r#"{"session_id":"session/one ?#%","loop":{"state":"scheduled","iteration":null,"iteration":0}}"#,
            r#"["session/one ?#%",null]"#,
            r#"{"session_id":"session/one ?#%","loop":[null,"scheduled",null,null,null]}"#,
            r#"{"session_id":"session/one ?#%","loop":{"state":"RAW_HOST_DETAILS"}}"#,
            r#"{"session_id":"session/one ?#%","loop":{"state":"scheduled","next_fire_at":"RAW_HOST_DETAILS"}}"#,
            "RAW_HOST_DETAILS",
        ] {
            assert_eq!(
                loop_preview_reply("200 OK", body)
                    .await
                    .unwrap_err()
                    .to_string(),
                "Invalid Session loop status"
            );
        }

        for (status, code) in [
            ("202 Accepted", 202),
            ("403 Forbidden", 403),
            ("404 Not Found", 404),
        ] {
            assert_eq!(
                loop_preview_reply(status, "RAW_HOST_DETAILS")
                    .await
                    .unwrap_err()
                    .to_string(),
                format!("Could not fetch Session loop status (HTTP {code})")
            );
        }
    }

    #[tokio::test]
    async fn selected_session_loop_status_does_not_redirect_or_wait_for_error_body() {
        use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
        let (release, held) = oneshot::channel();
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_loop_preview_request(&mut socket).await;
            socket
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /unexpected\r\nContent-Length: 42\r\n\r\n",
                )
                .await
                .unwrap();
            held.await.unwrap();
            drop(socket);
        };
        let fetch = async {
            let result = client
                .selected_session_loop_status(LOOP_PREVIEW_SESSION)
                .await;
            release.send(()).unwrap();
            result
        };
        let (result, ()) = timeout(Duration::from_secs(4), async { tokio::join!(fetch, serve) })
            .await
            .unwrap();
        assert_eq!(
            result.unwrap_err().to_string(),
            "Could not fetch Session loop status (HTTP 302)"
        );
    }

    #[tokio::test]
    async fn selected_session_loop_status_bounds_advertised_and_chunked_bodies() {
        use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot, time::timeout};

        for advertised in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
            let (release, held) = oneshot::channel();
            let serve = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_loop_preview_request(&mut socket).await;
                if advertised {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                        MAX_SESSION_LOOP_PREVIEW_BYTES + 1
                    );
                    socket.write_all(headers.as_bytes()).await.unwrap();
                } else {
                    socket
                        .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                        .await
                        .unwrap();
                    const CHUNK_BYTES: usize = 16 * 1024;
                    let mut frame = format!("{CHUNK_BYTES:x}\r\n").into_bytes();
                    frame.resize(frame.len() + CHUNK_BYTES, b'x');
                    frame.extend_from_slice(b"\r\n");
                    for _ in 0..(MAX_SESSION_LOOP_PREVIEW_BYTES / CHUNK_BYTES) {
                        socket.write_all(&frame).await.unwrap();
                    }
                    socket.write_all(b"1\r\nx\r\n").await.unwrap();
                }
                // Keep the body open until the byte limit rejects the response.
                held.await.unwrap();
                drop(socket);
            };
            let fetch = async {
                let result = client
                    .selected_session_loop_status(LOOP_PREVIEW_SESSION)
                    .await;
                release.send(()).unwrap();
                result
            };
            let (result, ()) =
                timeout(Duration::from_secs(5), async { tokio::join!(fetch, serve) })
                    .await
                    .unwrap();
            assert_eq!(
                result.unwrap_err().to_string(),
                "Session loop status response is too large (128 KiB limit)"
            );
        }
    }

    async fn schedule_action_reply<T, F>(
        request_line: &str,
        request_body: Option<serde_json::Value>,
        status: &str,
        body: &str,
        request: impl FnOnce(BambooClient) -> F,
    ) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        use tokio::{net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert_eq!(request.lines().next(), Some(request_line));
            if let Some(expected) = request_body {
                assert_eq!(request_json(&request), expected);
            } else {
                assert_eq!(request.split_once("\r\n\r\n").unwrap().1, "");
            }
            respond(&mut socket, status, "1", body).await;
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(request(client), serve)
        })
        .await
        .unwrap();
        result
    }

    #[tokio::test]
    async fn list_schedules_preserves_empty_catalogs_and_returns_fixed_errors() {
        const GET: &str = "GET /api/v1/schedules HTTP/1.1";
        for body in [r#"{"schedules":[]}"#, "{}"] {
            assert!(
                schedule_action_reply(GET, None, "200 OK", body, |client| async move {
                    client.list_schedules().await
                })
                .await
                .unwrap()
                .is_empty()
            );
        }
        for (status, body, expected) in [
            (
                "503 Service Unavailable",
                r#"{"error":{"message":"RAW_HOST_BODY"}}"#,
                "Could not load schedules. Reopen Schedules to try again.",
            ),
            (
                "408 Request Timeout",
                "",
                "Could not load schedules. Reopen Schedules to try again.",
            ),
            (
                "409 Conflict",
                r#"{"error":{"code":"project_archived","message":"RAW_HOST_BODY"}}"#,
                "The Host refused to load schedules. Reopen Schedules to try again.",
            ),
            (
                "200 OK",
                r#"{"schedules":"RAW_HOST_BODY"}"#,
                "The Host returned an invalid schedule list. Reopen Schedules to try again.",
            ),
        ] {
            assert_eq!(
                schedule_action_reply(GET, None, status, body, |client| async move {
                    client.list_schedules().await
                })
                .await
                .unwrap_err()
                .to_string(),
                expected
            );
        }
    }

    async fn create_schedule_action_reply(expr: &str, status: &str, body: &str) -> Result<()> {
        schedule_action_reply(
            "POST /api/v1/schedules HTTP/1.1",
            Some(serde_json::json!({
                "name":"Review results",
                "trigger":{"type":"cron","expr":expr},
                "enabled":true,
                "run_config":{"task_message":"Check the report","auto_execute":true},
            })),
            status,
            body,
            |client| async move {
                client
                    .create_schedule(CreateScheduleRequest {
                        name: "Review results".into(),
                        trigger: ScheduleTriggerReq::Cron { expr: expr.into() },
                        timezone: None,
                        enabled: true,
                        run_config: ScheduleRunConfigReq {
                            project_id: None,
                            task_message: Some("Check the report".into()),
                            auto_execute: true,
                        },
                    })
                    .await
            },
        )
        .await
    }

    #[tokio::test]
    async fn create_schedule_preserves_request_and_maps_validation_and_project_failures() {
        const EXPR: &str = "0 0 9 * * MON";
        create_schedule_action_reply(EXPR, "201 Created", "")
            .await
            .unwrap();
        for (status, body, expected) in [
            (
                "400 Bad Request",
                r#"{"error":{"type":"api_error","message":"trigger.expr is not a valid cron expression: invalid cron"}}"#,
                "Could not create the schedule. Check its details and Host settings.",
            ),
            (
                "400 Bad Request",
                r#"{"error":{"code":"project_archived","message":"RAW_HOST_BODY"}}"#,
                "Could not create the schedule. Check its details and Host settings.",
            ),
            (
                "409 Conflict",
                r#"{"error":{"code":"project_archived","message":"RAW_HOST_BODY"},"project_id":"example"}"#,
                "The schedule needs an active Project.",
            ),
            (
                "409 Conflict",
                r#"{"error":{"code":"project_path_missing","message":"RAW_HOST_BODY"}}"#,
                "Set a workspace path for the schedule's Project.",
            ),
            (
                "409 Conflict",
                r#"{"error":{"code":"future_conflict","message":"RAW_HOST_BODY"}}"#,
                "The Host refused to create the schedule.",
            ),
            (
                "503 Service Unavailable",
                "RAW_HOST_BODY",
                "Could not confirm creation. Check Schedules before trying again.",
            ),
        ] {
            let expr = if status.starts_with("400") {
                "invalid cron"
            } else {
                EXPR
            };
            assert_eq!(
                create_schedule_action_reply(expr, status, body)
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn run_schedule_now_reports_stale_selection_project_conflicts_and_uncertain_outcomes() {
        const POST: &str = "POST /api/v1/schedules/schedule-17/run HTTP/1.1";
        let run =
            |client: BambooClient| async move { client.run_schedule_now("schedule-17").await };
        schedule_action_reply(POST, None, "202 Accepted", "", run)
            .await
            .unwrap();
        for (status, body, expected) in [
            (
                "404 Not Found",
                r#"{"error":{"type":"api_error","message":"Schedule not found"},"schedule_id":"schedule-17"}"#,
                "Schedule is unavailable or changed. Reopen Schedules.",
            ),
            (
                "409 Conflict",
                r#"{"error":{"code":"project_workspace_conflict","message":"RAW_HOST_BODY"}}"#,
                "The workspace belongs to another Project. Check the Project settings.",
            ),
            (
                "409 Conflict",
                r#"{"error":{"code":"project_path_unavailable","message":"RAW_HOST_BODY"}}"#,
                "The Project workspace is unavailable. Check its path on the Host.",
            ),
            (
                "400 Bad Request",
                r#"{"error":{"type":"api_error","message":"run_config.project_id references a Project that does not exist"},"project_id":"example"}"#,
                "The Host refused Run Now. Check the saved schedule and Host settings.",
            ),
            (
                "500 Internal Server Error",
                "RAW_HOST_BODY",
                "Could not confirm Run Now. Check Recent runs before trying again.",
            ),
        ] {
            assert_eq!(
                schedule_action_reply(POST, None, status, body, run)
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn delete_schedule_reports_stale_selection_and_uncertain_outcomes() {
        const DELETE: &str = "DELETE /api/v1/schedules/schedule-17 HTTP/1.1";
        let delete =
            |client: BambooClient| async move { client.delete_schedule("schedule-17").await };
        schedule_action_reply(DELETE, None, "204 No Content", "", delete)
            .await
            .unwrap();
        for (status, expected) in [
            (
                "404 Not Found",
                "Schedule is unavailable or changed. Reopen Schedules.",
            ),
            (
                "408 Request Timeout",
                "Could not confirm deletion. Check the schedule before trying again.",
            ),
            ("403 Forbidden", "The Host refused to delete the schedule."),
        ] {
            assert_eq!(
                schedule_action_reply(DELETE, None, status, "RAW_HOST_BODY", delete)
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn schedule_transport_failures_provide_actionable_list_and_run_feedback() {
        use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

        for (list, request_line, expected) in [
            (
                true,
                "GET /api/v1/schedules HTTP/1.1",
                "Could not load schedules. Reopen Schedules to try again.",
            ),
            (
                false,
                "POST /api/v1/schedules/schedule-17/run HTTP/1.1",
                "Could not confirm Run Now. Check Recent runs before trying again.",
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
            let serve = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                assert_eq!(
                    read_request(&mut socket).await.lines().next(),
                    Some(request_line)
                );
                socket.shutdown().await.unwrap();
            };
            let request = async {
                if list {
                    client.list_schedules().await.map(|_| ())
                } else {
                    client.run_schedule_now("schedule-17").await
                }
            };
            let (result, ()) = timeout(Duration::from_secs(5), async {
                tokio::join!(request, serve)
            })
            .await
            .unwrap();
            assert_eq!(result.unwrap_err().to_string(), expected);
        }
    }

    #[tokio::test]
    async fn schedule_project_conflict_falls_back_when_the_host_holds_the_error_body() {
        use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
        let (release, held) = oneshot::channel();
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                read_request(&mut socket).await.lines().next(),
                Some("POST /api/v1/schedules/schedule-17/run HTTP/1.1")
            );
            socket
                .write_all(b"HTTP/1.1 409 Conflict\r\nContent-Length: 99\r\n\r\n")
                .await
                .unwrap();
            held.await.unwrap();
            drop(socket);
        };
        let run = async {
            let result = client.run_schedule_now("schedule-17").await;
            release.send(()).unwrap();
            result
        };
        let (result, ()) = timeout(Duration::from_secs(5), async { tokio::join!(run, serve) })
            .await
            .unwrap();
        assert_eq!(
            result.unwrap_err().to_string(),
            "The Host refused Run Now. Check the saved schedule and Host settings."
        );
    }

    #[tokio::test]
    async fn list_schedules_projects_saved_cadence_in_server_order() {
        use std::time::Duration;
        use tokio::{net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            let (headers, body) = request.split_once("\r\n\r\n").unwrap();
            assert_eq!(
                headers.lines().next(),
                Some("GET /api/v1/schedules HTTP/1.1")
            );
            assert!(body.is_empty());
            respond(
                &mut socket,
                "200 OK",
                "1",
                r#"{"schedules":[
                    {"id":"cron","name":"晨间检查 🧭","enabled":true,"timezone":"UTC",
                     "trigger":{"type":"cron","expr":"0 30 8 * * *"},
                     "state":{"last_finished_at":"2026-10-20T08:31:00Z","next_fire_at":"2026-10-21T08:30:00Z"},
                     "run_config":{"task_message":"Review the overnight changes"}},
                    {"id":"once","enabled":false,"trigger":{"type":"once","at":"2026-10-20T16:15:00.125+08:00"}},
                    {"id":"weekly","timezone":"America/New_York",
                     "trigger":{"type":"weekly","weekdays":["mon","wed"],"hour":8,"minute":30,"second":5}},
                    {"id":"interval","trigger":{"type":"interval","every_seconds":90,"anchor_at":"2026-10-20T08:15:00Z"}},
                    {"id":"daily","trigger":{"type":"daily","hour":8,"minute":30}},
                    {"id":"monthly","timezone":null,
                     "trigger":{"type":"monthly","days":[1,15,31],"hour":8,"minute":30,"second":0}},
                    {"id":"future","trigger":{"type":"quarterly","months":[1,4,7,10]}},
                    {"id":"incomplete","trigger":{"type":"daily","hour":8}},
                    {"id":"legacy-cron","trigger":{"type":"cron"}},
                    {"id":"legacy-unknown"}
                ]}"#,
            )
            .await;
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(client.list_schedules(), serve)
        })
        .await
        .unwrap();
        let schedules = result.unwrap();

        let cadence: Vec<_> = schedules
            .iter()
            .map(|schedule| {
                (
                    schedule.id.as_str(),
                    schedule.cron.as_deref(),
                    schedule.cadence_detail.as_deref(),
                )
            })
            .collect();
        assert_eq!(
            cadence,
            [
                ("cron", Some("0 30 8 * * *"), Some("Cron 0 30 8 * * * UTC")),
                ("once", Some("once"), Some("Once 2026-10-20T08:15:00.125Z")),
                (
                    "weekly",
                    Some("weekly"),
                    Some("Weekly Mon, Wed at 08:30:05 America/New_York")
                ),
                ("interval", Some("interval"), Some("Every 90 seconds")),
                ("daily", Some("daily"), Some("Daily 08:30 UTC")),
                (
                    "monthly",
                    Some("monthly"),
                    Some("Monthly 1, 15, 31 at 08:30 UTC")
                ),
                ("future", Some("quarterly"), Some("Unknown trigger")),
                (
                    "incomplete",
                    Some("daily"),
                    Some("Daily cadence unavailable")
                ),
                (
                    "legacy-cron",
                    Some("cron"),
                    Some("Cron cadence unavailable")
                ),
                ("legacy-unknown", Some("-"), Some("Cadence unavailable")),
            ]
        );

        let cron = &schedules[0];
        assert_eq!(cron.name.as_deref(), Some("晨间检查 🧭"));
        assert_eq!(cron.enabled, Some(true));
        assert_eq!(schedules[1].enabled, Some(false));
        assert_eq!(cron.prompt.as_deref(), Some("Review the overnight changes"));
        assert_eq!(cron.last_run, Some("2026-10-20T08:31:00Z".parse().unwrap()));
        assert_eq!(cron.next_run, Some("2026-10-21T08:30:00Z".parse().unwrap()));
    }

    const SESSIONS_SCHEDULE_ID: &str = "schedule/17 ?#%";

    #[tokio::test]
    async fn list_schedule_sessions_preserves_host_order_and_session_summaries() {
        let sessions = schedule_sessions_reply(
            "200 OK",
            r#"{"schedule_id":"schedule/17 ?#%","sessions":[
                {"id":"session-z","created_by_schedule_id":"schedule/17 ?#%","title":"Saved review",
                 "updated_at":"2030-02-11T09:00:00Z","project_id":"project-42","is_running":true},
                {"id":"session-a","created_by_schedule_id":"schedule/17 ?#%","title":"Newer review",
                 "updated_at":"2030-02-12T09:00:00Z","last_run_status":"success"}
            ]}"#,
        )
        .await
        .unwrap();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].id, "session-z");
        assert_eq!(sessions[0].title, "Saved review");
        assert_eq!(sessions[0].project_id.as_deref(), Some("project-42"));
        assert!(sessions[0].is_running);
        assert_eq!(sessions[1].id, "session-a");
        assert_eq!(sessions[1].last_run_status.as_deref(), Some("success"));
        assert!(sessions[0].updated_at < sessions[1].updated_at);

        let empty = schedule_sessions_reply(
            "200 OK",
            r#"{"schedule_id":"schedule/17 ?#%","sessions":[]}"#,
        )
        .await
        .unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn list_schedule_sessions_rejects_malformed_and_foreign_rows_without_echoing_the_body() {
        for body in [
            r#"{"schedule_id":"DIFFERENT_SCHEDULE","sessions":[]}"#,
            r#"{"schedule_id":"schedule/17 ?#%"}"#,
            r#"{"schedule_id":"schedule/17 ?#%","sessions":[{"id":"UNRELATED_SESSION"}]}"#,
            r#"{"schedule_id":"schedule/17 ?#%","sessions":[
                {"id":"session-z","created_by_schedule_id":"schedule/17 ?#%"},
                {"id":"UNRELATED_SESSION","created_by_schedule_id":"DIFFERENT_SCHEDULE"}
            ]}"#,
            r#"{"schedule_id":"schedule/17 ?#%","sessions":[
                {"id":"session-z","created_by_schedule_id":null}
            ]}"#,
            r#"{"schedule_id":"schedule/17 ?#%","sessions":[
                {"id":"   ","created_by_schedule_id":"schedule/17 ?#%"}
            ]}"#,
            "RAW_HOST_RESPONSE",
        ] {
            assert_eq!(
                schedule_sessions_reply("200 OK", body)
                    .await
                    .unwrap_err()
                    .to_string(),
                "Invalid schedule sessions response"
            );
        }

        for (status, code) in [("202 Accepted", 202), ("404 Not Found", 404)] {
            assert_eq!(
                schedule_sessions_reply(status, "RAW_HOST_RESPONSE")
                    .await
                    .unwrap_err()
                    .to_string(),
                format!("Could not fetch schedule sessions (HTTP {code})")
            );
        }
    }

    #[tokio::test]
    async fn list_schedule_sessions_bounds_advertised_and_chunked_responses() {
        use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot, time::timeout};

        for advertised in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
            let (release, held) = oneshot::channel();
            let serve = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                assert_eq!(
                    read_request(&mut socket).await.lines().next(),
                    Some("GET /api/v1/schedules/schedule-17/sessions HTTP/1.1")
                );
                if advertised {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
                        MAX_SCHEDULE_RESPONSE_BYTES + 1
                    );
                    socket.write_all(headers.as_bytes()).await.unwrap();
                } else {
                    socket
                        .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
                        .await
                        .unwrap();
                    const CHUNK_BYTES: usize = 64 * 1024;
                    let mut frame = format!("{CHUNK_BYTES:x}\r\n").into_bytes();
                    frame.resize(frame.len() + CHUNK_BYTES, b'x');
                    frame.extend_from_slice(b"\r\n");
                    for _ in 0..(MAX_SCHEDULE_RESPONSE_BYTES / CHUNK_BYTES) {
                        socket.write_all(&frame).await.unwrap();
                    }
                    socket.write_all(b"1\r\nx\r\n").await.unwrap();
                }
                // Do not finish the HTTP body before the client rejects it.
                held.await.unwrap();
                drop(socket);
            };
            let fetch = async {
                let result = client.list_schedule_sessions("schedule-17").await;
                release.send(()).unwrap();
                result
            };
            let (result, ()) =
                timeout(Duration::from_secs(5), async { tokio::join!(fetch, serve) })
                    .await
                    .unwrap();
            assert_eq!(
                result.unwrap_err().to_string(),
                "Schedule sessions response is too large (2 MiB limit)"
            );
        }
    }

    async fn schedule_sessions_reply(status: &str, body: &str) -> Result<Vec<SessionSummary>> {
        use tokio::{net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!(
            "http://{}/gateway/",
            listener.local_addr().unwrap()
        ));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            let (head, body_sent) = request.split_once("\r\n\r\n").unwrap();
            assert_eq!(
                head.lines().next(),
                Some("GET /gateway/api/v1/schedules/schedule%2F17%20%3F%23%25/sessions HTTP/1.1")
            );
            assert!(body_sent.is_empty());
            respond(&mut socket, status, "1", body).await;
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(client.list_schedule_sessions(SESSIONS_SCHEDULE_ID), serve)
        })
        .await
        .unwrap();
        result
    }

    #[tokio::test]
    async fn list_schedule_runs_reads_typed_history_in_server_order() {
        let runs = schedule_runs_reply(
            "200 OK",
            r#"{"schedule_id":"schedule-17","runs":[
                {"run_id":"latest","scheduled_for":"2030-02-13T17:00:00+08:00","status":"future_status",
                 "was_catch_up":false,"session_id":"session-CaSe_42"},
                {"run_id":"older","scheduled_for":"2030-02-12T09:00:00Z","status":"succeeded",
                 "workflow_run_id":"workflow-12"},
                {"run_id":"oldest","scheduled_for":"2030-02-11T09:00:00Z","status":"skipped","session_id":null}
            ]}"#,
        )
        .await
        .unwrap();
        assert_eq!(
            runs,
            [
                ScheduleRunView {
                    run_id: "latest".into(),
                    scheduled_for: "2030-02-13T09:00:00Z".parse().unwrap(),
                    status: "future_status".into(),
                    session_id: Some("session-CaSe_42".into()),
                    workflow_run_id: None,
                },
                ScheduleRunView {
                    run_id: "older".into(),
                    scheduled_for: "2030-02-12T09:00:00Z".parse().unwrap(),
                    status: "succeeded".into(),
                    session_id: None,
                    workflow_run_id: Some("workflow-12".into()),
                },
                ScheduleRunView {
                    run_id: "oldest".into(),
                    scheduled_for: "2030-02-11T09:00:00Z".parse().unwrap(),
                    status: "skipped".into(),
                    session_id: None,
                    workflow_run_id: None,
                },
            ]
        );
        assert!(
            schedule_runs_reply("200 OK", r#"{"schedule_id":"schedule-17","runs":[]}"#)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn list_schedule_runs_rejects_wrong_schedule_and_reports_only_http_status() {
        for (status, body, error) in [
            (
                "200 OK",
                r#"{"schedule_id":"other","runs":[]}"#,
                "Schedule runs response has a different schedule ID",
            ),
            (
                "200 OK",
                r#"{"schedule_id":"schedule-17"}"#,
                "Invalid schedule runs response",
            ),
            (
                "503 Service Unavailable",
                r#"{"error":"backend details"}"#,
                "Could not fetch schedule runs (503 Service Unavailable)",
            ),
        ] {
            assert_eq!(
                schedule_runs_reply(status, body)
                    .await
                    .unwrap_err()
                    .to_string(),
                error
            );
        }
    }

    async fn schedule_runs_reply(status: &str, body: &str) -> Result<Vec<ScheduleRunView>> {
        use std::time::Duration;
        use tokio::{net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            let (head, body_sent) = request.split_once("\r\n\r\n").unwrap();
            assert_eq!(
                head.lines().next(),
                Some("GET /api/v1/schedules/schedule-17/runs HTTP/1.1")
            );
            assert_eq!(body_sent, "");
            respond(&mut socket, status, "1", body).await;
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(client.list_schedule_runs("schedule-17"), serve)
        })
        .await
        .unwrap();
        result
    }

    const TOGGLE_SCHEDULE_ID: &str = "schedule/17 ?#%";

    #[tokio::test]
    async fn rename_schedule_requires_the_exact_receipt_and_keeps_the_dedicated_route() {
        use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

        for (status, extra, body, expected) in [
            (
                "200 OK",
                "",
                r#"{"id":"other","name":"Weekly plan"}"#,
                "Schedule rename response does not match the request",
            ),
            (
                "200 OK",
                "",
                r#"{"id":"schedule-17","name":"Earlier title"}"#,
                "Schedule rename response does not match the request",
            ),
            (
                "404 Not Found",
                "",
                r#"{"error":{"code":"schedule_not_found","message":"RAW_DETAILS"}}"#,
                "Schedule no longer exists",
            ),
            (
                "307 Temporary Redirect",
                "Location: /api/v1/schedules/schedule-17\r\n",
                "",
                "Could not rename schedule (HTTP 307)",
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
            let serve = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request = read_request(&mut socket).await;
                assert_eq!(
                    request.lines().next(),
                    Some("PATCH /api/v1/schedules/schedule-17/name HTTP/1.1")
                );
                let response = format!(
                    "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
                if !extra.is_empty() {
                    assert!(timeout(Duration::from_millis(120), listener.accept())
                        .await
                        .is_err());
                }
            };
            let (result, ()) = timeout(Duration::from_secs(5), async {
                tokio::join!(client.rename_schedule("schedule-17", "Weekly plan"), serve)
            })
            .await
            .unwrap();
            assert_eq!(result.unwrap_err().to_string(), expected);
        }
    }

    #[tokio::test]
    async fn update_schedule_daily_time_sends_only_clock_and_projects_confirmed_timezone() {
        let body = serde_json::json!({
            "id": TOGGLE_SCHEDULE_ID,
            "name": "Morning review",
            "enabled": false,
            "trigger": {"type":"daily","hour":8,"minute":15,"second":0},
            "timezone": "Asia/Shanghai",
            "state": {"next_fire_at":"2030-02-13T00:15:00Z"},
            "run_config": {"task_message":"Review progress"},
        });
        let schedule =
            schedule_clock_time_reply(ScheduleClockKind::Daily, "200 OK", "", &body.to_string())
                .await
                .unwrap();
        assert_eq!(schedule.id, TOGGLE_SCHEDULE_ID);
        assert_eq!(schedule.daily_time, ScheduleClockTime::new(8, 15, 0));
        assert_eq!(schedule.timezone.as_deref(), Some("Asia/Shanghai"));
        assert_eq!(schedule.enabled, Some(false));
        assert_eq!(
            schedule.cadence_detail.as_deref(),
            Some("Daily 08:15 Asia/Shanghai")
        );
        assert_eq!(
            schedule.next_run.unwrap().to_rfc3339(),
            "2030-02-13T00:15:00+00:00"
        );
    }

    #[tokio::test]
    async fn update_schedule_daily_time_rejects_mismatched_or_incomplete_receipts() {
        let daily = serde_json::json!({"type":"daily","hour":8,"minute":15,"second":0});
        for (id, trigger) in [
            ("other-schedule", daily.clone()),
            (
                TOGGLE_SCHEDULE_ID,
                serde_json::json!({"type":"daily","hour":8,"minute":15,"second":1}),
            ),
            (
                TOGGLE_SCHEDULE_ID,
                serde_json::json!({"type":"daily","hour":8,"minute":15}),
            ),
            (
                TOGGLE_SCHEDULE_ID,
                serde_json::json!({"type":"weekly","weekdays":["mon"],"hour":8,"minute":15,"second":0}),
            ),
        ] {
            let body =
                serde_json::json!({"id": id, "trigger": trigger, "state": {}, "run_config": {}});
            assert_eq!(
                schedule_clock_time_reply(
                    ScheduleClockKind::Daily,
                    "200 OK",
                    "",
                    &body.to_string()
                )
                .await
                .unwrap_err()
                .to_string(),
                "Daily time response does not match the request"
            );
        }
        for body in [
            serde_json::json!({"id": TOGGLE_SCHEDULE_ID, "trigger": daily}),
            serde_json::json!({"id": TOGGLE_SCHEDULE_ID, "trigger": daily, "run_config": {}}),
            serde_json::json!({"id": TOGGLE_SCHEDULE_ID, "trigger": daily, "state": {}}),
        ] {
            assert_eq!(
                schedule_clock_time_reply(
                    ScheduleClockKind::Daily,
                    "200 OK",
                    "",
                    &body.to_string()
                )
                .await
                .unwrap_err()
                .to_string(),
                "Invalid Daily time response"
            );
        }
    }

    #[tokio::test]
    async fn update_schedule_daily_time_sanitizes_refusals_and_never_falls_back() {
        for (status, code, expected) in [
            (
                "400 Bad Request",
                Some("invalid_schedule_daily_time"),
                "Enter a Daily time in HH:MM or HH:MM:SS format",
            ),
            (
                "409 Conflict",
                Some("schedule_trigger_changed"),
                "Schedule is no longer a Daily schedule",
            ),
            (
                "409 Conflict",
                Some("schedule_no_future_occurrence"),
                "Schedule has no future occurrence",
            ),
            (
                "404 Not Found",
                Some("schedule_not_found"),
                "Schedule no longer exists",
            ),
            (
                "404 Not Found",
                None,
                "Editing Daily time requires a newer Host",
            ),
            (
                "405 Method Not Allowed",
                None,
                "Editing Daily time requires a newer Host",
            ),
            (
                "503 Service Unavailable",
                Some("schedule_trigger_changed"),
                "Could not update Daily time (HTTP 503)",
            ),
        ] {
            let body = code.map_or_else(
                || "RAW_DETAILS".to_owned(),
                |code| {
                    serde_json::json!({"error":{"code":code,"message":"RAW_DETAILS"}}).to_string()
                },
            );
            assert_eq!(
                schedule_clock_time_reply(ScheduleClockKind::Daily, status, "", &body)
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
        assert_eq!(
            schedule_clock_time_reply(
                ScheduleClockKind::Daily,
                "307 Temporary Redirect",
                "Location: /api/v1/schedules/schedule-17\r\n",
                "",
            )
            .await
            .unwrap_err()
            .to_string(),
            "Could not update Daily time (HTTP 307)"
        );
    }

    #[tokio::test]
    async fn update_schedule_weekly_time_sends_only_clock_and_retains_reply_weekdays() {
        let body = serde_json::json!({
            "id": TOGGLE_SCHEDULE_ID,
            "name": "Weekly review",
            "enabled": false,
            "trigger": {"type":"weekly","weekdays":["fri","mon"],"hour":8,"minute":15,"second":0},
            "timezone": "Asia/Shanghai",
            "state": {"next_fire_at":"2030-02-18T00:15:00Z"},
            "run_config": {"task_message":"Review progress"},
        });
        let schedule =
            schedule_clock_time_reply(ScheduleClockKind::Weekly, "200 OK", "", &body.to_string())
                .await
                .unwrap();
        assert_eq!(schedule.id, TOGGLE_SCHEDULE_ID);
        assert_eq!(
            schedule.clock_time(ScheduleClockKind::Weekly),
            ScheduleClockTime::new(8, 15, 0)
        );
        assert_eq!(schedule.clock_time(ScheduleClockKind::Daily), None);
        assert_eq!(
            schedule.weekly_time.unwrap().weekdays,
            [ScheduleWeekdayReq::Fri, ScheduleWeekdayReq::Mon]
        );
        assert_eq!(schedule.timezone.as_deref(), Some("Asia/Shanghai"));
        assert_eq!(schedule.enabled, Some(false));
        assert_eq!(
            schedule.cadence_detail.as_deref(),
            Some("Weekly Mon, Fri at 08:15 Asia/Shanghai")
        );
        assert_eq!(
            schedule.next_run.unwrap().to_rfc3339(),
            "2030-02-18T00:15:00+00:00"
        );
    }

    #[tokio::test]
    async fn update_schedule_weekly_time_requires_an_exact_ordinary_weekly_receipt() {
        let weekly =
            serde_json::json!({"type":"weekly","weekdays":["mon"],"hour":8,"minute":15,"second":0});
        for (id, trigger, config) in [
            ("other-schedule", weekly.clone(), serde_json::json!({})),
            (
                TOGGLE_SCHEDULE_ID,
                serde_json::json!({"type":"daily","hour":8,"minute":15,"second":0}),
                serde_json::json!({}),
            ),
            (
                TOGGLE_SCHEDULE_ID,
                serde_json::json!({"type":"weekly","weekdays":[],"hour":8,"minute":15,"second":0}),
                serde_json::json!({}),
            ),
            (
                TOGGLE_SCHEDULE_ID,
                serde_json::json!({"type":"weekly","weekdays":["unknown"],"hour":8,"minute":15,"second":0}),
                serde_json::json!({}),
            ),
            (
                TOGGLE_SCHEDULE_ID,
                serde_json::json!({"type":"weekly","weekdays":["mon"],"hour":8,"minute":15}),
                serde_json::json!({}),
            ),
            (
                TOGGLE_SCHEDULE_ID,
                serde_json::json!({"type":"weekly","weekdays":["mon"],"hour":8,"minute":15,"second":1}),
                serde_json::json!({}),
            ),
            (
                TOGGLE_SCHEDULE_ID,
                weekly.clone(),
                serde_json::json!({"workflow_target":{"workflow_id":"report"}}),
            ),
        ] {
            let body =
                serde_json::json!({"id":id,"trigger":trigger,"state":{},"run_config":config});
            assert_eq!(
                schedule_clock_time_reply(
                    ScheduleClockKind::Weekly,
                    "200 OK",
                    "",
                    &body.to_string()
                )
                .await
                .unwrap_err()
                .to_string(),
                "Weekly time response does not match the request"
            );
        }
        let body = serde_json::json!({"id":TOGGLE_SCHEDULE_ID,"trigger":weekly,"run_config":{}});
        assert_eq!(
            schedule_clock_time_reply(ScheduleClockKind::Weekly, "200 OK", "", &body.to_string())
                .await
                .unwrap_err()
                .to_string(),
            "Invalid Weekly time response"
        );
    }

    #[tokio::test]
    async fn update_schedule_weekly_time_sanitizes_refusals_and_requires_a_supported_host() {
        for (status, code, expected) in [
            (
                "400 Bad Request",
                "invalid_schedule_weekly_time",
                "Enter a Weekly time in HH:MM or HH:MM:SS format",
            ),
            (
                "409 Conflict",
                "schedule_trigger_changed",
                "Schedule is no longer a Weekly schedule",
            ),
            (
                "409 Conflict",
                "schedule_no_future_occurrence",
                "Schedule has no future occurrence",
            ),
            (
                "404 Not Found",
                "schedule_not_found",
                "Schedule no longer exists",
            ),
            (
                "404 Not Found",
                "unknown_route",
                "Editing Weekly time requires a newer Host",
            ),
            (
                "405 Method Not Allowed",
                "method_not_allowed",
                "Editing Weekly time requires a newer Host",
            ),
        ] {
            let body = serde_json::json!({"error":{"code":code,"message":"RAW_DETAILS"}});
            assert_eq!(
                schedule_clock_time_reply(ScheduleClockKind::Weekly, status, "", &body.to_string())
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn update_schedule_monthly_time_sends_only_clock_and_retains_reply_days() {
        let body = serde_json::json!({
            "id": TOGGLE_SCHEDULE_ID,
            "name": "Monthly review",
            "enabled": false,
            "trigger": {"type":"monthly","days":[31,2,1,31],"hour":8,"minute":15,"second":0},
            "timezone": "Asia/Shanghai",
            "state": {"next_fire_at":"2030-03-31T00:15:00Z"},
            "run_config": {"task_message":"Review progress"},
        });
        let schedule =
            schedule_clock_time_reply(ScheduleClockKind::Monthly, "200 OK", "", &body.to_string())
                .await
                .unwrap();
        assert_eq!(schedule.id, TOGGLE_SCHEDULE_ID);
        assert_eq!(
            schedule.clock_time(ScheduleClockKind::Monthly),
            ScheduleClockTime::new(8, 15, 0)
        );
        assert_eq!(schedule.clock_time(ScheduleClockKind::Daily), None);
        assert_eq!(schedule.clock_time(ScheduleClockKind::Weekly), None);
        assert_eq!(schedule.monthly_time.unwrap().days, [31, 2, 1, 31]);
        assert_eq!(schedule.timezone.as_deref(), Some("Asia/Shanghai"));
        assert_eq!(schedule.enabled, Some(false));
        assert_eq!(
            schedule.cadence_detail.as_deref(),
            Some("Monthly 1–2, 31 at 08:15 Asia/Shanghai")
        );
        assert_eq!(
            schedule.next_run.unwrap().to_rfc3339(),
            "2030-03-31T00:15:00+00:00"
        );
    }

    #[tokio::test]
    async fn update_schedule_monthly_time_requires_an_exact_ordinary_monthly_receipt() {
        let receipt = serde_json::json!({
            "id": TOGGLE_SCHEDULE_ID,
            "trigger": {"type":"monthly","days":[31],"hour":8,"minute":15,"second":0},
            "state": {},
            "run_config": {},
        });
        for (path, value) in [
            ("/id", serde_json::json!("other-schedule")),
            (
                "/trigger",
                serde_json::json!({"type":"daily","hour":8,"minute":15,"second":0}),
            ),
            ("/trigger/days", serde_json::json!([])),
            ("/trigger/days", serde_json::json!([1, 32])),
            ("/trigger/second", serde_json::json!(1)),
            (
                "/trigger",
                serde_json::json!({"type":"monthly","days":[31],"hour":8,"minute":15}),
            ),
            (
                "/run_config",
                serde_json::json!({"workflow_target":{"workflow_id":"review"}}),
            ),
        ] {
            let mut body = receipt.clone();
            *body.pointer_mut(path).unwrap() = value;
            assert_eq!(
                schedule_clock_time_reply(
                    ScheduleClockKind::Monthly,
                    "200 OK",
                    "",
                    &body.to_string()
                )
                .await
                .unwrap_err()
                .to_string(),
                "Monthly time response does not match the request"
            );
        }
        for field in ["state", "run_config"] {
            let mut body = receipt.clone();
            body.as_object_mut().unwrap().remove(field);
            assert_eq!(
                schedule_clock_time_reply(
                    ScheduleClockKind::Monthly,
                    "200 OK",
                    "",
                    &body.to_string()
                )
                .await
                .unwrap_err()
                .to_string(),
                "Invalid Monthly time response"
            );
        }
    }

    #[tokio::test]
    async fn update_schedule_monthly_time_sanitizes_refusals_and_requires_a_supported_host() {
        for (status, code, expected) in [
            (
                "400 Bad Request",
                "invalid_schedule_monthly_time",
                "Enter a Monthly time in HH:MM or HH:MM:SS format",
            ),
            (
                "409 Conflict",
                "schedule_trigger_changed",
                "Schedule is no longer a Monthly schedule",
            ),
            (
                "409 Conflict",
                "schedule_no_future_occurrence",
                "Schedule has no future occurrence",
            ),
            (
                "404 Not Found",
                "schedule_not_found",
                "Schedule no longer exists",
            ),
            (
                "404 Not Found",
                "unknown_route",
                "Editing Monthly time requires a newer Host",
            ),
            (
                "405 Method Not Allowed",
                "method_not_allowed",
                "Editing Monthly time requires a newer Host",
            ),
        ] {
            let body = serde_json::json!({"error":{"code":code,"message":"RAW_DETAILS"}});
            assert_eq!(
                schedule_clock_time_reply(
                    ScheduleClockKind::Monthly,
                    status,
                    "",
                    &body.to_string()
                )
                .await
                .unwrap_err()
                .to_string(),
                expected
            );
        }
    }

    async fn schedule_clock_time_reply(
        kind: ScheduleClockKind,
        status: &str,
        extra: &str,
        body: &str,
    ) -> Result<Schedule> {
        use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!(
            "http://{}/gateway/",
            listener.local_addr().unwrap()
        ));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert_eq!(
                request.lines().next(),
                Some(match kind {
                    ScheduleClockKind::Daily => "PATCH /gateway/api/v1/schedules/schedule%2F17%20%3F%23%25/daily-time HTTP/1.1",
                    ScheduleClockKind::Weekly => "PATCH /gateway/api/v1/schedules/schedule%2F17%20%3F%23%25/weekly-time HTTP/1.1",
                    ScheduleClockKind::Monthly => "PATCH /gateway/api/v1/schedules/schedule%2F17%20%3F%23%25/monthly-time HTTP/1.1",
                })
            );
            assert_eq!(
                request_json(&request),
                serde_json::json!({"hour":8,"minute":15,"second":0})
            );
            let response = format!(
                "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
            if status.starts_with("404") || status.starts_with("405") || status.starts_with("307") {
                assert!(timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err());
            }
        };
        let time = ScheduleClockTime::new(8, 15, 0).unwrap();
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                client.update_schedule_clock_time(TOGGLE_SCHEDULE_ID, kind, time),
                serve
            )
        })
        .await
        .unwrap();
        result
    }

    #[tokio::test]
    async fn update_schedule_once_at_sends_only_rfc3339_and_retains_confirmed_instant() {
        let body = serde_json::json!({
            "id": TOGGLE_SCHEDULE_ID,
            "name": "Review results",
            "enabled": false,
            "trigger": {"type":"once","at":"2030-03-31T08:15:00.125+08:00"},
            "timezone": "Asia/Shanghai",
            "state": {"next_fire_at": ONCE_SCHEDULE_AT},
            "run_config": {"task_message": "Review results"},
        });
        let schedule = schedule_once_at_reply(ONCE_SCHEDULE_AT, "200 OK", "", &body.to_string())
            .await
            .unwrap();
        let at = ONCE_SCHEDULE_AT.parse().unwrap();
        assert_eq!(schedule.id, TOGGLE_SCHEDULE_ID);
        assert_eq!(schedule.once_at, Some(at));
        assert_eq!(schedule.next_run, Some(at));
        assert_eq!(schedule.enabled, Some(false));
        assert_eq!(schedule.timezone.as_deref(), Some("Asia/Shanghai"));
    }

    #[tokio::test]
    async fn update_schedule_once_at_preserves_a_valid_offset_across_the_utc_year_boundary() {
        let wire_at = "9999-12-31T23:59:59.125-23:59";
        let utc_at = "+10000-01-01T23:58:59.125Z";
        let body = serde_json::json!({
            "id": TOGGLE_SCHEDULE_ID,
            "trigger": {"type":"once","at":utc_at},
            "state": {"next_fire_at":utc_at},
            "run_config": {},
        });
        let schedule = schedule_once_at_reply(wire_at, "200 OK", "", &body.to_string())
            .await
            .unwrap();
        let at = chrono::DateTime::parse_from_rfc3339(wire_at)
            .unwrap()
            .to_utc();
        assert_eq!(schedule.once_at, Some(at));
        assert_eq!(schedule.next_run, Some(at));
    }

    #[tokio::test]
    async fn update_schedule_once_at_rejects_mismatched_or_incomplete_receipts() {
        let receipt = serde_json::json!({
            "id": TOGGLE_SCHEDULE_ID,
            "trigger": {"type":"once","at":ONCE_SCHEDULE_AT},
            "state": {"next_fire_at":ONCE_SCHEDULE_AT},
            "run_config": {},
        });
        for (path, value) in [
            ("/id", serde_json::json!("different-id")),
            ("/trigger/type", serde_json::json!("daily")),
            ("/trigger/at", serde_json::json!("2030-03-31T00:15:00Z")),
            (
                "/state/next_fire_at",
                serde_json::json!("2030-03-31T00:15:00Z"),
            ),
            ("/state/next_fire_at", serde_json::json!(null)),
            (
                "/run_config",
                serde_json::json!({"workflow_target":{"workflow_id":"review"}}),
            ),
        ] {
            let mut body = receipt.clone();
            *body.pointer_mut(path).unwrap() = value;
            assert_eq!(
                schedule_once_at_reply(ONCE_SCHEDULE_AT, "200 OK", "", &body.to_string())
                    .await
                    .unwrap_err()
                    .to_string(),
                "Once time response does not match the request"
            );
        }
        for field in ["state", "run_config"] {
            let mut body = receipt.clone();
            body.as_object_mut().unwrap().remove(field);
            assert_eq!(
                schedule_once_at_reply(ONCE_SCHEDULE_AT, "200 OK", "", &body.to_string())
                    .await
                    .unwrap_err()
                    .to_string(),
                "Invalid Once time response"
            );
        }
    }

    #[tokio::test]
    async fn update_schedule_once_at_preserves_typed_refusals_and_never_falls_back() {
        for (status, code, expected) in [
            (
                "400 Bad Request",
                "invalid_schedule_once_at",
                "Enter a future RFC3339 timestamp for Once time",
            ),
            (
                "409 Conflict",
                "schedule_once_not_pending",
                "Once schedule no longer has a pending occurrence",
            ),
            (
                "409 Conflict",
                "schedule_trigger_changed",
                "Schedule is no longer a Once schedule",
            ),
            (
                "409 Conflict",
                "schedule_no_future_occurrence",
                "Schedule has no future occurrence",
            ),
            (
                "404 Not Found",
                "schedule_not_found",
                "Schedule no longer exists",
            ),
            (
                "404 Not Found",
                "unknown_route",
                "Editing Once time requires a newer Host",
            ),
            (
                "405 Method Not Allowed",
                "method_not_allowed",
                "Editing Once time requires a newer Host",
            ),
            (
                "503 Service Unavailable",
                "internal_error",
                "Could not update Once time (HTTP 503)",
            ),
        ] {
            let body = serde_json::json!({"error":{"code":code,"message":"RAW_DETAILS"}});
            assert_eq!(
                schedule_once_at_reply(ONCE_SCHEDULE_AT, status, "", &body.to_string())
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
        assert_eq!(
            schedule_once_at_reply(
                ONCE_SCHEDULE_AT,
                "307 Temporary Redirect",
                "Location: /api/v1/schedules/other\r\n",
                ""
            )
            .await
            .unwrap_err()
            .to_string(),
            "Could not update Once time (HTTP 307)"
        );
    }

    const ONCE_SCHEDULE_AT: &str = "2030-03-31T00:15:00.125Z";

    async fn schedule_once_at_reply(
        wire_at: &str,
        status: &str,
        extra: &str,
        body: &str,
    ) -> Result<Schedule> {
        use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!(
            "http://{}/gateway/",
            listener.local_addr().unwrap()
        ));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert_eq!(
                request.lines().next(),
                Some("PATCH /gateway/api/v1/schedules/schedule%2F17%20%3F%23%25/once-at HTTP/1.1")
            );
            assert_eq!(request_json(&request), serde_json::json!({"at":wire_at}));
            let response = format!(
                "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
            if status.starts_with("404") || status.starts_with("405") || status.starts_with("307") {
                assert!(timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err());
            }
        };
        let at = chrono::DateTime::parse_from_rfc3339(wire_at)
            .unwrap()
            .to_utc();
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                client.update_schedule_once_at(TOGGLE_SCHEDULE_ID, at),
                serve
            )
        })
        .await
        .unwrap();
        result
    }

    const CRON_EXPRESSION: &str = "0  30 8 * * MON-FRI";

    fn cron_expression_receipt() -> serde_json::Value {
        serde_json::json!({
            "id": TOGGLE_SCHEDULE_ID,
            "name": "Review results",
            "enabled": false,
            "trigger": {"type": "cron", "expr": CRON_EXPRESSION},
            "timezone": "America/New_York",
            "state": {"next_fire_at": "2030-04-01T12:30:00Z"},
            "run_config": {"task_message": "Review results"},
        })
    }

    #[tokio::test]
    async fn update_schedule_cron_expression_sends_only_expr_and_keeps_canonical_state() {
        let mut body = cron_expression_receipt();
        let input = format!(" \t{CRON_EXPRESSION}\n");
        let schedule = cron_expression_reply(&input, "200 OK", "", &body.to_string())
            .await
            .unwrap();
        assert_eq!(schedule.id, TOGGLE_SCHEDULE_ID);
        assert_eq!(schedule.name.as_deref(), Some("Review results"));
        assert_eq!(schedule.cron.as_deref(), Some(CRON_EXPRESSION));
        assert_eq!(schedule.cron_expression.as_deref(), Some(CRON_EXPRESSION));
        assert_eq!(
            schedule.next_run,
            Some("2030-04-01T12:30:00Z".parse().unwrap())
        );
        assert_eq!(schedule.enabled, Some(false));
        assert_eq!(schedule.timezone.as_deref(), Some("America/New_York"));
        assert_eq!(schedule.prompt.as_deref(), Some("Review results"));

        body["trigger"]["expr"] = serde_json::json!(input);
        for state in [
            serde_json::json!({}),
            serde_json::json!({"next_fire_at":null}),
        ] {
            body["state"] = state;
            let saved = cron_expression_reply(CRON_EXPRESSION, "200 OK", "", &body.to_string())
                .await
                .unwrap();
            assert_eq!(
                saved.cron_expression.as_deref(),
                body["trigger"]["expr"].as_str()
            );
            assert_eq!(saved.next_run, None);
            assert_eq!(saved.enabled, Some(false));
        }
    }

    #[tokio::test]
    async fn update_schedule_cron_expression_rejects_mismatched_or_incomplete_receipts() {
        let receipt = cron_expression_receipt();
        for (path, value) in [
            ("/id", serde_json::json!("different-id")),
            (
                "/trigger",
                serde_json::json!({"type":"daily","hour":8,"minute":30,"second":0}),
            ),
            ("/trigger/expr", serde_json::json!("@daily")),
            (
                "/run_config",
                serde_json::json!({"workflow_target":{"workflow_id":"review"}}),
            ),
        ] {
            let mut body = receipt.clone();
            *body.pointer_mut(path).unwrap() = value;
            assert_eq!(
                cron_expression_reply(CRON_EXPRESSION, "200 OK", "", &body.to_string())
                    .await
                    .unwrap_err()
                    .to_string(),
                "Cron expression response does not match the request"
            );
        }
        for field in ["state", "run_config", "name", "enabled"] {
            let mut body = receipt.clone();
            body.as_object_mut().unwrap().remove(field);
            assert_eq!(
                cron_expression_reply(CRON_EXPRESSION, "200 OK", "", &body.to_string())
                    .await
                    .unwrap_err()
                    .to_string(),
                "Invalid Cron expression response"
            );
        }
        let body = receipt.to_string();
        for conflicting in [
            body.replacen(r#""id":"#, r#""id":"other","id":"#, 1),
            body.replacen(r#""expr":"#, r#""expr":"@daily","expr":"#, 1),
        ] {
            assert_eq!(
                cron_expression_reply(CRON_EXPRESSION, "200 OK", "", &conflicting)
                    .await
                    .unwrap_err()
                    .to_string(),
                "Invalid Cron expression response"
            );
        }
    }

    #[tokio::test]
    async fn update_schedule_cron_expression_preserves_typed_refusals_and_never_falls_back() {
        for (status, code, expected) in [
            (
                "400 Bad Request",
                "invalid_schedule_cron",
                "Enter a valid Cron expression",
            ),
            (
                "409 Conflict",
                "schedule_trigger_changed",
                "Schedule is no longer an editable Cron Task schedule",
            ),
            (
                "409 Conflict",
                "schedule_no_future_occurrence",
                "Schedule has no future occurrence",
            ),
            (
                "404 Not Found",
                "schedule_not_found",
                "Schedule no longer exists",
            ),
            (
                "404 Not Found",
                "unknown_route",
                "Editing a Cron expression requires a newer Host",
            ),
            (
                "405 Method Not Allowed",
                "method_not_allowed",
                "Editing a Cron expression requires a newer Host",
            ),
            (
                "503 Service Unavailable",
                "internal_error",
                "Could not update the Cron expression (HTTP 503)",
            ),
        ] {
            let body = serde_json::json!({"error":{"code":code,"message":"RAW_DETAILS"}});
            assert_eq!(
                cron_expression_reply(CRON_EXPRESSION, status, "", &body.to_string())
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
        assert_eq!(
            cron_expression_reply(
                CRON_EXPRESSION,
                "307 Temporary Redirect",
                "Location: /api/v1/schedules/other\r\n",
                ""
            )
            .await
            .unwrap_err()
            .to_string(),
            "Could not update the Cron expression (HTTP 307)"
        );
    }

    async fn cron_expression_reply(
        input: &str,
        status: &str,
        extra: &str,
        body: &str,
    ) -> Result<Schedule> {
        use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!(
            "http://{}/gateway/",
            listener.local_addr().unwrap()
        ));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert_eq!(
                request.lines().next(),
                Some("PATCH /gateway/api/v1/schedules/schedule%2F17%20%3F%23%25/cron-expression HTTP/1.1")
            );
            assert_eq!(
                request_json(&request),
                serde_json::json!({"expr": CRON_EXPRESSION})
            );
            let response = format!(
                "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
            if status.starts_with("404") || status.starts_with("405") || status.starts_with("307") {
                assert!(timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err());
            }
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                client.update_schedule_cron_expression(TOGGLE_SCHEDULE_ID, input),
                serve
            )
        })
        .await
        .unwrap();
        result
    }

    #[tokio::test]
    async fn update_schedule_interval_sends_only_the_period_and_projects_confirmed_state() {
        let body = serde_json::json!({
            "id": TOGGLE_SCHEDULE_ID,
            "name": "Status review",
            "enabled": true,
            "trigger": {
                "type": "interval",
                "every_seconds": 120,
                "anchor_at": "2030-02-13T13:30:00Z"
            },
            "state": {"next_fire_at": "2030-02-13T13:32:00Z"},
            "run_config": {"task_message": "Review progress"}
        });
        let schedule = schedule_interval_reply("200 OK", "", &body.to_string())
            .await
            .unwrap();
        assert_eq!(schedule.id, TOGGLE_SCHEDULE_ID);
        assert_eq!(schedule.interval_seconds, Some(120));
        assert_eq!(
            schedule.cadence_detail.as_deref(),
            Some("Every 120 seconds")
        );
        assert_eq!(schedule.name.as_deref(), Some("Status review"));
        assert_eq!(schedule.enabled, Some(true));
        assert_eq!(schedule.prompt.as_deref(), Some("Review progress"));
        assert_eq!(
            schedule.next_run,
            Some("2030-02-13T13:32:00Z".parse().unwrap())
        );
    }

    #[tokio::test]
    async fn update_schedule_interval_rejects_mismatched_identity_kind_and_period() {
        for (id, trigger) in [
            (
                "other",
                serde_json::json!({"type":"interval","every_seconds":120}),
            ),
            (
                TOGGLE_SCHEDULE_ID,
                serde_json::json!({"type":"interval","every_seconds":60}),
            ),
            (TOGGLE_SCHEDULE_ID, serde_json::json!({"type":"interval"})),
            (
                TOGGLE_SCHEDULE_ID,
                serde_json::json!({"type":"daily","every_seconds":120}),
            ),
        ] {
            let body = serde_json::json!({"id": id, "trigger": trigger});
            assert_eq!(
                schedule_interval_reply("200 OK", "", &body.to_string())
                    .await
                    .unwrap_err()
                    .to_string(),
                "Schedule interval response does not match the request"
            );
        }
    }

    #[tokio::test]
    async fn update_schedule_interval_preserves_typed_refusals_and_never_falls_back() {
        for (status, code, expected) in [
            (
                "400 Bad Request",
                Some("invalid_schedule_interval"),
                "Schedule interval must be a positive representable number of seconds",
            ),
            (
                "409 Conflict",
                Some("schedule_trigger_changed"),
                "Schedule is no longer an Interval schedule",
            ),
            (
                "409 Conflict",
                Some("schedule_no_future_occurrence"),
                "Schedule has no future occurrence",
            ),
            (
                "404 Not Found",
                Some("schedule_not_found"),
                "Schedule no longer exists",
            ),
            (
                "404 Not Found",
                None,
                "Editing the interval requires a newer Host",
            ),
            (
                "404 Not Found",
                Some("not_found"),
                "Editing the interval requires a newer Host",
            ),
            (
                "405 Method Not Allowed",
                None,
                "Editing the interval requires a newer Host",
            ),
            (
                "503 Service Unavailable",
                Some("schedule_trigger_changed"),
                "Could not update schedule interval (HTTP 503)",
            ),
        ] {
            let body = code.map_or_else(
                || "RAW_DETAILS".to_owned(),
                |code| {
                    serde_json::json!({"error":{"code":code,"message":"RAW_DETAILS"}}).to_string()
                },
            );
            assert_eq!(
                schedule_interval_reply(status, "", &body)
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }

        assert_eq!(
            schedule_interval_reply(
                "307 Temporary Redirect",
                "Location: /api/v1/schedules/schedule-17\r\n",
                "",
            )
            .await
            .unwrap_err()
            .to_string(),
            "Could not update schedule interval (HTTP 307)"
        );
    }

    async fn schedule_interval_reply(status: &str, extra: &str, body: &str) -> Result<Schedule> {
        use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!(
            "http://{}/gateway/",
            listener.local_addr().unwrap()
        ));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert_eq!(
                request.lines().next(),
                Some("PATCH /gateway/api/v1/schedules/schedule%2F17%20%3F%23%25/interval HTTP/1.1")
            );
            assert_eq!(
                request_json(&request),
                serde_json::json!({"every_seconds":120})
            );
            let response = format!(
                "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                client.update_schedule_interval(TOGGLE_SCHEDULE_ID, 120),
                serve
            )
        })
        .await
        .unwrap();
        result
    }

    const SCHEDULE_TASK_PROMPT: &str =
        "  Review the weekly report 🧪\r\n\n  Follow up in 中文.  \n";

    #[tokio::test]
    async fn set_schedule_prompt_sends_only_the_exact_multiline_task_and_accepts_the_echo() {
        let body = serde_json::json!({
            "id": TOGGLE_SCHEDULE_ID,
            "name": "Weekly review",
            "enabled": false,
            "trigger": {"type":"interval","every_seconds":60},
            "state": {"next_fire_at":null},
            "run_config": {
                "task_message": SCHEDULE_TASK_PROMPT,
                "system_prompt": "Keep the report concise"
            }
        });
        assert_eq!(
            schedule_prompt_reply("200 OK", "", &body.to_string())
                .await
                .unwrap(),
            SCHEDULE_TASK_PROMPT
        );
    }

    #[tokio::test]
    async fn set_schedule_prompt_rejects_wrong_identity_and_missing_or_changed_task() {
        for (body, expected) in [
            (
                serde_json::json!({"id":"another-schedule","run_config":{"task_message":SCHEDULE_TASK_PROMPT}}),
                "Schedule task prompt response does not match the request",
            ),
            (
                serde_json::json!({"id":TOGGLE_SCHEDULE_ID,"run_config":{"task_message":SCHEDULE_TASK_PROMPT.trim()}}),
                "Schedule task prompt response does not match the request",
            ),
            (
                serde_json::json!({"schedule_id":TOGGLE_SCHEDULE_ID,"run_config":{"task_message":SCHEDULE_TASK_PROMPT}}),
                "Invalid schedule task prompt response",
            ),
            (
                serde_json::json!({"id":TOGGLE_SCHEDULE_ID,"task_message":SCHEDULE_TASK_PROMPT}),
                "Invalid schedule task prompt response",
            ),
            (
                serde_json::json!({"id":TOGGLE_SCHEDULE_ID,"run_config":{"task_message":null}}),
                "Invalid schedule task prompt response",
            ),
        ] {
            assert_eq!(
                schedule_prompt_reply("200 OK", "", &body.to_string())
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
        assert_eq!(
            BambooClient::new("unused")
                .set_schedule_prompt("schedule-17", " \r\n\t")
                .await
                .unwrap_err()
                .to_string(),
            "Enter a task prompt"
        );
    }

    #[tokio::test]
    async fn set_schedule_prompt_reports_known_refusals_without_fallback_or_response_details() {
        for (status, extra, code, expected) in [
            (
                "400 Bad Request",
                "",
                Some("invalid_schedule_prompt"),
                "Enter a task prompt",
            ),
            (
                "409 Conflict",
                "",
                Some("schedule_prompt_changed"),
                "Schedule task prompt can no longer be edited",
            ),
            (
                "404 Not Found",
                "",
                Some("schedule_not_found"),
                "Schedule no longer exists",
            ),
            (
                "404 Not Found",
                "",
                None,
                "Editing the task prompt requires a newer Host",
            ),
            (
                "405 Method Not Allowed",
                "",
                None,
                "Editing the task prompt requires a newer Host",
            ),
            (
                "503 Service Unavailable",
                "",
                Some("schedule_not_found"),
                "Could not update task prompt (HTTP 503)",
            ),
            (
                "307 Temporary Redirect",
                "Location: /gateway/api/v1/schedules/schedule-17\r\n",
                None,
                "Could not update task prompt (HTTP 307)",
            ),
        ] {
            let body = code.map_or_else(
                || "RAW_DETAILS".to_owned(),
                |code| {
                    serde_json::json!({"error":{"code":code,"message":"RAW_DETAILS"}}).to_string()
                },
            );
            assert_eq!(
                schedule_prompt_reply(status, extra, &body)
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
    }

    async fn schedule_prompt_reply(status: &str, extra: &str, body: &str) -> Result<String> {
        use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!(
            "http://{}/gateway/",
            listener.local_addr().unwrap()
        ));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert_eq!(
                request.lines().next(),
                Some("PATCH /gateway/api/v1/schedules/schedule%2F17%20%3F%23%25/prompt HTTP/1.1")
            );
            assert_eq!(
                request_json(&request),
                serde_json::json!({"task_message": SCHEDULE_TASK_PROMPT})
            );
            let response = format!(
                "HTTP/1.1 {status}\r\n{extra}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
            if matches!(
                status,
                "404 Not Found" | "405 Method Not Allowed" | "307 Temporary Redirect"
            ) {
                assert!(timeout(Duration::from_millis(120), listener.accept())
                    .await
                    .is_err());
            }
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                client.set_schedule_prompt(TOGGLE_SCHEDULE_ID, SCHEDULE_TASK_PROMPT),
                serve
            )
        })
        .await
        .unwrap();
        result
    }

    #[tokio::test]
    async fn set_schedule_enabled_patches_only_the_saved_flag_and_projects_the_reply() {
        for enabled in [true, false] {
            let body = serde_json::json!({
                "id": TOGGLE_SCHEDULE_ID,
                "name": "Daily review",
                "enabled": enabled,
                "trigger": {"type": "daily", "hour": 8, "minute": 30, "second": 0},
                "timezone": "America/New_York",
                "state": {
                    "next_fire_at": "2030-02-13T13:30:00Z",
                    "last_finished_at": "2030-02-12T13:31:00Z"
                },
                "run_config": {"task_message": "Review progress"}
            });
            let schedule = schedule_toggle_reply(enabled, "200 OK", &body.to_string())
                .await
                .unwrap();
            assert_eq!(schedule.id, TOGGLE_SCHEDULE_ID);
            assert_eq!(schedule.enabled, Some(enabled));
            assert_eq!(schedule.name.as_deref(), Some("Daily review"));
            assert_eq!(schedule.cron.as_deref(), Some("daily"));
            assert_eq!(
                schedule.cadence_detail.as_deref(),
                Some("Daily 08:30 America/New_York")
            );
            assert_eq!(schedule.prompt.as_deref(), Some("Review progress"));
            assert_eq!(
                schedule.next_run,
                Some("2030-02-13T13:30:00Z".parse().unwrap())
            );
            assert_eq!(
                schedule.last_run,
                Some("2030-02-12T13:31:00Z".parse().unwrap())
            );
        }
    }

    #[tokio::test]
    async fn set_schedule_enabled_rejects_wrong_identity_and_missing_or_wrong_flag() {
        for (body, expected) in [
            (
                r#"{"id":"DIFFERENT_ID","enabled":false}"#,
                "Schedule update response has a different schedule ID",
            ),
            (
                r#"{"id":"schedule/17 ?#%","enabled":true}"#,
                "Schedule update response has a different enabled state",
            ),
            (
                r#"{"id":"schedule/17 ?#%"}"#,
                "Invalid schedule update response",
            ),
            (
                r#"{"id":"schedule/17 ?#%","enabled":null}"#,
                "Invalid schedule update response",
            ),
            (
                r#"{"id":"schedule/17 ?#%","enabled":"false"}"#,
                "Invalid schedule update response",
            ),
            (
                r#"{"id":"schedule/17 ?#%","enabled":true,"enabled":false}"#,
                "Invalid schedule update response",
            ),
        ] {
            assert_eq!(
                schedule_toggle_reply(false, "200 OK", body)
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn set_schedule_enabled_reports_only_the_known_conflict_and_sanitized_errors() {
        let oversized = serde_json::json!({
            "error": {
                "code": "schedule_no_future_occurrence",
                "message": "RAW_BODY".repeat(1_024),
            }
        })
        .to_string();
        for (status, body, expected) in [
            (
                "409 Conflict",
                r#"{"error":{"code":"schedule_no_future_occurrence","message":"RAW_BODY"}}"#,
                "Schedule has no future occurrence",
            ),
            (
                "409 Conflict",
                r#"{"error":{"code":"RAW_ERROR_CODE","message":"RAW_BODY"}}"#,
                "Could not update schedule (HTTP 409)",
            ),
            (
                "409 Conflict",
                r#"{"code":"schedule_no_future_occurrence","message":"RAW_BODY"}"#,
                "Could not update schedule (HTTP 409)",
            ),
            (
                "409 Conflict",
                oversized.as_str(),
                "Could not update schedule (HTTP 409)",
            ),
            (
                "503 Service Unavailable",
                r#"{"error":{"code":"schedule_no_future_occurrence","message":"RAW_BODY"}}"#,
                "Could not update schedule (HTTP 503)",
            ),
            (
                "201 Created",
                r#"{"id":"schedule/17 ?#%","enabled":true}"#,
                "Could not update schedule (HTTP 201)",
            ),
        ] {
            assert_eq!(
                schedule_toggle_reply(true, status, body)
                    .await
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn set_schedule_enabled_times_out_while_the_host_holds_the_response_body() {
        use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
        let (release, hold_response) = oneshot::channel();
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert_eq!(
                request.lines().next(),
                Some("PATCH /api/v1/schedules/schedule-17 HTTP/1.1")
            );
            assert_eq!(
                request_json(&request),
                serde_json::json!({ "enabled": false })
            );
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 40\r\n\r\n{",
                )
                .await
                .unwrap();
            // Hold the partial body until the caller returns.
            hold_response.await.unwrap();
        };
        let request = async {
            let result = client
                .set_schedule_enabled_with_timeout("schedule-17", false, Duration::from_secs(1))
                .await;
            release.send(()).unwrap();
            result
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(request, serve)
        })
        .await
        .unwrap();
        assert_eq!(
            result.unwrap_err().to_string(),
            "Timed out waiting for the schedule update response"
        );
    }

    async fn schedule_toggle_reply(enabled: bool, status: &str, body: &str) -> Result<Schedule> {
        use std::time::Duration;
        use tokio::{net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!(
            "http://{}/gateway/",
            listener.local_addr().unwrap()
        ));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert_eq!(
                request.lines().next(),
                Some("PATCH /gateway/api/v1/schedules/schedule%2F17%20%3F%23%25 HTTP/1.1")
            );
            assert_eq!(
                request_json(&request),
                serde_json::json!({ "enabled": enabled })
            );
            respond(&mut socket, status, "1", body).await;
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(
                client.set_schedule_enabled(TOGGLE_SCHEDULE_ID, enabled),
                serve
            )
        })
        .await
        .unwrap();
        result
    }

    #[tokio::test]
    async fn cancel_schedule_run_posts_exact_ids_and_reads_requested_or_terminal() {
        let requested = schedule_cancel_reply(
            "202 Accepted",
            r#"{"schedule_id":"schedule-17","run_id":"run-42","status":"cancel_requested"}"#,
        )
        .await
        .unwrap();
        assert_eq!(requested, CancelScheduleRunOutcome::Requested);

        for status in ["success", "failed", "skipped", "missed", "cancelled"] {
            let completed = schedule_cancel_reply(
                "200 OK",
                &serde_json::json!({
                    "schedule_id": "schedule-17",
                    "run_id": "run-42",
                    "scheduled_for": "2030-02-13T09:00:00Z",
                    "status": status,
                    "session_id": "session-18",
                    "outcome_reason": (status == "cancelled").then(|| "x".repeat(64 * 1024 + 1))
                })
                .to_string(),
            )
            .await
            .unwrap();
            assert_eq!(
                completed,
                CancelScheduleRunOutcome::Terminal(ScheduleRunView {
                    run_id: "run-42".into(),
                    scheduled_for: "2030-02-13T09:00:00Z".parse().unwrap(),
                    status: status.into(),
                    session_id: Some("session-18".into()),
                    workflow_run_id: None,
                })
            );
        }
    }

    #[tokio::test]
    async fn cancel_schedule_run_requires_matching_identity_and_expected_state() {
        for (status, schedule_id, run_id, run_status, error) in [
            (
                "202 Accepted",
                "other",
                "run-42",
                "cancel_requested",
                "Schedule cancellation response has a different schedule or run ID",
            ),
            (
                "202 Accepted",
                "schedule-17",
                "other",
                "cancel_requested",
                "Schedule cancellation response has a different schedule or run ID",
            ),
            (
                "202 Accepted",
                "schedule-17",
                "run-42",
                "cancelled",
                "Invalid schedule cancellation acknowledgment",
            ),
            (
                "200 OK",
                "other",
                "run-42",
                "cancelled",
                "Schedule cancellation response has a different schedule or run ID",
            ),
            (
                "200 OK",
                "schedule-17",
                "other",
                "cancelled",
                "Schedule cancellation response has a different schedule or run ID",
            ),
            (
                "200 OK",
                "schedule-17",
                "run-42",
                "running",
                "Schedule cancellation response is not a terminal result",
            ),
            (
                "200 OK",
                "schedule-17",
                "run-42",
                "future_status",
                "Schedule cancellation response is not a terminal result",
            ),
        ] {
            let body = serde_json::json!({
                "schedule_id": schedule_id,
                "run_id": run_id,
                "status": run_status,
                "scheduled_for": "2030-02-13T09:00:00Z"
            });
            assert_eq!(
                schedule_cancel_reply(status, &body.to_string())
                    .await
                    .unwrap_err()
                    .to_string(),
                error
            );
        }
    }

    #[tokio::test]
    async fn cancel_schedule_run_errors_are_bounded_and_do_not_display_response_body() {
        for (status, body, error) in [
            (
                "404 Not Found",
                r#"{"error":{"code":"schedule_run_not_found","message":"RAW_BODY"}}"#,
                "Schedule run not found",
            ),
            (
                "404 Not Found",
                "RAW_BODY",
                "This Host does not support schedule-run cancellation",
            ),
            (
                "409 Conflict",
                r#"{"error":{"code":"schedule_run_not_cancellable","message":"RAW_BODY"}}"#,
                "Schedule run has no cancellable live execution",
            ),
            (
                "409 Conflict",
                r#"{"error":{"code":"future_error","message":"RAW_BODY"}}"#,
                "Could not cancel schedule run (HTTP 409)",
            ),
            (
                "503 Service Unavailable",
                "RAW_BODY",
                "Could not cancel schedule run (HTTP 503)",
            ),
            (
                "202 Accepted",
                "RAW_BODY",
                "Invalid schedule cancellation response",
            ),
            (
                "200 OK",
                r#"{"schedule_id":"schedule-17","run_id":"run-42","status":"cancelled"}"#,
                "Invalid schedule cancellation response",
            ),
        ] {
            assert_eq!(
                schedule_cancel_reply(status, body)
                    .await
                    .unwrap_err()
                    .to_string(),
                error
            );
        }
    }

    async fn schedule_cancel_reply(status: &str, body: &str) -> Result<CancelScheduleRunOutcome> {
        use std::time::Duration;
        use tokio::{net::TcpListener, time::timeout};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BambooClient::new(&format!("http://{}", listener.local_addr().unwrap()));
        let serve = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            let (head, body_sent) = request.split_once("\r\n\r\n").unwrap();
            assert_eq!(
                head.lines().next(),
                Some("POST /api/v1/schedules/schedule-17/runs/run-42/cancel HTTP/1.1")
            );
            assert!(body_sent.is_empty());
            respond(&mut socket, status, "1", body).await;
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(client.cancel_schedule_run("schedule-17", "run-42"), serve)
        })
        .await
        .unwrap();
        result
    }

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
