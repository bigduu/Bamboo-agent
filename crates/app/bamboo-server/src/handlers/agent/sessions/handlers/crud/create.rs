use std::time::Instant;

use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse, Result};
use chrono::Utc;
use uuid::Uuid;

use crate::app_state::session_create_operations::{
    correlation_id, key_digest, payload_fingerprint, validate_key, SessionCreateOperationRecord,
    SessionCreateOperationStore, StoredOperationError, StoredOperationStatus,
};
use crate::app_state::AppState;
use bamboo_agent_core::Session;
use bamboo_engine::model_config_helper::normalize_gold_config_json;

use super::super::super::types::{
    CreateSessionRequest, CreateSessionResponse, SessionCreateOperationError,
    SessionCreateOperationResponse, SessionCreateOperationStatus, SessionSummary,
};

const IDEMPOTENCY_KEY_HEADER: &str = "Idempotency-Key";

/// Detects the handler future being dropped before it constructs a response.
/// That commonly means a client disconnect/abort, but can also mean server
/// cancellation; observability deliberately reports the bounded fact instead
/// of claiming that a response was or was not delivered on the wire.
struct SessionCreateTraceGuard {
    correlation_id: String,
    started: Instant,
    response_constructed: bool,
}

impl SessionCreateTraceGuard {
    fn new(correlation_id: String, started: Instant) -> Self {
        Self {
            correlation_id,
            started,
            response_constructed: false,
        }
    }

    fn response_constructed(&mut self, result: &Result<HttpResponse>) {
        match result {
            Ok(response) => tracing::info!(
                target: "bamboo.session_create",
                correlation_id = %self.correlation_id,
                phase = "response_constructed",
                outcome = "http_response",
                status = response.status().as_u16(),
                elapsed_ms = self.started.elapsed().as_millis() as u64,
                "session-create handler constructed a response"
            ),
            Err(_) => tracing::info!(
                target: "bamboo.session_create",
                correlation_id = %self.correlation_id,
                phase = "response_constructed",
                outcome = "handler_error",
                elapsed_ms = self.started.elapsed().as_millis() as u64,
                "session-create handler constructed an error result"
            ),
        }
        self.response_constructed = true;
    }
}

impl Drop for SessionCreateTraceGuard {
    fn drop(&mut self) {
        if !self.response_constructed {
            tracing::info!(
                target: "bamboo.session_create",
                correlation_id = %self.correlation_id,
                phase = "handler_dropped",
                outcome = "cancelled_or_disconnected",
                elapsed_ms = self.started.elapsed().as_millis() as u64,
                "session-create handler future dropped before response construction"
            );
        }
    }
}

/// Sync runtime workspace so tools can resolve the working directory. #480
/// gives `POST /sessions` the same `workspace_path` semantics as `POST /chat`;
/// this create path additionally uses its AppState-scoped provider pair so
/// preview and post-persistence materialization cannot cross test states.
fn sync_runtime_workspace(
    state: &AppState,
    session_id: &str,
    workspace_path: Option<&str>,
    workspace_source: &str,
    redact_paths: bool,
) {
    if let Some(workspace) = workspace_path
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .and_then(|path| std::fs::canonicalize(&path).ok().or(Some(path)))
    {
        let workspace_root = state
            .workspace_resolver
            .workspace_root_config()
            .map(|config| config.root);
        let published = state.workspace_resolver.publish_resolved_workspace(
            session_id,
            workspace,
            workspace_source,
        );
        if !published.is_dir() {
            if redact_paths {
                tracing::warn!(
                    target: "bamboo.session_create",
                    session_id,
                    phase = "workspace_projection",
                    outcome = "unusable_directory",
                    workspace_root_registered = workspace_root.is_some(),
                    "idempotent session-create workspace projection was unusable"
                );
            } else {
                let workspace_root_display = workspace_root
                    .as_deref()
                    .map(bamboo_config::paths::path_to_display_string)
                    .unwrap_or_else(|| "<unregistered>".to_string());
                tracing::warn!(
                    session_id,
                    path = %published.display(),
                    workspace_root = %workspace_root_display,
                    workspace_source,
                    "create-session workspace publication did not produce a usable directory"
                );
            }
        }
    }
}

/// `POST /api/v1/sessions`
pub async fn create_session(
    state: web::Data<AppState>,
    http_request: HttpRequest,
    req: web::Json<CreateSessionRequest>,
) -> Result<HttpResponse> {
    let Some(raw_key) = extract_idempotency_key(&http_request)? else {
        return create_session_once(state.get_ref(), &req, Uuid::new_v4().to_string(), None).await;
    };
    let key_digest = key_digest(&raw_key);
    // Drop the caller-controlled value before any durable operation or trace.
    drop(raw_key);
    let correlation_id = correlation_id(&key_digest).to_string();
    let fingerprint = payload_fingerprint(&*req).map_err(|_| {
        crate::error::json_internal_server_error("Failed to fingerprint session-create request")
    })?;
    let started = Instant::now();
    tracing::info!(
        target: "bamboo.session_create",
        correlation_id,
        phase = "accepted",
        "idempotent session create accepted"
    );

    let mut request_trace = SessionCreateTraceGuard::new(correlation_id.clone(), started);
    // The idempotent state machine owns durable recovery truth and must outlive
    // the request/connection future. Dropping this JoinHandle detaches the
    // spawned task; it does not cancel the create after the client disconnects.
    let core = actix_web::rt::spawn(create_session_idempotent_core(
        state,
        req.into_inner(),
        key_digest,
        fingerprint,
        correlation_id,
        started,
    ));
    let result = match core.await {
        Ok(result) => result,
        Err(error) => Err(crate::error::json_internal_server_error(format!(
            "Detached session-create task failed: {error}"
        ))),
    };
    request_trace.response_constructed(&result);
    result
}

async fn create_session_idempotent_core(
    state: web::Data<AppState>,
    req: CreateSessionRequest,
    key_digest: String,
    fingerprint: String,
    correlation_id: String,
    started: Instant,
) -> Result<HttpResponse> {
    let store = state.session_create_operations.clone();
    let lock_started = Instant::now();
    let _guard = store.lock(&key_digest).await.map_err(|_| {
        crate::error::json_internal_server_error("Failed to lock session-create operation")
    })?;
    tracing::info!(
        target: "bamboo.session_create",
        correlation_id,
        phase = "lock_acquired",
        outcome = "exclusive",
        lock_wait_ms = lock_started.elapsed().as_millis() as u64,
        "session-create operation claim acquired"
    );
    let existing = store.load_claimed(&key_digest).await.map_err(|_| {
        crate::error::json_internal_server_error("Failed to load session-create operation")
    })?;

    let (mut operation, is_new) = match existing {
        Some(operation) => {
            if operation.payload_fingerprint != fingerprint {
                tracing::info!(
                    target: "bamboo.session_create",
                    correlation_id,
                    phase = "conflict",
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    outcome = "payload_conflict",
                    "idempotent session create rejected"
                );
                return Ok(idempotency_conflict_response());
            }
            (operation, false)
        }
        None => {
            let operation = SessionCreateOperationRecord::pending(
                key_digest.clone(),
                fingerprint,
                Uuid::new_v4().to_string(),
            );
            store.save(&operation).await.map_err(|_| {
                crate::error::json_internal_server_error(
                    "Failed to reserve session-create operation",
                )
            })?;
            tracing::info!(
                target: "bamboo.session_create",
                correlation_id,
                phase = "reserved",
                outcome = "new",
                "idempotent session UUID reserved durably"
            );
            (operation, true)
        }
    };

    if operation.status == StoredOperationStatus::Failed {
        tracing::info!(
            target: "bamboo.session_create",
            correlation_id,
            phase = "replayed",
            elapsed_ms = started.elapsed().as_millis() as u64,
            outcome = "failed",
            "terminal session-create failure replayed"
        );
        return Ok(stored_failure_response(operation.error.as_ref()));
    }

    // A succeeded receipt has already crossed every projection barrier.
    // Replay through the canonical no-regression repository only; running
    // sessions must never have their live cache Arc replaced by recovery.
    if operation.status == StoredOperationStatus::Succeeded {
        if let Some(session) =
            load_succeeded_session_summary(state.get_ref(), &operation.session_id)
                .await
                .map_err(|_| {
                    crate::error::json_internal_server_error(
                        "Failed to load completed session-create result",
                    )
                })?
        {
            tracing::info!(
                target: "bamboo.session_create",
                correlation_id,
                phase = "replayed",
                elapsed_ms = started.elapsed().as_millis() as u64,
                outcome = "succeeded",
                "completed idempotent session create replayed"
            );
            return Ok(HttpResponse::Ok().json(CreateSessionResponse { session }));
        }
        operation.mark_failed(result_gone_error());
        store.save(&operation).await.map_err(|_| {
            crate::error::json_internal_server_error(
                "Failed to finalize missing session-create result",
            )
        })?;
        tracing::info!(
            target: "bamboo.session_create",
            correlation_id,
            phase = "replayed",
            elapsed_ms = started.elapsed().as_millis() as u64,
            outcome = "gone",
            "idempotent session-create target is no longer available"
        );
        return Ok(stored_failure_response(operation.error.as_ref()));
    }

    // Pending recovery is allowed only while this same-key exclusive claim
    // is held. Repair a missing rebuildable index first, then reconcile via
    // SessionRepository so an active cache can never regress.
    match recover_pending_committed_session(state.get_ref(), &operation.session_id).await {
        Ok(Some(session)) => {
            operation.mark_succeeded();
            store.save(&operation).await.map_err(|_| {
                crate::error::json_internal_server_error(
                    "Failed to finalize session-create operation",
                )
            })?;
            tracing::info!(
                target: "bamboo.session_create",
                correlation_id,
                phase = "recovered",
                elapsed_ms = started.elapsed().as_millis() as u64,
                outcome = "committed_pending",
                "committed pending session create recovered"
            );
            return Ok(HttpResponse::Ok().json(CreateSessionResponse { session }));
        }
        Ok(None) => {}
        Err(_) => {
            // Keep pending truth intact; a later retry can repair the
            // authoritative index or projections when dependencies recover.
            return Err(crate::error::json_internal_server_error(
                "Failed to recover committed session-create operation",
            ));
        }
    }

    debug_assert_eq!(operation.status, StoredOperationStatus::Pending);
    let attempt = create_session_once(
        state.get_ref(),
        &req,
        operation.session_id.clone(),
        Some(&correlation_id),
    )
    .await;

    match attempt {
        Ok(response) if response.status().is_success() => {
            operation.mark_succeeded();
            store.save(&operation).await.map_err(|_| {
                crate::error::json_internal_server_error(
                    "Failed to finalize session-create operation",
                )
            })?;
            tracing::info!(
                target: "bamboo.session_create",
                correlation_id,
                phase = "completed",
                elapsed_ms = started.elapsed().as_millis() as u64,
                outcome = if is_new { "created" } else { "retried" },
                "idempotent session create completed"
            );
            Ok(response)
        }
        Ok(response) => {
            match recover_pending_committed_session(state.get_ref(), &operation.session_id).await {
                Ok(Some(session)) => {
                    operation.mark_succeeded();
                    store.save(&operation).await.map_err(|_| {
                        crate::error::json_internal_server_error(
                            "Failed to finalize recovered session-create operation",
                        )
                    })?;
                    tracing::info!(
                        target: "bamboo.session_create",
                        correlation_id,
                        phase = "recovered",
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        outcome = "succeeded_after_ambiguous_commit",
                        "ambiguous session create recovered after durable commit"
                    );
                    Ok(HttpResponse::Created().json(CreateSessionResponse { session }))
                }
                Ok(None) => {
                    operation.mark_failed(safe_error_for_status(response.status()));
                    store.save(&operation).await.map_err(|_| {
                        crate::error::json_internal_server_error(
                            "Failed to finalize failed session-create operation",
                        )
                    })?;
                    Ok(response)
                }
                Err(_) => Err(crate::error::json_internal_server_error(
                    "Failed to reconcile ambiguous session-create operation",
                )),
            }
        }
        Err(error) => {
            match recover_pending_committed_session(state.get_ref(), &operation.session_id).await {
                Ok(Some(session)) => {
                    operation.mark_succeeded();
                    store.save(&operation).await.map_err(|_| {
                        crate::error::json_internal_server_error(
                            "Failed to finalize recovered session-create operation",
                        )
                    })?;
                    tracing::info!(
                        target: "bamboo.session_create",
                        correlation_id,
                        phase = "recovered",
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        outcome = "succeeded_after_error",
                        "session create recovered after post-commit error"
                    );
                    Ok(HttpResponse::Created().json(CreateSessionResponse { session }))
                }
                Ok(None) => {
                    operation.mark_failed(safe_error_for_status(StatusCode::INTERNAL_SERVER_ERROR));
                    store.save(&operation).await.map_err(|_| {
                        crate::error::json_internal_server_error(
                            "Failed to finalize failed session-create operation",
                        )
                    })?;
                    Err(error)
                }
                Err(_) => Err(crate::error::json_internal_server_error(
                    "Failed to reconcile ambiguous session-create operation",
                )),
            }
        }
    }
}

/// `GET /api/v1/session-create-operations/{key}`
pub async fn get_session_create_operation(
    state: web::Data<AppState>,
    key: web::Path<String>,
) -> Result<HttpResponse> {
    if let Err(message) = validate_key(key.as_str()) {
        return Ok(invalid_idempotency_key_response(message));
    }
    let key_digest = key_digest(key.as_str());
    let correlation_id = correlation_id(&key_digest).to_string();
    let started = Instant::now();
    let store = state.session_create_operations.clone();
    let Some(operation) = store.load_for_status(&key_digest).await.map_err(|_| {
        crate::error::json_internal_server_error("Failed to load session-create operation")
    })?
    else {
        return Ok(status_http_response(
            &correlation_id,
            started,
            "unknown",
            unknown_operation_status_response(),
        ));
    };

    if operation.is_expired(Utc::now()) {
        return Ok(status_http_response(
            &correlation_id,
            started,
            "expired",
            expired_operation_status_response(),
        ));
    }

    match operation.status {
        StoredOperationStatus::Failed => Ok(status_http_response(
            &correlation_id,
            started,
            "failed",
            operation_status_response(&operation, None),
        )),
        StoredOperationStatus::Succeeded => {
            if let Some(session) =
                load_succeeded_session_summary(state.get_ref(), &operation.session_id)
                    .await
                    .map_err(|_| {
                        crate::error::json_internal_server_error(
                            "Failed to load completed session-create result",
                        )
                    })?
            {
                return Ok(status_http_response(
                    &correlation_id,
                    started,
                    "succeeded",
                    operation_status_response(&operation, Some(session)),
                ));
            }
            // A missing succeeded target needs a claim before terminalizing the
            // receipt. If another claimant is active, report the safe observed
            // gone result without racing its re-read/update.
            let Some(_claim) = store.try_lock(&key_digest).await.map_err(|_| {
                crate::error::json_internal_server_error(
                    "Failed to try-lock session-create operation",
                )
            })?
            else {
                return Ok(status_http_response(
                    &correlation_id,
                    started,
                    "gone",
                    gone_operation_status_response(),
                ));
            };
            status_under_claim(
                state.get_ref(),
                store.as_ref(),
                &key_digest,
                &correlation_id,
                started,
            )
            .await
        }
        StoredOperationStatus::Pending => {
            // Never wait for the active POST's exclusive claim. Busy means the
            // detached core still owns recovery/projection finalization, so the
            // durable pending snapshot is immediately observable.
            let Some(_claim) = store.try_lock(&key_digest).await.map_err(|_| {
                crate::error::json_internal_server_error(
                    "Failed to try-lock session-create operation",
                )
            })?
            else {
                return Ok(status_http_response(
                    &correlation_id,
                    started,
                    "pending_claimed",
                    operation_status_response(&operation, None),
                ));
            };
            status_under_claim(
                state.get_ref(),
                store.as_ref(),
                &key_digest,
                &correlation_id,
                started,
            )
            .await
        }
    }
}

async fn status_under_claim(
    state: &AppState,
    store: &SessionCreateOperationStore,
    key_digest: &str,
    correlation_id: &str,
    started: Instant,
) -> Result<HttpResponse> {
    // The receipt may have changed while try_lock raced the detached POST.
    let Some(mut operation) = store.load_for_status(key_digest).await.map_err(|_| {
        crate::error::json_internal_server_error("Failed to reload session-create operation")
    })?
    else {
        return Ok(status_http_response(
            correlation_id,
            started,
            "unknown",
            unknown_operation_status_response(),
        ));
    };
    if operation.is_expired(Utc::now()) {
        return Ok(status_http_response(
            correlation_id,
            started,
            "expired",
            expired_operation_status_response(),
        ));
    }

    match operation.status {
        StoredOperationStatus::Failed => Ok(status_http_response(
            correlation_id,
            started,
            "failed",
            operation_status_response(&operation, None),
        )),
        StoredOperationStatus::Succeeded => {
            if let Some(session) = load_succeeded_session_summary(state, &operation.session_id)
                .await
                .map_err(|_| {
                    crate::error::json_internal_server_error(
                        "Failed to load completed session-create result",
                    )
                })?
            {
                return Ok(status_http_response(
                    correlation_id,
                    started,
                    "succeeded",
                    operation_status_response(&operation, Some(session)),
                ));
            }
            operation.mark_failed(result_gone_error());
            store.save(&operation).await.map_err(|_| {
                crate::error::json_internal_server_error(
                    "Failed to finalize missing session-create result",
                )
            })?;
            Ok(status_http_response(
                correlation_id,
                started,
                "gone",
                operation_status_response(&operation, None),
            ))
        }
        StoredOperationStatus::Pending => {
            match recover_pending_committed_session(state, &operation.session_id).await {
                Ok(Some(session)) => {
                    operation.mark_succeeded();
                    store.save(&operation).await.map_err(|_| {
                        crate::error::json_internal_server_error(
                            "Failed to finalize recovered session-create operation",
                        )
                    })?;
                    Ok(status_http_response(
                        correlation_id,
                        started,
                        "succeeded",
                        operation_status_response(&operation, Some(session)),
                    ))
                }
                Ok(None) => Ok(status_http_response(
                    correlation_id,
                    started,
                    "pending",
                    operation_status_response(&operation, None),
                )),
                Err(_) => Err(crate::error::json_internal_server_error(
                    "Failed to recover committed session-create operation",
                )),
            }
        }
    }
}

fn extract_idempotency_key(request: &HttpRequest) -> Result<Option<String>> {
    let Some(value) = request.headers().get(IDEMPOTENCY_KEY_HEADER) else {
        return Ok(None);
    };
    let raw = value.to_str().map_err(|_| {
        actix_web::error::InternalError::from_response(
            "invalid Idempotency-Key header encoding",
            invalid_idempotency_key_response(
                "Idempotency-Key must contain only visible ASCII characters",
            ),
        )
    })?;
    validate_key(raw).map_err(|message| {
        actix_web::error::InternalError::from_response(
            message,
            invalid_idempotency_key_response(message),
        )
    })?;
    Ok(Some(raw.to_string()))
}

fn invalid_idempotency_key_response(message: &str) -> HttpResponse {
    HttpResponse::BadRequest().json(serde_json::json!({
        "error": {
            "type": "api_error",
            "code": "invalid_idempotency_key",
            "message": message,
        }
    }))
}

fn idempotency_conflict_response() -> HttpResponse {
    HttpResponse::Conflict().json(serde_json::json!({
        "error": {
            "type": "api_error",
            "code": "idempotency_key_conflict",
            "message": "Idempotency-Key was already used with a different session-create payload",
        }
    }))
}

fn safe_error_for_status(status: StatusCode) -> StoredOperationError {
    let (code, message) = match status {
        StatusCode::BAD_REQUEST => (
            "session_create_invalid",
            "The session-create request was invalid",
        ),
        StatusCode::NOT_FOUND => (
            "session_create_target_not_found",
            "A referenced session-create target was not found",
        ),
        StatusCode::CONFLICT => (
            "session_create_conflict",
            "The session-create request conflicted with current state",
        ),
        _ => (
            "session_create_failed",
            "The session-create operation failed before committing a session",
        ),
    };
    StoredOperationError {
        code: code.to_string(),
        message: message.to_string(),
        http_status: status.as_u16(),
    }
}

fn result_gone_error() -> StoredOperationError {
    StoredOperationError {
        code: "session_result_gone".to_string(),
        message: "The session was created but is no longer available".to_string(),
        http_status: StatusCode::GONE.as_u16(),
    }
}

fn stored_failure_response(error: Option<&StoredOperationError>) -> HttpResponse {
    let fallback = safe_error_for_status(StatusCode::INTERNAL_SERVER_ERROR);
    let error = error.unwrap_or(&fallback);
    let status =
        StatusCode::from_u16(error.http_status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    HttpResponse::build(status).json(serde_json::json!({
        "error": {
            "type": "api_error",
            "code": error.code,
            "message": error.message,
        }
    }))
}

fn operation_status_response(
    operation: &SessionCreateOperationRecord,
    session: Option<SessionSummary>,
) -> SessionCreateOperationResponse {
    let status = match operation.status {
        StoredOperationStatus::Pending => SessionCreateOperationStatus::Pending,
        StoredOperationStatus::Succeeded => SessionCreateOperationStatus::Succeeded,
        StoredOperationStatus::Failed => SessionCreateOperationStatus::Failed,
    };
    SessionCreateOperationResponse {
        status,
        session,
        error: operation
            .error
            .as_ref()
            .map(|error| SessionCreateOperationError {
                code: error.code.clone(),
                message: error.message.clone(),
            }),
    }
}

fn unknown_operation_status_response() -> SessionCreateOperationResponse {
    SessionCreateOperationResponse {
        status: SessionCreateOperationStatus::Unknown,
        session: None,
        error: None,
    }
}

fn expired_operation_status_response() -> SessionCreateOperationResponse {
    SessionCreateOperationResponse {
        status: SessionCreateOperationStatus::Expired,
        session: None,
        error: Some(SessionCreateOperationError {
            code: "idempotency_key_expired".to_string(),
            message: "The idempotency-key recovery window has expired".to_string(),
        }),
    }
}

fn gone_operation_status_response() -> SessionCreateOperationResponse {
    let error = result_gone_error();
    SessionCreateOperationResponse {
        status: SessionCreateOperationStatus::Failed,
        session: None,
        error: Some(SessionCreateOperationError {
            code: error.code,
            message: error.message,
        }),
    }
}

fn status_http_response(
    correlation_id: &str,
    started: Instant,
    outcome: &'static str,
    response: SessionCreateOperationResponse,
) -> HttpResponse {
    tracing::info!(
        target: "bamboo.session_create",
        correlation_id,
        phase = "status_lookup",
        outcome,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "session-create operation status recovered"
    );
    HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .json(response)
}

fn session_created_event(session: &Session) -> bamboo_agent_core::AgentEvent {
    bamboo_agent_core::AgentEvent::SessionCreated {
        session_id: session.id.clone(),
        project_id: session.project_id_meta(),
        title: session.title.clone(),
        kind: session.kind,
        created_at: session.created_at,
    }
}

/// Load an already-terminal success through the canonical no-regression
/// repository. Success replay performs no workspace/account projection and no
/// direct cache replacement: those barriers precede the succeeded receipt.
async fn load_succeeded_session_summary(
    state: &AppState,
    session_id: &str,
) -> std::io::Result<Option<SessionSummary>> {
    // A succeeded receipt still distinguishes true deletion from a corrupt or
    // stale rebuildable index by strictly reading the authoritative root file.
    // The storage recovery repairs/rebases only the index; cache reconciliation
    // remains SessionRepository's no-regression responsibility below.
    if state
        .session_store
        .recover_root_session_from_disk(session_id)
        .await?
        .is_none()
    {
        return Ok(None);
    }
    if state.session_repo.load_merged(session_id).await.is_none() {
        return Err(std::io::Error::other(
            "authoritative session disappeared during succeeded-result reconciliation",
        ));
    }
    state
        .session_store
        .get_index_entry(session_id)
        .await
        .map(|entry| SessionSummary::from_entry(entry, false))
        .map(Some)
        .ok_or_else(|| {
            std::io::Error::other(
                "authoritative session missing from repaired index during succeeded replay",
            )
        })
}

/// Pending-only recovery. The caller must own the operation's exclusive claim.
/// Strictly probe the authoritative file even when the rebuildable index has an
/// entry. When the probe succeeds, every later reconciliation miss is an
/// internal error rather than evidence that the reserved UUID was never
/// committed. Use SessionRepository's no-regression merge before publishing the
/// remaining workspace/account projections.
async fn recover_pending_committed_session(
    state: &AppState,
    session_id: &str,
) -> std::io::Result<Option<SessionSummary>> {
    let Some(_authoritative) = state
        .session_store
        .probe_root_session_from_disk(session_id)
        .await?
    else {
        return Ok(None);
    };
    if state
        .session_store
        .recover_root_session_from_disk(session_id)
        .await?
        .is_none()
    {
        return Err(std::io::Error::other(
            "authoritative session disappeared during pending index reconciliation",
        ));
    }
    let Some(session) = state.session_repo.load_merged(session_id).await else {
        return Err(std::io::Error::other(
            "authoritative session disappeared during pending-result reconciliation",
        ));
    };
    let workspace_source = session
        .metadata
        .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
        .map(String::as_str)
        .unwrap_or("session_create_recovery");
    sync_runtime_workspace(
        state,
        session_id,
        session.workspace_path_meta().as_deref(),
        workspace_source,
        true,
    );
    if !state
        .account_sink
        .record_confirmed(Some(session_id), &session_created_event(&session))
        .await
    {
        return Err(std::io::Error::other(
            "failed to confirm SessionCreated projection",
        ));
    }
    let entry = state
        .session_store
        .get_index_entry(session_id)
        .await
        .ok_or_else(|| std::io::Error::other("recovered session missing from repaired index"))?;
    Ok(Some(SessionSummary::from_entry(entry, false)))
}

async fn create_session_once(
    state: &AppState,
    req: &CreateSessionRequest,
    id: String,
    correlation_id: Option<&str>,
) -> Result<HttpResponse> {
    if let Some(project_id) = req.project_id.as_ref() {
        match state.project_store.get(project_id) {
            Ok(project) if project.status == bamboo_domain::ProjectStatus::Active => {}
            Ok(_) => {
                return Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": "project_archived",
                        "message": "Sessions can only be created in an active Project"
                    },
                    "project_id": project_id,
                })));
            }
            Err(bamboo_projects::ProjectStoreError::NotFound(_)) => {
                return Ok(crate::error::json_error(
                    actix_web::http::StatusCode::NOT_FOUND,
                    "target Project not found",
                ));
            }
            Err(error) => {
                return Err(crate::error::json_internal_server_error(format!(
                    "Failed to validate target Project: {error}"
                )));
            }
        }
    }
    let final_workspace = match crate::project_context::validate_workspace_assignment_with_resolver(
        &state.project_store,
        req.project_id.as_ref(),
        req.workspace_path.as_deref(),
        &state.workspace_resolver,
    ) {
        Ok(workspace) => workspace,
        Err(error) => {
            return match error {
                crate::project_context::ProjectWorkspaceValidationError::Invalid {
                    code,
                    workspace,
                    message,
                } => {
                    let project_path_error = code.starts_with("project_path_");
                    let mut response = if project_path_error {
                        HttpResponse::Conflict()
                    } else {
                        HttpResponse::BadRequest()
                    };
                    let mut body = serde_json::json!({
                        "error": {
                            "type": "api_error",
                            "code": code,
                            "message": message
                        },
                        "workspace": workspace,
                    });
                    if project_path_error {
                        body["project_id"] = serde_json::json!(req.project_id);
                        body["project_path"] = body["workspace"].clone();
                    }
                    Ok(response.json(body))
                }
                crate::project_context::ProjectWorkspaceValidationError::Conflict {
                    workspace,
                    owner_project_id,
                    session_project_id,
                } => Ok(HttpResponse::Conflict().json(serde_json::json!({
                    "error": {
                        "type": "api_error",
                        "code": "project_workspace_conflict",
                        "message": "Workspace belongs to another Project"
                    },
                    "workspace": workspace,
                    "owner_project_id": owner_project_id,
                    "session_project_id": session_project_id,
                }))),
                crate::project_context::ProjectWorkspaceValidationError::Store(error) => {
                    Err(crate::error::json_internal_server_error(format!(
                        "Failed to validate workspace Project ownership: {error}"
                    )))
                }
            };
        }
    };
    let final_workspace_display = final_workspace
        .as_deref()
        .map(bamboo_config::paths::path_to_display_string);
    let global_default_prompt =
        bamboo_engine::prompt_defaults::read_global_default_system_prompt_template();
    let config_snapshot = state.config.read().await.clone();
    let gold_config_json = match req
        .gold_config
        .as_ref()
        .map(normalize_gold_config_json)
        .transpose()
    {
        Ok(value) => value,
        Err(error) => {
            // Canonical nested error envelope (#251 finding 2); `message` is kept
            // as a top-level sibling field too since existing callers already
            // read the detail from there, not from `error.message`.
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": crate::error::error_value("Invalid gold_config"),
                "message": error.to_string()
            })));
        }
    };

    let mut session = build_new_session(
        &id,
        req,
        gold_config_json,
        global_default_prompt.as_str(),
        &config_snapshot,
    );
    let configured_default_workspace = config_snapshot.get_default_work_area_path();
    let workspace_source = if let Some(workspace) = final_workspace_display.as_deref() {
        session.set_workspace_path_meta(workspace);
        let source = if req
            .workspace_path
            .as_deref()
            .is_some_and(|path| !path.trim().is_empty())
        {
            bamboo_engine::project_context::WorkspaceSource::Explicit
        } else if req.project_id.is_some() {
            bamboo_engine::project_context::WorkspaceSource::ProjectDefault
        } else {
            bamboo_engine::project_context::WorkspaceSource::Session
        };
        session.metadata.insert(
            bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY.to_string(),
            source.as_str().to_string(),
        );
        source.as_str()
    } else if let Some(workspace) = configured_default_workspace {
        session.set_workspace_path_meta(bamboo_config::paths::path_to_display_string(&workspace));
        "configured_default"
    } else {
        "session_fallback"
    };
    if let Err(error) = state
        .project_context_resolver
        .refresh_session_prompt_read_only(&mut session)
        .await
    {
        return Ok(match error {
            bamboo_engine::project_context::ProjectContextError::WorkspaceConflict {
                workspace,
                owner_project_id,
                session_project_id,
            } => HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_workspace_conflict",
                    "message": "Workspace belongs to another Project"
                },
                "workspace": workspace,
                "owner_project_id": owner_project_id,
                "session_project_id": session_project_id,
            })),
            bamboo_engine::project_context::ProjectContextError::UnassignedWorkspaceConflict {
                workspace,
                owner_project_id,
            } => HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_workspace_conflict",
                    "message": "Workspace belongs to another Project"
                },
                "workspace": workspace,
                "owner_project_id": owner_project_id,
                "session_project_id": "unassigned",
            })),
            bamboo_engine::project_context::ProjectContextError::WorkspaceInvalid {
                workspace,
                message,
            } => HttpResponse::BadRequest().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "workspace_invalid",
                    "message": message
                },
                "workspace": workspace,
            })),
            bamboo_engine::project_context::ProjectContextError::ProjectPathMissing {
                project_id,
            } => HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_path_missing",
                    "message": "Assigned Project has no configured project_path"
                },
                "project_id": project_id,
            })),
            bamboo_engine::project_context::ProjectContextError::ProjectPathUnavailable {
                project_id,
                project_path,
                message,
            } => HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_path_unavailable",
                    "message": message
                },
                "project_id": project_id,
                "project_path": project_path,
            })),
            error => {
                return Err(crate::error::json_internal_server_error(format!(
                    "Failed to initialize Project prompt context: {error}"
                )));
            }
        });
    }

    let save_started = Instant::now();
    if let Some(correlation_id) = correlation_id {
        tracing::info!(
            target: "bamboo.session_create",
            correlation_id,
            phase = "save_started",
            "session-create durable save started"
        );
    }
    state
        .storage
        .save_session(&session)
        .await
        .map_err(|error| {
            crate::error::json_internal_server_error(format!("Failed to save session: {error}"))
        })?;
    if let Some(correlation_id) = correlation_id {
        tracing::info!(
            target: "bamboo.session_create",
            correlation_id,
            phase = "session_committed",
            save_elapsed_ms = save_started.elapsed().as_millis() as u64,
            outcome = "durable",
            "session-create durable save committed"
        );
    }

    state.sessions.insert(
        id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session.clone())),
    );

    // Publish only the exact candidate that passed Project ownership checks,
    // and only after the authoritative session is durable. A storage failure
    // must not leave an orphan runtime workspace entry for an ID the API never
    // created.
    sync_runtime_workspace(
        state,
        &id,
        session.workspace_path_meta().as_deref(),
        workspace_source,
        correlation_id.is_some(),
    );

    // Publish onto the account change feed so other clients insert the new
    // session into their list without polling `GET /sessions`.
    let created_event = session_created_event(&session);
    if correlation_id.is_some() {
        if !state
            .account_sink
            .record_confirmed(Some(&id), &created_event)
            .await
        {
            return Err(crate::error::json_internal_server_error(
                "Failed to durably publish SessionCreated",
            ));
        }
    } else {
        state.account_sink.record(Some(&id), &created_event);
    }

    match state.session_store.get_index_entry(&id).await {
        // 201 Created — a new resource was created. Aligns `POST /api/v1/sessions`
        // with every other create endpoint (chat, mcp-add, prompt-presets,
        // provider-instances, cluster-nodes), which already return 201. #251
        // (finding 3).
        Some(entry) => Ok(HttpResponse::Created().json(CreateSessionResponse {
            session: SessionSummary::from_entry(entry, false),
        })),
        None => Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": crate::error::error_value("Session created but missing from index"),
            "session_id": id
        }))),
    }
}

fn build_new_session(
    id: &str,
    req: &CreateSessionRequest,
    gold_config_json: Option<String>,
    global_default_prompt: &str,
    config: &bamboo_llm::Config,
) -> Session {
    use bamboo_engine::session_app::session_create::{
        build_new_session as crate_build, CreateSessionConfig, CreateSessionInput,
    };

    let input = CreateSessionInput {
        id: id.to_string(),
        project_id: req.project_id.clone(),
        title: req.title.clone(),
        title_generated: req.title_generated,
        system_prompt: req.system_prompt.clone(),
        model: req.model.clone(),
        model_ref: req.model_ref.clone(),
        reasoning_effort: req.reasoning_effort,
        gold_config_json,
        workspace_path: req.workspace_path.clone(),
    };
    let create_config = CreateSessionConfig {
        default_model: config.get_model(),
        default_reasoning_effort: config.get_reasoning_effort(),
        global_default_prompt: global_default_prompt.to_string(),
        builtin_fallback_prompt: crate::app_state::DEFAULT_BASE_PROMPT,
    };

    crate_build(&input, &create_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::header, http::StatusCode, test, web, App};
    use bamboo_config::AccessControlConfig;
    use chrono::{Duration, Utc};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;

    async fn new_state() -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir").keep();
        bamboo_config::paths::init_bamboo_dir(temp_dir.clone());
        web::Data::new(AppState::new(temp_dir).await.expect("app state"))
    }

    #[actix_web::test]
    async fn create_session_returns_201_created() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({ "title": "New session" }))
                .to_request(),
        )
        .await;

        // 201 Created — aligns with the other create endpoints. #251 (finding 3).
        assert_eq!(resp.status(), StatusCode::CREATED);

        let body: Value = test::read_body_json(resp).await;
        assert!(
            body["session"]["id"].as_str().is_some(),
            "response should carry the created session summary"
        );
    }

    #[actix_web::test]
    async fn same_key_replays_the_same_session_and_status_lookup_recovers_it() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let key = "same-key-replay";
        let payload = serde_json::json!({"title": "One logical create"});

        let first = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::CREATED);
        let first: Value = test::read_body_json(first).await;
        let session_id = first["session"]["id"].as_str().unwrap().to_string();

        let replay = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(replay.status(), StatusCode::OK);
        let replay: Value = test::read_body_json(replay).await;
        assert_eq!(replay["session"]["id"], session_id);

        let status = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/session-create-operations/{key}"))
                .to_request(),
        )
        .await;
        assert_eq!(status.status(), StatusCode::OK);
        let status: Value = test::read_body_json(status).await;
        assert_eq!(status["status"], "succeeded");
        assert_eq!(status["session"]["id"], session_id);
        assert_eq!(
            state
                .session_store
                .list_index_entries()
                .await
                .into_iter()
                .filter(|entry| entry.id == session_id)
                .count(),
            1
        );
    }

    #[actix_web::test]
    async fn discarded_success_response_is_recovered_by_same_key_retry() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let key = "lost-response-retry";
        let payload = serde_json::json!({"title": "Response will be discarded"});

        let discarded = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(discarded.status(), StatusCode::CREATED);
        drop(discarded);

        let retry = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::OK);
        let retry: Value = test::read_body_json(retry).await;
        let session_id = retry["session"]["id"].as_str().unwrap();
        assert_eq!(
            state
                .session_store
                .list_index_entries()
                .await
                .into_iter()
                .filter(|entry| entry.id == session_id)
                .count(),
            1
        );
    }

    #[actix_web::test]
    async fn aborting_outer_handler_before_save_does_not_cancel_detached_create() {
        let state = new_state().await;
        let key = "detached-lost-response";
        let digest = key_digest(key);
        let claim = state.session_create_operations.lock(&digest).await.unwrap();
        let payload = serde_json::json!({"title": "Detached create survives"});
        let request: CreateSessionRequest = serde_json::from_value(payload.clone()).unwrap();
        let http_request = test::TestRequest::post()
            .uri("/api/v1/sessions")
            .insert_header((IDEMPOTENCY_KEY_HEADER, key))
            .to_http_request();

        let outer = actix_web::rt::spawn(create_session(
            state.clone(),
            http_request,
            web::Json(request),
        ));
        // The detached core reaches the held claim before any reservation/save.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(state
            .session_create_operations
            .load_for_status(&digest)
            .await
            .unwrap()
            .is_none());
        assert!(state.session_store.list_index_entries().await.is_empty());

        outer.abort();
        assert!(outer.await.is_err(), "outer request future must be aborted");
        // Still holding the gate proves the abort happened before save, not
        // after a response had already been constructed.
        assert!(state
            .session_create_operations
            .load_for_status(&digest)
            .await
            .unwrap()
            .is_none());
        drop(claim);

        let committed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Some(operation) = state
                    .session_create_operations
                    .load_for_status(&digest)
                    .await
                    .unwrap()
                {
                    if operation.status == StoredOperationStatus::Succeeded {
                        break operation;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("detached core must finish after the outer handler is gone");
        let session_id = committed.session_id;

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let retry = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::OK);
        let retry: Value = test::read_body_json(retry).await;
        assert_eq!(retry["session"]["id"], session_id);
        assert_eq!(
            state
                .session_store
                .list_index_entries()
                .await
                .into_iter()
                .filter(|entry| entry.id == session_id)
                .count(),
            1
        );
    }

    #[actix_web::test]
    async fn concurrent_same_key_requests_create_at_most_one_session() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let requests = (0..8).map(|_| {
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, "concurrent-key"))
                .set_json(serde_json::json!({"title": "Concurrent logical create"}))
                .to_request()
        });
        let responses =
            futures::future::join_all(requests.map(|request| test::call_service(&app, request)))
                .await;

        let mut ids = Vec::new();
        let mut created = 0;
        for response in responses {
            if response.status() == StatusCode::CREATED {
                created += 1;
            } else {
                assert_eq!(response.status(), StatusCode::OK);
            }
            let body: Value = test::read_body_json(response).await;
            ids.push(body["session"]["id"].as_str().unwrap().to_string());
        }
        assert_eq!(created, 1);
        assert!(ids.iter().all(|id| id == &ids[0]));
        assert_eq!(
            state
                .session_store
                .list_index_entries()
                .await
                .into_iter()
                .filter(|entry| entry.id == ids[0])
                .count(),
            1
        );
    }

    #[actix_web::test]
    async fn same_key_with_different_payload_returns_conflict() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let first = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, "payload-conflict"))
                .set_json(serde_json::json!({"title": "First"}))
                .to_request(),
        )
        .await;
        assert_eq!(first.status(), StatusCode::CREATED);

        let conflict = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, "payload-conflict"))
                .set_json(serde_json::json!({"title": "Different"}))
                .to_request(),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let conflict: Value = test::read_body_json(conflict).await;
        assert_eq!(conflict["error"]["code"], "idempotency_key_conflict");
        assert_eq!(state.session_store.list_index_entries().await.len(), 1);
    }

    #[actix_web::test]
    async fn operation_only_pending_survives_restart_and_reuses_reserved_uuid() {
        let data_dir = tempdir().expect("data dir").keep();
        let key = "pending-restart";
        let payload = serde_json::json!({"title": "Retry after restart"});
        let request: CreateSessionRequest = serde_json::from_value(payload.clone()).unwrap();
        let reserved_id = Uuid::new_v4().to_string();
        {
            let state = AppState::new(data_dir.clone()).await.expect("first state");
            let digest = key_digest(key);
            let operation = SessionCreateOperationRecord::pending(
                digest.clone(),
                payload_fingerprint(&request).unwrap(),
                reserved_id.clone(),
            );
            let _guard = state.session_create_operations.lock(&digest).await.unwrap();
            state
                .session_create_operations
                .save(&operation)
                .await
                .unwrap();
        }

        let state = web::Data::new(AppState::new(data_dir).await.expect("restarted state"));
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let pending = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/session-create-operations/{key}"))
                .to_request(),
        )
        .await;
        let pending: Value = test::read_body_json(pending).await;
        assert_eq!(pending["status"], "pending");

        let retry = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::CREATED);
        let retry: Value = test::read_body_json(retry).await;
        assert_eq!(retry["session"]["id"], reserved_id);
    }

    #[actix_web::test]
    async fn status_observes_pending_while_post_claim_lock_is_still_owned() {
        let state = new_state().await;
        let key = "pending-under-active-claim";
        let digest = key_digest(key);
        let request: CreateSessionRequest =
            serde_json::from_value(serde_json::json!({"title": "Still creating"})).unwrap();
        let operation = SessionCreateOperationRecord::pending(
            digest.clone(),
            payload_fingerprint(&request).unwrap(),
            Uuid::new_v4().to_string(),
        );
        state
            .session_create_operations
            .save(&operation)
            .await
            .unwrap();

        // Model the exclusive claim held for the full POST attempt. The status
        // route must read the durable pending receipt without joining this
        // lock, otherwise timeout recovery would wait for the original POST.
        let claim = state.session_create_operations.lock(&digest).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&format!("/api/v1/session-create-operations/{key}"))
                    .to_request(),
            ),
        )
        .await
        .expect("status lookup must not block on the POST claim lock");
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["status"], "pending");
        drop(claim);
    }

    #[actix_web::test]
    async fn pending_committed_session_repairs_missing_index_after_restart() {
        let data_dir = tempdir().expect("data dir").keep();
        let key = "committed-index-missing";
        let reserved_id = Uuid::new_v4().to_string();
        {
            let state = AppState::new(data_dir.clone()).await.expect("first state");
            let session = Session::new(reserved_id.clone(), "model");
            state.storage.save_session(&session).await.unwrap();
            let request: CreateSessionRequest =
                serde_json::from_value(serde_json::json!({"title": "Committed"})).unwrap();
            let digest = key_digest(key);
            let operation = SessionCreateOperationRecord::pending(
                digest,
                payload_fingerprint(&request).unwrap(),
                reserved_id.clone(),
            );
            state
                .session_create_operations
                .save(&operation)
                .await
                .unwrap();

            let index_path = data_dir.join("sessions.json");
            let mut index: Value =
                serde_json::from_slice(&tokio::fs::read(&index_path).await.expect("read index"))
                    .unwrap();
            index["sessions"]
                .as_object_mut()
                .unwrap()
                .remove(&reserved_id);
            tokio::fs::write(&index_path, serde_json::to_vec_pretty(&index).unwrap())
                .await
                .unwrap();
        }

        let state = web::Data::new(AppState::new(data_dir).await.expect("restarted state"));
        assert!(state
            .session_store
            .get_index_entry(&reserved_id)
            .await
            .is_none());
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let status = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/session-create-operations/{key}"))
                .to_request(),
        )
        .await;
        assert_eq!(status.status(), StatusCode::OK);
        let status: Value = test::read_body_json(status).await;
        assert_eq!(status["status"], "succeeded");
        assert_eq!(status["session"]["id"], reserved_id);
        assert!(state
            .session_store
            .get_index_entry(&reserved_id)
            .await
            .is_some());
    }

    #[actix_web::test]
    async fn pending_committed_session_repairs_present_wrong_index_path() {
        let data_dir = tempdir().expect("data dir").keep();
        let key = "committed-index-wrong-path";
        let reserved_id = Uuid::new_v4().to_string();
        {
            let state = AppState::new(data_dir.clone()).await.expect("first state");
            state
                .storage
                .save_session(&Session::new(reserved_id.clone(), "model"))
                .await
                .unwrap();
            let request: CreateSessionRequest =
                serde_json::from_value(serde_json::json!({"title": "Committed"})).unwrap();
            state
                .session_create_operations
                .save(&SessionCreateOperationRecord::pending(
                    key_digest(key),
                    payload_fingerprint(&request).unwrap(),
                    reserved_id.clone(),
                ))
                .await
                .unwrap();

            let index_path = data_dir.join("sessions.json");
            let mut index: Value =
                serde_json::from_slice(&tokio::fs::read(&index_path).await.unwrap()).unwrap();
            index["sessions"][&reserved_id]["rel_path"] =
                Value::String("sessions/not-the-reserved-session".to_string());
            tokio::fs::write(&index_path, serde_json::to_vec_pretty(&index).unwrap())
                .await
                .unwrap();
        }

        let state = web::Data::new(AppState::new(data_dir).await.expect("restarted state"));
        assert_eq!(
            state
                .session_store
                .get_index_entry(&reserved_id)
                .await
                .unwrap()
                .rel_path,
            "sessions/not-the-reserved-session"
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let status = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/session-create-operations/{key}"))
                .to_request(),
        )
        .await;
        assert_eq!(status.status(), StatusCode::OK);
        let status: Value = test::read_body_json(status).await;
        assert_eq!(status["status"], "succeeded");
        assert_eq!(status["session"]["id"], reserved_id);
        assert_eq!(
            state
                .session_store
                .get_index_entry(&reserved_id)
                .await
                .unwrap()
                .rel_path,
            format!("sessions/{reserved_id}")
        );
    }

    #[actix_web::test]
    async fn succeeded_receipt_repairs_missing_index_without_republishing_projection() {
        let data_dir = tempdir().expect("data dir").keep();
        let key = "succeeded-index-missing";
        let reserved_id = Uuid::new_v4().to_string();
        {
            let state = AppState::new(data_dir.clone()).await.expect("first state");
            state
                .storage
                .save_session(&Session::new(reserved_id.clone(), "model"))
                .await
                .unwrap();
            let request: CreateSessionRequest =
                serde_json::from_value(serde_json::json!({"title": "Succeeded"})).unwrap();
            let mut operation = SessionCreateOperationRecord::pending(
                key_digest(key),
                payload_fingerprint(&request).unwrap(),
                reserved_id.clone(),
            );
            operation.mark_succeeded();
            state
                .session_create_operations
                .save(&operation)
                .await
                .unwrap();

            let index_path = data_dir.join("sessions.json");
            let mut index: Value =
                serde_json::from_slice(&tokio::fs::read(&index_path).await.unwrap()).unwrap();
            index["sessions"]
                .as_object_mut()
                .unwrap()
                .remove(&reserved_id);
            tokio::fs::write(&index_path, serde_json::to_vec_pretty(&index).unwrap())
                .await
                .unwrap();
        }

        let state = web::Data::new(AppState::new(data_dir).await.expect("fresh state"));
        assert!(state
            .session_store
            .get_index_entry(&reserved_id)
            .await
            .is_none());
        let mut account_events = state.account_sink.subscribe();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let status = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/session-create-operations/{key}"))
                .to_request(),
        )
        .await;
        assert_eq!(status.status(), StatusCode::OK);
        let status: Value = test::read_body_json(status).await;
        assert_eq!(status["status"], "succeeded");
        assert_eq!(status["session"]["id"], reserved_id);
        assert!(state
            .session_store
            .get_index_entry(&reserved_id)
            .await
            .is_some());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), account_events.recv())
                .await
                .is_err(),
            "succeeded index repair must not republish SessionCreated"
        );
    }

    #[actix_web::test]
    async fn corrupt_authoritative_succeeded_result_returns_500_and_keeps_receipt() {
        let state = new_state().await;
        let key = "succeeded-corrupt-authoritative";
        let digest = key_digest(key);
        let reserved_id = Uuid::new_v4().to_string();
        state
            .storage
            .save_session(&Session::new(reserved_id.clone(), "model"))
            .await
            .unwrap();
        let request: CreateSessionRequest =
            serde_json::from_value(serde_json::json!({"title": "Corrupt"})).unwrap();
        let mut operation = SessionCreateOperationRecord::pending(
            digest.clone(),
            payload_fingerprint(&request).unwrap(),
            reserved_id.clone(),
        );
        operation.mark_succeeded();
        state
            .session_create_operations
            .save(&operation)
            .await
            .unwrap();
        let session_path = state
            .session_store
            .sessions_root_dir()
            .join(&reserved_id)
            .join("session.json");
        tokio::fs::write(&session_path, b"corrupt-authoritative")
            .await
            .unwrap();

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        for request in [
            test::TestRequest::get()
                .uri(&format!("/api/v1/session-create-operations/{key}"))
                .to_request(),
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(serde_json::json!({"title": "Corrupt"}))
                .to_request(),
        ] {
            let response = test::call_service(&app, request).await;
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(
                tokio::fs::read(&session_path).await.unwrap(),
                b"corrupt-authoritative"
            );
            assert_eq!(
                state
                    .session_create_operations
                    .load_for_status(&digest)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                StoredOperationStatus::Succeeded
            );
        }
    }

    #[actix_web::test]
    async fn corrupt_authoritative_pending_result_is_not_overwritten_or_terminalized() {
        let state = new_state().await;
        let key = "pending-corrupt-authoritative";
        let digest = key_digest(key);
        let reserved_id = Uuid::new_v4().to_string();
        state
            .storage
            .save_session(&Session::new(reserved_id.clone(), "model"))
            .await
            .unwrap();
        let request: CreateSessionRequest =
            serde_json::from_value(serde_json::json!({"title": "Pending corrupt"})).unwrap();
        state
            .session_create_operations
            .save(&SessionCreateOperationRecord::pending(
                digest.clone(),
                payload_fingerprint(&request).unwrap(),
                reserved_id.clone(),
            ))
            .await
            .unwrap();
        let session_path = state
            .session_store
            .sessions_root_dir()
            .join(&reserved_id)
            .join("session.json");
        tokio::fs::write(&session_path, b"pending-corrupt")
            .await
            .unwrap();

        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(serde_json::json!({"title": "Pending corrupt"}))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            tokio::fs::read(&session_path).await.unwrap(),
            b"pending-corrupt"
        );
        assert_eq!(
            state
                .session_create_operations
                .load_for_status(&digest)
                .await
                .unwrap()
                .unwrap()
                .status,
            StoredOperationStatus::Pending
        );
    }

    #[actix_web::test]
    async fn stale_preinitialized_state_replays_other_process_success_without_projection_or_410() {
        let data_dir = tempdir().expect("data dir").keep();
        let state_a = web::Data::new(AppState::new(data_dir.clone()).await.expect("state A"));
        let state_b = web::Data::new(AppState::new(data_dir).await.expect("state B"));
        let key = "rolling-state-success";
        let payload = serde_json::json!({"title": "Rolling success"});
        let app_a = test::init_service(
            App::new()
                .app_data(state_a.clone())
                .configure(configure_routes),
        )
        .await;
        let created = test::call_service(
            &app_a,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: Value = test::read_body_json(created).await;
        let session_id = created["session"]["id"].as_str().unwrap().to_string();
        assert!(state_b
            .session_store
            .get_index_entry(&session_id)
            .await
            .is_none());

        let mut account_events = state_b.account_sink.subscribe();
        let app_b = test::init_service(
            App::new()
                .app_data(state_b.clone())
                .configure(configure_routes),
        )
        .await;
        let status = test::call_service(
            &app_b,
            test::TestRequest::get()
                .uri(&format!("/api/v1/session-create-operations/{key}"))
                .to_request(),
        )
        .await;
        assert_eq!(status.status(), StatusCode::OK);
        let status: Value = test::read_body_json(status).await;
        assert_eq!(status["status"], "succeeded");
        assert_eq!(status["session"]["id"], session_id);

        let replay = test::call_service(
            &app_b,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(replay.status(), StatusCode::OK);
        let replay: Value = test::read_body_json(replay).await;
        assert_eq!(replay["session"]["id"], session_id);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), account_events.recv())
                .await
                .is_err(),
            "stale succeeded replay must not publish through the second sink"
        );
        assert_eq!(
            state_b
                .session_store
                .list_index_entries()
                .await
                .into_iter()
                .filter(|entry| entry.id == session_id)
                .count(),
            1
        );
    }

    #[actix_web::test]
    async fn succeeded_status_and_replay_preserve_newer_live_cache_arc_without_projection() {
        let state = new_state().await;
        let key = "succeeded-live-cache";
        let payload = serde_json::json!({"title": "Durable title"});
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let created = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        let created: Value = test::read_body_json(created).await;
        let session_id = created["session"]["id"].as_str().unwrap().to_string();
        let session_dir = state.session_store.sessions_root_dir().join(&session_id);
        let older_session_json = tokio::fs::read(session_dir.join("session.json"))
            .await
            .unwrap();
        let older_runtime_json = tokio::fs::read(session_dir.join("runtime.json"))
            .await
            .unwrap();
        let original_arc = state.sessions.get(&session_id).unwrap().value().clone();
        {
            let mut live = original_arc.write();
            live.title = "LIVE-SENTINEL".to_string();
            live.metadata
                .insert("live_sentinel".to_string(), "must-survive".to_string());
            live.updated_at = Utc::now() + Duration::minutes(5);
        }
        // Publish the newer live summary into the global index, then restore
        // the older authoritative files to model a delayed transcript snapshot.
        // Succeeded replay must repair identity/path without regressing either
        // the cache Arc or the newer index fields.
        let newer_live = original_arc.read().clone();
        state.storage.save_session(&newer_live).await.unwrap();
        tokio::fs::write(session_dir.join("session.json"), older_session_json)
            .await
            .unwrap();
        tokio::fs::write(session_dir.join("runtime.json"), older_runtime_json)
            .await
            .unwrap();
        let mut account_events = state.account_sink.subscribe();

        let status = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/session-create-operations/{key}"))
                .to_request(),
        )
        .await;
        let status: Value = test::read_body_json(status).await;
        assert_eq!(status["status"], "succeeded");
        assert_eq!(status["session"]["id"], session_id);
        assert_eq!(status["session"]["title"], "LIVE-SENTINEL");
        let after_status = state.sessions.get(&session_id).unwrap().value().clone();
        assert!(std::sync::Arc::ptr_eq(&original_arc, &after_status));
        assert_eq!(after_status.read().title, "LIVE-SENTINEL");

        let replay = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        assert_eq!(replay.status(), StatusCode::OK);
        let replay: Value = test::read_body_json(replay).await;
        assert_eq!(replay["session"]["id"], session_id);
        assert_eq!(replay["session"]["title"], "LIVE-SENTINEL");
        let after_replay = state.sessions.get(&session_id).unwrap().value().clone();
        assert!(std::sync::Arc::ptr_eq(&original_arc, &after_replay));
        assert_eq!(
            after_replay
                .read()
                .metadata
                .get("live_sentinel")
                .map(String::as_str),
            Some("must-survive")
        );
        let index_entry = state
            .session_store
            .get_index_entry(&session_id)
            .await
            .unwrap();
        assert_eq!(index_entry.title, "LIVE-SENTINEL");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), account_events.recv())
                .await
                .is_err(),
            "succeeded status/replay must not republish SessionCreated"
        );
    }

    #[actix_web::test]
    async fn deleted_succeeded_target_is_terminal_gone_and_is_never_recreated() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let key = "deleted-success";
        let payload = serde_json::json!({"title": "Delete after create"});
        let created = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                .set_json(&payload)
                .to_request(),
        )
        .await;
        let created: Value = test::read_body_json(created).await;
        let session_id = created["session"]["id"].as_str().unwrap().to_string();
        assert!(state.storage.delete_session(&session_id).await.unwrap());

        for _ in 0..2 {
            let replay = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/sessions")
                    .insert_header((IDEMPOTENCY_KEY_HEADER, key))
                    .set_json(&payload)
                    .to_request(),
            )
            .await;
            assert_eq!(replay.status(), StatusCode::GONE);
            let replay: Value = test::read_body_json(replay).await;
            assert_eq!(replay["error"]["code"], "session_result_gone");
        }
        assert!(state
            .storage
            .load_session(&session_id)
            .await
            .unwrap()
            .is_none());

        let status = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/session-create-operations/{key}"))
                .to_request(),
        )
        .await;
        let status: Value = test::read_body_json(status).await;
        assert_eq!(status["status"], "failed");
        assert_eq!(status["error"]["code"], "session_result_gone");
    }

    #[actix_web::test]
    async fn invalid_unknown_and_expired_operation_contracts_are_explicit() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;
        let invalid = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, "contains space"))
                .set_json(serde_json::json!({"title": "Invalid"}))
                .to_request(),
        )
        .await;
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);
        let invalid: Value = test::read_body_json(invalid).await;
        assert_eq!(invalid["error"]["code"], "invalid_idempotency_key");

        let unknown = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/session-create-operations/unknown-key")
                .to_request(),
        )
        .await;
        assert_eq!(unknown.headers().get("Cache-Control").unwrap(), "no-store");
        let unknown: Value = test::read_body_json(unknown).await;
        assert_eq!(unknown["status"], "unknown");

        let digest = key_digest("expired-key");
        let mut expired = SessionCreateOperationRecord::pending(
            digest.clone(),
            payload_fingerprint(&serde_json::json!({"title": "Expired"})).unwrap(),
            Uuid::new_v4().to_string(),
        );
        expired.mark_failed(safe_error_for_status(StatusCode::BAD_REQUEST));
        expired.expires_at = Some(Utc::now() - Duration::seconds(1));
        state
            .session_create_operations
            .save(&expired)
            .await
            .unwrap();
        let expired_response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/session-create-operations/expired-key")
                .to_request(),
        )
        .await;
        let expired_response: Value = test::read_body_json(expired_response).await;
        assert_eq!(expired_response["status"], "expired");
        assert_eq!(expired_response["error"]["code"], "idempotency_key_expired");
        assert!(state
            .session_create_operations
            .load_for_status(&digest)
            .await
            .unwrap()
            .is_some());

        // Expired tombstones remain queryable, but a POST claim may prune and
        // reuse the key for a new logical operation.
        let reused = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .insert_header((IDEMPOTENCY_KEY_HEADER, "expired-key"))
                .set_json(serde_json::json!({"title": "Expired"}))
                .to_request(),
        )
        .await;
        assert_eq!(reused.status(), StatusCode::CREATED);
        let replacement = state
            .session_create_operations
            .load_for_status(&digest)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replacement.status, StoredOperationStatus::Succeeded);
        assert_ne!(replacement.session_id, expired.session_id);
    }

    #[actix_web::test]
    async fn operation_status_route_remains_behind_api_access_authentication() {
        let state = new_state().await;
        {
            let mut config = state.config.write().await;
            config.access_control = Some(AccessControlConfig {
                password_enabled: true,
                repair_required: false,
                password_hash: Some(
                    "a65192f8d645bc4d19765b8ea61bfbb896dc999cb88a4be419518c5493f92c9d".to_string(),
                ),
                password_salt: Some("01010101010101010101010101010101".to_string()),
                password_credential_ref: None,
                password_configured: false,
                updated_at: None,
                devices: Vec::new(),
            });
        }
        let app = test::init_service(App::new().app_data(state).configure(configure_routes)).await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/session-create-operations/auth-probe")
                .peer_addr("203.0.113.7:5700".parse().unwrap())
                .insert_header((header::HOST, "bamboo.example.com"))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[actix_web::test]
    async fn fingerprint_covers_every_caller_controlled_create_field() {
        let baseline: CreateSessionRequest = serde_json::from_value(serde_json::json!({})).unwrap();
        let baseline = payload_fingerprint(&baseline).unwrap();
        let variants = [
            serde_json::json!({"project_id": "project-1"}),
            serde_json::json!({"title": "title"}),
            serde_json::json!({"title_generated": false}),
            serde_json::json!({"system_prompt": "prompt"}),
            serde_json::json!({"model": "model"}),
            serde_json::json!({"provider": "provider"}),
            serde_json::json!({"model_ref": {"provider": "provider", "model": "model"}}),
            serde_json::json!({"reasoning_effort": "high"}),
            serde_json::json!({"gold_config": {"gold": true}}),
            serde_json::json!({"workspace_path": "/workspace"}),
        ];
        for value in variants {
            let request: CreateSessionRequest = serde_json::from_value(value.clone()).unwrap();
            assert_ne!(
                payload_fingerprint(&request).unwrap(),
                baseline,
                "caller field was omitted from fingerprint: {value}"
            );
        }
    }

    /// #480: `POST /sessions` gets the same `workspace_path` semantics as
    /// `POST /chat` — the created session's metadata carries the resolved
    /// workspace path.
    #[actix_web::test]
    async fn create_session_with_workspace_path_sets_it() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let workspace_dir = tempdir().expect("workspace tempdir");
        let workspace_path = workspace_dir.path().to_string_lossy().to_string();
        let canonical_workspace_path = std::fs::canonicalize(workspace_dir.path())
            .expect("canonical workspace")
            .to_string_lossy()
            .into_owned();

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Session with workspace",
                    "workspace_path": workspace_path,
                }))
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(resp).await;
        assert_eq!(
            body["session"]["workspace_path"].as_str(),
            Some(canonical_workspace_path.as_str())
        );
        let session_id = body["session"]["id"]
            .as_str()
            .expect("session id")
            .to_string();

        let session = state
            .storage
            .load_session(&session_id)
            .await
            .expect("load")
            .expect("session exists");
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(canonical_workspace_path.as_str())
        );

        let list_resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        assert_eq!(list_resp.status(), StatusCode::OK);
        let list_body: Value = test::read_body_json(list_resp).await;
        assert_eq!(
            list_body["sessions"][0]["workspace_path"].as_str(),
            Some(canonical_workspace_path.as_str())
        );

        let detail_resp = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .to_request(),
        )
        .await;
        assert_eq!(detail_resp.status(), StatusCode::OK);
        let detail_body: Value = test::read_body_json(detail_resp).await;
        assert_eq!(
            detail_body["session"]["workspace_path"].as_str(),
            Some(canonical_workspace_path.as_str())
        );
    }

    /// Omitting `workspace_path` persists the same validated fallback that
    /// runtime tools will use.
    #[actix_web::test]
    async fn create_session_without_workspace_path_persists_validated_fallback() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({ "title": "No workspace" }))
                .to_request(),
        )
        .await;

        assert_eq!(resp.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(resp).await;
        assert!(
            body["session"].get("project_id").is_some(),
            "create response must expose the Unassigned Project as null"
        );
        assert!(body["session"]["project_id"].is_null());
        let session_id = body["session"]["id"]
            .as_str()
            .expect("session id")
            .to_string();

        let session = state
            .storage
            .load_session(&session_id)
            .await
            .expect("load")
            .expect("session exists");
        let workspace = session
            .workspace_path_meta()
            .expect("validated session fallback workspace");
        let workspace_root = bamboo_config::paths::resolve_workspace_root_in(&state.app_data_dir);
        assert!(
            std::path::Path::new(&workspace).is_dir(),
            "authoritative create must materialize the validated fallback: \
             resolved={workspace}, root={}, source=session_fallback",
            workspace_root.display(),
        );
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(&session_id)
                .as_deref()
                .map(bamboo_config::paths::path_to_display_string),
            Some(workspace.clone()),
            "runtime publication drifted: resolved={workspace}, root={}, \
             source=session_fallback",
            workspace_root.display(),
        );
        assert!(
            bamboo_tools::tools::workspace_state::workspace_or_process_cwd(Some(&session_id))
                .is_dir(),
            "tool cwd must be usable immediately after create"
        );

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        let list: Value = test::read_body_json(list).await;
        let listed = list["sessions"]
            .as_array()
            .expect("sessions")
            .iter()
            .find(|entry| entry["id"] == session_id)
            .expect("created session in list");
        assert!(
            listed.get("project_id").is_some(),
            "list response must expose the Unassigned Project as null"
        );
        assert!(listed["project_id"].is_null());

        let detail = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{session_id}"))
                .to_request(),
        )
        .await;
        let detail: Value = test::read_body_json(detail).await;
        assert!(
            detail["session"].get("project_id").is_some(),
            "detail response must expose the Unassigned Project as null"
        );
        assert!(detail["session"]["project_id"].is_null());
    }

    #[actix_web::test]
    async fn project_workspace_ownership_is_checked_before_create_side_effects() {
        let state = new_state().await;
        let owner_workspace = tempdir().expect("owner workspace");
        let nested_workspace = owner_workspace.path().join("nested");
        std::fs::create_dir_all(&nested_workspace).expect("nested workspace");
        let owner = state
            .project_store
            .create_with_bindings(
                "Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: owner_workspace.path().to_string_lossy().to_string(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("owner Project");
        let other = state
            .project_store
            .create("Other", None)
            .expect("other Project");
        let nested_workspace_display = nested_workspace.to_string_lossy().to_string();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let conflict = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Must not exist",
                    "project_id": other.id.to_string(),
                    "workspace_path": nested_workspace_display.clone(),
                }))
                .to_request(),
        )
        .await;
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        let conflict_body: Value = test::read_body_json(conflict).await;
        assert_eq!(conflict_body["error"]["code"], "project_workspace_conflict");

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        let list: Value = test::read_body_json(list).await;
        assert_eq!(list["sessions"].as_array().map(Vec::len), Some(0));

        let created = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Owned nested workspace",
                    "project_id": owner.id.to_string(),
                    "workspace_path": nested_workspace_display.clone(),
                }))
                .to_request(),
        )
        .await;
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: Value = test::read_body_json(created).await;
        assert_eq!(created["session"]["project_id"], owner.id.as_str());
        let session_id = created["session"]["id"].as_str().expect("session id");

        let prompt = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/api/v1/sessions/{session_id}/system-prompt"))
                .to_request(),
        )
        .await;
        assert_eq!(prompt.status(), StatusCode::OK);
        let prompt: Value = test::read_body_json(prompt).await;
        assert!(prompt["project_context"]
            .as_str()
            .is_some_and(|value| value.contains(owner.id.as_str())));
        let effective = prompt["effective_system_prompt"]
            .as_str()
            .expect("effective prompt");
        assert_eq!(
            effective
                .matches("<!-- BAMBOO_PROJECT_CONTEXT_START -->")
                .count(),
            1
        );
        assert_eq!(
            effective
                .matches("<!-- BAMBOO_WORKSPACE_CONTEXT_START -->")
                .count(),
            1
        );
        assert!(effective.contains("Binding status: registered"));
    }

    #[actix_web::test]
    async fn assigned_session_created_event_replays_project_identity_from_journal() {
        let state = new_state().await;
        let project_path = tempdir().expect("Project path");
        let project = state
            .project_store
            .create_with_project_path(
                "Journal Project",
                None,
                project_path.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("Project");
        let mut live = state.account_sink.subscribe();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Journaled session",
                    "project_id": project.id.to_string()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(response).await;
        let session_id = body["session"]["id"].as_str().expect("session id");
        let live_event = tokio::time::timeout(std::time::Duration::from_secs(1), live.recv())
            .await
            .expect("live event timeout")
            .expect("live event");
        assert!(matches!(
            &live_event.event,
            bamboo_agent_core::AgentEvent::SessionCreated {
                session_id: event_session_id,
                project_id: Some(event_project_id),
                ..
            } if event_session_id == session_id && event_project_id == project.id.as_str()
        ));

        let replay = bamboo_engine::events::journal::read_since(
            state.account_sink.events_dir(),
            live_event.seq.saturating_sub(1),
        )
        .expect("journal replay");
        assert!(replay.iter().any(|change| matches!(
            &change.event,
            bamboo_agent_core::AgentEvent::SessionCreated {
                session_id: event_session_id,
                project_id: Some(event_project_id),
                ..
            } if event_session_id == session_id && event_project_id == project.id.as_str()
        )));
    }

    #[actix_web::test]
    async fn assigned_project_path_wins_over_foreign_configured_default() {
        let state = new_state().await;
        let workspace = tempdir().expect("foreign default workspace");
        let other_workspace = tempdir().expect("assigned Project path");
        let owner = state
            .project_store
            .create_with_project_path(
                "Default Owner",
                None,
                workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("owner Project");
        let other = state
            .project_store
            .create_with_project_path(
                "Other Project",
                None,
                other_workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("other Project");
        state.config.write().await.default_work_area = Some(bamboo_config::DefaultWorkAreaConfig {
            path: Some(workspace.path().to_string_lossy().into_owned()),
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Uses Project path",
                    "project_id": other.id.to_string()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(
            body["session"]["workspace_path"],
            bamboo_config::paths::path_to_display_string(
                &other_workspace.path().canonicalize().unwrap()
            )
        );
        assert_ne!(body["session"]["project_id"], owner.id.as_str());
    }

    #[actix_web::test]
    async fn same_project_default_workspace_is_persisted_with_prompt_marker() {
        let state = new_state().await;
        let workspace = tempdir().expect("default workspace");
        let foreign_default = tempdir().expect("foreign global default");
        let project = state
            .project_store
            .create_with_project_path(
                "Default Owner",
                None,
                workspace.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("Project");
        state.config.write().await.default_work_area = Some(bamboo_config::DefaultWorkAreaConfig {
            path: Some(foreign_default.path().to_string_lossy().into_owned()),
        });
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Uses validated default",
                    "project_id": project.id.to_string()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);
        let body: Value = test::read_body_json(response).await;
        let session_id = body["session"]["id"].as_str().expect("session id");
        let canonical = workspace
            .path()
            .canonicalize()
            .expect("canonical workspace");
        let canonical_display = bamboo_config::paths::path_to_display_string(&canonical);
        assert_eq!(
            body["session"]["workspace_path"].as_str(),
            Some(canonical_display.as_str())
        );
        let session = state
            .storage
            .load_session(session_id)
            .await
            .expect("load")
            .expect("session");
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(canonical_display.as_str())
        );
        let index_entry = state
            .session_store
            .list_index_entries()
            .await
            .into_iter()
            .find(|entry| entry.id == session_id)
            .expect("session index entry");
        assert_eq!(
            index_entry.workspace_path.as_deref(),
            Some(canonical_display.as_str())
        );
        assert_eq!(index_entry.project_id.as_deref(), Some(project.id.as_str()));
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(session_id).as_deref(),
            Some(canonical.as_path())
        );
        let snapshot = session.prompt_snapshot.expect("prompt snapshot");
        assert!(snapshot.workspace_context.as_deref().is_some_and(|value| {
            value.contains("Binding status: registered")
                && value.contains("Workspace source: project_default")
        }));
        let project_context = snapshot
            .project_context
            .as_deref()
            .expect("Project context");
        assert!(project_context.contains(&format!("Project path: {canonical_display}")));
        assert!(project_context.contains("Project home (Bamboo data):"));
        assert_eq!(
            snapshot
                .effective_system_prompt
                .matches("BAMBOO_PROJECT_CONTEXT_START")
                .count(),
            1
        );
        assert_eq!(
            snapshot
                .effective_system_prompt
                .matches("BAMBOO_WORKSPACE_CONTEXT_START")
                .count(),
            1
        );
    }

    #[actix_web::test]
    async fn assigned_project_without_path_fails_before_session_side_effects() {
        let state = new_state().await;
        let project = state
            .project_store
            .create("Legacy unconfigured", None)
            .expect("legacy Project");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Must not persist",
                    "project_id": project.id.to_string()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "project_path_missing");
        assert!(state.session_store.list_index_entries().await.is_empty());
    }

    #[actix_web::test]
    async fn unavailable_project_path_fails_before_session_side_effects() {
        let state = new_state().await;
        let project_path = tempdir().expect("Project path");
        let project = state
            .project_store
            .create_with_project_path(
                "Moved Project",
                None,
                project_path.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("Project");
        project_path.close().expect("remove Project path");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({
                    "title": "Must not persist",
                    "project_id": project.id.to_string()
                }))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body: Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["code"], "project_path_unavailable");
        assert_eq!(body["project_id"], project.id.as_str());
        assert!(state.session_store.list_index_entries().await.is_empty());
    }

    #[actix_web::test]
    async fn same_project_identity_is_stable_across_root_session_workspaces_and_apis() {
        let state = new_state().await;
        let first_workspace = tempdir().expect("first workspace");
        let second_workspace = tempdir().expect("second workspace");
        let project = state
            .project_store
            .create_with_project_path(
                "Multi-workspace Project",
                None,
                first_workspace.path().to_string_lossy(),
                vec![bamboo_domain::WorkspaceBinding {
                    path: second_workspace.path().to_string_lossy().into_owned(),
                    label: Some("second".to_string()),
                    git_common_dir: None,
                }],
            )
            .expect("Project");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let mut created = Vec::new();
        for (title, workspace) in [
            ("First root", first_workspace.path()),
            ("Second root", second_workspace.path()),
        ] {
            let canonical_workspace = std::fs::canonicalize(workspace)
                .expect("canonical workspace")
                .to_string_lossy()
                .into_owned();
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/sessions")
                    .set_json(serde_json::json!({
                        "title": title,
                        "project_id": project.id,
                        "workspace_path": workspace,
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED);
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["session"]["project_id"], project.id.as_str());
            assert_eq!(body["session"]["workspace_path"], canonical_workspace);
            created.push((
                body["session"]["id"]
                    .as_str()
                    .expect("session id")
                    .to_string(),
                canonical_workspace,
            ));
        }

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        let list: Value = test::read_body_json(list).await;
        for (session_id, workspace) in &created {
            let listed = list["sessions"]
                .as_array()
                .expect("sessions")
                .iter()
                .find(|entry| entry["id"] == session_id.as_str())
                .expect("session in list");
            assert_eq!(listed["project_id"], project.id.as_str());
            assert_eq!(listed["workspace_path"], workspace.as_str());

            let detail = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&format!("/api/v1/sessions/{session_id}"))
                    .to_request(),
            )
            .await;
            let detail: Value = test::read_body_json(detail).await;
            assert_eq!(detail["session"]["project_id"], project.id.as_str());
            assert_eq!(detail["session"]["workspace_path"], workspace.as_str());
        }
    }

    #[actix_web::test]
    async fn invalid_workspace_inputs_return_400_without_creating_sessions() {
        let state = new_state().await;
        let fixture = tempdir().expect("workspace fixture");
        let missing = fixture.path().join("missing");
        let file = fixture.path().join("file.txt");
        std::fs::write(&file, "not a directory").expect("file fixture");
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        for (path, code) in [
            (missing.to_string_lossy().to_string(), "workspace_not_found"),
            (
                file.to_string_lossy().to_string(),
                "workspace_not_directory",
            ),
        ] {
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/api/v1/sessions")
                    .set_json(serde_json::json!({
                        "title": "Must not exist",
                        "workspace_path": path,
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body: Value = test::read_body_json(response).await;
            assert_eq!(body["error"]["code"], code);
        }

        let list = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/sessions")
                .to_request(),
        )
        .await;
        let list: Value = test::read_body_json(list).await;
        assert_eq!(list["sessions"].as_array().map(Vec::len), Some(0));
    }
}
