//! Owner-only storage maintenance. Conversation compaction is a separate API.

use std::{io, sync::Arc, time::Duration};

use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse};
use bamboo_storage::v2::{
    SessionArchiveOperation as Operation, SessionArchiveOutcome as Outcome,
    DEFAULT_SESSION_ARCHIVE_IDLE_DAYS,
};
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

use super::crud::{owner_authorized, process_busy};
use crate::app_state::AppState;

const MAX_BATCH: usize = 32;
const BETWEEN_BATCHES: Duration = Duration::from_secs(2);
const FIRST_SWEEP_DELAY: Duration = Duration::from_secs(60);
const SWEEP_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

fn idle_days() -> io::Result<u16> {
    match std::env::var("BAMBOO_SESSION_ARCHIVE_IDLE_DAYS") {
        Ok(value) => value
            .parse::<u16>()
            .ok()
            .filter(|days| *days > 0)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "BAMBOO_SESSION_ARCHIVE_IDLE_DAYS must be a positive integer",
                )
            }),
        Err(std::env::VarError::NotPresent) => Ok(DEFAULT_SESSION_ARCHIVE_IDLE_DAYS),
        Err(error) => Err(io::Error::new(io::ErrorKind::InvalidInput, error)),
    }
}

fn cutoff(now: DateTime<Utc>, days: u16) -> DateTime<Utc> {
    now - TimeDelta::days(i64::from(days))
}

async fn maintain(
    state: &AppState,
    id: &str,
    cutoff: DateTime<Utc>,
    operation: Operation,
) -> io::Result<Option<Outcome>> {
    let _local = state.persistence.acquire_lock(id).await;
    if operation != Operation::Restore
        && (process_busy(state, id).await
            || !bamboo_tools::tools::bash_runtime::running_shells_for_session(id).is_empty()
            || !bamboo_tools::tools::bash_runtime::uncollected_polled_shells_for_session(id)
                .is_empty())
    {
        return Ok(Some(Outcome::Skipped {
            reason: "execution_owner_present",
        }));
    }
    let outcome = state
        .session_store
        .maintain_session_archive(id, cutoff, operation)
        .await?;
    if matches!(outcome, Some(Outcome::Archived { .. })) {
        state.sessions.remove(id);
    }
    Ok(outcome)
}

pub(crate) fn spawn_automatic_session_archive(state: &Arc<AppState>) {
    if std::env::var("BAMBOO_SESSION_ARCHIVE_ENABLED").as_deref() != Ok("true") {
        return;
    }
    let days = match idle_days() {
        Ok(days) => days,
        Err(error) => {
            tracing::warn!(%error, "Session archive configuration is invalid");
            return;
        }
    };
    let shutdown = state.mcp_proxy_shutdown.clone();
    let weak = Arc::downgrade(state);
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval_at(
            tokio::time::Instant::now() + FIRST_SWEEP_DELAY,
            SWEEP_INTERVAL,
        );
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = ticks.tick() => {}
            }
            let Some(state) = weak.upgrade() else { break };
            let cutoff = cutoff(Utc::now(), days);
            let ids = state.session_store.session_archive_candidates(cutoff).await;
            drop(state);
            for (index, batch) in ids.chunks(MAX_BATCH).enumerate() {
                if index > 0 {
                    tokio::select! {
                        _ = shutdown.cancelled() => return,
                        _ = tokio::time::sleep(BETWEEN_BATCHES) => {}
                    }
                }
                let Some(state) = weak.upgrade() else { return };
                for id in batch {
                    if shutdown.is_cancelled() {
                        return;
                    }
                    match maintain(&state, id, cutoff, Operation::AutomaticArchive).await {
                        Ok(Some(Outcome::Archived {
                            original_bytes,
                            archived_bytes,
                        })) => {
                            tracing::info!(session_id = %id, original_bytes, archived_bytes, "Archived idle Session");
                        }
                        Err(error) => {
                            tracing::warn!(session_id = %id, %error, "Session archive skipped")
                        }
                        _ => {}
                    }
                }
            }
        }
    });
}

#[derive(Deserialize)]
pub struct ArchivePreviewQuery {
    #[serde(default)]
    offset: usize,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct ArchivePreviewEntry {
    session_id: String,
    #[serde(flatten)]
    outcome: Outcome,
}

fn error(status: StatusCode, code: &str, message: &str) -> HttpResponse {
    HttpResponse::build(status)
        .json(serde_json::json!({"error":{"type":"api_error","code":code,"message":message}}))
}

fn unauthorized() -> HttpResponse {
    error(
        StatusCode::UNAUTHORIZED,
        "host_authentication_required",
        "Host owner authentication is required.",
    )
}

pub async fn preview_session_archives(
    state: web::Data<AppState>,
    request: HttpRequest,
    query: web::Query<ArchivePreviewQuery>,
) -> HttpResponse {
    if !owner_authorized(&state, &request).await {
        return unauthorized();
    }
    let days = match idle_days() {
        Ok(days) => days,
        Err(error) => return archive_unavailable(error),
    };
    let cutoff = cutoff(Utc::now(), days);
    let ids = state.session_store.session_archive_candidates(cutoff).await;
    let total = ids.len();
    let offset = query.offset.min(total);
    let limit = query.limit.unwrap_or(16).clamp(1, MAX_BATCH);
    let mut entries = Vec::new();
    let mut projected_saved_bytes = 0;
    for id in ids.into_iter().skip(offset).take(limit) {
        let outcome = match maintain(&state, &id, cutoff, Operation::Preview).await {
            Ok(Some(outcome)) => outcome,
            Ok(None) => Outcome::Skipped {
                reason: "session_changed",
            },
            Err(error) => {
                tracing::warn!(session_id = %id, %error, "Session archive preview unavailable");
                Outcome::Skipped {
                    reason: "storage_unavailable",
                }
            }
        };
        if let Outcome::Eligible {
            original_bytes,
            archived_bytes,
        } = &outcome
        {
            projected_saved_bytes += original_bytes - archived_bytes;
        }
        entries.push(ArchivePreviewEntry {
            session_id: id,
            outcome,
        });
    }
    let end = offset + entries.len();
    HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .json(serde_json::json!({
            "idle_days":days,
            "cutoff":cutoff,
            "total_index_candidates":total,
            "projected_saved_bytes":projected_saved_bytes,
            "next_offset":(end < total).then_some(end),
            "sessions":entries,
        }))
}

pub async fn archive_session(
    state: web::Data<AppState>,
    request: HttpRequest,
    id: web::Path<String>,
) -> HttpResponse {
    change_archive(state, request, id.into_inner(), Operation::Archive).await
}

pub async fn restore_session_archive(
    state: web::Data<AppState>,
    request: HttpRequest,
    id: web::Path<String>,
) -> HttpResponse {
    change_archive(state, request, id.into_inner(), Operation::Restore).await
}

async fn change_archive(
    state: web::Data<AppState>,
    request: HttpRequest,
    id: String,
    operation: Operation,
) -> HttpResponse {
    if !owner_authorized(&state, &request).await {
        return unauthorized();
    }
    let cutoff = if operation == Operation::Restore {
        Utc::now()
    } else {
        match idle_days() {
            Ok(days) => cutoff(Utc::now(), days),
            Err(error) => return archive_unavailable(error),
        }
    };
    // Once admitted, the operation owns its locks through durable publication
    // even if the client closes its HTTP request.
    match tokio::spawn(async move { maintain(&state, &id, cutoff, operation).await }).await {
        Ok(Ok(Some(outcome @ Outcome::Skipped { .. }))) => HttpResponse::Conflict().json(outcome),
        Ok(Ok(Some(outcome))) => HttpResponse::Ok().json(outcome),
        Ok(Ok(None)) => error(
            StatusCode::NOT_FOUND,
            "session_not_found",
            "Session not found.",
        ),
        Ok(Err(error)) => archive_unavailable(error),
        Err(error) => archive_unavailable(error),
    }
}

fn archive_unavailable(cause: impl std::fmt::Display) -> HttpResponse {
    tracing::warn!(%cause, "Session archive operation unavailable");
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "session_archive_unavailable",
        "Session storage could not be verified. Check the current state before retrying.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::Method, test, App};
    use bamboo_domain::{Message, Session};
    use serde_json::{json, Value};

    const ID: &str = "idle-archive-http";
    const PREVIEW: &str = "/api/v1/session-storage/archive-preview";
    const ARCHIVE: &str = "/api/v1/sessions/idle-archive-http/storage-archive";
    const HISTORY: &str = "/api/v1/sessions/idle-archive-http/history";

    fn owner(method: Method, uri: &str) -> test::TestRequest {
        test::TestRequest::default()
            .method(method)
            .uri(uri)
            .peer_addr("127.0.0.1:1234".parse().unwrap())
            .insert_header(("Host", "localhost"))
    }

    #[actix_web::test]
    async fn owner_can_preview_archive_read_and_continue_the_same_session_after_restart() {
        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(home.path().into()).await.unwrap());
        let mut original = Session::new(ID, "test-model");
        original.title = "Original title".into();
        original.title_generated = true;
        original.add_message(Message::user("archive-history-marker"));
        original.add_message(Message::assistant("history line\n".repeat(90_000), None));
        original.created_at = Utc::now() - TimeDelta::days(31);
        original.updated_at = original.created_at;
        state.storage.save_session(&original).await.unwrap();

        let main = home.path().join("sessions").join(ID).join("session.json");
        std::fs::File::options()
            .write(true)
            .open(&main)
            .unwrap()
            .set_modified(original.updated_at.into())
            .unwrap();
        let plain = tokio::fs::read(&main).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let remote = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(ARCHIVE)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(remote.status(), StatusCode::UNAUTHORIZED);
        let preview: Value =
            test::call_and_read_body_json(&app, owner(Method::GET, PREVIEW).to_request()).await;
        assert_eq!(preview["idle_days"], 30);
        assert_eq!(preview["total_index_candidates"], 1);
        assert_eq!(preview["sessions"][0]["session_id"], ID);
        assert_eq!(preview["sessions"][0]["status"], "eligible");
        assert_eq!(tokio::fs::read(&main).await.unwrap(), plain);
        let archived: Value =
            test::call_and_read_body_json(&app, owner(Method::POST, ARCHIVE).to_request()).await;
        assert_eq!(archived["status"], "archived");
        let compressed = tokio::fs::read(&main).await.unwrap();
        assert!(compressed.len() < plain.len());
        drop(app);
        drop(state);

        let state = web::Data::new(
            AppState::new_for_legacy_queued_ingress_test(home.path().into())
                .await
                .unwrap(),
        );
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        let history: Value =
            test::call_and_read_body_json(&app, owner(Method::GET, HISTORY).to_request()).await;
        assert_eq!(history["messages"][0]["content"], "archive-history-marker");
        assert_eq!(history["messages"][0]["id"], original.messages[0].id);
        assert_eq!(tokio::fs::read(&main).await.unwrap(), compressed);

        let response = test::call_service(
            &app,
            owner(Method::POST, "/api/v1/chat")
                .set_json(json!({"session_id": ID, "model": "test-model", "message": "continue this same chat"}))
                .to_request(),
        ).await;
        let status = response.status();
        let body = test::read_body(response).await;
        assert!(
            status.is_success(),
            "{status}: {}",
            String::from_utf8_lossy(&body)
        );
        let admitted = state.admit_chat_for_execute(ID).await.unwrap();
        drop(admitted);

        let continued = state.storage.load_session(ID).await.unwrap().unwrap();
        assert_eq!(
            continued.root_mode_birth_token(),
            original.root_mode_birth_token()
        );
        assert!(continued
            .messages
            .iter()
            .any(|message| message.id == original.messages[0].id
                && message.content == original.messages[0].content));
        assert!(continued
            .messages
            .iter()
            .any(|message| message.content == "continue this same chat"));
        let written = tokio::fs::read(&main).await.unwrap();
        assert!(serde_json::from_slice::<Session>(&written).is_ok());
    }
}

#[cfg(all(test, target_os = "macos"))]
#[path = "archive_enospc_tests.rs"]
mod enospc_tests;
