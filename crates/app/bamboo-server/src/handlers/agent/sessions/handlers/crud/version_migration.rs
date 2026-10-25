//! Owner-requested compatible-session migration; no provider or model tool.
use crate::app_state::{AgentStatus, AppState};
use actix_web::{http::StatusCode, web, HttpMessage, HttpRequest, HttpResponse};
use bamboo_storage::v2::{SessionVersionMigrationDecision, SessionVersionMigrationRequest};

fn error(status: StatusCode, code: &str, message: &str) -> HttpResponse {
    HttpResponse::build(status)
        .insert_header(("Cache-Control", "no-store"))
        .json(serde_json::json!({"error":{"type":"api_error","code":code,"message":message}}))
}

async fn process_busy(state: &AppState, id: &str) -> bool {
    if crate::handlers::agent::events::execute_startup_is_in_flight(state, id) {
        return true;
    }
    let runner = state.agent_runners.read().await.get(id).cloned();
    match runner {
        Some(runner) => {
            matches!(runner.status, AgentStatus::Pending | AgentStatus::Running)
                || runner.root_reset_admission_reserved()
                || state
                    .session_activation_router
                    .owns_run(id, &runner.run_id)
                    .await
        }
        None => false,
    }
}

async fn owner_authorized(state: &web::Data<AppState>, request: &HttpRequest) -> bool {
    // Worker middleware may remove the original header after verifying it.
    // Its retained run context must never become Host-owner access on loopback.
    if request
        .extensions()
        .get::<crate::codex_run_tokens::CodexRunAuthContext>()
        .is_some()
    {
        return false;
    }
    crate::handlers::agent::actor_snapshot::host_owner_authorized(state, request).await
}

pub async fn inspect_session_version_migration(
    state: web::Data<AppState>,
    id: web::Path<String>,
    request: HttpRequest,
) -> HttpResponse {
    if !owner_authorized(&state, &request).await {
        return error(
            StatusCode::UNAUTHORIZED,
            "host_authentication_required",
            "Host owner authentication is required.",
        );
    }
    let id = id.into_inner();
    let _persistence = state.persistence.acquire_lock(&id).await;
    match state
        .session_store
        .inspect_session_version_migration(&id)
        .await
    {
        Ok(Some(mut observation)) => {
            if process_busy(&state, &id).await {
                observation.eligible = false;
                observation
                    .blockers
                    .push(bamboo_storage::v2::SessionVersionMigrationBlocker {
                        code: "execution_owner_present".into(),
                        message: "Wait for the current execution owner to retire.".into(),
                    });
            }
            HttpResponse::Ok()
                .insert_header(("Cache-Control", "no-store"))
                .json(observation)
        }
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            "session_not_found",
            "Session not found.",
        ),
        Err(cause) => {
            tracing::warn!(session_id=%id, %cause, "migration readiness is unavailable");
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "migration_authority_unavailable",
                "Migration authority cannot currently be verified.",
            )
        }
    }
}

pub async fn migrate_session_version(
    state: web::Data<AppState>,
    id: web::Path<String>,
    request: HttpRequest,
    body: web::Json<SessionVersionMigrationRequest>,
) -> HttpResponse {
    if !owner_authorized(&state, &request).await {
        return error(
            StatusCode::UNAUTHORIZED,
            "host_authentication_required",
            "Host owner authentication is required.",
        );
    }
    if !body.valid() {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid_session_version_migration",
            "Provide the exact canonical readiness token and operation UUID.",
        );
    }
    let id = id.into_inner();
    let intent = body.into_inner();
    // The owned request survives an HTTP timeout. The same per-session lock
    // serializes chat and execute startup registration with migration.
    let owner = tokio::spawn(async move {
        let _persistence = state.persistence.acquire_lock(&id).await;
        if process_busy(&state, &id).await {
            return Ok(SessionVersionMigrationDecision::Blocked(vec![
                bamboo_storage::v2::SessionVersionMigrationBlocker {
                    code: "execution_owner_present".into(),
                    message: "Wait for the current execution owner to retire.".into(),
                },
            ]));
        }
        let decision = state
            .session_store
            .migrate_session_version(&id, &intent)
            .await?;
        if matches!(decision, SessionVersionMigrationDecision::Committed(_)) {
            state.sessions.remove(&id);
        }
        Ok::<_, std::io::Error>(decision)
    });
    match owner.await {
        Ok(Ok(SessionVersionMigrationDecision::Committed(receipt))) => HttpResponse::Ok().insert_header(("Cache-Control", "no-store")).json(receipt),
        Ok(Ok(SessionVersionMigrationDecision::Blocked(blockers))) => HttpResponse::Conflict().json(serde_json::json!({"error":{"type":"api_error","code":"session_version_migration_blocked","message":"Resolve the reported blockers before migration."},"blockers":blockers})),
        Ok(Ok(SessionVersionMigrationDecision::PreconditionFailed)) => error(StatusCode::PRECONDITION_FAILED, "session_version_migration_precondition_failed", "The Session changed; read readiness before creating a new intent."),
        Ok(Ok(SessionVersionMigrationDecision::OperationConflict)) => error(StatusCode::CONFLICT, "session_version_migration_operation_conflict", "This operation ID or Session lifetime already belongs to another migration intent."),
        Ok(Ok(SessionVersionMigrationDecision::NotFound)) => error(StatusCode::NOT_FOUND, "session_not_found", "Session not found."),
        result => {
            tracing::warn!(?result, "migration outcome is unconfirmed; preserve the exact intent");
            error(StatusCode::SERVICE_UNAVAILABLE, "session_version_migration_outcome_unconfirmed", "Preserve this exact operation intent and confirm its result before starting another migration.")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    use bamboo_domain::{Message, Session};
    use serde_json::{json, Value};

    #[actix_web::test]
    async fn version_migration_http_owner_only_preserves_history_and_returns_current_version() {
        let home = tempfile::tempdir().unwrap();
        let state = web::Data::new(AppState::new(home.path().into()).await.unwrap());
        let mut legacy = Session::new("migration-http", "kept-model");
        legacy.session_version = None;
        legacy.add_message(Message::user("Keep this exact history."));
        state.storage.save_session(&legacy).await.unwrap();
        let app = test::init_service(
            App::new().app_data(state.clone()).service(
                web::resource("/sessions/{id}/version-migration")
                    .route(web::get().to(inspect_session_version_migration))
                    .route(web::post().to(migrate_session_version)),
            ),
        )
        .await;
        let uri = "/sessions/migration-http/version-migration";
        let remote = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(remote.status(), StatusCode::UNAUTHORIZED);
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(uri)
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header(("Host", "localhost"))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let view: Value = test::read_body_json(response).await;
        assert_eq!(view["eligible"], true);
        let intent = json!({"operation_id":uuid::Uuid::new_v4().to_string(),"birth_token":view["birth_token"],
            "expected_session_version":view["session_version"],"expected_metadata_version":view["metadata_version"],
            "expected_snapshot_sha256":view["snapshot_sha256"]});
        let denied = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(uri)
                .set_json(&intent)
                .peer_addr("203.0.113.9:1234".parse().unwrap())
                .to_request(),
        )
        .await;
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let directory = home.path().join("sessions").join(&legacy.id);
        let mut original_pair = Vec::new();
        for name in ["session.json", "runtime.json"] {
            original_pair.push(tokio::fs::read(directory.join(name)).await.unwrap());
        }
        // Independently mounted handlers must reject a verified Worker even
        // after middleware consumed its bearer header on loopback.
        for post in [false, true] {
            let request = if post {
                test::TestRequest::post().set_json(&intent)
            } else {
                test::TestRequest::get()
            }
            .uri(uri)
            .peer_addr("127.0.0.1:1234".parse().unwrap())
            .insert_header(("Host", "localhost"))
            .to_request();
            request
                .extensions_mut()
                .insert(crate::codex_run_tokens::CodexRunAuthContext {
                    session_id: legacy.id.clone(),
                });
            let denied = test::call_service(&app, request).await;
            assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        }
        let mut retained_pair = Vec::new();
        for name in ["session.json", "runtime.json"] {
            retained_pair.push(tokio::fs::read(directory.join(name)).await.unwrap());
        }
        assert_eq!(retained_pair, original_pair);
        assert!(!directory.join("session-version-migration.json").exists());
        assert!(
            tokio::fs::read_dir(home.path().join(".session-version-migrations"))
                .await
                .unwrap()
                .next_entry()
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            state
                .storage
                .load_session(&legacy.id)
                .await
                .unwrap()
                .unwrap()
                .session_version,
            None
        );
        let migrated = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(uri)
                .set_json(&intent)
                .peer_addr("127.0.0.1:1234".parse().unwrap())
                .insert_header(("Host", "localhost"))
                .to_request(),
        )
        .await;
        assert_eq!(migrated.status(), StatusCode::OK);
        let receipt: Value = test::read_body_json(migrated).await;
        assert_eq!(receipt["status"], "committed");
        assert_eq!(receipt["target_version"], 2);
        let loaded = state
            .storage
            .load_session(&legacy.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&loaded.messages).unwrap(),
            serde_json::to_value(&legacy.messages).unwrap()
        );
        assert_eq!(loaded.model, legacy.model);
        assert_eq!(loaded.created_at, legacy.created_at);
        assert_eq!(loaded.session_version, Some(2));
    }
}
