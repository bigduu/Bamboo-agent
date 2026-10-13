//! Authenticated single-owner HTTP projections over the same Ticket service.
use actix_web::{http::StatusCode, web, HttpMessage, HttpRequest, HttpResponse, ResponseError};
use bamboo_engine::ticket_worker_plan::tickets::*;
use serde::Deserialize;

use crate::{
    app_state::AppState,
    handlers::settings::{bootstrap_access_snapshot, BootstrapRequestState},
};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TicketHttpError(#[from] Error);

impl ResponseError for TicketHttpError {
    fn status_code(&self) -> StatusCode {
        StatusCode::from_u16(self.0.status_code()).unwrap_or(StatusCode::SERVICE_UNAVAILABLE)
    }
    fn error_response(&self) -> HttpResponse {
        crate::error::json_error(self.status_code(), self.0.to_string())
    }
}

pub(crate) async fn user(state: &AppState, req: &HttpRequest) -> Result<Principal> {
    let config = state.config.read().await;
    // A run-scoped Worker credential cannot be promoted to a User by reaching
    // this endpoint. Remote open-instance traffic also needs verified access.
    if req
        .extensions()
        .get::<crate::codex_run_tokens::CodexRunAuthContext>()
        .is_some()
        || bootstrap_access_snapshot(&config, req).request_state
            == BootstrapRequestState::Unauthenticated
    {
        return Err(Error::ScopeDenied("verified owner access required".into()));
    }
    Ok(Principal::User {
        user_id: "host-owner".into(),
    })
}

/// Existing canonical Supervisor read access stays owner-only after mutation rollback.
pub(crate) async fn require_supervisor_owner(
    state: &AppState,
    req: &HttpRequest,
    session_id: &str,
) -> Result<()> {
    let authority = state
        .storage
        .load_root_authority(session_id)
        .await
        .map_err(|e| Error::AuthorityUnavailable(e.to_string()))?;
    if authority.is_some_and(|session| {
        matches!(
            session.authority_identity,
            bamboo_domain::SessionAuthorityIdentity::Supervisor { .. }
        )
    }) {
        user(state, req).await?;
    }
    Ok(())
}

pub async fn scope(
    state: web::Data<AppState>,
    req: HttpRequest,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    user(&state, &req).await?;
    Ok(HttpResponse::Ok().json(state.tickets.status().await))
}

pub async fn overview(
    state: web::Data<AppState>,
    req: HttpRequest,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    let (service, authority) = state.tickets.authority(user(&state, &req).await?).await?;
    Ok(HttpResponse::Ok().json(service.work_overview(&authority)?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    #[serde(default)]
    pub filter: SearchFilter,
    pub limit: usize,
    pub cursor: Option<ReadCursor>,
    pub fixed_commit: Option<String>,
}

pub async fn search(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<SearchRequest>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    let (service, authority) = state.tickets.authority(user(&state, &req).await?).await?;
    Ok(HttpResponse::Ok().json(service.work_search(
        &authority,
        &body.filter,
        body.limit,
        body.cursor.as_ref(),
        body.fixed_commit.as_deref(),
    )?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectRequest {
    pub ids: Vec<String>,
    pub depth: usize,
    pub budget_bytes: usize,
    pub fixed_commit: Option<String>,
    #[serde(default)]
    pub sections: Option<std::collections::BTreeSet<InspectSection>>,
}
impl InspectRequest {
    pub fn options(&self) -> InspectOptions {
        InspectOptions {
            sections: self.sections.clone().unwrap_or_else(all_inspect_sections),
            depth: self.depth,
            budget_bytes: self.budget_bytes,
            fixed_commit: self.fixed_commit.clone(),
        }
    }
}

pub async fn inspect(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<InspectRequest>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    let (service, authority) = state.tickets.authority(user(&state, &req).await?).await?;
    Ok(HttpResponse::Ok().json(service.work_inspect_sections(
        &authority,
        &body.ids,
        &body.options(),
    )?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangesRequest {
    pub since_seq: u64,
    pub limit: usize,
    pub cursor: Option<ReadCursor>,
}

pub async fn changes(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<ChangesRequest>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    let (service, authority) = state.tickets.authority(user(&state, &req).await?).await?;
    Ok(HttpResponse::Ok().json(service.work_changes(
        &authority,
        body.since_seq,
        body.limit,
        body.cursor.as_ref(),
    )?))
}

/// Deliberately excludes Host-only adapter source and Authority fields.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateRequest {
    pub operation_id: String,
    pub binding: ScopeBinding,
    pub expected_seq: u64,
    pub expected_epoch: u64,
    pub operations: Vec<Operation>,
}

impl UpdateRequest {
    pub fn command(self) -> Command {
        Command {
            operation_id: self.operation_id,
            binding: self.binding,
            expected_seq: self.expected_seq,
            expected_epoch: self.expected_epoch,
            operations: self.operations,
            source: None,
        }
    }
}

pub async fn update(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<UpdateRequest>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    Ok(HttpResponse::Ok().json(
        state
            .tickets
            .update(user(&state, &req).await?, &body.into_inner().command())
            .await?,
    ))
}

pub async fn dispatch(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<UpdateRequest>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    Ok(HttpResponse::Ok().json(
        state
            .tickets
            .dispatch(user(&state, &req).await?, &body.into_inner().command())
            .await?,
    ))
}

pub async fn dispatch_query(
    state: web::Data<AppState>,
    req: HttpRequest,
    key: web::Path<String>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    user(&state, &req).await?;
    Ok(HttpResponse::Ok().json(state.tickets.query_dispatch(&key).await?))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestResponse {
    pub operation_id: String,
    pub binding: ScopeBinding,
    pub expected_seq: u64,
    pub expected_epoch: u64,
    pub target: RequestReference,
    pub decision: RequestDecision,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RequestDecision {
    Question { answer: String },
    Approval { fingerprint: String, approve: bool },
}

/// Negotiated buttons carry the entire immutable request binding. The same
/// User service checks current Work versions, status and fingerprint under
/// CAS; exact successful retries retain their original receipt.
pub async fn respond(
    state: web::Data<AppState>,
    req: HttpRequest,
    body: web::Json<RequestResponse>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    let principal = user(&state, &req).await?;
    let (service, _) = state.tickets.authority(principal.clone()).await?;
    let snapshot = service.published()?.1;
    let q = snapshot
        .requests
        .get(&body.target.request_id)
        .ok_or_else(|| Error::ScopeDenied("request not in this scope".into()))?;
    let t = &body.target;
    if q.work_id != t.work_id
        || q.assignment_id != t.assignment_id
        || q.generation != t.generation
        || q.contract_revision != t.contract_revision
        || q.prompt_revision != t.prompt_revision
    {
        return Err(Error::RevisionConflict.into());
    }
    let operation = match (&body.decision, &q.kind) {
        (RequestDecision::Question { answer }, RequestKind::Question) => Operation::Answer {
            request_id: q.id.clone(),
            prompt_revision: t.prompt_revision,
            answer: answer.clone(),
        },
        (
            RequestDecision::Approval {
                fingerprint,
                approve,
            },
            RequestKind::Approval {
                fingerprint: expected,
                ..
            },
        ) if fingerprint == expected => Operation::DecideApproval {
            request_id: q.id.clone(),
            prompt_revision: t.prompt_revision,
            fingerprint: fingerprint.clone(),
            approve: *approve,
        },
        _ => {
            return Err(Error::ScopeDenied(
                "decision kind or exact action fingerprint changed".into(),
            )
            .into())
        }
    };
    let command = Command {
        operation_id: body.operation_id.clone(),
        binding: body.binding.clone(),
        expected_seq: body.expected_seq,
        expected_epoch: body.expected_epoch,
        operations: vec![operation],
        source: None,
    };
    Ok(HttpResponse::Ok().json(state.tickets.update(principal, &command).await?))
}

pub async fn artifact(
    state: web::Data<AppState>,
    req: HttpRequest,
    hash: web::Path<String>,
) -> std::result::Result<HttpResponse, TicketHttpError> {
    let (service, authority) = state.tickets.authority(user(&state, &req).await?).await?;
    let artifact = Artifact {
        uri: format!("{}{}", store::MANAGED_ARTIFACT_PREFIX, hash),
        sha256: hash.into_inner(),
    };
    Ok(HttpResponse::Ok()
        .content_type("application/octet-stream")
        .body(service.read_artifact(&authority, &artifact, store::MAX_ARTIFACT_BYTES)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn rollback_keeps_supervisor_chat_and_execute_owner_only() {
        use actix_web::{test, App};
        use std::sync::Arc;
        let root = tempfile::tempdir().unwrap();
        let mut state = AppState::new(root.path().to_path_buf()).await.unwrap();
        *state.config.write().await = serde_json::from_value(serde_json::json!({
            "provider":"openai", "features":{"ticket_mutation":true},
            "providers":{"openai":{"api_key":"fixture","model":"fixture-model"}}
        }))
        .unwrap();
        state.tickets = Arc::new(
            crate::app_state::ticket_application::TicketApplication::open(
                root.path(),
                state.storage.clone(),
                state.config.clone(),
            )
            .await,
        );
        let supervisor = bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID;
        let service = state.tickets.service().unwrap();
        let before = serde_json::to_value(service.published().unwrap()).unwrap();
        let session_before =
            serde_json::to_value(state.storage.load_session(supervisor).await.unwrap()).unwrap();
        state.config.write().await.features.ticket_mutation = false;
        let state = web::Data::new(state);
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(crate::routes::configure_routes),
        )
        .await;
        for execute in [false, true] {
            let uri = if execute {
                format!("/api/v1/execute/{supervisor}")
            } else {
                "/api/v1/chat".into()
            };
            let body = if execute {
                serde_json::json!({})
            } else {
                serde_json::json!({
                    "session_id":supervisor, "message":"read private Ticket evidence", "model":"fixture-model"
                })
            };
            let response = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri(&uri)
                    .peer_addr("203.0.113.17:5700".parse().unwrap())
                    .insert_header(("Idempotency-Key", "rollback-unauthenticated"))
                    .set_json(body)
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            let body: serde_json::Value = test::read_body_json(response).await;
            assert!(body["error"].is_object(), "{body}");
        }
        assert_eq!(
            serde_json::to_value(service.published().unwrap()).unwrap(),
            before
        );
        assert_eq!(
            serde_json::to_value(state.storage.load_session(supervisor).await.unwrap()).unwrap(),
            session_before
        );
        assert!(state
            .session_inbox
            .claim(supervisor, 10)
            .await
            .unwrap()
            .is_empty());
        // Rollback does not disable owner access or change ordinary open-instance chat.
        for (session_id, peer) in [
            (supervisor, "127.0.0.1:5700"),
            ("ordinary-rollback", "203.0.113.17:5700"),
        ] {
            let response = test::call_service(&app, test::TestRequest::post().uri("/api/v1/chat")
                .peer_addr(peer.parse().unwrap()).insert_header(("Idempotency-Key", format!("owner-{session_id}"))).set_json(serde_json::json!({
                    "session_id":session_id, "message":"ordinary owner input", "model":"fixture-model"
                })).to_request()).await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }
        let owner_session =
            serde_json::to_value(state.storage.load_session(supervisor).await.unwrap()).unwrap();
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/chat")
                .peer_addr("203.0.113.17:5700".parse().unwrap())
                .insert_header(("Idempotency-Key", format!("owner-{supervisor}")))
                .set_json(serde_json::json!({"session_id":supervisor,
                "message":"ordinary owner input", "model":"fixture-model"}))
                .to_request(),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::FORBIDDEN,
            "cached owner response cannot bypass auth"
        );
        assert_eq!(
            serde_json::to_value(state.storage.load_session(supervisor).await.unwrap()).unwrap(),
            owner_session
        );
        assert_eq!(
            serde_json::to_value(service.published().unwrap()).unwrap(),
            before
        );
    }

    #[actix_web::test]
    async fn managed_binary_artifact_http_preserves_bytes_without_guessing_media_type() {
        use actix_web::{test, App};
        use std::{collections::BTreeSet, sync::Arc};

        let root = tempfile::tempdir().unwrap();
        let mut state = AppState::new(root.path().to_path_buf()).await.unwrap();
        {
            let mut config = state.config.write().await;
            *config = serde_json::from_value(serde_json::json!({
                "provider": "openai", "features": {"ticket_mutation": true},
                "providers": {"openai": {"api_key": "fixture", "model": "fixture-model"}}
            }))
            .unwrap();
        }
        state.tickets = Arc::new(
            crate::app_state::ticket_application::TicketApplication::open(
                root.path(),
                state.storage.clone(),
                state.config.clone(),
            )
            .await,
        );
        assert!(!state.config.read().await.features.ticket_dispatch);
        let (service, runtime) = state.tickets.authority(Principal::Runtime).await.unwrap();
        let user = Authority::from_verified_host(
            service.published().unwrap().1.binding,
            Principal::User {
                user_id: "host-owner".into(),
            },
        );
        let create = service
            .prepare_command(
                &user,
                "binary-create",
                vec![
                    Operation::Create {
                        temp_id: "work".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: Contract {
                            title: "binary result".into(),
                            objective: "preserve bytes".into(),
                            constraints: vec![],
                            acceptance: vec!["exact bytes".into()],
                            user_acceptance_required: true,
                            allowed_tools: BTreeSet::from(["Task".into()]),
                        },
                        depends_on: BTreeSet::new(),
                    },
                    Operation::Ready {
                        work_id: "work".into(),
                    },
                    Operation::Start {
                        work_id: "work".into(),
                        temp_id: "assignment".into(),
                        workspace: None,
                    },
                ],
            )
            .unwrap();
        let ids = service.execute(&user, &create).unwrap().ids;
        let snapshot = service.published().unwrap().1;
        let assignment = &snapshot.assignments[&ids["assignment"]];
        let receipt = RuntimeReceipt {
            dispatch_key: assignment.dispatch_key.clone(),
            spec_hash: snapshot.intents[&assignment.dispatch_key].spec_hash.clone(),
            run_id: "fixture-run".into(),
            session_id: "fixture-child".into(),
        };
        let admit = service
            .prepare_command(
                &runtime,
                "binary-admit",
                vec![
                    Operation::Admitted {
                        assignment_id: assignment.id.clone(),
                        receipt: receipt.clone(),
                    },
                    Operation::Running {
                        assignment_id: assignment.id.clone(),
                    },
                ],
            )
            .unwrap();
        service.execute(&runtime, &admit).unwrap();
        let bytes = b"%PDF-1.7\n\0\xff\x80binary output";
        let artifact = service.store_artifact(&runtime, bytes).unwrap();
        let worker = Authority::from_verified_host(
            snapshot.binding,
            Principal::Worker {
                assignment_id: assignment.id.clone(),
                generation: assignment.generation,
                run_id: receipt.run_id,
                session_id: receipt.session_id,
            },
        );
        let submit = service
            .prepare_command(
                &worker,
                "binary-submit",
                vec![Operation::Submit {
                    assignment_id: assignment.id.clone(),
                    temp_id: "submission".into(),
                    artifacts: vec![artifact.clone()],
                    evidence: vec!["verified output".into()],
                }],
            )
            .unwrap();
        service.execute(&worker, &submit).unwrap();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .route("/artifact/{hash}", web::get().to(super::artifact)),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&format!("/artifact/{}", artifact.sha256))
                .peer_addr("127.0.0.1:12345".parse().unwrap())
                .insert_header(("Host", "localhost"))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("Content-Type").unwrap(),
            "application/octet-stream"
        );
        assert_eq!(test::read_body(response).await.as_ref(), bytes);
    }

    #[actix_web::test]
    async fn ticket_error_envelope_preserves_exact_conflict_and_authority_semantics() {
        let cases = [
            (
                Error::RevisionConflict,
                StatusCode::CONFLICT,
                "revision_conflict",
            ),
            (
                Error::IdempotencyConflict,
                StatusCode::CONFLICT,
                "idempotency_conflict",
            ),
            (
                Error::ScopeDenied("owner required".into()),
                StatusCode::FORBIDDEN,
                "scope_denied: owner required",
            ),
            (
                Error::InvalidTransition("stale request".into()),
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_transition: stale request",
            ),
            (
                Error::ResourceBlocked("live run".into()),
                StatusCode::LOCKED,
                "resource_blocked: live run",
            ),
            (
                Error::AuthorityUnavailable("verify HEAD".into()),
                StatusCode::SERVICE_UNAVAILABLE,
                "authority_unavailable: verify HEAD",
            ),
            (Error::ResyncRequired, StatusCode::GONE, "resync_required"),
        ];
        for (error, status, message) in cases {
            let response = TicketHttpError(error).error_response();
            assert_eq!(response.status(), status);
            let bytes = actix_web::body::to_bytes(response.into_body())
                .await
                .unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"]["type"], "api_error");
            assert_eq!(body["error"]["message"], message);
        }
    }

    #[test]
    fn ticket_http_cannot_select_authority_or_private_worker_adapter_source() {
        let mut request = serde_json::json!({"operation_id":"op","binding":{"scope_id":"scope","supervisor_session_id":"supervisor","binding_revision":1},"expected_seq":1,"expected_epoch":1,"operations":[]});
        assert!(serde_json::from_value::<UpdateRequest>(request.clone()).is_ok());
        for field in ["authority", "principal", "approved", "source"] {
            request[field] = serde_json::json!(true);
            assert!(serde_json::from_value::<UpdateRequest>(request.clone()).is_err());
            request.as_object_mut().unwrap().remove(field);
        }
    }
}
