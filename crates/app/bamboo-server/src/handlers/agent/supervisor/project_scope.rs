//! Explicit owner HTTP adapter over the existing Host management CAS.
//! This is not a model tool, workspace binding, Ticket initialization or attach.
use std::{collections::BTreeSet, io};

use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse};
use bamboo_domain::{
    ProjectId, ProjectStatus, SupervisorReference, DEFAULT_SUPERVISOR_SESSION_ID,
    MAX_SUPERVISOR_PROJECTS,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_projects::ProjectStoreError;
use serde::Deserialize;
use serde_json::json;

use crate::{app_state::AppState, handlers::agent::tickets};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectScopeUpdate {
    pub incarnation_id: uuid::Uuid,
    pub expected_state_revision: u64,
    pub allowed_projects: BTreeSet<ProjectId>,
}

fn error(status: StatusCode, code: &str, message: impl ToString) -> HttpResponse {
    HttpResponse::build(status).json(json!({
        "error":crate::error::error_value(message.to_string()), "code":code,
        "recovery":"The verified Host owner must read the current scope and explicitly choose the complete Project set. Refresh incarnation/revision after a conflict; never auto-grant, attach historical Sessions or change Work/security settings."
    }))
}

fn storage_error(error_value: io::Error) -> HttpResponse {
    let (status, code) = match error_value.kind() {
        io::ErrorKind::WouldBlock => (StatusCode::CONFLICT, "scope_revision_conflict"),
        io::ErrorKind::PermissionDenied => (StatusCode::FORBIDDEN, "supervisor_identity_mismatch"),
        io::ErrorKind::NotFound => (StatusCode::NOT_FOUND, "supervisor_missing"),
        io::ErrorKind::InvalidInput => (StatusCode::BAD_REQUEST, "project_scope_invalid"),
        io::ErrorKind::InvalidData => (StatusCode::SERVICE_UNAVAILABLE, "scope_authority_invalid"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "scope_unavailable"),
    };
    error(status, code, error_value)
}

/// Observation only: absence does not bootstrap or promote a Supervisor.
pub async fn inspect_project_scope(
    state: web::Data<AppState>,
    request: HttpRequest,
) -> actix_web::Result<HttpResponse> {
    tickets::user(&state, &request)
        .await
        .map_err(tickets::TicketHttpError::from)?;
    let canonical = match state
        .storage
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
    {
        Ok(Some(session)) => session,
        Ok(None) => {
            return Ok(error(
                StatusCode::NOT_FOUND,
                "supervisor_missing",
                "Open the default Supervisor explicitly before inspecting its scope",
            ))
        }
        Err(failure) => return Ok(storage_error(failure)),
    };
    let Some(incarnation_id) = canonical.supervisor_incarnation_id() else {
        return Ok(error(
            StatusCode::FORBIDDEN,
            "supervisor_identity_mismatch",
            "Canonical Supervisor identity required",
        ));
    };
    let reference = SupervisorReference {
        session_id: canonical.id,
        incarnation_id,
    };
    match SupervisorSessionService::new(state.storage.clone()).inspect_scope(&reference).await {
        Ok(scope) => Ok(HttpResponse::Ok().json(json!({
            "code":if scope.allowed_projects.is_empty() {"project_scope_uninitialized"} else {"project_scope_configured"},
            "capabilities":{"project_scope_v1":true,"supervised_root_create_v1":true},
            "scope":scope,
            "note":"This management scope enables only subsequent authorized Root operations. It does not initialize Work, bind a workspace, attach an existing Session or grant shell/build permissions."
        }))),
        Err(failure) => Ok(storage_error(failure)),
    }
}

/// Replace the complete explicitly selected set. The existing storage CAS
/// retains its lifetime/Task/Session fences and revocation/tombstone semantics.
pub async fn configure_project_scope(
    state: web::Data<AppState>,
    request: HttpRequest,
    body: web::Json<ProjectScopeUpdate>,
) -> actix_web::Result<HttpResponse> {
    tickets::user(&state, &request)
        .await
        .map_err(tickets::TicketHttpError::from)?;
    let body = body.into_inner();
    if body.allowed_projects.len() > MAX_SUPERVISOR_PROJECTS {
        return Ok(error(
            StatusCode::BAD_REQUEST,
            "project_scope_invalid",
            "At most 64 Projects may be explicitly selected",
        ));
    }
    let reference = SupervisorReference {
        session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
        incarnation_id: body.incarnation_id,
    };
    let service = SupervisorSessionService::new(state.storage.clone());
    let observed = match service.inspect_scope(&reference).await {
        Ok(scope) => scope,
        Err(failure) => return Ok(storage_error(failure)),
    };
    if observed.state_revision != body.expected_state_revision {
        return Ok(storage_error(io::Error::new(
            io::ErrorKind::WouldBlock,
            "Supervisor management revision changed; explicitly reload the scope",
        )));
    }
    let projects = state.project_store.clone();
    let selected = body.allowed_projects.clone();
    let validation = tokio::task::spawn_blocking(move || {
        for id in selected {
            match projects.get(&id) {
                Ok(project) if project.status == ProjectStatus::Active => {}
                Ok(_) => {
                    return Err((
                        StatusCode::CONFLICT,
                        "project_archived",
                        format!("Project {id} is archived"),
                    ))
                }
                Err(ProjectStoreError::NotFound(_)) => {
                    return Err((
                        StatusCode::NOT_FOUND,
                        "project_unavailable",
                        format!("Project {id} is unavailable"),
                    ))
                }
                Err(failure) => {
                    return Err((
                        StatusCode::SERVICE_UNAVAILABLE,
                        "project_store_unavailable",
                        failure.to_string(),
                    ))
                }
            }
        }
        Ok(())
    })
    .await;
    match validation {
        Ok(Ok(())) => {}
        Ok(Err((status, code, message))) => return Ok(error(status, code, message)),
        Err(failure) => {
            return Ok(error(
                StatusCode::SERVICE_UNAVAILABLE,
                "project_store_unavailable",
                failure,
            ))
        }
    }
    match service
        .configure_project_scope(
            &reference,
            body.expected_state_revision,
            body.allowed_projects,
        )
        .await
    {
        Ok(receipt) => Ok(HttpResponse::Ok().json(json!({"receipt":receipt}))),
        Err(failure) => Ok(storage_error(failure)),
    }
}
