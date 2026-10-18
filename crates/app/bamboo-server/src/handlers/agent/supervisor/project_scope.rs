//! Explicit owner HTTP adapter over the existing Host management CAS.
//! This is not a model tool, workspace binding, Ticket initialization or attach.
use std::{collections::BTreeSet, io};

use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse};
use bamboo_domain::{
    ProjectId, ProjectStatus, SupervisorManagementMutation, SupervisorManagementRequest,
    SupervisorReference, DEFAULT_SUPERVISOR_SESSION_ID, MAX_SUPERVISOR_PROJECTS,
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

async fn canonical_reference(state: &AppState) -> Result<SupervisorReference, Box<HttpResponse>> {
    let canonical =
        match state
            .storage
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
        {
            Ok(Some(session)) => session,
            Ok(None) => return Err(Box::new(error(
                StatusCode::NOT_FOUND,
                "supervisor_missing",
                "Open the default Supervisor explicitly before inspecting or updating its scope",
            ))),
            Err(failure) => return Err(Box::new(storage_error(failure))),
        };
    let Some(incarnation_id) = canonical.supervisor_incarnation_id() else {
        return Err(Box::new(error(
            StatusCode::FORBIDDEN,
            "supervisor_identity_mismatch",
            "Canonical Supervisor identity required",
        )));
    };
    Ok(SupervisorReference {
        session_id: canonical.id,
        incarnation_id,
    })
}

/// Observation only under the existing owner-read transport policy. Absence
/// does not bootstrap/promote identity; it provides no mutation authorization.
pub async fn inspect_project_scope(
    state: web::Data<AppState>,
    request: HttpRequest,
) -> actix_web::Result<HttpResponse> {
    tickets::user(&state, &request)
        .await
        .map_err(tickets::TicketHttpError::from)?;
    let reference = match canonical_reference(&state).await {
        Ok(reference) => reference,
        Err(response) => return Ok(*response),
    };
    match SupervisorSessionService::new(state.storage.clone()).inspect_scope(&reference).await {
        Ok(scope) => Ok(HttpResponse::Ok().json(json!({
            "code":if scope.allowed_projects.is_empty() {"project_scope_uninitialized"} else {"project_scope_configured"},
            "capabilities":{"project_scope_v1":true,"supervised_root_create_v1":true},
            "scope":scope,
            "owner_authorization":{"available":state.supervisor_project_scope_owner_authorizer.is_some(),"required":true,"kind":"trusted_host_intent","http_registration":false},
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
    let reference = match canonical_reference(&state).await {
        Ok(reference) => reference,
        Err(response) => return Ok(*response),
    };
    if body.incarnation_id != reference.incarnation_id {
        return Ok(error(
            StatusCode::FORBIDDEN,
            "supervisor_identity_mismatch",
            "Supervisor incarnation changed; read its current scope",
        ));
    }
    let Some(authorizer) = state.supervisor_project_scope_owner_authorizer.as_ref() else {
        return Ok(error(StatusCode::SERVICE_UNAVAILABLE,"host_owner_authorization_unavailable",
            "The Host has not installed a trusted owner authorization channel. Use the existing trusted Rust Host service; do not enable settings, mint HTTP credentials or infer consent from loopback access."));
    };
    let intent = SupervisorManagementRequest {
        supervisor: reference.clone(),
        expected_state_revision: body.expected_state_revision,
        mutation: SupervisorManagementMutation::ConfigureProjectScope {
            allowed_projects: body.allowed_projects.clone(),
        },
    };
    if !authorizer.authorize(&request, &intent) {
        return Ok(error(StatusCode::FORBIDDEN,"host_owner_authorization_required",
            "Explicit Host owner consent for this exact Supervisor, revision and complete Project set is required. Local bypass and run/device/access credentials alone do not authorize it."));
    }
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
