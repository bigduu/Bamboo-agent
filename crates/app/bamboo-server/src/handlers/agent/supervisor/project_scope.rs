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

use super::project_scope_owner;
use crate::{
    app_state::AppState,
    handlers::{agent::tickets, settings::OwnerCredentialStatus},
};

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
        "recovery":"Sign in with existing owner credentials, read the current scope and explicitly choose the complete Project set. Refresh incarnation/revision after a conflict; never auto-grant, attach historical Sessions or change Work/security settings."
    }))
}

fn owner_code(status: OwnerCredentialStatus) -> Option<&'static str> {
    match status {
        OwnerCredentialStatus::Authenticated => None,
        OwnerCredentialStatus::Required => Some("owner_authentication_required"),
        OwnerCredentialStatus::Unconfigured => Some("owner_authentication_unconfigured"),
        OwnerCredentialStatus::Unavailable => Some("owner_authentication_unavailable"),
    }
}
fn owner_error(status: OwnerCredentialStatus) -> Option<HttpResponse> {
    owner_code(status).map(|code|error(if status==OwnerCredentialStatus::Required { StatusCode::UNAUTHORIZED }
        else { StatusCode::SERVICE_UNAVAILABLE },code,"Verified existing owner credentials are required; local/open access is not an authenticated owner. No credential or security setting was changed."))
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
    let owner = if state.supervisor_project_scope_owner_authorizer.is_some() {
        OwnerCredentialStatus::Authenticated
    } else {
        project_scope_owner::credential_status(&state, &request).await
    };
    match SupervisorSessionService::new(state.storage.clone()).inspect_scope(&reference).await {
        Ok(scope) => Ok(HttpResponse::Ok().insert_header((actix_web::http::header::CACHE_CONTROL,"no-store")).insert_header((actix_web::http::header::VARY,"Origin, Cookie, Authorization")).json(json!({
            "code":if scope.allowed_projects.is_empty() {"project_scope_uninitialized"} else {"project_scope_configured"},
            "capabilities":{"project_scope_v1":true,"supervised_root_create_v1":true,"project_scope_http_write_v1":true},
            "scope":scope,
            "owner_authorization":{"available":owner==OwnerCredentialStatus::Authenticated,"required":true,"kind":"authenticated_owner","http_registration":false,"code":owner_code(owner)},
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

    let intent = SupervisorManagementRequest {
        supervisor: reference.clone(),
        expected_state_revision: body.expected_state_revision,
        mutation: SupervisorManagementMutation::ConfigureProjectScope {
            allowed_projects: body.allowed_projects.clone(),
        },
    };
    let web_owner = if let Some(authorizer) =
        state.supervisor_project_scope_owner_authorizer.as_ref()
    {
        // Preserve the existing explicit Rust embedding verifier. Desktop and
        // ordinary browser clients use the same credential-authenticated API below.
        if !authorizer.authorize(&request, &intent) {
            return Ok(error(
                StatusCode::FORBIDDEN,
                "host_owner_authorization_required",
                "The explicit Host verifier rejected this exact scope intent.",
            ));
        }
        false
    } else {
        if let Some(response) =
            owner_error(project_scope_owner::credential_status(&state, &request).await)
        {
            return Ok(response);
        }
        if !project_scope_owner::same_origin_json(&request) {
            return Ok(error(StatusCode::FORBIDDEN,"owner_request_origin_invalid","This owner mutation requires application/json and the exact same or explicitly configured browser Origin."));
        }
        true
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
    if web_owner {
        // Re-read canonical credentials at mutation admission, including rotation,
        // revocation and repair. The earlier observation never confers a grant.
        if let Some(response) =
            owner_error(project_scope_owner::credential_status(&state, &request).await)
        {
            return Ok(response);
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
