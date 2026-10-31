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
        "recovery":"Reload canonical identity/revision after a conflict. Legacy Project-list updates do not grant or revoke current-user Supervisor management. Worker/Observer rights and Work/security settings remain independent."
    }))
}

// Once canonical publication is issued, an untyped I/O error cannot prove
// no commit occurred. Match the existing native adapter's conservative boundary.
pub(super) fn publication_error(failure: io::Error) -> HttpResponse {
    if failure
        .get_ref()
        .is_some_and(|inner| inner.is::<bamboo_domain::SessionAuthorityConflict>())
    {
        error(StatusCode::CONFLICT, "scope_revision_conflict", failure)
    } else {
        error(StatusCode::SERVICE_UNAVAILABLE, "scope_outcome_unconfirmed", "The scope publication outcome is unknown. Read canonical scope; do not automatically replay the PUT or infer a successful receipt.")
    }
}

pub(super) fn storage_error(error_value: io::Error) -> HttpResponse {
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

pub(super) async fn canonical_reference(
    state: &AppState,
) -> Result<SupervisorReference, Box<HttpResponse>> {
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
    let scope = match state.storage.inspect_supervisor_scope(&reference).await {
        Ok(scope) => scope,
        Err(failure) => return Ok(storage_error(failure)),
    };
    let domain = match state
        .storage
        .inspect_supervisor_current_user_scope(&reference)
        .await
    {
        Ok(scope) => scope,
        Err(failure) => return Ok(storage_error(failure)),
    };
    Ok(HttpResponse::Ok()
        .insert_header((actix_web::http::header::CACHE_CONTROL,"no-store"))
        .insert_header((actix_web::http::header::VARY,"Origin, Cookie, Authorization"))
        .json(json!({
            "code":"current_user_scope_ready",
            "capabilities":{"project_scope_v1":true,"supervised_root_create_v1":true,
                "project_scope_http_write_v1":true,"supervisor_current_user_scope_v1":true},
            "scope":scope,
            "management_access":{"kind":"current_user", "default_all_projects":true,
                "default_all_sessions":true, "requires_project_authorization":false,
                "requires_session_attachment":false, "orchestration_revision":domain.orchestration_revision},
            "owner_authorization":{"available":true,"required":true,"kind":"existing_access","http_registration":false,"code":null},
            "note":"The canonical Supervisor manages this application's current-user Projects/Sessions by default. Legacy scope/link fields are retained metadata, not access restrictions. Worker/Observer target rights, authentication, Work feature initialization and file permissions remain independent."
        })))
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
    if let Some(authorizer) = state.supervisor_project_scope_owner_authorizer.as_ref() {
        // An explicitly installed embedding verifier may add an exact-intent
        // restriction. The normal HTTP path uses the existing request gate above.
        if !authorizer.authorize(&request, &intent) {
            return Ok(error(
                StatusCode::FORBIDDEN,
                "host_owner_authorization_required",
                "The explicit Host verifier rejected this exact scope intent.",
            ));
        }
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
        Ok(receipt) => Ok(HttpResponse::Ok().json(json!({"receipt":receipt,"legacy_scope_is_authorization":false,"management_access":"current_user"}))),
        Err(failure) => Ok(publication_error(failure)),
    }
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    #[actix_web::test]
    async fn only_typed_conflict_is_known_rejected_after_cas_issuance() {
        let typed = publication_error(io::Error::new(
            io::ErrorKind::WouldBlock,
            bamboo_domain::SessionAuthorityConflict("stale scope".into()),
        ));
        assert_eq!(typed.status(), StatusCode::CONFLICT);
        let value: serde_json::Value =
            serde_json::from_slice(&actix_web::body::to_bytes(typed.into_body()).await.unwrap())
                .unwrap();
        assert_eq!(value["code"], "scope_revision_conflict");
        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::InvalidData,
        ] {
            let response = publication_error(io::Error::new(kind, "injected publication I/O"));
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            let value: serde_json::Value = serde_json::from_slice(
                &actix_web::body::to_bytes(response.into_body())
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(value["code"], "scope_outcome_unconfirmed");
            assert!(value.get("receipt").is_none());
            assert!(value.get("not_submitted").is_none());
        }
    }
}
