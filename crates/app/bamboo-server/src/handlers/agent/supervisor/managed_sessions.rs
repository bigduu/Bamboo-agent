//! Owner-selected existing Roots over the canonical Supervisor management ports.
//! A listed relationship is an observation, never a retained execution grant.
use std::{collections::BTreeMap, io};

use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse};
use bamboo_domain::{
    ProjectId, ProjectStatus, SessionKind, SupervisorManagedLink, SupervisorManagementMutation,
    SupervisorManagementRequest, SupervisorReference, SupervisorScopeObservation,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_projects::ProjectStoreError;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::project_scope::{canonical_reference, publication_error, storage_error};
use crate::{app_state::AppState, handlers::agent::tickets};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedSessionUpdate {
    pub incarnation_id: uuid::Uuid,
    pub expected_state_revision: u64,
    pub enabled: bool,
}

#[derive(Serialize)]
struct ManagedSessionView {
    session_id: String,
    link_revision: u64,
    enabled: bool,
    authorized: bool,
    target_created_at: chrono::DateTime<chrono::Utc>,
    target_project_id: ProjectId,
    target_metadata_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    authorization_error: Option<&'static str>,
}

fn error(status: StatusCode, code: &str, message: impl ToString) -> HttpResponse {
    HttpResponse::build(status).json(json!({
        "error":crate::error::error_value(message.to_string()), "code":code,
        "recovery":"Reload the managed Sessions and explicitly select the intended existing Root in its allowed Project. Refresh incarnation/revision after a conflict. An uncertain mutation needs canonical readback, never an automatic retry. This operation starts no task and changes no target permissions."
    }))
}

fn changed_scope() -> HttpResponse {
    error(
        StatusCode::CONFLICT,
        "scope_revision_conflict",
        "Supervisor identity or management scope changed; reload the managed Sessions",
    )
}

fn same_scope(left: &SupervisorScopeObservation, right: &SupervisorScopeObservation) -> bool {
    left.supervisor == right.supervisor
        && left.state_revision == right.state_revision
        && left.allowed_projects == right.allowed_projects
}

async fn management_snapshot(
    state: &AppState,
    reference: &SupervisorReference,
) -> Result<
    (
        SupervisorScopeObservation,
        BTreeMap<String, SupervisorManagedLink>,
    ),
    HttpResponse,
> {
    // The existing reader holds canonical lifetime/Session/proof authority. Do
    // not replace it with the index, raw runtime metadata or a cached Session.
    let current = state
        .storage
        .load_root_authority(&reference.session_id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            storage_error(io::Error::new(
                io::ErrorKind::NotFound,
                "Supervisor is unavailable",
            ))
        })?;
    if current.supervisor_incarnation_id() != Some(reference.incarnation_id) {
        return Err(changed_scope());
    }
    let (revision, projects, links) = if let Some(management) = current.supervisor_management {
        management
            .validate(reference.incarnation_id)
            .map_err(|failure| {
                error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "scope_authority_invalid",
                    failure,
                )
            })?;
        (
            management.revision,
            management.allowed_projects,
            management.links,
        )
    } else {
        (0, Default::default(), BTreeMap::new())
    };
    Ok((
        SupervisorScopeObservation {
            supervisor: reference.clone(),
            state_revision: revision,
            allowed_projects: projects,
        },
        links,
    ))
}

/// Bounded owner-only relationship metadata. No target transcript is returned;
/// current delivery/execution still performs its independent final checks.
pub async fn inspect_managed_sessions(
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
    let (scope, links) = match management_snapshot(&state, &reference).await {
        Ok(snapshot) => snapshot,
        Err(response) => return Ok(response),
    };
    let service = SupervisorSessionService::new(state.storage.clone());
    let observed = match service.inspect_scope(&reference).await {
        Ok(scope) => scope,
        Err(failure) => return Ok(storage_error(failure)),
    };
    if !same_scope(&scope, &observed) {
        return Ok(changed_scope());
    }
    // The canonical management schema bounds this map to 256 entries and its
    // BTreeMap order is stable. Disabled tombstones remain visible/revocable.
    let mut sessions = Vec::with_capacity(links.len());
    for (session_id, link) in links {
        // Attribute only an explicit target-reader failure to this row. An
        // inspect_link error can originate from the Supervisor itself and must
        // fail the whole read. A valid target racing to corruption can cause one
        // whole-read refusal; the next read can diagnose the damaged target.
        let target_unavailable = if link.enabled {
            state
                .storage
                .load_root_authority(&session_id)
                .await
                .is_err()
        } else {
            false
        };
        let authorized = if target_unavailable {
            false
        } else {
            let observed = match service.inspect_link(&reference, &session_id).await {
                Ok(observation) => observation,
                Err(failure) => return Ok(storage_error(failure)),
            };
            if observed.supervisor != reference
                || observed.state_revision != scope.state_revision
                || observed.target_session_id != session_id
                || observed.link.as_ref() != Some(&link)
            {
                return Ok(changed_scope());
            }
            observed.authorized
        };
        sessions.push(ManagedSessionView {
            session_id,
            link_revision: link.revision,
            enabled: link.enabled,
            authorized,
            target_created_at: link.target_created_at,
            target_project_id: link.target_project_id,
            target_metadata_version: link.target_metadata_version,
            authorization_error: target_unavailable.then_some("target_authority_unavailable"),
        });
    }
    let final_scope = match service.inspect_scope(&reference).await {
        Ok(scope) => scope,
        Err(failure) => return Ok(storage_error(failure)),
    };
    if !same_scope(&scope, &final_scope) {
        return Ok(changed_scope());
    }
    Ok(HttpResponse::Ok()
        .insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"))
        .insert_header((actix_web::http::header::VARY, "Origin, Cookie, Authorization"))
        .json(json!({
            "capabilities":{"managed_sessions_http_v1":true}, "scope":scope, "sessions":sessions,
            "owner_authorization":{"available":true,"required":true,"kind":"existing_access","http_registration":false,"code":null}
        })))
}

async fn validate_attach_target(
    state: &AppState,
    target_id: &str,
    scope: &SupervisorScopeObservation,
) -> Result<(), HttpResponse> {
    let target = state
        .storage
        .load_root_authority(target_id)
        .await
        .map_err(|_| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "target_authority_unavailable",
                "The intended target's canonical Root authority could not be verified",
            )
        })?
        .ok_or_else(|| {
            error(
                StatusCode::NOT_FOUND,
                "target_unavailable",
                "The intended independent Root is unavailable",
            )
        })?;
    if target.kind != SessionKind::Root || !target.authority_identity.is_ordinary() {
        return Err(error(
            StatusCode::FORBIDDEN,
            "target_not_ordinary_root",
            "Only an independent Ordinary Root can be managed",
        ));
    }
    let project = target
        .project_id_meta()
        .and_then(|id| id.parse::<ProjectId>().ok())
        .ok_or_else(|| {
            error(
                StatusCode::FORBIDDEN,
                "target_project_required",
                "The intended Root has no valid Project",
            )
        })?;
    if !scope.allowed_projects.contains(&project) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "target_outside_scope",
            "The intended Root's Project is outside the explicitly selected Supervisor scope",
        ));
    }
    let projects = state.project_store.clone();
    match tokio::task::spawn_blocking(move || projects.get(&project)).await {
        Ok(Ok(project)) if project.status == ProjectStatus::Active => Ok(()),
        Ok(Ok(_)) => Err(error(
            StatusCode::CONFLICT,
            "project_archived",
            "The intended Root's Project is archived",
        )),
        Ok(Err(ProjectStoreError::NotFound(_))) => Err(error(
            StatusCode::NOT_FOUND,
            "project_unavailable",
            "The intended Root's Project is unavailable",
        )),
        _ => Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "project_store_unavailable",
            "The intended Root's Project could not be verified",
        )),
    }
}

/// Explicit owner attachment/revalidation or revocation of one existing Root.
/// Target history, model, workspace, permission and runtime are never rewritten.
pub async fn configure_managed_session(
    state: web::Data<AppState>,
    request: HttpRequest,
    target: web::Path<String>,
    body: web::Json<ManagedSessionUpdate>,
) -> actix_web::Result<HttpResponse> {
    tickets::user(&state, &request)
        .await
        .map_err(tickets::TicketHttpError::from)?;
    let target_id = target.into_inner();
    if let Err(failure) = bamboo_domain::validate_supervisor_target_id(&target_id) {
        return Ok(error(StatusCode::BAD_REQUEST, "target_invalid", failure));
    }
    let reference = match canonical_reference(&state).await {
        Ok(reference) => reference,
        Err(response) => return Ok(*response),
    };
    let body = body.into_inner();
    if body.incarnation_id != reference.incarnation_id {
        return Ok(error(
            StatusCode::FORBIDDEN,
            "supervisor_identity_mismatch",
            "Supervisor incarnation changed; reload managed Sessions",
        ));
    }
    if target_id == reference.session_id {
        return Ok(error(
            StatusCode::FORBIDDEN,
            "target_not_ordinary_root",
            "The Supervisor cannot manage itself",
        ));
    }
    let mutation = if body.enabled {
        SupervisorManagementMutation::Attach {
            target_session_id: target_id.clone(),
        }
    } else {
        SupervisorManagementMutation::Detach {
            target_session_id: target_id.clone(),
        }
    };
    let intent = SupervisorManagementRequest {
        supervisor: reference.clone(),
        expected_state_revision: body.expected_state_revision,
        mutation,
    };
    if state
        .supervisor_project_scope_owner_authorizer
        .as_ref()
        .is_some_and(|authorizer| !authorizer.authorize(&request, &intent))
    {
        return Ok(error(
            StatusCode::FORBIDDEN,
            "host_owner_authorization_required",
            "The explicit Host verifier rejected this exact management intent",
        ));
    }
    let service = SupervisorSessionService::new(state.storage.clone());
    let scope = match service.inspect_scope(&reference).await {
        Ok(scope) => scope,
        Err(failure) => return Ok(storage_error(failure)),
    };
    if scope.state_revision != body.expected_state_revision {
        return Ok(changed_scope());
    }
    if body.enabled {
        if let Err(response) = validate_attach_target(&state, &target_id, &scope).await {
            return Ok(response);
        }
    }
    // The storage CAS retains its canonical locks through publication and
    // captures the exact current target birth/Project/metadata for Attach.
    // Detach deliberately never loads a missing or damaged target.
    let result = if body.enabled {
        service
            .attach(&reference, body.expected_state_revision, &target_id)
            .await
    } else {
        service
            .detach(&reference, body.expected_state_revision, &target_id)
            .await
    };
    match result {
        Ok(receipt) => Ok(HttpResponse::Ok().json(json!({"receipt":receipt}))),
        Err(failure) => Ok(publication_error(failure)),
    }
}
