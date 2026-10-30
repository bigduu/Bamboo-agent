//! Current-user discovery plus retained legacy link annotations.
//! A listed selector is an observation, never a retained execution grant.
use std::{collections::BTreeMap, io};

use actix_web::{http::StatusCode, web, HttpRequest, HttpResponse};
use bamboo_domain::{
    ProjectId, ProjectStatus, SessionKind, SupervisorCurrentUserTarget, SupervisorManagedLink,
    SupervisorManagementMutation, SupervisorManagementRequest, SupervisorReference,
    SupervisorScopeObservation,
};
use bamboo_engine::session_app::supervisor::SupervisorSessionService;
use bamboo_projects::ProjectStoreError;
use serde::Deserialize;
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

fn error(status: StatusCode, code: &str, message: impl ToString) -> HttpResponse {
    HttpResponse::build(status).json(json!({
        "error":crate::error::error_value(message.to_string()), "code":code,
        "recovery":"Refresh canonical incarnation/revision after a legacy annotation conflict. Default Supervisor current-user access needs no Project authorization or attachment. An uncertain mutation needs canonical readback, never an automatic retry. This operation starts no task and changes no target permissions."
    }))
}

fn changed_scope() -> HttpResponse {
    error(
        StatusCode::CONFLICT,
        "scope_revision_conflict",
        "Supervisor identity or management scope changed; reload the managed Sessions",
    )
}

async fn management_snapshot(
    state: &AppState,
    reference: &SupervisorReference,
) -> Result<
    (
        SupervisorScopeObservation,
        BTreeMap<String, SupervisorManagedLink>,
    ),
    Box<HttpResponse>,
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
        return Err(changed_scope().into());
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

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentUserDirectoryQuery {
    query: Option<String>,
    after: Option<String>,
    limit: Option<usize>,
}

/// The authenticated user's default management directory. Legacy links remain
/// observable separately; they cannot restrict or grant the current role.
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
    let query = match web::Query::<CurrentUserDirectoryQuery>::from_query(request.query_string()) {
        Ok(query) => query.into_inner(),
        Err(failure) => {
            return Ok(error(
                StatusCode::BAD_REQUEST,
                "invalid_directory_query",
                failure,
            ))
        }
    };
    let page = match state
        .storage
        .list_supervisor_current_user_targets(
            &reference,
            query.query.as_deref(),
            query.after.as_deref(),
            query.limit.unwrap_or(20),
        )
        .await
    {
        Ok(page) => page,
        Err(failure) => return Ok(storage_error(failure)),
    };
    let (scope, links) = match management_snapshot(&state, &reference).await {
        Ok(snapshot) => snapshot,
        Err(response) => return Ok(*response),
    };
    if scope.state_revision != page.scope.legacy_state_revision {
        return Ok(changed_scope());
    }
    let sessions: Vec<_> = page.targets.into_iter().map(|target| json!({
        "session_id":target.session_id,"kind":target.kind,"root_session_id":target.root_session_id,
        "parent_session_id":target.parent_session_id,"link_revision":0,
        "enabled":true,"authorized":true,"target_created_at":target.created_at,
        "target_project_id":target.project_id,"target_metadata_version":target.metadata_version,
        "current_user_target":target,"authorization_error":null,
    })).collect();
    let after = match state
        .storage
        .inspect_supervisor_current_user_scope(&reference)
        .await
    {
        Ok(after) => after,
        Err(failure) => return Ok(storage_error(failure)),
    };
    if after != page.scope {
        return Ok(changed_scope());
    }
    Ok(HttpResponse::Ok()
        .insert_header((actix_web::http::header::CACHE_CONTROL,"no-store"))
        .insert_header((actix_web::http::header::VARY,"Origin, Cookie, Authorization"))
        .json(json!({
            "capabilities":{"managed_sessions_http_v1":true,"supervisor_current_user_scope_v1":true,
                "current_user_bound_history_v1":state.storage.supports_supervisor_current_user_bound_history()},
            "scope":scope,"sessions":sessions,"next_after":page.next_after,"legacy_links":links,
            "management_access":{"kind":"current_user","requires_session_attachment":false},
            "owner_authorization":{"available":true,"required":true,"kind":"existing_access","http_registration":false,"code":null}
        })))
}

fn history_error(status: StatusCode, code: &str, message: &str) -> HttpResponse {
    HttpResponse::build(status).json(json!({
        "error": crate::error::error_value(message),
        "code": code,
        "recovery": "Refresh the Supervisor directory and explicitly select the target again.",
    }))
}

/// An observation selects a snapshot; it never supplies authorization.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundHistoryRequest {
    supervisor: SupervisorReference,
    target: SupervisorCurrentUserTarget,
    #[serde(default)]
    query: crate::handlers::agent::history::HistoryQuery,
}

/// Read UI history and its selected identity from one retained storage read.
/// The returned body can be rendered after the physical guard is released:
/// both the Session and target are immutable values from the same read.
pub async fn inspect_managed_history(
    state: web::Data<AppState>,
    request: HttpRequest,
    path: web::Path<String>,
    body: web::Json<BoundHistoryRequest>,
) -> actix_web::Result<HttpResponse> {
    tickets::user(&state, &request)
        .await
        .map_err(tickets::TicketHttpError::from)?;
    let session_id = path.into_inner();
    if body.target.session_id != session_id {
        return Ok(history_error(
            StatusCode::BAD_REQUEST,
            "target_id_mismatch",
            "The path and observed target must match",
        ));
    }
    let reference = match canonical_reference(&state).await {
        Ok(reference) => reference,
        Err(response) => return Ok(*response),
    };
    if reference != body.supervisor {
        return Ok(history_error(
            StatusCode::CONFLICT,
            "supervisor_observation_stale",
            "The Supervisor changed; refresh the directory",
        ));
    }
    if !state
        .storage
        .supports_supervisor_current_user_bound_history()
    {
        return Ok(history_error(
            StatusCode::NOT_IMPLEMENTED,
            "bound_history_unsupported",
            "This storage backend does not support bound history",
        ));
    }
    // The existing guarded Session snapshot is authoritative for freshness.
    // Matching cache identity alone cannot prove that its history includes
    // every durable append, edit or truncation.
    let (session, observed) = match state
        .storage
        .load_supervisor_current_user_bound_history(&reference, &body.target)
        .await
    {
        Ok(snapshot) => snapshot,
        Err(failure) => {
            tracing::warn!(%session_id, %failure, "bound Supervisor history unavailable");
            let (status, code, message) = match failure.kind() {
                io::ErrorKind::Unsupported => (
                    StatusCode::NOT_IMPLEMENTED,
                    "bound_history_unsupported",
                    "This storage backend does not support bound history",
                ),
                io::ErrorKind::WouldBlock => (
                    StatusCode::CONFLICT,
                    "target_observation_stale",
                    "The target changed; refresh the directory",
                ),
                io::ErrorKind::NotFound | io::ErrorKind::PermissionDenied => (
                    StatusCode::CONFLICT,
                    "target_observation_unavailable",
                    "The observed target is unavailable or changed; refresh the directory",
                ),
                _ => (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "bound_history_unavailable",
                    "Bound history could not be read",
                ),
            };
            return Ok(history_error(status, code, message));
        }
    };
    // A custom backend cannot turn its returned identity into a grant or cause
    // this endpoint to expose history for a different selected target.
    if observed != body.target {
        return Ok(history_error(
            StatusCode::CONFLICT,
            "target_observation_stale",
            "The target changed; refresh the directory",
        ));
    }
    // Token state belongs to this same immutable snapshot. A managed client
    // can safely recover after a clear without a separate ID-only detail read.
    let bound_token_usage = session.token_usage.clone();
    let mut result =
        crate::handlers::agent::history::render_history(&session_id, session, &body.query);
    let object = result
        .as_object_mut()
        .expect("history renderer always returns an object");
    object.insert("supervisor".into(), json!(reference));
    object.insert("current_user_target".into(), json!(observed));
    object.insert("history_snapshot_source".into(), json!("durable"));
    object.insert("bound_token_usage".into(), json!(bound_token_usage));
    Ok(HttpResponse::Ok()
        .insert_header((actix_web::http::header::CACHE_CONTROL, "no-store"))
        .insert_header((
            actix_web::http::header::VARY,
            "Origin, Cookie, Authorization",
        ))
        .json(result))
}

async fn validate_attach_target(
    state: &AppState,
    target_id: &str,
    scope: &SupervisorScopeObservation,
) -> Result<(), Box<HttpResponse>> {
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
        )
        .into());
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
        )
        .into());
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
    .map_err(Box::new)
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
            return Ok(*response);
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
