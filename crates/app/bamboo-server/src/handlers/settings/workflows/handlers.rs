use std::path::PathBuf;
use std::sync::Arc;

use actix_web::{http::StatusCode, web, HttpResponse};
use bamboo_skills::catalog::workflow_catalog_content_digest;
use bamboo_skills::clone_publication::clone_marker_name;
use bamboo_skills::legacy::LegacyWorkflowMigrationOutcome;
use bamboo_skills::store::builtin::{
    builtin_clone_files, builtin_workflow_catalog_entry, load_builtin_skill_bundles,
};
use bamboo_skills::{
    LegacyWorkflowMigrationStatus, SkillStore, WorkflowCatalogEntry, WorkflowCatalogSnapshot,
    WorkflowSource, WorkflowStatus,
};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::{app_state::AppState, error::AppError};

use super::clone_publication::{
    publish_builtin_clone, validate_clone_bundle, ClonePublicationError,
};
use super::types::{
    CloneWorkflowRequest, CloneWorkflowResponse, CloneWorkflowTarget, MigrateWorkflowRequest,
    MigrateWorkflowResponse, SaveWorkflowRequest, WorkflowCatalogQuery, WorkflowGetResponse,
    WorkflowListItem,
};
use super::validation::is_safe_workflow_name;

fn legacy_workflow_io_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn workspace_skills_dir(workspace: &std::path::Path) -> Result<std::path::PathBuf, AppError> {
    let workspace = tokio::fs::canonicalize(workspace).await?;
    let bamboo_dir = workspace.join(".bamboo");
    let skills_dir = bamboo_dir.join("skills");
    for directory in [&bamboo_dir, &skills_dir] {
        match tokio::fs::symlink_metadata(directory).await {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(AppError::Forbidden(format!(
                    "Workspace publication directory '{}' must be a real directory",
                    directory
                        .strip_prefix(&workspace)
                        .unwrap_or(directory)
                        .display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::fs::create_dir(directory).await?;
            }
            Err(error) => return Err(AppError::StorageError(error)),
        }
        let canonical = tokio::fs::canonicalize(directory).await?;
        if !canonical.starts_with(&workspace) {
            return Err(AppError::Forbidden(
                "Workspace publication directory escapes the trusted workspace".to_string(),
            ));
        }
    }
    tokio::fs::canonicalize(skills_dir)
        .await
        .map_err(AppError::StorageError)
}

/// Metadata-only catalog shared by Lotus palette, explicit selection and model matching.
pub async fn list_workflow_catalog(
    app_state: web::Data<AppState>,
    query: web::Query<WorkflowCatalogQuery>,
) -> Result<HttpResponse, AppError> {
    let store = if let Some(session_id) = query
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let session = app_state
            .load_session(session_id)
            .await
            .ok_or_else(|| AppError::NotFound(format!("Session '{session_id}'")))?;
        let project_id =
            match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
                &session,
            ) {
                bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => {
                    Some(project_id)
                }
                bamboo_engine::project_context::SessionProjectIdentity::Unassigned => None,
                bamboo_engine::project_context::SessionProjectIdentity::Invalid {
                    raw,
                    message,
                } => {
                    return Err(AppError::BadRequest(format!(
                        "Session carries an invalid Project identity '{raw}': {message}"
                    )));
                }
            };
        let persisted_workspace = (session
            .metadata
            .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
            .map(String::as_str)
            != Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str()))
        .then(|| session.workspace_path_meta())
        .flatten();
        let workspace = crate::project_context::validate_workspace_assignment_with_resolver(
            &app_state.project_store,
            project_id.as_ref(),
            persisted_workspace.as_deref(),
            &app_state.workspace_resolver,
        )
        .map_err(|error| match error {
            crate::project_context::ProjectWorkspaceValidationError::Invalid { .. }
            | crate::project_context::ProjectWorkspaceValidationError::Conflict { .. } => {
                AppError::BadRequest(error.to_string())
            }
            crate::project_context::ProjectWorkspaceValidationError::Store(error) => {
                AppError::InternalError(anyhow::anyhow!(error))
            }
        })?;
        if let Some(project_id) = project_id {
            app_state.project_store.get(&project_id).map_err(|error| {
                AppError::BadRequest(format!("Assigned Project is unavailable: {error}"))
            })?;
            let project_home = app_state.project_store.paths().project_home(&project_id);
            app_state
                .skill_manager
                .store_for_project_workspace(&project_id, &project_home, workspace.as_deref())
                .await
                .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?
        } else if let Some(workspace) = workspace.as_ref() {
            app_state
                .skill_manager
                .store_for_workspace(Some(workspace))
                .await
                .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?
        } else {
            app_state
                .skill_manager
                .store_for_workspace(None)
                .await
                .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?
        }
    } else {
        app_state
            .skill_manager
            .store_for_workspace(None)
            .await
            .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?
    };

    // Instruction Skills and orchestration definitions are independent
    // activation namespaces, but they form one product-level Workflow
    // Library. Clone both metadata-only snapshots while the store holds one
    // publication guard, then expose only the public Workflow side of the
    // orchestration/legacy namespace.
    let (skill_catalog, workflow_catalog) = store.command_catalog_snapshots().await;
    let mut entries = skill_catalog.entries;
    entries.extend(
        workflow_catalog
            .entries
            .into_iter()
            .filter(WorkflowCatalogEntry::is_public_workflow),
    );
    let snapshot = WorkflowCatalogSnapshot {
        revision: skill_catalog.revision.max(workflow_catalog.revision),
        entries,
    };
    Ok(HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .json(snapshot))
}

struct CloneTargetScope {
    trusted_root: PathBuf,
    store: Arc<SkillStore>,
    published_source: WorkflowSource,
    session_guard: Option<bamboo_storage::session_merge::SessionLockGuard>,
}

fn fixed_clone_error(status: StatusCode, message: &'static str) -> HttpResponse {
    crate::error::json_error(status, message)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn session_project_scope_error(
    error: crate::project_context::ProjectWorkspaceValidationError,
) -> HttpResponse {
    match error {
        crate::project_context::ProjectWorkspaceValidationError::Invalid { .. }
        | crate::project_context::ProjectWorkspaceValidationError::Conflict { .. } => {
            fixed_clone_error(
                StatusCode::CONFLICT,
                "Session Project/workspace assignment is invalid",
            )
        }
        crate::project_context::ProjectWorkspaceValidationError::Store(error) => {
            tracing::error!(%error, "failed to validate Workflow clone Project scope");
            fixed_clone_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Workflow clone scope is unavailable",
            )
        }
    }
}

async fn clone_target_scope(
    app_state: &AppState,
    target: CloneWorkflowTarget,
    session_id: Option<&str>,
) -> Result<CloneTargetScope, HttpResponse> {
    match target {
        CloneWorkflowTarget::User => {
            if session_id.is_some_and(|value| !value.trim().is_empty()) {
                return Err(fixed_clone_error(
                    StatusCode::BAD_REQUEST,
                    "session_id is only valid for a Project Workflow clone",
                ));
            }
            let store = app_state
                .skill_manager
                .store_for_workspace(None)
                .await
                .map_err(|error| {
                    tracing::error!(%error, "failed to resolve user Workflow clone store");
                    fixed_clone_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Workflow clone scope is unavailable",
                    )
                })?;
            Ok(CloneTargetScope {
                trusted_root: app_state.app_data_dir.clone(),
                store,
                published_source: WorkflowSource::User,
                session_guard: None,
            })
        }
        CloneWorkflowTarget::Project => {
            let session_id = session_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    fixed_clone_error(
                        StatusCode::BAD_REQUEST,
                        "session_id is required for a Project Workflow clone",
                    )
                })?;
            let session_guard = app_state.persistence.acquire_lock(session_id).await;
            let session = app_state
                .persistence
                .storage()
                .load_session(session_id)
                .await
                .map_err(|error| {
                    tracing::error!(%error, "failed to load Workflow clone Session");
                    fixed_clone_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Workflow clone scope is unavailable",
                    )
                })?
                .ok_or_else(|| fixed_clone_error(StatusCode::NOT_FOUND, "Session not found"))?;
            let project_id = match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(&session) {
                bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => {
                    project_id
                }
                bamboo_engine::project_context::SessionProjectIdentity::Unassigned => {
                    return Err(fixed_clone_error(
                        StatusCode::BAD_REQUEST,
                        "Project Workflow clone requires an assigned Project",
                    ));
                }
                bamboo_engine::project_context::SessionProjectIdentity::Invalid { .. } => {
                    return Err(fixed_clone_error(
                        StatusCode::CONFLICT,
                        "Session Project/workspace assignment is invalid",
                    ));
                }
            };
            let persisted_workspace = (session
                .metadata
                .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str)
                != Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str()))
            .then(|| session.workspace_path_meta())
            .flatten();
            let project = app_state.project_store.get(&project_id).map_err(|error| {
                tracing::warn!(%error, "assigned Project is unavailable for Workflow clone");
                fixed_clone_error(StatusCode::CONFLICT, "Assigned Project is unavailable")
            })?;
            // Project-home publication does not require a source checkout. If
            // the Project or Session does declare one, validate it before it
            // can influence the scoped source-catalog winner.
            let workspace = if persisted_workspace.is_some() || project.project_path.is_some() {
                crate::project_context::validate_workspace_assignment_with_resolver(
                    &app_state.project_store,
                    Some(&project_id),
                    persisted_workspace.as_deref(),
                    &app_state.workspace_resolver,
                )
                .map_err(session_project_scope_error)?
            } else {
                None
            };
            let project_home = app_state.project_store.paths().project_home(&project_id);
            let store = app_state
                .skill_manager
                .store_for_project_workspace(&project_id, &project_home, workspace.as_deref())
                .await
                .map_err(|error| {
                    tracing::error!(%error, "failed to resolve Project Workflow clone store");
                    fixed_clone_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Workflow clone scope is unavailable",
                    )
                })?;
            Ok(CloneTargetScope {
                trusted_root: project_home,
                store,
                published_source: WorkflowSource::Project,
                session_guard: Some(session_guard),
            })
        }
    }
}

async fn combined_catalog_snapshot(store: &SkillStore) -> WorkflowCatalogSnapshot {
    let (skill_catalog, workflow_catalog) = store.command_catalog_snapshots().await;
    let mut entries = skill_catalog.entries;
    entries.extend(workflow_catalog.entries);
    WorkflowCatalogSnapshot {
        revision: skill_catalog.revision.max(workflow_catalog.revision),
        entries,
    }
}

fn unique_winner<'a>(
    catalog: &'a WorkflowCatalogSnapshot,
    workflow_id: &str,
) -> Result<Option<&'a WorkflowCatalogEntry>, ()> {
    let mut matches = catalog
        .entries
        .iter()
        .filter(|entry| entry.id == workflow_id && entry.winner);
    let first = matches.next();
    if matches.next().is_some() {
        Err(())
    } else {
        Ok(first)
    }
}

/// Publish one exact builtin Workflow into the canonical Project or user layer.
///
/// Interrupted exact transactions resume from their durable marker. A deleted
/// completed generation retires before a bounded no-replace epoch rollover;
/// existing or replacement targets are never adopted or overwritten.
pub async fn clone_workflow(
    app_state: web::Data<AppState>,
    workflow_id: web::Path<String>,
    payload: web::Json<CloneWorkflowRequest>,
) -> Result<HttpResponse, AppError> {
    let workflow_id = workflow_id.into_inner();
    if !is_safe_workflow_name(&workflow_id)
        || payload.revision == 0
        || !valid_sha256(&payload.content_digest)
    {
        return Ok(fixed_clone_error(
            StatusCode::BAD_REQUEST,
            "Workflow clone selection is invalid",
        ));
    }
    if payload.source != WorkflowSource::Builtin {
        return Ok(fixed_clone_error(
            StatusCode::BAD_REQUEST,
            "Only builtin Workflows can be cloned",
        ));
    }

    let mut scope = match clone_target_scope(
        app_state.get_ref(),
        payload.target,
        payload.session_id.as_deref(),
    )
    .await
    {
        Ok(scope) => scope,
        Err(response) => return Ok(response),
    };
    if let Err(error) = scope.store.reload().await {
        tracing::error!(%error, "failed to reload Workflow clone source catalog");
        return Ok(fixed_clone_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Workflow catalog is unavailable",
        ));
    }
    let source_catalog = combined_catalog_snapshot(&scope.store).await;
    let catalog_winner = match unique_winner(&source_catalog, &workflow_id) {
        Ok(entry) => entry.cloned(),
        Err(()) => {
            return Ok(fixed_clone_error(
                StatusCode::CONFLICT,
                "Workflow catalog identity is ambiguous",
            ));
        }
    };

    let bundles = match load_builtin_skill_bundles() {
        Ok(bundles) => bundles,
        Err(error) => {
            tracing::error!(%error, "failed to load embedded Workflow bundles");
            return Ok(fixed_clone_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Builtin Workflow materialization is unavailable",
            ));
        }
    };
    let Some(bundle) = bundles
        .into_iter()
        .find(|bundle| bundle.skill.id == workflow_id)
    else {
        return Ok(fixed_clone_error(
            StatusCode::CONFLICT,
            "Workflow clone source is not an embedded bundle",
        ));
    };
    let source_entry = match builtin_workflow_catalog_entry(&bundle, payload.revision) {
        Ok(entry) => entry,
        Err(error) => {
            tracing::error!(%error, "failed to rebuild builtin Workflow catalog identity");
            return Ok(fixed_clone_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Builtin Workflow identity is unavailable",
            ));
        }
    };
    if source_entry.revision != payload.revision
        || source_entry.content_digest != payload.content_digest
    {
        return Ok(fixed_clone_error(
            StatusCode::CONFLICT,
            "Workflow clone selection is stale or shadowed",
        ));
    }
    let winner_is_exact_builtin = catalog_winner.as_ref().is_some_and(|entry| {
        entry.source == WorkflowSource::Builtin
            && entry.status == WorkflowStatus::Valid
            && entry.revision == payload.revision
            && entry.content_digest == payload.content_digest
    });
    if !winner_is_exact_builtin {
        let marker = scope
            .trusted_root
            .join("skills")
            .join(clone_marker_name(&workflow_id));
        match tokio::fs::symlink_metadata(&marker).await {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(fixed_clone_error(
                    if catalog_winner.is_some() {
                        StatusCode::CONFLICT
                    } else {
                        StatusCode::NOT_FOUND
                    },
                    "Workflow clone selection is stale or shadowed",
                ));
            }
            Err(error) => {
                tracing::error!(%error, workflow_id, "failed to inspect Workflow clone recovery marker");
                return Ok(fixed_clone_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Workflow clone publication failed",
                ));
            }
        }
    }
    if source_entry.source != WorkflowSource::Builtin {
        tracing::error!(
            workflow_id,
            "builtin Workflow catalog identity diverged from embedded bytes"
        );
        return Ok(fixed_clone_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Builtin Workflow identity is unavailable",
        ));
    }
    let files = match builtin_clone_files(&bundle) {
        Ok(files) => files,
        Err(error) => {
            tracing::error!(%error, "failed to render builtin Workflow clone bundle");
            return Ok(fixed_clone_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Builtin Workflow materialization is unavailable",
            ));
        }
    };
    if let Err(error) = validate_clone_bundle(&files) {
        tracing::error!(
            ?error,
            "builtin Workflow clone bundle failed publication validation"
        );
        return Ok(fixed_clone_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Builtin Workflow materialization is unavailable",
        ));
    }

    let trusted_root = scope.trusted_root.clone();
    let publish_id = workflow_id.clone();
    let publish_digest = source_entry.content_digest.clone();
    let source_revision = source_entry.revision;
    let session_guard = scope.session_guard.take();
    let publication = tokio::task::spawn_blocking(move || {
        let result = publish_builtin_clone(
            &trusted_root,
            &publish_id,
            source_revision,
            &publish_digest,
            &files,
        );
        (result, session_guard)
    })
    .await;
    let (publication, _session_guard) = match publication {
        Ok(result) => result,
        Err(error) => {
            tracing::error!(%error, "Workflow clone publication task failed");
            return Ok(fixed_clone_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Workflow clone publication failed",
            ));
        }
    };
    let receipt = match publication {
        Ok(receipt) => receipt,
        Err(ClonePublicationError::Conflict(message)) => {
            tracing::warn!(
                workflow_id,
                reason = message,
                "Workflow clone publication conflict"
            );
            return Ok(fixed_clone_error(
                StatusCode::CONFLICT,
                "Workflow clone conflicts with existing target state",
            ));
        }
        Err(ClonePublicationError::Io(error)) => {
            tracing::error!(%error, workflow_id, "Workflow clone publication I/O failure");
            return Ok(fixed_clone_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Workflow clone publication failed",
            ));
        }
        Err(ClonePublicationError::Internal(message)) => {
            tracing::error!(
                workflow_id,
                reason = message,
                "Workflow clone publication invariant failed"
            );
            return Ok(fixed_clone_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Workflow clone publication failed",
            ));
        }
    };
    tracing::debug!(
        workflow_id,
        target_device = receipt.target_identity.device,
        target_inode = receipt.target_identity.inode,
        "published exact builtin Workflow clone"
    );

    if let Err(error) = scope.store.reload().await {
        tracing::error!(%error, workflow_id, "failed to reload published Workflow clone");
        return Ok(fixed_clone_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Published Workflow catalog verification failed",
        ));
    }
    let published_catalog = combined_catalog_snapshot(&scope.store).await;
    let published_entry = match unique_winner(&published_catalog, &workflow_id) {
        Ok(Some(entry))
            if entry.source == scope.published_source && entry.status == WorkflowStatus::Valid =>
        {
            entry
        }
        _ => {
            tracing::error!(
                workflow_id,
                "published Workflow clone is not the exact catalog winner"
            );
            return Ok(fixed_clone_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Published Workflow catalog verification failed",
            ));
        }
    };
    let mut expected_entry = source_entry.clone();
    expected_entry.source = scope.published_source;
    expected_entry.content_digest.clear();
    expected_entry.last_error = None;
    expected_entry.shadowed_candidates.clear();
    let expected_digest = workflow_catalog_content_digest(
        &expected_entry,
        Some(&bundle.skill),
        bundle
            .files
            .iter()
            .map(|(path, bytes)| (path.as_str(), bytes.as_slice())),
    );
    if published_entry.content_digest != expected_digest {
        tracing::error!(
            workflow_id,
            "published Workflow clone digest does not match embedded bytes"
        );
        return Ok(fixed_clone_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Published Workflow catalog verification failed",
        ));
    }

    Ok(HttpResponse::Created()
        .insert_header(("Cache-Control", "no-store"))
        .json(CloneWorkflowResponse {
            workflow_id,
            target: payload.target,
            source_preserved: true,
            source_revision: source_entry.revision,
            source_content_digest: source_entry.content_digest,
            published_source: published_entry.source,
            published_revision: published_entry.revision,
            published_content_digest: published_entry.content_digest.clone(),
            catalog_revision: published_catalog.revision,
        }))
}

/// Clone one read-only global/workspace/plugin legacy workflow into the trusted
/// session workspace's canonical `.bamboo/skills/<id>/SKILL.md` bundle.
///
/// The legacy source is never changed or removed, and an existing target is
/// never overwritten. Repeating a completed migration is an idempotent
/// `already_migrated` success.
pub async fn migrate_workflow(
    app_state: web::Data<AppState>,
    workflow_id: web::Path<String>,
    payload: web::Json<MigrateWorkflowRequest>,
) -> Result<HttpResponse, AppError> {
    let workflow_id = workflow_id.into_inner();
    let session_id = payload.session_id.trim();
    if session_id.is_empty() {
        return Err(AppError::BadRequest("session_id is required".to_string()));
    }
    let session = app_state
        .load_session(session_id)
        .await
        .ok_or_else(|| AppError::NotFound(format!("Session '{session_id}'")))?;
    let project_id =
        match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
            &session,
        ) {
            bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => {
                Some(project_id)
            }
            bamboo_engine::project_context::SessionProjectIdentity::Unassigned => None,
            bamboo_engine::project_context::SessionProjectIdentity::Invalid { raw, message } => {
                return Err(AppError::BadRequest(format!(
                    "Session carries an invalid Project identity '{raw}': {message}"
                )));
            }
        };
    let persisted_workspace = (session
        .metadata
        .get(bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY)
        .map(String::as_str)
        != Some(bamboo_engine::project_context::WorkspaceSource::ProjectDefault.as_str()))
    .then(|| session.workspace_path_meta())
    .flatten();
    let workspace = crate::project_context::validate_workspace_assignment_with_resolver(
        &app_state.project_store,
        project_id.as_ref(),
        persisted_workspace.as_deref(),
        &app_state.workspace_resolver,
    )
    .map_err(|error| match error {
        crate::project_context::ProjectWorkspaceValidationError::Invalid { .. }
        | crate::project_context::ProjectWorkspaceValidationError::Conflict { .. } => {
            AppError::BadRequest(error.to_string())
        }
        crate::project_context::ProjectWorkspaceValidationError::Store(error) => {
            AppError::InternalError(anyhow::anyhow!(error))
        }
    })?
    .ok_or_else(|| {
        AppError::BadRequest("Legacy workflow migration requires a session workspace".to_string())
    })?;

    let store = if let Some(project_id) = project_id {
        app_state.project_store.get(&project_id).map_err(|error| {
            AppError::BadRequest(format!("Assigned Project is unavailable: {error}"))
        })?;
        let project_home = app_state.project_store.paths().project_home(&project_id);
        app_state
            .skill_manager
            .store_for_project_workspace(&project_id, &project_home, Some(&workspace))
            .await
    } else {
        app_state
            .skill_manager
            .store_for_workspace(Some(&workspace))
            .await
    }
    .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    store
        .reload()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    let catalog = store.workflow_catalog_snapshot().await;
    let entry = catalog
        .entries
        .iter()
        .find(|entry| entry.id == workflow_id)
        .ok_or_else(|| AppError::NotFound(format!("Workflow '{workflow_id}'")))?;
    if entry.migration_status == Some(LegacyWorkflowMigrationStatus::Migrated) {
        return Ok(HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(MigrateWorkflowResponse {
                workflow_id,
                outcome: LegacyWorkflowMigrationOutcome::AlreadyMigrated,
                source_preserved: true,
                catalog_revision: catalog.revision,
            }));
    }
    if entry.migration_status != Some(LegacyWorkflowMigrationStatus::Available) {
        if entry.shadowed_candidates.iter().any(|candidate| {
            candidate.migration_status == Some(LegacyWorkflowMigrationStatus::Available)
        }) {
            return Ok(crate::error::json_error(
                StatusCode::CONFLICT,
                format!(
                    "Workflow '{workflow_id}' already has a target Skill bundle; it was not overwritten"
                ),
            ));
        }
        return Err(AppError::BadRequest(format!(
            "Workflow '{workflow_id}' is not a migratable legacy workflow"
        )));
    }

    let source = store
        .get_legacy_workflow_source(&workflow_id)
        .await
        .map_err(|error| {
            AppError::BadRequest(format!("Legacy workflow is unavailable: {error}"))
        })?;
    let source = tokio::fs::canonicalize(&source).await.map_err(|error| {
        AppError::BadRequest(format!("Legacy workflow source is unavailable: {error}"))
    })?;
    let canonical_workspace = tokio::fs::canonicalize(&workspace).await?;
    let workspace_legacy = workspace.join(".bamboo/workflows");
    let workspace_source_identity = match tokio::fs::canonicalize(&workspace_legacy).await {
        Ok(root) => {
            if root.starts_with(&canonical_workspace) && source.parent() == Some(root.as_path()) {
                source
                    .file_name()
                    .and_then(|filename| filename.to_str())
                    .map(|filename| format!(".bamboo/workflows/{filename}"))
            } else {
                None
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(AppError::StorageError(error)),
    };
    let plugin_source_identity =
        match tokio::fs::canonicalize(app_state.app_data_dir.join("plugins")).await {
            Ok(root) => source.strip_prefix(&root).ok().and_then(|relative| {
                let components: Vec<_> = relative.components().collect();
                if components.len() != 3 || components[1].as_os_str() != "workflows" {
                    return None;
                }
                Some(format!(
                    "plugins/{}/workflows/{}",
                    components[0].as_os_str().to_str()?,
                    components[2].as_os_str().to_str()?
                ))
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(AppError::StorageError(error)),
        };
    let global_workflows = app_state.app_data_dir.join("workflows");
    let global_source_identity = match tokio::fs::symlink_metadata(&global_workflows).await {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
            let canonical_app_data = tokio::fs::canonicalize(&app_state.app_data_dir).await?;
            let root = tokio::fs::canonicalize(&global_workflows).await?;
            if root.starts_with(&canonical_app_data) && source.parent() == Some(root.as_path()) {
                source
                    .file_name()
                    .and_then(|filename| filename.to_str())
                    .map(|filename| format!("workflows/{filename}"))
            } else {
                None
            }
        }
        Ok(_) => None,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(AppError::StorageError(error)),
    };
    let source_identity = workspace_source_identity
        .or(global_source_identity)
        .or(plugin_source_identity)
        .ok_or_else(|| {
            AppError::Forbidden(
                "Legacy workflow source is outside a migratable global/workspace/plugin scope"
                    .to_string(),
            )
        })?;

    let skills_dir = workspace_skills_dir(&canonical_workspace).await?;
    let outcome = bamboo_skills::legacy::migrate_legacy_markdown_workflow(
        &source,
        &source_identity,
        &skills_dir,
        &workflow_id,
        payload.description.as_deref(),
    )
    .await
    .map_err(|error| AppError::BadRequest(format!("Legacy workflow migration failed: {error}")))?;
    if outcome == LegacyWorkflowMigrationOutcome::Conflict {
        return Ok(crate::error::json_error(
            StatusCode::CONFLICT,
            format!(
                "Workflow '{workflow_id}' already has a target Skill bundle; it was not overwritten"
            ),
        ));
    }
    store
        .reload()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    let revision = store.workflow_catalog_snapshot().await.revision;
    Ok(HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .json(MigrateWorkflowResponse {
            workflow_id,
            outcome,
            source_preserved: true,
            catalog_revision: revision,
        }))
}

/// Lists all workflow markdown files
///
/// # HTTP Route
/// `GET /bamboo/workflows`
///
/// # Response Format
/// Returns array of workflow metadata:
/// ```json
/// [
///   {
///     "name": "myworkflow",
///     "filename": "myworkflow.md",
///     "size": 1234,
///     "modified_at": null
///   }
/// ]
/// ```
///
/// # Response Status
/// - `200 OK`: Successfully retrieved workflow list
pub async fn list_workflows(app_state: web::Data<AppState>) -> Result<HttpResponse, AppError> {
    let workflows_dir = app_state.app_data_dir.join("workflows");
    let mut workflows = Vec::new();
    let mut entries = match fs::read_dir(&workflows_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(legacy_response().json(workflows));
        }
        Err(error) => return Err(AppError::StorageError(error)),
    };
    while let Some(entry) = entries.next_entry().await? {
        let Some(filename) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let Some(name) = filename.strip_suffix(".md") else {
            continue;
        };
        if !is_safe_workflow_name(name) {
            continue;
        }
        let file_type = entry.file_type().await?;
        if !file_type.is_file() || file_type.is_symlink() {
            continue;
        }
        workflows.push(WorkflowListItem {
            name: name.to_string(),
            filename,
            size: entry.metadata().await?.len(),
            modified_at: None,
        });
    }

    workflows.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(legacy_response().json(workflows))
}

/// Gets a specific workflow by name.
///
/// # HTTP Route
/// `GET /bamboo/workflows/{name}`
pub async fn get_workflow(
    app_state: web::Data<AppState>,
    workflow_name: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let name = workflow_name.into_inner();
    if !is_safe_workflow_name(&name) {
        // An invalid (malformed) name is a 400, matching every other workflow
        // handler — not a 404, which would imply a valid-but-absent workflow. #97.
        return Err(AppError::BadRequest("Invalid workflow name".to_string()));
    }

    let dir = app_state.app_data_dir.join("workflows");
    let filename = format!("{name}.md");
    let file_path = dir.join(&filename);
    let metadata = match fs::symlink_metadata(&file_path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(AppError::NotFound(format!("Workflow '{name}'")));
        }
        Err(error) => return Err(AppError::StorageError(error)),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(AppError::NotFound(format!("Workflow '{name}'")));
    }
    let content = bamboo_skills::legacy::read_legacy_markdown_workflow(&file_path)
        .await
        .map_err(|error| AppError::BadRequest(format!("Workflow '{name}' is invalid: {error}")))?;
    let size = content.len() as u64;

    Ok(legacy_response().json(WorkflowGetResponse {
        name,
        filename,
        content,
        size,
        modified_at: None,
    }))
}

/// Creates or updates a workflow.
///
/// # HTTP Route
/// `POST /bamboo/workflows`
pub async fn save_workflow(
    app_state: web::Data<AppState>,
    payload: web::Json<SaveWorkflowRequest>,
) -> Result<HttpResponse, AppError> {
    let _io_guard = legacy_workflow_io_lock().lock().await;
    let name = payload.name.trim();
    if !is_safe_workflow_name(name) {
        return Err(AppError::BadRequest("Invalid workflow name".to_string()));
    }

    let dir = app_state.app_data_dir.join("workflows");
    fs::create_dir_all(&dir).await?;

    let file_path = dir.join(format!("{}.md", name));

    let temporary = dir.join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4()));
    let mut staging = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .await?;
    if let Err(error) = async {
        staging.write_all(payload.content.as_bytes()).await?;
        staging.flush().await?;
        staging.sync_all().await?;
        drop(staging);
        bamboo_skills::legacy::atomic_replace_file(&temporary, &file_path).await
    }
    .await
    {
        let _ = fs::remove_file(&temporary).await;
        return Err(error.into());
    }

    // The legacy source remains a Workflow. Reload discovers it through the
    // read-only adapter; saving must never materialize or overwrite a Skill.
    app_state
        .skill_manager
        .store()
        .reload_global_workflow_views()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;

    Ok(legacy_response().json(serde_json::json!({
        "success": true,
        "path": file_path.to_string_lossy(),
        "catalog_revision": app_state.skill_manager.store().workflow_catalog_snapshot().await.revision,
    })))
}

/// Deletes a workflow file.
///
/// # HTTP Route
/// `DELETE /bamboo/workflows/{name}`
pub async fn delete_workflow(
    app_state: web::Data<AppState>,
    workflow_name: web::Path<String>,
) -> Result<HttpResponse, AppError> {
    let _io_guard = legacy_workflow_io_lock().lock().await;
    let name = workflow_name.into_inner();
    if !is_safe_workflow_name(&name) {
        return Err(AppError::BadRequest("Invalid workflow name".to_string()));
    }

    let dir = app_state.app_data_dir.join("workflows");
    let file_path = dir.join(format!("{}.md", name));
    let metadata = match fs::symlink_metadata(&file_path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(AppError::NotFound(format!("Workflow '{name}'")));
        }
        Err(error) => return Err(AppError::StorageError(error)),
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(AppError::NotFound(format!("Workflow '{name}'")));
    }
    let skill_id = bamboo_skills::legacy::legacy_workflow_skill_id(&name);
    let removed = app_state
        .skill_manager
        .store()
        .remove_legacy_workflow(&file_path, &skill_id)
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    if !removed {
        return Err(AppError::NotFound(format!("Workflow '{name}'")));
    }
    app_state
        .skill_manager
        .store()
        .reload_global_workflow_views()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;

    Ok(legacy_response().json(serde_json::json!({ "success": true })))
}

fn legacy_response() -> actix_web::HttpResponseBuilder {
    let mut response = HttpResponse::Ok();
    response
        .insert_header(("Deprecation", "true"))
        .insert_header(("Sunset", "2026-12-01"))
        .insert_header((
            "Link",
            "</api/v1/bamboo/workflow-catalog>; rel=\"successor-version\"",
        ));
    response
}
