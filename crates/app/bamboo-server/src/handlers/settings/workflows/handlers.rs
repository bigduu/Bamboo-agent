use std::path::{Path, PathBuf};
use std::sync::Arc;

use actix_web::{http::StatusCode, web, HttpResponse};
use bamboo_skills::catalog::workflow_catalog_content_digest;
use bamboo_skills::clone_publication::clone_marker_name;
use bamboo_skills::legacy::LegacyWorkflowMigrationOutcome;
use bamboo_skills::store::builtin::{
    builtin_clone_files, builtin_workflow_catalog_entry, load_builtin_skill_bundles,
};
use bamboo_skills::{
    LegacyWorkflowMigrationStatus, ShadowedWorkflowCandidate, SkillStore, WorkflowCatalogEntry,
    WorkflowCatalogSnapshot, WorkflowSource, WorkflowStatus,
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

fn legacy_source_is_same_file(selected: &std::fs::Metadata, current: &std::fs::Metadata) -> bool {
    if selected.file_type().is_symlink()
        || current.file_type().is_symlink()
        || !selected.is_file()
        || !current.is_file()
    {
        return false;
    }
    legacy_source_identity_matches(selected, current)
}

#[cfg(unix)]
fn legacy_source_identity_matches(
    selected: &std::fs::Metadata,
    current: &std::fs::Metadata,
) -> bool {
    use std::os::unix::fs::MetadataExt;
    selected.dev() == current.dev() && selected.ino() == current.ino()
}

#[cfg(windows)]
fn legacy_source_identity_matches(
    selected: &std::fs::Metadata,
    current: &std::fs::Metadata,
) -> bool {
    use std::os::windows::fs::MetadataExt;
    selected.volume_serial_number() == current.volume_serial_number()
        && selected.file_index() == current.file_index()
}

#[cfg(not(any(unix, windows)))]
fn legacy_source_identity_matches(
    selected: &std::fs::Metadata,
    current: &std::fs::Metadata,
) -> bool {
    selected.len() == current.len() && selected.modified().ok() == current.modified().ok()
}

async fn workspace_publication_root(
    workspace: &std::path::Path,
) -> Result<std::path::PathBuf, AppError> {
    let workspace = tokio::fs::canonicalize(workspace).await?;
    let bamboo_dir = workspace.join(".bamboo");
    let metadata = tokio::fs::symlink_metadata(&bamboo_dir).await?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(AppError::Forbidden(
            "Workspace publication root must be a real directory".to_string(),
        ));
    }
    let canonical = tokio::fs::canonicalize(bamboo_dir).await?;
    if !canonical.starts_with(&workspace) {
        return Err(AppError::Forbidden(
            "Workspace publication root escapes the trusted workspace".to_string(),
        ));
    }
    Ok(canonical)
}

fn public_workflow_catalog_snapshot(
    mut skill_catalog: WorkflowCatalogSnapshot,
    workflow_catalog: WorkflowCatalogSnapshot,
) -> WorkflowCatalogSnapshot {
    let workflow_revision = workflow_catalog.revision;
    for workflow in workflow_catalog
        .entries
        .into_iter()
        .filter(WorkflowCatalogEntry::is_public_workflow)
    {
        let migrated = skill_catalog.entries.iter_mut().find(|entry| {
            entry.id == workflow.id
                && entry.migration_status == Some(LegacyWorkflowMigrationStatus::Migrated)
        });
        if workflow.migration_status == Some(LegacyWorkflowMigrationStatus::Available) {
            if let Some(migrated) = migrated {
                let source = ShadowedWorkflowCandidate {
                    source: workflow.source,
                    status: workflow.status,
                    legacy: true,
                    migration_status: workflow.migration_status,
                    last_error: workflow.last_error,
                };
                if !migrated.shadowed_candidates.contains(&source) {
                    migrated.shadowed_candidates.push(source);
                }
                continue;
            }
        }
        skill_catalog.entries.push(workflow);
    }
    skill_catalog.revision = skill_catalog.revision.max(workflow_revision);
    skill_catalog
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
    let snapshot = public_workflow_catalog_snapshot(skill_catalog, workflow_catalog);
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

/// Migrate one read-only legacy workflow into its canonical user or Project
/// Skill layer. The source stays untouched through the documented
/// compatibility boundary and target publication is atomic no-replace.
pub async fn migrate_workflow(
    app_state: web::Data<AppState>,
    workflow_id: web::Path<String>,
    payload: web::Json<MigrateWorkflowRequest>,
) -> Result<HttpResponse, AppError> {
    migrate_workflow_with_source_hook(
        app_state,
        workflow_id.into_inner(),
        payload.into_inner(),
        |_| {},
    )
    .await
}

pub(super) async fn migrate_workflow_with_source_hook<F>(
    app_state: web::Data<AppState>,
    workflow_id: String,
    payload: MigrateWorkflowRequest,
    after_source_selection: F,
) -> Result<HttpResponse, AppError>
where
    F: FnOnce(&Path),
{
    let session_id = payload.session_id.trim();
    if session_id.is_empty() {
        return Err(AppError::BadRequest("session_id is required".to_string()));
    }

    let _session_guard = app_state.persistence.acquire_lock(session_id).await;
    let session = app_state
        .persistence
        .storage()
        .load_session(session_id)
        .await
        .map_err(AppError::StorageError)?
        .ok_or_else(|| AppError::NotFound(format!("Session '{session_id}'")))?;
    let project_id =
        match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
            &session,
        ) {
            bamboo_engine::project_context::SessionProjectIdentity::Assigned(project_id) => {
                Some(project_id)
            }
            bamboo_engine::project_context::SessionProjectIdentity::Unassigned => None,
            bamboo_engine::project_context::SessionProjectIdentity::Invalid { .. } => {
                return Err(AppError::BadRequest(
                    "Session Project identity is invalid".to_string(),
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
    let workspace = crate::project_context::validate_workspace_assignment_with_resolver(
        &app_state.project_store,
        project_id.as_ref(),
        persisted_workspace.as_deref(),
        &app_state.workspace_resolver,
    )
    .map_err(|error| match error {
        crate::project_context::ProjectWorkspaceValidationError::Invalid { .. }
        | crate::project_context::ProjectWorkspaceValidationError::Conflict { .. } => {
            AppError::BadRequest("Session Project/workspace assignment is invalid".to_string())
        }
        crate::project_context::ProjectWorkspaceValidationError::Store(error) => {
            AppError::InternalError(anyhow::anyhow!(error))
        }
    })?;

    let store = if let Some(project_id) = project_id.as_ref() {
        app_state
            .project_store
            .get(project_id)
            .map_err(|_| AppError::BadRequest("Assigned Project is unavailable".to_string()))?;
        let project_home = app_state.project_store.paths().project_home(project_id);
        app_state
            .skill_manager
            .store_for_project_workspace(project_id, &project_home, workspace.as_deref())
            .await
    } else {
        app_state
            .skill_manager
            .store_for_workspace(workspace.as_deref())
            .await
    }
    .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    store
        .reload()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    let (skill_catalog, workflow_catalog) = store.command_catalog_snapshots().await;

    let accepted = workflow_catalog
        .entries
        .iter()
        .find(|entry| entry.id == workflow_id)
        .cloned()
        .ok_or_else(|| AppError::NotFound(format!("Workflow '{workflow_id}'")))?;
    if accepted.migration_status != Some(LegacyWorkflowMigrationStatus::Available) {
        return Ok(crate::error::json_error(
            StatusCode::CONFLICT,
            "Workflow is not an available legacy migration source",
        ));
    }

    let catalog_source = store
        .get_legacy_workflow_source(&workflow_id)
        .await
        .map_err(|_| AppError::BadRequest("Legacy workflow source is unavailable".to_string()))?;
    let selected_metadata = tokio::fs::symlink_metadata(&catalog_source)
        .await
        .map_err(|_| AppError::BadRequest("Legacy workflow source is unavailable".to_string()))?;
    if selected_metadata.file_type().is_symlink() || !selected_metadata.is_file() {
        return Ok(crate::error::json_error(
            StatusCode::CONFLICT,
            "Legacy workflow source changed; refresh the catalog before migrating",
        ));
    }
    let canonical_source = tokio::fs::canonicalize(&catalog_source)
        .await
        .map_err(|_| AppError::BadRequest("Legacy workflow source is unavailable".to_string()))?;
    let canonical_app_data = tokio::fs::canonicalize(&app_state.app_data_dir).await?;
    let canonical_workspace = match workspace.as_ref() {
        Some(workspace) => Some(tokio::fs::canonicalize(workspace).await?),
        None => None,
    };

    enum LegacySourceLayer {
        Workspace,
        User,
    }
    let workspace_source_identity = if let Some(workspace) = workspace.as_ref() {
        let workspace_legacy = workspace.join(".bamboo/workflows");
        match tokio::fs::canonicalize(&workspace_legacy).await {
            Ok(root)
                if canonical_workspace
                    .as_ref()
                    .is_some_and(|workspace| root.starts_with(workspace))
                    && canonical_source.parent() == Some(root.as_path()) =>
            {
                canonical_source
                    .file_name()
                    .and_then(|filename| filename.to_str())
                    .map(|filename| format!(".bamboo/workflows/{filename}"))
            }
            Ok(_) => None,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(AppError::StorageError(error)),
        }
    } else {
        None
    };
    let global_source_identity =
        match tokio::fs::canonicalize(app_state.app_data_dir.join("workflows")).await {
            Ok(root)
                if root.starts_with(&canonical_app_data)
                    && canonical_source.parent() == Some(root.as_path()) =>
            {
                canonical_source
                    .file_name()
                    .and_then(|filename| filename.to_str())
                    .map(|filename| format!("workflows/{filename}"))
            }
            Ok(_) => None,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(AppError::StorageError(error)),
        };
    let plugin_source_identity =
        match tokio::fs::canonicalize(app_state.app_data_dir.join("plugins")).await {
            Ok(root) => canonical_source
                .strip_prefix(&root)
                .ok()
                .and_then(|relative| {
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
    let (source_identity, source_layer) = if let Some(identity) = workspace_source_identity {
        (identity, LegacySourceLayer::Workspace)
    } else if let Some(identity) = global_source_identity.or(plugin_source_identity) {
        (identity, LegacySourceLayer::User)
    } else {
        return Err(AppError::Forbidden(
            "Legacy workflow source is outside a supported migration scope".to_string(),
        ));
    };

    let (trusted_root, published_source) = match source_layer {
        LegacySourceLayer::User => (app_state.app_data_dir.clone(), WorkflowSource::User),
        LegacySourceLayer::Workspace => {
            if let Some(project_id) = project_id.as_ref() {
                (
                    app_state.project_store.paths().project_home(project_id),
                    WorkflowSource::Project,
                )
            } else {
                let workspace = canonical_workspace.as_ref().ok_or_else(|| {
                    AppError::BadRequest(
                        "Workspace legacy migration requires a session workspace".to_string(),
                    )
                })?;
                (
                    workspace_publication_root(workspace).await?,
                    WorkflowSource::Workspace,
                )
            }
        }
    };

    if let Some(migrated) = skill_catalog.entries.iter().find(|entry| {
        entry.id == workflow_id
            && entry.source == published_source
            && entry.migration_status == Some(LegacyWorkflowMigrationStatus::Migrated)
    }) {
        let exact_migration = store
            .get_skill(&workflow_id)
            .await
            .ok()
            .is_some_and(|skill| {
                skill.metadata.as_ref().is_some_and(|metadata| {
                    metadata
                        .get("legacy_migration")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true)
                        && metadata
                            .get("original_source")
                            .and_then(serde_json::Value::as_str)
                            == Some(source_identity.as_str())
                        && metadata
                            .get("legacy_source_content_digest")
                            .and_then(serde_json::Value::as_str)
                            == Some(accepted.content_digest.as_str())
                })
            });
        if exact_migration {
            return Ok(HttpResponse::Ok()
                .insert_header(("Cache-Control", "no-store"))
                .json(MigrateWorkflowResponse {
                    workflow_id,
                    outcome: LegacyWorkflowMigrationOutcome::AlreadyMigrated,
                    source_preserved: true,
                    catalog_revision: skill_catalog.revision.max(workflow_catalog.revision),
                }));
        }
        tracing::warn!(
            workflow_id,
            source = ?migrated.source,
            "legacy migration target source identity is inconsistent"
        );
        return Ok(crate::error::json_error(
            StatusCode::CONFLICT,
            "Workflow migration conflicts with an existing target Skill",
        ));
    }

    let target_rank = |source| match source {
        WorkflowSource::Builtin => 0,
        WorkflowSource::Plugin => 1,
        WorkflowSource::User => 2,
        WorkflowSource::Project => 3,
        WorkflowSource::Workspace => 4,
    };
    if skill_catalog.entries.iter().any(|entry| {
        entry.id == workflow_id && target_rank(entry.source) >= target_rank(published_source)
    }) {
        return Ok(crate::error::json_error(
            StatusCode::CONFLICT,
            "Workflow migration conflicts with an existing target Skill",
        ));
    }

    after_source_selection(&catalog_source);
    let source_content =
        match bamboo_skills::legacy::read_legacy_markdown_workflow(&catalog_source).await {
            Ok(content) => content,
            Err(_) => {
                return Ok(crate::error::json_error(
                    StatusCode::CONFLICT,
                    "Legacy workflow source changed; refresh the catalog before migrating",
                ));
            }
        };
    let current_metadata = match tokio::fs::symlink_metadata(&catalog_source).await {
        Ok(metadata) => metadata,
        Err(_) => {
            return Ok(crate::error::json_error(
                StatusCode::CONFLICT,
                "Legacy workflow source changed; refresh the catalog before migrating",
            ));
        }
    };
    let live_digest = match bamboo_skills::legacy::legacy_markdown_catalog_content_digest(
        &accepted,
        &catalog_source,
        &workflow_id,
        &source_content,
    ) {
        Ok(digest) => digest,
        Err(_) => {
            return Ok(crate::error::json_error(
                StatusCode::CONFLICT,
                "Legacy workflow source changed; refresh the catalog before migrating",
            ));
        }
    };
    if !legacy_source_is_same_file(&selected_metadata, &current_metadata)
        || live_digest != accepted.content_digest
    {
        return Ok(crate::error::json_error(
            StatusCode::CONFLICT,
            "Legacy workflow source changed; refresh the catalog before migrating",
        ));
    }

    let prepared = bamboo_skills::legacy::prepare_legacy_markdown_workflow(
        &catalog_source,
        &source_identity,
        &workflow_id,
        &source_content,
        Some((accepted.revision, accepted.content_digest.as_str())),
        payload.description.as_deref(),
    )
    .map_err(|_| AppError::BadRequest("Legacy workflow cannot be migrated".to_string()))?;
    if let Err(error) = validate_clone_bundle(&prepared.files) {
        tracing::error!(
            ?error,
            "legacy migration bundle failed publication validation"
        );
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Legacy workflow bundle validation failed"
        )));
    }
    let publish_id = workflow_id.clone();
    let source_revision = accepted.revision;
    let source_digest = accepted.content_digest.clone();
    let publication = tokio::task::spawn_blocking(move || {
        publish_builtin_clone(
            &trusted_root,
            &publish_id,
            source_revision,
            &source_digest,
            &prepared.files,
        )
    })
    .await
    .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    match publication {
        Ok(_) => {}
        Err(ClonePublicationError::Conflict(reason)) => {
            tracing::warn!(workflow_id, reason, "legacy workflow migration conflict");
            return Ok(crate::error::json_error(
                StatusCode::CONFLICT,
                "Workflow migration conflicts with an existing target Skill",
            ));
        }
        Err(ClonePublicationError::Io(error)) => {
            tracing::error!(%error, workflow_id, "legacy workflow publication failed");
            return Err(AppError::InternalError(anyhow::anyhow!(
                "Legacy workflow publication failed"
            )));
        }
        Err(ClonePublicationError::Internal(reason)) => {
            tracing::error!(
                workflow_id,
                reason,
                "legacy workflow publication invariant failed"
            );
            return Err(AppError::InternalError(anyhow::anyhow!(
                "Legacy workflow publication failed"
            )));
        }
    }

    store
        .reload()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    let (published_skills, published_workflows) = store.command_catalog_snapshots().await;
    let published = published_skills.entries.iter().any(|entry| {
        entry.id == workflow_id
            && entry.source == published_source
            && entry.status == WorkflowStatus::Valid
            && entry.migration_status == Some(LegacyWorkflowMigrationStatus::Migrated)
    });
    if !published {
        return Err(AppError::InternalError(anyhow::anyhow!(
            "Migrated workflow was not published in its canonical target layer"
        )));
    }
    Ok(HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .json(MigrateWorkflowResponse {
            workflow_id,
            outcome: LegacyWorkflowMigrationOutcome::Migrated,
            source_preserved: true,
            catalog_revision: published_skills.revision.max(published_workflows.revision),
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
