use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use actix_web::{http::StatusCode, web, HttpResponse};
use bamboo_skills::legacy::LegacyWorkflowMigrationOutcome;
use bamboo_skills::{
    LegacyWorkflowMigrationStatus, SkillStore, WorkflowCatalogEntry, WorkflowSource, WorkflowStatus,
};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::{app_state::AppState, error::AppError};

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

#[derive(Clone)]
struct ResolvedWorkflowScope {
    store: Arc<SkillStore>,
    project_home: Option<PathBuf>,
    workspace: Option<PathBuf>,
}

async fn resolve_workflow_scope(
    app_state: &AppState,
    session_id: Option<&str>,
) -> Result<ResolvedWorkflowScope, AppError> {
    let Some(session_id) = session_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(ResolvedWorkflowScope {
            store: app_state
                .skill_manager
                .store_for_workspace(None)
                .await
                .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?,
            project_home: None,
            workspace: None,
        });
    };
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
    })?;
    let project_home = if let Some(project_id) = project_id.as_ref() {
        app_state.project_store.get(project_id).map_err(|error| {
            AppError::BadRequest(format!("Assigned Project is unavailable: {error}"))
        })?;
        Some(app_state.project_store.paths().project_home(project_id))
    } else {
        None
    };
    let store = match (project_id.as_ref(), project_home.as_ref()) {
        (Some(project_id), Some(project_home)) => {
            app_state
                .skill_manager
                .store_for_project_workspace(project_id, project_home, workspace.as_deref())
                .await
        }
        _ => {
            app_state
                .skill_manager
                .store_for_workspace(workspace.as_deref())
                .await
        }
    }
    .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    Ok(ResolvedWorkflowScope {
        store,
        project_home,
        workspace,
    })
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
    let scope = resolve_workflow_scope(&app_state, query.session_id.as_deref()).await?;
    let snapshot = scope.store.workflow_catalog_snapshot().await;
    Ok(HttpResponse::Ok()
        .insert_header(("Cache-Control", "no-store"))
        .json(snapshot.public_workflows()))
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct ClonePublicationMarker {
    schema: u8,
    workflow_id: String,
    source_revision: u64,
    digest: String,
}

#[derive(Debug)]
enum ClonePublicationError {
    Conflict(String),
    Io(std::io::Error),
    Internal(String),
}

impl From<std::io::Error> for ClonePublicationError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

async fn confined_skills_dir(root: &Path) -> Result<PathBuf, AppError> {
    let metadata = tokio::fs::symlink_metadata(root).await?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(AppError::Forbidden(
            "Workflow clone root must be a real directory".to_string(),
        ));
    }
    let canonical_root = tokio::fs::canonicalize(root).await?;
    let skills_dir = root.join("skills");
    match tokio::fs::symlink_metadata(&skills_dir).await {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(AppError::Forbidden(
                "Workflow clone target must be a real skills directory".to_string(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir(&skills_dir).await?;
        }
        Err(error) => return Err(AppError::StorageError(error)),
    }
    let canonical_skills = tokio::fs::canonicalize(&skills_dir).await?;
    if !canonical_skills.starts_with(&canonical_root) {
        return Err(AppError::Forbidden(
            "Workflow clone target escapes its trusted root".to_string(),
        ));
    }
    Ok(canonical_skills)
}

fn clone_bundle_files(
    bundle: &bamboo_skills::store::builtin::BuiltinSkillBundle,
) -> Result<BTreeMap<String, Vec<u8>>, AppError> {
    let mut files = bundle
        .files
        .iter()
        .map(|(path, bytes)| (path.clone(), bytes.clone()))
        .collect::<BTreeMap<_, _>>();
    let markdown = bamboo_skills::store::parser::render_skill_markdown(&bundle.skill)
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    files.insert("SKILL.md".to_string(), markdown.into_bytes());
    Ok(files)
}

fn clone_bundle_digest(files: &BTreeMap<String, Vec<u8>>) -> String {
    let mut digest = Sha256::new();
    for (path, bytes) in files {
        digest.update((path.len() as u64).to_le_bytes());
        digest.update(path.as_bytes());
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    hex::encode(digest.finalize())
}

fn checked_clone_relative_path(path: &str) -> Result<PathBuf, ClonePublicationError> {
    let path = Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ClonePublicationError::Internal(
            "embedded Workflow bundle contains an unsafe resource path".to_string(),
        ));
    }
    Ok(path.to_path_buf())
}

fn ensure_clone_parent(root: &Path, relative: &Path) -> Result<(), ClonePublicationError> {
    let Some(parent) = relative.parent() else {
        return Ok(());
    };
    let mut current = root.to_path_buf();
    for component in parent.components() {
        let Component::Normal(component) = component else {
            return Err(ClonePublicationError::Internal(
                "embedded Workflow bundle contains an unsafe resource parent".to_string(),
            ));
        };
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(ClonePublicationError::Conflict(format!(
                    "clone target '{}' is not a real directory",
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&current)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn write_clone_file(
    target_root: &Path,
    relative: &str,
    bytes: &[u8],
) -> Result<(), ClonePublicationError> {
    let relative_path = checked_clone_relative_path(relative)?;
    ensure_clone_parent(target_root, &relative_path)?;
    let target = target_root.join(&relative_path);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&target)
    {
        Ok(mut file) => {
            use std::io::Write;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = std::fs::symlink_metadata(&target)?;
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || std::fs::read(&target)? != bytes
            {
                return Err(ClonePublicationError::Conflict(format!(
                    "clone target resource '{relative}' already exists with different content"
                )));
            }
        }
        Err(error) => return Err(error.into()),
    }
    #[cfg(unix)]
    if relative.starts_with("scripts/") {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))?;
        std::fs::File::open(&target)?.sync_all()?;
    }
    Ok(())
}

fn sync_clone_directory(path: &Path) -> Result<(), ClonePublicationError> {
    #[cfg(unix)]
    std::fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn verify_clone_tree(
    target: &Path,
    workflow_id: &str,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<(), ClonePublicationError> {
    let mut actual = BTreeMap::new();
    for entry in walkdir::WalkDir::new(target).follow_links(false) {
        let entry = entry.map_err(|error| ClonePublicationError::Internal(error.to_string()))?;
        if entry.path() == target {
            continue;
        }
        if entry.file_type().is_symlink() {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone target contains a symbolic link".to_string(),
            ));
        }
        if entry.file_type().is_file() {
            let relative = entry
                .path()
                .strip_prefix(target)
                .map_err(|error| ClonePublicationError::Internal(error.to_string()))?
                .to_string_lossy()
                .replace('\\', "/");
            actual.insert(relative, std::fs::read(entry.path())?);
        } else if !entry.file_type().is_dir() {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone target contains a non-regular resource".to_string(),
            ));
        }
    }
    if actual != *files {
        return Err(ClonePublicationError::Conflict(format!(
            "Workflow clone target '{workflow_id}' contains divergent resources"
        )));
    }
    Ok(())
}

fn sync_clone_tree_directories(root: &Path) -> Result<(), ClonePublicationError> {
    #[cfg(unix)]
    {
        let mut directories = walkdir::WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_dir())
            .map(|entry| entry.into_path())
            .collect::<Vec<_>>();
        directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for directory in directories {
            sync_clone_directory(&directory)?;
        }
    }
    #[cfg(not(unix))]
    let _ = root;
    Ok(())
}

fn clone_staging_parent(skills_dir: &Path) -> Result<PathBuf, ClonePublicationError> {
    let trusted_root = skills_dir.parent().ok_or_else(|| {
        ClonePublicationError::Internal(
            "Workflow clone skills directory has no trusted parent".to_string(),
        )
    })?;
    let canonical_trusted_root = std::fs::canonicalize(trusted_root)?;
    let staging_parent = canonical_trusted_root.join(".workflow-clone-staging");
    match std::fs::symlink_metadata(&staging_parent) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone staging root is not a real directory".to_string(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(&staging_parent)?;
            sync_clone_directory(&canonical_trusted_root)?;
        }
        Err(error) => return Err(error.into()),
    }
    let canonical = std::fs::canonicalize(&staging_parent)?;
    if !canonical.starts_with(&canonical_trusted_root) {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone staging root escapes its trusted directory".to_string(),
        ));
    }
    Ok(canonical)
}

fn publish_clone_marker(
    skills_dir: &Path,
    workflow_id: &str,
    marker_path: &Path,
    marker_bytes: &[u8],
) -> Result<(), ClonePublicationError> {
    let temporary = skills_dir.join(format!(".{workflow_id}.clone-v1.json.tmp"));
    match std::fs::symlink_metadata(&temporary) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone temporary marker is not a regular file".to_string(),
            ));
        }
        Ok(_) => {
            // No authoritative marker or target exists while the clone lock is
            // held, so this can only be an uncommitted pre-rename crash remnant.
            std::fs::remove_file(&temporary)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut marker_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    use std::io::Write;
    marker_file.write_all(marker_bytes)?;
    marker_file.sync_all()?;
    drop(marker_file);
    std::fs::rename(&temporary, marker_path)?;
    sync_clone_directory(skills_dir)?;
    Ok(())
}

fn publish_builtin_clone_blocking(
    skills_dir: &Path,
    workflow_id: &str,
    source_revision: u64,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<(), ClonePublicationError> {
    let skill_markdown = files.get("SKILL.md").ok_or_else(|| {
        ClonePublicationError::Internal("embedded Workflow bundle is missing SKILL.md".to_string())
    })?;
    for relative in files.keys() {
        checked_clone_relative_path(relative)?;
    }
    let lock_path = skills_dir.join(".workflow-clone.lock");
    if std::fs::symlink_metadata(&lock_path)
        .is_ok_and(|metadata| metadata.file_type().is_symlink() || !metadata.is_file())
    {
        return Err(ClonePublicationError::Conflict(
            "Workflow clone lock path is not a regular file".to_string(),
        ));
    }
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)?;
    FileExt::lock_exclusive(&lock)?;

    let marker = ClonePublicationMarker {
        schema: 1,
        workflow_id: workflow_id.to_string(),
        source_revision,
        digest: clone_bundle_digest(files),
    };
    let marker_bytes = serde_json::to_vec(&marker)
        .map_err(|error| ClonePublicationError::Internal(error.to_string()))?;
    let marker_path = skills_dir.join(format!(".{workflow_id}.clone-v1.json"));
    let target = skills_dir.join(workflow_id);

    let marker_exists = match std::fs::symlink_metadata(&marker_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(ClonePublicationError::Conflict(
                    "Workflow clone recovery marker is not a regular file".to_string(),
                ));
            }
            if std::fs::read(&marker_path)? != marker_bytes {
                return Err(ClonePublicationError::Conflict(format!(
                    "a different clone publication for '{workflow_id}' requires recovery"
                )));
            }
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    match std::fs::symlink_metadata(&target) {
        Ok(metadata) => {
            if !marker_exists {
                return Err(ClonePublicationError::Conflict(format!(
                    "Workflow '{workflow_id}' already exists in the target layer"
                )));
            }
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(ClonePublicationError::Conflict(format!(
                    "Workflow clone target '{workflow_id}' is not a real directory"
                )));
            }
            // A crash may occur after the atomic directory rename but before
            // marker removal. Exact bytes prove the publication completed;
            // only then is it safe to acknowledge and clear recovery state.
            verify_clone_tree(&target, workflow_id, files)?;
            std::fs::remove_file(&marker_path)?;
            sync_clone_directory(skills_dir)?;
            FileExt::unlock(&lock)?;
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if !marker_exists {
                publish_clone_marker(skills_dir, workflow_id, &marker_path, &marker_bytes)?;
            }
        }
        Err(error) => return Err(error.into()),
    }

    // Build outside the recursively scanned skills root, then atomically rename
    // the complete tree into place. A crash at any individual file write only
    // damages server-owned staging bytes, which the exact marker authorizes us
    // to discard and rebuild; clients never observe a partial Workflow bundle.
    let staging_parent = clone_staging_parent(skills_dir)?;
    let staging = staging_parent.join(format!("{workflow_id}.clone-v1"));
    match std::fs::symlink_metadata(&staging) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone staging target is not a real directory".to_string(),
            ));
        }
        Ok(_) => std::fs::remove_dir_all(&staging)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    std::fs::create_dir(&staging)?;
    for (relative, bytes) in files.iter().filter(|(path, _)| path.as_str() != "SKILL.md") {
        write_clone_file(&staging, relative, bytes)?;
    }
    write_clone_file(&staging, "SKILL.md", skill_markdown)?;
    verify_clone_tree(&staging, workflow_id, files)?;
    sync_clone_tree_directories(&staging)?;
    std::fs::rename(&staging, &target)?;
    sync_clone_directory(skills_dir)?;
    verify_clone_tree(&target, workflow_id, files)?;
    std::fs::remove_file(&marker_path)?;
    sync_clone_directory(skills_dir)?;
    FileExt::unlock(&lock)?;
    Ok(())
}

fn exact_catalog_entry<'a>(
    skill_entries: &'a [WorkflowCatalogEntry],
    workflow_entries: &'a [WorkflowCatalogEntry],
    workflow_id: &str,
    source: WorkflowSource,
) -> Option<&'a WorkflowCatalogEntry> {
    skill_entries
        .iter()
        .chain(workflow_entries)
        .find(|entry| entry.id == workflow_id && entry.source == source && entry.winner)
}

/// Clone one immutable builtin bundle into the current Project or user layer.
/// Client-controlled filesystem paths are never accepted, the selected source
/// revision must still be exact, and an existing target is never overwritten.
pub async fn clone_workflow(
    app_state: web::Data<AppState>,
    workflow_id: web::Path<String>,
    payload: web::Json<CloneWorkflowRequest>,
) -> Result<HttpResponse, AppError> {
    let workflow_id = workflow_id.into_inner();
    if !is_safe_workflow_name(&workflow_id) || payload.revision == 0 {
        return Err(AppError::BadRequest(
            "A safe workflow id and positive revision are required".to_string(),
        ));
    }
    if payload.source != WorkflowSource::Builtin {
        return Err(AppError::BadRequest(
            "Only read-only builtin Workflows can be cloned".to_string(),
        ));
    }
    if payload.target == CloneWorkflowTarget::Project
        && payload
            .session_id
            .as_deref()
            .map(str::trim)
            .is_none_or(str::is_empty)
    {
        return Err(AppError::BadRequest(
            "session_id is required for a Project clone".to_string(),
        ));
    }
    let scope_session_id = match payload.target {
        CloneWorkflowTarget::Project => payload.session_id.as_deref(),
        CloneWorkflowTarget::User => None,
    };
    let scope = resolve_workflow_scope(&app_state, scope_session_id).await?;
    scope
        .store
        .reload()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    let (skill_catalog, workflow_catalog) = scope.store.command_catalog_snapshots().await;
    let entry = exact_catalog_entry(
        &skill_catalog.entries,
        &workflow_catalog.entries,
        &workflow_id,
        payload.source,
    );
    let Some(entry) = entry else {
        let known_but_shadowed = skill_catalog
            .entries
            .iter()
            .chain(&workflow_catalog.entries)
            .any(|entry| {
                entry.id == workflow_id
                    && (entry.source == payload.source
                        || entry
                            .shadowed_candidates
                            .iter()
                            .any(|candidate| candidate.source == payload.source))
            });
        if known_but_shadowed {
            return Ok(crate::error::json_error(
                StatusCode::CONFLICT,
                format!(
                    "Builtin Workflow '{workflow_id}' is shadowed; refresh the catalog before cloning"
                ),
            ));
        }
        return Err(AppError::NotFound(format!(
            "Builtin Workflow '{workflow_id}'"
        )));
    };
    if entry.status != WorkflowStatus::Valid || entry.revision != payload.revision {
        return Ok(crate::error::json_error(
            StatusCode::CONFLICT,
            format!(
                "Builtin Workflow '{workflow_id}' changed or became invalid; refresh the catalog"
            ),
        ));
    }
    let bundles = bamboo_skills::store::builtin::load_builtin_skill_bundles()
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    let bundle = bundles
        .iter()
        .find(|bundle| bundle.skill.id == workflow_id)
        .ok_or_else(|| AppError::NotFound(format!("Builtin Workflow '{workflow_id}'")))?;
    let target_root = match payload.target {
        CloneWorkflowTarget::Project => scope.project_home.as_deref().ok_or_else(|| {
            AppError::BadRequest(
                "Project clone requires a session assigned to an active Project".to_string(),
            )
        })?,
        CloneWorkflowTarget::User => app_state.app_data_dir.as_path(),
    };
    let skills_dir = confined_skills_dir(target_root).await?;
    let files = clone_bundle_files(bundle)?;
    let publish_skills_dir = skills_dir.clone();
    let publish_id = workflow_id.clone();
    let revision = payload.revision;
    match tokio::task::spawn_blocking(move || {
        publish_builtin_clone_blocking(&publish_skills_dir, &publish_id, revision, &files)
    })
    .await
    .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?
    {
        Ok(()) => {}
        Err(ClonePublicationError::Conflict(message)) => {
            return Ok(crate::error::json_error(StatusCode::CONFLICT, message));
        }
        Err(ClonePublicationError::Io(error)) => return Err(AppError::StorageError(error)),
        Err(ClonePublicationError::Internal(message)) => {
            return Err(AppError::InternalError(anyhow::anyhow!(message)));
        }
    }

    // Reload both the global base and the session-scoped composite store so the
    // response and subsequent palette request observe the clone immediately.
    app_state
        .skill_manager
        .store()
        .reload()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    scope
        .store
        .reload()
        .await
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    let (skill_catalog, workflow_catalog) = scope.store.command_catalog_snapshots().await;
    let target_source = match payload.target {
        CloneWorkflowTarget::Project => WorkflowSource::Project,
        CloneWorkflowTarget::User => WorkflowSource::User,
    };
    let entry = exact_catalog_entry(
        &skill_catalog.entries,
        &workflow_catalog.entries,
        &workflow_id,
        target_source,
    )
    .cloned()
    .ok_or_else(|| {
        AppError::InternalError(anyhow::anyhow!(
            "cloned Workflow was not published in the target catalog"
        ))
    })?;
    let catalog_revision = skill_catalog.revision.max(workflow_catalog.revision);
    Ok(HttpResponse::Created()
        .insert_header(("Cache-Control", "no-store"))
        .json(CloneWorkflowResponse {
            workflow_id,
            target: payload.target,
            source_preserved: true,
            catalog_revision,
            entry,
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
    let scope = resolve_workflow_scope(&app_state, Some(session_id)).await?;
    let workspace = scope.workspace.as_ref().ok_or_else(|| {
        AppError::BadRequest("Legacy workflow migration requires a session workspace".to_string())
    })?;
    let store = scope.store;
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

#[cfg(test)]
mod clone_publication_tests {
    use super::*;

    fn resumable_files() -> BTreeMap<String, Vec<u8>> {
        BTreeMap::from([
            (
                "SKILL.md".to_string(),
                b"---\nid: resumable\nname: Resumable\ndescription: exact recovery\n---\n\nInstructions\n"
                    .to_vec(),
            ),
            (
                "references/contract.txt".to_string(),
                b"immutable contract\n".to_vec(),
            ),
        ])
    }

    #[test]
    fn builtin_clone_recovers_uncommitted_partial_marker_write() {
        let temporary = tempfile::tempdir().expect("temporary clone root");
        let skills_dir = temporary.path().join("skills");
        std::fs::create_dir(&skills_dir).expect("skills directory");
        std::fs::write(
            skills_dir.join(".resumable.clone-v1.json.tmp"),
            b"{\"schema\":1",
        )
        .expect("partial marker write");
        let files = resumable_files();

        publish_builtin_clone_blocking(&skills_dir, "resumable", 7, &files)
            .expect("uncommitted marker temp is safely rebuilt");

        assert_eq!(
            std::fs::read(skills_dir.join("resumable/SKILL.md")).expect("published definition"),
            files["SKILL.md"]
        );
        assert!(!skills_dir.join(".resumable.clone-v1.json.tmp").exists());
        assert!(!skills_dir.join(".resumable.clone-v1.json").exists());
    }

    #[test]
    fn builtin_clone_resumes_partial_staging_and_post_rename_crashes_without_overwrite() {
        let temporary = tempfile::tempdir().expect("temporary clone root");
        let skills_dir = temporary.path().join("skills");
        std::fs::create_dir(&skills_dir).expect("skills directory");
        let files = resumable_files();
        let marker = ClonePublicationMarker {
            schema: 1,
            workflow_id: "resumable".to_string(),
            source_revision: 7,
            digest: clone_bundle_digest(&files),
        };
        std::fs::write(
            skills_dir.join(".resumable.clone-v1.json"),
            serde_json::to_vec(&marker).expect("marker serialization"),
        )
        .expect("recovery marker");
        let staging = temporary
            .path()
            .join(".workflow-clone-staging/resumable.clone-v1");
        std::fs::create_dir_all(staging.join("references")).expect("partial staging target");
        std::fs::write(
            staging.join("references/contract.txt"),
            b"partial resource write",
        )
        .expect("partial staged resource");
        let target = skills_dir.join("resumable");

        publish_builtin_clone_blocking(&skills_dir, "resumable", 7, &files)
            .expect("exact partial staging resumes");

        assert_eq!(
            std::fs::read(target.join("SKILL.md")).expect("published definition"),
            files["SKILL.md"]
        );
        assert_eq!(
            std::fs::read(target.join("references/contract.txt")).expect("published resource"),
            files["references/contract.txt"]
        );
        assert!(!skills_dir.join(".resumable.clone-v1.json").exists());
        assert!(!staging.exists());

        // Simulate a crash after the complete staging tree was atomically
        // renamed but before the durable marker was removed.
        std::fs::write(
            skills_dir.join(".resumable.clone-v1.json"),
            serde_json::to_vec(&marker).expect("marker serialization"),
        )
        .expect("post-rename recovery marker");
        publish_builtin_clone_blocking(&skills_dir, "resumable", 7, &files)
            .expect("complete post-rename publication is acknowledged exactly");
        assert!(!skills_dir.join(".resumable.clone-v1.json").exists());

        let before = std::fs::read(target.join("SKILL.md")).expect("definition before retry");
        let error = publish_builtin_clone_blocking(&skills_dir, "resumable", 7, &files)
            .expect_err("completed clone must never be overwritten");
        assert!(matches!(error, ClonePublicationError::Conflict(_)));
        assert_eq!(
            std::fs::read(target.join("SKILL.md")).expect("definition after retry"),
            before
        );
    }
}
