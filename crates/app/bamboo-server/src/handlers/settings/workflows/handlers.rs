use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use actix_web::{http::StatusCode, web, HttpResponse};
use bamboo_skills::legacy::LegacyWorkflowMigrationOutcome;
use bamboo_skills::{
    LegacyWorkflowMigrationStatus, SkillStore, WorkflowCatalogEntry, WorkflowCatalogSnapshot,
    WorkflowSource, WorkflowStatus,
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

#[cfg(test)]
pub(crate) mod clone_scope_test_hooks {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};

    use tokio::sync::Semaphore;

    pub(crate) struct CloneScopeHook {
        pub(crate) reached: Semaphore,
        pub(crate) resume: Semaphore,
    }

    fn hooks() -> &'static Mutex<HashMap<String, Arc<CloneScopeHook>>> {
        static HOOKS: OnceLock<Mutex<HashMap<String, Arc<CloneScopeHook>>>> = OnceLock::new();
        HOOKS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub(crate) fn install(session_id: &str) -> Arc<CloneScopeHook> {
        let hook = Arc::new(CloneScopeHook {
            reached: Semaphore::new(0),
            resume: Semaphore::new(0),
        });
        hooks()
            .lock()
            .expect("clone scope hooks lock")
            .insert(session_id.to_string(), hook.clone());
        hook
    }

    pub(crate) async fn pause_after_authoritative_scope(session_id: &str) {
        let hook = hooks()
            .lock()
            .expect("clone scope hooks lock")
            .remove(session_id);
        if let Some(hook) = hook {
            hook.reached.add_permits(1);
            hook.resume
                .acquire()
                .await
                .expect("clone scope test barrier remains open")
                .forget();
        }
    }
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
    resolve_workflow_scope_from_session(app_state, session_id, &session).await
}

async fn resolve_workflow_scope_from_session(
    app_state: &AppState,
    session_id: &str,
    session: &bamboo_agent_core::Session,
) -> Result<ResolvedWorkflowScope, AppError> {
    let project_id =
        match bamboo_engine::project_context::ProjectContextResolver::session_project_identity(
            session,
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
        let project = app_state.project_store.get(project_id).map_err(|error| {
            AppError::BadRequest(format!("Assigned Project is unavailable: {error}"))
        })?;
        if project.status != bamboo_domain::ProjectStatus::Active {
            return Err(AppError::Forbidden(format!(
                "Session '{session_id}' is assigned to archived Project '{project_id}'"
            )));
        }
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
    let (skill_catalog, workflow_catalog) = scope.store.command_catalog_snapshots().await;
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
) -> Result<BTreeMap<String, bamboo_skills::store::builtin::BuiltinSkillFile>, AppError> {
    let mut files = bundle
        .files
        .iter()
        .map(|(path, bytes)| (path.clone(), bytes.clone()))
        .collect::<BTreeMap<_, _>>();
    let markdown = bamboo_skills::store::parser::render_skill_markdown(&bundle.skill)
        .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    files.insert(
        "SKILL.md".to_string(),
        bamboo_skills::store::builtin::BuiltinSkillFile {
            bytes: markdown.into_bytes(),
            executable: false,
        },
    );
    Ok(files)
}

fn clone_bundle_digest(
    files: &BTreeMap<String, bamboo_skills::store::builtin::BuiltinSkillFile>,
) -> String {
    let mut digest = Sha256::new();
    for (path, file) in files {
        digest.update((path.len() as u64).to_le_bytes());
        digest.update(path.as_bytes());
        digest.update([u8::from(file.executable)]);
        digest.update((file.bytes.len() as u64).to_le_bytes());
        digest.update(&file.bytes);
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
                return Err(ClonePublicationError::Conflict(
                    "clone target resource parent is not a real directory".to_string(),
                ));
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
    embedded: &bamboo_skills::store::builtin::BuiltinSkillFile,
) -> Result<(), ClonePublicationError> {
    let relative_path = checked_clone_relative_path(relative)?;
    ensure_clone_parent(target_root, &relative_path)?;
    let target = target_root.join(&relative_path);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&target)
    {
        Ok(mut output) => {
            use std::io::Write;
            output.write_all(&embedded.bytes)?;
            output.sync_all()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = std::fs::symlink_metadata(&target)?;
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || std::fs::read(&target)? != embedded.bytes
            {
                return Err(ClonePublicationError::Conflict(format!(
                    "clone target resource '{relative}' already exists with different content"
                )));
            }
        }
        Err(error) => return Err(error.into()),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            &target,
            std::fs::Permissions::from_mode(if embedded.executable { 0o755 } else { 0o644 }),
        )?;
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
    files: &BTreeMap<String, bamboo_skills::store::builtin::BuiltinSkillFile>,
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
            #[cfg(unix)]
            let executable = {
                use std::os::unix::fs::PermissionsExt;
                std::fs::metadata(entry.path())?.permissions().mode() & 0o111 != 0
            };
            #[cfg(not(unix))]
            let executable = files
                .get(&relative)
                .is_some_and(|expected| expected.executable);
            actual.insert(
                relative,
                bamboo_skills::store::builtin::BuiltinSkillFile {
                    bytes: std::fs::read(entry.path())?,
                    executable,
                },
            );
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
            let partial = std::fs::read(&temporary)?;
            if !marker_bytes.starts_with(&partial) {
                return Err(ClonePublicationError::Conflict(
                    "Workflow clone temporary marker contains divergent data".to_string(),
                ));
            }
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
    if let Err(error) = rename_noreplace(&temporary, marker_path) {
        if std::fs::symlink_metadata(marker_path).is_ok() {
            return Err(ClonePublicationError::Conflict(
                "Workflow clone recovery marker appeared concurrently".to_string(),
            ));
        }
        return Err(error.into());
    }
    sync_clone_directory(skills_dir)?;
    Ok(())
}

fn publish_builtin_clone_blocking(
    skills_dir: &Path,
    workflow_id: &str,
    source_revision: u64,
    files: &BTreeMap<String, bamboo_skills::store::builtin::BuiltinSkillFile>,
) -> Result<(), ClonePublicationError> {
    publish_builtin_clone_blocking_with_before_publish(
        skills_dir,
        workflow_id,
        source_revision,
        files,
        |_| {},
    )
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn rename_noreplace(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let from = CString::new(from.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let to = CString::new(to.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    // SAFETY: both C strings are owned for the duration of the call. renameat2
    // performs the publication atomically and RENAME_NOREPLACE makes a
    // check-to-rename target race fail instead of replacing user content.
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_vendor = "apple")]
fn rename_noreplace(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let from = CString::new(from.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    let to = CString::new(to.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    // SAFETY: both pointers reference live C strings and RENAME_EXCL is the
    // Darwin atomic no-replace primitive.
    let result = unsafe { libc::renamex_np(from.as_ptr(), to.as_ptr(), libc::RENAME_EXCL) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn rename_noreplace(from: &Path, to: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    #[link(name = "Kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let from = from
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let to = to
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: both UTF-16 buffers are NUL-terminated and live for the call.
    // Passing zero flags deliberately omits MOVEFILE_REPLACE_EXISTING, so a
    // concurrent destination is never overwritten (unlike std::fs::rename).
    let result = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), 0) };
    if result != 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_vendor = "apple"))
))]
fn rename_noreplace(_from: &Path, _to: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic no-replace directory publication is unavailable on this platform",
    ))
}

fn publish_builtin_clone_blocking_with_before_publish<F>(
    skills_dir: &Path,
    workflow_id: &str,
    source_revision: u64,
    files: &BTreeMap<String, bamboo_skills::store::builtin::BuiltinSkillFile>,
    before_publish: F,
) -> Result<(), ClonePublicationError>
where
    F: FnOnce(&Path),
{
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
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    // Build outside the recursively scanned skills root, then atomically rename
    // the complete tree into place. A crash at any individual file write only
    // damages server-owned staging bytes, which the exact marker authorizes us
    // to discard and rebuild; clients never observe a partial Workflow bundle.
    let staging_parent = clone_staging_parent(skills_dir)?;
    let staging = staging_parent.join(format!("{workflow_id}.clone-v1"));
    if !marker_exists {
        match std::fs::symlink_metadata(&staging) {
            Ok(_) => {
                return Err(ClonePublicationError::Conflict(
                    "Workflow clone found unclaimed staging data; refusing to overwrite it"
                        .to_string(),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                publish_clone_marker(skills_dir, workflow_id, &marker_path, &marker_bytes)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
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
    for (relative, file) in files.iter().filter(|(path, _)| path.as_str() != "SKILL.md") {
        write_clone_file(&staging, relative, file)?;
    }
    write_clone_file(&staging, "SKILL.md", skill_markdown)?;
    verify_clone_tree(&staging, workflow_id, files)?;
    sync_clone_tree_directories(&staging)?;
    before_publish(&target);
    if let Err(error) = rename_noreplace(&staging, &target) {
        if std::fs::symlink_metadata(&target).is_ok() {
            return Err(ClonePublicationError::Conflict(format!(
                "Workflow '{workflow_id}' appeared in the target layer while cloning"
            )));
        }
        return Err(error.into());
    }
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
    // A Project clone derives its destination exclusively from the current
    // durable session. Hold the same per-session lock used by Project/Workspace
    // PATCH through the final publication and catalog reload: a reassignment
    // can therefore happen wholly before or wholly after this clone, never in
    // the scope-resolution -> filesystem-publication gap.
    let (scope_guard, scope) = match payload.target {
        CloneWorkflowTarget::Project => {
            let session_id = payload
                .session_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .expect("Project session_id was validated above");
            let guard = app_state.persistence.acquire_lock(session_id).await;
            let session = app_state
                .persistence
                .storage()
                .load_session(session_id)
                .await
                .map_err(AppError::StorageError)?
                .ok_or_else(|| AppError::NotFound(format!("Session '{session_id}'")))?;
            let scope =
                resolve_workflow_scope_from_session(&app_state, session_id, &session).await?;
            #[cfg(test)]
            clone_scope_test_hooks::pause_after_authoritative_scope(session_id).await;
            (Some(guard), scope)
        }
        CloneWorkflowTarget::User => (None, resolve_workflow_scope(&app_state, None).await?),
    };
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
    let (publication, scope_guard) = tokio::task::spawn_blocking(move || {
        let publication =
            publish_builtin_clone_blocking(&publish_skills_dir, &publish_id, revision, &files);
        // Keep Project/Workspace reassignment serialized even if the request
        // future is cancelled while this non-cancellable filesystem task runs.
        (publication, scope_guard)
    })
    .await
    .map_err(|error| AppError::InternalError(anyhow::anyhow!(error)))?;
    match publication {
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
    // On the ordinary path retain the returned guard through both catalog
    // reloads and the correlated response snapshot. On request cancellation,
    // the blocking task owns it until publication finishes and then dropping
    // the unobserved output releases it.
    drop(scope_guard);
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
    let _scope_guard = app_state.persistence.acquire_lock(session_id).await;
    let session = app_state
        .persistence
        .storage()
        .load_session(session_id)
        .await
        .map_err(AppError::StorageError)?
        .ok_or_else(|| AppError::NotFound(format!("Session '{session_id}'")))?;
    let scope = resolve_workflow_scope_from_session(&app_state, session_id, &session).await?;
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

    fn embedded_file(
        bytes: &[u8],
        executable: bool,
    ) -> bamboo_skills::store::builtin::BuiltinSkillFile {
        bamboo_skills::store::builtin::BuiltinSkillFile {
            bytes: bytes.to_vec(),
            executable,
        }
    }

    fn resumable_files() -> BTreeMap<String, bamboo_skills::store::builtin::BuiltinSkillFile> {
        BTreeMap::from([
            (
                "SKILL.md".to_string(),
                embedded_file(
                    b"---\nid: resumable\nname: Resumable\ndescription: exact recovery\n---\n\nInstructions\n",
                    false,
                ),
            ),
            (
                "references/contract.txt".to_string(),
                embedded_file(b"immutable contract\n", false),
            ),
            (
                "scripts/run.sh".to_string(),
                embedded_file(b"#!/bin/sh\nexit 0\n", true),
            ),
            (
                "scripts/helpers.py".to_string(),
                embedded_file(b"HELPER = True\n", false),
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
            files["SKILL.md"].bytes
        );
        assert!(!skills_dir.join(".resumable.clone-v1.json.tmp").exists());
        assert!(!skills_dir.join(".resumable.clone-v1.json").exists());
    }

    #[test]
    fn builtin_clone_preserves_divergent_temporary_marker_data() {
        let temporary = tempfile::tempdir().expect("temporary clone root");
        let skills_dir = temporary.path().join("skills");
        std::fs::create_dir(&skills_dir).expect("skills directory");
        let temporary_marker = skills_dir.join(".resumable.clone-v1.json.tmp");
        std::fs::write(&temporary_marker, b"unrelated writer data").expect("divergent marker data");

        let error = publish_builtin_clone_blocking(&skills_dir, "resumable", 7, &resumable_files())
            .expect_err("divergent hidden data must never be deleted as a crash remnant");

        assert!(matches!(error, ClonePublicationError::Conflict(_)));
        assert_eq!(
            std::fs::read(&temporary_marker).expect("divergent marker remains"),
            b"unrelated writer data"
        );
        assert!(!skills_dir.join(".resumable.clone-v1.json").exists());
        assert!(!skills_dir.join("resumable").exists());
    }

    #[test]
    fn builtin_clone_preserves_staging_without_an_authoritative_marker() {
        let temporary = tempfile::tempdir().expect("temporary clone root");
        let skills_dir = temporary.path().join("skills");
        std::fs::create_dir(&skills_dir).expect("skills directory");
        let staging = temporary
            .path()
            .join(".workflow-clone-staging/resumable.clone-v1");
        std::fs::create_dir_all(&staging).expect("unclaimed staging");
        let sentinel = staging.join("user-sentinel.txt");
        std::fs::write(&sentinel, b"must remain").expect("unclaimed staging data");

        let error = publish_builtin_clone_blocking(&skills_dir, "resumable", 7, &resumable_files())
            .expect_err("staging without a durable marker is not server-owned");

        assert!(matches!(error, ClonePublicationError::Conflict(_)));
        assert_eq!(
            std::fs::read(&sentinel).expect("unclaimed staging remains"),
            b"must remain"
        );
        assert!(!skills_dir.join(".resumable.clone-v1.json").exists());
        assert!(!skills_dir.join("resumable").exists());
    }

    #[cfg(unix)]
    #[test]
    fn builtin_clone_preserves_and_verifies_executable_modes() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = tempfile::tempdir().expect("temporary clone root");
        let skills_dir = temporary.path().join("skills");
        std::fs::create_dir(&skills_dir).expect("skills directory");
        let files = resumable_files();
        publish_builtin_clone_blocking(&skills_dir, "resumable", 7, &files)
            .expect("publish exact modes");
        let target = skills_dir.join("resumable");
        assert_ne!(
            std::fs::metadata(target.join("scripts/run.sh"))
                .expect("script metadata")
                .permissions()
                .mode()
                & 0o111,
            0
        );
        assert_eq!(
            std::fs::metadata(target.join("scripts/helpers.py"))
                .expect("helper metadata")
                .permissions()
                .mode()
                & 0o111,
            0
        );

        let marker = ClonePublicationMarker {
            schema: 1,
            workflow_id: "resumable".to_string(),
            source_revision: 7,
            digest: clone_bundle_digest(&files),
        };
        std::fs::write(
            skills_dir.join(".resumable.clone-v1.json"),
            serde_json::to_vec(&marker).expect("marker JSON"),
        )
        .expect("recovery marker");
        std::fs::set_permissions(
            target.join("scripts/helpers.py"),
            std::fs::Permissions::from_mode(0o755),
        )
        .expect("inject mode drift");
        let error = publish_builtin_clone_blocking(&skills_dir, "resumable", 7, &files)
            .expect_err("mode drift must not be accepted as exact recovery");
        assert!(matches!(error, ClonePublicationError::Conflict(_)));
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
            files["SKILL.md"].bytes
        );
        assert_eq!(
            std::fs::read(target.join("references/contract.txt")).expect("published resource"),
            files["references/contract.txt"].bytes
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

    #[test]
    fn builtin_clone_does_not_replace_directory_created_during_publish() {
        let temporary = tempfile::tempdir().expect("temporary clone root");
        let skills_dir = temporary.path().join("skills");
        std::fs::create_dir(&skills_dir).expect("skills directory");
        let files = resumable_files();
        let target = skills_dir.join("resumable");

        let error = publish_builtin_clone_blocking_with_before_publish(
            &skills_dir,
            "resumable",
            7,
            &files,
            |publish_target| std::fs::create_dir(publish_target).expect("racing target"),
        )
        .expect_err("racing target must win without replacement");

        assert!(matches!(error, ClonePublicationError::Conflict(_)));
        assert!(target.is_dir());
        assert!(std::fs::read_dir(&target)
            .expect("racing directory")
            .next()
            .is_none());
    }

    #[test]
    fn clone_publication_never_replaces_a_concurrent_marker() {
        let temporary = tempfile::tempdir().expect("temporary clone root");
        let staging = temporary.path().join("marker.tmp");
        let marker = temporary.path().join("marker.json");
        std::fs::write(&staging, b"server marker").expect("staging marker");
        std::fs::write(&marker, b"concurrent writer").expect("racing marker");

        rename_noreplace(&staging, &marker).expect_err("existing marker must win");

        assert_eq!(
            std::fs::read(&marker).expect("racing marker remains"),
            b"concurrent writer"
        );
        assert_eq!(
            std::fs::read(&staging).expect("server staging remains recoverable"),
            b"server marker"
        );
    }

    #[cfg(unix)]
    #[test]
    fn builtin_clone_does_not_replace_symlink_created_during_publish() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("temporary clone root");
        let skills_dir = temporary.path().join("skills");
        std::fs::create_dir(&skills_dir).expect("skills directory");
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&outside).expect("outside directory");
        let files = resumable_files();
        let target = skills_dir.join("resumable");

        let error = publish_builtin_clone_blocking_with_before_publish(
            &skills_dir,
            "resumable",
            7,
            &files,
            |publish_target| symlink(&outside, publish_target).expect("racing symlink"),
        )
        .expect_err("racing symlink must win without replacement");

        assert!(matches!(error, ClonePublicationError::Conflict(_)));
        assert!(std::fs::symlink_metadata(&target)
            .expect("racing target")
            .file_type()
            .is_symlink());
        assert!(std::fs::read_dir(&outside)
            .expect("outside directory")
            .next()
            .is_none());
    }
}
