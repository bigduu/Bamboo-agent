//! Crash-safe, revisioned storage for first-class Bamboo Projects.
//!
//! `project.json` is authoritative. `projects/index.json` is a derived cache
//! rebuilt from manifests on startup or after every mutation.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;

use bamboo_domain::{
    LegacyProjectAssignment, LegacyProjectDryRunReport, LegacyProjectMatchBasis,
    LegacyProjectSuggestion, LegacyProjectUnassigned, LegacySessionProjectInput, ProjectId,
    ProjectIndex, ProjectIndexEntry, ProjectManifest, ProjectPathStatus, ProjectResourceEntry,
    ProjectResourceKind, ProjectResourceSummary, ProjectStatus, WorkspaceBinding,
    PROJECT_INDEX_SCHEMA_VERSION, PROJECT_MANIFEST_SCHEMA_VERSION,
};
use chrono::Utc;
use fs2::FileExt;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

mod legacy_memory;
pub use legacy_memory::{LegacyMemoryReadRoot, ProjectMemoryReadRoots};

const PROJECT_MANIFEST_FILE: &str = "project.json";
const PROJECT_MANIFEST_BACKUP_FILE: &str = "project.json.bak";
const PROJECT_MANIFEST_REVISION_FILE: &str = "manifest-revision";
const PROJECT_INDEX_FILE: &str = "index.json";

#[derive(Debug, Error)]
pub enum ProjectStoreError {
    #[error("project store I/O failed")]
    Io(#[from] std::io::Error),
    #[error("project document is invalid")]
    Json(#[from] serde_json::Error),
    #[error("project {0} was not found")]
    NotFound(ProjectId),
    #[error("project {0} already exists")]
    AlreadyExists(ProjectId),
    #[error("project revision conflict: expected {expected}, actual {actual}")]
    Conflict { expected: u64, actual: u64 },
    #[error("project {0} is not archived")]
    NotArchived(ProjectId),
    #[error(
        "project_path '{project_path}' cannot be unbound from Project '{project_id}'; select another Project path first"
    )]
    ProjectPathUnbindConflict {
        project_id: ProjectId,
        project_path: String,
    },
    #[error("project validation failed: {0}")]
    Validation(String),
    #[error("invalid project path component: {0}")]
    InvalidPathComponent(String),
}

pub type ProjectStoreResult<T> = Result<T, ProjectStoreError>;

/// Centralized paths rooted at `${BAMBOO_DATA_DIR}`.
#[derive(Debug, Clone)]
pub struct ProjectPaths {
    data_dir: PathBuf,
}

impl ProjectPaths {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn projects_dir(&self) -> PathBuf {
        self.data_dir.join("projects")
    }

    pub fn index_path(&self) -> PathBuf {
        self.projects_dir().join(PROJECT_INDEX_FILE)
    }

    pub fn project_home(&self, project_id: &ProjectId) -> PathBuf {
        self.projects_dir().join(project_id.as_str())
    }

    pub fn manifest_path(&self, project_id: &ProjectId) -> PathBuf {
        self.project_home(project_id).join(PROJECT_MANIFEST_FILE)
    }

    pub fn settings_path(&self, project_id: &ProjectId) -> PathBuf {
        self.project_home(project_id).join("settings.json")
    }

    pub fn skills_dir(
        &self,
        project_id: &ProjectId,
        mode: Option<&str>,
    ) -> ProjectStoreResult<PathBuf> {
        let name = match mode {
            None => "skills".to_string(),
            Some(mode) => {
                validate_component(mode)?;
                format!("skills-{mode}")
            }
        };
        Ok(self.project_home(project_id).join(name))
    }

    pub fn commands_dir(&self, project_id: &ProjectId) -> PathBuf {
        self.project_home(project_id).join("commands")
    }

    pub fn memory_v1_dir(&self, project_id: &ProjectId) -> PathBuf {
        self.project_home(project_id).join("memory").join("v1")
    }

    pub fn artifacts_dir(&self, project_id: &ProjectId) -> PathBuf {
        self.project_home(project_id).join("artifacts")
    }

    pub fn state_dir(&self, project_id: &ProjectId) -> PathBuf {
        self.project_home(project_id).join("state")
    }

    pub fn manifest_revision_path(&self, project_id: &ProjectId) -> PathBuf {
        self.state_dir(project_id)
            .join(PROJECT_MANIFEST_REVISION_FILE)
    }
}

fn validate_component(value: &str) -> ProjectStoreResult<()> {
    let valid = !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_');
    if valid {
        Ok(())
    } else {
        Err(ProjectStoreError::InvalidPathComponent(value.to_string()))
    }
}

pub(crate) fn validate_legacy_project_key(value: &str) -> ProjectStoreResult<()> {
    let valid = !value.is_empty()
        && value.len() <= 256
        && value.trim() == value
        && value != "."
        && value != ".."
        && !value.contains('/')
        && !value.contains('\\')
        && !value.contains('\0');
    if valid {
        Ok(())
    } else {
        Err(ProjectStoreError::InvalidPathComponent(value.to_string()))
    }
}

fn prepare_data_dir(data_dir: PathBuf) -> ProjectStoreResult<PathBuf> {
    let mut requested = if data_dir.is_absolute() {
        data_dir
    } else {
        std::env::current_dir()?.join(data_dir)
    };
    let mut missing = Vec::new();
    loop {
        match std::fs::symlink_metadata(&requested) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(ProjectStoreError::Validation(format!(
                        "data directory component is not a plain directory: {}",
                        requested.display()
                    )));
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let component = requested.file_name().ok_or_else(|| {
                    ProjectStoreError::Validation(format!(
                        "data directory has no creatable component: {}",
                        requested.display()
                    ))
                })?;
                missing.push(component.to_os_string());
                requested = requested
                    .parent()
                    .ok_or_else(|| {
                        ProjectStoreError::Validation(
                            "data directory has no existing ancestor".to_string(),
                        )
                    })?
                    .to_path_buf();
            }
            Err(error) => return Err(error.into()),
        }
    }

    let mut current = std::fs::canonicalize(&requested)?;
    for component in missing.into_iter().rev() {
        assert_plain_directory(&current)?;
        let next = current.join(component);
        match std::fs::symlink_metadata(&next) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(ProjectStoreError::Validation(format!(
                        "data directory component is not a plain directory: {}",
                        next.display()
                    )));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                assert_plain_directory(&current)?;
                std::fs::create_dir(&next)?;
                assert_plain_directory(&next)?;
                sync_directory(&current)?;
            }
            Err(error) => return Err(error.into()),
        }
        let resolved = std::fs::canonicalize(&next)?;
        if !resolved.starts_with(&current) {
            return Err(ProjectStoreError::Validation(format!(
                "data directory component escaped its parent: {}",
                next.display()
            )));
        }
        current = resolved;
    }
    Ok(current)
}

pub(crate) fn ensure_confined_directory(
    trusted_base: &Path,
    directory: &Path,
) -> ProjectStoreResult<PathBuf> {
    walk_confined_directory(trusted_base, directory, true)
}

pub(crate) fn validate_existing_confined_directory(
    trusted_base: &Path,
    directory: &Path,
) -> ProjectStoreResult<PathBuf> {
    walk_confined_directory(trusted_base, directory, false)
}

/// Walk from an already trusted base without following symlinks. Missing
/// components are created one at a time after their parent is revalidated.
fn walk_confined_directory(
    trusted_base: &Path,
    directory: &Path,
    create_missing: bool,
) -> ProjectStoreResult<PathBuf> {
    assert_plain_directory(trusted_base)?;
    let canonical_base = std::fs::canonicalize(trusted_base)?;
    let relative = directory
        .strip_prefix(trusted_base)
        .or_else(|_| directory.strip_prefix(&canonical_base))
        .map_err(|_| {
            ProjectStoreError::Validation(format!(
                "project store directory escapes trusted base: {}",
                directory.display()
            ))
        })?;
    let mut current = canonical_base.clone();
    for component in relative.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(ProjectStoreError::Validation(
                "project store directory has an invalid component".to_string(),
            ));
        };
        current.push(component);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(ProjectStoreError::Validation(format!(
                        "project store directory component is not a plain directory: {}",
                        current.display()
                    )));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && create_missing => {
                let parent = current.parent().ok_or_else(|| {
                    ProjectStoreError::Validation(
                        "project store directory has no parent".to_string(),
                    )
                })?;
                assert_plain_directory(parent)?;
                std::fs::create_dir(&current)?;
                assert_plain_directory(&current)?;
                sync_directory(parent)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    let resolved = std::fs::canonicalize(&current)?;
    if !resolved.starts_with(&canonical_base) {
        return Err(ProjectStoreError::Validation(format!(
            "project store directory resolves outside trusted base: {}",
            resolved.display()
        )));
    }
    Ok(resolved)
}

pub(crate) fn assert_plain_directory(path: &Path) -> ProjectStoreResult<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ProjectStoreError::Validation(format!(
            "expected a plain directory: {}",
            path.display()
        )));
    }
    Ok(())
}

fn validate_regular_file_if_exists(path: &Path, label: &str) -> ProjectStoreResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(ProjectStoreError::Validation(format!(
            "{label} is not a plain regular file: {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn validate_required_regular_file(path: &Path, label: &str) -> ProjectStoreResult<()> {
    if validate_regular_file_if_exists(path, label)? {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("{label} was not found: {}", path.display()),
        )
        .into())
    }
}

#[derive(Debug, Clone)]
pub struct ProjectStore {
    paths: ProjectPaths,
}

impl ProjectStore {
    /// Open the registry and rebuild its derived index. Corrupt individual
    /// manifests are recovered from their last-known-good backup when possible;
    /// otherwise they are quarantined and skipped rather than blocking startup.
    pub fn open(data_dir: impl Into<PathBuf>) -> ProjectStoreResult<Self> {
        let data_dir = prepare_data_dir(data_dir.into())?;
        let paths = ProjectPaths::new(data_dir);
        ensure_confined_directory(paths.data_dir(), &paths.projects_dir())?;
        let store = Self { paths };
        store.remove_orphan_temps()?;
        store.rebuild_index()?;
        Ok(store)
    }

    pub fn paths(&self) -> &ProjectPaths {
        &self.paths
    }

    pub fn create(
        &self,
        name: impl Into<String>,
        description: Option<String>,
    ) -> ProjectStoreResult<ProjectManifest> {
        self.create_with_bindings(name, description, Vec::new())
    }

    pub fn create_with_bindings(
        &self,
        name: impl Into<String>,
        description: Option<String>,
        workspace_bindings: Vec<WorkspaceBinding>,
    ) -> ProjectStoreResult<ProjectManifest> {
        let mut manifest = ProjectManifest::new(ProjectId::new(), name, description, Utc::now());
        manifest.workspace_bindings = workspace_bindings;
        self.create_manifest(manifest)
    }

    /// Create a configured active Project with one authoritative user source
    /// folder plus zero or more additional workspaces/worktrees.
    pub fn create_with_project_path(
        &self,
        name: impl Into<String>,
        description: Option<String>,
        project_path: impl Into<String>,
        workspace_bindings: Vec<WorkspaceBinding>,
    ) -> ProjectStoreResult<ProjectManifest> {
        let mut manifest = ProjectManifest::new(ProjectId::new(), name, description, Utc::now());
        manifest.project_path = Some(project_path.into());
        manifest.project_path_status = ProjectPathStatus::Configured;
        manifest.workspace_bindings = workspace_bindings;
        self.create_manifest(manifest)
    }

    pub fn create_with_id(
        &self,
        project_id: ProjectId,
        name: impl Into<String>,
        description: Option<String>,
    ) -> ProjectStoreResult<ProjectManifest> {
        let manifest = ProjectManifest::new(project_id, name, description, Utc::now());
        self.create_manifest(manifest)
    }

    pub fn create_manifest(
        &self,
        mut manifest: ProjectManifest,
    ) -> ProjectStoreResult<ProjectManifest> {
        canonicalize_manifest_paths(&mut manifest)?;
        validate_manifest(&manifest)?;
        if manifest.revision != 1 {
            return Err(ProjectStoreError::Validation(
                "a new project must start at revision 1".to_string(),
            ));
        }
        let projects_dir = validate_existing_confined_directory(
            self.paths.data_dir(),
            &self.paths.projects_dir(),
        )?;
        let _registry_lock = lock_exclusive(projects_dir.join(".registry.lock"))?;
        validate_existing_confined_directory(self.paths.data_dir(), &projects_dir)?;
        let existing_projects = self.load_registry_manifests()?;
        validate_new_workspace_roots(
            &manifest.id,
            manifest.project_path.as_deref(),
            &manifest.workspace_bindings,
            &existing_projects,
        )?;
        let home = self.paths.project_home(&manifest.id);
        ensure_confined_directory(&projects_dir, &home)?;
        {
            let _lock = lock_exclusive(home.join(".project.lock"))?;
            validate_existing_confined_directory(&projects_dir, &home)?;
            let path = self.paths.manifest_path(&manifest.id);
            if validate_regular_file_if_exists(&path, "project manifest")? {
                return Err(ProjectStoreError::AlreadyExists(manifest.id));
            }
            self.write_manifest_revision_floor(&manifest.id, manifest.revision)?;
            write_json_atomic(&path, &manifest)?;
        }
        self.rebuild_index()?;
        Ok(manifest)
    }

    pub fn get(&self, project_id: &ProjectId) -> ProjectStoreResult<ProjectManifest> {
        let home = self.paths.project_home(project_id);
        self.validate_project_home(project_id)?;
        let _lock = lock_exclusive(home.join(".project.lock"))?;
        self.validate_project_home(project_id)?;
        self.load_manifest_locked(project_id)
    }

    pub fn list(&self) -> ProjectStoreResult<Vec<ProjectManifest>> {
        let index = self.index()?;
        let mut projects = Vec::with_capacity(index.projects.len());
        for project_id in index.projects.keys() {
            match self.get(project_id) {
                Ok(manifest) => projects.push(manifest),
                Err(error) => {
                    tracing::warn!(project_id = %project_id, %error, "skipping unavailable project");
                }
            }
        }
        Ok(projects)
    }

    pub fn index(&self) -> ProjectStoreResult<ProjectIndex> {
        validate_existing_confined_directory(self.paths.data_dir(), &self.paths.projects_dir())?;
        let path = self.paths.index_path();
        let bytes = read_regular_file(&path, "project index")?;
        let index: ProjectIndex = serde_json::from_slice(&bytes)?;
        validate_index(&index)?;
        Ok(index)
    }

    /// CAS update under the per-Project cross-process lock.
    pub fn update<F>(
        &self,
        project_id: &ProjectId,
        expected_revision: u64,
        mutate: F,
    ) -> ProjectStoreResult<ProjectManifest>
    where
        F: FnOnce(&mut ProjectManifest) -> ProjectStoreResult<()>,
    {
        self.update_inner(project_id, expected_revision, false, false, mutate)
    }

    /// Atomically update the authoritative Project path and other metadata
    /// under the registry lock. The new path is canonicalized and checked
    /// against every Project root before the CAS write becomes visible.
    pub fn update_with_project_path<F>(
        &self,
        project_id: &ProjectId,
        expected_revision: u64,
        project_path: &str,
        mutate: F,
    ) -> ProjectStoreResult<ProjectManifest>
    where
        F: FnOnce(&mut ProjectManifest) -> ProjectStoreResult<()>,
    {
        let project_path = canonicalize_project_path(project_path)?;
        let projects_dir = validate_existing_confined_directory(
            self.paths.data_dir(),
            &self.paths.projects_dir(),
        )?;
        let _registry_lock = lock_exclusive(projects_dir.join(".registry.lock"))?;
        validate_existing_confined_directory(self.paths.data_dir(), &projects_dir)?;

        let current = self.get(project_id)?;
        let remaining_bindings = current
            .workspace_bindings
            .iter()
            .filter(|binding| binding.path != project_path)
            .cloned()
            .collect::<Vec<_>>();
        let existing_projects = self
            .load_registry_manifests()?
            .into_iter()
            .filter(|project| project.id != *project_id)
            .collect::<Vec<_>>();
        validate_new_workspace_roots(
            project_id,
            Some(&project_path),
            &remaining_bindings,
            &existing_projects,
        )?;

        self.update_inner(project_id, expected_revision, true, true, move |manifest| {
            mutate(manifest)?;
            manifest.project_path = Some(project_path);
            manifest.project_path_status = ProjectPathStatus::Configured;
            manifest.workspace_bindings = remaining_bindings;
            Ok(())
        })
    }

    fn update_inner<F>(
        &self,
        project_id: &ProjectId,
        expected_revision: u64,
        allow_project_path_change: bool,
        allow_workspace_binding_change: bool,
        mutate: F,
    ) -> ProjectStoreResult<ProjectManifest>
    where
        F: FnOnce(&mut ProjectManifest) -> ProjectStoreResult<()>,
    {
        let home = self.paths.project_home(project_id);
        self.validate_project_home(project_id)?;
        let updated = {
            let _lock = lock_exclusive(home.join(".project.lock"))?;
            self.validate_project_home(project_id)?;
            let current = self.load_manifest_locked(project_id)?;
            if current.revision != expected_revision {
                return Err(ProjectStoreError::Conflict {
                    expected: expected_revision,
                    actual: current.revision,
                });
            }
            let mut candidate = current.clone();
            mutate(&mut candidate)?;
            if candidate.id != current.id
                || candidate.schema_version != current.schema_version
                || candidate.created_at != current.created_at
            {
                return Err(ProjectStoreError::Validation(
                    "project id, schema version, and created_at are immutable".to_string(),
                ));
            }
            if !allow_workspace_binding_change
                && candidate.workspace_bindings != current.workspace_bindings
            {
                return Err(ProjectStoreError::Validation(
                    "workspace bindings must be changed through bind/unbind APIs".to_string(),
                ));
            }
            if !allow_project_path_change
                && (candidate.project_path != current.project_path
                    || candidate.project_path_status != current.project_path_status)
            {
                return Err(ProjectStoreError::Validation(
                    "project_path must be changed through the Project path CAS API".to_string(),
                ));
            }
            candidate.revision = current
                .revision
                .checked_add(1)
                .ok_or_else(|| ProjectStoreError::Validation("revision exhausted".to_string()))?;
            candidate.updated_at = Utc::now();
            validate_manifest(&candidate)?;
            self.write_manifest_locked(&current, &candidate)?;
            candidate
        };
        self.rebuild_index()?;
        Ok(updated)
    }

    pub fn archive(
        &self,
        project_id: &ProjectId,
        expected_revision: u64,
    ) -> ProjectStoreResult<ProjectManifest> {
        self.update(project_id, expected_revision, |manifest| {
            manifest.status = ProjectStatus::Archived;
            Ok(())
        })
    }

    /// Restore an archived Project under the same per-Project CAS lock used by
    /// every manifest mutation. The status check is intentionally performed
    /// inside the update closure so two concurrent restores cannot both
    /// succeed.
    pub fn unarchive(
        &self,
        project_id: &ProjectId,
        expected_revision: u64,
    ) -> ProjectStoreResult<ProjectManifest> {
        self.update(project_id, expected_revision, |manifest| {
            if manifest.status != ProjectStatus::Archived {
                return Err(ProjectStoreError::NotArchived(manifest.id.clone()));
            }
            manifest.status = ProjectStatus::Active;
            Ok(())
        })
    }

    /// Register an exact canonical workspace path under a registry-wide lock.
    /// A path already owned by another Project is rejected.
    pub fn bind_workspace(
        &self,
        project_id: &ProjectId,
        expected_revision: u64,
        binding: WorkspaceBinding,
    ) -> ProjectStoreResult<ProjectManifest> {
        let binding = canonicalize_binding(binding)?;
        let projects_dir = validate_existing_confined_directory(
            self.paths.data_dir(),
            &self.paths.projects_dir(),
        )?;
        let _registry_lock = lock_exclusive(projects_dir.join(".registry.lock"))?;
        validate_existing_confined_directory(self.paths.data_dir(), &projects_dir)?;
        let existing_projects = self.load_registry_manifests()?;
        validate_new_workspace_roots(
            project_id,
            None,
            std::slice::from_ref(&binding),
            &existing_projects,
        )?;
        self.update_inner(
            project_id,
            expected_revision,
            false,
            true,
            move |manifest| {
                if manifest.status != ProjectStatus::Active {
                    return Err(ProjectStoreError::Validation(
                        "cannot bind a workspace to an archived project".to_string(),
                    ));
                }
                manifest.workspace_bindings.push(binding);
                Ok(())
            },
        )
    }

    /// Remove only the exact binding; no session or Project resource is
    /// deleted or moved.
    pub fn unbind_workspace(
        &self,
        project_id: &ProjectId,
        expected_revision: u64,
        workspace_path: &str,
    ) -> ProjectStoreResult<ProjectManifest> {
        validate_absolute_path(workspace_path, "workspace binding")?;
        let requested_path = workspace_path.to_string();
        let projects_dir = validate_existing_confined_directory(
            self.paths.data_dir(),
            &self.paths.projects_dir(),
        )?;
        let _registry_lock = lock_exclusive(projects_dir.join(".registry.lock"))?;
        validate_existing_confined_directory(self.paths.data_dir(), &projects_dir)?;
        self.update_inner(
            project_id,
            expected_revision,
            false,
            true,
            move |manifest| {
                if manifest.project_path.as_deref() == Some(requested_path.as_str()) {
                    return Err(ProjectStoreError::ProjectPathUnbindConflict {
                        project_id: manifest.id.clone(),
                        project_path: requested_path.clone(),
                    });
                }
                // The exact manifest string is authoritative for DELETE. In
                // particular, do not canonicalize it first: the workspace may be
                // stale, missing, or replaced by a symlink after it was bound.
                let matched_path = if manifest
                    .workspace_bindings
                    .iter()
                    .any(|binding| binding.path == requested_path)
                {
                    requested_path.clone()
                } else {
                    // Compatibility for callers that send an existing path alias
                    // (`nested/..`, platform spelling, and similar). This fallback
                    // is reached only when the raw stored identity did not match.
                    let canonical_path =
                        canonicalize_utf8(Path::new(&requested_path), "workspace binding")
                            .unwrap_or_else(|_| requested_path.clone());
                    if manifest.project_path.as_deref() == Some(canonical_path.as_str()) {
                        return Err(ProjectStoreError::ProjectPathUnbindConflict {
                            project_id: manifest.id.clone(),
                            project_path: canonical_path,
                        });
                    }
                    if manifest
                        .workspace_bindings
                        .iter()
                        .any(|binding| binding.path == canonical_path)
                    {
                        canonical_path
                    } else {
                        return Err(ProjectStoreError::Validation(format!(
                            "workspace binding was not found: {requested_path}"
                        )));
                    }
                };
                let before = manifest.workspace_bindings.len();
                manifest
                    .workspace_bindings
                    .retain(|binding| binding.path != matched_path);
                if manifest.workspace_bindings.len() == before {
                    return Err(ProjectStoreError::Validation(format!(
                        "workspace binding was not found: {requested_path}"
                    )));
                }
                Ok(())
            },
        )
    }

    pub fn bump_resource_revision(
        &self,
        project_id: &ProjectId,
        expected_revision: u64,
    ) -> ProjectStoreResult<ProjectManifest> {
        self.update(project_id, expected_revision, |manifest| {
            manifest.resource_revision =
                manifest.resource_revision.checked_add(1).ok_or_else(|| {
                    ProjectStoreError::Validation("resource revision exhausted".to_string())
                })?;
            Ok(())
        })
    }

    /// Resolve an exact registered workspace owner. Multiple owners are a
    /// corrupt/ambiguous registry state and return a validation error.
    pub fn find_workspace_owner(
        &self,
        workspace_path: &str,
    ) -> ProjectStoreResult<Option<ProjectManifest>> {
        let canonical_path = canonicalize_utf8(Path::new(workspace_path), "workspace binding")?;
        let matches = self
            .list()?
            .into_iter()
            .filter(|project| project.workspace_roots().any(|root| root == canonical_path))
            .collect::<Vec<_>>();
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.into_iter().next()),
            _ => Err(ProjectStoreError::Validation(format!(
                "workspace is bound to multiple projects: {canonical_path}"
            ))),
        }
    }

    /// Resolve the owner of an existing candidate path using component-aware
    /// containment below registered canonical workspace bindings.
    ///
    /// A candidate that is equal to a binding or is a descendant of one
    /// matches. If nested bindings make more than one Project match, resolution
    /// fails closed instead of picking the longest prefix or registry order.
    pub fn find_workspace_owner_for_path(
        &self,
        candidate_path: &str,
    ) -> ProjectStoreResult<Option<ProjectManifest>> {
        let canonical_path =
            canonicalize_candidate_utf8(Path::new(candidate_path), "workspace candidate")?;
        let candidate = Path::new(&canonical_path);
        let matches = self
            .list()?
            .into_iter()
            .filter(|project| {
                project.workspace_roots().any(|root| {
                    let root = Path::new(root);
                    candidate == root || candidate.starts_with(root)
                })
            })
            .collect::<Vec<_>>();
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.into_iter().next()),
            _ => Err(ProjectStoreError::Validation(format!(
                "workspace candidate is contained by multiple project bindings: {canonical_path}"
            ))),
        }
    }

    pub fn find_workspace_binding(
        &self,
        workspace_path: &str,
    ) -> ProjectStoreResult<Option<(ProjectManifest, WorkspaceBinding)>> {
        let canonical_path = canonicalize_utf8(Path::new(workspace_path), "workspace binding")?;
        let Some(project) = self.find_workspace_owner(&canonical_path)? else {
            return Ok(None);
        };
        if project.project_path.as_deref() == Some(canonical_path.as_str()) {
            return Ok(Some((
                project,
                WorkspaceBinding {
                    path: canonical_path,
                    label: Some("Project path".to_string()),
                    git_common_dir: None,
                },
            )));
        }
        let binding = project
            .workspace_bindings
            .iter()
            .find(|binding| binding.path == canonical_path)
            .cloned()
            .ok_or_else(|| {
                ProjectStoreError::Validation(
                    "workspace owner disappeared during lookup".to_string(),
                )
            })?;
        Ok(Some((project, binding)))
    }

    /// Return only counts/presence and revisions; never read resource contents.
    pub fn resource_summary(
        &self,
        project_id: &ProjectId,
    ) -> ProjectStoreResult<ProjectResourceSummary> {
        let manifest = self.get(project_id)?;
        let home = self.paths.project_home(project_id);
        let settings = self.paths.settings_path(project_id);
        let skills = count_skills_layers(&home)?;
        let resources = vec![
            ProjectResourceEntry {
                kind: ProjectResourceKind::Settings,
                present: settings.is_file(),
                item_count: u64::from(settings.is_file()),
            },
            ProjectResourceEntry {
                kind: ProjectResourceKind::Skills,
                present: skills > 0,
                item_count: skills,
            },
            resource_dir_summary(
                ProjectResourceKind::Commands,
                &self.paths.commands_dir(project_id),
            )?,
            resource_dir_summary(
                ProjectResourceKind::Memory,
                &self.paths.memory_v1_dir(project_id),
            )?,
            resource_dir_summary(
                ProjectResourceKind::Artifacts,
                &self.paths.artifacts_dir(project_id),
            )?,
            resource_dir_summary(
                ProjectResourceKind::State,
                &self.paths.state_dir(project_id),
            )?,
        ];
        Ok(ProjectResourceSummary {
            project_id: project_id.clone(),
            resource_revision: manifest.resource_revision,
            resources,
        })
    }

    /// Rebuild the derived index from authoritative manifests.
    pub fn rebuild_index(&self) -> ProjectStoreResult<ProjectIndex> {
        let projects_dir = validate_existing_confined_directory(
            self.paths.data_dir(),
            &self.paths.projects_dir(),
        )?;
        let _index_lock = lock_exclusive(projects_dir.join(".index.lock"))?;
        validate_existing_confined_directory(self.paths.data_dir(), &projects_dir)?;
        let old_revision = self.read_or_quarantine_index_revision()?;
        let mut projects = BTreeMap::new();

        for entry in std::fs::read_dir(&projects_dir)? {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    tracing::warn!(%error, "project index rebuild skipped unreadable entry");
                    continue;
                }
            };
            if !entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(project_id) = name.parse::<ProjectId>() else {
                tracing::warn!(directory = %name, "project index rebuild skipped invalid id directory");
                continue;
            };
            if let Err(error) = validate_existing_confined_directory(&projects_dir, &entry.path()) {
                tracing::warn!(project_id = %project_id, %error, "project index rebuild skipped unsafe project home");
                continue;
            }
            let manifest = {
                let _project_lock = match lock_exclusive(entry.path().join(".project.lock")) {
                    Ok(lock) => lock,
                    Err(error) => {
                        tracing::warn!(project_id = %project_id, %error, "project index rebuild could not lock manifest");
                        continue;
                    }
                };
                if let Err(error) =
                    validate_existing_confined_directory(&projects_dir, &entry.path())
                {
                    tracing::warn!(project_id = %project_id, %error, "project index rebuild skipped project home changed after lock");
                    continue;
                }
                match self.load_manifest_locked(&project_id) {
                    Ok(manifest) => manifest,
                    Err(error) => {
                        tracing::warn!(project_id = %project_id, %error, "project index rebuild skipped invalid manifest");
                        continue;
                    }
                }
            };
            projects.insert(project_id, ProjectIndexEntry::from(&manifest));
        }

        let index = ProjectIndex {
            schema_version: PROJECT_INDEX_SCHEMA_VERSION,
            revision: old_revision.saturating_add(1),
            updated_at: Utc::now(),
            projects,
        };
        write_json_atomic(&self.paths.index_path(), &index)?;
        Ok(index)
    }

    fn validate_project_home(&self, project_id: &ProjectId) -> ProjectStoreResult<PathBuf> {
        let projects_dir = validate_existing_confined_directory(
            self.paths.data_dir(),
            &self.paths.projects_dir(),
        )?;
        let home = self.paths.project_home(project_id);
        match validate_existing_confined_directory(&projects_dir, &home) {
            Ok(home) => Ok(home),
            Err(ProjectStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(ProjectStoreError::NotFound(project_id.clone()))
            }
            Err(error) => Err(error),
        }
    }

    /// Load valid authoritative manifests directly from Project homes while
    /// the caller holds the registry lock. The derived index is deliberately
    /// not identity evidence for overlap enforcement.
    fn load_registry_manifests(&self) -> ProjectStoreResult<Vec<ProjectManifest>> {
        let projects_dir = validate_existing_confined_directory(
            self.paths.data_dir(),
            &self.paths.projects_dir(),
        )?;
        let mut manifests = Vec::new();
        for entry in std::fs::read_dir(&projects_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(project_id) = name.parse::<ProjectId>() else {
                continue;
            };
            validate_existing_confined_directory(&projects_dir, &entry.path())?;
            let _project_lock = lock_exclusive(entry.path().join(".project.lock"))?;
            validate_existing_confined_directory(&projects_dir, &entry.path())?;
            match self.load_manifest_locked(&project_id) {
                Ok(manifest) => manifests.push(manifest),
                Err(ProjectStoreError::NotFound(_)) | Err(ProjectStoreError::Json(_)) => {
                    tracing::warn!(
                        project_id = %project_id,
                        "registry overlap scan skipped Project without a recoverable manifest"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        Ok(manifests)
    }

    fn load_manifest_locked(&self, project_id: &ProjectId) -> ProjectStoreResult<ProjectManifest> {
        self.validate_project_home(project_id)?;
        let path = self.paths.manifest_path(project_id);
        if !validate_regular_file_if_exists(&path, "project manifest")? {
            return Err(ProjectStoreError::NotFound(project_id.clone()));
        }
        let primary = match read_regular_file(&path, "project manifest") {
            Ok(bytes) => bytes,
            Err(ProjectStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ProjectStoreError::NotFound(project_id.clone()));
            }
            Err(error) => return Err(error),
        };
        match decode_manifest(&primary, project_id) {
            Ok(decoded) if decoded.migrated_from_v1 => {
                self.persist_migrated_manifest_locked(project_id, &primary, decoded.manifest)
            }
            Ok(decoded) => self.normalize_manifest_revision_locked(decoded.manifest),
            Err(primary_error) => {
                let quarantine =
                    path.with_file_name(format!("project.json.corrupt.{}", Uuid::new_v4()));
                write_bytes_atomic(&quarantine, &primary)?;
                let backup = self
                    .paths
                    .project_home(project_id)
                    .join(PROJECT_MANIFEST_BACKUP_FILE);
                let recovered =
                    if validate_regular_file_if_exists(&backup, "project manifest backup")? {
                        read_regular_file(&backup, "project manifest backup")
                            .ok()
                            .and_then(|bytes| decode_manifest(&bytes, project_id).ok())
                    } else {
                        None
                    };
                if let Some(decoded) = recovered {
                    let manifest = decoded.manifest;
                    let revision_floor = self.read_manifest_revision_floor(project_id)?;
                    let mut candidate = manifest.clone();
                    candidate.revision = candidate
                        .revision
                        .max(revision_floor)
                        .checked_add(1)
                        .ok_or_else(|| {
                            ProjectStoreError::Validation("revision exhausted".to_string())
                        })?;
                    candidate.updated_at = Utc::now();
                    self.write_manifest_locked(&manifest, &candidate)?;
                    tracing::warn!(
                        project_id = %project_id,
                        quarantine = %quarantine.display(),
                        "recovered corrupt project manifest from backup"
                    );
                    Ok(candidate)
                } else {
                    Err(primary_error)
                }
            }
        }
    }

    fn persist_migrated_manifest_locked(
        &self,
        project_id: &ProjectId,
        v1_bytes: &[u8],
        manifest: ProjectManifest,
    ) -> ProjectStoreResult<ProjectManifest> {
        let revision_floor = self.read_manifest_revision_floor(project_id)?;
        let mut candidate = manifest;
        candidate.revision = candidate
            .revision
            .max(revision_floor)
            .checked_add(1)
            .ok_or_else(|| ProjectStoreError::Validation("revision exhausted".to_string()))?;
        candidate.updated_at = Utc::now();

        let home = self.validate_project_home(project_id)?;
        write_bytes_atomic(&home.join(PROJECT_MANIFEST_BACKUP_FILE), v1_bytes)?;
        self.write_manifest_revision_floor(project_id, candidate.revision)?;
        write_json_atomic(&self.paths.manifest_path(project_id), &candidate)?;
        Ok(candidate)
    }

    fn write_manifest_locked(
        &self,
        previous: &ProjectManifest,
        candidate: &ProjectManifest,
    ) -> ProjectStoreResult<()> {
        let home = self.validate_project_home(&previous.id)?;
        let backup = home.join(PROJECT_MANIFEST_BACKUP_FILE);
        write_json_atomic(&backup, previous)?;
        // Persist the monotonic floor before publishing the new manifest. If
        // the process dies between these writes, the next load advances past
        // the issued revision instead of allowing a stale CAS token to win.
        self.write_manifest_revision_floor(&previous.id, candidate.revision)?;
        write_json_atomic(&self.paths.manifest_path(&previous.id), candidate)
    }

    fn normalize_manifest_revision_locked(
        &self,
        manifest: ProjectManifest,
    ) -> ProjectStoreResult<ProjectManifest> {
        let floor = self.read_manifest_revision_floor(&manifest.id)?;
        if manifest.revision < floor {
            let mut candidate = manifest.clone();
            candidate.revision = floor
                .checked_add(1)
                .ok_or_else(|| ProjectStoreError::Validation("revision exhausted".to_string()))?;
            candidate.updated_at = Utc::now();
            self.write_manifest_locked(&manifest, &candidate)?;
            Ok(candidate)
        } else {
            if manifest.revision > floor {
                self.write_manifest_revision_floor(&manifest.id, manifest.revision)?;
            }
            Ok(manifest)
        }
    }

    fn read_manifest_revision_floor(&self, project_id: &ProjectId) -> ProjectStoreResult<u64> {
        let home = self.validate_project_home(project_id)?;
        let state = self.paths.state_dir(project_id);
        match validate_existing_confined_directory(&home, &state) {
            Ok(_) => {}
            Err(ProjectStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(0);
            }
            Err(error) => return Err(error),
        }
        let path = self.paths.manifest_revision_path(project_id);
        if !validate_regular_file_if_exists(&path, "project manifest revision floor")? {
            return Ok(0);
        }
        let bytes = match read_regular_file(&path, "project manifest revision floor") {
            Ok(bytes) => bytes,
            Err(ProjectStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(0);
            }
            Err(error) => return Err(error),
        };
        let value = std::str::from_utf8(&bytes)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .ok_or_else(|| {
                ProjectStoreError::Validation(format!(
                    "project manifest revision floor is invalid: {}",
                    path.display()
                ))
            })?;
        Ok(value)
    }

    fn write_manifest_revision_floor(
        &self,
        project_id: &ProjectId,
        revision: u64,
    ) -> ProjectStoreResult<()> {
        let home = self.validate_project_home(project_id)?;
        ensure_confined_directory(&home, &self.paths.state_dir(project_id))?;
        write_bytes_atomic(
            &self.paths.manifest_revision_path(project_id),
            format!("{revision}\n").as_bytes(),
        )
    }

    fn read_or_quarantine_index_revision(&self) -> ProjectStoreResult<u64> {
        validate_existing_confined_directory(self.paths.data_dir(), &self.paths.projects_dir())?;
        let path = self.paths.index_path();
        if !validate_regular_file_if_exists(&path, "project index")? {
            return Ok(0);
        }
        let bytes = match read_regular_file(&path, "project index") {
            Ok(bytes) => bytes,
            Err(ProjectStoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(0);
            }
            Err(error) => return Err(error),
        };
        match serde_json::from_slice::<ProjectIndex>(&bytes)
            .map_err(ProjectStoreError::from)
            .and_then(|index| match index.schema_version {
                1 => Ok(index),
                PROJECT_INDEX_SCHEMA_VERSION => {
                    validate_index(&index)?;
                    Ok(index)
                }
                schema_version => Err(ProjectStoreError::Validation(format!(
                    "unsupported project index schema {schema_version}"
                ))),
            }) {
            Ok(index) => Ok(index.revision),
            Err(error) => {
                let quarantine =
                    path.with_file_name(format!("index.json.corrupt.{}", Uuid::new_v4()));
                write_bytes_atomic(&quarantine, &bytes)?;
                tracing::warn!(%error, quarantine = %quarantine.display(), "rebuilding corrupt project index");
                Ok(0)
            }
        }
    }

    fn remove_orphan_temps(&self) -> ProjectStoreResult<()> {
        let projects_dir = validate_existing_confined_directory(
            self.paths.data_dir(),
            &self.paths.projects_dir(),
        )?;
        {
            let _index_lock = lock_exclusive(projects_dir.join(".index.lock"))?;
            validate_existing_confined_directory(self.paths.data_dir(), &projects_dir)?;
            remove_temp_files_in(&projects_dir)?;
        }
        for entry in std::fs::read_dir(&projects_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                validate_existing_confined_directory(&projects_dir, &entry.path())?;
                let _project_lock = lock_exclusive(entry.path().join(".project.lock"))?;
                validate_existing_confined_directory(&projects_dir, &entry.path())?;
                remove_temp_files_in(&entry.path())?;
            }
        }
        Ok(())
    }
}

struct DecodedManifest {
    manifest: ProjectManifest,
    migrated_from_v1: bool,
}

fn decode_manifest(bytes: &[u8], expected_id: &ProjectId) -> ProjectStoreResult<DecodedManifest> {
    let mut manifest: ProjectManifest = serde_json::from_slice(bytes)?;
    let migrated_from_v1 = match manifest.schema_version {
        1 => {
            // A sole v1 binding is the only deterministic primary-folder
            // signal. Zero or multiple bindings deliberately remain
            // unconfigured instead of choosing by ordering.
            if manifest.workspace_bindings.len() == 1 {
                let binding = manifest.workspace_bindings.remove(0);
                manifest.project_path = Some(binding.path);
                manifest.project_path_status = ProjectPathStatus::Configured;
            } else if manifest.workspace_bindings.is_empty() {
                manifest.project_path_status = ProjectPathStatus::NeedsConfiguration;
            } else {
                manifest.project_path_status = ProjectPathStatus::NeedsSelection;
            }
            manifest.schema_version = PROJECT_MANIFEST_SCHEMA_VERSION;
            true
        }
        PROJECT_MANIFEST_SCHEMA_VERSION => false,
        schema_version => {
            return Err(ProjectStoreError::Validation(format!(
                "unsupported project manifest schema {schema_version}"
            )));
        }
    };
    validate_manifest(&manifest)?;
    if &manifest.id != expected_id {
        return Err(ProjectStoreError::Validation(format!(
            "manifest id {} does not match directory {}",
            manifest.id, expected_id
        )));
    }
    Ok(DecodedManifest {
        manifest,
        migrated_from_v1,
    })
}

fn canonicalize_manifest_paths(manifest: &mut ProjectManifest) -> ProjectStoreResult<()> {
    if let Some(project_path) = manifest.project_path.as_deref() {
        manifest.project_path = Some(canonicalize_project_path(project_path)?);
        manifest.project_path_status = ProjectPathStatus::Configured;
    }
    for binding in &mut manifest.workspace_bindings {
        *binding = canonicalize_binding(binding.clone())?;
    }
    Ok(())
}

fn canonicalize_project_path(project_path: &str) -> ProjectStoreResult<String> {
    validate_absolute_path(project_path, "project_path")?;
    let canonical = canonicalize_utf8(Path::new(project_path), "project_path")?;
    let metadata = std::fs::symlink_metadata(&canonical)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ProjectStoreError::Validation(format!(
            "project_path must be a plain directory: {canonical}"
        )));
    }
    Ok(canonical)
}

fn validate_new_workspace_roots(
    project_id: &ProjectId,
    project_path: Option<&str>,
    incoming: &[WorkspaceBinding],
    existing_projects: &[ProjectManifest],
) -> ProjectStoreResult<()> {
    let incoming_roots = project_path
        .into_iter()
        .chain(incoming.iter().map(|binding| binding.path.as_str()))
        .collect::<Vec<_>>();
    for (index, root) in incoming_roots.iter().enumerate() {
        for other in incoming_roots.iter().skip(index + 1) {
            if workspace_paths_overlap(root, other) {
                return Err(ProjectStoreError::Validation(format!(
                    "project {project_id} contains overlapping workspace roots: {root} and {other}"
                )));
            }
        }
    }
    for root in incoming_roots {
        for project in existing_projects {
            for existing in project.workspace_roots() {
                if workspace_paths_overlap(root, existing) {
                    return Err(ProjectStoreError::Validation(format!(
                        "workspace root {root} overlaps project {} root {existing}",
                        project.id
                    )));
                }
            }
        }
    }
    Ok(())
}

fn workspace_paths_overlap(left: &str, right: &str) -> bool {
    let left = Path::new(left);
    let right = Path::new(right);
    left == right || left.starts_with(right) || right.starts_with(left)
}

fn canonicalize_binding(mut binding: WorkspaceBinding) -> ProjectStoreResult<WorkspaceBinding> {
    binding.path = canonicalize_utf8(Path::new(&binding.path), "workspace binding")?;
    let actual_git_common_dir = resolve_git_common_dir(Path::new(&binding.path))?;
    if let Some(supplied) = binding.git_common_dir.as_deref() {
        let supplied = canonicalize_utf8(Path::new(supplied), "git common dir")?;
        if actual_git_common_dir.as_deref() != Some(supplied.as_str()) {
            return Err(ProjectStoreError::Validation(format!(
                "supplied git common dir does not match workspace {}",
                binding.path
            )));
        }
    }
    binding.git_common_dir = actual_git_common_dir;
    Ok(binding)
}

fn resolve_git_common_dir(workspace: &Path) -> ProjectStoreResult<Option<String>> {
    let absolute = run_git_common_dir(
        workspace,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
    )?;
    let value = match absolute {
        Some(value) => Some(value),
        None => run_git_common_dir(workspace, &["rev-parse", "--git-common-dir"])?,
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let path = PathBuf::from(value);
    let path = if path.is_absolute() {
        path
    } else {
        workspace.join(path)
    };
    let canonical = canonicalize_utf8(&path, "git common dir")?;
    let metadata = std::fs::symlink_metadata(&canonical)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(ProjectStoreError::Validation(
            "resolved git common dir is not a plain directory".to_string(),
        ));
    }
    Ok(Some(canonical))
}

fn run_git_common_dir(workspace: &Path, args: &[&str]) -> ProjectStoreResult<Option<String>> {
    let output = match Command::new("git")
        .current_dir(workspace)
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_CEILING_DIRECTORIES")
        .output()
    {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !output.status.success() {
        return Ok(None);
    }
    let output = String::from_utf8(output.stdout).map_err(|_| {
        ProjectStoreError::Validation("git common dir output is not valid UTF-8".to_string())
    })?;
    let output = output.trim();
    if output.is_empty() || output.contains('\0') || output.lines().count() != 1 {
        return Err(ProjectStoreError::Validation(
            "git common dir output is invalid".to_string(),
        ));
    }
    Ok(Some(output.to_string()))
}

fn canonicalize_utf8(path: &Path, field: &str) -> ProjectStoreResult<String> {
    let canonical = std::fs::canonicalize(path).map_err(|error| {
        ProjectStoreError::Validation(format!(
            "{field} could not be canonicalized ({}): {error}",
            path.display()
        ))
    })?;
    canonical
        .into_os_string()
        .into_string()
        .map_err(|_| ProjectStoreError::Validation(format!("{field} must be valid UTF-8")))
}

/// Canonicalize an existing candidate, or canonicalize its deepest existing
/// ancestor and append the missing suffix lexically. Project-aware preflight
/// uses this for a confinement relocation target before authorization has
/// materialized that directory.
fn canonicalize_candidate_utf8(path: &Path, field: &str) -> ProjectStoreResult<String> {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical
            .into_os_string()
            .into_string()
            .map_err(|_| ProjectStoreError::Validation(format!("{field} must be valid UTF-8")));
    }

    let mut missing = Vec::new();
    let mut probe = path;
    loop {
        if let Ok(mut canonical) = std::fs::canonicalize(probe) {
            for component in missing.into_iter().rev() {
                canonical.push(component);
            }
            let canonical = lexically_clean_candidate(&canonical);
            return canonical.into_os_string().into_string().map_err(|_| {
                ProjectStoreError::Validation(format!("{field} must be valid UTF-8"))
            });
        }
        let Some(parent) = probe.parent() else {
            return Err(ProjectStoreError::Validation(format!(
                "{field} has no existing ancestor ({})",
                path.display()
            )));
        };
        if let Some(component) = probe.components().next_back() {
            match component {
                Component::Normal(_) | Component::ParentDir | Component::CurDir => {
                    missing.push(component.as_os_str().to_os_string());
                }
                Component::Prefix(_) | Component::RootDir => {}
            }
        }
        probe = parent;
    }
}

fn lexically_clean_candidate(path: &Path) -> PathBuf {
    let mut clean = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                clean.pop();
            }
            Component::CurDir => {}
            other => clean.push(other.as_os_str()),
        }
    }
    clean
}

fn validate_manifest(manifest: &ProjectManifest) -> ProjectStoreResult<()> {
    if manifest.schema_version != PROJECT_MANIFEST_SCHEMA_VERSION {
        return Err(ProjectStoreError::Validation(format!(
            "unsupported project manifest schema {}",
            manifest.schema_version
        )));
    }
    if manifest.name.trim().is_empty() || manifest.name.len() > 200 {
        return Err(ProjectStoreError::Validation(
            "project name must be 1..=200 bytes".to_string(),
        ));
    }
    if manifest
        .description
        .as_ref()
        .is_some_and(|description| description.len() > 4096)
    {
        return Err(ProjectStoreError::Validation(
            "project description exceeds 4096 bytes".to_string(),
        ));
    }
    if manifest.revision == 0 || manifest.resource_revision == 0 {
        return Err(ProjectStoreError::Validation(
            "project revisions must be positive".to_string(),
        ));
    }
    let mut paths = HashSet::new();
    if let Some(project_path) = manifest.project_path.as_deref() {
        validate_absolute_path(project_path, "project_path")?;
        paths.insert(project_path);
    }
    match (manifest.project_path.as_ref(), manifest.project_path_status) {
        (Some(_), ProjectPathStatus::Configured)
        | (None, ProjectPathStatus::NeedsConfiguration | ProjectPathStatus::NeedsSelection) => {}
        (Some(_), status) => {
            return Err(ProjectStoreError::Validation(format!(
                "configured project_path has incompatible status {status:?}"
            )));
        }
        (None, ProjectPathStatus::Configured) => {
            return Err(ProjectStoreError::Validation(
                "project_path status is configured but no path is present".to_string(),
            ));
        }
    }
    for binding in &manifest.workspace_bindings {
        validate_absolute_path(&binding.path, "workspace binding")?;
        if !paths.insert(binding.path.as_str()) {
            return Err(ProjectStoreError::Validation(format!(
                "duplicate workspace root: {}",
                binding.path
            )));
        }
        if manifest
            .workspace_roots()
            .any(|other| other != binding.path && workspace_paths_overlap(&binding.path, other))
        {
            return Err(ProjectStoreError::Validation(format!(
                "overlapping workspace root: {}",
                binding.path
            )));
        }
        if binding
            .label
            .as_ref()
            .is_some_and(|label| label.is_empty() || label.len() > 100)
        {
            return Err(ProjectStoreError::Validation(
                "workspace label must be 1..=100 bytes".to_string(),
            ));
        }
        if let Some(git_common_dir) = &binding.git_common_dir {
            validate_absolute_path(git_common_dir, "git common dir")?;
        }
    }
    let mut legacy_keys = HashSet::new();
    for key in &manifest.legacy_project_keys {
        validate_legacy_project_key(key)?;
        if !legacy_keys.insert(key) {
            return Err(ProjectStoreError::Validation(
                "legacy project keys must be unique".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_absolute_path(value: &str, field: &str) -> ProjectStoreResult<()> {
    if value.is_empty() || !Path::new(value).is_absolute() {
        return Err(ProjectStoreError::Validation(format!(
            "{field} must be an absolute path"
        )));
    }
    Ok(())
}

fn validate_index(index: &ProjectIndex) -> ProjectStoreResult<()> {
    if index.schema_version != PROJECT_INDEX_SCHEMA_VERSION {
        return Err(ProjectStoreError::Validation(format!(
            "unsupported project index schema {}",
            index.schema_version
        )));
    }
    for (id, entry) in &index.projects {
        if id != &entry.id {
            return Err(ProjectStoreError::Validation(
                "project index key/id mismatch".to_string(),
            ));
        }
    }
    Ok(())
}

struct FileLock(File);

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.0);
    }
}

fn lock_exclusive(path: PathBuf) -> ProjectStoreResult<FileLock> {
    let parent = path.parent().ok_or_else(|| {
        ProjectStoreError::Validation("project lock has no parent directory".to_string())
    })?;
    assert_plain_directory(parent)?;
    validate_regular_file_if_exists(&path, "project lock")?;
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    configure_open_no_follow(&mut options);
    let file = options.open(&path)?;
    validate_open_regular_file(&file, &path, "project lock")?;
    file.lock_exclusive()?;
    assert_plain_directory(parent)?;
    validate_open_regular_file(&file, &path, "project lock")?;
    Ok(FileLock(file))
}

fn read_regular_file(path: &Path, label: &str) -> ProjectStoreResult<Vec<u8>> {
    validate_required_regular_file(path, label)?;
    let mut options = OpenOptions::new();
    options.read(true);
    configure_open_no_follow(&mut options);
    let mut file = options.open(path)?;
    validate_open_regular_file(&file, path, label)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    validate_open_regular_file(&file, path, label)?;
    Ok(bytes)
}

#[cfg(unix)]
fn configure_open_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(windows)]
fn configure_open_no_follow(options: &mut OpenOptions) {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
    options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
}

#[cfg(not(any(unix, windows)))]
fn configure_open_no_follow(_options: &mut OpenOptions) {}

fn validate_open_regular_file(file: &File, path: &Path, label: &str) -> ProjectStoreResult<()> {
    let opened = file.metadata()?;
    let current = std::fs::symlink_metadata(path)?;
    if !opened.is_file()
        || current.file_type().is_symlink()
        || !current.is_file()
        || !same_open_file(&opened, &current)
    {
        return Err(ProjectStoreError::Validation(format!(
            "{label} changed during no-follow open: {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn same_open_file(opened: &std::fs::Metadata, current: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    opened.dev() == current.dev() && opened.ino() == current.ino()
}

#[cfg(not(unix))]
fn same_open_file(_opened: &std::fs::Metadata, _current: &std::fs::Metadata) -> bool {
    true
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> ProjectStoreResult<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    write_bytes_atomic(path, &bytes)
}

fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> ProjectStoreResult<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    assert_plain_directory(parent)?;
    validate_regular_file_if_exists(path, "project store destination")?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("project.json");
    let temp = parent.join(format!(".{file_name}.tmp.{}", Uuid::new_v4()));
    let mut cleanup = TempCleanup(Some(temp.clone()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    assert_plain_directory(parent)?;
    validate_regular_file_if_exists(path, "project store destination")?;
    sync_directory(parent)?;
    replace_path(&temp, path)?;
    cleanup.0 = None;
    sync_directory(parent)?;
    Ok(())
}

#[cfg(windows)]
fn replace_path(source: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: both buffers are stable, NUL-terminated UTF-16 strings for the
    // duration of the call. MoveFileExW does not retain their pointers.
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn replace_path(source: &Path, target: &Path) -> std::io::Result<()> {
    std::fs::rename(source, target)
}

#[cfg(not(any(unix, windows)))]
fn replace_path(source: &Path, target: &Path) -> std::io::Result<()> {
    std::fs::rename(source, target)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
        .open(path)?;
    let opened = file.metadata()?;
    let current = std::fs::symlink_metadata(path)?;
    if !opened.is_dir()
        || current.file_type().is_symlink()
        || !current.is_dir()
        || !same_open_file(&opened, &current)
    {
        return Err(std::io::Error::other(format!(
            "directory changed during no-follow sync: {}",
            path.display()
        )));
    }
    file.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

struct TempCleanup(Option<PathBuf>);

impl Drop for TempCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn remove_temp_files_in(directory: &Path) -> ProjectStoreResult<()> {
    if !directory.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') && name.contains(".tmp.") && entry.file_type()?.is_file() {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn count_direct_entries(path: &Path) -> ProjectStoreResult<u64> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    Ok(entries.filter_map(Result::ok).count() as u64)
}

fn count_skills_layers(home: &Path) -> ProjectStoreResult<u64> {
    let mut count = 0;
    for entry in std::fs::read_dir(home)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if (name == "skills" || name.starts_with("skills-")) && entry.file_type()?.is_dir() {
            count += count_direct_entries(&entry.path())?;
        }
    }
    Ok(count)
}

fn resource_dir_summary(
    kind: ProjectResourceKind,
    path: &Path,
) -> ProjectStoreResult<ProjectResourceEntry> {
    let item_count = count_direct_entries(path)?;
    Ok(ProjectResourceEntry {
        kind,
        present: path.is_dir(),
        item_count,
    })
}

/// Produce a read-only legacy migration report. No Project, session, or memory
/// data is written. Basenames, remotes, missing paths, and path hashes never
/// become identity evidence.
pub fn plan_legacy_migration(
    inputs: &[LegacySessionProjectInput],
    projects: &[ProjectManifest],
) -> LegacyProjectDryRunReport {
    let mut report = LegacyProjectDryRunReport::default();
    let mut by_path: HashMap<&str, Vec<&ProjectManifest>> = HashMap::new();
    let mut by_git: HashMap<String, Vec<&ProjectManifest>> = HashMap::new();
    for project in projects {
        if let Some(project_path) = project.project_path.as_deref() {
            by_path.entry(project_path).or_default().push(project);
            let project_path = Path::new(project_path);
            let is_stable_plain_directory = std::fs::symlink_metadata(project_path)
                .ok()
                .is_some_and(|metadata| {
                    !metadata.file_type().is_symlink()
                        && metadata.is_dir()
                        && std::fs::canonicalize(project_path).ok().as_deref() == Some(project_path)
                });
            if is_stable_plain_directory {
                if let Ok(Some(git_common_dir)) = resolve_git_common_dir(project_path) {
                    by_git.entry(git_common_dir).or_default().push(project);
                }
            }
        }
        for binding in &project.workspace_bindings {
            by_path.entry(&binding.path).or_default().push(project);
            if let Some(git_common_dir) = binding.git_common_dir.as_deref() {
                by_git
                    .entry(git_common_dir.to_string())
                    .or_default()
                    .push(project);
            }
        }
    }

    let mut pending = Vec::new();
    for input in inputs {
        let exact = input
            .canonical_path
            .as_deref()
            .and_then(|path| by_path.get(path));
        if let Some(matches) = exact {
            if let Some(project) = unique_project(matches) {
                report.assignments.push(LegacyProjectAssignment {
                    session_id: input.session_id.clone(),
                    project_id: project.id.clone(),
                    basis: LegacyProjectMatchBasis::ExactCanonicalBinding,
                });
            } else {
                report.unassigned.push(LegacyProjectUnassigned {
                    session_id: input.session_id.clone(),
                    reason: "canonical workspace is bound to multiple Projects".to_string(),
                });
                report.diagnostics.push(format!(
                    "session {} has an ambiguous canonical workspace binding",
                    input.session_id
                ));
            }
            continue;
        }

        let git = input
            .git_common_dir
            .as_deref()
            .and_then(|path| by_git.get(path));
        if let Some(matches) = git {
            if let Some(project) = unique_project(matches) {
                report.assignments.push(LegacyProjectAssignment {
                    session_id: input.session_id.clone(),
                    project_id: project.id.clone(),
                    basis: LegacyProjectMatchBasis::GitCommonDir,
                });
            } else {
                report.unassigned.push(LegacyProjectUnassigned {
                    session_id: input.session_id.clone(),
                    reason: "git common dir is registered by multiple Projects".to_string(),
                });
                report.diagnostics.push(format!(
                    "session {} has an ambiguous git common dir",
                    input.session_id
                ));
            }
            continue;
        }
        pending.push(input);
    }

    let mut suggested = HashSet::new();
    suggest_groups(
        &pending,
        |input| input.canonical_path.as_deref(),
        LegacyProjectMatchBasis::ExactCanonicalBinding,
        &mut suggested,
        &mut report,
    );
    suggest_groups(
        &pending,
        |input| input.git_common_dir.as_deref(),
        LegacyProjectMatchBasis::GitCommonDir,
        &mut suggested,
        &mut report,
    );

    for input in pending {
        if !suggested.contains(&input.session_id) {
            report.unassigned.push(LegacyProjectUnassigned {
                session_id: input.session_id.clone(),
                reason: "no exact canonical binding or shared git common dir".to_string(),
            });
        }
    }
    report
}

fn unique_project<'a>(matches: &[&'a ProjectManifest]) -> Option<&'a ProjectManifest> {
    let mut ids = matches
        .iter()
        .map(|project| &project.id)
        .collect::<BTreeSet<_>>();
    if ids.len() == 1 {
        let id = ids.pop_first()?;
        matches.iter().copied().find(|project| &project.id == id)
    } else {
        None
    }
}

fn suggest_groups<'a>(
    pending: &[&'a LegacySessionProjectInput],
    key: impl Fn(&'a LegacySessionProjectInput) -> Option<&'a str>,
    basis: LegacyProjectMatchBasis,
    suggested: &mut HashSet<String>,
    report: &mut LegacyProjectDryRunReport,
) {
    let mut groups: BTreeMap<&str, Vec<&LegacySessionProjectInput>> = BTreeMap::new();
    for input in pending {
        if !suggested.contains(&input.session_id) {
            if let Some(key) = key(input) {
                groups.entry(key).or_default().push(input);
            }
        }
    }
    for group in groups.into_values().filter(|group| group.len() >= 2) {
        let mut session_ids = BTreeSet::new();
        let mut workspace_paths = BTreeSet::new();
        let mut legacy_project_keys = BTreeSet::new();
        for input in group {
            session_ids.insert(input.session_id.clone());
            if let Some(workspace_path) = &input.workspace_path {
                workspace_paths.insert(workspace_path.clone());
            }
            legacy_project_keys.extend(input.legacy_project_keys.iter().cloned());
        }
        suggested.extend(session_ids.iter().cloned());
        report.suggestions.push(LegacyProjectSuggestion {
            basis,
            session_ids: session_ids.into_iter().collect(),
            workspace_paths: workspace_paths.into_iter().collect(),
            legacy_project_keys: legacy_project_keys.into_iter().collect(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::WorkspaceBinding;
    use tempfile::TempDir;

    fn store() -> (TempDir, ProjectStore) {
        let temp = tempfile::tempdir().unwrap();
        let store = ProjectStore::open(temp.path()).unwrap();
        (temp, store)
    }

    fn binding(path: &Path) -> WorkspaceBinding {
        WorkspaceBinding {
            path: path.to_string_lossy().into_owned(),
            label: None,
            git_common_dir: None,
        }
    }

    #[test]
    fn paths_never_use_name_and_reject_traversal_components() {
        let paths = ProjectPaths::new("/tmp/bamboo-data");
        let id: ProjectId = "01JPROJECT00000000000000000".parse().unwrap();
        assert_eq!(
            paths.project_home(&id),
            Path::new("/tmp/bamboo-data/projects/01JPROJECT00000000000000000")
        );
        assert!(paths.skills_dir(&id, Some("../escape")).is_err());
        assert!(paths.skills_dir(&id, Some("ask")).is_ok());
    }

    #[test]
    fn create_update_cas_and_rename_keep_home_stable() {
        let (_temp, store) = store();
        let created = store.create("Zenith", None).unwrap();
        let home = store.paths().project_home(&created.id);
        let updated = store
            .update(&created.id, created.revision, |project| {
                project.name = "Zenith renamed".to_string();
                Ok(())
            })
            .unwrap();
        assert_eq!(updated.revision, 2);
        assert_eq!(store.paths().project_home(&updated.id), home);
        assert!(matches!(
            store.update(&created.id, 1, |_| Ok(())),
            Err(ProjectStoreError::Conflict {
                expected: 1,
                actual: 2
            })
        ));
    }

    #[test]
    fn unarchive_is_atomic_and_preserves_project_identity_and_resources() {
        let (temp, store) = store();
        let project_path = temp.path().join("project");
        let workspace_path = temp.path().join("workspace");
        std::fs::create_dir_all(&project_path).unwrap();
        std::fs::create_dir_all(&workspace_path).unwrap();

        let project = store
            .create_with_project_path(
                "Zenith",
                Some("Project restore contract".to_string()),
                project_path.to_string_lossy(),
                vec![WorkspaceBinding {
                    path: workspace_path.to_string_lossy().into_owned(),
                    label: Some("Issue worktree".to_string()),
                    git_common_dir: None,
                }],
            )
            .unwrap();
        assert!(matches!(
            store.unarchive(&project.id, project.revision),
            Err(ProjectStoreError::NotArchived(project_id)) if project_id == project.id
        ));
        assert_eq!(
            store.get(&project.id).unwrap().revision,
            project.revision,
            "rejected restore must not bump the manifest"
        );

        let project = store
            .update(&project.id, project.revision, |manifest| {
                manifest
                    .legacy_project_keys
                    .push("legacy-zenith".to_string());
                Ok(())
            })
            .unwrap();
        let project = store
            .bump_resource_revision(&project.id, project.revision)
            .unwrap();
        let settings_path = store.paths().settings_path(&project.id);
        let memory_path = store.paths().memory_v1_dir(&project.id).join("index.json");
        std::fs::create_dir_all(memory_path.parent().unwrap()).unwrap();
        std::fs::write(&settings_path, br#"{"theme":"dark"}"#).unwrap();
        std::fs::write(&memory_path, br#"{"entries":["stable"]}"#).unwrap();

        let archived = store.archive(&project.id, project.revision).unwrap();
        let settings_before = std::fs::read(&settings_path).unwrap();
        let memory_before = std::fs::read(&memory_path).unwrap();
        let restored = store.unarchive(&archived.id, archived.revision).unwrap();

        assert_eq!(restored.status, ProjectStatus::Active);
        assert_eq!(restored.revision, archived.revision + 1);
        assert!(restored.updated_at >= archived.updated_at);
        assert_eq!(restored.id, archived.id);
        assert_eq!(restored.project_path, archived.project_path);
        assert_eq!(restored.project_path_status, archived.project_path_status);
        assert_eq!(restored.workspace_bindings, archived.workspace_bindings);
        assert_eq!(restored.legacy_project_keys, archived.legacy_project_keys);
        assert_eq!(restored.resource_revision, archived.resource_revision);
        assert_eq!(restored.created_at, archived.created_at);
        assert_eq!(std::fs::read(&settings_path).unwrap(), settings_before);
        assert_eq!(std::fs::read(&memory_path).unwrap(), memory_before);

        assert!(matches!(
            store.unarchive(&restored.id, restored.revision),
            Err(ProjectStoreError::NotArchived(project_id)) if project_id == restored.id
        ));
        assert_eq!(
            store.get(&restored.id).unwrap(),
            restored,
            "repeated restore must leave the canonical manifest unchanged"
        );
    }

    #[test]
    fn project_path_is_canonical_owned_and_cas_update_keeps_identity() {
        let (temp, store) = store();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        std::fs::create_dir_all(first.join("nested")).unwrap();
        std::fs::create_dir_all(&second).unwrap();

        let project = store
            .create_with_project_path(
                "Zenith",
                None,
                first.join("nested").join("..").to_string_lossy(),
                Vec::new(),
            )
            .unwrap();
        let first = first.canonicalize().unwrap().to_string_lossy().into_owned();
        assert_eq!(project.project_path.as_deref(), Some(first.as_str()));
        assert_eq!(
            store
                .find_workspace_owner(&first)
                .unwrap()
                .map(|owner| owner.id),
            Some(project.id.clone())
        );

        let attempted_override = first.clone();
        let updated = store
            .update_with_project_path(
                &project.id,
                project.revision,
                second.to_string_lossy().as_ref(),
                move |manifest| {
                    manifest.project_path = Some(attempted_override);
                    manifest.project_path_status = ProjectPathStatus::NeedsSelection;
                    Ok(())
                },
            )
            .unwrap();
        let second = second
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert_eq!(updated.id, project.id);
        assert_eq!(updated.project_path.as_deref(), Some(second.as_str()));
        assert!(store.find_workspace_owner(&first).unwrap().is_none());
        assert_eq!(
            store
                .find_workspace_owner(&second)
                .unwrap()
                .map(|owner| owner.id),
            Some(project.id.clone())
        );
        assert!(matches!(
            store.unbind_workspace(&project.id, updated.revision, &second),
            Err(ProjectStoreError::ProjectPathUnbindConflict {
                project_id,
                project_path,
            }) if project_id == project.id && project_path == second
        ));

        let reopened = ProjectStore::open(temp.path()).unwrap();
        let indexed = reopened.index().unwrap();
        assert_eq!(
            indexed.projects[&project.id].project_path.as_deref(),
            Some(second.as_str())
        );
        assert_eq!(
            indexed.projects[&project.id].project_path_status,
            ProjectPathStatus::Configured
        );
    }

    #[test]
    fn project_path_create_and_cas_update_reject_cross_project_overlap() {
        let (temp, store) = store();
        let owner_root = temp.path().join("owner");
        let nested = owner_root.join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        let owner = store
            .create_with_project_path("Owner", None, owner_root.to_string_lossy(), Vec::new())
            .unwrap();
        let project_count = store.list().unwrap().len();

        assert!(matches!(
            store.create_with_project_path(
                "Overlapping create",
                None,
                nested.to_string_lossy(),
                Vec::new(),
            ),
            Err(ProjectStoreError::Validation(message)) if message.contains("overlaps")
        ));
        assert_eq!(store.list().unwrap().len(), project_count);

        let target = store.create("Target", None).unwrap();
        assert!(matches!(
            store.update_with_project_path(
                &target.id,
                target.revision,
                nested.to_string_lossy().as_ref(),
                |_| Ok(()),
            ),
            Err(ProjectStoreError::Validation(message)) if message.contains("overlaps")
        ));
        let unchanged = store.get(&target.id).unwrap();
        assert_eq!(unchanged.revision, target.revision);
        assert!(unchanged.project_path.is_none());
        assert_eq!(
            store
                .find_workspace_owner_for_path(nested.to_string_lossy().as_ref())
                .unwrap()
                .map(|project| project.id),
            Some(owner.id)
        );
    }

    #[test]
    fn v1_manifest_migration_promotes_only_one_binding() {
        fn rewrite_as_v1(store: &ProjectStore, project: &ProjectManifest) {
            let path = store.paths().manifest_path(&project.id);
            let mut value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            value["schema_version"] = serde_json::json!(1);
            value.as_object_mut().unwrap().remove("project_path");
            std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
        }

        let (temp, store) = store();
        let single = temp.path().join("single");
        let multiple_a = temp.path().join("multiple-a");
        let multiple_b = temp.path().join("multiple-b");
        std::fs::create_dir_all(&single).unwrap();
        std::fs::create_dir_all(&multiple_a).unwrap();
        std::fs::create_dir_all(&multiple_b).unwrap();

        let one = store
            .create_with_bindings("one", None, vec![binding(&single)])
            .unwrap();
        rewrite_as_v1(&store, &one);
        let migrated = store.get(&one.id).unwrap();
        assert_eq!(migrated.schema_version, PROJECT_MANIFEST_SCHEMA_VERSION);
        assert_eq!(migrated.revision, one.revision + 1);
        assert_eq!(
            migrated.project_path.as_deref(),
            Some(single.canonicalize().unwrap().to_string_lossy().as_ref())
        );
        assert_eq!(migrated.project_path_status, ProjectPathStatus::Configured);
        assert!(migrated.workspace_bindings.is_empty());
        let backup: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                store
                    .paths()
                    .project_home(&one.id)
                    .join(PROJECT_MANIFEST_BACKUP_FILE),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(backup["schema_version"], 1);

        let zero = store.create("zero", None).unwrap();
        rewrite_as_v1(&store, &zero);
        let migrated = store.get(&zero.id).unwrap();
        assert!(migrated.project_path.is_none());
        assert_eq!(
            migrated.project_path_status,
            ProjectPathStatus::NeedsConfiguration
        );
        assert!(migrated.workspace_bindings.is_empty());

        let many = store
            .create_with_bindings(
                "many",
                None,
                vec![binding(&multiple_a), binding(&multiple_b)],
            )
            .unwrap();
        rewrite_as_v1(&store, &many);
        let migrated = store.get(&many.id).unwrap();
        assert!(migrated.project_path.is_none());
        assert_eq!(
            migrated.project_path_status,
            ProjectPathStatus::NeedsSelection
        );
        assert_eq!(migrated.workspace_bindings.len(), 2);
    }

    #[test]
    fn atomic_writer_replaces_existing_target_without_remove_window() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("project.json");
        write_bytes_atomic(&target, b"old").unwrap();
        write_bytes_atomic(&target, b"new").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        assert!(
            std::fs::read_dir(temp.path())
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp.")),
            "atomic replacement must not leave a temp file"
        );
    }

    #[cfg(unix)]
    #[test]
    fn projects_symlink_is_rejected_without_external_writes() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        symlink(outside.path(), temp.path().join("projects")).unwrap();

        assert!(ProjectStore::open(temp.path()).is_err());
        assert_eq!(
            std::fs::read_dir(outside.path()).unwrap().count(),
            0,
            "opening a registry must not create locks or index files through a projects symlink"
        );
    }

    #[cfg(unix)]
    #[test]
    fn project_home_symlink_is_rejected_without_external_writes() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let store = ProjectStore::open(temp.path()).unwrap();
        let project_id: ProjectId = "01JPROJECTHOMESYMLINK00000".parse().unwrap();
        symlink(outside.path(), store.paths().project_home(&project_id)).unwrap();

        assert!(store
            .create_with_id(project_id, "Unsafe home", None)
            .is_err());
        assert_eq!(
            std::fs::read_dir(outside.path()).unwrap().count(),
            0,
            "creating a Project must not create a lock, state, or manifest through a home symlink"
        );
    }

    #[cfg(unix)]
    #[test]
    fn manifest_symlink_is_rejected_without_external_writes() {
        use std::os::unix::fs::symlink;

        let (_temp, store) = store();
        let project = store.create("Manifest safety", None).unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_manifest = outside.path().join("external.json");
        let sentinel = b"external sentinel";
        std::fs::write(&outside_manifest, sentinel).unwrap();
        let manifest = store.paths().manifest_path(&project.id);
        std::fs::remove_file(&manifest).unwrap();
        symlink(&outside_manifest, &manifest).unwrap();

        assert!(store.get(&project.id).is_err());
        assert_eq!(std::fs::read(&outside_manifest).unwrap(), sentinel);
        assert_eq!(
            std::fs::read_dir(outside.path()).unwrap().count(),
            1,
            "manifest recovery must not quarantine or replace an external symlink target"
        );
    }

    #[cfg(unix)]
    #[test]
    fn index_and_lock_symlinks_are_rejected_without_external_writes() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects");
        std::fs::create_dir(&projects).unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_lock = outside.path().join("external.lock");
        std::fs::write(&outside_lock, b"lock sentinel").unwrap();
        symlink(&outside_lock, projects.join(".index.lock")).unwrap();
        assert!(ProjectStore::open(temp.path()).is_err());
        assert_eq!(
            std::fs::read(&outside_lock).unwrap(),
            b"lock sentinel",
            "registry locking must not open an external symlink target"
        );

        std::fs::remove_file(projects.join(".index.lock")).unwrap();
        let store = ProjectStore::open(temp.path()).unwrap();
        let outside_index = outside.path().join("external-index.json");
        std::fs::write(&outside_index, b"index sentinel").unwrap();
        std::fs::remove_file(store.paths().index_path()).unwrap();
        symlink(&outside_index, store.paths().index_path()).unwrap();
        assert!(store.rebuild_index().is_err());
        assert_eq!(
            std::fs::read(&outside_index).unwrap(),
            b"index sentinel",
            "index rebuild must not read, quarantine, or replace an external symlink target"
        );
    }

    #[test]
    fn binding_is_canonicalized_and_cross_project_conflicts() {
        let (temp, store) = store();
        let project_a = store.create("A", None).unwrap();
        let project_b = store.create("B", None).unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(workspace.join("nested")).unwrap();
        let non_canonical = workspace.join("nested").join("..");

        let bound = store
            .bind_workspace(
                &project_a.id,
                project_a.revision,
                WorkspaceBinding {
                    path: non_canonical.to_string_lossy().into_owned(),
                    label: Some("main".to_string()),
                    git_common_dir: None,
                },
            )
            .unwrap();
        let canonical = std::fs::canonicalize(&workspace)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert_eq!(bound.workspace_bindings[0].path, canonical);
        assert_eq!(bound.workspace_bindings[0].git_common_dir, None);
        assert_eq!(
            store
                .find_workspace_owner(non_canonical.to_string_lossy().as_ref())
                .unwrap()
                .map(|project| project.id),
            Some(project_a.id.clone())
        );
        assert!(store
            .bind_workspace(
                &project_b.id,
                project_b.revision,
                WorkspaceBinding {
                    path: workspace.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .is_err());

        let projects_before = store.list().unwrap().len();
        assert!(store
            .create_with_bindings(
                "conflicting-create",
                None,
                vec![WorkspaceBinding {
                    path: workspace.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .is_err());
        assert_eq!(
            store.list().unwrap().len(),
            projects_before,
            "a binding conflict must not leave a partially created Project"
        );
    }

    #[test]
    fn exact_stored_binding_can_be_unbound_after_workspace_disappears() {
        let (temp, store) = store();
        let project = store.create("A", None).unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let bound = store
            .bind_workspace(&project.id, project.revision, binding(&workspace))
            .unwrap();
        let stored_path = bound.workspace_bindings[0].path.clone();
        std::fs::remove_dir(&workspace).unwrap();

        let unbound = store
            .unbind_workspace(&project.id, bound.revision, &stored_path)
            .unwrap();
        assert!(unbound.workspace_bindings.is_empty());
        assert!(!workspace.exists());
    }

    #[test]
    fn unbind_uses_canonical_alias_only_after_raw_path_misses() {
        let (temp, store) = store();
        let project = store.create("A", None).unwrap();
        let workspace = temp.path().join("workspace");
        let nested = workspace.join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        let bound = store
            .bind_workspace(&project.id, project.revision, binding(&workspace))
            .unwrap();
        let alias = nested.join("..");

        let unbound = store
            .unbind_workspace(
                &project.id,
                bound.revision,
                alias.to_string_lossy().as_ref(),
            )
            .unwrap();
        assert!(unbound.workspace_bindings.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn exact_unbind_does_not_follow_replaced_workspace_symlink() {
        use std::os::unix::fs::symlink;

        let (temp, store) = store();
        let project = store.create("A", None).unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let bound = store
            .bind_workspace(&project.id, project.revision, binding(&workspace))
            .unwrap();
        let stored_path = bound.workspace_bindings[0].path.clone();

        std::fs::remove_dir(&workspace).unwrap();
        let outside = tempfile::tempdir().unwrap();
        let sentinel = outside.path().join("sentinel");
        std::fs::write(&sentinel, b"external data").unwrap();
        symlink(outside.path(), &workspace).unwrap();

        let unbound = store
            .unbind_workspace(&project.id, bound.revision, &stored_path)
            .unwrap();
        assert!(unbound.workspace_bindings.is_empty());
        assert!(std::fs::symlink_metadata(&workspace)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"external data");
        assert_eq!(
            std::fs::read_dir(outside.path()).unwrap().count(),
            1,
            "exact unbind must not follow or write through the replacement symlink"
        );
    }

    #[test]
    fn workspace_descendant_resolves_registered_owner() {
        let (temp, store) = store();
        let project_a = store.create("A", None).unwrap();
        let project_b = store.create("B", None).unwrap();
        let workspace_a = temp.path().join("workspace-a");
        let workspace_b = temp.path().join("workspace-b");
        let nested_b = workspace_b.join("nested").join("deeper");
        std::fs::create_dir_all(&workspace_a).unwrap();
        std::fs::create_dir_all(&nested_b).unwrap();
        store
            .bind_workspace(
                &project_a.id,
                project_a.revision,
                WorkspaceBinding {
                    path: workspace_a.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        store
            .bind_workspace(
                &project_b.id,
                project_b.revision,
                WorkspaceBinding {
                    path: workspace_b.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();

        let owner = store
            .find_workspace_owner_for_path(nested_b.to_string_lossy().as_ref())
            .unwrap()
            .unwrap();
        assert_eq!(owner.id, project_b.id);
    }

    #[test]
    fn missing_descendant_parent_escape_resolves_sibling_owner() {
        let (temp, store) = store();
        let project_a = store.create("A", None).unwrap();
        let project_b = store.create("B", None).unwrap();
        let workspace_a = temp.path().join("workspace-a");
        let workspace_b = temp.path().join("workspace-b");
        std::fs::create_dir_all(&workspace_a).unwrap();
        std::fs::create_dir_all(&workspace_b).unwrap();
        store
            .bind_workspace(&project_a.id, project_a.revision, binding(&workspace_a))
            .unwrap();
        store
            .bind_workspace(&project_b.id, project_b.revision, binding(&workspace_b))
            .unwrap();

        let escaped_missing = workspace_a
            .join("missing")
            .join("..")
            .join("..")
            .join("workspace-b")
            .join("new");
        assert!(!escaped_missing.exists());
        assert_eq!(
            canonicalize_candidate_utf8(&escaped_missing, "test").unwrap(),
            workspace_b
                .canonicalize()
                .unwrap()
                .join("new")
                .to_string_lossy()
        );
        let owner = store
            .find_workspace_owner_for_path(escaped_missing.to_string_lossy().as_ref())
            .unwrap()
            .expect("sibling owner");
        assert_eq!(owner.id, project_b.id);
    }

    #[test]
    fn outer_then_inner_cross_project_binding_is_rejected() {
        let (temp, store) = store();
        let project_a = store.create("A", None).unwrap();
        let project_b = store.create("B", None).unwrap();
        let outer = temp.path().join("outer");
        let inner = outer.join("inner");
        let candidate = inner.join("src");
        std::fs::create_dir_all(&candidate).unwrap();
        store
            .bind_workspace(&project_a.id, project_a.revision, binding(&outer))
            .unwrap();
        let error = store
            .bind_workspace(&project_b.id, project_b.revision, binding(&inner))
            .unwrap_err();
        assert!(
            matches!(error, ProjectStoreError::Validation(message) if message.contains("overlaps"))
        );
        let owner = store
            .find_workspace_owner_for_path(candidate.to_string_lossy().as_ref())
            .unwrap()
            .unwrap();
        assert_eq!(owner.id, project_a.id);
    }

    #[test]
    fn inner_then_outer_cross_project_binding_is_rejected() {
        let (temp, store) = store();
        let project_a = store.create("A", None).unwrap();
        let project_b = store.create("B", None).unwrap();
        let outer = temp.path().join("outer");
        let inner = outer.join("inner");
        let candidate = inner.join("src");
        std::fs::create_dir_all(&candidate).unwrap();
        store
            .bind_workspace(&project_b.id, project_b.revision, binding(&inner))
            .unwrap();
        let error = store
            .bind_workspace(&project_a.id, project_a.revision, binding(&outer))
            .unwrap_err();
        assert!(
            matches!(error, ProjectStoreError::Validation(message) if message.contains("overlaps"))
        );
        let owner = store
            .find_workspace_owner_for_path(candidate.to_string_lossy().as_ref())
            .unwrap()
            .unwrap();
        assert_eq!(owner.id, project_b.id);
    }

    #[test]
    fn create_rejects_external_and_internal_binding_overlap() {
        let (temp, store) = store();
        let outer = temp.path().join("outer");
        let inner = outer.join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        let existing = store
            .create_with_bindings("existing", None, vec![binding(&inner)])
            .unwrap();
        let count = store.list().unwrap().len();

        assert!(store
            .create_with_bindings("external overlap", None, vec![binding(&outer)])
            .is_err());
        assert!(store
            .create_with_bindings(
                "internal overlap",
                None,
                vec![binding(&outer), binding(&inner)],
            )
            .is_err());
        assert_eq!(store.list().unwrap().len(), count);
        assert_eq!(
            store
                .find_workspace_owner(inner.to_string_lossy().as_ref())
                .unwrap()
                .unwrap()
                .id,
            existing.id
        );
    }

    #[test]
    fn same_project_overlap_and_generic_update_bypass_are_rejected() {
        let (temp, store) = store();
        let project = store.create("A", None).unwrap();
        let outer = temp.path().join("outer");
        let inner = outer.join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        let bound = store
            .bind_workspace(&project.id, project.revision, binding(&outer))
            .unwrap();

        assert!(store
            .bind_workspace(&project.id, bound.revision, binding(&inner))
            .is_err());
        assert!(store
            .update(&project.id, bound.revision, |manifest| {
                manifest.workspace_bindings.push(binding(&inner));
                Ok(())
            })
            .is_err());
        let unchanged = store.get(&project.id).unwrap();
        assert_eq!(unchanged.revision, bound.revision);
        assert_eq!(unchanged.workspace_bindings.len(), 1);
    }

    #[test]
    fn component_boundary_paths_do_not_overlap() {
        let (temp, store) = store();
        let project_a = store.create("A", None).unwrap();
        let project_b = store.create("B", None).unwrap();
        let repo = temp.path().join("repo");
        let repo2 = temp.path().join("repo2");
        let repo2_child = repo2.join("src");
        std::fs::create_dir_all(&repo).unwrap();
        std::fs::create_dir_all(&repo2_child).unwrap();
        store
            .bind_workspace(&project_a.id, project_a.revision, binding(&repo))
            .unwrap();
        store
            .bind_workspace(&project_b.id, project_b.revision, binding(&repo2))
            .unwrap();

        let owner = store
            .find_workspace_owner_for_path(repo2_child.to_string_lossy().as_ref())
            .unwrap()
            .unwrap();
        assert_eq!(owner.id, project_b.id);
    }

    fn run_git(cwd: &Path, args: &[&str]) {
        let output = Command::new("git")
            .current_dir(cwd)
            .args(args)
            .output()
            .expect("git must be installed for repository identity tests");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn initialize_git_repository(root: &Path) {
        std::fs::create_dir_all(root).unwrap();
        run_git(root, &["init"]);
        run_git(
            root,
            &["config", "user.email", "project-store@example.test"],
        );
        run_git(root, &["config", "user.name", "Project Store Test"]);
        std::fs::write(root.join("README.md"), "project identity\n").unwrap();
        run_git(root, &["add", "README.md"]);
        run_git(root, &["commit", "-m", "initial"]);
    }

    #[test]
    fn repository_and_linked_worktree_use_the_actual_common_dir() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repository");
        let linked_worktree = temp.path().join("linked-worktree");
        initialize_git_repository(&repository);
        let linked_worktree_arg = linked_worktree.to_string_lossy().into_owned();
        run_git(
            &repository,
            &["worktree", "add", "-b", "linked", &linked_worktree_arg],
        );

        let store = ProjectStore::open(temp.path().join("data")).unwrap();
        let project = store.create("Git project", None).unwrap();
        let bound_repository = store
            .bind_workspace(
                &project.id,
                project.revision,
                WorkspaceBinding {
                    path: repository.to_string_lossy().into_owned(),
                    label: Some("main".to_string()),
                    git_common_dir: None,
                },
            )
            .unwrap();
        let expected_common_dir = std::fs::canonicalize(repository.join(".git"))
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            bound_repository.workspace_bindings[0]
                .git_common_dir
                .as_deref(),
            Some(expected_common_dir.as_str())
        );

        let bound_linked_worktree = store
            .bind_workspace(
                &project.id,
                bound_repository.revision,
                WorkspaceBinding {
                    path: linked_worktree.to_string_lossy().into_owned(),
                    label: Some("linked".to_string()),
                    git_common_dir: None,
                },
            )
            .unwrap();
        assert_eq!(bound_linked_worktree.workspace_bindings.len(), 2);
        assert!(bound_linked_worktree
            .workspace_bindings
            .iter()
            .all(|binding| {
                binding.git_common_dir.as_deref() == Some(expected_common_dir.as_str())
            }));
    }

    #[test]
    fn migrated_primary_project_path_retains_git_evidence_for_linked_worktree() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repository");
        let linked_worktree = temp.path().join("linked-worktree");
        initialize_git_repository(&repository);
        let linked_worktree_arg = linked_worktree.to_string_lossy().into_owned();
        run_git(
            &repository,
            &[
                "worktree",
                "add",
                "-b",
                "linked-migration",
                &linked_worktree_arg,
            ],
        );

        let store = ProjectStore::open(temp.path().join("data")).unwrap();
        let legacy = store
            .create_with_bindings(
                "Legacy Git Project",
                None,
                vec![WorkspaceBinding {
                    path: repository.to_string_lossy().into_owned(),
                    label: Some("main".to_string()),
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let manifest_path = store.paths().manifest_path(&legacy.id);
        let mut raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        raw["schema_version"] = serde_json::json!(1);
        raw.as_object_mut().unwrap().remove("project_path");
        raw.as_object_mut().unwrap().remove("project_path_status");
        std::fs::write(&manifest_path, serde_json::to_vec_pretty(&raw).unwrap()).unwrap();

        let migrated = store.get(&legacy.id).unwrap();
        assert_eq!(
            migrated.project_path.as_deref(),
            Some(
                repository
                    .canonicalize()
                    .unwrap()
                    .to_string_lossy()
                    .as_ref()
            )
        );
        assert!(migrated.workspace_bindings.is_empty());
        let linked_canonical = linked_worktree.canonicalize().unwrap();
        let linked_git_common_dir = resolve_git_common_dir(&linked_canonical)
            .unwrap()
            .expect("linked worktree common dir");
        let report = plan_legacy_migration(
            &[LegacySessionProjectInput {
                session_id: "linked-legacy-session".to_string(),
                workspace_path: Some(linked_canonical.to_string_lossy().into_owned()),
                canonical_path: Some(linked_canonical.to_string_lossy().into_owned()),
                git_common_dir: Some(linked_git_common_dir),
                legacy_project_keys: Vec::new(),
            }],
            &[migrated],
        );
        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].project_id, legacy.id);
        assert_eq!(
            report.assignments[0].basis,
            LegacyProjectMatchBasis::GitCommonDir
        );
    }

    #[cfg(unix)]
    #[test]
    fn legacy_dry_run_rejects_git_evidence_from_replaced_project_path_symlink() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let configured_path = temp.path().join("configured-project");
        let replacement_repository = temp.path().join("replacement-repository");
        std::fs::create_dir_all(&configured_path).unwrap();
        let configured_path = configured_path.canonicalize().unwrap();
        initialize_git_repository(&replacement_repository);
        let replacement_common_dir = resolve_git_common_dir(&replacement_repository)
            .unwrap()
            .expect("replacement common dir");
        let mut project = ProjectManifest::new(
            "01JREPLACED000000000000000".parse().unwrap(),
            "Replaced Project path",
            None,
            Utc::now(),
        );
        project.project_path = Some(configured_path.to_string_lossy().into_owned());
        project.project_path_status = ProjectPathStatus::Configured;

        std::fs::remove_dir(&configured_path).unwrap();
        symlink(&replacement_repository, &configured_path).unwrap();

        let report = plan_legacy_migration(
            &[LegacySessionProjectInput {
                session_id: "replacement-session".to_string(),
                workspace_path: Some(replacement_repository.to_string_lossy().into_owned()),
                canonical_path: Some(
                    replacement_repository
                        .canonicalize()
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                ),
                git_common_dir: Some(replacement_common_dir),
                legacy_project_keys: Vec::new(),
            }],
            &[project],
        );
        assert!(report.assignments.is_empty());
        assert!(report
            .unassigned
            .iter()
            .any(|entry| entry.session_id == "replacement-session"));
    }

    #[test]
    fn forged_git_common_dir_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path().join("repository");
        let forged_common_dir = temp.path().join("forged-common-dir");
        initialize_git_repository(&repository);
        std::fs::create_dir_all(&forged_common_dir).unwrap();

        let store = ProjectStore::open(temp.path().join("data")).unwrap();
        let project = store.create("Git project", None).unwrap();
        let error = store
            .bind_workspace(
                &project.id,
                project.revision,
                WorkspaceBinding {
                    path: repository.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: Some(forged_common_dir.to_string_lossy().into_owned()),
                },
            )
            .unwrap_err();
        assert!(
            matches!(error, ProjectStoreError::Validation(message) if message.contains(
                "supplied git common dir does not match workspace"
            ))
        );
        assert!(store
            .get(&project.id)
            .unwrap()
            .workspace_bindings
            .is_empty());
    }

    #[test]
    fn concurrent_cas_allows_exactly_one_writer() {
        let (_temp, store) = store();
        let project = store.create("CAS", None).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let mut threads = Vec::new();
        for name in ["winner-a", "winner-b"] {
            let store = store.clone();
            let id = project.id.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                store.update(&id, 1, |manifest| {
                    manifest.name = name.to_string();
                    Ok(())
                })
            }));
        }
        barrier.wait();
        let results = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(ProjectStoreError::Conflict { .. })))
                .count(),
            1
        );
        assert_eq!(store.get(&project.id).unwrap().revision, 2);
    }

    #[test]
    fn corrupt_primary_recovers_from_backup_and_index_rebuild_skips_bad_record() {
        let (temp, store) = store();
        let created = store.create("Recover", None).unwrap();
        let updated = store
            .update(&created.id, 1, |project| {
                project.description = Some("new".to_string());
                Ok(())
            })
            .unwrap();
        std::fs::write(store.paths().manifest_path(&created.id), b"{broken").unwrap();
        let recovered = store.get(&created.id).unwrap();
        assert_eq!(
            recovered.revision, 3,
            "recovery must advance past the issued revision floor"
        );
        assert_eq!(recovered.description, None);
        assert!(matches!(
            store.update(&created.id, updated.revision, |_| Ok(())),
            Err(ProjectStoreError::Conflict {
                expected: 2,
                actual: 3
            })
        ));

        let bad_id: ProjectId = "01JBADPROJECT000000000000000".parse().unwrap();
        let bad_home = store.paths().project_home(&bad_id);
        std::fs::create_dir_all(&bad_home).unwrap();
        std::fs::write(bad_home.join(PROJECT_MANIFEST_FILE), b"{broken").unwrap();
        let reopened = ProjectStore::open(temp.path()).unwrap();
        let index = reopened.index().unwrap();
        assert!(index.projects.contains_key(&created.id));
        assert!(!index.projects.contains_key(&bad_id));
        assert!(recovered.revision > updated.revision);
    }

    #[test]
    fn corrupt_derived_index_is_quarantined_and_rebuilt() {
        let (temp, store) = store();
        let created = store.create("Indexed", None).unwrap();
        std::fs::write(store.paths().index_path(), b"{broken-index").unwrap();

        let reopened = ProjectStore::open(temp.path()).unwrap();
        assert!(reopened.index().unwrap().projects.contains_key(&created.id));
        assert!(
            std::fs::read_dir(reopened.paths().projects_dir())
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("index.json.corrupt.")),
            "corrupt derived index bytes should be retained for diagnostics"
        );
    }

    #[test]
    fn resource_summary_is_redacted_counts_only() {
        let (_temp, store) = store();
        let created = store.create("Resources", None).unwrap();
        let skills = store.paths().skills_dir(&created.id, None).unwrap();
        std::fs::create_dir_all(&skills).unwrap();
        std::fs::write(skills.join("secret-token-skill"), "super-secret").unwrap();
        std::fs::write(
            store.paths().settings_path(&created.id),
            r#"{"api_key":"never-return"}"#,
        )
        .unwrap();
        let summary = store.resource_summary(&created.id).unwrap();
        let encoded = serde_json::to_string(&summary).unwrap();
        assert!(!encoded.contains("super-secret"));
        assert!(!encoded.contains("never-return"));
        assert_eq!(
            summary
                .resources
                .iter()
                .find(|entry| entry.kind == ProjectResourceKind::Skills)
                .map(|entry| entry.item_count),
            Some(1)
        );
    }

    #[test]
    fn legacy_dry_run_only_uses_safe_evidence() {
        let now = Utc::now();
        let mut existing = ProjectManifest::new(
            "01JEXISTING0000000000000000".parse().unwrap(),
            "Existing",
            None,
            now,
        );
        existing.workspace_bindings.push(WorkspaceBinding {
            path: "/work/main".to_string(),
            label: None,
            git_common_dir: Some("/work/repo/.git".to_string()),
        });
        let inputs = vec![
            LegacySessionProjectInput {
                session_id: "exact".to_string(),
                workspace_path: Some("/work/main".to_string()),
                canonical_path: Some("/work/main".to_string()),
                git_common_dir: None,
                legacy_project_keys: vec![],
            },
            LegacySessionProjectInput {
                session_id: "linked-a".to_string(),
                workspace_path: Some("/other/a".to_string()),
                canonical_path: Some("/other/a".to_string()),
                git_common_dir: Some("/other/repo/.git".to_string()),
                legacy_project_keys: vec!["old-a".to_string()],
            },
            LegacySessionProjectInput {
                session_id: "linked-b".to_string(),
                workspace_path: Some("/other/b".to_string()),
                canonical_path: Some("/other/b".to_string()),
                git_common_dir: Some("/other/repo/.git".to_string()),
                legacy_project_keys: vec!["old-b".to_string()],
            },
            LegacySessionProjectInput {
                session_id: "basename-only".to_string(),
                workspace_path: Some("/missing/zenith".to_string()),
                canonical_path: None,
                git_common_dir: None,
                legacy_project_keys: vec!["zenith-hash".to_string()],
            },
        ];
        let report = plan_legacy_migration(&inputs, &[existing]);
        assert_eq!(report.assignments.len(), 1);
        assert_eq!(report.assignments[0].session_id, "exact");
        assert_eq!(report.suggestions.len(), 1);
        assert_eq!(
            report.suggestions[0].basis,
            LegacyProjectMatchBasis::GitCommonDir
        );
        assert!(report
            .unassigned
            .iter()
            .any(|entry| entry.session_id == "basename-only"));
    }
}
