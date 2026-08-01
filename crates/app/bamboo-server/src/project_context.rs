//! Adapter from the authoritative `bamboo-projects` store into the engine's
//! secret-free Project context seam.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_domain::ProjectId;
use bamboo_engine::project_context::{
    ProjectContextError, ProjectContextSource, ProjectDescriptor, ProjectMemoryReadRoots,
};
use bamboo_memory::memory_store::LegacyProjectMemoryReadRoot;
use bamboo_projects::{ProjectStore, ProjectStoreError};

#[derive(Debug, thiserror::Error)]
pub enum ProjectWorkspaceValidationError {
    #[error("{message}")]
    Invalid {
        code: &'static str,
        workspace: String,
        message: String,
    },
    #[error(
        "workspace '{workspace}' belongs to Project '{owner_project_id}', not session Project '{session_project_id}'"
    )]
    Conflict {
        workspace: String,
        owner_project_id: ProjectId,
        session_project_id: String,
    },
    #[error("failed to resolve workspace Project ownership: {0}")]
    Store(#[from] ProjectStoreError),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SessionWorkspaceValidationError {
    #[error("{message}")]
    Invalid { workspace: String, message: String },
    #[error(
        "workspace '{workspace}' belongs to Project '{owner_project_id}', not session Project '{session_project_id}'"
    )]
    Conflict {
        workspace: String,
        owner_project_id: ProjectId,
        session_project_id: String,
    },
    #[error("workspace '{workspace}' is not bound to assigned Project '{session_project_id}'")]
    Unbound {
        workspace: String,
        session_project_id: ProjectId,
    },
    #[error("assigned Project '{project_id}' is archived")]
    ProjectArchived { project_id: ProjectId },
    #[error("assigned Project '{project_id}' is unavailable")]
    ProjectUnavailable { project_id: ProjectId },
    #[error("failed to resolve workspace Project ownership: {0}")]
    Store(#[from] ProjectStoreError),
}

pub(crate) fn session_workspace_error_response(
    error: SessionWorkspaceValidationError,
) -> actix_web::HttpResponse {
    match error {
        SessionWorkspaceValidationError::Invalid { workspace, message } => {
            actix_web::HttpResponse::BadRequest().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "workspace_invalid",
                    "message": message
                },
                "workspace": workspace,
            }))
        }
        SessionWorkspaceValidationError::Conflict {
            workspace,
            owner_project_id,
            session_project_id,
        } => actix_web::HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_workspace_conflict",
                "message": "Workspace belongs to another Project"
            },
            "workspace": workspace,
            "owner_project_id": owner_project_id,
            "session_project_id": session_project_id,
        })),
        SessionWorkspaceValidationError::Unbound {
            workspace,
            session_project_id,
        } => actix_web::HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_workspace_unbound",
                "message": "Workspace must be bound to the session Project before switching"
            },
            "workspace": workspace,
            "session_project_id": session_project_id,
        })),
        SessionWorkspaceValidationError::ProjectArchived { project_id } => {
            actix_web::HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_archived",
                    "message": "Archived Projects cannot switch session workspaces"
                },
                "project_id": project_id,
            }))
        }
        SessionWorkspaceValidationError::ProjectUnavailable { project_id } => {
            actix_web::HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_unavailable",
                    "message": "Assigned Project is unavailable"
                },
                "project_id": project_id,
            }))
        }
        SessionWorkspaceValidationError::Store(error) => {
            tracing::error!(%error, "failed to validate session workspace Project ownership");
            crate::error::json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to validate workspace Project ownership",
            )
        }
    }
}

pub(crate) fn project_context_error_response(
    error: ProjectContextError,
) -> actix_web::HttpResponse {
    match error {
        ProjectContextError::WorkspaceConflict {
            workspace,
            owner_project_id,
            session_project_id,
        } => actix_web::HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_workspace_conflict",
                "message": "Workspace belongs to another Project"
            },
            "workspace": workspace,
            "owner_project_id": owner_project_id,
            "session_project_id": session_project_id,
        })),
        ProjectContextError::UnassignedWorkspaceConflict {
            workspace,
            owner_project_id,
        } => actix_web::HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_workspace_conflict",
                "message": "Workspace belongs to another Project"
            },
            "workspace": workspace,
            "owner_project_id": owner_project_id,
            "session_project_id": "unassigned",
        })),
        ProjectContextError::WorkspaceInvalid { workspace, message } => {
            actix_web::HttpResponse::BadRequest().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "workspace_invalid",
                    "message": message
                },
                "workspace": workspace,
            }))
        }
        ProjectContextError::InvalidProjectIdentity { raw, message } => {
            actix_web::HttpResponse::BadRequest().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "invalid_project_identity",
                    "message": format!(
                        "Session carries an invalid Project identity '{raw}': {message}"
                    )
                }
            }))
        }
        ProjectContextError::ProjectUnavailable { project_id } => {
            actix_web::HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_unavailable",
                    "message": "Assigned Project is unavailable"
                },
                "project_id": project_id,
            }))
        }
        ProjectContextError::ProjectPathMissing { project_id } => {
            actix_web::HttpResponse::Conflict().json(serde_json::json!({
                "error": {
                    "type": "api_error",
                    "code": "project_path_missing",
                    "message": "Assigned Project has no configured project_path"
                },
                "project_id": project_id,
            }))
        }
        ProjectContextError::ProjectPathUnavailable {
            project_id,
            project_path,
            message,
        } => actix_web::HttpResponse::Conflict().json(serde_json::json!({
            "error": {
                "type": "api_error",
                "code": "project_path_unavailable",
                "message": message
            },
            "project_id": project_id,
            "project_path": project_path,
        })),
        error @ (ProjectContextError::Source(_) | ProjectContextError::IdentityMismatch { .. }) => {
            tracing::error!(%error, "failed to resolve Project context");
            crate::error::json_error(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to resolve Project context",
            )
        }
    }
}

/// Resolve confinement before checking ownership, without mutating the global
/// workspace registry or session metadata. HTTP creation paths must call this
/// before any workspace/session side effect.
pub fn validate_workspace_assignment(
    store: &ProjectStore,
    session_project_id: Option<&ProjectId>,
    requested_workspace: Option<&str>,
) -> Result<Option<std::path::PathBuf>, ProjectWorkspaceValidationError> {
    validate_workspace_assignment_with(
        store,
        session_project_id,
        requested_workspace,
        bamboo_agent_core::workspace_state::preview_workspace_path,
        false,
    )
}

pub fn validate_workspace_assignment_with_resolver(
    store: &ProjectStore,
    session_project_id: Option<&ProjectId>,
    requested_workspace: Option<&str>,
    workspace_resolver: &bamboo_agent_core::workspace_state::WorkspaceResolver,
) -> Result<Option<std::path::PathBuf>, ProjectWorkspaceValidationError> {
    validate_workspace_assignment_with(
        store,
        session_project_id,
        requested_workspace,
        |workspace| workspace_resolver.preview_workspace_path(workspace),
        false,
    )
}

/// Validate an explicit session Workspace switch.
///
/// This is stricter than the generic creation/runtime resolver: an assigned
/// session may switch only to a path already registered to its active Project.
/// The API never binds a path as a side effect. Unassigned sessions may switch
/// to an unregistered path, but still cannot borrow another Project's binding.
pub(crate) fn validate_explicit_session_workspace_with_resolver(
    store: &ProjectStore,
    session_project_id: Option<&ProjectId>,
    requested_workspace: &str,
    workspace_resolver: &bamboo_agent_core::workspace_state::WorkspaceResolver,
) -> Result<std::path::PathBuf, SessionWorkspaceValidationError> {
    let requested_workspace = requested_workspace.trim();
    if requested_workspace.is_empty() {
        return Err(SessionWorkspaceValidationError::Invalid {
            workspace: String::new(),
            message: "Workspace path must be a non-empty existing directory".to_string(),
        });
    }

    if let Some(project_id) = session_project_id {
        match store.get(project_id) {
            Ok(project) if project.status == bamboo_domain::ProjectStatus::Active => {}
            Ok(_) => {
                return Err(SessionWorkspaceValidationError::ProjectArchived {
                    project_id: project_id.clone(),
                });
            }
            Err(ProjectStoreError::NotFound(_)) => {
                return Err(SessionWorkspaceValidationError::ProjectUnavailable {
                    project_id: project_id.clone(),
                });
            }
            Err(error) => return Err(SessionWorkspaceValidationError::Store(error)),
        }
    }

    let final_workspace = validate_workspace_assignment_with_resolver(
        store,
        session_project_id,
        Some(requested_workspace),
        workspace_resolver,
    )
    .map_err(|error| match error {
        ProjectWorkspaceValidationError::Invalid {
            workspace, message, ..
        } => SessionWorkspaceValidationError::Invalid { workspace, message },
        ProjectWorkspaceValidationError::Conflict {
            workspace,
            owner_project_id,
            session_project_id,
        } => SessionWorkspaceValidationError::Conflict {
            workspace,
            owner_project_id,
            session_project_id,
        },
        ProjectWorkspaceValidationError::Store(error) => {
            SessionWorkspaceValidationError::Store(error)
        }
    })?
    .ok_or_else(|| SessionWorkspaceValidationError::Invalid {
        workspace: requested_workspace.to_string(),
        message: "Workspace path must be a non-empty existing directory".to_string(),
    })?;

    let display = bamboo_config::paths::path_to_display_string(&final_workspace);
    let owner = match store.find_workspace_owner_for_path(&display) {
        Ok(owner) => owner,
        Err(ProjectStoreError::Validation(message))
        | Err(ProjectStoreError::InvalidPathComponent(message)) => {
            return Err(SessionWorkspaceValidationError::Invalid {
                workspace: display,
                message,
            });
        }
        Err(error) => return Err(SessionWorkspaceValidationError::Store(error)),
    };
    match (session_project_id, owner) {
        (Some(session_project_id), Some(owner)) if owner.id == *session_project_id => {
            Ok(final_workspace)
        }
        (Some(session_project_id), Some(owner)) => Err(SessionWorkspaceValidationError::Conflict {
            workspace: display,
            owner_project_id: owner.id,
            session_project_id: session_project_id.to_string(),
        }),
        (Some(session_project_id), None) => Err(SessionWorkspaceValidationError::Unbound {
            workspace: display,
            session_project_id: session_project_id.clone(),
        }),
        (None, Some(owner)) => Err(SessionWorkspaceValidationError::Conflict {
            workspace: display,
            owner_project_id: owner.id,
            session_project_id: "unassigned".to_string(),
        }),
        (None, None) => Ok(final_workspace),
    }
}

/// Validate a proposed authoritative Project path with the same
/// canonicalization, confinement, and ownership rules used at session
/// resolution time. `project_id` is `None` during create and the existing
/// stable ID during a CAS update.
pub fn validate_project_path_candidate_with_resolver(
    store: &ProjectStore,
    project_id: Option<&ProjectId>,
    project_path: &str,
    workspace_resolver: &bamboo_agent_core::workspace_state::WorkspaceResolver,
) -> Result<std::path::PathBuf, ProjectWorkspaceValidationError> {
    validate_workspace_assignment_with(
        store,
        project_id,
        Some(project_path),
        |workspace| workspace_resolver.preview_workspace_path(workspace),
        true,
    )?
    .ok_or_else(|| ProjectWorkspaceValidationError::Invalid {
        code: "project_path_missing",
        workspace: project_path.to_string(),
        message: "Project path must be a non-empty existing directory".to_string(),
    })
}

fn validate_workspace_assignment_with(
    store: &ProjectStore,
    session_project_id: Option<&ProjectId>,
    requested_workspace: Option<&str>,
    resolve_workspace: impl FnOnce(std::path::PathBuf) -> std::path::PathBuf,
    explicit_is_project_path: bool,
) -> Result<Option<std::path::PathBuf>, ProjectWorkspaceValidationError> {
    let explicit_workspace = requested_workspace
        .map(str::trim)
        .filter(|workspace| !workspace.is_empty());
    if explicit_is_project_path && explicit_workspace.is_none() {
        return Err(ProjectWorkspaceValidationError::Invalid {
            code: "project_path_missing",
            workspace: requested_workspace.unwrap_or_default().to_string(),
            message: "Project path must be a non-empty existing directory".to_string(),
        });
    }
    let (requested_workspace, is_project_default, is_persisted_project_default) =
        if let Some(workspace) = explicit_workspace {
            (workspace.to_string(), explicit_is_project_path, false)
        } else if let Some(project_id) = session_project_id {
            let project = store.get(project_id)?;
            let Some(project_path) = project.project_path else {
                return Err(ProjectWorkspaceValidationError::Invalid {
                    code: "project_path_missing",
                    workspace: String::new(),
                    message: format!("Project '{project_id}' has no configured project_path"),
                });
            };
            (project_path, true, true)
        } else {
            return Ok(None);
        };
    let requested_path = std::path::Path::new(&requested_workspace);
    if !requested_path.exists() {
        return Err(ProjectWorkspaceValidationError::Invalid {
            code: if is_project_default {
                "project_path_unavailable"
            } else {
                "workspace_not_found"
            },
            workspace: requested_workspace,
            message: if is_project_default {
                "Project path does not exist".to_string()
            } else {
                "Workspace path does not exist".to_string()
            },
        });
    }
    if !requested_path.is_dir() {
        return Err(ProjectWorkspaceValidationError::Invalid {
            code: if is_project_default {
                "project_path_unavailable"
            } else {
                "workspace_not_directory"
            },
            workspace: requested_workspace,
            message: if is_project_default {
                "Project path is not a directory".to_string()
            } else {
                "Workspace path is not a directory".to_string()
            },
        });
    }
    if is_persisted_project_default {
        let metadata = std::fs::symlink_metadata(requested_path).map_err(|error| {
            ProjectWorkspaceValidationError::Invalid {
                code: "project_path_unavailable",
                workspace: requested_workspace.clone(),
                message: format!("Project path metadata is unavailable: {error}"),
            }
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ProjectWorkspaceValidationError::Invalid {
                code: "project_path_unavailable",
                workspace: requested_workspace,
                message: "Configured Project path is no longer a plain directory".to_string(),
            });
        }
    }
    let canonical =
        requested_path
            .canonicalize()
            .map_err(|_| ProjectWorkspaceValidationError::Invalid {
                code: if is_project_default {
                    "project_path_unavailable"
                } else {
                    "workspace_invalid"
                },
                workspace: requested_workspace.clone(),
                message: if is_project_default {
                    "Project path could not be canonicalized".to_string()
                } else {
                    "Workspace path could not be canonicalized".to_string()
                },
            })?;
    if is_persisted_project_default && canonical != requested_path {
        return Err(ProjectWorkspaceValidationError::Invalid {
            code: "project_path_unavailable",
            workspace: requested_workspace,
            message:
                "Configured Project path no longer resolves to its registered canonical directory"
                    .to_string(),
        });
    }
    let final_workspace = resolve_workspace(canonical.clone());
    let final_workspace = final_workspace.canonicalize().unwrap_or(final_workspace);
    let display = bamboo_config::paths::path_to_display_string(&final_workspace);
    if is_project_default && final_workspace != canonical {
        return Err(ProjectWorkspaceValidationError::Invalid {
            code: "project_path_unavailable",
            workspace: requested_workspace,
            message: format!(
                "Workspace confinement redirected the Project path to '{}'",
                final_workspace.display()
            ),
        });
    }
    let owner = match store.find_workspace_owner_for_path(&display) {
        Ok(owner) => owner,
        Err(ProjectStoreError::Validation(message))
        | Err(ProjectStoreError::InvalidPathComponent(message)) => {
            return Err(ProjectWorkspaceValidationError::Invalid {
                code: if is_project_default {
                    "project_path_unavailable"
                } else {
                    "workspace_invalid"
                },
                workspace: display,
                message,
            });
        }
        Err(error) => return Err(ProjectWorkspaceValidationError::Store(error)),
    };
    let Some(owner) = owner else {
        return Ok(Some(final_workspace));
    };
    if session_project_id == Some(&owner.id) {
        return Ok(Some(final_workspace));
    }
    Err(ProjectWorkspaceValidationError::Conflict {
        workspace: display,
        owner_project_id: owner.id,
        session_project_id: session_project_id
            .map(ToString::to_string)
            .unwrap_or_else(|| "unassigned".to_string()),
    })
}

pub struct ProjectStoreContextSource {
    store: Arc<ProjectStore>,
}

impl ProjectStoreContextSource {
    pub fn new(store: Arc<ProjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ProjectContextSource for ProjectStoreContextSource {
    async fn find_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<ProjectDescriptor>, ProjectContextError> {
        let manifest = match self.store.get(project_id) {
            Ok(project) => project,
            Err(ProjectStoreError::NotFound(_)) => return Ok(None),
            Err(error) => return Err(ProjectContextError::Source(error.to_string())),
        };
        let resources = self
            .store
            .resource_summary(project_id)
            .map_err(|error| ProjectContextError::Source(error.to_string()))?;
        let memory_read_roots = self
            .store
            .project_memory_read_roots(project_id)
            .map_err(|error| ProjectContextError::Source(error.to_string()))?;
        Ok(Some(ProjectDescriptor {
            id: manifest.id.clone(),
            name: manifest.name,
            project_path: manifest.project_path.map(std::path::PathBuf::from),
            home: self.store.paths().project_home(project_id),
            workspace_bindings: manifest.workspace_bindings,
            resources,
            memory_read_roots: ProjectMemoryReadRoots {
                primary: memory_read_roots.primary,
                legacy_aliases: memory_read_roots
                    .legacy_aliases
                    .into_iter()
                    .map(|legacy| LegacyProjectMemoryReadRoot {
                        project_key: legacy.legacy_project_key,
                        root: legacy.root,
                    })
                    .collect(),
            },
        }))
    }

    async fn list_projects(&self) -> Result<Vec<ProjectDescriptor>, ProjectContextError> {
        let project_ids = self
            .store
            .list()
            .map_err(|error| ProjectContextError::Source(error.to_string()))?
            .into_iter()
            .map(|project| project.id)
            .collect::<Vec<_>>();
        let mut projects = Vec::with_capacity(project_ids.len());
        for project_id in project_ids {
            if let Some(project) = self.find_project(&project_id).await? {
                projects.push(project);
            }
        }
        Ok(projects)
    }

    async fn find_workspace_owner(
        &self,
        workspace: &std::path::Path,
    ) -> Result<Option<ProjectId>, ProjectContextError> {
        let display = bamboo_config::paths::path_to_display_string(workspace);
        self.store
            .find_workspace_owner_for_path(&display)
            .map(|owner| owner.map(|project| project.id))
            .map_err(|error| ProjectContextError::Source(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confinement_final_path_is_the_authority_for_ownership_and_persistence() {
        let data = tempfile::tempdir().expect("data");
        let raw = tempfile::tempdir().expect("raw workspace");
        let confined = tempfile::tempdir().expect("confined workspace");
        std::fs::write(raw.path().join("raw-only.txt"), "RAW MUST NOT BE USED")
            .expect("raw fixture");
        std::fs::write(confined.path().join("final-only.txt"), "FINAL").expect("final fixture");
        let store = ProjectStore::open(data.path()).expect("Project store");
        let owner = store
            .create_with_bindings(
                "Owner",
                None,
                vec![bamboo_domain::WorkspaceBinding {
                    path: confined.path().to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .expect("owner Project");
        let other = store.create("Other", None).expect("other Project");

        let resolved = validate_workspace_assignment_with(
            &store,
            Some(&owner.id),
            Some(raw.path().to_string_lossy().as_ref()),
            |_| confined.path().to_path_buf(),
            false,
        )
        .expect("same owner");
        assert_eq!(
            resolved.as_deref(),
            Some(confined.path().canonicalize().unwrap().as_path())
        );
        assert!(resolved
            .as_ref()
            .is_some_and(|path| path.join("final-only.txt").exists()));
        assert!(!resolved
            .as_ref()
            .is_some_and(|path| path.join("raw-only.txt").exists()));

        let conflict = validate_workspace_assignment_with(
            &store,
            Some(&other.id),
            Some(raw.path().to_string_lossy().as_ref()),
            |_| confined.path().to_path_buf(),
            false,
        )
        .expect_err("final workspace owner must win over the raw request");
        assert!(matches!(
            conflict,
            ProjectWorkspaceValidationError::Conflict {
                owner_project_id,
                ..
            } if owner_project_id == owner.id
        ));
    }

    #[test]
    fn assigned_omission_uses_project_path_and_reports_unconfigured_legacy_project() {
        let data = tempfile::tempdir().expect("data");
        let project_path = tempfile::tempdir().expect("Project path");
        let foreign_default = tempfile::tempdir().expect("foreign default");
        let store = ProjectStore::open(data.path()).expect("Project store");
        let project = store
            .create_with_project_path(
                "Configured",
                None,
                project_path.path().to_string_lossy(),
                Vec::new(),
            )
            .expect("configured Project");

        let resolved = validate_workspace_assignment_with(
            &store,
            Some(&project.id),
            None,
            |_| foreign_default.path().to_path_buf(),
            false,
        )
        .expect_err("confinement must not silently relocate project_path");
        assert!(matches!(
            resolved,
            ProjectWorkspaceValidationError::Invalid {
                code: "project_path_unavailable",
                ..
            }
        ));

        let resolved = validate_workspace_assignment_with(
            &store,
            Some(&project.id),
            None,
            |workspace| workspace,
            false,
        )
        .expect("Project path fallback");
        assert_eq!(
            resolved.as_deref(),
            Some(project_path.path().canonicalize().unwrap().as_path())
        );

        let legacy = store.create("Legacy", None).expect("legacy Project");
        let error = validate_workspace_assignment_with(
            &store,
            Some(&legacy.id),
            None,
            |workspace| workspace,
            false,
        )
        .expect_err("unconfigured Project must fail closed");
        assert!(matches!(
            error,
            ProjectWorkspaceValidationError::Invalid {
                code: "project_path_missing",
                ..
            }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn assigned_omission_rejects_project_path_replaced_by_symlink() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().expect("root");
        let data = root.path().join("data");
        let project_path = root.path().join("project");
        let replacement = root.path().join("replacement");
        std::fs::create_dir_all(&project_path).expect("Project path");
        std::fs::create_dir_all(&replacement).expect("replacement");
        let store = ProjectStore::open(&data).expect("Project store");
        let project = store
            .create_with_project_path(
                "Configured",
                None,
                project_path.to_string_lossy(),
                Vec::new(),
            )
            .expect("configured Project");

        std::fs::remove_dir(&project_path).expect("remove original Project path");
        symlink(&replacement, &project_path).expect("replace Project path with symlink");

        let error = validate_workspace_assignment_with(
            &store,
            Some(&project.id),
            None,
            |workspace| workspace,
            false,
        )
        .expect_err("persisted Project path must not follow a replacement symlink");
        assert!(matches!(
            error,
            ProjectWorkspaceValidationError::Invalid {
                code: "project_path_unavailable",
                ..
            }
        ));
    }
}
