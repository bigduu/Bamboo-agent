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
    )
}

fn validate_workspace_assignment_with(
    store: &ProjectStore,
    session_project_id: Option<&ProjectId>,
    requested_workspace: Option<&str>,
    resolve_workspace: impl FnOnce(std::path::PathBuf) -> std::path::PathBuf,
) -> Result<Option<std::path::PathBuf>, ProjectWorkspaceValidationError> {
    let Some(requested_workspace) = requested_workspace
        .map(str::trim)
        .filter(|workspace| !workspace.is_empty())
    else {
        return Ok(None);
    };
    let requested_path = std::path::Path::new(requested_workspace);
    if !requested_path.exists() {
        return Err(ProjectWorkspaceValidationError::Invalid {
            code: "workspace_not_found",
            workspace: requested_workspace.to_string(),
            message: "Workspace path does not exist".to_string(),
        });
    }
    if !requested_path.is_dir() {
        return Err(ProjectWorkspaceValidationError::Invalid {
            code: "workspace_not_directory",
            workspace: requested_workspace.to_string(),
            message: "Workspace path is not a directory".to_string(),
        });
    }
    let canonical =
        requested_path
            .canonicalize()
            .map_err(|_| ProjectWorkspaceValidationError::Invalid {
                code: "workspace_invalid",
                workspace: requested_workspace.to_string(),
                message: "Workspace path could not be canonicalized".to_string(),
            })?;
    let final_workspace = resolve_workspace(canonical);
    let final_workspace = final_workspace.canonicalize().unwrap_or(final_workspace);
    let display = bamboo_config::paths::path_to_display_string(&final_workspace);
    let owner = match store.find_workspace_owner_for_path(&display) {
        Ok(owner) => owner,
        Err(ProjectStoreError::Validation(message))
        | Err(ProjectStoreError::InvalidPathComponent(message)) => {
            return Err(ProjectWorkspaceValidationError::Invalid {
                code: "workspace_invalid",
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
}
