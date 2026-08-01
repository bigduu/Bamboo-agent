//! Stable Project identity and shared-resource resolution.
//!
//! A Project is not a workspace.  The Project id carried by a session is the
//! authority for memory and shared resources; the workspace is only the
//! mutable filesystem execution context.  This module is the single engine
//! seam for resolving those two identities together.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::Session;
use bamboo_domain::{ProjectId, ProjectResourceKind, ProjectResourceSummary, WorkspaceBinding};
use serde::{Deserialize, Serialize};

pub const PROJECT_ID_METADATA_KEY: &str = "project_id";
pub const PROJECT_RESOURCES_RENDERED_KEY: &str = "project_resources_rendered";
pub const WORKSPACE_SOURCE_METADATA_KEY: &str = "workspace_source";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceBindingStatus {
    Registered,
    Unregistered,
}

impl WorkspaceBindingStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Unregistered => "unregistered",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectDescriptor {
    pub id: ProjectId,
    pub name: String,
    /// Canonical user source folder and default execution workspace.
    pub project_path: Option<PathBuf>,
    /// Bamboo-owned Project data/resources directory.
    pub home: PathBuf,
    pub workspace_bindings: Vec<WorkspaceBinding>,
    pub resources: ProjectResourceSummary,
    pub memory_read_roots: ProjectMemoryReadRoots,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMemoryReadRoots {
    pub primary: PathBuf,
    pub legacy_aliases: Vec<bamboo_memory::memory_store::LegacyProjectMemoryReadRoot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedProjectContext {
    pub project: ProjectDescriptor,
    pub workspace: Option<PathBuf>,
    pub workspace_source: WorkspaceSource,
    pub binding_status: WorkspaceBindingStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceSource {
    Explicit,
    Session,
    ProjectDefault,
}

impl WorkspaceSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Session => "session",
            Self::ProjectDefault => "project_default",
        }
    }
}

impl ResolvedProjectContext {
    pub fn resource_scope(&self) -> ProjectResourceScope {
        ProjectResourceScope {
            project_id: self.project.id.clone(),
            project_home: self.project.home.clone(),
            workspace: self.workspace.clone(),
            binding_status: self.binding_status,
            resource_revision: self.project.resources.resource_revision,
        }
    }

    pub fn render_resource_inventory(&self) -> String {
        let mut entries = self.project.resources.resources.clone();
        entries.sort_by_key(|entry| entry.kind);
        let rendered = entries
            .into_iter()
            .map(|entry| {
                format!(
                    "- {:?}: status={}, items={}",
                    entry.kind,
                    if entry.present { "available" } else { "empty" },
                    entry.item_count
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            "Project ID: {}\nResource revision: {}\n{}",
            self.project.id,
            self.project.resources.resource_revision,
            if rendered.is_empty() {
                "No Project-shared resources are currently advertised.".to_string()
            } else {
                rendered
            }
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProjectContextError {
    #[error("project context source failed: {0}")]
    Source(String),
    #[error("project context source returned '{actual}' for requested project '{requested}'")]
    IdentityMismatch { requested: String, actual: String },
    #[error(
        "workspace '{workspace}' belongs to Project '{owner_project_id}', not session Project '{session_project_id}'"
    )]
    WorkspaceConflict {
        workspace: String,
        owner_project_id: ProjectId,
        session_project_id: ProjectId,
    },
    #[error("workspace '{workspace}' belongs to Project '{owner_project_id}', but the session is Unassigned")]
    UnassignedWorkspaceConflict {
        workspace: String,
        owner_project_id: ProjectId,
    },
    #[error("session carries an invalid Project identity '{raw}': {message}")]
    InvalidProjectIdentity { raw: String, message: String },
    #[error("assigned Project '{project_id}' is unavailable")]
    ProjectUnavailable { project_id: ProjectId },
    #[error("assigned Project '{project_id}' has no configured project_path")]
    ProjectPathMissing { project_id: ProjectId },
    #[error("assigned Project '{project_id}' path '{project_path}' is unavailable: {message}")]
    ProjectPathUnavailable {
        project_id: ProjectId,
        project_path: String,
        message: String,
    },
    #[error("workspace '{workspace}' is invalid: {message}")]
    WorkspaceInvalid { workspace: String, message: String },
}

/// Adapter implemented by the authoritative Project registry.
///
/// The engine deliberately depends on this redacted descriptor rather than on
/// registry persistence details. Secret settings and credential values must
/// never be added to this interface.
#[async_trait]
pub trait ProjectContextSource: Send + Sync {
    async fn find_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<ProjectDescriptor>, ProjectContextError>;

    async fn list_projects(&self) -> Result<Vec<ProjectDescriptor>, ProjectContextError> {
        Ok(Vec::new())
    }

    /// Resolve the global Project owner for an exact workspace. Sources that
    /// cannot provide a registry-wide answer remain backward compatible.
    async fn find_workspace_owner(
        &self,
        _workspace: &Path,
    ) -> Result<Option<ProjectId>, ProjectContextError> {
        Ok(None)
    }
}

#[derive(Clone)]
pub struct ProjectContextResolver {
    source: Arc<dyn ProjectContextSource>,
    workspace_resolver: bamboo_agent_core::workspace_state::WorkspaceResolver,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionProjectIdentity {
    Unassigned,
    Assigned(ProjectId),
    Invalid { raw: String, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectMemoryScope {
    Assigned {
        project_id: ProjectId,
        legacy_aliases: Vec<bamboo_memory::memory_store::LegacyProjectMemoryReadRoot>,
    },
    LegacyReadOnly(String),
}

impl ProjectMemoryScope {
    pub fn key(&self) -> &str {
        match self {
            Self::Assigned { project_id, .. } => project_id.as_str(),
            Self::LegacyReadOnly(project_key) => project_key,
        }
    }

    pub fn scoped_store(
        &self,
        store: &bamboo_memory::memory_store::MemoryStore,
    ) -> bamboo_memory::memory_store::MemoryStore {
        match self {
            Self::Assigned {
                project_id,
                legacy_aliases,
            } => store.for_project_with_legacy_read_roots(project_id, legacy_aliases.clone()),
            Self::LegacyReadOnly(_) => store.clone(),
        }
    }
}

impl ProjectContextResolver {
    pub fn new(source: Arc<dyn ProjectContextSource>) -> Self {
        Self {
            source,
            workspace_resolver:
                bamboo_agent_core::workspace_state::WorkspaceResolver::from_process_globals(),
        }
    }

    /// Build a Project resolver against one coherent workspace-provider pair.
    ///
    /// The server uses this form so preview/ownership validation cannot observe
    /// a different `AppState`'s process-global first-wins provider. Other
    /// embeddings retain [`Self::new`]'s dynamic global behavior.
    pub fn new_with_workspace_resolver(
        source: Arc<dyn ProjectContextSource>,
        workspace_resolver: bamboo_agent_core::workspace_state::WorkspaceResolver,
    ) -> Self {
        Self {
            source,
            workspace_resolver,
        }
    }

    /// Return the stable, opaque Project id persisted on the session.
    ///
    /// The domain accessor dual-writes this compatibility key. Keeping the
    /// read centralized here prevents memory, Dream, prompt, and resource
    /// callers from independently falling back to mutable workspace identity.
    pub fn project_id_from_session(session: &Session) -> Option<ProjectId> {
        match Self::session_project_identity(session) {
            SessionProjectIdentity::Assigned(project_id) => Some(project_id),
            SessionProjectIdentity::Invalid { raw, message } => {
                tracing::warn!(
                    session_id = %session.id,
                    "ignoring invalid persisted Project id '{raw}': {message}"
                );
                None
            }
            SessionProjectIdentity::Unassigned => None,
        }
    }

    /// Parse persisted Project membership into an authoritative three-state
    /// value. Whitespace is normalized exactly like the rebuildable storage
    /// index. Callers with security/resource consequences must distinguish
    /// `Invalid` from truly `Unassigned`.
    pub fn session_project_identity(session: &Session) -> SessionProjectIdentity {
        let Some(raw) = session.project_id_meta() else {
            return SessionProjectIdentity::Unassigned;
        };
        let normalized = raw.trim();
        match ProjectId::parse(normalized) {
            Ok(project_id) => SessionProjectIdentity::Assigned(project_id),
            Err(error) => SessionProjectIdentity::Invalid {
                raw,
                message: error.to_string(),
            },
        }
    }

    /// Resolve the Project id used for memory reads.
    ///
    /// Assigned sessions always use their stable Project id. The path-derived
    /// fallback is read-compatibility for unassigned legacy sessions only; new
    /// sessions and writes must use [`Self::project_id_from_session`].
    pub fn memory_read_scope_for_session(session: &Session) -> Option<String> {
        Self::memory_read_identity_for_session(session).map(|scope| scope.key().to_string())
    }

    pub fn memory_read_identity_for_session(session: &Session) -> Option<ProjectMemoryScope> {
        match Self::session_project_identity(session) {
            SessionProjectIdentity::Assigned(project_id) => Some(ProjectMemoryScope::Assigned {
                project_id,
                legacy_aliases: Vec::new(),
            }),
            SessionProjectIdentity::Unassigned => session
                .workspace_path_meta()
                .map(PathBuf::from)
                .or_else(|| {
                    bamboo_tools::tools::workspace_state::get_workspace(session.id.as_str())
                })
                .map(|path| {
                    ProjectMemoryScope::LegacyReadOnly(
                        bamboo_memory::memory_store::project_key_from_path(&path),
                    )
                }),
            SessionProjectIdentity::Invalid { .. } => None,
        }
    }

    pub async fn resolve_memory_read_scope(
        &self,
        session: &Session,
        workspace: Option<&Path>,
    ) -> Result<Option<ProjectMemoryScope>, ProjectContextError> {
        match Self::session_project_identity(session) {
            SessionProjectIdentity::Unassigned => {
                return Ok(Self::memory_read_identity_for_session(session));
            }
            SessionProjectIdentity::Invalid { raw, message } => {
                return Err(ProjectContextError::InvalidProjectIdentity { raw, message });
            }
            SessionProjectIdentity::Assigned(_) => {}
        }
        Ok(self
            .resolve(session, workspace)
            .await?
            .map(|context| ProjectMemoryScope::Assigned {
                project_id: context.project.id,
                legacy_aliases: context.project.memory_read_roots.legacy_aliases,
            }))
    }

    pub async fn list_memory_read_scopes(
        &self,
    ) -> Result<Vec<ProjectMemoryScope>, ProjectContextError> {
        Ok(self
            .source
            .list_projects()
            .await?
            .into_iter()
            .map(|project| ProjectMemoryScope::Assigned {
                project_id: project.id,
                legacy_aliases: project.memory_read_roots.legacy_aliases,
            })
            .collect())
    }

    /// Resolve the only valid Project write scope.
    ///
    /// Unassigned legacy sessions intentionally return `None`: their
    /// path-derived scopes are read/migration aliases and must never receive
    /// new Project memory or Dream writes.
    pub fn memory_write_scope_for_session(session: &Session) -> Option<String> {
        match Self::session_project_identity(session) {
            SessionProjectIdentity::Assigned(project_id) => Some(project_id.into_string()),
            SessionProjectIdentity::Unassigned | SessionProjectIdentity::Invalid { .. } => None,
        }
    }

    pub async fn resolve(
        &self,
        session: &Session,
        workspace: Option<&Path>,
    ) -> Result<Option<ResolvedProjectContext>, ProjectContextError> {
        let project_id = match Self::session_project_identity(session) {
            SessionProjectIdentity::Assigned(project_id) => project_id,
            SessionProjectIdentity::Invalid { raw, message } => {
                return Err(ProjectContextError::InvalidProjectIdentity { raw, message });
            }
            SessionProjectIdentity::Unassigned => {
                let workspace =
                    self.resolve_workspace_candidate_for_instance(session, workspace)?;
                if let Some(candidate) = workspace.as_deref() {
                    if let Some(owner_project_id) =
                        self.source.find_workspace_owner(candidate).await?
                    {
                        return Err(ProjectContextError::UnassignedWorkspaceConflict {
                            workspace: candidate.to_string_lossy().into_owned(),
                            owner_project_id,
                        });
                    }
                }
                return Ok(None);
            }
        };
        let project = self
            .source
            .find_project(&project_id)
            .await?
            .ok_or_else(|| ProjectContextError::ProjectUnavailable {
                project_id: project_id.clone(),
            })?;
        if project.id != project_id {
            return Err(ProjectContextError::IdentityMismatch {
                requested: project_id.to_string(),
                actual: project.id.to_string(),
            });
        }

        let (workspace, workspace_source) = if let Some(workspace) = workspace {
            (
                resolve_existing_workspace_with_resolver(workspace, &self.workspace_resolver)?,
                WorkspaceSource::Explicit,
            )
        } else if session
            .metadata
            .get(WORKSPACE_SOURCE_METADATA_KEY)
            .map(String::as_str)
            == Some(WorkspaceSource::ProjectDefault.as_str())
        {
            (
                resolve_project_default_workspace(&project, &self.workspace_resolver)?,
                WorkspaceSource::ProjectDefault,
            )
        } else if let Some(workspace) = session.workspace_path_meta() {
            let persisted_source = session
                .metadata
                .get(WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str);
            let workspace = resolve_existing_workspace_with_resolver(
                Path::new(&workspace),
                &self.workspace_resolver,
            )?;
            let source = match persisted_source {
                Some("explicit") => WorkspaceSource::Explicit,
                Some("session") => WorkspaceSource::Session,
                _ => WorkspaceSource::Session,
            };
            (workspace, source)
        } else {
            (
                resolve_project_default_workspace(&project, &self.workspace_resolver)?,
                WorkspaceSource::ProjectDefault,
            )
        };

        self.resolve_assigned_project(project, workspace, workspace_source)
            .await
            .map(Some)
    }

    /// Resolve the exact workspace the runtime will use without publishing it.
    ///
    /// This is shared by HTTP preflight, SDK/execute prompt refresh, and the
    /// Workspace tool so configured/session-default fallbacks cannot bypass
    /// Project ownership checks.
    pub fn resolve_workspace_candidate(
        session: &Session,
        workspace: Option<&Path>,
    ) -> Result<Option<PathBuf>, ProjectContextError> {
        Self::resolve_workspace_candidate_with(
            &bamboo_agent_core::workspace_state::WorkspaceResolver::from_process_globals(),
            session,
            workspace,
        )
    }

    fn resolve_workspace_candidate_for_instance(
        &self,
        session: &Session,
        workspace: Option<&Path>,
    ) -> Result<Option<PathBuf>, ProjectContextError> {
        Self::resolve_workspace_candidate_with(&self.workspace_resolver, session, workspace)
    }

    fn resolve_workspace_candidate_with(
        workspace_resolver: &bamboo_agent_core::workspace_state::WorkspaceResolver,
        session: &Session,
        workspace: Option<&Path>,
    ) -> Result<Option<PathBuf>, ProjectContextError> {
        let preferred = workspace
            .map(Path::to_path_buf)
            .or_else(|| session.workspace_path_meta().map(PathBuf::from));
        if preferred.is_none()
            && matches!(
                Self::session_project_identity(session),
                SessionProjectIdentity::Assigned(_)
            )
        {
            // This synchronous helper cannot consult the Project source.
            // Assigned callers must use `resolve`, which selects project_path;
            // they must never fall through to process-global/session-temp
            // compatibility defaults here.
            return Ok(None);
        }
        workspace_resolver
            .resolve_session_workspace_candidate(&session.id, preferred)
            .map(|candidate| resolve_final_workspace_with(&candidate, workspace_resolver))
            .transpose()
    }

    async fn resolve_assigned_project(
        &self,
        project: ProjectDescriptor,
        workspace: PathBuf,
        workspace_source: WorkspaceSource,
    ) -> Result<ResolvedProjectContext, ProjectContextError> {
        let binding_status = match self.source.find_workspace_owner(&workspace).await? {
            Some(owner) if owner == project.id => WorkspaceBindingStatus::Registered,
            Some(owner) => {
                return Err(ProjectContextError::WorkspaceConflict {
                    workspace: workspace.to_string_lossy().into_owned(),
                    owner_project_id: owner,
                    session_project_id: project.id.clone(),
                });
            }
            None if project
                .project_path
                .iter()
                .map(PathBuf::as_path)
                .chain(
                    project
                        .workspace_bindings
                        .iter()
                        .map(|binding| Path::new(&binding.path)),
                )
                .any(|root| path_is_within_binding(root, &workspace)) =>
            {
                WorkspaceBindingStatus::Registered
            }
            None => WorkspaceBindingStatus::Unregistered,
        };

        Ok(ResolvedProjectContext {
            project,
            workspace: Some(workspace),
            workspace_source,
            binding_status,
        })
    }

    pub async fn workspace_owner(
        &self,
        workspace: &Path,
    ) -> Result<Option<ProjectId>, ProjectContextError> {
        self.source.find_workspace_owner(workspace).await
    }

    /// Resolve and persist the stable Project and mutable Workspace prompt
    /// markers immediately.
    ///
    /// Session-create and chat APIs call this before their first response so a
    /// freshly-created assigned session is already self-describing when read
    /// back, rather than waiting for the first execution round. The round
    /// prelude calls the same helper to keep the markers current.
    pub async fn refresh_session_prompt(
        &self,
        session: &mut Session,
    ) -> Result<Option<ResolvedProjectContext>, ProjectContextError> {
        self.refresh_session_prompt_inner(session, true).await
    }

    /// Resolve Project/Workspace prompt markers on an in-memory snapshot
    /// without changing runtime workspace state.
    ///
    /// Read APIs use this for sessions that have never entered the runner
    /// (for example a disabled schedule or a child created with
    /// `auto_run=false`). The caller is expected to discard the temporary
    /// session after building the response.
    pub async fn refresh_session_prompt_read_only(
        &self,
        session: &mut Session,
    ) -> Result<Option<ResolvedProjectContext>, ProjectContextError> {
        self.refresh_session_prompt_inner(session, false).await
    }

    async fn refresh_session_prompt_inner(
        &self,
        session: &mut Session,
        sync_runtime_workspace: bool,
    ) -> Result<Option<ResolvedProjectContext>, ProjectContextError> {
        let resolved = self.resolve(session, None).await?;
        let workspace = if let Some(context) = resolved.as_ref() {
            context.workspace.clone()
        } else {
            self.resolve_workspace_candidate_for_instance(session, None)?
        };
        if let Some(workspace) = workspace.as_deref() {
            let final_workspace = if sync_runtime_workspace {
                self.workspace_resolver.publish_resolved_workspace(
                    &session.id,
                    workspace.into(),
                    "project_context_refresh",
                )
            } else {
                workspace.to_path_buf()
            };
            session.set_workspace_path_meta(bamboo_config::paths::path_to_display_string(
                &final_workspace,
            ));
        }
        if let Some(context) = resolved.as_ref() {
            session.metadata.insert(
                WORKSPACE_SOURCE_METADATA_KEY.to_string(),
                context.workspace_source.as_str().to_string(),
            );
        } else {
            session.metadata.remove(WORKSPACE_SOURCE_METADATA_KEY);
        }
        let current = session
            .messages
            .iter()
            .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
            .map(|message| message.content.clone())
            .or_else(|| session.metadata.get("base_system_prompt").cloned())
            .unwrap_or_default();
        let mut updated =
            crate::runtime::context::upsert_project_prompt_context(&current, resolved.as_ref());

        if let Some(context) = resolved.as_ref() {
            session.metadata.insert(
                PROJECT_RESOURCES_RENDERED_KEY.to_string(),
                context.render_resource_inventory(),
            );
        } else {
            session.metadata.remove(PROJECT_RESOURCES_RENDERED_KEY);
        }
        let workspace_display = workspace
            .as_deref()
            .map(bamboo_config::paths::path_to_display_string);
        updated = crate::runtime::context::upsert_workspace_prompt_context_with_source(
            &updated,
            workspace_display.as_deref(),
            resolved
                .as_ref()
                .map(|context| context.binding_status)
                .unwrap_or(WorkspaceBindingStatus::Unregistered),
            resolved.as_ref().map(|context| context.workspace_source),
        );

        if let Some(system_message) = session
            .messages
            .iter_mut()
            .find(|message| matches!(message.role, bamboo_agent_core::Role::System))
        {
            system_message.content = updated;
        } else if !updated.trim().is_empty() {
            session
                .messages
                .insert(0, bamboo_agent_core::Message::system(updated));
        }
        crate::runner::refresh_prompt_snapshot(session);

        Ok(resolved)
    }
}

fn resolve_final_workspace_with(
    workspace: &Path,
    workspace_resolver: &bamboo_agent_core::workspace_state::WorkspaceResolver,
) -> Result<PathBuf, ProjectContextError> {
    if workspace.exists() && !workspace.is_dir() {
        return Err(ProjectContextError::WorkspaceInvalid {
            workspace: workspace.to_string_lossy().into_owned(),
            message: "path is not a directory".to_string(),
        });
    }
    let canonical = std::fs::canonicalize(workspace).unwrap_or_else(|_| workspace.to_path_buf());
    let final_workspace = workspace_resolver.preview_workspace_path(canonical);
    if final_workspace.exists() && !final_workspace.is_dir() {
        return Err(ProjectContextError::WorkspaceInvalid {
            workspace: final_workspace.to_string_lossy().into_owned(),
            message: "resolved path is not a directory".to_string(),
        });
    }
    Ok(std::fs::canonicalize(&final_workspace).unwrap_or(final_workspace))
}

/// Resolve an already-selected workspace with the same existence, directory,
/// and instance-confinement checks used by assigned runtime resolution.
///
/// Server-side Project/Workspace tools use this seam so read diagnostics
/// cannot report a stale or differently confined workspace as runnable.
pub fn resolve_existing_workspace_with_resolver(
    workspace: &Path,
    workspace_resolver: &bamboo_agent_core::workspace_state::WorkspaceResolver,
) -> Result<PathBuf, ProjectContextError> {
    if !workspace.exists() {
        return Err(ProjectContextError::WorkspaceInvalid {
            workspace: workspace.to_string_lossy().into_owned(),
            message: "path does not exist".to_string(),
        });
    }
    resolve_final_workspace_with(workspace, workspace_resolver)
}

fn resolve_project_default_workspace(
    project: &ProjectDescriptor,
    workspace_resolver: &bamboo_agent_core::workspace_state::WorkspaceResolver,
) -> Result<PathBuf, ProjectContextError> {
    let project_path =
        project
            .project_path
            .as_deref()
            .ok_or_else(|| ProjectContextError::ProjectPathMissing {
                project_id: project.id.clone(),
            })?;
    let display = project_path.to_string_lossy().into_owned();
    if !project_path.exists() {
        return Err(ProjectContextError::ProjectPathUnavailable {
            project_id: project.id.clone(),
            project_path: display,
            message: "path does not exist".to_string(),
        });
    }
    let metadata = std::fs::symlink_metadata(project_path).map_err(|error| {
        ProjectContextError::ProjectPathUnavailable {
            project_id: project.id.clone(),
            project_path: display.clone(),
            message: format!("path metadata is unavailable: {error}"),
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ProjectContextError::ProjectPathUnavailable {
            project_id: project.id.clone(),
            project_path: display,
            message: "configured path is no longer a plain directory".to_string(),
        });
    }
    let canonical = std::fs::canonicalize(project_path).map_err(|error| {
        ProjectContextError::ProjectPathUnavailable {
            project_id: project.id.clone(),
            project_path: display.clone(),
            message: format!("path could not be canonicalized: {error}"),
        }
    })?;
    if canonical != project_path {
        return Err(ProjectContextError::ProjectPathUnavailable {
            project_id: project.id.clone(),
            project_path: display,
            message: "configured path no longer resolves to its registered canonical directory"
                .to_string(),
        });
    }
    let final_workspace = workspace_resolver.preview_workspace_path(canonical.clone());
    let final_workspace = std::fs::canonicalize(&final_workspace).map_err(|error| {
        ProjectContextError::ProjectPathUnavailable {
            project_id: project.id.clone(),
            project_path: display.clone(),
            message: format!("confinement target is unavailable: {error}"),
        }
    })?;
    if final_workspace != canonical {
        return Err(ProjectContextError::ProjectPathUnavailable {
            project_id: project.id.clone(),
            project_path: display,
            message: format!(
                "workspace confinement redirected the Project path to '{}'",
                final_workspace.display()
            ),
        });
    }
    Ok(canonical)
}

fn path_is_within_binding(binding: &Path, candidate: &Path) -> bool {
    let Ok(metadata) = std::fs::symlink_metadata(binding) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return false;
    }
    let Ok(canonical_binding) = std::fs::canonicalize(binding) else {
        return false;
    };
    if canonical_binding != binding {
        return false;
    }
    candidate == binding || candidate.starts_with(binding)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectResourceLayer {
    Project,
    Workspace,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectResourceCandidate {
    pub kind: ProjectResourceKind,
    pub layer: ProjectResourceLayer,
    pub path: PathBuf,
    pub exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectResourceDiagnostic {
    pub project_id: ProjectId,
    pub resource_revision: u64,
    pub workspace_binding_status: WorkspaceBindingStatus,
    pub candidates: Vec<ProjectResourceCandidate>,
}

/// Stable Project-home resources plus the current workspace overlay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectResourceScope {
    pub project_id: ProjectId,
    pub project_home: PathBuf,
    pub workspace: Option<PathBuf>,
    pub binding_status: WorkspaceBindingStatus,
    pub resource_revision: u64,
}

impl ProjectResourceScope {
    pub fn project_memory_root(&self) -> PathBuf {
        self.project_home.join("memory").join("v1")
    }

    pub fn project_skills_dir(&self) -> PathBuf {
        self.project_home.join("skills")
    }

    pub fn project_mode_skills_dir(&self, mode: &str) -> PathBuf {
        self.project_home.join(format!("skills-{mode}"))
    }

    pub fn workspace_skills_dir(&self) -> Option<PathBuf> {
        self.workspace
            .as_ref()
            .map(|workspace| workspace.join(".bamboo").join("skills"))
    }

    pub fn workspace_mode_skills_dir(&self, mode: &str) -> Option<PathBuf> {
        self.workspace
            .as_ref()
            .map(|workspace| workspace.join(".bamboo").join(format!("skills-{mode}")))
    }

    pub fn project_commands_dir(&self) -> PathBuf {
        self.project_home.join("commands")
    }

    pub fn workspace_commands_dir(&self) -> Option<PathBuf> {
        let workspace = self.workspace.as_deref()?;
        let boundary = nearest_git_boundary(workspace).unwrap_or_else(|| workspace.to_path_buf());
        Some(boundary.join(".bamboo").join("commands"))
    }

    /// Ordinary resource precedence is Project first, then the more-specific
    /// workspace overlay. Security policies must use their dedicated managed /
    /// deny / trust merge logic instead of this shadowing order.
    pub fn candidates(&self, kind: ProjectResourceKind) -> Vec<ProjectResourceCandidate> {
        let mut paths = match kind {
            ProjectResourceKind::Settings => vec![(
                ProjectResourceLayer::Project,
                self.project_home.join("settings.json"),
            )],
            ProjectResourceKind::Memory => {
                vec![(ProjectResourceLayer::Project, self.project_memory_root())]
            }
            ProjectResourceKind::Skills => {
                let mut values = vec![(ProjectResourceLayer::Project, self.project_skills_dir())];
                if let Some(path) = self.workspace_skills_dir() {
                    values.push((ProjectResourceLayer::Workspace, path));
                }
                values
            }
            ProjectResourceKind::Commands => {
                let mut values = vec![(ProjectResourceLayer::Project, self.project_commands_dir())];
                if let Some(path) = self.workspace_commands_dir() {
                    values.push((ProjectResourceLayer::Workspace, path));
                }
                values
            }
            ProjectResourceKind::Artifacts => vec![(
                ProjectResourceLayer::Project,
                self.project_home.join("artifacts"),
            )],
            ProjectResourceKind::State => vec![(
                ProjectResourceLayer::Project,
                self.project_home.join("state"),
            )],
        };

        paths
            .drain(..)
            .map(|(layer, path)| ProjectResourceCandidate {
                kind,
                layer,
                exists: path.exists(),
                path,
            })
            .collect()
    }

    pub fn diagnostic(&self) -> ProjectResourceDiagnostic {
        let mut candidates = Vec::new();
        for kind in [
            ProjectResourceKind::Settings,
            ProjectResourceKind::Memory,
            ProjectResourceKind::Skills,
            ProjectResourceKind::Commands,
            ProjectResourceKind::Artifacts,
            ProjectResourceKind::State,
        ] {
            candidates.extend(self.candidates(kind));
        }
        ProjectResourceDiagnostic {
            project_id: self.project_id.clone(),
            resource_revision: self.resource_revision,
            workspace_binding_status: self.binding_status,
            candidates,
        }
    }
}

fn nearest_git_boundary(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .find(|candidate| {
            let git = candidate.join(".git");
            git.is_dir() || git.is_file()
        })
        .map(Path::to_path_buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StaticSource(ProjectDescriptor);

    #[async_trait]
    impl ProjectContextSource for StaticSource {
        async fn find_project(
            &self,
            project_id: &ProjectId,
        ) -> Result<Option<ProjectDescriptor>, ProjectContextError> {
            Ok((&self.0.id == project_id).then(|| self.0.clone()))
        }
    }

    struct OwnedWorkspaceSource {
        descriptor: ProjectDescriptor,
        owner: ProjectId,
    }

    #[async_trait]
    impl ProjectContextSource for OwnedWorkspaceSource {
        async fn find_project(
            &self,
            project_id: &ProjectId,
        ) -> Result<Option<ProjectDescriptor>, ProjectContextError> {
            Ok((&self.descriptor.id == project_id).then(|| self.descriptor.clone()))
        }

        async fn find_workspace_owner(
            &self,
            _workspace: &Path,
        ) -> Result<Option<ProjectId>, ProjectContextError> {
            Ok(Some(self.owner.clone()))
        }
    }

    #[tokio::test]
    async fn workspace_changes_do_not_change_project_identity_or_home() {
        let directory = tempfile::tempdir().expect("tempdir");
        let main = directory.path().join("main");
        let worktree = directory.path().join("worktree");
        std::fs::create_dir_all(&main).expect("main");
        std::fs::create_dir_all(&worktree).expect("worktree");
        let main = main.canonicalize().expect("canonical main");
        let worktree = worktree.canonicalize().expect("canonical worktree");
        let project_id = ProjectId::parse("01JPROJECT00000000000000000").expect("project id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Zenith".to_string(),
            project_path: Some(main.clone()),
            home: directory
                .path()
                .join("projects/01JPROJECT00000000000000000"),
            workspace_bindings: vec![
                WorkspaceBinding {
                    path: main.to_string_lossy().to_string(),
                    label: Some("main".to_string()),
                    git_common_dir: None,
                },
                WorkspaceBinding {
                    path: worktree.to_string_lossy().to_string(),
                    label: Some("worktree".to_string()),
                    git_common_dir: None,
                },
            ],
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 7,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory
                    .path()
                    .join("projects/01JPROJECT00000000000000000/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(Arc::new(StaticSource(descriptor)));
        let mut session = Session::new("session-1", "test");
        session.set_project_id_meta(project_id.to_string());

        let first = resolver
            .resolve(&session, Some(&main))
            .await
            .expect("resolve")
            .expect("assigned");
        let second = resolver
            .resolve(&session, Some(&worktree))
            .await
            .expect("resolve")
            .expect("assigned");
        assert_eq!(first.project.id, second.project.id);
        assert_eq!(first.project.home, second.project.home);
        assert_eq!(second.binding_status, WorkspaceBindingStatus::Registered);
    }

    #[tokio::test]
    async fn assigned_session_uses_project_path_before_global_or_temp_fallback() {
        let directory = tempfile::tempdir().expect("tempdir");
        let project_path = directory.path().join("project");
        let foreign_default = directory.path().join("foreign-default");
        let workspace_root = directory.path().join("session-workspaces");
        std::fs::create_dir_all(&project_path).expect("project path");
        std::fs::create_dir_all(&foreign_default).expect("foreign default");
        std::fs::create_dir_all(&workspace_root).expect("workspace root");
        let project_id = ProjectId::parse("project-default").expect("Project id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Project Default".to_string(),
            project_path: Some(project_path.canonicalize().unwrap()),
            home: directory.path().join("projects/project-default"),
            workspace_bindings: Vec::new(),
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory.path().join("projects/project-default/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new_with_workspace_resolver(
            Arc::new(StaticSource(descriptor)),
            bamboo_agent_core::workspace_state::WorkspaceResolver::new(
                {
                    let foreign_default = foreign_default.clone();
                    move || Some(foreign_default.clone())
                },
                {
                    let workspace_root = workspace_root.clone();
                    move || bamboo_agent_core::workspace_state::WorkspaceRootConfig {
                        root: workspace_root.clone(),
                        confine: false,
                    }
                },
            ),
        );
        let mut session = Session::new("project-default-session", "test");
        session.set_project_id_meta(project_id.to_string());
        session
            .messages
            .insert(0, bamboo_agent_core::Message::system("base"));

        let resolved = resolver
            .refresh_session_prompt_read_only(&mut session)
            .await
            .expect("Project default must resolve")
            .expect("assigned Project");
        assert_eq!(
            resolved.workspace.as_deref(),
            Some(project_path.canonicalize().unwrap().as_path())
        );
        assert_eq!(resolved.workspace_source, WorkspaceSource::ProjectDefault);
        assert_eq!(
            session.workspace_path_meta().as_deref(),
            Some(
                bamboo_config::paths::path_to_display_string(&project_path.canonicalize().unwrap())
                    .as_str()
            )
        );
        let prompt = &session.messages[0].content;
        assert_eq!(prompt.matches("Project path:").count(), 1);
        assert_eq!(prompt.matches("Project home (Bamboo data):").count(), 1);
        assert_eq!(prompt.matches("Workspace path:").count(), 1);
        assert_eq!(
            prompt.matches("Workspace source: project_default").count(),
            1
        );
        assert!(!prompt.contains(foreign_default.to_string_lossy().as_ref()));
        assert!(!workspace_root.join(&session.id).exists());

        let moved_project_path = directory.path().join("project-moved");
        std::fs::create_dir_all(&moved_project_path).expect("moved Project path");
        let moved_descriptor = ProjectDescriptor {
            id: project_id,
            name: "Project Default".to_string(),
            project_path: Some(moved_project_path.canonicalize().unwrap()),
            home: directory.path().join("projects/project-default"),
            workspace_bindings: Vec::new(),
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: ProjectId::parse("project-default").unwrap(),
                resource_revision: 2,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory.path().join("projects/project-default/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let moved_resolver = ProjectContextResolver::new(Arc::new(StaticSource(moved_descriptor)));
        let moved = moved_resolver
            .resolve(&session, None)
            .await
            .expect("moved Project path")
            .expect("assigned Project");
        assert_eq!(
            moved.workspace.as_deref(),
            Some(moved_project_path.canonicalize().unwrap().as_path())
        );
        assert_eq!(moved.workspace_source, WorkspaceSource::ProjectDefault);
    }

    #[tokio::test]
    async fn legacy_persisted_workspace_equal_to_project_path_remains_session_owned_after_path_cas()
    {
        let directory = tempfile::tempdir().expect("tempdir");
        let project_path = directory.path().join("project");
        let moved_project_path = directory.path().join("project-moved");
        std::fs::create_dir_all(&project_path).expect("Project path");
        std::fs::create_dir_all(&moved_project_path).expect("moved Project path");
        let project_path = project_path.canonicalize().expect("canonical Project path");
        let moved_project_path = moved_project_path
            .canonicalize()
            .expect("canonical moved Project path");
        let project_id = ProjectId::parse("legacy-persisted-workspace").expect("Project id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Legacy persisted workspace".to_string(),
            project_path: Some(project_path.clone()),
            home: directory.path().join("projects/legacy-persisted-workspace"),
            workspace_bindings: Vec::new(),
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory
                    .path()
                    .join("projects/legacy-persisted-workspace/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(Arc::new(StaticSource(descriptor)));
        let mut session = Session::new("legacy-persisted-workspace", "test");
        session.set_project_id_meta(project_id.to_string());
        session
            .set_workspace_path_meta(bamboo_config::paths::path_to_display_string(&project_path));

        let initial = resolver
            .refresh_session_prompt_read_only(&mut session)
            .await
            .expect("legacy persisted workspace must resolve")
            .expect("assigned Project");
        assert_eq!(initial.workspace.as_deref(), Some(project_path.as_path()));
        assert_eq!(initial.workspace_source, WorkspaceSource::Session);
        assert_eq!(
            session
                .metadata
                .get(WORKSPACE_SOURCE_METADATA_KEY)
                .map(String::as_str),
            Some("session")
        );

        let moved_descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Legacy persisted workspace".to_string(),
            project_path: Some(moved_project_path),
            home: directory.path().join("projects/legacy-persisted-workspace"),
            workspace_bindings: Vec::new(),
            resources: bamboo_domain::ProjectResourceSummary {
                project_id,
                resource_revision: 2,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory
                    .path()
                    .join("projects/legacy-persisted-workspace/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let moved_resolver = ProjectContextResolver::new(Arc::new(StaticSource(moved_descriptor)));
        let after_cas = moved_resolver
            .resolve(&session, None)
            .await
            .expect("persisted workspace must remain authoritative")
            .expect("assigned Project");
        assert_eq!(after_cas.workspace.as_deref(), Some(project_path.as_path()));
        assert_eq!(after_cas.workspace_source, WorkspaceSource::Session);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn project_default_rejects_configured_path_replaced_by_symlink() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("tempdir");
        let project_path = directory.path().join("project");
        let replacement = directory.path().join("replacement");
        std::fs::create_dir_all(&project_path).expect("Project path");
        std::fs::create_dir_all(&replacement).expect("replacement");
        let configured_path = project_path.canonicalize().expect("canonical Project path");
        let project_id = ProjectId::parse("symlink-replaced-project").expect("Project id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Symlink replacement".to_string(),
            project_path: Some(configured_path),
            home: directory.path().join("projects/symlink-replaced-project"),
            workspace_bindings: Vec::new(),
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory
                    .path()
                    .join("projects/symlink-replaced-project/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(Arc::new(StaticSource(descriptor)));
        let mut session = Session::new("symlink-replaced-project", "test");
        session.set_project_id_meta(project_id.to_string());

        std::fs::remove_dir(&project_path).expect("remove configured Project path");
        symlink(&replacement, &project_path).expect("replace Project path with symlink");

        assert!(matches!(
            resolver.resolve(&session, None).await,
            Err(ProjectContextError::ProjectPathUnavailable { .. })
        ));
        assert!(session.workspace_path_meta().is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn explicit_workspace_is_not_registered_through_a_replaced_project_path_symlink() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("tempdir");
        let project_path = directory.path().join("project");
        let replacement = directory.path().join("replacement");
        let explicit_workspace = replacement.join("explicit");
        std::fs::create_dir_all(&project_path).expect("Project path");
        std::fs::create_dir_all(&explicit_workspace).expect("explicit workspace");
        let configured_path = project_path.canonicalize().expect("canonical Project path");
        let project_id = ProjectId::parse("symlink-explicit-project").expect("Project id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Symlink explicit workspace".to_string(),
            project_path: Some(configured_path),
            home: directory.path().join("projects/symlink-explicit-project"),
            workspace_bindings: Vec::new(),
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory
                    .path()
                    .join("projects/symlink-explicit-project/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(Arc::new(StaticSource(descriptor)));
        let mut session = Session::new("symlink-explicit-project", "test");
        session.set_project_id_meta(project_id.to_string());

        std::fs::remove_dir(&project_path).expect("remove configured Project path");
        symlink(&replacement, &project_path).expect("replace Project path with symlink");

        let resolved = resolver
            .resolve(&session, Some(&explicit_workspace))
            .await
            .expect("explicit workspace remains independently resolvable")
            .expect("assigned Project");
        assert_eq!(
            resolved.workspace.as_deref(),
            Some(explicit_workspace.canonicalize().unwrap().as_path())
        );
        assert_eq!(
            resolved.binding_status,
            WorkspaceBindingStatus::Unregistered
        );
        assert_eq!(resolved.workspace_source, WorkspaceSource::Explicit);
    }

    #[tokio::test]
    async fn missing_or_unavailable_project_path_fails_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let project_id = ProjectId::parse("unconfigured-project").expect("Project id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Unconfigured".to_string(),
            project_path: None,
            home: directory.path().join("projects/unconfigured-project"),
            workspace_bindings: Vec::new(),
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory
                    .path()
                    .join("projects/unconfigured-project/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(Arc::new(StaticSource(descriptor)));
        let mut session = Session::new("unconfigured", "test");
        session.set_project_id_meta(project_id.to_string());
        assert!(matches!(
            resolver.resolve(&session, None).await,
            Err(ProjectContextError::ProjectPathMissing { .. })
        ));

        let missing = directory.path().join("moved-away");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Unavailable".to_string(),
            project_path: Some(missing.clone()),
            home: directory.path().join("projects/unconfigured-project"),
            workspace_bindings: Vec::new(),
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory
                    .path()
                    .join("projects/unconfigured-project/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(Arc::new(StaticSource(descriptor)));
        assert!(matches!(
            resolver.resolve(&session, None).await,
            Err(ProjectContextError::ProjectPathUnavailable {
                project_path,
                ..
            }) if project_path == missing.to_string_lossy()
        ));
        assert!(session.workspace_path_meta().is_none());
    }

    #[tokio::test]
    async fn prompt_refresh_removes_stale_project_and_updates_unassigned_workspace() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let workspace = workspace.canonicalize().expect("canonical workspace");
        let project_id = ProjectId::parse("project-prompt-refresh").expect("Project id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Prompt Project".to_string(),
            project_path: Some(workspace.clone()),
            home: directory.path().join("projects/project-prompt-refresh"),
            workspace_bindings: vec![WorkspaceBinding {
                path: workspace.to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory
                    .path()
                    .join("projects/project-prompt-refresh/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(Arc::new(StaticSource(descriptor)));
        let mut session = Session::new("prompt-project-switch", "test");
        session
            .messages
            .insert(0, bamboo_agent_core::Message::system("base"));
        session.set_project_id_meta(project_id.to_string());
        session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());

        resolver
            .refresh_session_prompt(&mut session)
            .await
            .expect("assigned refresh");
        let assigned = &session.messages[0].content;
        assert_eq!(assigned.matches("BAMBOO_PROJECT_CONTEXT_START").count(), 1);
        assert!(assigned.contains("Binding status: registered"));

        session.clear_project_id_meta();
        resolver
            .refresh_session_prompt(&mut session)
            .await
            .expect("unassigned refresh");
        let unassigned = &session.messages[0].content;
        assert_eq!(
            unassigned.matches("BAMBOO_PROJECT_CONTEXT_START").count(),
            0
        );
        assert_eq!(
            unassigned.matches("BAMBOO_WORKSPACE_CONTEXT_START").count(),
            1
        );
        assert!(unassigned.contains("Binding status: unregistered"));
        assert!(!session
            .metadata
            .contains_key(PROJECT_RESOURCES_RENDERED_KEY));
    }

    #[tokio::test]
    async fn workspace_owned_by_another_project_fails_closed() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let project_id = ProjectId::parse("project-a").expect("project id");
        let owner = ProjectId::parse("project-b").expect("owner id");
        let descriptor = ProjectDescriptor {
            id: project_id.clone(),
            name: "Project A".to_string(),
            project_path: Some(workspace.clone()),
            home: directory.path().join("projects/project-a"),
            workspace_bindings: vec![WorkspaceBinding {
                path: workspace.to_string_lossy().into_owned(),
                label: None,
                git_common_dir: None,
            }],
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: project_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory.path().join("projects/project-a/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(Arc::new(OwnedWorkspaceSource {
            descriptor,
            owner: owner.clone(),
        }));
        let mut session = Session::new("session-conflict", "test");
        session.set_project_id_meta(project_id.to_string());

        let error = resolver
            .resolve(&session, Some(&workspace))
            .await
            .expect_err("cross-Project workspace must fail closed");
        assert!(matches!(
            error,
            ProjectContextError::WorkspaceConflict {
                owner_project_id,
                session_project_id,
                ..
            } if owner_project_id == owner && session_project_id == project_id
        ));

        let safe_workspace = directory.path().join("safe");
        std::fs::create_dir_all(&safe_workspace).expect("safe workspace");
        let safe_canonical = safe_workspace.canonicalize().expect("canonical safe");
        bamboo_tools::tools::workspace_state::set_workspace(&session.id, safe_canonical.clone());
        session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
        let error = resolver
            .refresh_session_prompt(&mut session)
            .await
            .expect_err("refresh must validate before publishing workspace");
        assert!(matches!(
            error,
            ProjectContextError::WorkspaceConflict { .. }
        ));
        assert_eq!(
            bamboo_tools::tools::workspace_state::get_workspace(&session.id).as_deref(),
            Some(safe_canonical.as_path()),
            "a post-preflight ownership change must not publish the rejected workspace"
        );
    }

    #[tokio::test]
    async fn malformed_project_identity_never_falls_back_to_legacy_workspace_scope() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let descriptor_id = ProjectId::parse("descriptor").expect("Project id");
        let descriptor = ProjectDescriptor {
            id: descriptor_id.clone(),
            name: "Descriptor".to_string(),
            project_path: Some(workspace.clone()),
            home: directory.path().join("projects/descriptor"),
            workspace_bindings: Vec::new(),
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: descriptor_id.clone(),
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory.path().join("projects/descriptor/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(Arc::new(StaticSource(descriptor)));
        let mut session = Session::new("malformed", "test");
        session.set_project_id_meta("../malformed");
        session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());

        assert!(ProjectContextResolver::memory_read_identity_for_session(&session).is_none());
        assert!(matches!(
            resolver.resolve(&session, Some(&workspace)).await,
            Err(ProjectContextError::InvalidProjectIdentity { .. })
        ));
        assert!(matches!(
            resolver
                .resolve_memory_read_scope(&session, Some(&workspace))
                .await,
            Err(ProjectContextError::InvalidProjectIdentity { .. })
        ));
    }

    #[tokio::test]
    async fn unassigned_session_cannot_resolve_a_project_owned_workspace() {
        let directory = tempfile::tempdir().expect("tempdir");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let descriptor_id = ProjectId::parse("descriptor").expect("Project id");
        let owner = ProjectId::parse("owner").expect("owner Project id");
        let descriptor = ProjectDescriptor {
            id: descriptor_id.clone(),
            name: "Descriptor".to_string(),
            project_path: None,
            home: directory.path().join("projects/descriptor"),
            workspace_bindings: Vec::new(),
            resources: bamboo_domain::ProjectResourceSummary {
                project_id: descriptor_id,
                resource_revision: 1,
                resources: Vec::new(),
            },
            memory_read_roots: ProjectMemoryReadRoots {
                primary: directory.path().join("projects/descriptor/memory/v1"),
                legacy_aliases: Vec::new(),
            },
        };
        let resolver = ProjectContextResolver::new(Arc::new(OwnedWorkspaceSource {
            descriptor,
            owner: owner.clone(),
        }));
        let session = Session::new("unassigned", "test");

        assert!(matches!(
            resolver.resolve(&session, Some(&workspace)).await,
            Err(ProjectContextError::UnassignedWorkspaceConflict {
                owner_project_id,
                ..
            }) if owner_project_id == owner
        ));
    }

    #[test]
    fn project_identity_parser_trims_like_the_storage_index() {
        let mut session = Session::new("whitespace", "test");
        session.set_project_id_meta("  project-1  ");
        assert_eq!(
            ProjectContextResolver::session_project_identity(&session),
            SessionProjectIdentity::Assigned(ProjectId::parse("project-1").unwrap())
        );
    }

    #[test]
    fn resource_diagnostic_distinguishes_project_and_workspace_layers() {
        let directory = tempfile::tempdir().expect("tempdir");
        let project_home = directory.path().join("project-home");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(project_home.join("skills")).expect("project skills");
        std::fs::create_dir_all(workspace.join(".bamboo/skills")).expect("workspace skills");
        let scope = ProjectResourceScope {
            project_id: ProjectId::parse("project-1").expect("project id"),
            project_home,
            workspace: Some(workspace),
            binding_status: WorkspaceBindingStatus::Registered,
            resource_revision: 4,
        };

        let candidates = scope.candidates(ProjectResourceKind::Skills);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].layer, ProjectResourceLayer::Project);
        assert_eq!(candidates[1].layer, ProjectResourceLayer::Workspace);
        assert!(candidates.iter().all(|candidate| candidate.exists));
    }

    #[test]
    fn workspace_commands_resolve_from_nearest_git_boundary() {
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = directory.path().join("repo");
        let nested = repository.join("nested/path");
        std::fs::create_dir_all(repository.join(".git")).expect("git");
        std::fs::create_dir_all(&nested).expect("nested");
        let scope = ProjectResourceScope {
            project_id: ProjectId::parse("project-1").expect("project id"),
            project_home: directory.path().join("project-home"),
            workspace: Some(nested),
            binding_status: WorkspaceBindingStatus::Registered,
            resource_revision: 1,
        };
        assert_eq!(
            scope.workspace_commands_dir(),
            Some(repository.join(".bamboo/commands"))
        );
    }

    #[test]
    fn unassigned_legacy_scope_is_read_only() {
        let mut session = Session::new("legacy-session", "legacy");
        session.set_workspace_path_meta("/tmp/legacy-workspace");
        assert!(ProjectContextResolver::memory_read_scope_for_session(&session).is_some());
        assert!(ProjectContextResolver::memory_write_scope_for_session(&session).is_none());
    }
}
