//! Project-aware `Workspace` and `Project` tools.
//!
//! These are server overlays: they keep the framework Workspace semantics but
//! enrich every response with stable Project identity and enforce the global
//! workspace binding invariant after confinement has resolved the final path.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::{Session, Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult};
use bamboo_domain::{ProjectId, ProjectManifest, ProjectStatus, WorkspaceBinding};
use bamboo_engine::project_context::{ProjectContextResolver, SessionProjectIdentity};
use bamboo_engine::SessionRepository;
use bamboo_projects::{ProjectStore, ProjectStoreError};
use serde_json::{json, Value};

pub struct ProjectWorkspaceTool {
    sessions: SessionRepository,
    projects: Arc<ProjectStore>,
}

impl ProjectWorkspaceTool {
    pub fn new(sessions: SessionRepository, projects: Arc<ProjectStore>) -> Self {
        Self { sessions, projects }
    }

    async fn session_project(
        &self,
        session_id: &str,
    ) -> Result<(Option<Session>, Option<ProjectManifest>), ToolError> {
        let Some(session) = self.sessions.try_load(session_id).await.map_err(|error| {
            ToolError::Execution(format!("failed to load Workspace session: {error}"))
        })?
        else {
            return Ok((None, None));
        };
        let Some(raw_project_id) = session.project_id_meta() else {
            return Ok((Some(session), None));
        };
        let project_id = raw_project_id
            .trim()
            .parse::<ProjectId>()
            .map_err(|error| {
                ToolError::Execution(format!(
                    "session carries an invalid assigned Project identity: {error}"
                ))
            })?;
        match self.projects.get(&project_id) {
            Ok(project) => Ok((Some(session), Some(project))),
            Err(error) => Err(ToolError::Execution(format!(
                "assigned Project is unavailable; Workspace change refused: {error}"
            ))),
        }
    }

    fn response(
        &self,
        session_id: Option<&str>,
        workspace: &Path,
        project: Option<&ProjectManifest>,
        binding_status: &str,
        relocated_from: Option<&Path>,
    ) -> Value {
        json!({
            "session_id": session_id,
            "project_id": project.map(|value| value.id.to_string()),
            "project_name": project.map(|value| value.name.clone()),
            "project_home": project.map(|value| {
                bamboo_config::paths::path_to_display_string(
                    &self.projects.paths().project_home(&value.id)
                )
            }),
            "workspace": bamboo_config::paths::path_to_display_string(workspace),
            "binding_status": binding_status,
            "relocated_from": relocated_from.map(
                bamboo_config::paths::path_to_display_string
            ),
        })
    }
}

#[async_trait]
impl Tool for ProjectWorkspaceTool {
    fn name(&self) -> &str {
        "Workspace"
    }

    fn description(&self) -> &str {
        "Get or set the mutable session workspace while preserving Project identity. Returns structured Project/binding diagnostics."
    }

    fn classify(&self, args: &Value) -> ToolClass {
        if args
            .get("path")
            .and_then(Value::as_str)
            .is_some_and(|path| !path.trim().is_empty())
        {
            ToolClass::MUTATING_SERIAL
        } else {
            ToolClass::READONLY_PARALLEL
        }
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Workspace directory to set. Omit to inspect."
                }
            },
            "additionalProperties": false
        })
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let session_id = ctx.session_id();
        let (session, mut project) = match session_id {
            Some(id) => self.session_project(id).await?,
            None => (None, None),
        };
        let requested = args
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|path| !path.is_empty());

        let (workspace, relocated_from, binding_status) = if let Some(requested) = requested {
            let session_id = session_id.ok_or_else(|| {
                ToolError::Execution(
                    "Workspace(set) requires a session_id in tool context".to_string(),
                )
            })?;
            let requested_path = Path::new(requested);
            let requested_path = if requested_path.is_absolute() {
                requested_path.to_path_buf()
            } else {
                let preferred = session
                    .as_ref()
                    .and_then(Session::workspace_path_meta)
                    .map(std::path::PathBuf::from);
                let base = bamboo_agent_core::workspace_state::resolve_session_workspace_candidate(
                    session_id, preferred,
                )
                .unwrap_or_else(|| {
                    std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
                });
                base.join(requested_path)
            };
            if !requested_path.exists() {
                return Ok(completed(
                    false,
                    json!({
                        "code": "workspace_not_found",
                        "message": format!("Path does not exist: {}", requested_path.display())
                    }),
                ));
            }
            if !requested_path.is_dir() {
                return Ok(completed(
                    false,
                    json!({
                        "code": "workspace_not_directory",
                        "message": format!("Path is not a directory: {}", requested_path.display())
                    }),
                ));
            }
            let canonical = requested_path.canonicalize().map_err(|error| {
                ToolError::Execution(format!("failed to canonicalize workspace: {error}"))
            })?;
            // Resolve confinement before checking the authoritative global
            // registry. No session state changes until this check passes.
            let final_path =
                bamboo_agent_core::workspace_state::preview_workspace_path(canonical.clone());
            let final_display = bamboo_config::paths::path_to_display_string(&final_path);
            let owner = self
                .projects
                .find_workspace_owner_for_path(&final_display)
                .map_err(project_tool_error)?;
            let _binding_status = match owner {
                Some(owner) if project.as_ref().map(|value| &value.id) == Some(&owner.id) => {
                    "registered"
                }
                Some(owner) => {
                    return Ok(completed(
                        false,
                        json!({
                            "code": "project_workspace_conflict",
                            "message": "The confinement-resolved workspace is registered to another Project",
                            "workspace": final_display,
                            "owner_project_id": owner.id,
                            "session_project_id": project.as_ref().map(|value| value.id.to_string()),
                            "relocated_from": (final_path != canonical).then(|| {
                                bamboo_config::paths::path_to_display_string(&canonical)
                            }),
                        }),
                    ));
                }
                None => "unregistered",
            };
            // Re-load and validate the durable authority while holding the same
            // per-session lock used by chat and Project reassignment. A cached
            // snapshot must never overwrite a concurrent message append or
            // publish a workspace under a Project identity that has changed.
            let persistence_guard = self.sessions.persistence().acquire_lock(session_id).await;
            let mut authoritative = self
                .sessions
                .storage()
                .load_session(session_id)
                .await
                .map_err(|error| {
                    ToolError::Execution(format!(
                        "failed to reload Workspace session authority: {error}"
                    ))
                })?
                .ok_or_else(|| ToolError::Execution("session was not found".to_string()))?;
            let authoritative_project =
                match ProjectContextResolver::session_project_identity(&authoritative) {
                    SessionProjectIdentity::Assigned(project_id) => {
                        Some(self.projects.get(&project_id).map_err(project_tool_error)?)
                    }
                    SessionProjectIdentity::Unassigned => None,
                    SessionProjectIdentity::Invalid { raw, message } => {
                        return Err(ToolError::Execution(format!(
                        "session carries an invalid assigned Project identity '{raw}': {message}"
                    )));
                    }
                };
            let authoritative_owner = self
                .projects
                .find_workspace_owner_for_path(&final_display)
                .map_err(project_tool_error)?;
            let authoritative_binding_status = match authoritative_owner {
                Some(owner)
                    if authoritative_project.as_ref().map(|value| &value.id) == Some(&owner.id) =>
                {
                    "registered"
                }
                Some(owner) => {
                    return Ok(completed(
                        false,
                        json!({
                            "code": "project_workspace_conflict",
                            "message": "The confinement-resolved workspace is registered to another Project",
                            "workspace": final_display,
                            "owner_project_id": owner.id,
                            "session_project_id": authoritative_project
                                .as_ref()
                                .map(|value| value.id.to_string()),
                            "relocated_from": (final_path != canonical).then(|| {
                                bamboo_config::paths::path_to_display_string(&canonical)
                            }),
                        }),
                    ));
                }
                None => "unregistered",
            };
            authoritative.set_workspace_path_meta(final_display);
            self.sessions
                .storage()
                .save_session(&authoritative)
                .await
                .map_err(|error| {
                    ToolError::Execution(format!(
                        "failed to persist validated workspace before publication: {error}"
                    ))
                })?;
            self.sessions.cache().insert(
                session_id.to_string(),
                Arc::new(parking_lot::RwLock::new(authoritative.clone())),
            );
            drop(persistence_guard);
            project = authoritative_project;
            let stored = bamboo_agent_core::workspace_state::publish_resolved_workspace(
                session_id,
                final_path.clone(),
            );
            let relocated = (stored != canonical).then_some(canonical);
            (stored, relocated, authoritative_binding_status)
        } else {
            let preferred = session
                .as_ref()
                .and_then(Session::workspace_path_meta)
                .map(std::path::PathBuf::from);
            let workspace = session_id
                .and_then(|session_id| {
                    bamboo_agent_core::workspace_state::resolve_session_workspace_candidate(
                        session_id, preferred,
                    )
                })
                .unwrap_or_else(|| {
                    std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
                });
            let workspace_display = bamboo_config::paths::path_to_display_string(&workspace);
            let owner = self
                .projects
                .find_workspace_owner_for_path(&workspace_display)
                .map_err(project_tool_error)?;
            let binding_status = match owner {
                Some(owner) if project.as_ref().map(|project| &project.id) == Some(&owner.id) => {
                    "registered"
                }
                Some(owner) => {
                    return Ok(completed(
                        false,
                        json!({
                            "code": "project_workspace_conflict",
                            "message": "The current workspace is registered to another Project",
                            "workspace": workspace_display,
                            "owner_project_id": owner.id,
                            "session_project_id": project.as_ref().map(|value| value.id.to_string()),
                        }),
                    ));
                }
                None => "unregistered",
            };
            (workspace, None, binding_status)
        };
        Ok(completed(
            true,
            self.response(
                session_id,
                &workspace,
                project.as_ref(),
                binding_status,
                relocated_from.as_deref(),
            ),
        ))
    }
}

pub struct ProjectTool {
    sessions: SessionRepository,
    projects: Arc<ProjectStore>,
    account_sink: Option<Arc<bamboo_engine::events::AccountEventSink>>,
}

impl ProjectTool {
    pub fn new(sessions: SessionRepository, projects: Arc<ProjectStore>) -> Self {
        Self {
            sessions,
            projects,
            account_sink: None,
        }
    }

    pub fn with_account_sink(
        mut self,
        account_sink: Arc<bamboo_engine::events::AccountEventSink>,
    ) -> Self {
        self.account_sink = Some(account_sink);
        self
    }

    async fn current_project(
        &self,
        ctx: &ToolCtx,
    ) -> Result<(String, Session, ProjectManifest), ToolError> {
        let session_id = ctx.session_id().ok_or_else(|| {
            ToolError::Execution("Project requires a session_id in tool context".to_string())
        })?;
        let session = self
            .sessions
            .try_load(session_id)
            .await
            .map_err(|error| ToolError::Execution(format!("failed to load session: {error}")))?
            .ok_or_else(|| ToolError::Execution("session was not found".to_string()))?;
        let project_id = match ProjectContextResolver::session_project_identity(&session) {
            SessionProjectIdentity::Assigned(project_id) => project_id,
            SessionProjectIdentity::Unassigned => {
                return Err(ToolError::Execution(
                    "session is not assigned to a Project".to_string(),
                ));
            }
            SessionProjectIdentity::Invalid { raw, message } => {
                return Err(ToolError::Execution(format!(
                    "session carries an invalid assigned Project identity '{raw}': {message}"
                )));
            }
        };
        let project = self.projects.get(&project_id).map_err(project_tool_error)?;
        Ok((session_id.to_string(), session, project))
    }
}

#[async_trait]
impl Tool for ProjectTool {
    fn name(&self) -> &str {
        "Project"
    }

    fn description(&self) -> &str {
        "Inspect the current Project and shared resources, or explicitly bind/unbind workspaces using Project revision CAS."
    }

    fn classify(&self, args: &Value) -> ToolClass {
        match args.get("action").and_then(Value::as_str) {
            Some("inspect" | "list_resources") => ToolClass::READONLY_PARALLEL,
            _ => ToolClass::MUTATING_SERIAL,
        }
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["inspect", "list_resources", "bind_workspace", "unbind_workspace"]
                },
                "path": {"type": "string"},
                "label": {"type": "string"},
                "git_common_dir": {"type": "string"},
                "expected_revision": {"type": "integer", "minimum": 1}
            },
            "required": ["action"],
            "additionalProperties": false
        })
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        let (session_id, session, project) = self.current_project(&ctx).await?;
        let action = args
            .get("action")
            .and_then(Value::as_str)
            .ok_or_else(|| ToolError::InvalidArguments("action is required".to_string()))?;
        match action {
            "inspect" => {
                let summary = self
                    .projects
                    .resource_summary(&project.id)
                    .map_err(project_tool_error)?;
                let resource_revision = summary.resource_revision;
                let workspace = ProjectContextResolver::resolve_workspace_candidate(&session, None)
                    .map_err(|error| ToolError::Execution(error.to_string()))?;
                let (binding_status, owner_project_id) = match workspace.as_deref() {
                    Some(workspace) => {
                        let display = bamboo_config::paths::path_to_display_string(workspace);
                        match self
                            .projects
                            .find_workspace_owner_for_path(&display)
                            .map_err(project_tool_error)?
                        {
                            Some(owner) if owner.id == project.id => ("registered", Some(owner.id)),
                            Some(owner) => ("owned_by_other_project", Some(owner.id)),
                            None => ("unregistered", None),
                        }
                    }
                    None => ("unavailable", None),
                };
                Ok(completed(
                    true,
                    json!({
                        "session_id": session_id,
                        "project": project,
                        "project_home": bamboo_config::paths::path_to_display_string(
                            &self.projects.paths().project_home(&project.id)
                        ),
                        "resource_summary": summary,
                        "resource_revision": resource_revision,
                        "workspace": workspace.as_deref().map(
                            bamboo_config::paths::path_to_display_string
                        ),
                        "binding_status": binding_status,
                        "workspace_owner_project_id": owner_project_id,
                    }),
                ))
            }
            "list_resources" => {
                let summary = self
                    .projects
                    .resource_summary(&project.id)
                    .map_err(project_tool_error)?;
                Ok(completed(
                    true,
                    json!({
                        "session_id": session_id,
                        "project_id": project.id,
                        "resources": summary,
                    }),
                ))
            }
            "bind_workspace" | "unbind_workspace" => {
                if project.status != ProjectStatus::Active {
                    return Ok(completed(
                        false,
                        json!({
                            "code": "project_archived",
                            "message": "Archived Projects cannot change workspace bindings"
                        }),
                    ));
                }
                let path = args
                    .get("path")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|path| !path.is_empty())
                    .ok_or_else(|| ToolError::InvalidArguments("path is required".to_string()))?;
                let expected = args
                    .get("expected_revision")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| {
                        ToolError::InvalidArguments(
                            "expected_revision is required for binding mutations".to_string(),
                        )
                    })?;
                let updated = if action == "bind_workspace" {
                    self.projects.bind_workspace(
                        &project.id,
                        expected,
                        WorkspaceBinding {
                            path: path.to_string(),
                            label: args
                                .get("label")
                                .and_then(Value::as_str)
                                .map(ToString::to_string),
                            git_common_dir: args
                                .get("git_common_dir")
                                .and_then(Value::as_str)
                                .map(ToString::to_string),
                        },
                    )
                } else {
                    self.projects.unbind_workspace(&project.id, expected, path)
                };
                match updated {
                    Ok(updated) => {
                        if let Some(sink) = self.account_sink.as_ref() {
                            sink.record(
                                None,
                                &bamboo_agent_core::AgentEvent::ProjectUpdated {
                                    project_id: updated.id.to_string(),
                                    revision: updated.revision,
                                },
                            );
                        }
                        Ok(completed(
                            true,
                            json!({
                                "session_id": session_id,
                                "project_id": updated.id,
                                "revision": updated.revision,
                                "workspace_bindings": updated.workspace_bindings,
                            }),
                        ))
                    }
                    Err(ProjectStoreError::Conflict { expected, actual }) => Ok(completed(
                        false,
                        json!({
                            "code": "project_revision_conflict",
                            "message": "Project revision precondition failed",
                            "expected_revision": expected,
                            "actual_revision": actual,
                        }),
                    )),
                    Err(error) => Ok(completed(
                        false,
                        json!({
                            "code": "project_binding_failed",
                            "message": error.to_string(),
                        }),
                    )),
                }
            }
            _ => Err(ToolError::InvalidArguments(format!(
                "unsupported Project action: {action}"
            ))),
        }
    }
}

fn project_tool_error(error: ProjectStoreError) -> ToolError {
    ToolError::Execution(format!("Project registry operation failed: {error}"))
}

fn completed(success: bool, value: Value) -> ToolOutcome {
    ToolOutcome::Completed(ToolResult {
        success,
        result: value.to_string(),
        display_preference: Some("Default".to_string()),
        images: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::tools::ToolExecutionContext;
    use bamboo_agent_core::Session;
    use bamboo_storage::LockedSessionStore;
    use tokio::sync::RwLock;

    #[derive(Default)]
    struct TestStorage {
        sessions: RwLock<HashMap<String, Session>>,
    }

    #[async_trait]
    impl Storage for TestStorage {
        async fn save_session(&self, session: &Session) -> std::io::Result<()> {
            self.sessions
                .write()
                .await
                .insert(session.id.clone(), session.clone());
            Ok(())
        }

        async fn load_session(&self, session_id: &str) -> std::io::Result<Option<Session>> {
            Ok(self.sessions.read().await.get(session_id).cloned())
        }

        async fn delete_session(&self, session_id: &str) -> std::io::Result<bool> {
            Ok(self.sessions.write().await.remove(session_id).is_some())
        }
    }

    fn context(session_id: &str, tool: &str) -> ToolCtx {
        ToolExecutionContext {
            session_id: Some(session_id),
            tool_call_id: tool,
            event_tx: None,
            available_tool_schemas: None,
            bypass_permissions: false,
            can_async_resume: false,
            bash_completion_sink: None,
            pre_parsed_args: None,
        }
        .to_tool_ctx()
    }

    async fn repository(session: Session) -> SessionRepository {
        let storage = Arc::new(TestStorage::default());
        storage.save_session(&session).await.unwrap();
        let storage: Arc<dyn Storage> = storage;
        SessionRepository::new(
            Arc::new(dashmap::DashMap::new()),
            storage.clone(),
            Arc::new(LockedSessionStore::new(storage)),
        )
    }

    fn result(outcome: ToolOutcome) -> ToolResult {
        let ToolOutcome::Completed(result) = outcome else {
            panic!("expected completed tool result")
        };
        result
    }

    #[tokio::test]
    async fn workspace_reports_registered_project_and_preserves_identity() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let nested_workspace = workspace.join("linked-worktree-subdir");
        std::fs::create_dir_all(&nested_workspace).unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());
        let project = store
            .create_with_bindings(
                "Zenith",
                None,
                vec![WorkspaceBinding {
                    path: workspace.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let mut session = Session::new("session-1", "model");
        session.set_project_id_meta(project.id.to_string());
        let tool = ProjectWorkspaceTool::new(repository(session).await, store.clone());

        let output = result(
            tool.invoke(
                json!({"path": nested_workspace}),
                context("session-1", "Workspace"),
            )
            .await
            .unwrap(),
        );
        assert!(output.success);
        let value: Value = serde_json::from_str(&output.result).unwrap();
        assert_eq!(value["project_id"], project.id.to_string());
        assert_eq!(value["project_name"], "Zenith");
        assert_eq!(value["binding_status"], "registered");
        assert!(value["project_home"]
            .as_str()
            .is_some_and(|path| path.ends_with(project.id.as_str())));
        assert!(value.get("relocated_from").is_some());
    }

    #[tokio::test]
    async fn workspace_conflict_is_checked_before_session_state_changes() {
        let dir = tempfile::tempdir().unwrap();
        let old_workspace = dir.path().join("old");
        let owned_workspace = dir.path().join("owned");
        let owned_nested = owned_workspace.join("nested");
        std::fs::create_dir_all(&old_workspace).unwrap();
        std::fs::create_dir_all(&owned_nested).unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());
        let owner = store
            .create_with_bindings(
                "Owner",
                None,
                vec![WorkspaceBinding {
                    path: owned_workspace.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let session_project = store.create("Other", None).unwrap();
        let mut session = Session::new("session-conflict", "model");
        session.set_project_id_meta(session_project.id.to_string());
        session.set_workspace_path_meta(old_workspace.to_string_lossy().into_owned());
        session.add_message(bamboo_agent_core::Message::system(format!(
            "base\n\n{}\nProject ID: {}\n{}\n\n{}\nWorkspace path: {}\n{}",
            bamboo_engine::context::PROJECT_CONTEXT_START_MARKER,
            session_project.id,
            bamboo_engine::context::PROJECT_CONTEXT_END_MARKER,
            bamboo_engine::context::WORKSPACE_CONTEXT_START_MARKER,
            old_workspace.display(),
            bamboo_engine::context::WORKSPACE_CONTEXT_END_MARKER,
        )));
        let original_prompt = session.messages[0].content.clone();
        bamboo_agent_core::workspace_state::set_workspace(
            &session.id,
            old_workspace.canonicalize().unwrap(),
        );
        let repository = repository(session.clone()).await;
        let tool = ProjectWorkspaceTool::new(repository.clone(), store);

        let output = result(
            tool.invoke(
                json!({"path": owned_nested}),
                context(&session.id, "Workspace"),
            )
            .await
            .unwrap(),
        );
        assert!(!output.success);
        let value: Value = serde_json::from_str(&output.result).unwrap();
        assert_eq!(value["code"], "project_workspace_conflict");
        assert_eq!(value["owner_project_id"], owner.id.to_string());
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(&session.id),
            Some(old_workspace.canonicalize().unwrap())
        );
        let persisted = repository
            .try_load(&session.id)
            .await
            .unwrap()
            .expect("persisted session");
        assert_eq!(
            persisted.workspace_path_meta(),
            session.workspace_path_meta(),
            "conflict must not update session metadata"
        );
        assert_eq!(
            persisted.messages[0].content, original_prompt,
            "conflict must not update Project or Workspace prompt blocks"
        );
    }

    #[tokio::test]
    async fn workspace_set_preserves_messages_appended_after_the_cached_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());
        let mut session = Session::new("workspace-stale-message", "model");
        session.add_message(bamboo_agent_core::Message::user("cached message"));
        let repository = repository(session.clone()).await;
        let tool = ProjectWorkspaceTool::new(repository.clone(), store);

        let mut durable = repository
            .storage()
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        durable.add_message(bamboo_agent_core::Message::assistant(
            "concurrently appended durable message",
            None,
        ));
        repository.storage().save_session(&durable).await.unwrap();

        let output = result(
            tool.invoke(
                json!({"path": workspace}),
                context(&session.id, "Workspace"),
            )
            .await
            .unwrap(),
        );
        assert!(output.success);
        let persisted = repository
            .storage()
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.messages.len(), 2);
        assert_eq!(
            persisted.messages[1].content,
            "concurrently appended durable message"
        );
    }

    #[tokio::test]
    async fn workspace_set_rechecks_durable_project_after_a_stale_cache_hit() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("owned");
        std::fs::create_dir_all(&workspace).unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());
        let owner = store
            .create_with_bindings(
                "Original owner",
                None,
                vec![WorkspaceBinding {
                    path: workspace.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let reassigned = store.create("Reassigned", None).unwrap();
        let mut session = Session::new("workspace-stale-project", "model");
        session.set_project_id_meta(owner.id.to_string());
        let repository = repository(session.clone()).await;
        let tool = ProjectWorkspaceTool::new(repository.clone(), store);

        let mut durable = repository
            .storage()
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        durable.set_project_id_meta(reassigned.id.to_string());
        repository.storage().save_session(&durable).await.unwrap();

        let output = result(
            tool.invoke(
                json!({"path": workspace}),
                context(&session.id, "Workspace"),
            )
            .await
            .unwrap(),
        );
        assert!(!output.success);
        let value: Value = serde_json::from_str(&output.result).unwrap();
        assert_eq!(value["code"], "project_workspace_conflict");
        assert_eq!(value["session_project_id"], reassigned.id.to_string());
        let persisted = repository
            .storage()
            .load_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.project_id_meta().as_deref(),
            Some(reassigned.id.as_str())
        );
        assert!(persisted.workspace_path_meta().is_none());
        assert!(bamboo_agent_core::workspace_state::peek_workspace(&session.id).is_none());
    }

    #[tokio::test]
    async fn workspace_get_conflict_does_not_publish_persisted_candidate() {
        let dir = tempfile::tempdir().unwrap();
        let safe_workspace = dir.path().join("safe");
        let foreign_workspace = dir.path().join("foreign");
        std::fs::create_dir_all(&safe_workspace).unwrap();
        std::fs::create_dir_all(&foreign_workspace).unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());
        let owner = store
            .create_with_bindings(
                "Owner",
                None,
                vec![WorkspaceBinding {
                    path: foreign_workspace.to_string_lossy().into_owned(),
                    label: None,
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let assigned = store.create("Assigned", None).unwrap();
        let mut session = Session::new("workspace-get-conflict", "model");
        session.set_project_id_meta(assigned.id.to_string());
        session.set_workspace_path_meta(foreign_workspace.to_string_lossy().into_owned());
        bamboo_agent_core::workspace_state::set_workspace(
            &session.id,
            safe_workspace.canonicalize().unwrap(),
        );
        let tool = ProjectWorkspaceTool::new(repository(session.clone()).await, store);

        let output = result(
            tool.invoke(json!({}), context(&session.id, "Workspace"))
                .await
                .unwrap(),
        );
        assert!(!output.success);
        let value: Value = serde_json::from_str(&output.result).unwrap();
        assert_eq!(value["code"], "project_workspace_conflict");
        assert_eq!(value["owner_project_id"], owner.id.to_string());
        assert_eq!(
            bamboo_agent_core::workspace_state::get_workspace(&session.id),
            Some(safe_workspace.canonicalize().unwrap()),
            "GET must not publish the rejected persisted/default candidate"
        );
    }

    #[tokio::test]
    async fn invalid_or_missing_assigned_project_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let original = dir.path().join("original");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::create_dir_all(&original).unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());

        for (session_id, project_id) in [
            ("invalid-project", "../unsafe"),
            ("missing-project", "project-does-not-exist"),
        ] {
            let mut session = Session::new(session_id, "model");
            session
                .metadata
                .insert("project_id".to_string(), project_id.to_string());
            bamboo_agent_core::workspace_state::set_workspace(
                &session.id,
                original.canonicalize().unwrap(),
            );
            let tool = ProjectWorkspaceTool::new(repository(session.clone()).await, store.clone());
            let error = tool
                .invoke(
                    json!({"path": workspace}),
                    context(&session.id, "Workspace"),
                )
                .await
                .expect_err("corrupt assigned identity must fail closed");
            assert!(matches!(error, ToolError::Execution(_)));
            assert_eq!(
                bamboo_agent_core::workspace_state::get_workspace(&session.id),
                Some(original.canonicalize().unwrap())
            );
        }
    }

    #[tokio::test]
    async fn project_tool_uses_normalized_three_state_project_identity() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());
        let project = store.create("Normalized", None).unwrap();

        let mut assigned = Session::new("project-tool-trimmed", "model");
        assigned
            .metadata
            .insert("project_id".to_string(), format!("  {}  ", project.id));
        let tool = ProjectTool::new(repository(assigned).await, store.clone());
        let output = result(
            tool.invoke(
                json!({"action": "inspect"}),
                context("project-tool-trimmed", "Project"),
            )
            .await
            .expect("trimmed assigned Project id must resolve"),
        );
        assert!(output.success);
        let value: Value = serde_json::from_str(&output.result).unwrap();
        assert_eq!(value["project"]["id"], project.id.to_string());

        let mut invalid = Session::new("project-tool-invalid", "model");
        invalid
            .metadata
            .insert("project_id".to_string(), " ../unsafe ".to_string());
        let tool = ProjectTool::new(repository(invalid).await, store);
        let error = tool
            .invoke(
                json!({"action": "inspect"}),
                context("project-tool-invalid", "Project"),
            )
            .await
            .expect_err("invalid Project identity must fail closed");
        assert!(
            matches!(error, ToolError::Execution(ref message) if message.contains("invalid assigned Project identity"))
        );
    }

    #[tokio::test]
    async fn project_inspect_reports_redacted_resources_and_workspace_binding_contract() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());
        let project = store
            .create_with_bindings(
                "Inspect Contract",
                None,
                vec![WorkspaceBinding {
                    path: workspace.to_string_lossy().into_owned(),
                    label: Some("primary".to_string()),
                    git_common_dir: None,
                }],
            )
            .unwrap();
        let secret = "TOP-SECRET-PROJECT-TOKEN";
        std::fs::write(
            store.paths().settings_path(&project.id),
            format!(r#"{{"api_key":"{secret}"}}"#),
        )
        .unwrap();
        let mut session = Session::new("project-inspect-contract", "model");
        session.set_project_id_meta(project.id.to_string());
        session.set_workspace_path_meta(workspace.to_string_lossy().into_owned());
        let tool = ProjectTool::new(repository(session).await, store);

        let output = result(
            tool.invoke(
                json!({"action": "inspect"}),
                context("project-inspect-contract", "Project"),
            )
            .await
            .unwrap(),
        );
        assert!(output.success);
        assert!(!output.result.contains(secret));
        assert!(!output.result.contains("api_key"));
        let value: Value = serde_json::from_str(&output.result).unwrap();
        assert_eq!(value["project"]["id"], project.id.to_string());
        assert_eq!(value["resource_revision"], project.resource_revision);
        assert_eq!(
            value["resource_summary"]["resource_revision"],
            project.resource_revision
        );
        assert_eq!(value["binding_status"], "registered");
        assert_eq!(value["workspace_owner_project_id"], project.id.to_string());
        assert_eq!(
            value["workspace"],
            workspace.canonicalize().unwrap().to_string_lossy().as_ref()
        );
        assert!(value["resource_summary"]["resources"].is_array());
    }

    #[tokio::test]
    async fn project_binding_uses_cas_and_publishes_change_feed_event() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("bind");
        std::fs::create_dir_all(&workspace).unwrap();
        let store = Arc::new(ProjectStore::open(dir.path()).unwrap());
        let project = store.create("Project", None).unwrap();
        let mut session = Session::new("session-project-tool", "model");
        session.set_project_id_meta(project.id.to_string());
        let sink = bamboo_engine::events::AccountEventSink::new(dir.path().join("events")).unwrap();
        let mut events = sink.subscribe();
        let tool =
            ProjectTool::new(repository(session).await, store.clone()).with_account_sink(sink);

        let stale = result(
            tool.invoke(
                json!({
                    "action": "bind_workspace",
                    "path": workspace,
                    "expected_revision": project.revision + 1
                }),
                context("session-project-tool", "Project"),
            )
            .await
            .unwrap(),
        );
        assert!(!stale.success);
        assert_eq!(
            serde_json::from_str::<Value>(&stale.result).unwrap()["code"],
            "project_revision_conflict"
        );

        let success = result(
            tool.invoke(
                json!({
                    "action": "bind_workspace",
                    "path": workspace,
                    "expected_revision": project.revision
                }),
                context("session-project-tool", "Project"),
            )
            .await
            .unwrap(),
        );
        assert!(success.success);
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            &event.event,
            bamboo_agent_core::AgentEvent::ProjectUpdated { project_id, .. }
                if project_id == project.id.as_str()
        ));
    }
}
