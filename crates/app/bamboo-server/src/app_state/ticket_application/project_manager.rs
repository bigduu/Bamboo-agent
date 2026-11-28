use super::*;

use actix_web::{web, HttpRequest, HttpResponse};
use bamboo_domain::{
    ProjectId, ProjectManagerBinding, ProjectManifest, ProjectStatus, Session,
    SessionCallerReference, SessionKind, SessionMessageId, WorkspaceBinding,
};
use bamboo_engine::session_app::session_create::{
    build_new_session, CreateSessionConfig, CreateSessionInput,
};
use serde::Deserialize;
use std::path::Path;

const MANAGER_PROMPT: &str = "You are this Project's Project Manager. Use project_manager_work to list Work and its Sections or inspect the exact Work ID. If the tool is deferred, discover it by name. When instructed to start Work, select the requested Section or an existing Section that fits its content. Report the committed dispatch and let the Work Session execute. A submission awaits user acceptance.";

pub async fn open(
    state: web::Data<super::super::AppState>,
    request: HttpRequest,
    project_id: web::Path<ProjectId>,
) -> std::result::Result<HttpResponse, crate::handlers::agent::tickets::TicketHttpError> {
    crate::handlers::agent::tickets::user(&state, &request).await?;
    let project_id = project_id.into_inner();
    let session = ensure_manager(
        &state.project_store,
        &state.persistence,
        &state.config,
        &project_id,
    )
    .await?;
    Ok(HttpResponse::Ok().json(
        json!({"session_id": session.id, "project_id": project_id, "created_at": session.created_at}),
    ))
}

async fn ensure_manager(
    projects: &bamboo_projects::ProjectStore,
    persistence: &bamboo_storage::LockedSessionStore,
    config: &RwLock<Config>,
    project_id: &ProjectId,
) -> Result<Session> {
    let storage = persistence.storage();
    for _ in 0..3 {
        let mut project = active_project(projects, project_id)?;
        if let Some(binding) = project.project_manager.clone() {
            let id = binding.session_id.to_string();
            let _guard = persistence.acquire_lock(&id).await;
            project = active_project(projects, project_id)?;
            if project.project_manager.as_ref() != Some(&binding) {
                continue;
            }
            if let Some(session) = storage.load_root_authority(&id).await? {
                if manager_matches(&project, &session) {
                    return Ok(session);
                }
                return Err(Error::ScopeDenied(
                    "Project Manager lifetime changed".into(),
                ));
            }
            // An absent canonical Root may be an interrupted reservation or a
            // deleted lifetime. In either case the old ID is never restored.
        }

        let binding = ProjectManagerBinding {
            session_id: uuid::Uuid::new_v4(),
            created_at: chrono::Utc::now(),
        };
        let id = binding.session_id.to_string();
        // Concurrent openers must wait for publication before observing this
        // binding's Session as missing and trying another reservation.
        let _guard = persistence.acquire_lock(&id).await;
        match projects.update(project_id, project.revision, |manifest| {
            manifest.project_manager = Some(binding.clone());
            Ok(())
        }) {
            Ok(_) => {}
            Err(bamboo_projects::ProjectStoreError::Conflict { .. }) => continue,
            Err(error) => return Err(Error::AuthorityUnavailable(error.to_string())),
        }

        let config = config.read().await;
        let mut session = build_new_session(
            &CreateSessionInput {
                id: id.clone(),
                project_id: Some(project_id.clone()),
                title: Some("Project Manager".into()),
                title_generated: Some(true),
                system_prompt: Some(MANAGER_PROMPT.into()),
                model: None,
                model_ref: config
                    .defaults
                    .as_ref()
                    .map(|defaults| defaults.chat.clone()),
                reasoning_effort: None,
                gold_config_json: None,
                workspace_path: project.project_path.clone(),
                permission_mode: None,
            },
            &CreateSessionConfig {
                default_model: config.get_model(),
                default_reasoning_effort: config.get_reasoning_effort(),
                global_default_prompt: String::new(),
                builtin_fallback_prompt: MANAGER_PROMPT,
            },
        );
        session.metadata.insert(
            bamboo_domain::session::tool_authority::PROJECT_MANAGER_SESSION_HINT.into(),
            id,
        );
        session.created_at = binding.created_at;
        session.updated_at = binding.created_at;
        drop(config);
        storage.save_session(&session).await?;
        return Ok(session);
    }
    Err(Error::RevisionConflict)
}

fn active_project(
    store: &bamboo_projects::ProjectStore,
    id: &ProjectId,
) -> Result<ProjectManifest> {
    let project = store
        .get(id)
        .map_err(|error| Error::ScopeDenied(error.to_string()))?;
    if project.status != ProjectStatus::Active {
        return Err(Error::ScopeDenied("Project is archived".into()));
    }
    Ok(project)
}

pub(crate) fn manager_matches(project: &ProjectManifest, session: &Session) -> bool {
    project.project_manager.as_ref().is_some_and(|binding| {
        session.kind == SessionKind::Root
            && session.authority_identity.is_ordinary()
            && session.id == binding.session_id.to_string()
            && session.created_at == binding.created_at
            && session.project_id_meta().as_deref() == Some(project.id.as_str())
    })
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum ManagerRequest {
    List {
        cursor: Option<ReadCursor>,
        fixed_commit: Option<String>,
    },
    Inspect {
        work_id: String,
    },
    Start {
        work_id: String,
        section_id: Option<String>,
        worktree: Option<String>,
        operation_id: uuid::Uuid,
    },
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SupervisorProjectRequest {
    Projects {
        query: Option<String>,
        offset: Option<usize>,
    },
    Inspect {
        project_id: ProjectId,
        cursor: Option<ReadCursor>,
        fixed_commit: Option<String>,
    },
    Send {
        project_id: ProjectId,
        message: String,
        operation_id: uuid::Uuid,
    },
}

fn project_board_request(
    project_id: ProjectId,
    cursor: Option<ReadCursor>,
    fixed_commit: Option<String>,
) -> ProjectBoardRequest {
    ProjectBoardRequest {
        project_id,
        include_cancelled: false,
        include_archived: false,
        sprint_filter: None,
        overdue_as_of: None,
        limit: 24,
        cursor,
        fixed_commit,
    }
}

fn operation_key(
    kind: &str,
    caller: &SessionCallerReference,
    project_id: &ProjectId,
    id: uuid::Uuid,
) -> Result<String> {
    if id.is_nil() {
        return Err(Error::InvalidTransition(
            "operation_id must be a non-nil UUID".into(),
        ));
    }
    Ok(SessionMessageId::stable(
        kind,
        &json!([caller.session_id, caller.created_at, project_id, id]),
    )
    .to_string())
}

fn coordinator() -> Principal {
    Principal::Supervisor {
        session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
    }
}

impl TicketApplication {
    pub(crate) async fn supervisor_project_manager(
        &self,
        caller: &SessionCallerReference,
        request: SupervisorProjectRequest,
    ) -> Result<Value> {
        let host = self.managed_work_host.get().ok_or_else(|| {
            Error::AuthorityUnavailable("Project Work Host is unavailable".into())
        })?;
        let current = host
            .creator
            .check_original_caller(caller)
            .await
            .map_err(|error| Error::ScopeDenied(error.to_string()))?;
        let supervisor = bamboo_domain::SupervisorReference {
            session_id: current.id.clone(),
            incarnation_id: current
                .supervisor_incarnation_id()
                .ok_or_else(|| Error::ScopeDenied("Verified Supervisor required".into()))?,
        };
        let (service, authority) = self
            .authority(Principal::Supervisor {
                session_id: current.id,
            })
            .await?;
        match request {
            SupervisorProjectRequest::Projects { query, offset } => {
                let projects = host
                    .creator
                    .projects
                    .list()
                    .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?;
                let query = query.as_deref().map(|value| value.trim().to_lowercase());
                let offset = offset.unwrap_or(0);
                let mut matching = projects
                    .iter()
                    .filter(|project| project.status == ProjectStatus::Active)
                    .filter(|project| {
                        query.as_ref().is_none_or(|term| {
                            project.name.to_lowercase().contains(term)
                                || project.id.as_str().to_lowercase().contains(term)
                        })
                    })
                    .skip(offset);
                let page = matching
                    .by_ref()
                    .take(32)
                    .map(|project| json!({"id":project.id,"name":project.name}))
                    .collect::<Vec<_>>();
                let next_offset = matching.next().and_then(|_| offset.checked_add(page.len()));
                Ok(json!({"projects":page,"next_offset":next_offset}))
            }
            SupervisorProjectRequest::Inspect {
                project_id,
                cursor,
                fixed_commit,
            } => {
                let project = active_project(&host.creator.projects, &project_id)?;
                let board = service.project_board(
                    &authority,
                    &project_board_request(project_id, cursor, fixed_commit),
                )?;
                Ok(
                    json!({"name":project.name,"manager":project.project_manager.map(|binding| binding.session_id),"board":board}),
                )
            }
            SupervisorProjectRequest::Send {
                project_id,
                message,
                operation_id,
            } => {
                let key = operation_key(
                    "supervisor-project-manager-v1",
                    caller,
                    &project_id,
                    operation_id,
                )?;
                let manager = ensure_manager(
                    &host.creator.projects,
                    &host.persistence,
                    &self.config,
                    &project_id,
                )
                .await?;
                let target = bamboo_domain::SupervisorCurrentUserTarget::capture(&manager)
                    .map_err(|error| Error::ScopeDenied(error.to_string()))?;
                let receipt = host
                    .creator
                    .messenger
                    .supervisor_followup_with_material_notifications(
                        caller,
                        &supervisor,
                        &target,
                        &key,
                        &message,
                        true,
                    )
                    .await
                    .map_err(|error| Error::AuthorityUnavailable(error.to_string()))?;
                Ok(json!({
                    "project_id":project_id, "manager_session_id":manager.id,
                    "message_id":receipt.delivery.id, "inbox_sequence":receipt.delivery.generation,
                }))
            }
        }
    }

    async fn verify_project_manager(
        &self,
        caller: &SessionCallerReference,
    ) -> Result<ProjectManifest> {
        let host = self.managed_work_host.get().ok_or_else(|| {
            Error::AuthorityUnavailable("Project Work Host is unavailable".into())
        })?;
        let current = host
            .creator
            .check_original_caller(caller)
            .await
            .map_err(|error| Error::ScopeDenied(error.to_string()))?;
        let project_id = current
            .project_id_meta()
            .ok_or_else(|| Error::ScopeDenied("Session has no Project".into()))?;
        let project_id =
            ProjectId::parse(&project_id).map_err(|error| Error::ScopeDenied(error.to_string()))?;
        let project = active_project(&host.creator.projects, &project_id)?;
        if !manager_matches(&project, &current) {
            return Err(Error::ScopeDenied(
                "This Project's verified Project Manager is required".into(),
            ));
        }
        Ok(project)
    }

    pub(crate) async fn project_manager_work(
        &self,
        caller: &SessionCallerReference,
        request: ManagerRequest,
    ) -> Result<Value> {
        let project = self.verify_project_manager(caller).await?;
        let (service, authority) = self.authority(coordinator()).await?;
        match request {
            ManagerRequest::List {
                cursor,
                fixed_commit,
            } => {
                let board = service.project_board(
                    &authority,
                    &project_board_request(project.id.clone(), cursor, fixed_commit),
                )?;
                Ok(json!({
                    "board": board,
                    "sections": project.sidebar.as_ref().map(|sidebar| &sidebar.sections),
                    "workspace_roots": project.workspace_roots().collect::<Vec<_>>(),
                }))
            }
            ManagerRequest::Inspect { work_id } => {
                let snapshot = service.published()?.1;
                if snapshot.project.links.get(&work_id) != Some(&project.id) {
                    return Err(Error::ScopeDenied("Work is outside this Project".into()));
                }
                Ok(json!({
                    "ticket": snapshot.tickets.get(&work_id).ok_or(Error::RevisionConflict)?,
                    "sections": project.sidebar.as_ref().map(|sidebar| &sidebar.sections),
                    "workspace_roots": project.workspace_roots().collect::<Vec<_>>(),
                }))
            }
            ManagerRequest::Start {
                work_id,
                section_id,
                worktree,
                operation_id,
            } => {
                self.start_project_work(
                    caller,
                    project,
                    &work_id,
                    section_id.as_deref(),
                    worktree.as_deref(),
                    operation_id,
                )
                .await
            }
        }
    }
}

fn selected_workspace(
    project: &ProjectManifest,
    requested: Option<&str>,
) -> Result<(WorkspaceBinding, ExecutionWorkspace)> {
    let candidates = project
        .workspace_roots()
        .filter(|path| requested.is_none_or(|selected| *path == selected))
        .filter(|path| {
            std::fs::symlink_metadata(Path::new(path).join(".git"))
                .is_ok_and(|entry| entry.is_file())
        })
        .collect::<Vec<_>>();
    let [path] = candidates.as_slice() else {
        return Err(Error::ResourceBlocked(
            "Select one registered, prepared isolated Git worktree in this Project.".into(),
        ));
    };
    let path = *path;
    let worktrees = crate::handlers::agent::worktrees::list_git_worktrees(Path::new(path))
        .map_err(Error::ResourceBlocked)?;
    let tree = worktrees
        .into_iter()
        .find(|tree| tree.path == Path::new(path) && tree.openable && !tree.prunable)
        .ok_or_else(|| Error::ResourceBlocked("The registered worktree is unavailable.".into()))?;
    let branch = tree.branch.ok_or_else(|| {
        Error::ResourceBlocked("The registered worktree needs an attached branch.".into())
    })?;
    let binding = project
        .workspace_bindings
        .iter()
        .find(|binding| binding.path == path)
        .cloned()
        .unwrap_or_else(|| WorkspaceBinding {
            path: path.into(),
            label: None,
            git_common_dir: None,
        });
    Ok((
        binding,
        ExecutionWorkspace {
            repo: path.into(),
            base_commit: tree.head,
            branch,
            worktree: path.into(),
            write_roots: vec![path.into()],
            claims: BTreeSet::from([format!("worktree:{path}")]),
        },
    ))
}
impl TicketApplication {
    async fn start_project_work(
        &self,
        caller: &SessionCallerReference,
        project: ProjectManifest,
        work_id: &str,
        section_id: Option<&str>,
        requested_worktree: Option<&str>,
        operation_id: uuid::Uuid,
    ) -> Result<Value> {
        self.require_managed_work_enabled().await?;
        let manager = self
            .storage
            .load_session(&caller.session_id)
            .await?
            .filter(|session| caller.matches_session(session))
            .ok_or_else(|| Error::ScopeDenied("Project Manager Session changed".into()))?;
        let iteration = manager
            .messages
            .iter()
            .rev()
            .find(|message| message.role == bamboo_domain::Role::User)
            .filter(|message| {
                message
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata["runtime_kind"] == "paced_loop_iteration")
            });
        if iteration.is_some() {
            return Err(Error::ScopeDenied(
                "Automatic Project dispatch requires a scan permit".into(),
            ));
        }
        let key = operation_key(
            "project-manager-start-v1",
            caller,
            &project.id,
            operation_id,
        )?;
        let (service, _) = self.authority(coordinator()).await?;
        let mut snapshot = service.published()?.1;
        if snapshot.project.links.get(work_id) != Some(&project.id) {
            return Err(Error::ScopeDenied("Work is outside this Project".into()));
        }
        let ticket = snapshot
            .tickets
            .get(work_id)
            .ok_or_else(|| Error::InvalidTransition("Work not found".into()))?;
        if let Some(receipt) = snapshot.receipts.get(&key) {
            let stored: Command = serde_json::from_str(&receipt.canonical_request)?;
            let matching = matches!(stored.operations.last(), Some(Operation::Start { work_id: id, workspace: Some(workspace), .. })
                if id == work_id && requested_worktree.is_none_or(|path| path == workspace.worktree));
            let same_section = receipt
                .ids
                .get("assignment")
                .and_then(|id| snapshot.assignments.get(id))
                .and_then(|assignment| snapshot.intents.get(&assignment.dispatch_key))
                .and_then(|intent| intent.immutable_spec.managed_root.as_ref())
                .is_some_and(|target| target.section_id.as_deref() == section_id);
            if !matching || !same_section {
                return Err(Error::IdempotencyConflict);
            }
            return self
                .enqueue_receipt(&service, &stored, receipt.clone())
                .await;
        }
        if let Some(assignment) = &ticket.active_assignment {
            return Ok(
                json!({"status": "already_dispatched", "work_id": work_id, "assignment_id": assignment}),
            );
        }
        if ticket.archived
            || ticket.paused
            || !matches!(ticket.state, WorkState::Draft | WorkState::Ready)
        {
            return Err(Error::InvalidTransition(
                "Work must be an unpaused Draft or Ready card to start".into(),
            ));
        }
        let sections = project
            .sidebar
            .as_ref()
            .map_or(&[][..], |sidebar| sidebar.sections.as_slice());
        match section_id {
            Some(id) if !sections.iter().any(|section| section.id == id) => {
                return Err(Error::ScopeDenied(
                    "Section is outside this Project or no longer exists".into(),
                ));
            }
            None if !sections.is_empty() => {
                return Err(Error::InvalidTransition(
                    "Choose an existing Project Section for this Work".into(),
                ));
            }
            _ => {}
        }

        let workspace_project = project.clone();
        let selection = requested_worktree.map(str::to_owned);
        let (binding, workspace) = tokio::task::spawn_blocking(move || {
            selected_workspace(&workspace_project, selection.as_deref())
        })
        .await
        .map_err(|error| Error::AuthorityUnavailable(error.to_string()))??;
        bamboo_engine::ticket_worker_plan::files::verify_workspace(workspace.clone()).await?;
        let latest_context = snapshot
            .project
            .work_contexts
            .get(work_id)
            .and_then(|hash| snapshot.project.contexts.get(hash));
        if latest_context.is_some_and(|context| context.project_id != project.id) {
            return Err(Error::RevisionConflict);
        }
        let context = ProjectContextSnapshot {
            project_id: project.id.clone(),
            project_revision: project.revision,
            resource_revision: project.resource_revision,
            workspace_binding: binding,
            contract_revision: ticket.contract_revision,
            contract: ticket.contract.clone(),
            sources: latest_context
                .map_or_else(Default::default, |context| context.sources.clone()),
        };
        // Creating a Draft is independent of execution preparation. Adopt the
        // current snapshot while the Work is stopped, then claim that revision.
        if latest_context != Some(&context) {
            if self.verify_project_manager(caller).await?.revision != project.revision {
                return Err(Error::RevisionConflict);
            }
            let prepare = Command {
                operation_id: SessionMessageId::stable(
                    "project-manager-context-v1",
                    &json!([key, snapshot.seq]),
                )
                .to_string(),
                binding: snapshot.binding.clone(),
                expected_seq: snapshot.seq,
                expected_epoch: snapshot.authority_epoch,
                operations: vec![Operation::SetProjectContext {
                    work_id: work_id.into(),
                    context: context.clone(),
                }],
                source: None,
            };
            self.update(coordinator(), &prepare).await?;
            snapshot = service.published()?.1;
        }
        let ticket = snapshot
            .tickets
            .get(work_id)
            .ok_or(Error::RevisionConflict)?;
        if snapshot.project.links.get(work_id) != Some(&project.id)
            || ticket.contract_revision != context.contract_revision
            || ticket.active_assignment.is_some()
            || ticket.archived
            || ticket.paused
            || !matches!(ticket.state, WorkState::Draft | WorkState::Ready)
            || snapshot
                .project
                .work_contexts
                .get(work_id)
                .and_then(|hash| snapshot.project.contexts.get(hash))
                != Some(&context)
        {
            return Err(Error::RevisionConflict);
        }
        // Ticket placement still uses the canonical Host Supervisor service
        // backing this ledger. Only the verified manager may enter this
        // adapter, and the exact Project is carried into managed admission.
        let default_root = self
            .storage
            .load_execution_admission_snapshot(DEFAULT_SUPERVISOR_SESSION_ID)
            .await?
            .filter(|root| root.supervisor_incarnation_id().is_some())
            .ok_or_else(|| {
                Error::AuthorityUnavailable("Ticket Supervisor is unavailable".into())
            })?;
        if self.verify_project_manager(caller).await?.revision != project.revision {
            return Err(Error::RevisionConflict);
        }
        let mut operations = Vec::new();
        if ticket.state == WorkState::Draft {
            operations.push(Operation::Ready {
                work_id: work_id.into(),
            });
        }
        operations.push(Operation::Start {
            work_id: work_id.into(),
            temp_id: "assignment".into(),
            workspace: Some(workspace),
        });
        let command = Command {
            operation_id: key,
            binding: snapshot.binding.clone(),
            expected_seq: snapshot.seq,
            expected_epoch: snapshot.authority_epoch,
            operations,
            source: None,
        };
        self.dispatch_managed_start(
            coordinator(),
            &command,
            Some(&SessionCallerReference::from(&default_root)),
            Some((&project.id, section_id)),
        )
        .await
    }
}
