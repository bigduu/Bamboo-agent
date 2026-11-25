use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use bamboo_agent_core::tools::{ToolCtx, ToolError};
use bamboo_domain::{
    session::TaskItemStatus, HostSessionRight, Session, SessionActivationPort,
    SessionCallerReference, SessionKind, Storage, CONNECT_SESSION_ORIGIN_KEY,
    DEFAULT_SUPERVISOR_SESSION_ID,
};
use bamboo_engine::{SessionActivationRouter, SessionMessenger};
use bamboo_server_tools::session_coordination::{RunningSessionDirectory, RunningSessionTarget};
use chrono::Utc;
use tokio::sync::RwLock;

use super::{AgentRunner, AgentStatus};

pub(super) struct ProjectRunningSessionDirectory {
    pub(super) storage: Arc<dyn Storage>,
    pub(super) messenger: Arc<SessionMessenger>,
    pub(super) router: Arc<SessionActivationRouter>,
    pub(super) runners: Arc<RwLock<HashMap<String, AgentRunner>>>,
}

impl ProjectRunningSessionDirectory {
    async fn caller_session(&self, caller: &SessionCallerReference) -> Result<Session, ToolError> {
        let current = self
            .storage
            .load_runtime_control_plane(&caller.session_id)
            .await
            .map_err(|_| authority_unavailable())?
            .filter(|session| caller.matches_session(session))
            .ok_or_else(authority_unavailable)?;
        current
            .check_execution_compatibility()
            .map_err(|_| authority_unavailable())?;
        Ok(current)
    }

    async fn is_running(&self, target: &Session, run_id: &str) -> bool {
        let id = target.id.clone();
        let birth = target.created_at;
        let run_id = run_id.to_owned();
        let runners = self.runners.clone();
        let router = self.router.clone();
        tokio::task::spawn_blocking(move || {
            let Ok(_owner) = router.retain_expected_run_blocking(&id, birth, &run_id) else {
                return false;
            };
            // Never wait for the runner table while holding its publication fence.
            runners.try_read().is_ok_and(|entries| {
                entries.get(&id).is_some_and(|runner| {
                    runner.run_id == run_id
                        && matches!(runner.status, AgentStatus::Running)
                        && !runner.cancel_token.is_cancelled()
                })
            })
        })
        .await
        .unwrap_or(false)
    }
}

#[async_trait]
impl RunningSessionDirectory for ProjectRunningSessionDirectory {
    async fn list_running(
        &self,
        ctx: &ToolCtx,
        query: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RunningSessionTarget>, ToolError> {
        let caller = ctx
            .session_id()
            .and_then(|id| ctx.executing_caller_for(id))
            .ok_or_else(authority_unavailable)?;
        let source = self.caller_session(caller).await?;
        let scope = self
            .storage
            .find_host_session_scope(caller)
            .await
            .map_err(|_| authority_unavailable())?;
        let mut candidates: Vec<_> = self
            .runners
            .read()
            .await
            .iter()
            .filter(|(id, runner)| {
                *id != &caller.session_id && matches!(runner.status, AgentStatus::Running)
            })
            .map(|(id, runner)| {
                (
                    id.clone(),
                    runner.run_id.clone(),
                    runner.last_activity_at().unwrap_or(runner.started_at),
                )
            })
            .collect();
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        candidates.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));

        let (scoped, peers) = match scope.as_ref() {
            Some(token) => (
                self.storage
                    .list_host_scoped_targets(token, caller)
                    .await
                    .map_err(|_| authority_unavailable())?
                    .into_iter()
                    .map(|target| (target.observation.identity.session_id.clone(), target))
                    .collect::<HashMap<_, _>>(),
                Default::default(),
            ),
            None if caller.authority_identity.is_ordinary() => (
                HashMap::new(),
                self.messenger
                    .list_peer_targets(&caller.session_id)
                    .await
                    .map_err(|_| authority_unavailable())?
                    .targets
                    .into_iter()
                    .map(|target| target.session_id)
                    .collect::<HashSet<_>>(),
            ),
            None => return Ok(Vec::new()),
        };
        let source_project = source.project_id_meta();
        let can_discover_project = project_root(&source) && source_project.is_some();
        let query = query.map(str::to_lowercase);
        let mut result = Vec::new();
        for (id, run_id, _) in candidates {
            let (target, can_send, may_include_task) = if scope.is_some() {
                let Some(grant) = scoped.get(&id) else {
                    continue;
                };
                let Ok(Some(target)) = self.storage.load_runtime_control_plane(&id).await else {
                    continue;
                };
                if grant.observation.check_session(&target).is_err()
                    || target.project_id_meta() != source_project
                    || (target.root_session_id != source.root_session_id
                        && !(can_discover_project && project_root(&target)))
                {
                    continue;
                }
                (
                    target,
                    grant.rights.contains(&HostSessionRight::SendPeer),
                    grant.rights.contains(&HostSessionRight::ReadContextView),
                )
            } else if peers.contains(&id) {
                let Ok(Some(target)) = self.storage.load_runtime_control_plane(&id).await else {
                    continue;
                };
                if target.root_session_id != source.root_session_id
                    || target.project_id_meta() != source_project
                {
                    continue;
                }
                (target, true, true)
            } else if can_discover_project {
                let Ok(target) = self.storage.load_same_project_root_peer(caller, &id).await else {
                    continue;
                };
                (target, true, true)
            } else {
                continue;
            };
            let title = bounded_label(&target.title, 160);
            let brief = may_include_task
                .then(|| task_brief(&target))
                .flatten()
                .unwrap_or_else(|| title.clone());
            if query.as_ref().is_some_and(|query| {
                ![&id, &title, &brief]
                    .iter()
                    .any(|field| field.to_lowercase().contains(query))
            }) || !self.is_running(&target, &run_id).await
            {
                continue;
            }
            result.push(RunningSessionTarget {
                session_id: id,
                run_id,
                title,
                brief,
                observed_at: Utc::now(),
                can_send,
            });
            if result.len() == limit {
                break;
            }
        }
        let current_scope = self
            .storage
            .find_host_session_scope(caller)
            .await
            .map_err(|_| authority_unavailable())?;
        if !match (scope, current_scope) {
            (Some(original), Some(current)) => original.same_installation(&current),
            (None, None) => true,
            _ => false,
        } || self.caller_session(caller).await?.project_id_meta() != source_project
        {
            return Err(authority_unavailable());
        }
        Ok(result)
    }
}

fn authority_unavailable() -> ToolError {
    ToolError::Execution(
        "running_session_directory_unavailable: caller scope could not be verified".into(),
    )
}

fn project_root(session: &Session) -> bool {
    session.kind == SessionKind::Root
        && session.authority_identity.is_ordinary()
        && session.id != DEFAULT_SUPERVISOR_SESSION_ID
        && !session.metadata.contains_key(CONNECT_SESSION_ORIGIN_KEY)
}

fn bounded_label(value: &str, max_chars: usize) -> String {
    value
        .chars()
        .take(max_chars)
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn task_brief(session: &Session) -> Option<String> {
    let tasks = session
        .task_list
        .as_ref()
        .filter(|tasks| tasks.session_id == session.id)?;
    let task = tasks
        .items
        .iter()
        .find(|task| task.status == TaskItemStatus::InProgress)?;
    let description = task.active_form.as_deref().unwrap_or(&task.description);
    let label = bounded_label(description, 200);
    (!label.is_empty()).then_some(label)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_agent_core::tools::{
        with_executing_session_caller, FunctionCall, ToolCall, ToolExecutionContext,
    };
    use bamboo_domain::{
        session::{TaskItem, TaskList},
        HostScopeSessionObservation, HostSessionScopeBinding, HostSessionTargetScope, Message,
        HOST_SESSION_SCOPE_VERSION,
    };
    use serde_json::{json, Value};

    use crate::{app_state::AppState, tools::ToolSurface};

    async fn list(
        state: &AppState,
        caller: &Session,
        query: Option<&str>,
        plan_read_only: bool,
    ) -> Value {
        let call = ToolCall {
            id: "coordination-list".into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "session_coordination".into(),
                arguments: json!({ "action": "list_running", "query": query, "limit": 1 })
                    .to_string(),
            },
        };
        let mut ctx = ToolExecutionContext::none(&call.id);
        ctx.session_id = Some(&caller.id);
        ctx.plan_read_only = plan_read_only;
        let surface = match caller.kind {
            SessionKind::Root => ToolSurface::Root,
            SessionKind::Child => ToolSurface::Child,
        };
        let tools = state.tools_for(surface);
        assert!(tools
            .framework_session_tool_for_exact("session_coordination")
            .is_some_and(|proof| proof.is_current()));
        let result = with_executing_session_caller(
            Some(Arc::new(SessionCallerReference::from(caller))),
            tools.execute_with_context(&call, ctx),
        )
        .await
        .unwrap();
        assert!(result.success, "{}", result.result);
        serde_json::from_str(&result.result).unwrap()
    }

    fn root(id: &str, project: &str) -> Session {
        let mut session = Session::new(id, "model");
        session.title = id.into();
        session.set_project_id_meta(project);
        session.set_last_run_status("running");
        session
    }

    #[tokio::test]
    async fn assembled_coordination_directory_lists_current_project_runs_and_respects_finite_scope()
    {
        let home = tempfile::tempdir().unwrap();
        let state = Box::pin(AppState::new_with_provider(
            home.path().into(),
            bamboo_llm::Config::default(),
            Arc::new(super::super::UnconfiguredProvider {
                message: "running directory must never call a model".into(),
            }),
        ))
        .await
        .unwrap();
        let caller = root("coordination-caller", "coordination-project");
        let mut peer = root("coordination-peer", "coordination-project");
        let unrelated = root("unrelated-active", "different-project");
        let idle = root("stale-running-mirror", "coordination-project");
        let mut child = Session::new_child_of("coordination-child", &caller, "model", "Child");
        child.set_project_id_meta("coordination-project");
        peer.add_message(Message::user("private transcript must never be listed"));
        peer.set_task_list(TaskList {
            session_id: peer.id.clone(),
            title: "Migration".into(),
            items: vec![TaskItem {
                id: "active".into(),
                description: "Prepare the database migration".into(),
                active_form: Some("Validating the database migration".into()),
                status: TaskItemStatus::InProgress,
                ..Default::default()
            }],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        for session in [&caller, &peer, &unrelated, &idle, &child] {
            state.storage.save_session(session).await.unwrap();
        }
        let mut registrations = Vec::new();
        for session in [&peer, &unrelated, &caller, &child] {
            let mut runner = AgentRunner::new();
            runner.status = AgentStatus::Running;
            registrations.push(
                state
                    .session_activation_router
                    .register_session_run(session, &runner.run_id)
                    .await
                    .unwrap(),
            );
            state
                .agent_runners
                .write()
                .await
                .insert(session.id.clone(), runner);
        }

        let page = list(&state, &caller, Some("database"), false).await;
        assert_eq!(page["targets"][0]["session_id"], peer.id);
        assert_eq!(
            page["targets"][0]["brief"],
            "Validating the database migration"
        );
        assert_eq!(page["targets"][0]["can_send"], true);
        let planned = list(&state, &caller, Some("database"), true).await;
        assert_eq!(planned["targets"][0]["session_id"], peer.id);
        assert_eq!(planned["targets"][0]["can_send"], false);
        assert!(page["targets"][0]["run_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()));
        assert!(!page.to_string().contains("private transcript"));
        for query in ["unrelated-active", "stale-running-mirror"] {
            assert_eq!(
                list(&state, &caller, Some(query), false).await["targets"],
                json!([])
            );
        }
        let same_tree = list(&state, &child, None, false).await;
        assert_eq!(same_tree["targets"][0]["session_id"], caller.id);
        assert_eq!(same_tree["targets"][0]["can_send"], true);
        assert_eq!(
            list(&state, &child, Some(&peer.id), false).await["targets"],
            json!([])
        );

        state
            .session_store
            .install_host_session_scope(
                0,
                HostSessionScopeBinding {
                    schema_version: HOST_SESSION_SCOPE_VERSION,
                    grant_id: uuid::Uuid::new_v4(),
                    revision: 1,
                    caller: HostScopeSessionObservation::capture(&caller, vec![]).unwrap(),
                    targets: [(
                        peer.id.clone(),
                        HostSessionTargetScope {
                            observation: HostScopeSessionObservation::capture(&peer, vec![])
                                .unwrap(),
                            rights: [HostSessionRight::ListMetadata].into(),
                        },
                    )]
                    .into(),
                    creates: Default::default(),
                },
            )
            .await
            .unwrap();
        let scoped = list(&state, &caller, None, false).await;
        assert_eq!(scoped["targets"][0]["session_id"], peer.id);
        assert_eq!(scoped["targets"][0]["brief"], peer.title);
        assert_eq!(scoped["targets"][0]["can_send"], false);

        registrations[0].begin_finalization().await;
        assert_eq!(
            list(&state, &caller, None, false).await["targets"],
            json!([])
        );
    }
}
