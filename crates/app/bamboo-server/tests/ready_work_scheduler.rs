//! Host-only Ready eligibility reaches the assembled native Child runner.
//! The provider is an in-process gate; no model service is contacted.
use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_domain::{Message, ProjectId, Role, Session, SessionKind, WorkspaceBinding};
use bamboo_engine::{execution::AgentStatus, ticket_runtime};
use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
use bamboo_server::app_state::ticket_application::{
    ReadyReconcile, ReadyWorkGrant, ReadyWorkPolicy, TicketApplication,
};
use bamboo_server::AppState;
use bamboo_storage::SessionStoreV2;
use bamboo_tickets::{
    AssignmentState, Contract, ExecutionWorkspace, Operation, Principal, ProjectContextSnapshot,
    Snapshot, StepStatus, TicketKind, WorkState,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, RwLock};

const PROVIDER: &str = "openai";
const MODEL: &str = "ready-work-controlled-model";
const RESULT: &str = "Ready Work completed through its real native Child.";
const TASK_CALL: &str = "ready-work-completed-plan";
const WAIT: Duration = Duration::from_secs(20);
const QUIET: Duration = Duration::from_millis(30);
const MAX_ROUNDS: u32 = 4;

#[derive(Debug)]
enum Reply {
    Task,
    LoopTask(u32),
    Complete,
}

#[derive(Debug)]
struct Request {
    process_id: u32,
    messages: Vec<Message>,
    tools: Vec<ToolSchema>,
    reply: oneshot::Sender<Reply>,
}

impl Request {
    fn is_work(&self, index: usize) -> bool {
        self.messages.iter().any(|message| {
            message.content.contains(&format!(
                "Produce bounded Ready Work fixture result {index}."
            ))
        })
    }

    fn task(self) {
        assert!(
            self.tools.iter().any(|tool| tool.function.name == "Task"),
            "the real worker must expose its authority-backed Task tool"
        );
        self.reply.send(Reply::Task).expect("live provider gate");
    }

    fn complete(self) {
        self.reply
            .send(Reply::Complete)
            .expect("live provider gate");
    }

    fn loop_task(self, round: u32) {
        assert!(self.tools.iter().any(|tool| tool.function.name == "Task"));
        self.reply
            .send(Reply::LoopTask(round))
            .expect("live bounded-loop provider gate");
    }
}

struct ControlledProvider {
    requests: mpsc::UnboundedSender<Request>,
    calls: AtomicUsize,
}

#[async_trait]
impl LLMProvider for ControlledProvider {
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        _max_output_tokens: Option<u32>,
        model: &str,
    ) -> Result<LLMStream, LLMError> {
        assert_eq!(model, MODEL, "all execution uses the injected local model");
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (reply, receive) = oneshot::channel();
        self.requests
            .send(Request {
                process_id: std::process::id(),
                messages: messages.to_vec(),
                tools: tools.to_vec(),
                reply,
            })
            .map_err(|_| LLMError::Api("test observer closed".into()))?;
        let chunk = match receive
            .await
            .map_err(|_| LLMError::Api("test provider gate closed".into()))?
        {
            Reply::Task => LLMChunk::ToolCalls(vec![ToolCall {
                id: TASK_CALL.into(),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "Task".into(),
                    arguments: json!({"tasks":[{
                        "id":"ready-result", "content":"Produce the bounded result",
                        "status":"completed"
                    }]})
                    .to_string(),
                },
            }]),
            Reply::LoopTask(round) => LLMChunk::ToolCalls(vec![ToolCall {
                id: format!("ready-work-loop-{round}"),
                tool_type: "function".into(),
                function: FunctionCall {
                    name: "Task".into(),
                    arguments: json!({"tasks":[{
                        "id":"bounded-loop", "content":"Keep the bounded plan in progress",
                        "status":"in_progress"
                    }]})
                    .to_string(),
                },
            }]),
            Reply::Complete => LLMChunk::Token(RESULT.into()),
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(chunk),
            Ok(LLMChunk::Done),
        ])))
    }
}

async fn wait_native_terminal(state: &AppState, child_id: &str) -> Session {
    let terminal = tokio::time::timeout(WAIT, async {
        loop {
            let child = state.storage.load_session(child_id).await.unwrap().unwrap();
            let stopped = child
                .metadata
                .get("runtime.local_task.state")
                .map(String::as_str)
                == Some("stopped");
            let runner_finished = state
                .agent_runners
                .read()
                .await
                .get(child_id)
                .is_none_or(|runner| !matches!(runner.status, AgentStatus::Running));
            if stopped
                && runner_finished
                && state
                    .session_activation_router
                    .current_run_id(child_id)
                    .await
                    .is_none()
            {
                return child;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    match terminal {
        Ok(child) => child,
        Err(_) => {
            let child = state.storage.load_session(child_id).await.unwrap().unwrap();
            let runner = state
                .agent_runners
                .read()
                .await
                .get(child_id)
                .map(|runner| format!("{:?}", runner.status));
            let logical_run = state
                .session_activation_router
                .current_run_id(child_id)
                .await;
            let dispatch = ticket_runtime::read_dispatch(&child).unwrap().unwrap();
            let (_, snapshot) = state.tickets.service().unwrap().published().unwrap();
            let observations = child
                .metadata
                .iter()
                .filter(|(key, _)| key.starts_with("runtime.") || key.starts_with("last_run"))
                .collect::<std::collections::BTreeMap<_, _>>();
            panic!(
                "round ceiling did not reach terminal observation: metadata={:?}, runner={runner:?}, logical_run={logical_run:?}, assignment={:?}",
                observations, snapshot.assignments[&dispatch.assignment_id]
            );
        }
    }
}

struct Work {
    id: String,
    contract_revision: u64,
    context_hash: String,
    workspace: ExecutionWorkspace,
}

impl Work {
    fn grant(&self) -> ReadyWorkGrant {
        ReadyWorkGrant {
            work_id: self.id.clone(),
            contract_revision: self.contract_revision,
            context_hash: self.context_hash.clone(),
            workspace: self.workspace.clone(),
        }
    }
}

struct Fixture {
    state: AppState,
    requests: mpsc::UnboundedReceiver<Request>,
    provider: Arc<ControlledProvider>,
    project: ProjectId,
    works: Vec<Work>,
    _root: tempfile::TempDir,
}

fn git(path: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn workspace(root: &Path, index: usize) -> ExecutionWorkspace {
    let repo = root.join(format!("repo-{index}"));
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-b", "fixture"]);
    std::fs::write(repo.join("result.txt"), "fixture\n").unwrap();
    git(&repo, &["add", "result.txt"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Ready Work Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-m",
            "fixture",
        ],
    );
    let base_commit = git(&repo, &["rev-parse", "HEAD"]);
    let worktree = root.join(format!("worktree-{index}"));
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "ready-work-fixture",
            worktree.to_str().unwrap(),
            &base_commit,
        ],
    );
    let worktree = worktree
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    ExecutionWorkspace {
        repo: repo.canonicalize().unwrap().to_string_lossy().into_owned(),
        base_commit,
        branch: "ready-work-fixture".into(),
        worktree: worktree.clone(),
        write_roots: vec![worktree.clone()],
        claims: BTreeSet::from([format!("worktree:{worktree}")]),
    }
}

impl Fixture {
    async fn new(work_count: usize) -> Self {
        let root = tempfile::tempdir().unwrap();
        let (send, requests) = mpsc::unbounded_channel();
        let provider = Arc::new(ControlledProvider {
            requests: send,
            calls: AtomicUsize::new(0),
        });
        let mut config: Config = serde_json::from_value(json!({
            "provider":PROVIDER,
            "features":{"ticket_mutation":true,"ticket_dispatch":true},
            "providers":{"openai":{"api_key":"fixture","model":MODEL}},
            "run_budget":{"max_rounds":MAX_ROUNDS},
            "subagents":{"max_concurrent":2}
        }))
        .unwrap();
        config
            .memory_mut()
            .get_or_insert_with(Default::default)
            .auto_dream_enabled = false;
        let state = AppState::new_with_provider_and_memory_store(
            root.path().to_path_buf(),
            config,
            provider.clone(),
            bamboo_memory::memory_store::MemoryStore::new(root.path().join("jiandu")),
        )
        .await
        .unwrap();
        // Keep the shared registry Arc already held by the assembled runner.
        state.provider_registry.replace_with(ProviderRegistry::new(
            HashMap::from([(PROVIDER.into(), provider.clone() as Arc<dyn LLMProvider>)]),
            PROVIDER.into(),
        ));
        {
            let assembled_config = state.config.read().await;
            assert!(assembled_config.subagents().broker.is_none());
            assert!(assembled_config.cluster_fabric.nodes.is_empty());
        }
        assert!(!root.path().join("broker").exists());
        let mut manifest = state
            .project_store
            .create("Ready Work fixture", None)
            .unwrap();
        let workspaces = (0..work_count)
            .map(|index| workspace(root.path(), index))
            .collect::<Vec<_>>();
        for workspace in &workspaces {
            manifest = state
                .project_store
                .bind_workspace(
                    &manifest.id,
                    manifest.revision,
                    WorkspaceBinding {
                        path: workspace.repo.clone(),
                        label: None,
                        git_common_dir: None,
                    },
                )
                .unwrap();
        }
        let owner = Principal::User {
            user_id: "fixture-host-owner".into(),
        };
        let (service, authority) = state.tickets.authority(owner.clone()).await.unwrap();
        let mut works = Vec::new();
        for (index, workspace) in workspaces.into_iter().enumerate() {
            let contract = Contract {
                title: format!("Ready Work {index}"),
                objective: format!("Produce bounded Ready Work fixture result {index}."),
                constraints: vec!["Use only the granted local plan".into()],
                acceptance: vec!["A human reviews the submitted result".into()],
                user_acceptance_required: true,
                allowed_tools: BTreeSet::from(["Task".into()]),
            };
            let create = service
                .prepare_command(
                    &authority,
                    &format!("fixture-create-{index}"),
                    vec![
                        Operation::Create {
                            temp_id: "work".into(),
                            kind: TicketKind::Work,
                            parent: None,
                            contract,
                            depends_on: BTreeSet::new(),
                        },
                        Operation::AssociateProject {
                            ticket_id: "work".into(),
                            project_id: manifest.id.clone(),
                        },
                    ],
                )
                .unwrap();
            let id = state
                .tickets
                .update(owner.clone(), &create)
                .await
                .unwrap()
                .ids["work"]
                .clone();
            let snapshot = service.published().unwrap().1;
            let ticket = &snapshot.tickets[&id];
            let context = ProjectContextSnapshot {
                project_id: manifest.id.clone(),
                project_revision: manifest.revision,
                resource_revision: manifest.resource_revision,
                workspace_binding: manifest
                    .workspace_bindings
                    .iter()
                    .find(|binding| binding.path == workspace.repo)
                    .unwrap()
                    .clone(),
                contract_revision: ticket.contract_revision,
                contract: ticket.contract.clone(),
                sources: BTreeMap::from([("fixture-spec".into(), "v1".into())]),
            };
            let ready = service
                .prepare_command(
                    &authority,
                    &format!("fixture-ready-{index}"),
                    vec![
                        Operation::SetProjectContext {
                            work_id: id.clone(),
                            context,
                        },
                        Operation::Ready {
                            work_id: id.clone(),
                        },
                    ],
                )
                .unwrap();
            state.tickets.update(owner.clone(), &ready).await.unwrap();
            let snapshot = service.published().unwrap().1;
            works.push(Work {
                contract_revision: snapshot.tickets[&id].contract_revision,
                context_hash: snapshot.project.work_contexts[&id].clone(),
                id,
                workspace,
            });
        }
        Self {
            state,
            requests,
            provider,
            project: manifest.id,
            works,
            _root: root,
        }
    }

    fn policy(&self, capacity: usize) -> ReadyWorkPolicy {
        ReadyWorkPolicy {
            project_id: self.project.clone(),
            grants: self.works.iter().map(Work::grant).collect(),
            capacity,
            max_rounds: MAX_ROUNDS,
            lifetime: Duration::from_secs(60),
        }
    }

    fn snapshot(&self) -> Snapshot {
        self.state.tickets.service().unwrap().published().unwrap().1
    }

    async fn next_request(&mut self) -> Request {
        tokio::time::timeout(WAIT, self.requests.recv())
            .await
            .expect("event-driven worker reaches the provider")
            .expect("live controlled provider")
    }

    async fn assert_quiet(&mut self, expected_calls: usize) {
        assert!(
            tokio::time::timeout(QUIET, self.requests.recv())
                .await
                .is_err(),
            "no additional provider call is authorized"
        );
        assert_eq!(self.provider.calls.load(Ordering::SeqCst), expected_calls);
    }

    async fn assert_supervisor_tool_boundary(&self) -> u64 {
        let snapshot = self.snapshot();
        let supervisor = self
            .state
            .storage
            .load_root_authority(&snapshot.binding.supervisor_session_id)
            .await
            .unwrap()
            .unwrap();
        assert!(supervisor.supervisor_incarnation_id().is_some());
        assert!(supervisor.model_orchestration_only_enabled());
        // This is the canonical Session predicate consumed by both the runtime
        // catalog projection and the final tool admission gate. The ordinary
        // Root executor's unfiltered list_tools/get_all_tool_schemas is not an
        // observation of this Supervisor's authority.
        for execution_name in [
            "SubAgent",
            "Plan",
            "Task",
            "Read",
            "Grep",
            "Glob",
            "GetFileInfo",
            "ViewImage",
            "work_overview",
            "work_search",
            "work_inspect",
            "work_changes",
            "work_update",
            "work_dispatch",
            "session_control",
            "session_history",
        ] {
            assert!(
                supervisor.allows_model_tool_execution(execution_name),
                "Supervisor retains bounded orchestration/read evidence: {execution_name}"
            );
        }
        for execution_name in [
            "Bash",
            "exec_command",
            "write_stdin",
            "Write",
            "Edit",
            "MultiEdit",
            "apply_patch",
            "browser",
            "load_skill",
            "workflow_run",
            "cluster",
            "deploy_agent",
            "unknown_implementation_tool",
        ] {
            assert!(
                !supervisor.allows_model_tool_execution(execution_name),
                "a worker blocker cannot grant Supervisor execution: {execution_name}"
            );
        }
        supervisor.root_tool_authority_revision
    }

    async fn wait_submitted(&self, work_id: &str) {
        tokio::time::timeout(WAIT, async {
            loop {
                let snapshot = self.snapshot();
                let work = &snapshot.tickets[work_id];
                assert_ne!(work.state, WorkState::Accepted, "only a human may accept");
                if work.state == WorkState::Submitted
                    && work.current_submission.as_ref().is_some_and(|id| {
                        snapshot.assignments[&snapshot.submissions[id].assignment_id]
                            .process_stopped
                    })
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("native completion did not submit: {:?}", self.snapshot()));
    }
}

#[tokio::test]
async fn no_host_policy_keeps_empty_and_ready_queues_provider_free() {
    let mut fixture = Fixture::new(0).await;
    fixture.state.tickets.wake_ready_work();
    assert!(matches!(
        fixture.state.tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    ));
    fixture.assert_quiet(0).await;
    fixture.state.shutdown().await;

    let mut fixture = Fixture::new(1).await;
    fixture.state.tickets.wake_ready_work();
    assert!(matches!(
        fixture.state.tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    ));
    assert!(fixture.snapshot().assignments.is_empty());
    assert_eq!(
        fixture.snapshot().tickets[&fixture.works[0].id].state,
        WorkState::Ready
    );
    fixture.assert_quiet(0).await;
    fixture.state.shutdown().await;
}

#[tokio::test]
async fn policy_wake_runs_real_child_task_and_submits_once_without_auto_acceptance() {
    let mut fixture = Fixture::new(1).await;
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(1))
        .await
        .unwrap();
    fixture.state.tickets.wake_ready_work();
    // No explicit reconcile drives admission: the installed worker consumes the wake.
    let first = fixture.next_request().await;
    assert_eq!(first.process_id, std::process::id());
    let snapshot = fixture.snapshot();
    assert_eq!(snapshot.assignments.len(), 1);
    let assignment = snapshot.assignments.values().next().unwrap();
    let dispatch_key = assignment.dispatch_key.clone();
    let child_id = assignment.runtime.as_ref().unwrap().session_id.clone();
    let child = fixture
        .state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child.kind, SessionKind::Child);
    assert_eq!(
        child.parent_session_id.as_deref(),
        Some(snapshot.binding.supervisor_session_id.as_str())
    );
    assert_eq!(
        snapshot.project.assignment_contexts[&assignment.id],
        fixture.works[0].context_hash
    );
    for _ in 0..3 {
        let result = fixture.state.tickets.reconcile_ready_work().await.unwrap();
        assert!(
            !matches!(result, ReadyReconcile::Dispatched { .. }),
            "same grant cannot dispatch twice: {result:?}"
        );
    }
    fixture.assert_quiet(1).await;
    first.task();
    let final_request = fixture.next_request().await;
    assert!(
        final_request.messages.iter().any(|message| {
            message.role == Role::Tool
                && message.tool_call_id.as_deref() == Some(TASK_CALL)
                && message.tool_success == Some(true)
        }),
        "continuation must consume its real Task reply"
    );
    let snapshot = fixture.snapshot();
    let assignment = snapshot.assignments.values().next().unwrap();
    assert_eq!(assignment.plan.plan_revision, 1);
    assert_eq!(assignment.plan.steps.len(), 1);
    assert_eq!(assignment.plan.steps[0].status, Some(StepStatus::Completed));
    final_request.complete();
    fixture.wait_submitted(&fixture.works[0].id).await;
    let snapshot = fixture.snapshot();
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(snapshot.intents.len(), 1);
    assert_eq!(snapshot.submissions.len(), 1);
    assert_eq!(
        snapshot.assignments.values().next().unwrap().dispatch_key,
        dispatch_key
    );
    let work = &snapshot.tickets[&fixture.works[0].id];
    assert!(work.accepted_submission.is_none());
    assert!(work.current_submission.is_some());
    fixture.state.tickets.wake_ready_work();
    fixture.state.tickets.reconcile_ready_work().await.unwrap();
    fixture.assert_quiet(2).await;
    fixture.state.tickets.revoke_ready_work_policy().await;
    fixture.state.shutdown().await;
}

#[tokio::test]
async fn sprint_carry_over_preserves_real_worker_identity_and_dispatch_budget() {
    use bamboo_tickets::{
        SprintAction, SprintCommand, SprintDefinition, SprintDestination, SprintId, SprintReceipt,
        SprintStatus,
    };

    async fn plan(
        fixture: &Fixture,
        sprint_id: &SprintId,
        action: SprintAction,
    ) -> (SprintCommand, SprintReceipt) {
        let snapshot = fixture.snapshot();
        let command = SprintCommand {
            operation_id: format!("native-sprint-{}", snapshot.seq),
            binding: snapshot.binding,
            expected_seq: snapshot.seq,
            expected_epoch: snapshot.authority_epoch,
            project_id: fixture.project.clone(),
            sprint_id: sprint_id.clone(),
            source: "controlled native Sprint carry-over".into(),
            action,
        };
        let receipt = fixture
            .state
            .tickets
            .sprint_command(
                Principal::User {
                    user_id: "fixture-host-owner".into(),
                },
                &command,
            )
            .await
            .unwrap();
        (command, receipt)
    }

    let mut fixture = Fixture::new(1).await;
    let work_id = fixture.works[0].id.clone();
    let sprint_id = SprintId::parse("native-sprint-running").unwrap();
    let next_id = SprintId::parse("native-sprint-next").unwrap();
    for (id, name) in [(&sprint_id, "Running Sprint"), (&next_id, "Next Sprint")] {
        plan(
            &fixture,
            id,
            SprintAction::Create {
                definition: SprintDefinition {
                    name: name.into(),
                    goal: "Carry the same Work without changing execution".into(),
                    starts_at: None,
                    ends_at: None,
                },
            },
        )
        .await;
    }
    plan(
        &fixture,
        &sprint_id,
        SprintAction::Add {
            expected_revision: fixture.snapshot().sprint.sprints[&sprint_id].revision,
            work_id: work_id.clone(),
        },
    )
    .await;
    plan(
        &fixture,
        &sprint_id,
        SprintAction::Start {
            expected_revision: fixture.snapshot().sprint.sprints[&sprint_id].revision,
        },
    )
    .await;
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(1))
        .await
        .unwrap();
    fixture.state.tickets.wake_ready_work();
    let first = fixture.next_request().await;
    assert!(first.is_work(0));
    first.task();
    let final_request = fixture.next_request().await;
    assert!(final_request.messages.iter().any(|message| {
        message.role == Role::Tool
            && message.tool_call_id.as_deref() == Some(TASK_CALL)
            && message.tool_success == Some(true)
    }));
    assert_eq!(fixture.provider.calls.load(Ordering::SeqCst), 2);

    // Hold the second real model request: the first Task round has completed,
    // but this actual Child still owns its execution and has not submitted.
    let before = fixture.snapshot();
    let assignment = before.assignments.values().next().unwrap().clone();
    assert_eq!(assignment.state, AssignmentState::Running);
    assert!(!assignment.process_stopped);
    assert_eq!(assignment.plan.plan_revision, 1);
    assert_eq!(assignment.plan.steps[0].status, Some(StepStatus::Completed));
    assert_eq!(
        assignment.workspace.as_ref().unwrap(),
        &fixture.works[0].workspace
    );
    assert!(!assignment.workspace.as_ref().unwrap().claims.is_empty());
    let receipt = assignment.runtime.as_ref().unwrap().clone();
    let child_id = receipt.session_id.clone();
    let logical_run = fixture
        .state
        .session_activation_router
        .current_run_id(&child_id)
        .await
        .expect("the held provider request still has its real run owner");
    let child = fixture
        .state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    let dispatch = ticket_runtime::read_dispatch(&child).unwrap().unwrap();
    assert_eq!(dispatch.max_rounds, Some(MAX_ROUNDS));
    let spent_round = child.agent_runtime_state.as_ref().unwrap().round.clone();
    assert!(
        spent_round.total_tool_calls > 0,
        "compare consumed counters, not an unstarted run"
    );
    assert_eq!(spent_round.max_rounds, Some(MAX_ROUNDS));

    let (close, closed) = plan(
        &fixture,
        &sprint_id,
        SprintAction::Close {
            expected_revision: before.sprint.sprints[&sprint_id].revision,
            dispositions: BTreeMap::from([(
                work_id.clone(),
                SprintDestination::Sprint {
                    sprint_id: next_id.clone(),
                },
            )]),
            target_revisions: BTreeMap::from([(
                next_id.clone(),
                before.sprint.sprints[&next_id].revision,
            )]),
        },
    )
    .await;
    let replay = fixture
        .state
        .tickets
        .sprint_command(
            Principal::User {
                user_id: "fixture-host-owner".into(),
            },
            &close,
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(&closed).unwrap(),
        serde_json::to_value(replay).unwrap()
    );
    let closure = closed.closure.unwrap();
    assert_eq!(closure.counts.accepted, 0);
    assert_eq!(closure.counts.unfinished, 1);

    let after = fixture.snapshot();
    assert_eq!(
        after.sprint.sprints[&sprint_id].status,
        SprintStatus::Closed
    );
    assert_eq!(after.sprint.sprints[&next_id].status, SprintStatus::Planned);
    assert_eq!(
        after.sprint.sprints[&next_id].members,
        BTreeSet::from([work_id.clone()])
    );
    assert_eq!(
        serde_json::to_value(&after.tickets).unwrap(),
        serde_json::to_value(&before.tickets).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&after.assignments).unwrap(),
        serde_json::to_value(&before.assignments).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&after.intents).unwrap(),
        serde_json::to_value(&before.intents).unwrap()
    );
    assert_eq!(
        fixture
            .state
            .session_activation_router
            .current_run_id(&child_id)
            .await,
        Some(logical_run)
    );
    let live_child = fixture
        .state
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ticket_runtime::read_dispatch(&live_child).unwrap().unwrap(),
        dispatch
    );
    assert_eq!(
        live_child.agent_runtime_state.as_ref().unwrap().round,
        spent_round
    );
    assert!(!matches!(
        fixture.state.tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Dispatched { .. }
    ));
    fixture.assert_quiet(2).await;

    final_request.complete();
    fixture.wait_submitted(&work_id).await;
    let stopped = wait_native_terminal(&fixture.state, &child_id).await;
    let final_snapshot = fixture.snapshot();
    assert_eq!(final_snapshot.tickets.len(), before.tickets.len());
    assert_eq!(final_snapshot.assignments.len(), 1);
    assert_eq!(final_snapshot.intents.len(), 1);
    assert_eq!(final_snapshot.submissions.len(), 1);
    let final_assignment = &final_snapshot.assignments[&assignment.id];
    assert_eq!(final_assignment.dispatch_key, assignment.dispatch_key);
    assert_eq!(final_assignment.generation, assignment.generation);
    assert_eq!(final_assignment.runtime.as_ref(), Some(&receipt));
    assert_eq!(final_assignment.workspace, assignment.workspace);
    assert!(final_assignment.process_stopped);
    let work = &final_snapshot.tickets[&work_id];
    assert_eq!(work.state, WorkState::Submitted);
    assert!(work.accepted_submission.is_none());
    assert_eq!(
        final_snapshot.submissions[work.current_submission.as_ref().unwrap()].runtime,
        receipt
    );
    assert_eq!(
        final_snapshot.sprint.sprints[&sprint_id].closure.as_ref(),
        Some(&closure)
    );
    assert_eq!(
        final_snapshot.sprint.sprints[&next_id].members,
        BTreeSet::from([work_id])
    );
    assert_eq!(
        ticket_runtime::read_dispatch(&stopped).unwrap().unwrap(),
        dispatch
    );
    assert!(
        stopped
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .round
            .total_tool_calls
            >= spent_round.total_tool_calls
    );
    assert_eq!(
        stopped
            .agent_runtime_state
            .as_ref()
            .unwrap()
            .round
            .max_rounds,
        Some(MAX_ROUNDS)
    );
    fixture.assert_quiet(2).await;
    fixture.state.tickets.revoke_ready_work_policy().await;
    fixture.state.shutdown().await;
}

#[tokio::test]
async fn pending_metadata_preserves_original_native_admission_and_resource_guard() {
    for invalidate_resource in [false, true] {
        let mut fixture = Fixture::new(1).await;
        let work_id = fixture.works[0].id.clone();
        let adopted_hash = fixture.works[0].context_hash.clone();
        let principal = Principal::User {
            user_id: "fixture-host-owner".into(),
        };
        let (service, authority) = fixture
            .state
            .tickets
            .authority(principal.clone())
            .await
            .unwrap();
        // Commit the actual canonical Start before runtime admission. No
        // Assignment, DispatchSpec or RuntimeReceipt is fabricated by this seed.
        let start = service
            .prepare_command(
                &authority,
                "pending-context-start",
                vec![Operation::Start {
                    work_id: work_id.clone(),
                    temp_id: "assignment".into(),
                    workspace: Some(fixture.works[0].workspace.clone()),
                }],
            )
            .unwrap();
        let receipt = service.execute(&authority, &start).unwrap();
        let assignment_id = receipt.ids["assignment"].clone();
        let original = fixture.snapshot();
        let dispatch_key = original.assignments[&assignment_id].dispatch_key.clone();
        let original_spec = original.intents[&dispatch_key].immutable_spec.clone();
        assert!(original.assignments[&assignment_id].runtime.is_none());
        let manifest = fixture.state.project_store.get(&fixture.project).unwrap();
        let mut updated = fixture
            .state
            .project_store
            .update(&fixture.project, manifest.revision, |project| {
                project.name = "Renamed while the original Start is pending".into();
                Ok(())
            })
            .unwrap();
        assert_eq!(updated.resource_revision, manifest.resource_revision);
        if invalidate_resource {
            updated = fixture
                .state
                .project_store
                .bump_resource_revision(&fixture.project, updated.revision)
                .unwrap();
        }
        let mut candidate = original.project.contexts[&adopted_hash].clone();
        candidate.project_revision = updated.revision;
        candidate.resource_revision = updated.resource_revision;
        let queue = service
            .prepare_command(
                &authority,
                "pending-context-metadata",
                vec![Operation::SetProjectContext {
                    work_id: work_id.clone(),
                    context: candidate,
                }],
            )
            .unwrap();
        fixture
            .state
            .tickets
            .update(principal, &queue)
            .await
            .unwrap();
        let queued = fixture.snapshot();
        let candidate_hash = queued.project.work_contexts[&work_id].clone();
        assert_ne!(candidate_hash, adopted_hash);
        assert_eq!(
            queued.project.assignment_contexts,
            original.project.assignment_contexts
        );
        assert_eq!(queued.intents[&dispatch_key].immutable_spec, original_spec);
        fixture
            .state
            .tickets
            .install_ready_work_policy(fixture.policy(1))
            .await
            .unwrap();
        if invalidate_resource {
            assert!(matches!(
                fixture.state.tickets.reconcile_ready_work().await.unwrap(),
                ReadyReconcile::Blocked { .. }
            ));
            fixture.assert_quiet(0).await;
            let snapshot = fixture.snapshot();
            assert!(snapshot.assignments[&assignment_id].runtime.is_none());
            assert!(snapshot.submissions.is_empty());
        } else {
            let request = fixture.next_request().await;
            assert!(request.is_work(0));
            assert_eq!(request.process_id, std::process::id());
            let running = fixture.snapshot();
            assert_eq!(running.assignments.len(), 1);
            assert!(running.assignments[&assignment_id].runtime.is_some());
            assert_eq!(running.intents[&dispatch_key].immutable_spec, original_spec);
            request.task();
            fixture.next_request().await.complete();
            fixture.wait_submitted(&work_id).await;
            fixture.assert_quiet(2).await;
            let submitted = fixture.snapshot();
            assert_eq!(submitted.submissions.len(), 1);
            assert!(submitted.tickets[&work_id].accepted_submission.is_none());
        }
        let final_snapshot = fixture.snapshot();
        assert_eq!(final_snapshot.assignments.len(), 1);
        assert_eq!(final_snapshot.intents.len(), 1);
        assert_eq!(final_snapshot.tickets[&work_id].generation, 1);
        assert_eq!(
            final_snapshot.project.work_contexts[&work_id],
            candidate_hash
        );
        assert_eq!(
            final_snapshot.project.assignment_contexts[&assignment_id],
            adopted_hash
        );
        assert_eq!(
            final_snapshot.intents[&dispatch_key].immutable_spec,
            original_spec
        );
        fixture.state.tickets.revoke_ready_work_policy().await;
        fixture.state.shutdown().await;
    }
}

#[tokio::test]
async fn changed_project_resources_and_stale_grant_context_block_before_start() {
    for stale_resource in [true, false] {
        let mut fixture = Fixture::new(1).await;
        let mut policy = fixture.policy(1);
        if stale_resource {
            let manifest = fixture.state.project_store.get(&fixture.project).unwrap();
            fixture
                .state
                .project_store
                .bump_resource_revision(&fixture.project, manifest.revision)
                .unwrap();
        } else {
            policy.grants[0].context_hash = "0".repeat(64);
        }
        let before = serde_json::to_value(fixture.snapshot()).unwrap();
        fixture
            .state
            .tickets
            .install_ready_work_policy(policy)
            .await
            .unwrap();
        assert!(matches!(
            fixture.state.tickets.reconcile_ready_work().await.unwrap(),
            ReadyReconcile::Blocked { work_id, reason, .. }
                if work_id == fixture.works[0].id && !reason.is_empty()
        ));
        fixture.state.tickets.wake_ready_work();
        fixture.assert_quiet(0).await;
        assert_eq!(
            serde_json::to_value(fixture.snapshot()).unwrap(),
            before,
            "eligibility failure cannot publish Start or a claim"
        );
        fixture.state.tickets.revoke_ready_work_policy().await;
        fixture.state.shutdown().await;
    }
}

#[tokio::test]
async fn held_native_provider_occupies_capacity_and_revocation_prevents_next_work() {
    let mut fixture = Fixture::new(2).await;
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(1))
        .await
        .unwrap();
    fixture.state.tickets.wake_ready_work();
    let running = fixture.next_request().await;
    let snapshot = fixture.snapshot();
    assert_eq!(snapshot.assignments.len(), 1);
    let running_work = snapshot
        .assignments
        .values()
        .next()
        .unwrap()
        .work_id
        .clone();
    assert!(matches!(
        fixture.state.tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::CapacityBlocked {
            occupied: 1,
            capacity: 1
        }
    ));
    fixture.assert_quiet(1).await;
    fixture.state.tickets.revoke_ready_work_policy().await;
    running.complete();
    fixture.wait_submitted(&running_work).await;
    fixture.state.tickets.wake_ready_work();
    assert!(matches!(
        fixture.state.tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    ));
    fixture.assert_quiet(1).await;
    let snapshot = fixture.snapshot();
    assert_eq!(snapshot.assignments.len(), 1);
    assert!(fixture
        .works
        .iter()
        .filter(|work| work.id != running_work)
        .all(|work| snapshot.tickets[&work.id].state == WorkState::Ready));
    fixture.state.shutdown().await;
}

#[tokio::test]
async fn queued_child_keeps_host_round_ceiling_after_global_budget_is_relaxed() {
    for relaxed_max_rounds in [None, Some(MAX_ROUNDS * 8)] {
        let mut fixture = Fixture::new(3).await;
        fixture
            .state
            .tickets
            .install_ready_work_policy(fixture.policy(3))
            .await
            .unwrap();
        // Each pass commits at most one Start. The real native backend has only
        // two concurrent slots, so the third durable Child remains queued.
        tokio::time::timeout(WAIT, async {
            while fixture.snapshot().assignments.len() < 3 {
                match fixture.state.tickets.reconcile_ready_work().await {
                    Ok(_) | Err(bamboo_tickets::Error::RevisionConflict) => {}
                    Err(error) => panic!("queued admission failed: {error}"),
                }
                // Runtime admission can advance the same scope while a Host
                // pass is checking eligibility. A CAS loss needs a full fresh
                // read and selection, just as the managed reconciler performs.
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("three exact Start receipts survive concurrent runtime ACKs");
        let first = fixture.next_request().await;
        let second = fixture.next_request().await;
        // A scope-CAS loss can defer an earlier Work while another eligible
        // grant starts. Identify actual owners from the controlled native
        // requests instead of treating grant order as a runtime receipt.
        let first_index = (0..fixture.works.len())
            .find(|index| first.is_work(*index))
            .unwrap();
        let second_index = (0..fixture.works.len())
            .find(|index| second.is_work(*index))
            .unwrap();
        assert_ne!(first_index, second_index);
        let queued_index = (0..fixture.works.len())
            .find(|index| *index != first_index && *index != second_index)
            .unwrap();
        let queued_work = fixture.works[queued_index].id.clone();
        let queued_child = tokio::time::timeout(WAIT, async {
            loop {
                let snapshot = fixture.snapshot();
                if snapshot.assignments.len() == 3 {
                    let assignment = snapshot
                        .assignments
                        .values()
                        .find(|assignment| assignment.work_id == queued_work)
                        .unwrap();
                    assert_eq!(assignment.state, AssignmentState::DispatchPending);
                    assert!(
                        assignment.runtime.is_none(),
                        "the third Child cannot be admitted before a native slot is released"
                    );
                    let child_id = ticket_runtime::ticket_child_id(&assignment.dispatch_key);
                    if let Some(child) =
                        fixture.state.storage.load_session(&child_id).await.unwrap()
                    {
                        assert_eq!(
                            ticket_runtime::read_dispatch(&child)
                                .unwrap()
                                .unwrap()
                                .max_rounds,
                            Some(MAX_ROUNDS),
                            "the Host ceiling must be canonical before queue wait"
                        );
                        break child_id;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("third Start and canonical queued Child are durable");
        fixture.assert_quiet(2).await;

        // This changes the global value before the queued native runner builds
        // its ExecuteRequest. Its immutable dispatch binding must still tighten
        // that later value to the Host's original finite decision.
        fixture.state.config.write().await.run_budget.max_rounds = relaxed_max_rounds;
        first.complete();
        for round in 0..=MAX_ROUNDS {
            let request = fixture.next_request().await;
            assert!(
                request.is_work(queued_index),
                "only the actual queued Work may advance"
            );
            let summary = request.messages.iter().any(|message| {
                message
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("runtime_kind"))
                    .and_then(serde_json::Value::as_str)
                    == Some("max_rounds_summary")
            });
            assert_eq!(
                summary,
                round == MAX_ROUNDS,
                "exactly one summary grace turn follows the retained Host cap"
            );
            // Even the summary ignores the stop request and emits another Task.
            // Unique tool call IDs keep this a real loop, rather than receipt replay.
            request.loop_task(round);
        }
        let stopped = tokio::select! {
            child = wait_native_terminal(&fixture.state, &queued_child) => child,
            extra = fixture.requests.recv() => {
                if let Some(request) = extra {
                    request.complete();
                }
                panic!("queued Child exceeded the Host cap and its one summary grace turn after global relaxation {relaxed_max_rounds:?}");
            }
        };
        assert_eq!(
            ticket_runtime::read_dispatch(&stopped)
                .unwrap()
                .unwrap()
                .max_rounds,
            Some(MAX_ROUNDS)
        );
        assert_eq!(
            stopped
                .agent_runtime_state
                .as_ref()
                .unwrap()
                .round
                .max_rounds,
            Some(MAX_ROUNDS),
            "runtime uses the canonical Host cap instead of the relaxed global value"
        );
        assert_eq!(
            stopped
                .metadata
                .get("runtime.completion_reason")
                .map(String::as_str),
            Some("max_rounds_reached")
        );
        assert_eq!(stopped.last_run_status().as_deref(), Some("error"));
        assert!(stopped
            .last_run_error()
            .unwrap()
            .contains("Ticket run round budget exhausted"));
        let snapshot = fixture.snapshot();
        let dispatch = ticket_runtime::read_dispatch(&stopped).unwrap().unwrap();
        let assignment = &snapshot.assignments[&dispatch.assignment_id];
        assert!(assignment.process_stopped);
        assert_eq!(assignment.state, AssignmentState::Failed);
        assert_eq!(snapshot.tickets[&queued_work].state, WorkState::Blocked);
        assert!(snapshot.tickets[&queued_work].current_submission.is_none());
        fixture.assert_quiet(2 + MAX_ROUNDS as usize + 1).await;
        second.complete();
        fixture.wait_submitted(&fixture.works[first_index].id).await;
        fixture
            .wait_submitted(&fixture.works[second_index].id)
            .await;
        assert_ne!(
            fixture.snapshot().tickets[&queued_work].state,
            WorkState::Accepted
        );
        assert_eq!(fixture.snapshot().assignments.len(), 3);
        fixture.state.tickets.revoke_ready_work_policy().await;
        fixture.state.shutdown().await;
    }
}

#[tokio::test]
async fn corrupt_durable_retry_metadata_never_starts_a_child_or_supervisor() {
    let mut fixture = Fixture::new(1).await;
    let authority = fixture.assert_supervisor_tool_boundary().await;
    let directory = fixture._root.path().join("ready-retry-v1");
    std::fs::create_dir(&directory).unwrap();
    std::fs::write(directory.join("state.json"), b"{truncated retry metadata").unwrap();
    let before = serde_json::to_value(fixture.snapshot()).unwrap();
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(1))
        .await
        .unwrap();
    for _ in 0..2 {
        assert!(
            matches!(fixture.state.tickets.reconcile_ready_work().await.unwrap(),
            ReadyReconcile::Blocked { work_id, reason, .. }
                if work_id == fixture.works[0].id && reason.contains("durable retry metadata unavailable"))
        );
    }
    fixture.state.tickets.wake_ready_work();
    fixture.assert_quiet(0).await;
    assert_eq!(serde_json::to_value(fixture.snapshot()).unwrap(), before);
    assert_eq!(fixture.assert_supervisor_tool_boundary().await, authority);
    assert!(fixture.snapshot().assignments.is_empty());
    fixture.state.tickets.revoke_ready_work_policy().await;
    fixture.state.shutdown().await;
}

#[tokio::test]
async fn missing_workspace_stays_fail_closed_across_repeated_reconcile() {
    let mut fixture = Fixture::new(1).await;
    let supervisor_authority = fixture.assert_supervisor_tool_boundary().await;
    std::fs::remove_dir_all(&fixture.works[0].workspace.worktree).unwrap();
    let before = serde_json::to_value(fixture.snapshot()).unwrap();
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(1))
        .await
        .unwrap();
    for _ in 0..2 {
        assert!(matches!(
            fixture.state.tickets.reconcile_ready_work().await.unwrap(),
            ReadyReconcile::Blocked { work_id, reason, .. }
                if work_id == fixture.works[0].id && !reason.is_empty()
        ));
    }
    fixture.state.tickets.wake_ready_work();
    fixture.assert_quiet(0).await;
    let snapshot = fixture.snapshot();
    assert_eq!(serde_json::to_value(&snapshot).unwrap(), before);
    assert!(snapshot.assignments.is_empty());
    assert!(!fixture
        .state
        .agent_runners
        .read()
        .await
        .contains_key(&snapshot.binding.supervisor_session_id));
    assert_eq!(
        fixture.assert_supervisor_tool_boundary().await,
        supervisor_authority,
        "workspace failure cannot widen durable Supervisor tool authority"
    );
    fixture.state.tickets.revoke_ready_work_policy().await;
    fixture.state.shutdown().await;
}

#[tokio::test]
async fn missing_workspace_does_not_poison_other_grants_or_execute_supervisor() {
    let mut fixture = Fixture::new(2).await;
    let supervisor_authority = fixture.assert_supervisor_tool_boundary().await;
    let bad_work = fixture.works[0].id.clone();
    let good_work = fixture.works[1].id.clone();
    std::fs::remove_dir_all(&fixture.works[0].workspace.worktree).unwrap();
    let before_bad = serde_json::to_value(&fixture.snapshot().tickets[&bad_work]).unwrap();
    fixture
        .state
        .tickets
        .install_ready_work_policy(fixture.policy(1))
        .await
        .unwrap();
    fixture.state.tickets.wake_ready_work();
    let good_request = fixture.next_request().await;
    assert!(
        good_request.messages.iter().any(|message| message
            .content
            .contains("Produce bounded Ready Work fixture result 1.")),
        "the sole provider request must belong to the valid Work"
    );
    for _ in 0..2 {
        fixture.state.tickets.reconcile_ready_work().await.unwrap();
        let snapshot = fixture.snapshot();
        assert_eq!(snapshot.assignments.len(), 1);
        assert_eq!(
            snapshot.assignments.values().next().unwrap().work_id,
            good_work
        );
        assert_eq!(
            serde_json::to_value(&snapshot.tickets[&bad_work]).unwrap(),
            before_bad,
            "missing workspace must never fall back to an ambient workspace"
        );
        assert!(!fixture
            .state
            .agent_runners
            .read()
            .await
            .contains_key(&snapshot.binding.supervisor_session_id));
        assert_eq!(
            fixture.assert_supervisor_tool_boundary().await,
            supervisor_authority,
            "reconciling another worker cannot widen Supervisor tool authority"
        );
    }
    fixture.assert_quiet(1).await;
    fixture.state.tickets.revoke_ready_work_policy().await;
    good_request.complete();
    fixture.wait_submitted(&good_work).await;
    fixture.assert_quiet(1).await;
    assert_eq!(
        fixture.snapshot().tickets[&bad_work].state,
        WorkState::Ready
    );
    assert!(fixture.snapshot().tickets[&bad_work]
        .active_assignment
        .is_none());
    fixture.state.shutdown().await;
}

// No AppState builder or worker interval is started in these policy lifecycle
// fixtures, so virtual time advances exactly one Host deadline.
async fn policy_fixture() -> (
    tempfile::TempDir,
    Arc<SessionStoreV2>,
    Arc<RwLock<Config>>,
    TicketApplication,
) {
    let root = tempfile::tempdir().unwrap();
    let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
    let config = Arc::new(RwLock::new(
        serde_json::from_value(json!({
            "provider":PROVIDER,
            "features":{"ticket_mutation":true,"ticket_dispatch":true},
            "providers":{"openai":{"api_key":"fixture","model":MODEL}},
            "run_budget":{"max_rounds":MAX_ROUNDS}
        }))
        .unwrap(),
    ));
    let app = TicketApplication::open(root.path(), storage.clone(), config.clone()).await;
    app.service().unwrap();
    (root, storage, config, app)
}

fn lifecycle_policy() -> ReadyWorkPolicy {
    ReadyWorkPolicy {
        project_id: ProjectId::parse("lifecycle-fixture").unwrap(),
        grants: vec![ReadyWorkGrant {
            work_id: "not-yet-existing-work".into(),
            contract_revision: 1,
            context_hash: "0".repeat(64),
            // This fixture never reaches eligibility or attempts a Start.
            workspace: ExecutionWorkspace {
                repo: "/fixture/repo".into(),
                base_commit: "0".repeat(40),
                branch: "fixture".into(),
                worktree: "/fixture/worktree".into(),
                write_roots: vec!["/fixture/worktree".into()],
                claims: BTreeSet::from(["worktree:/fixture/worktree".into()]),
            },
        }],
        capacity: 1,
        max_rounds: MAX_ROUNDS,
        lifetime: Duration::from_secs(7),
    }
}

#[tokio::test(start_paused = true)]
async fn host_policy_expires_at_virtual_deadline_and_can_be_explicitly_revoked() {
    let (_root, _storage, _config, app) = policy_fixture().await;
    let before = serde_json::to_value(app.service().unwrap().published().unwrap()).unwrap();
    app.install_ready_work_policy(lifecycle_policy())
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(
        app.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Idle
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        app.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    );
    app.install_ready_work_policy(lifecycle_policy())
        .await
        .unwrap();
    app.revoke_ready_work_policy().await;
    assert_eq!(
        app.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    );
    assert_eq!(
        serde_json::to_value(app.service().unwrap().published().unwrap()).unwrap(),
        before,
        "policy lifetime and revocation never publish a Ticket operation"
    );
}

#[tokio::test]
async fn finite_round_ceiling_is_required_before_host_policy_installation() {
    let (_root, _storage, config, app) = policy_fixture().await;
    for max_rounds in [None, Some(0), Some(MAX_ROUNDS + 1)] {
        config.write().await.run_budget.max_rounds = max_rounds;
        assert!(app
            .install_ready_work_policy(lifecycle_policy())
            .await
            .is_err());
        assert_eq!(
            app.reconcile_ready_work().await.unwrap(),
            ReadyReconcile::Disabled
        );
    }
    config.write().await.run_budget.max_rounds = Some(MAX_ROUNDS);
    app.install_ready_work_policy(lifecycle_policy())
        .await
        .unwrap();
    app.revoke_ready_work_policy().await;
}

#[tokio::test]
async fn reopening_the_ticket_writer_advances_epoch_without_restoring_host_policy() {
    let (root, storage, config, app) = policy_fixture().await;
    let epoch = app
        .service()
        .unwrap()
        .published()
        .unwrap()
        .1
        .authority_epoch;
    app.install_ready_work_policy(lifecycle_policy())
        .await
        .unwrap();
    drop(app);
    let reopened = TicketApplication::open(root.path(), storage, config).await;
    let snapshot = reopened.service().unwrap().published().unwrap().1;
    assert!(snapshot.authority_epoch > epoch);
    assert_eq!(
        reopened.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    );
    assert!(snapshot.assignments.is_empty());
}

#[path = "ready_work_scheduler/admission.rs"]
mod admission;
