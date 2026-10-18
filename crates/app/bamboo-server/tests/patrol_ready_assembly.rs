//! Real Ready admission and Patrol policy journals survive a Host process restart.
//! The parent owns the disposable root and waits for the seed process to exit
//! successfully before reopening that exact root. No lock is deleted or bypassed.
//! Normal shutdown is requested, but OS process exit supplies final ownership
//! release: AppState's existing strong reference cycle is a separate lifecycle
//! limitation, not something this fixture claims shutdown/drop has repaired.
//! The private support follows ready_work_scheduler.rs without importing its
//! tests or changing its native assertions. Worker calls use an in-process gate.
#![cfg(unix)]

use bamboo_orchestration::{
    patrol::{
        Checkpoint, CheckpointPort, Incident, IncidentId, Observation, ObservationPort, Phase,
        PortError, Reason, Scope, StoredCheckpoint,
    },
    ready_recovery::{
        ReadyRetryKey, ReadyRetryPhase, ReadyRetryRequest, ReadyRetryStore, VersionedReadyRetry,
    },
};
use bamboo_server::app_state::ticket_application::{FileReadyRetryStore, ReadyReconcile};
use bamboo_tickets::{RuntimeReceipt, ScopeBinding, WorkState};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

mod support {
    use async_trait::async_trait;
    use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
    use bamboo_domain::{Message, Role, WorkspaceBinding};
    use bamboo_engine::execution::AgentStatus;
    use bamboo_llm::{Config, LLMChunk, LLMError, LLMProvider, LLMStream, ProviderRegistry};
    use bamboo_server::app_state::ticket_application::{ReadyWorkGrant, ReadyWorkPolicy};
    use bamboo_server::AppState;
    use bamboo_tickets::{
        Contract, ExecutionWorkspace, Operation, Principal, ProjectContextSnapshot, Snapshot,
        TicketKind, WorkState,
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
    use tokio::sync::{mpsc, oneshot};

    const MODEL: &str = "patrol-ready-controlled-model";
    const OBJECTIVE: &str = "Produce the bounded Patrol/Ready assembly result.";
    const TASK_CALL: &str = "patrol-ready-completed-plan";
    const WAIT: Duration = Duration::from_secs(20);
    const MAX_ROUNDS: u32 = 4;

    enum Reply {
        Task,
        Complete,
    }

    pub(super) struct Gate {
        messages: Vec<Message>,
        tools: Vec<ToolSchema>,
        reply: oneshot::Sender<Reply>,
    }

    impl Gate {
        pub(super) fn finish_task(self) {
            assert!(self.messages.iter().any(|m| m.content.contains(OBJECTIVE)));
            assert!(self.tools.iter().any(|t| t.function.name == "Task"));
            assert!(self.reply.send(Reply::Task).is_ok());
        }

        pub(super) fn finish_result(self) {
            assert!(self.messages.iter().any(|m| {
                m.role == Role::Tool
                    && m.tool_call_id.as_deref() == Some(TASK_CALL)
                    && m.tool_success == Some(true)
            }));
            assert!(self.reply.send(Reply::Complete).is_ok());
        }
    }

    struct ControlledProvider {
        requests: mpsc::UnboundedSender<Gate>,
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
            assert_eq!(model, MODEL);
            self.calls.fetch_add(1, Ordering::SeqCst);
            let (reply, receive) = oneshot::channel();
            self.requests
                .send(Gate {
                    messages: messages.to_vec(),
                    tools: tools.to_vec(),
                    reply,
                })
                .map_err(|_| LLMError::Api("assembly observer closed".into()))?;
            let chunk = match receive
                .await
                .map_err(|_| LLMError::Api("assembly gate closed".into()))?
            {
                Reply::Task => LLMChunk::ToolCalls(vec![ToolCall {
                    id: TASK_CALL.into(),
                    tool_type: "function".into(),
                    function: FunctionCall {
                        name: "Task".into(),
                        arguments: json!({"tasks":[{
                            "id":"assembly-result", "content":"Produce the bounded result",
                            "status":"completed"
                        }]})
                        .to_string(),
                    },
                }]),
                Reply::Complete => LLMChunk::Token("Bounded result for human review.".into()),
            };
            Ok(Box::pin(futures::stream::iter([
                Ok(chunk),
                Ok(LLMChunk::Done),
            ])))
        }
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
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().into()
    }

    fn workspace(root: &Path) -> ExecutionWorkspace {
        let repo = root.join("repo");
        let tree = root.join("worktree");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-b", "fixture"]);
        std::fs::write(repo.join("result.txt"), "fixture\n").unwrap();
        git(&repo, &["add", "result.txt"]);
        git(
            &repo,
            &[
                "-c",
                "user.name=Assembly Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-m",
                "fixture",
            ],
        );
        let base_commit = git(&repo, &["rev-parse", "HEAD"]);
        git(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "assembly-work",
                tree.to_str().unwrap(),
                &base_commit,
            ],
        );
        let worktree = tree.canonicalize().unwrap().to_string_lossy().into_owned();
        ExecutionWorkspace {
            repo: repo.canonicalize().unwrap().to_string_lossy().into_owned(),
            base_commit,
            branch: "assembly-work".into(),
            write_roots: vec![worktree.clone()],
            claims: BTreeSet::from([format!("worktree:{worktree}")]),
            worktree,
        }
    }

    async fn open(root: &Path, config: Config, provider: Arc<ControlledProvider>) -> AppState {
        let state = AppState::new_with_provider_and_memory_store(
            root.to_path_buf(),
            config,
            provider.clone(),
            bamboo_memory::memory_store::MemoryStore::new(root.join("jiandu")),
        )
        .await
        .unwrap();
        state.provider_registry.replace_with(ProviderRegistry::new(
            HashMap::from([("openai".into(), provider as Arc<dyn LLMProvider>)]),
            "openai".into(),
        ));
        let config = state.config.read().await;
        assert!(config.subagents().broker.is_none());
        assert!(config.cluster_fabric.nodes.is_empty());
        drop(config);
        state
    }

    pub(super) struct Host {
        state: AppState,
        provider: Arc<ControlledProvider>,
        requests: mpsc::UnboundedReceiver<Gate>,
    }

    impl Host {
        pub(super) fn state(&self) -> &AppState {
            &self.state
        }

        pub(super) fn snapshot(&self) -> Snapshot {
            self.state.tickets.service().unwrap().published().unwrap().1
        }

        pub(super) fn calls(&self) -> usize {
            self.provider.calls.load(Ordering::SeqCst)
        }

        pub(super) fn policy(
            project: bamboo_domain::ProjectId,
            grant: ReadyWorkGrant,
        ) -> ReadyWorkPolicy {
            ReadyWorkPolicy {
                project_id: project,
                grants: vec![grant],
                capacity: 1,
                max_rounds: MAX_ROUNDS,
                lifetime: Duration::from_secs(60),
            }
        }

        pub(super) async fn new(root: &Path) -> Self {
            let (send, requests) = mpsc::unbounded_channel();
            let provider = Arc::new(ControlledProvider {
                requests: send,
                calls: AtomicUsize::new(0),
            });
            let mut config: Config = serde_json::from_value(json!({
                "provider":"openai",
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
            let state = open(root, config.clone(), provider.clone()).await;
            Self {
                state,
                provider,
                requests,
            }
        }

        pub(super) async fn seed_work(
            &self,
            root: &Path,
        ) -> (bamboo_domain::ProjectId, ReadyWorkGrant) {
            let state = &self.state;
            let workspace = workspace(root);
            let manifest = state
                .project_store
                .create("Patrol/Ready assembly", None)
                .unwrap();
            let manifest = state
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
            let owner = Principal::User {
                user_id: "assembly-host-owner".into(),
            };
            let (service, authority) = state.tickets.authority(owner.clone()).await.unwrap();
            let create = service
                .prepare_command(
                    &authority,
                    "assembly-create",
                    vec![
                        Operation::Create {
                            temp_id: "work".into(),
                            kind: TicketKind::Work,
                            parent: None,
                            contract: Contract {
                                title: "Patrol/Ready assembly".into(),
                                objective: OBJECTIVE.into(),
                                constraints: vec!["Use the controlled local provider".into()],
                                acceptance: vec!["Human checks the submitted result".into()],
                                user_acceptance_required: true,
                                allowed_tools: BTreeSet::from(["Task".into()]),
                            },
                            depends_on: BTreeSet::new(),
                        },
                        Operation::AssociateProject {
                            ticket_id: "work".into(),
                            project_id: manifest.id.clone(),
                        },
                    ],
                )
                .unwrap();
            let work_id = state
                .tickets
                .update(owner.clone(), &create)
                .await
                .unwrap()
                .ids["work"]
                .clone();
            let snapshot = service.published().unwrap().1;
            let work = &snapshot.tickets[&work_id];
            let ready = service
                .prepare_command(
                    &authority,
                    "assembly-ready",
                    vec![
                        Operation::SetProjectContext {
                            work_id: work_id.clone(),
                            context: ProjectContextSnapshot {
                                project_id: manifest.id.clone(),
                                project_revision: manifest.revision,
                                resource_revision: manifest.resource_revision,
                                workspace_binding: manifest
                                    .workspace_bindings
                                    .iter()
                                    .find(|binding| binding.path == workspace.repo)
                                    .unwrap()
                                    .clone(),
                                contract_revision: work.contract_revision,
                                contract: work.contract.clone(),
                                sources: BTreeMap::from([("assembly-spec".into(), "v1".into())]),
                            },
                        },
                        Operation::Ready {
                            work_id: work_id.clone(),
                        },
                    ],
                )
                .unwrap();
            state.tickets.update(owner, &ready).await.unwrap();
            let snapshot = service.published().unwrap().1;
            let grant = ReadyWorkGrant {
                work_id: work_id.clone(),
                contract_revision: snapshot.tickets[&work_id].contract_revision,
                context_hash: snapshot.project.work_contexts[&work_id].clone(),
                workspace,
            };
            (manifest.id, grant)
        }

        pub(super) async fn next_request(&mut self) -> Gate {
            tokio::time::timeout(WAIT, self.requests.recv())
                .await
                .expect("native Worker reaches controlled provider")
                .unwrap()
        }

        pub(super) async fn assert_quiet(&mut self, calls: usize) {
            assert!(
                tokio::time::timeout(Duration::from_millis(30), self.requests.recv())
                    .await
                    .is_err()
            );
            assert_eq!(self.provider.calls.load(Ordering::SeqCst), calls);
        }

        pub(super) async fn wait_submitted_and_stopped(&self, work_id: &str) -> Snapshot {
            tokio::time::timeout(WAIT, async {
                loop {
                    let snapshot = self.snapshot();
                    let work = &snapshot.tickets[work_id];
                    assert_ne!(work.state, WorkState::Accepted);
                    if let Some(submission) = work
                        .current_submission
                        .as_ref()
                        .and_then(|id| snapshot.submissions.get(id))
                    {
                        let assignment = &snapshot.assignments[&submission.assignment_id];
                        let child_id = &submission.runtime.session_id;
                        let child = self
                            .state()
                            .storage
                            .load_session(child_id)
                            .await
                            .unwrap()
                            .unwrap();
                        let stopped = child
                            .metadata
                            .get("runtime.local_task.state")
                            .map(String::as_str)
                            == Some("stopped");
                        let runner_done = self
                            .state()
                            .agent_runners
                            .read()
                            .await
                            .get(child_id)
                            .is_none_or(|r| !matches!(r.status, AgentStatus::Running));
                        if work.state == WorkState::Submitted
                            && assignment.process_stopped
                            && stopped
                            && runner_done
                            && self
                                .state()
                                .session_activation_router
                                .current_run_id(child_id)
                                .await
                                .is_none()
                        {
                            return snapshot;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("canonical submission and physical/logical runtime stop")
        }

        pub(super) async fn close(self) {
            // The process boundary, not this drop, breaks retained strong cycles.
            self.state.shutdown().await;
        }
    }
}

fn assert_unknown(observation: &Observation) {
    assert_eq!(observation.started_at_ms, None);
    assert_eq!(observation.heartbeat_at_ms, None);
    assert_eq!(observation.progress, None);
    assert_eq!(observation.expected_wait, None);
    assert!(!observation.terminal, "Submitted is not human acceptance");
}

async fn observe(
    app: &bamboo_server::app_state::ticket_application::TicketApplication,
    scope: &Scope,
    cursor: u64,
) -> Observation {
    let mut port = app.patrol_observation(scope).await.unwrap();
    let observation = port.observation(scope, cursor).unwrap();
    assert_unknown(&observation);
    assert_eq!(port.observation(scope, cursor), Err(PortError::Unavailable));
    observation
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

async fn ready_admission(
    store: &FileReadyRetryStore,
    key: &ReadyRetryKey,
    runtime: &RuntimeReceipt,
) -> VersionedReadyRetry {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            match store.load(key).await {
                Ok(Some(value))
                    if value.record.phase == ReadyRetryPhase::Deferred
                        && matches!(&value.record.request,
                        Some(ReadyRetryRequest::Intent { dispatch_key, spec_hash })
                            if dispatch_key == &runtime.dispatch_key
                                && spec_hash == &runtime.spec_hash) =>
                {
                    return value
                }
                Ok(_) | Err(bamboo_tickets::Error::ResourceBlocked(_)) => {}
                Err(error) => panic!("Ready admission journal unavailable: {error}"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the Ready admission's canonical receipt is durable")
}

const CHILD_TOKEN_ENV: &str = "BAMBOO_PATROL_READY_ASSEMBLY_CHILD_TOKEN";
const CHILD_ROOT_ENV: &str = "BAMBOO_PATROL_READY_ASSEMBLY_CHILD_ROOT";
const PARENT_MARKER: &str = "assembly-parent.json";
const SEED_RECEIPT: &str = "assembly-seed.json";
const TEST_NAME: &str =
    "ready_submission_and_patrol_checkpoint_reopen_under_the_same_canonical_host";

#[derive(Serialize, Deserialize)]
struct ParentMarker {
    token: String,
    parent_pid: u32,
}

/// Test-process observations, not a replacement authority for Ticket facts.
#[derive(Serialize, Deserialize)]
struct SeedReceipt {
    token: String,
    child_pid: u32,
    provider_calls: usize,
    binding: ScopeBinding,
    writer_epoch: u64,
    scope: Scope,
    runtime: RuntimeReceipt,
    ready: VersionedReadyRetry,
    checkpoint: StoredCheckpoint,
}

async fn seed(root: &Path, token: String) -> SeedReceipt {
    let mut host = support::Host::new(root).await;
    let (project, grant) = host.seed_work(root).await;
    let work_id = grant.work_id.clone();
    assert!(matches!(
        host.state().tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    ));
    assert!(host.snapshot().assignments.is_empty());
    host.assert_quiet(0).await;
    host.state()
        .tickets
        .install_ready_work_policy(support::Host::policy(project, grant))
        .await
        .unwrap();
    host.state().tickets.wake_ready_work();
    host.next_request().await.finish_task();
    host.next_request().await.finish_result();
    let before = host.wait_submitted_and_stopped(&work_id).await;
    assert_eq!(before.assignments.len(), 1);
    assert_eq!(before.intents.len(), 1);
    assert_eq!(before.submissions.len(), 1);
    let assignment = before.assignments.values().next().unwrap();
    let runtime = assignment.runtime.clone().unwrap();
    let spec = &before.intents[&assignment.dispatch_key].immutable_spec;
    assert!(before.tickets[&work_id].accepted_submission.is_none());
    assert!(assignment.process_stopped);
    host.assert_quiet(2).await;

    let key = ReadyRetryKey {
        scope_id: before.binding.scope_id.clone(),
        work_id: work_id.clone(),
    };
    let ready_store = FileReadyRetryStore::new(root);
    let ready = ready_admission(&ready_store, &key, &runtime).await;
    assert_eq!(ready.record.attempts, 1);
    assert!(ready.record.expires_at_ms > ready.record.created_at_ms);
    let scope = host
        .state()
        .tickets
        .patrol_scope(project_id(&before, &work_id), &work_id, &assignment.id)
        .await
        .unwrap();
    assert_eq!(scope.authority_epoch, spec.authority_epoch);
    let observation = observe(host.state().tickets.as_ref(), &scope, 0).await;
    let checked_at = now_ms(); // Host check time, never start or effective progress.
    let cp = Checkpoint {
        scope: scope.clone(),
        cursor: observation.cursor,
        source_revision: observation.source_revision,
        last_check_ms: checked_at,
        next_check_ms: checked_at + 1000,
        incident: Some(Incident {
            id: IncidentId {
                scope: scope.clone(),
                progress_sequence: 0,
            },
            reason: Reason::EvidenceUnknown,
            opened_at_ms: checked_at,
            opened_cursor: observation.cursor,
            source_revision: observation.source_revision,
            attempts: 0,
            last_diagnostic_cursor: observation.cursor,
            last_diagnostic_evidence_revision: None,
            last_diagnosis: None,
            pending_settlement: None,
            phase: Phase::Open,
            escalation: None,
        }),
    };
    let mut journal = host
        .state()
        .tickets
        .open_patrol_checkpoints(&scope)
        .await
        .unwrap();
    assert_eq!(journal.save(None, &cp), Ok(1));
    assert_eq!(journal.save(None, &cp), Err(PortError::Conflict));
    let checkpoint = journal.load(&scope).unwrap().unwrap();
    assert_eq!(checkpoint.checkpoint, cp);
    drop(journal);
    host.assert_quiet(2).await;
    assert_eq!(
        ready_store.load(&key).await.unwrap().unwrap(),
        ready,
        "Patrol cannot overwrite Ready's admission journal"
    );
    let receipt = SeedReceipt {
        token,
        child_pid: std::process::id(),
        provider_calls: host.calls(),
        binding: before.binding,
        writer_epoch: before.authority_epoch,
        scope,
        runtime,
        ready,
        checkpoint,
    };
    host.close().await;
    assert_eq!(
        ready_store.load(&key).await.unwrap().unwrap(),
        receipt.ready
    );
    receipt
}

fn project_id<'a>(snapshot: &'a bamboo_tickets::Snapshot, work_id: &str) -> &'a str {
    snapshot.project.links[work_id].as_str()
}

fn child_log(path: &Path) -> String {
    let bytes = std::fs::read(path).unwrap_or_default();
    String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(65_536)..]).into_owned()
}

async fn run_seed_process(root: &Path, token: &str) -> SeedReceipt {
    let stdout = root.join("seed.stdout.log");
    let stderr = root.join("seed.stderr.log");
    std::fs::create_dir(root.join("tmp")).unwrap();
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env_clear()
        .env(CHILD_TOKEN_ENV, token)
        .env(CHILD_ROOT_ENV, root)
        .env(
            "PATH",
            std::env::var_os("PATH").unwrap_or_else(|| "/usr/bin:/bin".into()),
        )
        .env("TMPDIR", root.join("tmp"))
        .env("BAMBOO_DATA_DIR", root)
        .env("BAMBOO_JIANDU_DATA_DIR", root.join("jiandu"))
        .env("RUST_MIN_STACK", "8388608")
        .env("RUST_BACKTRACE", "1")
        .env("LANG", "C")
        .stdout(std::fs::File::create(&stdout).unwrap())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .kill_on_drop(true);
    // Preserve only executable/loader paths required by Cargo's test binary;
    // provider keys, Host configuration and parent phase markers are not inherited.
    for name in [
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    let mut child = command.spawn().unwrap();
    let child_pid = child.id().unwrap();
    let status = match tokio::time::timeout(Duration::from_secs(120), child.wait()).await {
        Ok(status) => status.unwrap(),
        Err(_) => {
            // Only this owned Child handle is killed; no process scan or shared
            // cache cleanup. kill_on_drop remains a final cancellation guard.
            let _ = child.start_kill();
            let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
            panic!(
                "seed process timed out\nstdout:{}\nstderr:{}",
                child_log(&stdout),
                child_log(&stderr)
            );
        }
    };
    assert!(
        status.success(),
        "seed process {status}\nstdout:{}\nstderr:{}",
        child_log(&stdout),
        child_log(&stderr)
    );
    let receipt: SeedReceipt =
        serde_json::from_slice(&std::fs::read(root.join(SEED_RECEIPT)).unwrap()).unwrap();
    assert_eq!(receipt.token, token);
    assert_eq!(receipt.child_pid, child_pid);
    assert_ne!(receipt.child_pid, std::process::id());
    assert_eq!(receipt.provider_calls, 2);
    receipt
}

#[tokio::test]
async fn ready_submission_and_patrol_checkpoint_reopen_under_the_same_canonical_host() {
    if let Some(token) = std::env::var_os(CHILD_TOKEN_ENV) {
        let root = PathBuf::from(std::env::var_os(CHILD_ROOT_ENV).expect("parent-owned root"));
        assert!(root.is_absolute());
        assert_eq!(root.canonicalize().unwrap(), root);
        let metadata = std::fs::symlink_metadata(root.join(PARENT_MARKER)).unwrap();
        assert!(metadata.is_file() && !metadata.file_type().is_symlink());
        let marker: ParentMarker =
            serde_json::from_slice(&std::fs::read(root.join(PARENT_MARKER)).unwrap()).unwrap();
        let token = token.into_string().unwrap();
        assert_eq!(marker.token, token);
        assert_ne!(marker.parent_pid, std::process::id());
        let receipt = seed(&root, token).await;
        std::fs::write(
            root.join(SEED_RECEIPT),
            serde_json::to_vec(&receipt).unwrap(),
        )
        .unwrap();
        // No TempDir owner exists in this branch. Return through libtest and
        // exit normally; only the parent owns and eventually removes the root.
        return;
    }

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let token = uuid::Uuid::new_v4().to_string();
    let marker = ParentMarker {
        token: token.clone(),
        parent_pid: std::process::id(),
    };
    std::fs::write(
        root.join(PARENT_MARKER),
        serde_json::to_vec(&marker).unwrap(),
    )
    .unwrap();
    let seed = run_seed_process(&root, &token).await;
    // Successful wait above is the real OS ownership boundary. Never reopen
    // before it, delete a writer lock or substitute another root/epoch.
    let mut host = support::Host::new(&root).await;
    let after = host.snapshot();
    assert_eq!(after.binding, seed.binding);
    assert!(after.authority_epoch > seed.writer_epoch);
    assert_eq!(after.assignments.len(), 1);
    assert_eq!(after.intents.len(), 1);
    assert_eq!(after.submissions.len(), 1);
    let work_id = &seed.scope.work_id;
    let assignment_id = &seed.scope.assignment_id;
    assert_eq!(
        after.assignments[assignment_id].runtime.as_ref(),
        Some(&seed.runtime)
    );
    assert!(after.assignments[assignment_id].process_stopped);
    assert_eq!(after.tickets[work_id].state, WorkState::Submitted);
    assert!(after.tickets[work_id].accepted_submission.is_none());
    assert_eq!(
        after.intents[&seed.runtime.dispatch_key]
            .immutable_spec
            .authority_epoch,
        seed.scope.authority_epoch
    );
    for _ in 0..3 {
        host.state().tickets.wake_ready_work();
        assert!(matches!(
            host.state().tickets.reconcile_ready_work().await.unwrap(),
            ReadyReconcile::Disabled
        ));
    }
    host.assert_quiet(0).await;
    let key = ReadyRetryKey {
        scope_id: seed.binding.scope_id.clone(),
        work_id: work_id.clone(),
    };
    let ready_store = FileReadyRetryStore::new(&root);
    assert_eq!(
        ready_store.load(&key).await.unwrap().unwrap(),
        seed.ready,
        "restart retains the consumed attempt, original request, deadline and revision"
    );
    let recovered_scope = host
        .state()
        .tickets
        .patrol_scope(&seed.scope.project_id, work_id, assignment_id)
        .await
        .unwrap();
    assert_eq!(recovered_scope, seed.scope);
    assert!(recovered_scope.authority_epoch < after.authority_epoch);
    let fresh = observe(
        host.state().tickets.as_ref(),
        &seed.scope,
        seed.checkpoint.checkpoint.cursor,
    )
    .await;
    let mut journal = host
        .state()
        .tickets
        .open_patrol_checkpoints(&seed.scope)
        .await
        .unwrap();
    let recovered = journal.load(&seed.scope).unwrap().unwrap();
    assert_eq!(recovered, seed.checkpoint);
    assert_eq!(
        journal.save(None, &recovered.checkpoint),
        Err(PortError::Conflict)
    );
    let mut next = recovered.checkpoint.clone();
    next.cursor = fresh.cursor;
    next.source_revision = fresh.source_revision;
    next.last_check_ms = now_ms().max(next.last_check_ms);
    next.next_check_ms = next.last_check_ms + 1000;
    assert_eq!(journal.save(Some(recovered.version), &next), Ok(2));
    assert_eq!(
        journal.save(Some(recovered.version), &next),
        Err(PortError::Conflict)
    );
    assert_eq!(
        journal
            .load(&seed.scope)
            .unwrap()
            .unwrap()
            .checkpoint
            .incident,
        seed.checkpoint.checkpoint.incident,
        "reopen/CAS retain the original incident"
    );
    drop(journal);
    host.assert_quiet(0).await;
    let final_snapshot = host.snapshot();
    assert_eq!(final_snapshot.assignments.len(), 1);
    assert_eq!(final_snapshot.submissions.len(), 1);
    assert_eq!(final_snapshot.tickets[work_id].state, WorkState::Submitted);
    assert!(final_snapshot.tickets[work_id]
        .accepted_submission
        .is_none());
    host.close().await;
}
