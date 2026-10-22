//! #2062 same-process canonical Ticket writer lifecycle regression.
//!
//! Reuses a private copy of the assembly fixture's actual AppState, canonical
//! Project context, native Task tool, two gated provider responses, and complete
//! physical/logical stop predicate. No test/Host subprocess or lock bypass is
//! used. Git commands only create the disposable, real repository/worktree.
//! The same canonical root and the real writer-open epoch are used throughout.

use bamboo_orchestration::{
    patrol::{
        Checkpoint, CheckpointPort, Incident, IncidentId, ObservationPort, Phase, PortError,
        Reason, Scope,
    },
    ready_recovery::{
        ReadyRetryKey, ReadyRetryPhase, ReadyRetryRequest, ReadyRetryStore, VersionedReadyRetry,
    },
};
use bamboo_server::app_state::ticket_application::{FileReadyRetryStore, ReadyReconcile};
use bamboo_tickets::{canonical_bytes, Health, RuntimeReceipt, WorkState};
use std::{sync::Arc, time::Duration};

const WAIT: Duration = Duration::from_secs(20);

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

    async fn open(
        root: &Path,
        config: Config,
        provider: Arc<ControlledProvider>,
    ) -> Result<AppState, bamboo_server::AppError> {
        let state = AppState::new_with_provider_and_memory_store(
            root.to_path_buf(),
            config,
            provider.clone(),
            bamboo_memory::memory_store::MemoryStore::new(root.join("jiandu")),
        )
        .await?;
        state.provider_registry.replace_with(ProviderRegistry::new(
            HashMap::from([("openai".into(), provider as Arc<dyn LLMProvider>)]),
            "openai".into(),
        ));
        let config = state.config.read().await;
        assert!(config.subagents().broker.is_none());
        assert!(config.cluster_fabric.nodes.is_empty());
        drop(config);
        Ok(state)
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

        pub(super) async fn new(root: &Path) -> Result<Self, bamboo_server::AppError> {
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
            let state = open(root, config, provider.clone()).await?;
            Ok(Self {
                state,
                provider,
                requests,
            })
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

        pub(super) async fn shutdown_and_drop(self) {
            self.state.shutdown().await;
            drop(self);
        }
    }
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
    tokio::time::timeout(WAIT, async {
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
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the original native admission is durable before shutdown")
}

fn checkpoint(scope: Scope, cursor: u64, source_revision: u64) -> Checkpoint {
    let checked_at = now_ms();
    Checkpoint {
        scope: scope.clone(),
        cursor,
        source_revision,
        last_check_ms: checked_at,
        next_check_ms: checked_at + 1000,
        incident: Some(Incident {
            id: IncidentId {
                scope,
                progress_sequence: 0,
            },
            reason: Reason::EvidenceUnknown,
            opened_at_ms: checked_at,
            opened_cursor: cursor,
            source_revision,
            attempts: 0,
            last_diagnostic_cursor: cursor,
            last_diagnostic_evidence_revision: None,
            last_diagnosis: None,
            pending_settlement: None,
            pending_settlement_usage: None,
            phase: Phase::Open,
            escalation: None,
        }),
    }
}

#[tokio::test]
async fn stopped_native_child_shutdown_drop_reopens_the_same_writer_and_preserves_all_receipts() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let mut host = support::Host::new(&root).await.unwrap();
    let (project, grant) = host.seed_work(&root).await;
    let work_id = grant.work_id.clone();
    assert!(matches!(
        host.state().tickets.reconcile_ready_work().await.unwrap(),
        ReadyReconcile::Disabled
    ));
    host.assert_quiet(0).await;
    host.state()
        .tickets
        .install_ready_work_policy(support::Host::policy(project.clone(), grant))
        .await
        .unwrap();
    host.state().tickets.wake_ready_work();
    host.next_request().await.finish_task();
    host.next_request().await.finish_result();
    let before = host.wait_submitted_and_stopped(&work_id).await;
    assert_eq!(host.calls(), 2);
    assert_eq!(before.assignments.len(), 1);
    assert_eq!(before.intents.len(), 1);
    assert_eq!(before.submissions.len(), 1);
    assert_eq!(before.tickets[&work_id].state, WorkState::Submitted);
    assert!(before.tickets[&work_id].accepted_submission.is_none());
    let assignment = before.assignments.values().next().unwrap();
    assert!(assignment.process_stopped);
    let assignment_id = assignment.id.clone();
    let runtime = assignment.runtime.clone().unwrap();
    let dispatch_key = assignment.dispatch_key.clone();
    let original_spec = canonical_bytes(&before.intents[&dispatch_key].immutable_spec).unwrap();
    let original_assignments = canonical_bytes(&before.assignments).unwrap();
    let original_intents = canonical_bytes(&before.intents).unwrap();
    let original_submissions = canonical_bytes(&before.submissions).unwrap();
    let child_id = runtime.session_id.clone();
    let original_dispatch = {
        let child = host
            .state()
            .storage
            .load_session(&child_id)
            .await
            .unwrap()
            .unwrap();
        canonical_bytes(
            &bamboo_engine::ticket_runtime::read_dispatch(&child)
                .unwrap()
                .unwrap(),
        )
        .unwrap()
    };
    let key = ReadyRetryKey {
        scope_id: before.binding.scope_id.clone(),
        work_id: work_id.clone(),
    };
    // This wrapper acquires its file lock per operation; it retains no writer.
    let retry_store = FileReadyRetryStore::new(&root);
    let original_retry = ready_admission(&retry_store, &key, &runtime).await;
    assert_eq!(original_retry.record.attempts, 1);
    let scope = host
        .state()
        .tickets
        .patrol_scope(project.as_str(), &work_id, &assignment_id)
        .await
        .unwrap();
    assert_eq!(
        scope.authority_epoch,
        before.intents[&dispatch_key].immutable_spec.authority_epoch
    );
    let cp = {
        let mut observation_port = host
            .state()
            .tickets
            .patrol_observation(&scope)
            .await
            .unwrap();
        let observation = observation_port.observation(&scope, 0).unwrap();
        assert!(
            !observation.terminal,
            "Submitted does not confer acceptance"
        );
        assert_eq!(observation.started_at_ms, None);
        assert_eq!(observation.progress, None);
        checkpoint(
            scope.clone(),
            observation.cursor,
            observation.source_revision,
        )
    };
    let original_checkpoint = {
        let mut physical_writer = host
            .state()
            .tickets
            .open_patrol_checkpoints(&scope)
            .await
            .unwrap();
        assert_eq!(physical_writer.save(None, &cp), Ok(1));
        assert_eq!(physical_writer.save(None, &cp), Err(PortError::Conflict));
        let stored = physical_writer.load(&scope).unwrap().unwrap();
        assert_eq!(stored.checkpoint, cp);
        // The CP writer is explicitly dropped before shutdown; it is not the
        // Ticket writer whose physical lifecycle this test is measuring.
        drop(physical_writer);
        stored
    };
    host.assert_quiet(2).await;
    assert_eq!(
        retry_store.load(&key).await.unwrap().unwrap(),
        original_retry
    );
    let old_ticket_application = Arc::downgrade(&host.state().tickets);
    let old_service = {
        let service = host.state().tickets.service().unwrap();
        assert_eq!(service.health(), Health::Writable);
        Arc::downgrade(&service)
    };
    let writer_path = root
        .join("tickets")
        .join(before.binding.scope_id.strip_prefix("supervisor/").unwrap())
        .join("writer.lock");
    assert!(writer_path.is_file());
    host.shutdown_and_drop().await;
    // Only owned snapshots, stored values and Weak handles cross this boundary.
    // Do not assert the Weak result first: capture a REAL same-root reopen so
    // RED diagnoses the physical scope-lock failure as well as retained owners.
    let old_owners_after_drop = (
        old_ticket_application.strong_count(),
        old_service.strong_count(),
    );
    let reopened_result = tokio::time::timeout(WAIT, support::Host::new(&root)).await;
    let mut reopened = match reopened_result {
        Ok(Ok(host)) => host,
        Ok(Err(error)) => panic!(
            "same-root AppState reopen failed: {error}; old TicketApplication/service strong counts after shutdown+drop={old_owners_after_drop:?}"
        ),
        Err(_) => panic!(
            "same-root AppState reopen timed out; old TicketApplication/service strong counts after shutdown+drop={old_owners_after_drop:?}"
        ),
    };
    let actual_scope = reopened.state().tickets.service();
    let service = match actual_scope {
        Ok(service) => service,
        Err(error) => {
            let diagnostic = format!(
                "same-root AppState constructed but Ticket scope unavailable: {error}; old TicketApplication/service strong counts after shutdown+drop={old_owners_after_drop:?}; current old counts={:?}",
                (old_ticket_application.strong_count(), old_service.strong_count())
            );
            reopened.shutdown_and_drop().await;
            panic!("{diagnostic}");
        }
    };
    assert_eq!(service.health(), Health::Writable);
    let after = service.published().unwrap().1;
    drop(service); // Do not accidentally make the fixture a retained-owner case.
    assert!(
        old_ticket_application.upgrade().is_none(),
        "old TicketApplication retained after successful reopen: {old_owners_after_drop:?}"
    );
    assert!(
        old_service.upgrade().is_none(),
        "old Ticket writer retained after successful reopen: {old_owners_after_drop:?}"
    );
    assert!(
        writer_path.is_file(),
        "the existing lock file was never deleted"
    );
    assert_eq!(after.binding, before.binding);
    assert_eq!(
        after.authority_epoch,
        before.authority_epoch.checked_add(1).unwrap()
    );
    assert_eq!(
        canonical_bytes(&after.assignments).unwrap(),
        original_assignments
    );
    assert_eq!(canonical_bytes(&after.intents).unwrap(), original_intents);
    assert_eq!(
        canonical_bytes(&after.submissions).unwrap(),
        original_submissions
    );
    assert_eq!(
        canonical_bytes(&after.intents[&dispatch_key].immutable_spec).unwrap(),
        original_spec
    );
    assert_eq!(
        after.assignments[&assignment_id].runtime.as_ref(),
        Some(&runtime)
    );
    assert!(after.assignments[&assignment_id].process_stopped);
    assert_eq!(after.tickets[&work_id].state, WorkState::Submitted);
    assert!(after.tickets[&work_id].accepted_submission.is_none());
    for _ in 0..3 {
        reopened.state().tickets.wake_ready_work();
        assert!(matches!(
            reopened
                .state()
                .tickets
                .reconcile_ready_work()
                .await
                .unwrap(),
            ReadyReconcile::Disabled
        ));
    }
    reopened.assert_quiet(0).await;
    assert_eq!(
        retry_store.load(&key).await.unwrap().unwrap(),
        original_retry,
        "same-root reopen cannot reset consumed admission, request, deadline, or revision"
    );
    let recovered_scope = reopened
        .state()
        .tickets
        .patrol_scope(project.as_str(), &work_id, &assignment_id)
        .await
        .unwrap();
    assert_eq!(recovered_scope, scope);
    assert!(scope.authority_epoch < after.authority_epoch);
    let mut physical_writer = reopened
        .state()
        .tickets
        .open_patrol_checkpoints(&scope)
        .await
        .unwrap();
    assert_eq!(
        physical_writer.load(&scope).unwrap().unwrap(),
        original_checkpoint
    );
    assert_eq!(physical_writer.save(None, &cp), Err(PortError::Conflict));
    drop(physical_writer);
    let child = reopened
        .state()
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        canonical_bytes(
            &bamboo_engine::ticket_runtime::read_dispatch(&child)
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        original_dispatch
    );
    assert_eq!(
        child
            .metadata
            .get("runtime.local_task.state")
            .map(String::as_str),
        Some("stopped")
    );
    assert!(reopened
        .state()
        .session_activation_router
        .current_run_id(&child_id)
        .await
        .is_none());
    reopened.assert_quiet(0).await;
    let final_snapshot = reopened.snapshot();
    assert_eq!(
        canonical_bytes(&final_snapshot.assignments).unwrap(),
        original_assignments
    );
    assert_eq!(
        canonical_bytes(&final_snapshot.intents).unwrap(),
        original_intents
    );
    assert_eq!(
        canonical_bytes(&final_snapshot.submissions).unwrap(),
        original_submissions
    );
    assert_eq!(final_snapshot.tickets[&work_id].state, WorkState::Submitted);
    assert!(final_snapshot.tickets[&work_id]
        .accepted_submission
        .is_none());
    reopened.shutdown_and_drop().await;
}

#[tokio::test]
async fn explicitly_retained_ticket_service_blocks_reopen_until_the_owner_is_dropped() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let mut original = support::Host::new(&root).await.unwrap();
    let retained = original.state().tickets.service().unwrap();
    let (head, before) = retained.published().unwrap();
    let weak = Arc::downgrade(&retained);
    original.assert_quiet(0).await;
    original.shutdown_and_drop().await;
    let mut blocked = support::Host::new(&root).await.unwrap();
    let actual_scope = blocked.state().tickets.service();
    assert!(
        matches!(actual_scope,
        Err(bamboo_tickets::Error::AuthorityUnavailable(ref reason))
            if reason.contains("another local writer holds the process lock")),
        "an explicit Arc<TicketService> must keep its physical writer; no force-unlock"
    );
    let (still_head, still_snapshot) = retained.published().unwrap();
    assert_eq!(still_head, head);
    assert_eq!(still_snapshot.authority_epoch, before.authority_epoch);
    blocked.assert_quiet(0).await;
    blocked.shutdown_and_drop().await;
    assert!(weak.upgrade().is_some());
    drop(retained);
    // This second real reopen proves the retained handle, not a replacement
    // root, synthesized epoch, lock deletion, or force unlock, was the boundary.
    let mut reopened = support::Host::new(&root).await.unwrap();
    let service = reopened.state().tickets.service().unwrap();
    assert_eq!(service.health(), Health::Writable);
    let after = service.published().unwrap().1;
    drop(service);
    assert!(weak.upgrade().is_none());
    assert_eq!(after.binding, before.binding);
    assert_eq!(
        after.authority_epoch,
        before.authority_epoch.checked_add(1).unwrap()
    );
    assert!(after.assignments.is_empty());
    assert!(matches!(
        reopened
            .state()
            .tickets
            .reconcile_ready_work()
            .await
            .unwrap(),
        ReadyReconcile::Disabled
    ));
    reopened.assert_quiet(0).await;
    reopened.shutdown_and_drop().await;
}
