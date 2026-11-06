use super::*;
use crate::session_app::child_session::*;
use bamboo_domain::AgentRuntimeState;
use bamboo_tickets::{Contract, RuntimeReceipt, ScopeBinding, TicketKind};
use serde_json::json;
use std::collections::BTreeSet;

struct CreationPort {
    parent: Session,
    saved: std::sync::Mutex<Option<Session>>,
    validated_workspace: Option<String>,
}

#[async_trait::async_trait]
impl ChildSessionPort for CreationPort {
    async fn validate_child_workspace(
        &self,
        _: Option<&bamboo_domain::ProjectId>,
        workspace: &str,
    ) -> std::result::Result<String, ChildSessionError> {
        Ok(self
            .validated_workspace
            .clone()
            .unwrap_or_else(|| workspace.into()))
    }
    async fn validate_required_child_context_route(
        &self,
        _: &std::collections::HashMap<String, String>,
        _: &str,
    ) -> std::result::Result<(), ChildSessionError> {
        Ok(())
    }
    async fn load_root_session(&self, id: &str) -> std::result::Result<Session, ChildSessionError> {
        assert_eq!(id, self.parent.id);
        Ok(self.parent.clone())
    }
    async fn load_child_for_parent(
        &self,
        _: &str,
        id: &str,
    ) -> std::result::Result<Session, ChildSessionError> {
        Err(ChildSessionError::NotFound(id.into()))
    }
    async fn save_child_session(
        &self,
        child: &mut Session,
    ) -> std::result::Result<(), ChildSessionError> {
        *self.saved.lock().unwrap() = Some(child.clone());
        Ok(())
    }
    async fn save_child_session_authoritative_flags(
        &self,
        _: &mut Session,
    ) -> std::result::Result<(), ChildSessionError> {
        panic!("no resident reuse")
    }
    async fn is_child_running(&self, _: &str) -> bool {
        false
    }
    async fn list_children(&self, _: &str) -> Vec<ChildSessionEntry> {
        vec![]
    }
    async fn enqueue_child_run(
        &self,
        _: &Session,
        _: &Session,
    ) -> std::result::Result<(), ChildSessionError> {
        panic!("creation must not dispatch")
    }
    async fn cancel_child_run_and_wait(
        &self,
        _: &str,
    ) -> std::result::Result<(), ChildSessionError> {
        panic!("no cancel")
    }
    async fn delete_child_session(
        &self,
        _: &str,
        _: &str,
    ) -> std::result::Result<DeleteChildResult, ChildSessionError> {
        panic!("no delete")
    }
    async fn get_child_runner_info(&self, _: &str) -> Option<ChildRunnerInfo> {
        None
    }
    async fn register_parent_wait_for_child(
        &self,
        _: &str,
        _: &str,
        _: Option<&str>,
    ) -> std::result::Result<(), ChildSessionError> {
        panic!("no parent wait")
    }
    async fn register_parent_wait_for_children(
        &self,
        _: &str,
        _: &[String],
        _: bamboo_domain::ChildWaitPolicy,
    ) -> std::result::Result<usize, ChildSessionError> {
        panic!("no parent wait")
    }
    async fn active_child_ids(&self, _: &str) -> Vec<String> {
        vec![]
    }
    async fn find_resident_child(&self, _: &str, _: &str) -> Option<String> {
        None
    }
    async fn ensure_child_indexed(&self, _: &str) {}
}

pub(crate) struct Fixture {
    pub service: Arc<TicketService>,
    pub assignment: String,
    pub work: String,
    pub supervisor: Authority,
    pub session: Session,
    _dir: tempfile::TempDir,
    _source: Option<tempfile::TempDir>,
}

impl Fixture {
    pub fn new() -> Self {
        Self::with_objective("Own steps only", false)
    }

    fn with_objective(objective: &str, with_project: bool) -> Self {
        Self::with_workspace(objective, with_project, with_project)
    }

    fn with_workspace(objective: &str, with_project: bool, with_workspace: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let source = with_workspace.then(|| tempfile::tempdir().unwrap());
        let source_path = source.as_ref().map(|dir| {
            dir.path()
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        });
        let binding = ScopeBinding {
            scope_id: "fixture-scope".into(),
            supervisor_session_id: "fixture-supervisor".into(),
            binding_revision: 1,
        };
        let service = Arc::new(TicketService::open(dir.path(), binding.clone()).unwrap());
        let supervisor = Authority::from_verified_host(
            binding.clone(),
            Principal::Supervisor {
                session_id: binding.supervisor_session_id.clone(),
            },
        );
        let contract = Contract {
            title: "Isolated fixture".into(),
            objective: objective.into(),
            constraints: vec!["Keep root private".into()],
            acceptance: vec!["Evidence required".into()],
            user_acceptance_required: true,
            allowed_tools: BTreeSet::from(["Task".into()]),
        };
        let mut operations = vec![Operation::Create {
            temp_id: "work".into(),
            kind: TicketKind::Work,
            parent: None,
            contract: contract.clone(),
            depends_on: BTreeSet::new(),
        }];
        if with_project {
            let project_id: bamboo_domain::ProjectId =
                "00000000-0000-4000-8000-000000000548".parse().unwrap();
            operations.extend([
                Operation::AssociateProject {
                    ticket_id: "work".into(),
                    project_id: project_id.clone(),
                },
                Operation::SetProjectContext {
                    work_id: "work".into(),
                    context: bamboo_tickets::ProjectContextSnapshot {
                        project_id,
                        project_revision: 1,
                        resource_revision: 1,
                        workspace_binding: bamboo_domain::WorkspaceBinding {
                            path: source_path.clone().unwrap(),
                            label: Some("Accepted source audit".into()),
                            git_common_dir: None,
                        },
                        contract_revision: 1,
                        contract,
                        sources: std::collections::BTreeMap::from([(
                            "accepted-source".into(),
                            "f9e69b9fbefdc0b5249c3dbe4cd1ef33739f19f7".into(),
                        )]),
                    },
                },
            ]);
        }
        operations.extend([
            Operation::Ready {
                work_id: "work".into(),
            },
            Operation::Start {
                work_id: "work".into(),
                temp_id: "a".into(),
                // Metadata-only adapter fixture; physical Git/Host admission
                // belongs to the native Work integration tests.
                workspace: source_path
                    .as_ref()
                    .map(|_| bamboo_tickets::ExecutionWorkspace {
                        repo: source_path.clone().unwrap(),
                        base_commit: "f9e69b9fbefdc0b5249c3dbe4cd1ef33739f19f7".into(),
                        branch: "source-audit".into(),
                        worktree: source_path.clone().unwrap(),
                        write_roots: vec![source_path.clone().unwrap()],
                        claims: BTreeSet::from([format!(
                            "worktree:{}",
                            source_path.as_ref().unwrap()
                        )]),
                    }),
            },
        ]);
        let command = service
            .prepare_command(&supervisor, "create", operations)
            .unwrap();
        let receipt = service.execute(&supervisor, &command).unwrap();
        let work = receipt.ids["work"].clone();
        let assignment = receipt.ids["a"].clone();
        let snapshot = service.published().unwrap().1;
        let a = &snapshot.assignments[&assignment];
        // Synthetic receipt: this is a deterministic adapter test, not Runtime admission evidence.
        let runtime = Authority::from_verified_host(binding, Principal::Runtime);
        let receipt = RuntimeReceipt {
            dispatch_key: a.dispatch_key.clone(),
            spec_hash: snapshot.intents[&a.dispatch_key].spec_hash.clone(),
            run_id: "fixture-run".into(),
            session_id: "fixture-child".into(),
        };
        let cmd = service
            .prepare_command(
                &runtime,
                "admit",
                vec![
                    Operation::Admitted {
                        assignment_id: assignment.clone(),
                        receipt,
                    },
                    Operation::Running {
                        assignment_id: assignment.clone(),
                    },
                ],
            )
            .unwrap();
        service.execute(&runtime, &cmd).unwrap();
        let mut session = Session::new_child(
            "fixture-child",
            "fixture-supervisor",
            "fixture-model",
            "fixture",
        );
        session.agent_runtime_state = Some(AgentRuntimeState::new("fixture-run"));
        Self {
            service,
            assignment,
            work,
            supervisor,
            session,
            _dir: dir,
            _source: source,
        }
    }

    pub fn plan(&self) -> TicketWorkerPlan {
        TicketWorkerPlan::from_runtime_receipt(self.service.clone(), &self.assignment).unwrap()
    }
}

#[tokio::test]
async fn ticket_child_creation_requires_host_packet_and_starts_with_no_parent_plan() {
    let f = Fixture::new();
    let workspace = tempfile::tempdir().unwrap();
    let mut parent = Session::new("fixture-supervisor", "fixture-model");
    parent.task_list = Some(
        TaskTool::task_list_from_args(
            &json!({"tasks":[{"content":"Parent secret","status":"pending"}]}),
            &parent.id,
        )
        .unwrap(),
    );
    parent
        .messages
        .push(bamboo_domain::Message::user("Unrelated parent history"));
    let port = CreationPort {
        parent: parent.clone(),
        saved: std::sync::Mutex::new(None),
        validated_workspace: None,
    };
    let input = CreateChildInput {
        parent_session: parent,
        child_id: "fixture-child".into(),
        title: "Own work".into(),
        responsibility: "Own steps".into(),
        assignment_prompt: "placeholder".into(),
        subagent_type: "worker".into(),
        workspace: workspace.path().to_string_lossy().into(),
        workspace_source: crate::project_context::WorkspaceSource::Explicit,
        model_override: None,
        model_ref_override: None,
        runtime_metadata: Default::default(),
        read_only: false,
        auto_run: false,
        reasoning_effort: None,
        lifecycle: None,
        resident_name: None,
        resident_context: None,
        disabled_tools: None,
        context_fork: None,
    };
    let runtime =
        Authority::from_verified_host(f.service.published().unwrap().1.binding, Principal::Runtime);
    create_ticket_child_action(&port, input.clone(), &f.service, &runtime, &f.assignment)
        .await
        .unwrap();
    let child = port.saved.lock().unwrap().clone().unwrap();
    assert!(child.task_list.is_none());
    assert_eq!(
        child.metadata.get(TICKET_LOCAL_PLAN_KEY),
        Some(&f.assignment)
    );
    assert!(bamboo_domain::ChildContextBinding::from_session(&child)
        .unwrap()
        .is_some());
    assert!(child
        .messages
        .iter()
        .all(|m| !m.content.contains("Parent secret")
            && !m.content.contains("Unrelated parent history")));
    assert_eq!(
        child
            .messages
            .iter()
            .filter(|m| m
                .metadata
                .as_ref()
                .is_some_and(|v| v.get(TICKET_PLAN_PACKET_KEY).is_some()))
            .count(),
        1
    );
    let mut forbidden = input.clone();
    forbidden.auto_run = true;
    assert!(
        create_ticket_child_action(&port, forbidden, &f.service, &runtime, &f.assignment)
            .await
            .is_err()
    );
    let mut forged = input;
    forged
        .runtime_metadata
        .insert(TICKET_LOCAL_PLAN_KEY.into(), f.assignment.clone());
    assert!(create_child_action(&port, forged).await.is_err());
}

#[tokio::test]
async fn ticket_child_admits_multiline_packet_within_existing_required_byte_bounds() {
    assert_bounded_ticket_child_packet(24, false, false).await;
}

#[tokio::test]
async fn ticket_child_admits_project_packet_without_losing_adopted_context() {
    assert_bounded_ticket_child_packet(9, true, false).await;
}

#[tokio::test]
async fn ticket_child_accepts_same_physical_workspace_from_host_display_validation() {
    for with_project in [false, true] {
        assert_bounded_ticket_child_packet(24, with_project, true).await;
    }
}

async fn assert_bounded_ticket_child_packet(
    repetitions: usize,
    with_project: bool,
    display_validation: bool,
) {
    let objective =
        "Review the accepted source; cite exact lines and preserve evidence boundaries. "
            .repeat(repetitions);
    assert!(objective.len() < 2048);
    let f = Fixture::with_workspace(&objective, with_project, with_project || display_validation);
    let workspace = tempfile::tempdir().unwrap();
    let parent = Session::new("fixture-supervisor", "fixture-model");
    let runtime =
        Authority::from_verified_host(f.service.published().unwrap().1.binding, Principal::Runtime);
    let packet = f
        .service
        .child_context_packet(&runtime, &f.assignment, 65536)
        .unwrap();
    let validated_workspace = display_validation.then(|| {
        let canonical = std::path::Path::new(&packet.workspace.as_ref().unwrap().worktree);
        let display =
            std::path::PathBuf::from(bamboo_config::paths::path_to_display_string(canonical));
        // The Host returns display forms, including non-verbatim paths on
        // Windows. Exercise a distinct spelling on every local platform too.
        let nested = display.join("display-form");
        std::fs::create_dir(&nested).unwrap();
        let alias = nested.join("..");
        assert_ne!(alias, canonical);
        assert_eq!(alias.canonicalize().unwrap(), canonical);
        alias.to_string_lossy().into_owned()
    });
    let port = CreationPort {
        parent: parent.clone(),
        saved: std::sync::Mutex::new(None),
        validated_workspace,
    };
    let compact = serde_json::to_string(&packet).unwrap();
    assert!(
        compact.len() > 2048,
        "exercise serialization-induced line overflow: {} bytes",
        compact.len()
    );
    eprintln!(
        "objective={} compact_packet={} with_project={with_project}",
        objective.len(),
        compact.len()
    );
    let input = CreateChildInput {
        parent_session: parent,
        child_id: "fixture-child".into(),
        title: "Source audit".into(),
        responsibility: objective.clone(),
        assignment_prompt: String::new(),
        subagent_type: "worker".into(),
        workspace: packet.workspace.as_ref().map_or_else(
            || workspace.path().to_string_lossy().into_owned(),
            |workspace| workspace.worktree.clone(),
        ),
        workspace_source: crate::project_context::WorkspaceSource::Explicit,
        model_override: None,
        model_ref_override: None,
        runtime_metadata: Default::default(),
        read_only: false,
        auto_run: false,
        reasoning_effort: None,
        lifecycle: None,
        resident_name: None,
        resident_context: None,
        disabled_tools: None,
        context_fork: None,
    };
    create_ticket_child_action(&port, input.clone(), &f.service, &runtime, &f.assignment)
        .await
        .expect("a bounded Work must not fail because its JSON was serialized on one line");
    let child = port.saved.lock().unwrap().clone().unwrap();
    let binding = bamboo_domain::ChildContextBinding::from_session(&child)
        .unwrap()
        .unwrap();
    let brief: serde_json::Value =
        serde_json::from_str(&child.metadata["assignment_prompt"]).unwrap();
    assert_eq!(brief, serde_json::to_value(&packet).unwrap());
    assert!(binding.payload.required_assignment.contains(&objective));
    assert!(binding.payload.required_assignment.len() <= bamboo_domain::MAX_CHILD_ASSIGNMENT_BYTES);
    assert_eq!(child.metadata[TICKET_LOCAL_PLAN_KEY], f.assignment);
    assert!(child.task_list.is_none());
    assert_eq!(child.project_id_meta(), port.parent.project_id_meta());
    if display_validation {
        let other = tempfile::tempdir().unwrap();
        let relocated = CreationPort {
            parent: port.parent.clone(),
            saved: std::sync::Mutex::new(None),
            validated_workspace: Some(bamboo_config::paths::path_to_display_string(other.path())),
        };
        assert!(
            create_ticket_child_action(&relocated, input, &f.service, &runtime, &f.assignment)
                .await
                .is_err()
        );
        assert!(relocated.saved.lock().unwrap().is_none());
    }
}

#[test]
fn own_steps_empty_initial_plan_replay_does_not_roll_back_newer_projection() {
    let mut f = Fixture::new();
    f.session.task_list = Some(
        TaskTool::task_list_from_args(
            &json!({"tasks":[{"content":"Parent secret","status":"pending"}]}),
            "root",
        )
        .unwrap(),
    );
    let plan = f.plan();
    plan.bind_session(&mut f.session).unwrap();
    assert!(f.session.task_list.is_none());
    let first = json!({"tasks":[{"id":"own","content":"Own step","status":"in_progress"}]});
    plan.apply_task(&mut f.session, "call-1", &first).unwrap();
    let seq = f.service.published().unwrap().1.seq;
    plan.apply_task(&mut f.session, "call-1", &first).unwrap();
    assert_eq!(f.service.published().unwrap().1.seq, seq);
    let next = json!({"tasks":[{"id":"own","content":"Own step","status":"completed"}]});
    assert!(matches!(
        plan.apply_task(&mut f.session, "call-1", &next),
        Err(Error::IdempotencyConflict)
    ));
    plan.apply_task(&mut f.session, "call-2", &next).unwrap();
    plan.apply_task(&mut f.session, "call-1", &first).unwrap();
    assert_eq!(
        f.session.task_list.as_ref().unwrap().items[0].status,
        TaskItemStatus::Completed
    );
    let a = &f.service.published().unwrap().1.assignments[&f.assignment];
    assert_eq!(a.plan.plan_revision, 2);
    assert_eq!(a.plan.steps[0].status, Some(StepStatus::Completed));
    assert_eq!(f.session.task_list_version_meta().as_deref(), Some("2"));
}

#[test]
fn generated_ids_replay_original_input_after_rename_and_writer_restart() {
    let mut f = Fixture::new();
    let plan = f.plan();
    plan.bind_session(&mut f.session).unwrap();
    let first = json!({"tasks":[{"content":"Initial step","status":"pending"}]});
    let list = plan
        .apply_task(&mut f.session, "generated", &first)
        .unwrap();
    let id = list.items[0].id.clone();
    let snapshot = f.service.published().unwrap().1;
    let op_id = format!(
        "worker-plan/{}/generated",
        snapshot.assignments[&f.assignment].dispatch_key
    );
    let original_receipt = snapshot.receipts[&op_id].clone();
    plan.apply_task(
        &mut f.session,
        "rename",
        &json!({"tasks":[{"id":id,"content":"Renamed step","status":"completed"}]}),
    )
    .unwrap();
    let seq = f.service.published().unwrap().1.seq;
    plan.apply_task(&mut f.session, "generated", &first)
        .unwrap();
    assert_eq!(f.service.published().unwrap().1.seq, seq);
    assert_eq!(
        f.session.task_list.as_ref().unwrap().items[0].description,
        "Renamed step"
    );
    assert_eq!(
        f.session.task_list.as_ref().unwrap().items[0].status,
        TaskItemStatus::Completed
    );
    let changed =
        json!({"tasks":[{"content":"Initial step","status":"pending","phase":"execution"}]});
    assert!(matches!(
        plan.apply_task(&mut f.session, "generated", &changed),
        Err(Error::IdempotencyConflict)
    ));
    // A late concurrent adapter may have already generated different steps.
    // The original source input, rather than that second projection, owns replay.
    let source = CommandSource::WorkerTask { arguments: first };
    let original = f
        .service
        .prepare_source_command(&plan.authority, &op_id, vec![], source.clone())
        .unwrap();
    assert_eq!(
        f.service
            .execute(&plan.authority, &original)
            .unwrap()
            .committed_seq,
        original_receipt.committed_seq
    );
    let authority = plan.authority.clone();
    let binding = snapshot.binding;
    drop(plan);
    drop(f.service);
    let restarted = Arc::new(TicketService::open(f._dir.path(), binding).unwrap());
    let epoch_seq = restarted.published().unwrap().1.seq;
    let original = restarted
        .replay_source_command(&authority, &op_id, &source)
        .unwrap()
        .unwrap();
    assert_eq!(
        restarted
            .execute(&authority, &original)
            .unwrap()
            .request_hash,
        original_receipt.request_hash
    );
    assert_eq!(restarted.published().unwrap().1.seq, epoch_seq);
    // Immutable receipt lookup grants no renewed execution permission.
    assert!(restarted.authorize_tool(&authority, "Task").is_err());
    assert!(TicketWorkerPlan::from_runtime_receipt(restarted, &f.assignment).is_err());
}

#[test]
fn exact_child_run_and_live_generation_are_required() {
    let mut f = Fixture::new();
    let plan = f.plan();
    let mut root = Session::new("fixture-child", "model");
    root.agent_runtime_state = Some(AgentRuntimeState::new("fixture-run"));
    assert!(plan.bind_session(&mut root).is_err());
    plan.bind_session(&mut f.session).unwrap();
    let args = json!({"tasks":[{"content":"Own step","status":"pending"}]});
    let mut sibling = f.session.clone();
    sibling.id = "sibling".into();
    assert!(plan.apply_task(&mut sibling, "call", &args).is_err());
    let mut wrong_run = f.session.clone();
    wrong_run.agent_runtime_state = Some(AgentRuntimeState::new("forged-run"));
    assert!(plan.apply_task(&mut wrong_run, "call", &args).is_err());
    let mut wrong_parent = f.session.clone();
    wrong_parent.parent_session_id = Some("another-supervisor".into());
    assert!(plan.bind_session(&mut wrong_parent).is_err());
    assert!(plan.apply_task(&mut wrong_parent, "call", &args).is_err());
    let cmd = f
        .service
        .prepare_command(
            &f.supervisor,
            "cancel",
            vec![Operation::Cancel {
                work_id: f.work.clone(),
            }],
        )
        .unwrap();
    f.service.execute(&f.supervisor, &cmd).unwrap();
    assert!(plan.apply_task(&mut f.session, "call", &args).is_err());
    assert!(f.session.task_list.is_none());
    assert!(f.service.published().unwrap().1.assignments[&f.assignment]
        .plan
        .steps
        .is_empty());
}

#[tokio::test]
async fn native_bridge_writes_only_host_bound_plan_and_rejects_other_run() {
    use super::remote::{apply_host_plan_request, RemoteWorkerPlan};
    let mut f = Fixture::new();
    f.plan().bind_session(&mut f.session).unwrap();
    let runtime =
        Authority::from_verified_host(f.service.published().unwrap().1.binding, Principal::Runtime);
    let packet = f
        .service
        .child_context_packet(&runtime, &f.assignment, 65536)
        .unwrap();
    let (bridge, mut requests) = bamboo_subagent::executor::HostBridge::channel();
    let service = f.service.clone();
    let caller = f.session.clone();
    let pump = tokio::spawn(async move {
        let mut pending_once = true;
        while let Some(request) = requests.recv().await {
            assert_eq!(
                request.kind,
                bamboo_subagent::executor::HostRequestKind::SubAgent
            );
            let id = request.body["tool_call_id"].as_str().unwrap();
            if pending_once {
                assert_eq!(id, "ticket-plan-bootstrap");
                assert_eq!(
                    request.body["args"],
                    json!({(TICKET_PLAN_ACTION):{"read":true}})
                );
                pending_once = false;
                request
                    .reply
                    .send(json!({"error":super::remote::TICKET_BOOTSTRAP_POSTURE_PENDING}))
                    .unwrap();
                continue;
            }
            let result = apply_host_plan_request(
                &service,
                &caller,
                "fixture-run",
                &request.body["args"],
                id,
            );
            let reply = match result {
                Ok(projection) => json!({"result":projection}),
                Err(error) => json!({"error":error.to_string()}),
            };
            let _ = request.reply.send(reply);
        }
    });
    let mut forged_packet = packet.clone();
    forged_packet.contract.constraints.clear();
    assert!(
        RemoteWorkerPlan::from_host_packet(
            bridge.clone(),
            forged_packet,
            f.session.id.clone(),
            "fixture-run".into(),
        )
        .await
        .is_err(),
        "unchanged revision numbers cannot authorize modified constraints"
    );
    let remote = RemoteWorkerPlan::from_host_packet(
        bridge,
        packet,
        f.session.id.clone(),
        "fixture-run".into(),
    )
    .await
    .unwrap();
    WorkerLocalPlan::bind_session(&remote, &mut f.session).unwrap();
    let args = json!({"tasks":[{"id":"native","content":"Native own step","status":"blocked"}]});
    WorkerLocalPlan::apply_task(&remote, &mut f.session, "native-call", &args)
        .await
        .unwrap();
    WorkerLocalPlan::apply_task(&remote, &mut f.session, "native-call", &args)
        .await
        .unwrap();
    assert_eq!(
        f.service.published().unwrap().1.assignments[&f.assignment]
            .plan
            .plan_revision,
        1
    );
    assert_eq!(
        f.session.task_list.as_ref().unwrap().items[0].status,
        TaskItemStatus::Blocked
    );
    let seq = f.service.published().unwrap().1.seq;
    let oversized = json!({"tasks":[{"content":"X".repeat(16 * 1024),"status":"pending"}]});
    assert!(
        WorkerLocalPlan::apply_task(&remote, &mut f.session, "too-large", &oversized)
            .await
            .is_err()
    );
    assert_eq!(f.service.published().unwrap().1.seq, seq);
    assert!(apply_host_plan_request(
        &f.service,
        &f.session,
        "forged-run",
        &json!({(TICKET_PLAN_ACTION):{"read":true}}),
        "bad"
    )
    .is_err());
    let mut sibling = f.session.clone();
    sibling.id = "sibling".into();
    assert!(apply_host_plan_request(
        &f.service,
        &sibling,
        "fixture-run",
        &json!({(TICKET_PLAN_ACTION):{"read":true}}),
        "bad"
    )
    .is_err());
    let cmd = f
        .service
        .prepare_command(
            &f.supervisor,
            "native-cancel",
            vec![Operation::Cancel {
                work_id: f.work.clone(),
            }],
        )
        .unwrap();
    f.service.execute(&f.supervisor, &cmd).unwrap();
    assert!(
        WorkerLocalPlan::apply_task(&remote, &mut f.session, "late-call", &args)
            .await
            .is_err()
    );
    drop(remote);
    pump.await.unwrap();
}

#[test]
fn native_question_callback_is_atomic_exact_replay_and_rejects_json_approval() {
    let mut f = Fixture::new();
    let plan = f.plan();
    plan.bind_session(&mut f.session).unwrap();
    let args = json!({"tasks":[{"id":"own","content":"Own blocked step","status":"blocked"}],"question":{"prompt":"Own precise question"}});
    let first = remote::apply_host_plan_request(
        &f.service,
        &f.session,
        "fixture-run",
        &json!({(TICKET_PLAN_ACTION):{"task":args}}),
        "question-call",
    )
    .unwrap();
    let question = first.question.unwrap();
    assert_eq!(
        question.assignment_id.as_deref(),
        Some(f.assignment.as_str())
    );
    assert_eq!(question.kind, bamboo_tickets::RequestKind::Question);
    let seq = f.service.published().unwrap().1.seq;
    let replay = remote::apply_host_plan_request(
        &f.service,
        &f.session,
        "fixture-run",
        &json!({(TICKET_PLAN_ACTION):{"task":args}}),
        "question-call",
    )
    .unwrap();
    assert_eq!(replay.question.unwrap().id, question.id);
    assert_eq!(f.service.published().unwrap().1.seq, seq);
    assert!(remote::apply_host_plan_request(&f.service, &f.session, "fixture-run", &json!({(TICKET_PLAN_ACTION):{"task":{"tasks":[{"content":"Any","status":"blocked"}],"question":{"prompt":"Own","approved":true}}}}), "forged-question").is_err());
    assert!(f.service.published().unwrap().1.submissions.is_empty());
}
