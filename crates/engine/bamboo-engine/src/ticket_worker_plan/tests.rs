use super::*;
use bamboo_domain::AgentRuntimeState;
use bamboo_tickets::{Contract, RuntimeReceipt, ScopeBinding, TicketKind};
use serde_json::json;
use std::collections::BTreeSet;

pub(crate) struct Fixture {
    pub service: Arc<TicketService>,
    pub assignment: String,
    pub work: String,
    pub supervisor: Authority,
    pub session: Session,
    _dir: tempfile::TempDir,
}

impl Fixture {
    pub fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
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
        let command = service
            .prepare_command(
                &supervisor,
                "create",
                vec![
                    Operation::Create {
                        temp_id: "work".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: Contract {
                            title: "Isolated fixture".into(),
                            objective: "Own steps only".into(),
                            constraints: vec!["Keep root private".into()],
                            acceptance: vec!["Evidence required".into()],
                            user_acceptance_required: true,
                            allowed_tools: BTreeSet::from(["Task".into()]),
                        },
                        depends_on: BTreeSet::new(),
                    },
                    Operation::Ready {
                        work_id: "work".into(),
                    },
                    Operation::Start {
                        work_id: "work".into(),
                        temp_id: "a".into(),
                        workspace: None,
                    },
                ],
            )
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
        }
    }

    pub fn plan(&self) -> TicketWorkerPlan {
        TicketWorkerPlan::from_runtime_receipt(self.service.clone(), &self.assignment).unwrap()
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
