use bamboo_tickets::*;
use std::collections::BTreeSet;

fn fixture() -> (tempfile::TempDir, TicketService, Authority) {
    let dir = tempfile::tempdir().unwrap();
    let binding = ScopeBinding {
        scope_id: "validation".into(),
        supervisor_session_id: "root".into(),
        binding_revision: 1,
    };
    let service = TicketService::open(dir.path().join("tickets"), binding.clone()).unwrap();
    let authority = Authority::from_verified_host(
        binding,
        Principal::Supervisor {
            session_id: "root".into(),
        },
    );
    (dir, service, authority)
}
fn contract() -> Contract {
    Contract {
        title: "bounded".into(),
        objective: "deliver".into(),
        constraints: vec![],
        acceptance: vec!["evidence".into()],
        user_acceptance_required: true,
        allowed_tools: BTreeSet::from(["Task".into()]),
    }
}
fn create(contract: Contract) -> Vec<Operation> {
    vec![
        Operation::Create {
            temp_id: "w".into(),
            kind: TicketKind::Work,
            parent: None,
            contract,
            depends_on: BTreeSet::new(),
        },
        Operation::Ready {
            work_id: "w".into(),
        },
    ]
}
fn execute(
    service: &TicketService,
    authority: &Authority,
    id: &str,
    operations: Vec<Operation>,
) -> Result<OperationReceipt> {
    let command = service.prepare_command(authority, id, operations)?;
    service.execute(authority, &command)
}

#[test]
fn contract_rejects_unusable_tool_ceilings_and_oversize_before_publication() {
    let (_dir, service, authority) = fixture();
    for tools in [
        vec![],
        vec!["Read"],
        vec!["Task", "Shell"],
        vec!["Task", "task"],
    ] {
        let mut bad = contract();
        bad.allowed_tools = tools.iter().map(|v| (*v).into()).collect();
        let before = service.published().unwrap();
        assert!(
            execute(&service, &authority, "bad-tools", create(bad)).is_err(),
            "{tools:?}"
        );
        assert_eq!(
            canonical_bytes(&service.published().unwrap()).unwrap(),
            canonical_bytes(&before).unwrap()
        );
    }
    let mut huge = contract();
    huge.objective = "x".repeat(65536);
    let before = service.published().unwrap();
    assert!(matches!(
        execute(&service, &authority, "huge-create", create(huge.clone())),
        Err(Error::ContextBudgetExceeded)
    ));
    assert_eq!(
        canonical_bytes(&service.published().unwrap()).unwrap(),
        canonical_bytes(&before).unwrap()
    );
    let id = execute(&service, &authority, "valid-create", create(contract()))
        .unwrap()
        .ids["w"]
        .clone();
    let before = service.published().unwrap();
    assert!(execute(
        &service,
        &authority,
        "huge-steer",
        vec![Operation::UpdateContract {
            work_id: id,
            contract: huge
        }]
    )
    .is_err());
    assert_eq!(
        canonical_bytes(&service.published().unwrap()).unwrap(),
        canonical_bytes(&before).unwrap()
    );
}

#[test]
fn start_checks_complete_context_budget_before_creating_assignment_or_intent() {
    let (_dir, service, authority) = fixture();
    let mut large = contract();
    large.objective =
        "x".repeat(65500 - canonical_bytes(&large).unwrap().len() + large.objective.len());
    assert_eq!(canonical_bytes(&large).unwrap().len(), 65500);
    let work = execute(&service, &authority, "large-create", create(large))
        .unwrap()
        .ids["w"]
        .clone();
    let before = service.published().unwrap();
    assert!(matches!(
        execute(
            &service,
            &authority,
            "large-start",
            vec![Operation::Start {
                work_id: work,
                temp_id: "a".into(),
                workspace: None
            }]
        ),
        Err(Error::ContextBudgetExceeded)
    ));
    assert_eq!(
        canonical_bytes(&service.published().unwrap()).unwrap(),
        canonical_bytes(&before).unwrap()
    );
    let mut bounded = contract();
    bounded.objective = "x".repeat(60000);
    let work = execute(&service, &authority, "bounded-create", create(bounded))
        .unwrap()
        .ids["w"]
        .clone();
    let assignment = execute(
        &service,
        &authority,
        "bounded-start",
        vec![Operation::Start {
            work_id: work,
            temp_id: "a".into(),
            workspace: None,
        }],
    )
    .unwrap()
    .ids["a"]
        .clone();
    assert!(
        canonical_bytes(
            &service
                .child_context_packet(&authority, &assignment, 65536)
                .unwrap()
        )
        .unwrap()
        .len()
            <= 65536
    );
}

#[cfg(unix)]
#[test]
fn file_capable_start_requires_canonical_nonempty_directory_roots() {
    let mut admitted = Vec::new();
    for index in 0..=4 {
        let (dir, service, authority) = fixture();
        let root = dir.path().join("worktree");
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let directory = root.join("real");
        std::fs::create_dir(&directory).unwrap();
        let alias = root.join("alias");
        std::os::unix::fs::symlink(&directory, &alias).unwrap();
        let file = root.join("file");
        std::fs::write(&file, "existing").unwrap();
        let mut code = contract();
        code.allowed_tools.extend(["Read".into(), "Write".into()]);
        let work = execute(&service, &authority, "code-create", create(code))
            .unwrap()
            .ids["w"]
            .clone();
        let roots = match index {
            1 => vec![],
            2 => vec![alias],
            3 => vec![file],
            _ => vec![directory],
        };
        let workspace = (index != 0).then(|| ExecutionWorkspace {
            repo: "repo".into(),
            base_commit: "a".repeat(40),
            branch: "fixture".into(),
            worktree: root.to_string_lossy().into(),
            write_roots: roots
                .iter()
                .map(|p| p.to_string_lossy().into_owned())
                .collect(),
            claims: BTreeSet::from([format!("worktree:{}", root.display())]),
        });
        let before = service.published().unwrap();
        let result = execute(
            &service,
            &authority,
            "root-start",
            vec![Operation::Start {
                work_id: work,
                temp_id: "a".into(),
                workspace,
            }],
        );
        admitted.push(result.is_ok());
        if result.is_err() {
            assert_eq!(
                canonical_bytes(&service.published().unwrap()).unwrap(),
                canonical_bytes(&before).unwrap()
            );
        }
    }
    assert_eq!(
        admitted,
        [false, false, false, false, true],
        "no workspace / empty / symlink / file / canonical directory"
    );
}
