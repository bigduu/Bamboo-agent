use actix_web::{http::StatusCode, test, web, App};
use bamboo_server::AppState;
use bamboo_tickets::*;
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc};

fn command(
    service: &TicketService,
    who: &Authority,
    id: &str,
    operations: Vec<Operation>,
) -> Command {
    service.prepare_command(who, id, operations).unwrap()
}

#[actix_web::test]
async fn owner_accept_waits_for_runtime_receipt_and_inspect_exposes_the_exact_stop_fact() {
    let root = tempfile::tempdir().unwrap();
    let mut state = AppState::new_with_memory_store(
        root.path().to_path_buf(),
        bamboo_memory::memory_store::MemoryStore::new(root.path().join("jiandu")),
    )
    .await
    .unwrap();
    *state.config.write().await = serde_json::from_value(json!({"provider":"openai", "features":{"ticket_mutation":true}, "providers":{"openai":{"api_key":"fixture","model":"fixture-model"}}})).unwrap();
    state.tickets = Arc::new(
        bamboo_server::app_state::ticket_application::TicketApplication::open(
            root.path(),
            state.storage.clone(),
            state.config.clone(),
        )
        .await,
    );
    let service = state.tickets.service().unwrap();
    let binding = service.published().unwrap().1.binding;
    let owner = Authority::from_verified_host(
        binding.clone(),
        Principal::User {
            user_id: "host-owner".into(),
        },
    );
    let runtime = Authority::from_verified_host(binding.clone(), Principal::Runtime);
    let seed = command(
        &service,
        &owner,
        "seed",
        vec![
            Operation::Create {
                temp_id: "w".into(),
                kind: TicketKind::Work,
                parent: None,
                depends_on: BTreeSet::new(),
                contract: Contract {
                    title: "Completion".into(),
                    objective: "Retain exact evidence".into(),
                    constraints: vec![],
                    acceptance: vec!["User review".into()],
                    user_acceptance_required: true,
                    allowed_tools: BTreeSet::from(["Task".into()]),
                },
            },
            Operation::Ready {
                work_id: "w".into(),
            },
            Operation::Start {
                work_id: "w".into(),
                temp_id: "a".into(),
                workspace: None,
            },
        ],
    );
    let ids = service.execute(&owner, &seed).unwrap().ids;
    let work = ids["w"].clone();
    let assignment = ids["a"].clone();
    let snapshot = service.published().unwrap().1;
    let a = &snapshot.assignments[&assignment];
    let receipt = RuntimeReceipt {
        dispatch_key: a.dispatch_key.clone(),
        spec_hash: snapshot.intents[&a.dispatch_key].spec_hash.clone(),
        run_id: "synthetic-run".into(),
        session_id: "synthetic-child".into(),
    };
    service
        .execute(
            &runtime,
            &command(
                &service,
                &runtime,
                "admit",
                vec![
                    Operation::Admitted {
                        assignment_id: assignment.clone(),
                        receipt: receipt.clone(),
                    },
                    Operation::Running {
                        assignment_id: assignment.clone(),
                    },
                ],
            ),
        )
        .unwrap();
    let worker = Authority::from_verified_host(
        binding,
        Principal::Worker {
            assignment_id: assignment.clone(),
            generation: a.generation,
            run_id: receipt.run_id.clone(),
            session_id: receipt.session_id.clone(),
        },
    );
    let artifact = service
        .store_artifact(&runtime, b"real preserved bytes")
        .unwrap();
    let submission = service
        .execute(
            &worker,
            &command(
                &service,
                &worker,
                "submit",
                vec![Operation::Submit {
                    assignment_id: assignment.clone(),
                    temp_id: "s".into(),
                    artifacts: vec![artifact],
                    evidence: vec!["unverified Worker evidence".into()],
                }],
            ),
        )
        .unwrap()
        .ids["s"]
        .clone();
    let before = service.published().unwrap();
    assert!(before.1.tickets[&work].active_assignment.is_none());
    let accept = command(
        &service,
        &owner,
        "owner-accept",
        vec![Operation::Accept {
            work_id: work.clone(),
            submission_id: submission.clone(),
            evidence: vec!["explicit User decision; no test claim".into()],
        }],
    );
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&accept)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::LOCKED);
    assert_eq!(service.published().unwrap().0, before.0);
    let request = json!({"ids":[work],"sections":["controls"],"depth":0,"budget_bytes":65536,"fixed_commit":null});
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/inspect")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&request)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let inspection: Value = test::read_body_json(response).await;
    assert_eq!(
        inspection["data"][0]["controls"]["accept_requires_stopped_assignment"],
        true
    );
    assert_eq!(
        inspection["data"][0]["controls"]["assignment_id"],
        assignment
    );
    assert_eq!(inspection["data"][0]["controls"]["process_stopped"], false);
    service
        .execute(
            &runtime,
            &command(
                &service,
                &runtime,
                "exact-stop",
                vec![Operation::RuntimeStopped {
                    assignment_id: assignment.clone(),
                    receipt,
                    completed: true,
                }],
            ),
        )
        .unwrap();
    let stale = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&accept)
            .to_request(),
    )
    .await;
    assert_eq!(stale.status(), StatusCode::CONFLICT);
    let reconciled = command(&service, &owner, "owner-accept", accept.operations);
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/tickets/update")
            .peer_addr("127.0.0.1:5700".parse().unwrap())
            .set_json(&reconciled)
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let accepted: Value = test::read_body_json(response).await;
    assert_eq!(accepted["principal"], "user:host-owner");
    let after = service.published().unwrap().1;
    assert_eq!(after.tickets[&work].state, WorkState::Accepted);
    assert!(after.assignments[&assignment].process_stopped);
    assert_eq!(
        canonical_bytes(&before.1.submissions).unwrap(),
        canonical_bytes(&after.submissions).unwrap()
    );
}
