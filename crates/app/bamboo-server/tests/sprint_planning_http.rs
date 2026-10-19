use actix_web::{http::StatusCode, test, web, App};
use bamboo_domain::ProjectId;
use bamboo_engine::ticket_worker_plan::tickets::*;
use bamboo_server::{app_state::ticket_application::TicketApplication, AppState};
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc};

macro_rules! post {
    ($app:expr, $path:expr, $body:expr) => {
        test::call_service(
            &$app,
            test::TestRequest::post()
                .uri($path)
                .peer_addr("127.0.0.1:5700".parse().unwrap())
                .set_json($body)
                .to_request(),
        )
        .await
    };
}

struct Fixture {
    _root: tempfile::TempDir,
    state: web::Data<AppState>,
    service: Arc<TicketService>,
    project: ProjectId,
    works: Vec<String>,
}

impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        // All memory/storage state is disposable; never use the user's Jiandu root.
        let mut state = AppState::new_with_memory_store(
            root.path().to_path_buf(),
            bamboo_memory::memory_store::MemoryStore::new(root.path().join("jiandu")),
        )
        .await
        .unwrap();
        *state.config.write().await = serde_json::from_value(json!({
            "provider":"openai", "features":{"ticket_mutation":true,"ticket_dispatch":false},
            "providers":{"openai":{"api_key":"fixture","model":"fixture-model"}}
        }))
        .unwrap();
        state.tickets = Arc::new(
            TicketApplication::open(root.path(), state.storage.clone(), state.config.clone()).await,
        );
        let project = state.project_store.create("Sprints", None).unwrap().id;
        let service = state.tickets.service().unwrap();
        let (ticket_service, owner) = state
            .tickets
            .authority(Principal::User {
                user_id: "host-owner".into(),
            })
            .await
            .unwrap();
        let mut works = Vec::new();
        for n in 0..2 {
            let command = ticket_service
                .prepare_command(
                    &owner,
                    &format!("seed-{n}"),
                    vec![
                        Operation::Create {
                            temp_id: "w".into(),
                            kind: TicketKind::Work,
                            parent: None,
                            depends_on: BTreeSet::new(),
                            contract: Contract {
                                title: format!("Work {n}"),
                                objective: "deliver".into(),
                                constraints: vec![],
                                acceptance: vec!["review evidence".into()],
                                user_acceptance_required: true,
                                allowed_tools: BTreeSet::from(["Task".into()]),
                            },
                        },
                        Operation::AssociateProject {
                            ticket_id: "w".into(),
                            project_id: project.clone(),
                        },
                    ],
                )
                .unwrap();
            let receipt = state
                .tickets
                .update(
                    Principal::User {
                        user_id: "host-owner".into(),
                    },
                    &command,
                )
                .await
                .unwrap();
            works.push(receipt.ids["w"].clone());
        }
        Self {
            _root: root,
            state: web::Data::new(state),
            service,
            project,
            works,
        }
    }
    fn command(&self, sprint_id: &str, action: Value) -> Value {
        let snapshot = self.service.published().unwrap().1;
        json!({"operation_id":format!("sprint-{}",snapshot.seq),"binding":snapshot.binding,
            "expected_seq":snapshot.seq,"expected_epoch":snapshot.authority_epoch,
            "project_id":self.project,"sprint_id":sprint_id,"source":"explicit owner request","action":action})
    }
    fn create(&self, id: &str) -> Value {
        self.command(
            id,
            json!({"op":"create","definition":{"name":id,"goal":"deliver verified Work"}}),
        )
    }
    fn revision(&self, id: &str) -> u64 {
        self.service.published().unwrap().1.sprint.sprints[&SprintId::parse(id).unwrap()].revision
    }
}

#[actix_web::test]
async fn owner_http_planning_is_cas_idempotent_and_never_changes_work_or_dispatch() {
    let f = Fixture::new().await;
    let app = test::init_service(
        App::new()
            .app_data(f.state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let before = f.service.published().unwrap().1;
    let create = f.create("a");
    let response = post!(app, "/api/v1/tickets/sprints/command", &create);
    assert_eq!(response.status(), StatusCode::OK);
    let receipt: Value = test::read_body_json(response).await;
    let replay = post!(app, "/api/v1/tickets/sprints/command", &create);
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(test::read_body_json::<Value, _>(replay).await, receipt);
    let mut changed = create.clone();
    changed["source"] = "changed source".into();
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", changed).status(),
        StatusCode::CONFLICT
    );
    let mut collision = create.clone();
    collision["operation_id"] = "seed-0".into();
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", collision).status(),
        StatusCode::CONFLICT
    );
    let mut stale = create.clone();
    stale["operation_id"] = "stale".into();
    stale["sprint_id"] = "b".into();
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", &stale).status(),
        StatusCode::CONFLICT
    );
    for action in [
        json!({"op":"add","expected_revision":1,"work_id":f.works[0]}),
        json!({"op":"start","expected_revision":2}),
    ] {
        let command = f.command("a", action);
        assert_eq!(
            post!(app, "/api/v1/tickets/sprints/command", &command).status(),
            StatusCode::OK
        );
    }
    let list = post!(
        app,
        "/api/v1/tickets/sprints/list",
        json!({"project_id":f.project,"limit":10})
    );
    assert_eq!(list.status(), StatusCode::OK);
    let list: Value = test::read_body_json(list).await;
    assert_eq!(list["coverage"], "authoritative_scope");
    assert_eq!(list["data"][0]["status"], "active");
    assert_eq!(list["data"][0]["member_count"], 1);
    assert!(list["data"][0].get("history").is_none());
    for (sprint_filter, total) in [
        (Value::Null, 2),
        (json!({"kind":"backlog"}), 1),
        (json!({"kind":"sprint","sprint_id":"a"}), 1),
    ] {
        let board = post!(
            app,
            "/api/v1/tickets/project-board",
            json!({"project_id":f.project,"limit":10,"sprint_filter":sprint_filter})
        );
        assert_eq!(board.status(), StatusCode::OK);
        let board: Value = test::read_body_json(board).await;
        assert_eq!(board["data"]["counts"]["total"], total);
        assert_eq!(board["data"]["counts"]["done"], 0);
    }
    let close = f.command(
        "a",
        json!({"op":"close","expected_revision":3,
        "dispositions":{(f.works[0].clone()):{"destination":"backlog"}},"target_revisions":{}}),
    );
    let response = post!(app, "/api/v1/tickets/sprints/command", &close);
    assert_eq!(response.status(), StatusCode::OK);
    let closed: Value = test::read_body_json(response).await;
    assert_eq!(closed["closure"]["counts"]["unfinished"], 1);
    assert_eq!(closed["closure"]["counts"]["accepted"], 0);
    let inspect = post!(
        app,
        "/api/v1/tickets/sprints/inspect",
        json!({"project_id":f.project,"sprint_id":"a"})
    );
    assert_eq!(inspect.status(), StatusCode::OK);
    let inspect: Value = test::read_body_json(inspect).await;
    assert_eq!(inspect["data"]["sprint"]["status"], "closed");
    assert_eq!(inspect["data"]["sprint"]["closure"], closed["closure"]);
    assert_eq!(inspect["data"]["current_counts"]["unfinished"], 1);
    println!(
        "SPRINT_HTTP_CONTRACT {}",
        json!({
            "list_request":{"project_id":f.project,"limit":10},"list_response":list,
            "create_request":create,"create_response":receipt,
            "close_request":close,"close_response":closed,
            "inspect_request":{"project_id":f.project,"sprint_id":"a"},"inspect_response":inspect,
            "board_filter_examples":[{"kind":"backlog"},{"kind":"sprint","sprint_id":"a"}]
        })
    );
    let create_b = f.create("b");
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", &create_b).status(),
        StatusCode::OK
    );
    let replay = post!(app, "/api/v1/tickets/sprints/command", &close);
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(test::read_body_json::<Value, _>(replay).await, closed);
    let invalid = f.command("a", json!({"op":"set_state","state":"done"}));
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", invalid).status(),
        StatusCode::BAD_REQUEST
    );
    let after = f.service.published().unwrap().1;
    assert_eq!(
        canonical_bytes(&before.tickets).unwrap(),
        canonical_bytes(&after.tickets).unwrap()
    );
    assert!(after.assignments.is_empty());
    assert!(after.intents.is_empty());
    assert!(after.submissions.is_empty());
}

#[actix_web::test]
async fn sprint_routes_share_owner_auth_host_project_checks_and_mutation_rollback() {
    let f = Fixture::new().await;
    let app = test::init_service(
        App::new()
            .app_data(f.state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let create = f.create("a");
    let list = json!({"project_id":f.project,"limit":10});
    let inspect = json!({"project_id":f.project,"sprint_id":"a"});
    let before = f.service.published().unwrap().0;
    for (path, body) in [
        ("/api/v1/tickets/sprints/list", &list),
        ("/api/v1/tickets/sprints/inspect", &inspect),
        ("/api/v1/tickets/sprints/command", &create),
    ] {
        let denied = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(path)
                .peer_addr("203.0.113.1:5700".parse().unwrap())
                .set_json(body)
                .to_request(),
        )
        .await;
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    }
    assert_eq!(f.service.published().unwrap().0, before);
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", &create).status(),
        StatusCode::OK
    );
    f.state.config.write().await.features.ticket_mutation = false;
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/list", &list).status(),
        StatusCode::OK
    );
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/inspect", &inspect).status(),
        StatusCode::OK
    );
    let before = f.service.published().unwrap().0;
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", &create).status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(f.service.published().unwrap().0, before);
    f.state.config.write().await.features.ticket_mutation = true;
    let mut unknown = f.create("unknown");
    unknown["project_id"] = "missing-project".into();
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", &unknown).status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        post!(
            app,
            "/api/v1/tickets/sprints/list",
            json!({"project_id":"missing-project","limit":10})
        )
        .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        post!(
            app,
            "/api/v1/tickets/sprints/inspect",
            json!({"project_id":"missing-project","sprint_id":"a"})
        )
        .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(f.service.published().unwrap().0, before);
    let manifest = f.state.project_store.get(&f.project).unwrap();
    f.state
        .project_store
        .archive(&f.project, manifest.revision)
        .unwrap();
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/list", &list).status(),
        StatusCode::OK
    );
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/inspect", &inspect).status(),
        StatusCode::OK
    );
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", &create).status(),
        StatusCode::OK
    );
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", f.create("b")).status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(f.service.published().unwrap().0, before);
}

#[actix_web::test]
async fn http_board_and_sprint_list_cursors_bind_filters_and_preserve_old_membership() {
    let f = Fixture::new().await;
    let app = test::init_service(
        App::new()
            .app_data(f.state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    for id in ["a", "b"] {
        assert_eq!(
            post!(app, "/api/v1/tickets/sprints/command", f.create(id)).status(),
            StatusCode::OK
        );
    }
    for work in &f.works {
        let command = f.command(
            "a",
            json!({"op":"add","expected_revision":f.revision("a"),"work_id":work}),
        );
        assert_eq!(
            post!(app, "/api/v1/tickets/sprints/command", command).status(),
            StatusCode::OK
        );
    }
    let request =
        json!({"project_id":f.project,"limit":1,"sprint_filter":{"kind":"sprint","sprint_id":"a"}});
    let response = post!(app, "/api/v1/tickets/project-board", &request);
    assert_eq!(response.status(), StatusCode::OK);
    let first: Value = test::read_body_json(response).await;
    let list = post!(
        app,
        "/api/v1/tickets/sprints/list",
        json!({"project_id":f.project,"limit":1,"status":"planned"})
    );
    assert_eq!(list.status(), StatusCode::OK);
    let list: Value = test::read_body_json(list).await;
    let remove = f.command(
        "a",
        json!({"op":"remove","expected_revision":f.revision("a"),"work_id":f.works[0]}),
    );
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/command", remove).status(),
        StatusCode::OK
    );
    let mut next = request;
    next["cursor"] = first["next_cursor"].clone();
    let response = post!(app, "/api/v1/tickets/project-board", &next);
    assert_eq!(response.status(), StatusCode::OK);
    let second: Value = test::read_body_json(response).await;
    assert_eq!(second["snapshot"], first["snapshot"]);
    assert_eq!(second["data"]["counts"]["total"], 2);
    assert_ne!(
        second["data"]["rows"][0]["ticket"]["id"],
        first["data"]["rows"][0]["ticket"]["id"]
    );
    next["sprint_filter"] = json!({"kind":"backlog"});
    assert_eq!(
        post!(app, "/api/v1/tickets/project-board", next).status(),
        StatusCode::GONE
    );
    let other = f.state.project_store.create("Other", None).unwrap();
    assert_eq!(post!(app, "/api/v1/tickets/project-board", json!({"project_id":other.id,"limit":10,"sprint_filter":{"kind":"sprint","sprint_id":"a"}})).status(), StatusCode::FORBIDDEN);
    let page =
        json!({"project_id":f.project,"limit":10,"status":"planned","cursor":list["next_cursor"]});
    let response = post!(app, "/api/v1/tickets/sprints/list", &page);
    assert_eq!(response.status(), StatusCode::OK);
    let page_body: Value = test::read_body_json(response).await;
    assert_eq!(page_body["snapshot"], list["snapshot"]);
    let mut changed = page;
    changed["status"] = "active".into();
    assert_eq!(
        post!(app, "/api/v1/tickets/sprints/list", changed).status(),
        StatusCode::GONE
    );
    assert_eq!(
        post!(
            app,
            "/api/v1/tickets/sprints/list",
            json!({"project_id":f.project,"limit":0})
        )
        .status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
}
