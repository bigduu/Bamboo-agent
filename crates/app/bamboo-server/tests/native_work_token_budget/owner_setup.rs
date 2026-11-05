//! Owner budget HTTP over the actual FileStore/Inbox/native Host.
use super::*;
use actix_web::http::Method;
use bamboo_server::AppState;
use bamboo_tickets::{ExecutionTokenUsage, ExecutionWorkspace, TicketService};
use serde_json::Value;

const URI: &str = "/api/v1/tickets/token-budget";

async fn http(
    state: &web::Data<AppState>,
    method: Method,
    uri: &str,
    body: Option<&Value>,
) -> (StatusCode, Value) {
    request(state, method, uri, body, "127.0.0.1:43210").await
}
async fn request(
    state: &web::Data<AppState>,
    method: Method,
    uri: &str,
    body: Option<&Value>,
    peer: &str,
) -> (StatusCode, Value) {
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(bamboo_server::configure_routes),
    )
    .await;
    let mut req = test::TestRequest::default()
        .method(method)
        .uri(uri)
        .peer_addr(peer.parse().unwrap());
    if let Some(body) = body {
        req = req.set_json(body);
    }
    let response = test::call_service(&app, req.to_request()).await;
    let status = response.status();
    let body = test::read_body(response).await;
    let value = serde_json::from_slice(&body)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&body).into_owned()));
    (status, value)
}
fn owner() -> Principal {
    Principal::User {
        user_id: "host-owner".into(),
    }
}

struct Fixture {
    state: web::Data<AppState>,
    requests: mpsc::UnboundedReceiver<native_work::ModelRequest>,
    local: Arc<LocalProvider>,
    provider: Arc<dyn LLMProvider>,
    work: String,
    project: bamboo_domain::ProjectId,
    workspace: ExecutionWorkspace,
    home: tempfile::TempDir,
}

impl Fixture {
    async fn new(metered: bool) -> Self {
        Self::with_objective(metered, "Native Task with explicit finite owner limits").await
    }

    async fn with_objective(metered: bool, objective: &str) -> Self {
        let home = tempfile::tempdir().unwrap();
        let root = home.path().canonicalize().unwrap();
        // A pre-existing empty canonical Supervisor requires explicit Ticket init.
        let storage = bamboo_storage::SessionStoreV2::new(root.clone())
            .await
            .unwrap();
        storage
            .get_or_create_default_supervisor(MODEL)
            .await
            .unwrap();
        drop(storage);
        let (send, requests) = mpsc::unbounded_channel();
        let local = Arc::new(LocalProvider {
            requests: send,
            calls: AtomicUsize::new(0),
            max_requests: 2,
        });
        let provider: Arc<dyn LLMProvider> = if metered {
            Arc::new(Metered(local.clone()))
        } else {
            local.clone()
        };
        let state = web::Data::new(Box::pin(host(&root, provider.clone())).await);
        state.config.write().await.extra.insert("model_limits".into(), json!([{
            "model_pattern":MODEL,"max_context_tokens":INPUT,"max_output_tokens":OUTPUT,"safety_margin":1000
        }]));
        assert!(state.tickets.service().is_err());
        assert_eq!(
            http(&state, Method::GET, URI, None).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        let (code, scope) = http(&state, Method::GET, "/api/v1/tickets/scope", None).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(scope["can_initialize"], true);
        let (code, initialized) = http(
            &state,
            Method::POST,
            "/api/v1/tickets/initialize",
            Some(&scope["initialization"]),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "{initialized}");
        let status = http(&state, Method::GET, URI, None).await.1;
        assert_eq!(status["configured"], false);
        assert!(status["accounting"].is_null());
        let workspace = execution_workspace(&root);
        let project = state
            .project_store
            .create("Owner budget HTTP", None)
            .unwrap();
        let project = state
            .project_store
            .bind_workspace(
                &project.id,
                project.revision,
                WorkspaceBinding {
                    path: workspace.repo.clone(),
                    label: None,
                    git_common_dir: None,
                },
            )
            .unwrap();
        let (service, authority) = state.tickets.authority(owner()).await.unwrap();
        let contract = Contract {
            title: "HTTP budget Work".into(),
            objective: objective.into(),
            constraints: vec![],
            acceptance: vec!["Exact owner review".into()],
            user_acceptance_required: true,
            allowed_tools: BTreeSet::from(["Task".into()]),
        };
        let create = service
            .prepare_command(
                &authority,
                "http-budget-create",
                vec![
                    Operation::Create {
                        temp_id: "work".into(),
                        kind: TicketKind::Work,
                        parent: None,
                        contract: contract.clone(),
                        depends_on: BTreeSet::new(),
                    },
                    Operation::AssociateProject {
                        ticket_id: "work".into(),
                        project_id: project.id.clone(),
                    },
                ],
            )
            .unwrap();
        let (code, created) = http(
            &state,
            Method::POST,
            "/api/v1/tickets/update",
            Some(&serde_json::to_value(create).unwrap()),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "{created}");
        let work = created["ids"]["work"].as_str().unwrap().to_owned();
        let ready = service
            .prepare_command(
                &authority,
                "http-budget-ready",
                vec![
                    Operation::SetProjectContext {
                        work_id: work.clone(),
                        context: ProjectContextSnapshot {
                            project_id: project.id.clone(),
                            project_revision: project.revision,
                            resource_revision: project.resource_revision,
                            workspace_binding: project.workspace_bindings[0].clone(),
                            contract_revision: 1,
                            contract,
                            sources: BTreeMap::new(),
                        },
                    },
                    Operation::Ready {
                        work_id: work.clone(),
                    },
                ],
            )
            .unwrap();
        let (code, value) = http(
            &state,
            Method::POST,
            "/api/v1/tickets/update",
            Some(&serde_json::to_value(ready).unwrap()),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "{value}");
        assert_eq!(local.calls.load(Ordering::SeqCst), 0);
        Self {
            state,
            requests,
            local,
            provider,
            work,
            project: project.id,
            workspace,
            home,
        }
    }

    fn service(&self) -> Arc<TicketService> {
        self.state.tickets.service().unwrap()
    }
    async fn setup(&self, limit: u64) -> Value {
        let status = http(&self.state, Method::GET, URI, None).await.1;
        json!({"binding":status["binding"],"authority_epoch":status["observed"]["authority_epoch"],
            "expected_revision":status["budget_revision"],"limits":{"work_tokens":BTreeMap::from([(self.work.clone(),limit)]),"utc_day_tokens":limit,"total_tokens":limit}})
    }
    async fn policy(&self) {
        // Ready owner acceptance consumes the separately admitted budget API.
        // A missing HTTP entrance must fail; there is no trusted-port fallback.
        let policy_uri = "/api/v1/tickets/ready-policy";
        let (code, status) = http(&self.state, Method::GET, policy_uri, None).await;
        assert_eq!(code, StatusCode::OK, "{status}");
        let (commit, snapshot) = self.service().published().unwrap();
        let request = json!({
            "process_id":status["process_id"],"expected_revision":status["revision"],
            "operation_id":"budget-http-finite-policy","action":{"kind":"install","policy":{
                "binding":snapshot.binding,
                "observed":{"commit":commit,"seq":snapshot.seq,"authority_epoch":snapshot.authority_epoch},
                "project_id":self.project,"capacity":1,"max_rounds":4,"lifetime_secs":60,
                "grants":[{"work_id":self.work,"contract_revision":1,
                    "context_hash":snapshot.project.work_contexts[&self.work],"workspace":self.workspace}],
            }},
        });
        let receipt = http(&self.state, Method::PUT, policy_uri, Some(&request)).await;
        assert_eq!(receipt.0, StatusCode::OK, "{:?}", receipt.1);
        assert_eq!(
            http(&self.state, Method::PUT, policy_uri, Some(&request)).await,
            receipt
        );
    }
    async fn next(&mut self) -> native_work::ModelRequest {
        tokio::time::timeout(WAIT, self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn terminal(&self, submitted: bool) {
        tokio::time::timeout(WAIT, async {
            loop {
                let snapshot = self.service().published().unwrap().1;
                if let Some(assignment) = snapshot.assignments.values().next() {
                    if assignment.process_stopped
                        && (!submitted
                            || snapshot.tickets[&self.work].state == WorkState::Submitted)
                    {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}

fn task(request: native_work::ModelRequest) {
    assert!(request.tools.iter().any(|t| t.function.name == "Task"));
    request.reply.send(LLMChunk::ToolCalls(vec![ToolCall{id:"owner-budget-task".into(),tool_type:"function".into(),function:FunctionCall{name:"Task".into(),arguments:json!({"tasks":[{"id":"step","content":"Complete the bounded Task","status":"completed"}]}).to_string()}}])).unwrap();
}

#[actix_web::test]
async fn budget_owner_http_replay_conflict_capacity_and_auth_never_call_a_model() {
    let fixture = Fixture::new(true).await;
    let setup = fixture.setup(100).await;
    let (a, b) = futures::join!(
        http(&fixture.state, Method::PUT, URI, Some(&setup)),
        http(&fixture.state, Method::PUT, URI, Some(&setup))
    );
    assert_eq!(a, b);
    assert_eq!(a.0, StatusCode::OK);
    let original = a.1;
    for method in [Method::GET, Method::PUT] {
        assert_eq!(
            request(&fixture.state, method, URI, Some(&setup), "192.0.2.1:43210")
                .await
                .0,
            StatusCode::FORBIDDEN
        );
    }
    let mut changed = setup.clone();
    changed["limits"]["total_tokens"] = json!(101);
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&changed))
            .await
            .0,
        StatusCode::CONFLICT
    );
    let mut stale = setup.clone();
    stale["authority_epoch"] = json!(0);
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&stale)).await.0,
        StatusCode::CONFLICT
    );
    let mut foreign = setup.clone();
    foreign["binding"]["scope_id"] = json!("foreign");
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&foreign))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let mut forged = setup.clone();
    forged["principal"] = json!({"role":"owner"});
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&forged))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );
    let mut oversized = fixture.setup(200).await;
    oversized["limits"]["work_tokens"] = json!((0..65)
        .map(|n| (format!("work-{n}"), 200))
        .collect::<BTreeMap<_, _>>());
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&oversized))
            .await
            .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let left = fixture.setup(200).await;
    let right = fixture.setup(300).await;
    let (left, right) = futures::join!(
        http(&fixture.state, Method::PUT, URI, Some(&left)),
        http(&fixture.state, Method::PUT, URI, Some(&right))
    );
    assert_eq!(
        [left.0, right.0]
            .iter()
            .filter(|s| **s == StatusCode::OK)
            .count(),
        1
    );
    assert_eq!(
        [left.0, right.0]
            .iter()
            .filter(|s| **s == StatusCode::CONFLICT)
            .count(),
        1
    );
    let before = fixture.service().published().unwrap().0;
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&setup)).await,
        (StatusCode::OK, original.clone())
    );
    let (code, status) = http(
        &fixture.state,
        Method::GET,
        &format!("{URI}?configuration_revision=1"),
        None,
    )
    .await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(status["budget_revision"], 2);
    assert_eq!(status["requested_configuration"], original);
    assert_eq!(
        http(
            &fixture.state,
            Method::GET,
            &format!("{URI}?configuration_revision=99"),
            None
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(fixture.service().published().unwrap().0, before);
    fixture.state.config.write().await.features.ticket_mutation = false;
    assert_eq!(
        http(&fixture.state, Method::GET, URI, None).await.0,
        StatusCode::OK
    );
    assert_eq!(
        http(
            &fixture.state,
            Method::PUT,
            URI,
            Some(&fixture.setup(400).await)
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(fixture.service().published().unwrap().0, before);
    assert_eq!(fixture.local.calls.load(Ordering::SeqCst), 0);
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn budget_owner_http_funds_real_ready_worker_known_usage_and_exact_user_acceptance() {
    let objective =
        "Review the accepted source; cite exact lines and preserve evidence boundaries. ".repeat(9);
    assert!(objective.len() < 2048);
    let mut fixture = Fixture::with_objective(true, &objective).await;
    let setup = fixture.setup(CAP + 1000).await;
    let (code, receipt) = http(&fixture.state, Method::PUT, URI, Some(&setup)).await;
    assert_eq!(code, StatusCode::OK, "{receipt}");
    fixture.policy().await;
    let first = fixture.next().await;
    assert!(first
        .messages
        .iter()
        .any(|message| message.content.contains(&objective)));
    let (_, status) = http(&fixture.state, Method::GET, URI, None).await;
    assert_eq!(status["accounting"]["total_charged"], CAP);
    assert_eq!(status["accounting"]["reservations"]["pending_usage"], 1);
    let before = fixture.service().published().unwrap().0;
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&setup)).await,
        (StatusCode::OK, receipt)
    );
    assert_eq!(fixture.service().published().unwrap().0, before);
    task(first);
    let second = fixture.next().await;
    assert!(second
        .messages
        .iter()
        .any(|m| m.tool_call_id.as_deref() == Some("owner-budget-task")
            && m.tool_success == Some(true)));
    second
        .reply
        .send(LLMChunk::Token("Budget owner HTTP native result".into()))
        .unwrap();
    fixture.terminal(true).await;
    let service = fixture.service();
    let (_, authority) = fixture.state.tickets.authority(owner()).await.unwrap();
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(snapshot.intents.len(), 1);
    let submission = snapshot.tickets[&fixture.work]
        .current_submission
        .clone()
        .unwrap();
    assert!(snapshot.tickets[&fixture.work]
        .accepted_submission
        .is_none());
    let accept = service
        .prepare_command(
            &authority,
            "budget-owner-exact-accept",
            vec![Operation::Accept {
                work_id: fixture.work.clone(),
                submission_id: submission,
                evidence: vec!["Owner reviewed exact native result".into()],
            }],
        )
        .unwrap();
    let accept = serde_json::to_value(accept).unwrap();
    let accepted = http(
        &fixture.state,
        Method::POST,
        "/api/v1/tickets/update",
        Some(&accept),
    )
    .await;
    assert_eq!(accepted.0, StatusCode::OK);
    assert_eq!(
        http(
            &fixture.state,
            Method::POST,
            "/api/v1/tickets/update",
            Some(&accept)
        )
        .await,
        accepted
    );
    let (_, status) = http(&fixture.state, Method::GET, URI, None).await;
    assert_eq!(status["accounting"]["total_charged"], 60);
    assert_eq!(status["accounting"]["reservations"]["known_usage"], 2);
    assert_eq!(status["accounting"]["reservations"]["pending_usage"], 0);
    assert_eq!(
        service.published().unwrap().1.tickets[&fixture.work].state,
        WorkState::Accepted
    );
    let before = service.published().unwrap().0;
    let low = fixture.setup(59).await;
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&low)).await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(service.published().unwrap().0, before);
    assert_eq!(fixture.local.calls.load(Ordering::SeqCst), 2);
    fixture.state.shutdown().await;
}

#[actix_web::test]
async fn budget_owner_http_unknown_usage_restart_and_old_epoch_cannot_refund_or_redispatch() {
    let mut fixture = Fixture::new(false).await;
    let setup = fixture.setup(CAP).await;
    let original = http(&fixture.state, Method::PUT, URI, Some(&setup)).await;
    assert_eq!(original.0, StatusCode::OK);
    fixture.policy().await;
    task(fixture.next().await);
    fixture.terminal(false).await;
    let snapshot = fixture.service().published().unwrap().1;
    assert!(snapshot.tickets[&fixture.work].current_submission.is_none());
    let budget = snapshot.execution_budget.as_ref().unwrap();
    assert_eq!(budget.total_charged, CAP);
    assert_eq!(budget.reservations.len(), 1);
    assert!(budget
        .reservations
        .values()
        .all(|r| r.usage == Some(ExecutionTokenUsage::Unknown)));
    let assignment = snapshot.assignments.values().next().unwrap();
    let key = assignment.dispatch_key.clone();
    let spec = snapshot.intents[&key].immutable_spec.clone();
    assert_eq!(fixture.local.calls.load(Ordering::SeqCst), 1);
    let Fixture {
        state,
        requests: _,
        local,
        provider,
        work,
        project: _,
        workspace: _,
        home,
    } = fixture;
    state.shutdown().await;
    drop(state);
    let root = home.path().canonicalize().unwrap();
    let state = web::Data::new(Box::pin(host(&root, provider)).await);
    assert_eq!(
        http(&state, Method::PUT, URI, Some(&setup)).await.0,
        StatusCode::CONFLICT
    );
    let (_, status) = http(
        &state,
        Method::GET,
        &format!("{URI}?configuration_revision=1"),
        None,
    )
    .await;
    assert_eq!(status["requested_configuration"], original.1);
    assert_eq!(status["accounting"]["total_charged"], CAP);
    assert_eq!(status["accounting"]["reservations"]["unknown_usage"], 1);
    let mut fresh = setup.clone();
    fresh["authority_epoch"] = status["observed"]["authority_epoch"].clone();
    let service = state.tickets.service().unwrap();
    let before = service.published().unwrap().0;
    assert_eq!(http(&state, Method::PUT, URI, Some(&fresh)).await, original);
    assert_eq!(service.published().unwrap().0, before);
    assert!(matches!(
        state.tickets.reconcile_ready_work().await.unwrap(),
        bamboo_server::app_state::ticket_application::ReadyReconcile::Disabled
    ));
    assert!(matches!(
        bamboo_engine::ticket_runtime::query_dispatch(
            state.storage.as_ref(),
            &service,
            &key,
            &spec
        )
        .await
        .unwrap(),
        bamboo_engine::ticket_runtime::DispatchObservation::Terminal { .. }
    ));
    let snapshot = service.published().unwrap().1;
    assert_eq!(snapshot.assignments.len(), 1);
    assert_eq!(snapshot.intents.len(), 1);
    assert!(snapshot.tickets[&work].accepted_submission.is_none());
    assert_eq!(local.calls.load(Ordering::SeqCst), 1);
    state.shutdown().await;
}

#[actix_web::test]
async fn budget_owner_observation_retains_over_cap_accounting_without_a_model_call() {
    use bamboo_tickets::{
        ExecutionBudgetPurpose, ExecutionTokenOverCapUsage, WorkTokenAdmission, WorkTokenRequest,
    };
    let fixture = Fixture::new(true).await;
    let setup = fixture.setup(200).await;
    assert_eq!(
        http(&fixture.state, Method::PUT, URI, Some(&setup)).await.0,
        StatusCode::OK
    );
    let service = fixture.service();
    let snapshot = service.published().unwrap().1;
    let runtime = Authority::from_verified_host(snapshot.binding, Principal::Runtime);
    let input = WorkTokenRequest {
        operation_id: "owner-observation-over-cap".into(),
        project_id: fixture.project.clone(),
        work_id: fixture.work.clone(),
        purpose: ExecutionBudgetPurpose::SupervisorDecision,
        request_hash: bamboo_tickets::content_hash(b"actual canonical accounting port"),
        observed_seq: snapshot.seq,
        input_cap: 60,
        output_cap: 40,
        attempt: None,
    };
    let WorkTokenAdmission::Reserved(receipt) =
        service.reserve_work_tokens(&runtime, &input, 1).unwrap()
    else {
        panic!("canonical reservation")
    };
    service
        .report_work_token_over_cap(
            &runtime,
            &input.operation_id,
            &input.request_hash,
            &receipt.receipt,
            ExecutionTokenOverCapUsage {
                input_tokens: Some(61),
                output_tokens: None,
                total_tokens: None,
            },
        )
        .unwrap();
    let before = service.published().unwrap().0;
    let (code, status) = http(&fixture.state, Method::GET, URI, None).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(status["accounting"]["accounting_halted"], true);
    assert_eq!(status["accounting"]["total_charged"], 100);
    assert_eq!(status["accounting"]["reservations"]["over_cap_usage"], 1);
    assert_eq!(status["accounting"]["reservations"]["known_usage"], 0);
    assert_eq!(service.published().unwrap().0, before);
    assert_eq!(fixture.local.calls.load(Ordering::SeqCst), 0);
    fixture.state.shutdown().await;
}
