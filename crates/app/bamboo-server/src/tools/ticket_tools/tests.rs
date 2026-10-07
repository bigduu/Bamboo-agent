use super::*;
use bamboo_agent_core::tools::ExecutingSupervisorObservation;
use bamboo_config::Config;
use bamboo_domain::{Session, SessionAuthorityIdentity, Storage, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_storage::SessionStoreV2;
use tokio::sync::RwLock;

struct Fixture {
    _root: tempfile::TempDir,
    app: Arc<TicketApplication>,
    session: Session,
}
impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let storage = Arc::new(SessionStoreV2::new(root.path().join("host")).await.unwrap());
        let config: Config = serde_json::from_value(
            json!({"provider":"openai","features":{"ticket_mutation":true},
            "providers":{"openai":{"api_key":"fixture","model":"fixture-model"}}}),
        )
        .unwrap();
        let app = Arc::new(
            TicketApplication::open(root.path(), storage.clone(), Arc::new(RwLock::new(config)))
                .await,
        );
        let session = storage
            .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap()
            .unwrap();
        Self {
            _root: root,
            app,
            session,
        }
    }
    fn ctx(&self) -> ToolCtx {
        let mut ctx = ToolCtx::none("model-call");
        ctx.session_id = Some(Arc::from(self.session.id.as_str()));
        ctx.executing_supervisor =
            ExecutingSupervisorObservation::capture_from_executing_session(&self.session);
        ctx
    }
    fn mutation(&self, id: &str, operations: Value) -> Value {
        let snapshot = self.app.service().unwrap().published().unwrap().1;
        json!({"operation_id":id,"expected_seq":snapshot.seq,"expected_epoch":snapshot.authority_epoch,"operations":operations})
    }
    fn tool(&self, name: &'static str) -> TicketTool {
        TicketTool {
            app: self.app.clone(),
            name,
        }
    }
}

async fn invoke(tool: &TicketTool, args: Value, ctx: ToolCtx) -> (bool, Value) {
    let result = tool.invoke(args, ctx).await.unwrap().into_tool_result();
    (
        result.success,
        serde_json::from_str(&result.result).unwrap(),
    )
}

#[tokio::test]
async fn ticket_tools_require_original_incarnation_and_ignore_no_json_authority() {
    let f = Fixture::new().await;
    let tool = f.tool("work_overview");
    let (ok, denied) = invoke(&tool, json!({}), ToolCtx::none("none")).await;
    assert!(!ok);
    assert_eq!(denied["status_code"], 403);
    let mut old = f.session.clone();
    old.authority_identity = SessionAuthorityIdentity::Supervisor {
        incarnation_id: uuid::Uuid::new_v4(),
    };
    let mut ctx = f.ctx();
    ctx.executing_supervisor = ExecutingSupervisorObservation::capture_from_executing_session(&old);
    let (ok, denied) = invoke(&tool, json!({}), ctx).await;
    assert!(!ok);
    assert_eq!(denied["status_code"], 403);
    let mut child = f.ctx();
    child.session_id = Some(Arc::from("sibling-child"));
    assert_eq!(invoke(&tool, json!({}), child).await.1["status_code"], 403);
    let (ok, overview) = invoke(&tool, json!({}), f.ctx()).await;
    assert!(ok);
    assert_eq!(overview["data"]["work_count"], 0);
}

#[tokio::test]
async fn ticket_model_update_replays_exact_receipt_and_cannot_write_ingress_or_user_approval() {
    let f = Fixture::new().await;
    let tool = f.tool("work_update");
    let args = f.mutation("create", json!([
        {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],"contract":{
            "title":"One Work","objective":"One result","constraints":["Own plan only"],"acceptance":["Exact output"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
        {"op":"ready","work_id":"work"}]));
    let first = invoke(&tool, args.clone(), f.ctx()).await;
    assert!(first.0);
    assert_eq!(invoke(&tool, args.clone(), f.ctx()).await, first);
    let mut changed = args.clone();
    changed["operations"][0]["contract"]["title"] = json!("Changed");
    assert_eq!(invoke(&tool, changed, f.ctx()).await.1["status_code"], 409);
    let forbidden = f.mutation("approve", json!([{"op":"decide_approval","request_id":"other-work","prompt_revision":1,"fingerprint":"forged","approve":true}]));
    assert_eq!(
        invoke(&tool, forbidden, f.ctx()).await.1["status_code"],
        403
    );
    let mut forged = args;
    forged["principal"] = json!({"runtime":true});
    assert_eq!(invoke(&tool, forged, f.ctx()).await.1["status_code"], 422);
    let mut readonly = f.ctx();
    readonly.plan_read_only = true;
    assert_eq!(
        invoke(&tool, f.mutation("other", json!([])), readonly)
            .await
            .1["status_code"],
        403
    );
    assert!(f.session.task_list.is_none());
}

#[test]
fn six_ticket_schemas_remain_bounded_and_host_fields_are_absent() {
    for name in NAMES {
        let schema = schema::parameters(name);
        assert_eq!(schema["additionalProperties"], false);
        for forbidden in ["authority", "principal", "source", "approved", "binding"] {
            assert!(schema["properties"].get(forbidden).is_none());
        }
    }
    assert_eq!(
        schema::parameters("work_inspect")["properties"]["ids"]["maxItems"],
        32
    );
    assert_eq!(
        schema::parameters("work_update")["properties"]["operations"]["maxItems"],
        64
    );
}

#[tokio::test]
async fn canonical_human_requires_whole_proposal_and_zero_groups_remain_resolved() {
    let f = Fixture::new().await;
    let (service, user) = f
        .app
        .authority(Principal::User {
            user_id: "host-owner".into(),
        })
        .await
        .unwrap();
    let ingress = VerifiedUserIngress::from_verified_host(
        "hello".into(),
        HumanIngressRecord {
            user_id: "host-owner".into(),
            source_ingress_seq: 1,
            text: "你好".into(),
            thread_id: None,
            in_reply_to: None,
            correlation_id: None,
        },
    )
    .unwrap();
    service.register_user_ingress(&user, &ingress).unwrap();
    let seq = service.published().unwrap().1.seq;
    let mut read_only = f.ctx();
    read_only.plan_read_only = true;
    assert!(
        invoke(&f.tool("work_overview"), json!({}), read_only)
            .await
            .0
    );
    assert_eq!(
        service.published().unwrap().1.seq,
        seq,
        "Plan reads cannot pin/publish a message basis"
    );
    let overview = invoke(&f.tool("work_overview"), json!({}), f.ctx()).await;
    assert_eq!(
        overview.1["pending_message"]["input"]["human"]["text"],
        "你好"
    );
    let bypass = invoke(
        &f.tool("work_update"),
        f.mutation("bypass", json!([])),
        f.ctx(),
    )
    .await;
    assert_eq!(bypass.1["status_code"], 423);
    let args = json!({"message_id":"hello","proposal":{"groups":[]}});
    let resolved = invoke(&f.tool("work_update"), args.clone(), f.ctx()).await;
    assert!(resolved.0);
    let seq = service.published().unwrap().1.seq;
    assert_eq!(
        invoke(&f.tool("work_update"), args, f.ctx()).await,
        resolved
    );
    assert_eq!(service.published().unwrap().1.seq, seq);
    assert!(
        invoke(&f.tool("work_overview"), json!({}), f.ctx()).await.1["pending_message"].is_null()
    );
    assert_eq!(service.published().unwrap().1.tickets.len(), 0);
}

#[tokio::test]
async fn semantic_proposal_preserves_independent_group_results_and_no_json_user_grant() {
    let f = Fixture::new().await;
    let (service, user) = f
        .app
        .authority(Principal::User {
            user_id: "host-owner".into(),
        })
        .await
        .unwrap();
    service
        .register_user_ingress(
            &user,
            &VerifiedUserIngress::from_verified_host(
                "mixed".into(),
                HumanIngressRecord {
                    user_id: "host-owner".into(),
                    source_ingress_seq: 1,
                    text: "创建报告；那个先等等".into(),
                    thread_id: None,
                    in_reply_to: None,
                    correlation_id: None,
                },
            )
            .unwrap(),
        )
        .unwrap();
    let args = json!({"message_id":"mixed","proposal":{"groups":[
        {"group_id":"create","item_ids":["report"],"source_quote":"创建报告","clarification":null,"operations":[{"op":"create","temp_id":"report","kind":"work","parent":null,"depends_on":[],"contract":{"title":"报告","objective":"一份报告","constraints":[],"acceptance":["证据"],"user_acceptance_required":true,"allowed_tools":["Task"]}}]},
        {"group_id":"ambiguous","item_ids":["pause"],"source_quote":"那个先等等","operations":[],"clarification":"请指定需要暂停的 Work"}
    ]}});
    let result = invoke(&f.tool("work_update"), args.clone(), f.ctx()).await;
    assert!(result.0);
    assert_eq!(result.1["resolution"]["groups"][0]["status"], "committed");
    assert_eq!(
        result.1["resolution"]["groups"][1]["status"],
        "needs_clarification"
    );
    assert_eq!(service.published().unwrap().1.tickets.len(), 1);
    let mut forged = args;
    forged["user_id"] = json!("someone-else");
    assert_eq!(
        invoke(&f.tool("work_update"), forged, f.ctx()).await.1["status_code"],
        422
    );
}

#[tokio::test]
async fn model_raw_contracts_require_user_acceptance_on_both_mutation_tools() {
    for name in ["work_update", "work_dispatch"] {
        let f = Fixture::new().await;
        let tool = f.tool(name);
        let mut contract = json!({"title":"A","objective":"result","constraints":[],"acceptance":["evidence"],"user_acceptance_required":false,"allowed_tools":["Task"]});
        let create = json!({"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],"contract":contract});
        let before = f.app.service().unwrap().published().unwrap();
        assert!(matches!(
            tool.call(f.mutation("false-create", json!([create])), &f.ctx())
                .await,
            Err(Error::ScopeDenied(_))
        ));
        assert_eq!(
            canonical_bytes(&f.app.service().unwrap().published().unwrap()).unwrap(),
            canonical_bytes(&before).unwrap()
        );
        contract["user_acceptance_required"] = json!(true);
        let create = json!({"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],"contract":contract});
        let result = f
            .tool("work_update")
            .call(f.mutation("true-create", json!([create])), &f.ctx())
            .await
            .unwrap();
        let work = result["receipt"]["ids"]["work"].as_str().unwrap();
        let before = f.app.service().unwrap().published().unwrap();
        contract["user_acceptance_required"] = json!(false);
        let update = json!({"op":"update_contract","work_id":work,"contract":contract});
        assert!(matches!(
            tool.call(f.mutation("false-steer", json!([update])), &f.ctx())
                .await,
            Err(Error::ScopeDenied(_))
        ));
        assert_eq!(
            canonical_bytes(&f.app.service().unwrap().published().unwrap()).unwrap(),
            canonical_bytes(&before).unwrap()
        );
        contract["user_acceptance_required"] = json!(true);
        contract["objective"] = json!("updated result");
        let update = json!({"op":"update_contract","work_id":work,"contract":contract});
        assert!(f
            .tool("work_update")
            .call(f.mutation("true-steer", json!([update])), &f.ctx())
            .await
            .is_ok());
    }
}

#[tokio::test]
async fn verified_typed_user_can_explicitly_choose_supervisor_acceptance() {
    let f = Fixture::new().await;
    let (service, authority) = f
        .app
        .authority(Principal::User {
            user_id: "host-owner".into(),
        })
        .await
        .unwrap();
    let contract = Contract {
        title: "typed".into(),
        objective: "result".into(),
        constraints: vec![],
        acceptance: vec!["evidence".into()],
        user_acceptance_required: false,
        allowed_tools: std::collections::BTreeSet::from(["Task".into()]),
    };
    let create = service
        .prepare_command(
            &authority,
            "typed-false-create",
            vec![Operation::Create {
                temp_id: "work".into(),
                kind: TicketKind::Work,
                parent: None,
                contract: contract.clone(),
                depends_on: Default::default(),
            }],
        )
        .unwrap();
    let receipt = f
        .app
        .update(authority.principal().clone(), &create)
        .await
        .unwrap();
    let id = &receipt.ids["work"];
    let mut changed = contract;
    changed.objective = "typed update".into();
    let update = service
        .prepare_command(
            &authority,
            "typed-false-update",
            vec![Operation::UpdateContract {
                work_id: id.clone(),
                contract: changed,
            }],
        )
        .unwrap();
    f.app
        .update(authority.principal().clone(), &update)
        .await
        .unwrap();
    assert!(
        !service.published().unwrap().1.tickets[id]
            .contract
            .user_acceptance_required
    );
}

#[test]
fn all_model_contract_schema_branches_require_user_acceptance() {
    fn inspect(value: &Value, count: &mut usize) {
        if let Some(contract) = value
            .get("properties")
            .and_then(|p| p.get("user_acceptance_required"))
        {
            assert_eq!(contract["const"], true);
            *count += 1;
        }
        match value {
            Value::Object(values) => values.values().for_each(|v| inspect(v, count)),
            Value::Array(values) => values.iter().for_each(|v| inspect(v, count)),
            _ => {}
        }
    }
    let mut count = 0;
    inspect(&schema::parameters("work_update"), &mut count);
    inspect(&schema::parameters("work_dispatch"), &mut count);
    assert!(
        count >= 4,
        "must cover both semantic Create/Steer and raw Create/UpdateContract"
    );
}
