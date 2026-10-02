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
