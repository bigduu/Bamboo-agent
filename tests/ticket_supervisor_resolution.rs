//! Actual Host/Inbox/model-tool/Runtime evidence with a controlled provider.
//! Real-model semantic evaluation lives in ticket_model_semantics separately.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use bamboo_domain::{Storage, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_storage::SessionStoreV2;
use fixture::{command, get, post, Fixture};
use serde_json::{json, Value};
use std::{sync::atomic::Ordering, time::Duration};

#[actix_web::test]
async fn actual_one_human_multi_intent_commits_groups_dispatches_and_replays_after_host_restart() {
    let mut f = Fixture::new().await;
    let mut ids = Vec::new();
    for title in ["A", "B", "D"] {
        let ops = json!([
            {"op":"create","temp_id":"w","kind":"work","parent":null,"depends_on":[],"contract":{"title":title,"objective":format!("独立报告{title}"),"constraints":[],"acceptance":["具体证据"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
            {"op":"ready","work_id":"w"}]);
        let c = command(&f.client, &f.base, &format!("setup-{title}"), ops).await;
        let receipt = post(&f.client, &f.base, "/tickets/update", &c).await;
        ids.push(receipt["ids"]["w"].as_str().unwrap().to_owned());
    }
    let ask = command(&f.client,&f.base,"ask-A",json!([{"op":"ask","work_id":ids[0],"temp_id":"q","prompt":"A 使用什么颜色？","action":null}])).await;
    post(&f.client, &f.base, "/tickets/update", &ask).await;
    let human = json!({"session_id":DEFAULT_SUPERVISOR_SESSION_ID,"message_id":"multi-human", "message":"TICKET_SUPERVISOR_E2E TICKET_MULTI_E2E；A使用绿色；B改为英文；新建并开始C；取消D", "model":"ticket-model","provider":"openai","model_ref":{"provider":"openai","model":"ticket-model"}});
    let admitted = post(&f.client, &f.base, "/chat", &human).await;
    post(
        &f.client,
        &f.base,
        &format!("/execute/{DEFAULT_SUPERVISOR_SESSION_ID}"),
        &json!({}),
    )
    .await;
    let store = SessionStoreV2::new(f.data.clone()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let root = store
                .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
                .await
                .unwrap()
                .unwrap();
            assert_ne!(
                root.last_run_status().as_deref(),
                Some("error"),
                "{:?}",
                root.last_run_error()
            );
            if root.last_run_status().as_deref() == Some("completed")
                && f.probe.held.load(Ordering::SeqCst) == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("short multi-intent Root return while C remains running");
    let snapshot = get(&f.client, &f.base, "/tickets/overview").await;
    assert_eq!(snapshot["data"]["work_count"], 4);
    let inspected = post(&f.client,&f.base,"/tickets/inspect",&json!({"ids":ids,"depth":0,"sections":["requests"],"budget_bytes":65536,"fixed_commit":snapshot["snapshot"]["commit"]})).await;
    let row = |title: &str| -> &Value {
        inspected["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["ticket"]["contract"]["title"] == title)
            .unwrap()
    };
    assert_eq!(row("A")["requests"][0]["answer"], "绿色");
    assert_eq!(row("B")["ticket"]["contract_revision"], 2);
    assert_eq!(row("B")["ticket"]["contract"]["objective"], "英文报告B");
    assert_eq!(row("D")["ticket"]["state"], "cancelled");
    let root = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    let tool = root
        .messages
        .iter()
        .find(|m| m.tool_call_id.as_deref() == Some("ticket-root-create"))
        .unwrap();
    let resolved: Value = serde_json::from_str(&tool.content).unwrap();
    let resolved = resolved
        .get("result")
        .and_then(Value::as_str)
        .map(|s| serde_json::from_str::<Value>(s).unwrap())
        .unwrap_or(resolved);
    assert_eq!(
        resolved["resolution"]["groups"].as_array().unwrap().len(),
        4
    );
    assert!(resolved["resolution"]["groups"]
        .as_array()
        .unwrap()
        .iter()
        .all(|g| g["status"] == "committed"));
    let calls = f.probe.root_calls.load(Ordering::SeqCst);
    let worker_calls = f.probe.calls.load(Ordering::SeqCst);
    assert_eq!(calls, 3);
    f.restart().await;
    let retry = post(&f.client, &f.base, "/chat", &human).await;
    assert_eq!(retry["ingress_seq"], admitted["ingress_seq"]);
    let after = get(&f.client, &f.base, "/tickets/overview").await;
    assert_eq!(after["data"]["work_count"], 4);
    assert_eq!(
        f.probe.root_calls.load(Ordering::SeqCst),
        calls,
        "restart cannot re-propose already resolved Human input"
    );
    assert_eq!(
        f.probe.calls.load(Ordering::SeqCst),
        worker_calls,
        "unconfirmed old execution is never automatically re-dispatched"
    );
    f.probe.release.notify_waiters();
    f.finish().await;
}
