//! Actual Host transport/Inbox/SDK/replay evidence; no model-semantic claim.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use bamboo_domain::{Storage, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_storage::SessionStoreV2;
use fixture::command;
use fixture::{get, post, Fixture};
use serde_json::json;
use std::time::Duration;

#[actix_web::test]
async fn authenticated_chat_references_survive_inbox_sdk_replay_and_exact_retry() {
    let mut f = Fixture::new().await;
    let request = json!({"session_id":DEFAULT_SUPERVISOR_SESSION_ID,
        "message":"普通聊天，不批准任何动作", "message_id":"human-delivery-1481",
        "thread_id":"topic-five", "in_reply_to":"question-e",
        "correlation_id":"trace-human-1481", "model":"ticket-model","provider":"openai"});
    let first = post(&f.client, &f.base, "/chat", &request).await;
    let replay = post(&f.client, &f.base, "/chat", &request).await;
    assert_eq!(first["message_id"], "human-delivery-1481");
    assert_eq!(first["ingress_seq"], replay["ingress_seq"]);
    let mut changed = request.clone();
    changed["message"] = json!("changed payload");
    assert_eq!(
        f.client
            .post(format!("{}/chat", f.base))
            .json(&changed)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    post(
        &f.client,
        &f.base,
        &format!("/execute/{DEFAULT_SUPERVISOR_SESSION_ID}"),
        &json!({}),
    )
    .await;
    let store = SessionStoreV2::new(f.data.clone()).await.unwrap();
    let root = tokio::time::timeout(Duration::from_secs(20), async {
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
            if root.last_run_status().as_deref() == Some("completed") {
                break root;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("actual SDK admission and short reply");
    let delivered: Vec<_> = root
        .messages
        .iter()
        .filter(|m| m.id == "human-delivery-1481")
        .collect();
    assert_eq!(
        delivered.len(),
        1,
        "HTTP must not append a second User message"
    );
    let marker = &delivered[0].metadata.as_ref().unwrap()["session_message"];
    assert_eq!(marker["thread_id"], "topic-five");
    assert_eq!(marker["in_reply_to"], "question-e");
    assert_eq!(marker["correlation_id"], "trace-human-1481");
    assert!(root
        .session_inbox_admission()
        .unwrap()
        .contains_str("human-delivery-1481"));
    let event = serde_json::to_value(bamboo_agent_core::AgentEvent::message_appended(
        &root.id,
        delivered[0],
    ))
    .unwrap();
    assert_eq!(event["thread_id"], marker["thread_id"]);
    assert_eq!(event["in_reply_to"], marker["in_reply_to"]);
    assert_eq!(event["correlation_id"], marker["correlation_id"]);
    let history = get(
        &f.client,
        &f.base,
        &format!("/history/{DEFAULT_SUPERVISOR_SESSION_ID}?projection=messages"),
    )
    .await;
    let visible = history["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["id"] == "human-delivery-1481")
        .unwrap();
    assert_eq!(visible["thread_id"], "topic-five");
    assert!(visible.get("metadata").is_none());
    f.restart().await;
    let cold = post(&f.client, &f.base, "/chat", &request).await;
    assert_eq!(cold["ingress_seq"], first["ingress_seq"]);
    assert_eq!(
        store
            .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
            .await
            .unwrap()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.id == "human-delivery-1481")
            .count(),
        1
    );
    let mut invalid = request.clone();
    invalid["message_id"] = json!("reserved-trace");
    invalid["correlation_id"] = json!("session-guidance-after-run:forged");
    assert_eq!(
        f.client
            .post(format!("{}/chat", f.base))
            .json(&invalid)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    f.finish().await;
}

#[actix_web::test]
async fn negotiated_request_buttons_bind_all_versions_and_only_approve_the_named_action() {
    let f = Fixture::new().await;
    let scope = get(&f.client, &f.base, "/tickets/scope").await;
    assert_eq!(scope["capabilities"]["precise_request_response_v1"], true);
    let mut ops = vec![];
    for letter in ["A", "B"] {
        ops.push(json!({"op":"create","temp_id":letter,"kind":"work","parent":null,"depends_on":[],"contract":{
            "title":letter,"objective":"synthetic approval fixture, no external effect", "constraints":[],"acceptance":["exact approval"],"user_acceptance_required":true,"allowed_tools":["Task"]}}));
        ops.push(json!({"op":"ask","work_id":letter,"temp_id":format!("q{letter}"),"prompt":format!("批准 {letter}？"),"action":{
            "kind":"fixture","target":letter,"data_hash":"0".repeat(64),"amount":"100 CNY","permissions":[],"risk":"no external effect"}}));
    }
    let created = post(
        &f.client,
        &f.base,
        "/tickets/update",
        &command(&f.client, &f.base, "button-create", json!(ops)).await,
    )
    .await;
    let ids = vec![created["ids"]["A"].clone(), created["ids"]["B"].clone()];
    let inspect = post(
        &f.client,
        &f.base,
        "/tickets/inspect",
        &json!({"ids":ids,"depth":0,"budget_bytes":65536}),
    )
    .await;
    let q = inspect["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["ticket"]["id"] == created["ids"]["A"])
        .unwrap()["requests"][0]
        .clone();
    let scope = get(&f.client, &f.base, "/tickets/scope").await;
    let body = json!({"operation_id":"button-approve-A", "binding":scope["binding"],
        "expected_seq":scope["overview"]["snapshot"]["seq"],"expected_epoch":scope["overview"]["snapshot"]["authority_epoch"],
        "target":{"request_id":q["id"],"work_id":q["work_id"],"assignment_id":q["assignment_id"],"generation":q["generation"],"contract_revision":q["contract_revision"],"prompt_revision":q["prompt_revision"]},
        "decision":{"kind":"approval","fingerprint":q["kind"]["fingerprint"],"approve":true}});
    for field in ["generation", "contract_revision", "prompt_revision"] {
        let mut bad = body.clone();
        bad["target"][field] = json!(999);
        assert_eq!(
            f.client
                .post(format!("{}/tickets/requests/respond", f.base))
                .json(&bad)
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::CONFLICT
        );
    }
    let mut foreign = body.clone();
    foreign["target"]["work_id"] = created["ids"]["B"].clone();
    assert_eq!(
        f.client
            .post(format!("{}/tickets/requests/respond", f.base))
            .json(&foreign)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    let mut bad_fp = body.clone();
    bad_fp["decision"]["fingerprint"] = json!("forged");
    assert_eq!(
        f.client
            .post(format!("{}/tickets/requests/respond", f.base))
            .json(&bad_fp)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    let first = post(&f.client, &f.base, "/tickets/requests/respond", &body).await;
    assert_eq!(
        post(&f.client, &f.base, "/tickets/requests/respond", &body).await,
        first
    );
    let updated = post(
        &f.client,
        &f.base,
        "/tickets/inspect",
        &json!({"ids":ids,"depth":0,"budget_bytes":65536}),
    )
    .await;
    for row in updated["data"].as_array().unwrap() {
        assert_eq!(
            row["requests"][0]["status"],
            if row["ticket"]["id"] == created["ids"]["A"] {
                "approved"
            } else {
                "open"
            }
        );
    }
    f.finish().await;
}
