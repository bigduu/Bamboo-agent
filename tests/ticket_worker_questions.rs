//! Real five Native Workers; controlled routing evidence, not model semantics.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use fixture::{command, get, post, Fixture};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::atomic::Ordering, time::Duration};

async fn views(f: &Fixture, ids: &[String]) -> Value {
    post(
        &f.client,
        &f.base,
        "/tickets/inspect",
        &json!({"ids":ids,"depth":0,"budget_bytes":65536}),
    )
    .await
}
async fn completed_question(store: &bamboo_storage::SessionStoreV2, row: &Value) -> bool {
    use bamboo_agent_core::storage::Storage;
    let Some(id) = row["assignments"][0]["runtime"]["session_id"].as_str() else {
        return false;
    };
    let Ok(Some(child)) = store.load_session(id).await else {
        return false;
    };
    child.last_run_status().as_deref() == Some("completed")
        && child
            .metadata
            .contains_key("ticket.worker.question_yield.v1")
        && child.messages.last().is_some_and(|last| {
            serde_json::to_value(&last.role).ok().as_ref() == Some(&json!("tool"))
        })
}

async fn await_views(
    f: &Fixture,
    ids: &[String],
    ready: impl Fn(&Value, &[bool]) -> bool,
) -> Value {
    let store = bamboo_storage::SessionStoreV2::new(f.data.clone())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let value = views(f, ids).await;
            let mut completed = Vec::new();
            for row in value["data"].as_array().unwrap() {
                completed.push(completed_question(&store, row).await);
            }
            if ready(&value, &completed) {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "five questions deadline; calls={}, questions={}; retained Host log: {}",
            f.probe.calls.load(Ordering::SeqCst),
            f.probe.questions.load(Ordering::SeqCst),
            f.data.join("host.log").display()
        )
    })
}

#[actix_web::test]
async fn five_native_questions_release_workers_survive_restart_and_answer_e_b_d_a_c() {
    let mut f = Fixture::new().await;
    let mut operations = Vec::new();
    for letter in ["A", "B", "C", "D", "E"] {
        operations.push(json!({"op":"create","temp_id":letter,"kind":"work","parent":null,"depends_on":[],"contract":{
            "title":letter,"objective":format!("TICKET_QUESTION_E2E:{letter}"),"constraints":["Own private plan and answer only"],"acceptance":["Exact answer"],"user_acceptance_required":true,"allowed_tools":["Task"]}}));
        operations.push(json!({"op":"ready","work_id":letter}));
    }
    let created = post(
        &f.client,
        &f.base,
        "/tickets/update",
        &command(&f.client, &f.base, "create-five", json!(operations)).await,
    )
    .await;
    let works: BTreeMap<String, String> = ["A", "B", "C", "D", "E"]
        .into_iter()
        .map(|l| (l.into(), created["ids"][l].as_str().unwrap().into()))
        .collect();
    let ids: Vec<_> = works.values().cloned().collect();
    let starts:Vec<_> = works.iter().map(|(letter,id)|json!({"op":"start","work_id":id,"temp_id":format!("assignment-{letter}"),"workspace":null})).collect();
    let dispatched = post(
        &f.client,
        &f.base,
        "/tickets/dispatch",
        &command(&f.client, &f.base, "start-five", json!(starts)).await,
    )
    .await;
    assert_eq!(dispatched["errors"], json!([]), "{dispatched}");
    let pending = await_views(&f, &ids, |v, completed| {
        v["data"]
            .as_array()
            .unwrap()
            .iter()
            .zip(completed)
            .all(|(row, completed)| {
                row["requests"].as_array().unwrap().len() == 1
                    && row["assignments"][0]["process_stopped"] == true
                    && *completed
            })
    })
    .await;
    assert_eq!(f.probe.questions.load(Ordering::SeqCst), 5);
    assert_eq!(
        f.probe.calls.load(Ordering::SeqCst),
        5,
        "question yield must skip another model round"
    );
    for row in pending["data"].as_array().unwrap() {
        assert_eq!(row["ticket"]["state"], "blocked");
        assert!(row["ticket"]["active_assignment"].is_null());
        assert!(
            row["submissions"].as_array().unwrap().is_empty(),
            "question is not a delivery"
        );
        assert_eq!(
            row["assignments"][0]["plan"]["steps"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    let requests: BTreeMap<String, Value> = pending["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["ticket"]["contract"]["title"].as_str().unwrap().into(),
                row["requests"][0].clone(),
            )
        })
        .collect();
    f.restart().await;
    assert_eq!(
        get(&f.client, &f.base, "/tickets/overview").await["data"]["open_questions"],
        5
    );
    for (index, letter) in ["E", "B", "D", "A", "C"].into_iter().enumerate() {
        let request = &requests[letter];
        let answer=command(&f.client,&f.base,&format!("answer-{letter}"),json!([{"op":"answer","request_id":request["id"],"prompt_revision":request["prompt_revision"],"answer":format!("答案 {letter}")}])).await;
        post(&f.client, &f.base, "/tickets/update", &answer).await;
        assert_eq!(
            get(&f.client, &f.base, "/tickets/overview").await["data"]["open_questions"],
            4 - index
        );
        let fresh = command(
            &f.client,
            &f.base,
            &format!("resume-{letter}"),
            json!([{"op":"start","work_id":works[letter],"temp_id":"fresh","workspace":null}]),
        )
        .await;
        let resumed = post(&f.client, &f.base, "/tickets/dispatch", &fresh).await;
        assert_eq!(resumed["errors"], json!([]), "{resumed}");
        let result = await_views(&f, &[works[letter].clone()], |v, _| {
            v["data"][0]["ticket"]["state"] == "submitted"
        })
        .await;
        assert_eq!(result["data"][0]["ticket"]["generation"], 2);
        assert_eq!(
            result["data"][0]["requests"][0]["answer"],
            format!("答案 {letter}")
        );
    }
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 15);
    assert_eq!(f.probe.answer_checks.load(Ordering::SeqCst), 10);
    assert_eq!(
        get(&f.client, &f.base, "/tickets/overview").await["data"]["needs_acceptance"],
        5
    );
    f.finish().await;
}

#[actix_web::test]
async fn question_waiter_reads_completed_runtime_sidecar_over_stale_main() {
    use bamboo_agent_core::storage::Storage;
    use bamboo_domain::{Message, Session};
    let temp = tempfile::tempdir().unwrap();
    let store = bamboo_storage::SessionStoreV2::new(temp.path().to_path_buf())
        .await
        .unwrap();
    let root = Session::new("bamboo-default-supervisor", "fixture-model");
    store.save_session(&root).await.unwrap();
    let mut child = Session::new_child(
        "runtime-only-question",
        &root.id,
        "fixture-model",
        "Question",
    );
    child.metadata.insert(
        "ticket.worker.question_yield.v1".into(),
        "request-id".into(),
    );
    child.add_message(Message::tool_result("question-tool", "request created"));
    child.set_last_run_status("running");
    store.save_session(&child).await.unwrap();
    let main_path = temp
        .path()
        .join(store.resolve_rel_path(&child.id).await.unwrap())
        .join("session.json");
    let before = std::fs::read(&main_path).unwrap();
    child.set_last_run_status("completed");
    store.save_runtime_state(&child).await.unwrap();
    assert_eq!(
        std::fs::read(&main_path).unwrap(),
        before,
        "runtime-only write preserves Main bytes"
    );
    let canonical = store.load_session(&child.id).await.unwrap().unwrap();
    assert_eq!(canonical.last_run_status().as_deref(), Some("completed"));
    let row = json!({"assignments":[{"runtime":{"session_id":child.id}}]});
    assert!(
        completed_question(&store, &row).await,
        "completed canonical Child must not be rejected by stale Main metadata"
    );
}
