//! Actual Host/native processes with a controlled provider; no semantic PASS.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use fixture::{command, post, Fixture};
use serde_json::{json, Value};
use std::{sync::atomic::Ordering, time::Duration};

impl Fixture {
    async fn create_work(&self, title: &str, objective: &str, dependencies: Value) -> String {
        let create = command(&self.client, &self.base, &format!("create-{title}"), json!([
            {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":dependencies,
            "contract":{"title":title,"objective":objective,"constraints":["Own private plan only"],"acceptance":["Exact output"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
            {"op":"ready","work_id":"work"}])).await;
        post(&self.client, &self.base, "/tickets/update", &create).await["ids"]["work"]
            .as_str()
            .unwrap()
            .into()
    }
    async fn dispatch_ops(&self, id: &str, operations: Value) -> Value {
        let request = command(&self.client, &self.base, id, operations).await;
        let result = post(&self.client, &self.base, "/tickets/dispatch", &request).await;
        assert_eq!(result["status"], "accepted_for_dispatch");
        assert_eq!(result["errors"], json!([]), "{result}");
        result
    }
    async fn wait_view(&self, work: &str, predicate: impl Fn(&Value) -> bool) -> Value {
        tokio::time::timeout(Duration::from_secs(25), async {
            loop {
                let view = post(
                    &self.client,
                    &self.base,
                    "/tickets/inspect",
                    &json!({"ids":[work],"depth":0,"budget_bytes":65536}),
                )
                .await;
                if predicate(&view["data"][0]) {
                    break view["data"][0].clone();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "control deadline; calls={}, held={}; {}",
                self.probe.calls.load(Ordering::SeqCst),
                self.probe.held.load(Ordering::SeqCst),
                std::fs::read_to_string(self.data.join("host.log")).unwrap()
            )
        })
    }
}

#[actix_web::test]
async fn actual_pause_interrupts_worker_stays_paused_and_resumes_with_fresh_generation() {
    let f = Fixture::new().await;
    let work = f
        .create_work("pause", "WAIT_FOR_CANCEL TICKET_E2E_1481", json!([]))
        .await;
    f.dispatch_ops(
        "start",
        json!([{"op":"start","work_id":work,"temp_id":"first","workspace":null}]),
    )
    .await;
    f.wait_view(&work, |_| f.probe.held.load(Ordering::SeqCst) == 1)
        .await;
    let before = std::time::Instant::now();
    f.dispatch_ops(
        "pause",
        json!([{"op":"pause","work_id":work,"reason":"User postponed work"}]),
    )
    .await;
    assert!(before.elapsed() < Duration::from_secs(2));
    let paused = f
        .wait_view(&work, |v| v["assignments"][0]["process_stopped"] == true)
        .await;
    assert_eq!(paused["ticket"]["state"], "blocked");
    assert_eq!(paused["ticket"]["paused"], true);
    assert!(paused["ticket"]["active_assignment"].is_null());
    f.dispatch_ops("resume-start", json!([{"op":"ready","work_id":work},{"op":"start","work_id":work,"temp_id":"second","workspace":null}])).await;
    let next = f
        .wait_view(&work, |_| f.probe.held.load(Ordering::SeqCst) == 2)
        .await;
    assert_eq!(next["assignments"].as_array().unwrap().len(), 2);
    assert_eq!(next["ticket"]["generation"], 2);
    assert!(next["submissions"].as_array().unwrap().is_empty());
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 4);
    f.dispatch_ops("cancel-second", json!([{"op":"cancel","work_id":work}]))
        .await;
    f.wait_view(&work, |v| {
        v["assignments"]
            .as_array()
            .unwrap()
            .iter()
            .all(|a| a["process_stopped"] == true)
    })
    .await;
    f.finish().await;
}

#[actix_web::test]
async fn actual_steer_stops_old_worker_before_explicit_retry() {
    let f = Fixture::new().await;
    let work = f
        .create_work("steer", "WAIT_FOR_CANCEL TICKET_E2E_1481", json!([]))
        .await;
    f.dispatch_ops(
        "start",
        json!([{"op":"start","work_id":work,"temp_id":"first","workspace":null}]),
    )
    .await;
    let running = f
        .wait_view(&work, |_| f.probe.held.load(Ordering::SeqCst) == 1)
        .await;
    let mut contract = running["ticket"]["contract"].clone();
    contract["objective"] = json!("Changed TICKET_E2E_1481");
    f.dispatch_ops(
        "steer",
        json!([{"op":"update_contract","work_id":work,"contract":contract}]),
    )
    .await;
    let stopped = f
        .wait_view(&work, |v| v["assignments"][0]["process_stopped"] == true)
        .await;
    assert_eq!(stopped["ticket"]["contract_revision"], 2);
    assert_eq!(stopped["ticket"]["state"], "blocked");
    assert_eq!(stopped["assignments"].as_array().unwrap().len(), 1);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);
    f.dispatch_ops("explicit-retry", json!([{"op":"reopen","work_id":work},{"op":"start","work_id":work,"temp_id":"second","workspace":null}])).await;
    let submitted = f
        .wait_view(&work, |v| v["ticket"]["state"] == "submitted")
        .await;
    assert_eq!(submitted["ticket"]["generation"], 2);
    let current = submitted["ticket"]["current_submission"].as_str().unwrap();
    let submission = submitted["submissions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["id"] == current)
        .unwrap();
    assert_eq!(submission["contract_revision"], 2);
    assert_eq!(submission["stale"], false);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 4);
    f.finish().await;
}

#[actix_web::test]
async fn actual_downstream_worker_receives_verified_accepted_artifact_bytes() {
    let f = Fixture::new().await;
    let upstream = f
        .create_work("upstream", "TICKET_E2E_1481", json!([]))
        .await;
    f.dispatch_ops(
        "up-start",
        json!([{"op":"start","work_id":upstream,"temp_id":"up","workspace":null}]),
    )
    .await;
    let result = f
        .wait_view(&upstream, |v| v["ticket"]["state"] == "submitted")
        .await;
    let accept = command(&f.client, &f.base, "accept-upstream", json!([{"op":"accept","work_id":upstream,"submission_id":result["ticket"]["current_submission"],"evidence":["User verified exact bytes"]}])).await;
    post(&f.client, &f.base, "/tickets/update", &accept).await;
    let downstream = f
        .create_work(
            "downstream",
            "TICKET_ACCEPTED_INPUT_E2E TICKET_E2E_1481",
            json!([upstream]),
        )
        .await;
    f.dispatch_ops(
        "down-start",
        json!([{"op":"start","work_id":downstream,"temp_id":"down","workspace":null}]),
    )
    .await;
    let result = f
        .wait_view(&downstream, |v| v["ticket"]["state"] == "submitted")
        .await;
    assert_eq!(
        result["submissions"][0]["dependency_inputs"][0]["contract_revision"],
        1
    );
    assert_eq!(f.probe.input_checks.load(Ordering::SeqCst), 2);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 4);
    f.finish().await;
}
