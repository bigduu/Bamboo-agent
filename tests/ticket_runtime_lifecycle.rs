//! Actual isolated Host/native processes; controlled HTTP provider only.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use fixture::{command, get, post, Fixture};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::time::Duration;

#[actix_web::test]
async fn actual_ticket_cancel_interrupts_owned_worker_and_confirms_stop() {
    let f = Fixture::new().await;
    let (work, _, _) = f.start_held().await;
    let cancel = command(
        &f.client,
        &f.base,
        "cancel-held",
        json!([{"op":"cancel","work_id":work}]),
    )
    .await;
    let before = std::time::Instant::now();
    post(&f.client, &f.base, "/tickets/update", &cancel).await;
    assert!(
        before.elapsed() < std::time::Duration::from_secs(2),
        "user turn must not await the held provider"
    );
    let stopped = f
        .until(&work, |view| {
            view["data"][0]["assignments"][0]["state"] == "cancelled"
        })
        .await;
    assert_eq!(stopped["data"][0]["ticket"]["state"], "cancelled");
    assert_eq!(
        stopped["data"][0]["assignments"][0]["process_stopped"],
        true
    );
    assert!(stopped["data"][0]["submissions"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);
    f.finish().await;
}

#[actix_web::test]
async fn actual_host_loss_fences_unknown_run_without_automatic_redispatch() {
    let mut f = Fixture::new().await;
    let (work, start_request, dispatched) = f.start_held().await;
    let key = dispatched["runtime"][0]["dispatch_key"].as_str().unwrap();
    let running = get(&f.client, &f.base, &format!("/tickets/dispatch/{key}")).await;
    assert_eq!(running["status"], "admitted");
    f.restart().await;
    let view = f
        .until(&work, |view| {
            view["data"][0]["assignments"][0]["state"] == "outcome_unknown"
        })
        .await;
    assert_eq!(view["data"][0]["ticket"]["state"], "blocked");
    assert_ne!(
        view["data"][0]["assignments"][0]["process_stopped"], true,
        "lease/owner disappearance cannot invent stop proof"
    );
    let replay = post(&f.client, &f.base, "/tickets/dispatch", &start_request).await;
    assert_eq!(replay["receipt"], dispatched["receipt"]);
    assert_eq!(
        replay["runtime"][0]["observation"]["status"],
        "outcome_unknown"
    );
    assert_eq!(
        replay["runtime"][0]["observation"]["receipt"],
        running["receipt"]
    );
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert_eq!(
        f.probe.calls.load(Ordering::SeqCst),
        2,
        "same key cannot start a new native run after Host loss"
    );
    assert_eq!(view["data"][0]["assignments"].as_array().unwrap().len(), 1);
    f.finish().await;
}

impl Fixture {
    pub async fn start_held(&self) -> (String, Value, Value) {
        let create = command(&self.client, &self.base, "create-held", json!([
            {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],
                "contract":{"title":"TICKET_E2E_1481","objective":"WAIT_FOR_CANCEL TICKET_E2E_1481","constraints":["Own private plan only"],"acceptance":["Exact output"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
            {"op":"ready","work_id":"work"}])).await;
        let created = post(&self.client, &self.base, "/tickets/update", &create).await;
        let work = created["ids"]["work"].as_str().unwrap().to_owned();
        let request = command(
            &self.client,
            &self.base,
            "start-held",
            json!([{"op":"start","work_id":work,"temp_id":"assignment","workspace":null}]),
        )
        .await;
        let dispatched = post(&self.client, &self.base, "/tickets/dispatch", &request).await;
        assert_eq!(dispatched["errors"], json!([]), "{dispatched}");
        self.until(&work, |view| {
            assert_ne!(
                view["data"][0]["assignments"][0]["state"],
                "failed",
                "{}",
                self.log()
            );
            self.probe.held.load(Ordering::SeqCst) == 1
        })
        .await;
        (work, request, dispatched)
    }
    pub async fn until(&self, work: &str, predicate: impl Fn(&Value) -> bool) -> Value {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let view = post(
                    &self.client,
                    &self.base,
                    "/tickets/inspect",
                    &json!({"ids":[work],"depth":0,"budget_bytes":65536,"fixed_commit":null}),
                )
                .await;
                if predicate(&view) {
                    break view;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Ticket lifecycle deadline; provider calls {}, held {}: {}",
                self.probe.calls.load(Ordering::SeqCst),
                self.probe.held.load(Ordering::SeqCst),
                self.log()
            )
        })
    }
    pub fn log(&self) -> String {
        std::fs::read_to_string(self.data.join("host.log")).unwrap()
    }
}
