//! Actual Host exits at selected publication boundaries, fresh native Workers.
//! Controlled provider responses are not real-model semantic evaluation.
#![cfg(all(unix, feature = "ticket-runtime-fixtures"))]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use fixture::{command, get, post, Fixture};
use serde_json::{json, Value};
use std::{sync::atomic::Ordering, time::Duration};

async fn create(f: &Fixture) -> String {
    let request = command(&f.client, &f.base, "create", json!([
        {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],
            "contract":{"title":"TICKET_E2E_1481","objective":"Return TICKET_E2E_1481_DONE using only your private plan.","constraints":["Own plan only"],"acceptance":["Exact bytes"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
        {"op":"ready","work_id":"work"}])).await;
    post(&f.client, &f.base, "/tickets/update", &request).await["ids"]["work"]
        .as_str()
        .unwrap()
        .into()
}

async fn inspect(f: &Fixture, work: &str) -> Value {
    post(
        &f.client,
        &f.base,
        "/tickets/inspect",
        &json!({"ids":[work],"depth":0,"budget_bytes":65536,"fixed_commit":null}),
    )
    .await
}

async fn exited(f: &mut Fixture, prefix: &str) {
    let status = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            if let Some(status) = f.host.as_mut().unwrap().0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Host fault did not fire: {}",
            std::fs::read_to_string(f.data.join("host.log")).unwrap()
        )
    });
    assert_eq!(status.code(), Some(71));
    assert!(std::fs::read_to_string(f.data.join("host.log"))
        .unwrap()
        .contains(&format!("TICKET_FIXTURE_EXIT {prefix}")));
    eprintln!(
        "Verified actual Host exit 71 at {prefix}; Worker calls {}",
        f.probe.calls.load(Ordering::SeqCst)
    );
}

#[actix_web::test]
async fn actual_intent_exit_before_or_after_head_never_launches_unpublished_or_unknown_work() {
    for boundary in ["before_head", "after_head"] {
        let mut f = Fixture::with_fault(Some(("fixture-intent", boundary))).await;
        let work = create(&f).await;
        let start = command(
            &f.client,
            &f.base,
            "fixture-intent-start",
            json!([{"op":"start","work_id":work,"temp_id":"assignment","workspace":null}]),
        )
        .await;
        assert!(f
            .client
            .post(format!("{}/tickets/dispatch", f.base))
            .json(&start)
            .send()
            .await
            .is_err());
        exited(&mut f, "fixture-intent").await;
        f.restart().await;
        let view = inspect(&f, &work).await;
        if boundary == "before_head" {
            assert_eq!(view["data"][0]["ticket"]["state"], "ready");
            assert_eq!(view["data"][0]["assignments"], json!([]));
        } else {
            assert_eq!(
                view["data"][0]["assignments"][0]["state"],
                "outcome_unknown"
            );
            let replay = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
            let again = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
            assert_eq!(again["receipt"], replay["receipt"]);
            assert_eq!(
                replay["runtime"][0]["observation"]["status"],
                "outcome_unknown"
            );
            assert_eq!(
                inspect(&f, &work).await["data"][0]["assignments"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
        }
        assert_eq!(f.probe.calls.load(Ordering::SeqCst), 0);
        f.finish().await;
    }
}

#[actix_web::test]
async fn actual_admission_exit_retains_exact_prepared_run_without_worker_execution() {
    for boundary in ["before_head", "after_head"] {
        let mut f = Fixture::with_fault(Some(("runtime-admit/", boundary))).await;
        let work = create(&f).await;
        let start = command(
            &f.client,
            &f.base,
            "start",
            json!([{"op":"start","work_id":work,"temp_id":"assignment","workspace":null}]),
        )
        .await;
        let dispatched = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
        let key = dispatched["runtime"][0]["dispatch_key"].as_str().unwrap();
        exited(&mut f, "runtime-admit/").await;
        f.restart().await;
        let observed = get(&f.client, &f.base, &format!("/tickets/dispatch/{key}")).await;
        assert_eq!(observed["status"], "outcome_unknown");
        assert!(observed["receipt"]["run_id"].as_str().is_some());
        let replay = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
        assert_eq!(replay["receipt"], dispatched["receipt"]);
        assert_eq!(replay["runtime"][0]["observation"], observed);
        let view = inspect(&f, &work).await;
        assert_eq!(view["data"][0]["assignments"].as_array().unwrap().len(), 1);
        assert_eq!(
            view["data"][0]["assignments"][0]["state"],
            "outcome_unknown"
        );
        assert_eq!(
            f.probe.calls.load(Ordering::SeqCst),
            0,
            "RunSpec must follow confirmed admission publication"
        );
        f.finish().await;
    }
}

#[actix_web::test]
async fn actual_result_exit_recovers_submission_before_broker_ack_without_reexecution() {
    for boundary in ["before_head", "after_head"] {
        let mut f = Fixture::with_fault(Some(("runtime-submit/", boundary))).await;
        let work = create(&f).await;
        let start = command(
            &f.client,
            &f.base,
            "start",
            json!([{"op":"start","work_id":work,"temp_id":"assignment","workspace":null}]),
        )
        .await;
        let dispatched = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
        let key = dispatched["runtime"][0]["dispatch_key"].as_str().unwrap();
        exited(&mut f, "runtime-submit/").await;
        assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);
        f.restart().await;
        let view = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let view = inspect(&f, &work).await;
                if view["data"][0]["ticket"]["state"] == "submitted" {
                    break view;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Completion recovery failed: {}",
                std::fs::read_to_string(f.data.join("host.log")).unwrap()
            )
        });
        assert_eq!(view["data"][0]["assignments"][0]["process_stopped"], true);
        assert_eq!(view["data"][0]["submissions"].as_array().unwrap().len(), 1);
        assert_eq!(view["data"][0]["submissions"][0]["stale"], false);
        let hash = view["data"][0]["submissions"][0]["artifacts"][0]["sha256"]
            .as_str()
            .unwrap();
        let bytes = f
            .client
            .get(format!("{}/tickets/artifacts/{hash}", f.base))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(bytes, "TICKET_E2E_1481_DONE");
        let observed = get(&f.client, &f.base, &format!("/tickets/dispatch/{key}")).await;
        assert_eq!(observed["status"], "terminal");
        let run = observed["receipt"]["run_id"].clone();
        let replay = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
        assert_eq!(replay["receipt"], dispatched["receipt"]);
        assert_eq!(
            replay["runtime"][0]["observation"]["receipt"]["run_id"],
            run
        );
        assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);
        f.finish().await;
    }
}
