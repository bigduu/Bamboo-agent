//! Real local Host/native Worker processes with a controlled HTTP provider.
//! This is Runtime acceptance; it makes no real-model semantic-quality claim.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use fixture::{command, get, post, Fixture};
use serde_json::{json, Value};
use std::{sync::atomic::Ordering, time::Duration};

#[actix_web::test]
async fn actual_ticket_native_worker_create_plan_submit_accept_restart() {
    let mut f = Fixture::new().await;
    let client = f.client.clone();
    let base = f.base.clone();
    let data = f.data.clone();
    let calls = f.probe.clone();
    let create = command(&client, &base, "create-one", json!([
        {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],
            "contract":{"title":"TICKET_E2E_1481","objective":"Return TICKET_E2E_1481_DONE and update only your private plan.","constraints":["Never alter the Supervisor plan"],"acceptance":["Exact output bytes"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
        {"op":"ready","work_id":"work"}])).await;
    let created = post(&client, &base, "/tickets/update", &create).await;
    let work = created["ids"]["work"].as_str().unwrap();
    let start_request = command(
        &client,
        &base,
        "start-one",
        json!([{"op":"start","work_id":work,"temp_id":"assignment","workspace":null}]),
    )
    .await;
    let started = post(&client, &base, "/tickets/dispatch", &start_request).await;
    eprintln!("Ticket dispatch observation: {}", started["runtime"]);
    assert_eq!(started["status"], "accepted_for_dispatch");
    assert_eq!(started["errors"], json!([]), "{started}");
    let key = started["runtime"][0]["dispatch_key"].as_str().unwrap();
    let inspect = json!({"ids":[work],"depth":0,"budget_bytes":65536,"fixed_commit":null});
    let view = tokio::time::timeout(Duration::from_secs(90), async {
        let mut last_seq = Value::Null;
        loop {
            let view = post(&client, &base, "/tickets/inspect", &inspect).await;
            if last_seq != view["snapshot"]["seq"] {
                eprintln!(
                    "Ticket seq {}; Work {}; Assignment {}; plan revision {}; provider calls {}",
                    view["snapshot"]["seq"],
                    view["data"][0]["ticket"]["state"],
                    view["data"][0]["assignments"][0]["state"],
                    view["data"][0]["assignments"][0]["plan"]["plan_revision"],
                    calls.calls.load(Ordering::SeqCst)
                );
                last_seq = view["snapshot"]["seq"].clone();
            }
            if view["data"][0]["ticket"]["state"] == "submitted" {
                break view;
            }
            assert_ne!(
                view["data"][0]["assignments"][0]["state"],
                "failed",
                "native Worker failed before submission: {}",
                std::fs::read_to_string(data.join("host.log")).unwrap()
            );
            assert!(
                f.host.as_mut().unwrap().0.try_wait().unwrap().is_none(),
                "Host exited during Worker run"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "actual Ticket Worker completion: {}",
            std::fs::read_to_string(data.join("host.log")).unwrap()
        )
    });
    assert_eq!(
        calls.calls.load(Ordering::SeqCst),
        2,
        "actual native Task callback then final output"
    );
    assert_eq!(
        view["data"][0]["assignments"][0]["plan"]["steps"][0]["id"],
        "own-step"
    );
    assert_eq!(
        view["data"][0]["assignments"][0]["process_stopped"], true,
        "owned native process was reaped before resource release"
    );
    let submission = view["data"][0]["ticket"]["current_submission"]
        .as_str()
        .unwrap();
    let hash = view["data"][0]["submissions"][0]["artifacts"][0]["sha256"]
        .as_str()
        .unwrap();
    let output = client
        .get(format!("{base}/tickets/artifacts/{hash}"))
        .send()
        .await
        .unwrap();
    assert!(output.status().is_success());
    assert_eq!(output.text().await.unwrap(), "TICKET_E2E_1481_DONE");
    let child = get(&client, &base, &format!("/tickets/dispatch/{key}")).await;
    assert_eq!(child["status"], "terminal");
    let run = child["receipt"]["run_id"].clone();
    let accept = command(&client, &base, "accept-one", json!([{"op":"accept","work_id":work,"submission_id":submission,"evidence":["Verified exact Artifact output"]}])).await;
    post(&client, &base, "/tickets/update", &accept).await;
    assert_eq!(
        post(&client, &base, "/tickets/inspect", &inspect).await["data"][0]["ticket"]["state"],
        "accepted"
    );
    f.restart().await;
    let replay = post(&client, &base, "/tickets/dispatch", &start_request).await;
    assert_eq!(replay["receipt"], started["receipt"]);
    assert_eq!(
        replay["runtime"][0]["observation"]["receipt"]["run_id"],
        run
    );
    assert_eq!(
        calls.calls.load(Ordering::SeqCst),
        2,
        "restart does not reexecute a terminal dispatch"
    );
    assert_eq!(
        post(&client, &base, "/tickets/inspect", &inspect).await["data"][0]["ticket"]["state"],
        "accepted"
    );
    f.finish().await;
}
