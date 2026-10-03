//! Actual Host/native processes; synthetic provider, no production data.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use bamboo_agent::ticket_cli;
use bamboo_engine::ticket_worker_plan::tickets::*;
use fixture::{command, get, post, Fixture};
use serde_json::{json, Value};
use std::{process::Command, sync::atomic::Ordering, time::Duration};

async fn submitted(f: &Fixture, work: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let view = post(
                &f.client,
                &f.base,
                "/tickets/inspect",
                &json!({"ids":[work],"depth":0,"budget_bytes":65536,"fixed_commit":null}),
            )
            .await;
            if view["data"][0]["ticket"]["state"] == "submitted"
                && view["data"][0]["assignments"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|a| a["process_stopped"] == true)
            {
                break view;
            }
            assert_ne!(view["data"][0]["ticket"]["state"], "blocked", "{view}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "native completion deadline: {}",
            std::fs::read_to_string(f.data.join("host.log")).unwrap()
        )
    })
}

#[actix_web::test]
async fn actual_stopped_host_cli_migration_terminal_receipt_and_fresh_generation() {
    let mut f = Fixture::new().await;
    let create = command(&f.client, &f.base, "migration-create", json!([
        {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],"contract":{"title":"TICKET_E2E_1481","objective":"Return TICKET_E2E_1481_DONE using your private Task plan.","constraints":["No external side effects"],"acceptance":["Exact output"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
        {"op":"ready","work_id":"work"}])).await;
    let created = post(&f.client, &f.base, "/tickets/update", &create).await;
    let work = created["ids"]["work"].as_str().unwrap().to_owned();
    let start = command(
        &f.client,
        &f.base,
        "migration-start",
        json!([{"op":"start","work_id":work,"temp_id":"assignment","workspace":null}]),
    )
    .await;
    let dispatched = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
    assert_eq!(dispatched["errors"], json!([]));
    let key = dispatched["runtime"][0]["dispatch_key"].as_str().unwrap();
    let before = submitted(&f, &work).await;
    let terminal = get(&f.client, &f.base, &format!("/tickets/dispatch/{key}")).await;
    assert_eq!(terminal["status"], "terminal");
    let submission = before["data"][0]["ticket"]["current_submission"].clone();
    let artifact = before["data"][0]["submissions"][0]["artifacts"][0]["sha256"]
        .as_str()
        .unwrap()
        .to_owned();
    let source = f.data.clone();
    let destination = f.temp.canonicalize().unwrap().join("migrated");
    std::fs::create_dir(&destination).unwrap();
    assert!(
        ticket_cli::migration_plan(&source, &destination, "offline-transfer")
            .await
            .is_err(),
        "live Host OS authority lock must refuse offline migration"
    );
    drop(f.host.take()); // kill and wait the exact fixture Host; all Workers were already reaped.
    let plan = ticket_cli::migration_plan(&source, &destination, "offline-transfer")
        .await
        .unwrap();
    let request: MigrationRequest = serde_json::from_value(plan["request"].clone()).unwrap();
    let request_path = f.temp.join("migration-request.json");
    std::fs::write(&request_path, serde_json::to_vec_pretty(&request).unwrap()).unwrap();
    let cli = || {
        let out = Command::new(env!("CARGO_BIN_EXE_bamboo"))
            .args(["tickets", "migrate", "--data-dir"])
            .arg(&source)
            .arg("--request")
            .arg(&request_path)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "CLI: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice::<Value>(&out.stdout).unwrap()
    };
    let first = cli();
    assert_eq!(first["receipt"], cli()["receipt"]);
    assert!(first["destination_epoch"].as_u64().unwrap() > request.expected_epoch);
    let old = TicketService::open(&request.source_root, request.binding.clone()).unwrap();
    assert!(matches!(old.health(), Health::ReadOnly { .. }));
    drop(old);
    // Recreate this fixture's synthetic route. A running Host may already have
    // replaced config credentials with private references; those never migrate.
    std::fs::write(destination.join("config.json"), &f.synthetic_config).unwrap();
    f.data = destination;
    let mut host = fixture::start(&f.data, f.port);
    fixture::ready(&f.client, &f.base, &mut host, &f.data).await;
    f.host = Some(host);
    let after = submitted(&f, &work).await;
    assert_eq!(after["data"][0]["ticket"]["current_submission"], submission);
    assert_eq!(
        after["data"][0]["submissions"],
        before["data"][0]["submissions"]
    );
    let replay = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
    assert_eq!(replay["receipt"], dispatched["receipt"]);
    assert_eq!(replay["runtime"][0]["observation"]["status"], "terminal");
    assert_eq!(
        replay["runtime"][0]["observation"]["receipt"], terminal["receipt"],
        "old terminal lookup preserves the run receipt without a new admission"
    );
    assert_eq!(
        f.probe.calls.load(Ordering::SeqCst),
        2,
        "migration cannot rerun a terminal key"
    );
    let output = f
        .client
        .get(format!("{}/tickets/artifacts/{artifact}", f.base))
        .send()
        .await
        .unwrap();
    assert!(output.status().is_success());
    assert_eq!(output.text().await.unwrap(), "TICKET_E2E_1481_DONE");
    let accept = command(&f.client, &f.base, "migration-accept", json!([{"op":"accept","work_id":work,"submission_id":submission,"evidence":["Verified immutable transferred Artifact"]}])).await;
    post(&f.client, &f.base, "/tickets/update", &accept).await;
    let reopen = command(
        &f.client,
        &f.base,
        "migration-reopen",
        json!([{"op":"reopen","work_id":work}]),
    )
    .await;
    post(&f.client, &f.base, "/tickets/update", &reopen).await;
    let next = command(
        &f.client,
        &f.base,
        "migration-generation-2",
        json!([{"op":"start","work_id":work,"temp_id":"assignment","workspace":null}]),
    )
    .await;
    let admitted = post(&f.client, &f.base, "/tickets/dispatch", &next).await;
    assert_eq!(admitted["errors"], json!([]), "{admitted}");
    let final_view = submitted(&f, &work).await;
    assert_eq!(
        final_view["data"][0]["submissions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(final_view["data"][0]["ticket"]["generation"], 2);
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 4);
    eprintln!("P9 actual migration: original readonly, full Artifact/receipt preserved, old intent fenced, explicit acceptance, fresh generation 2 submitted; source_snapshot_bytes={}", plan["source_snapshot_bytes"]);
    f.finish().await;
}

#[actix_web::test]
async fn actual_feature_rollback_keeps_inflight_result_and_records() {
    let mut f = Fixture::new().await;
    let create = command(&f.client, &f.base, "flag-create", json!([
        {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],"contract":{"title":"flag rollback","objective":"WAIT_FOR_CANCEL TICKET_E2E_1481","constraints":[],"acceptance":["Exact output"],"user_acceptance_required":true,"allowed_tools":["Task"]}},
        {"op":"ready","work_id":"work"}])).await;
    let work = post(&f.client, &f.base, "/tickets/update", &create).await["ids"]["work"]
        .as_str()
        .unwrap()
        .to_owned();
    let start = command(
        &f.client,
        &f.base,
        "flag-start",
        json!([{"op":"start","work_id":work,"temp_id":"assignment","workspace":null}]),
    )
    .await;
    assert_eq!(
        post(&f.client, &f.base, "/tickets/dispatch", &start).await["errors"],
        json!([])
    );
    tokio::time::timeout(Duration::from_secs(30), async {
        while f.probe.held.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    post(
        &f.client,
        &f.base,
        "/bamboo/config",
        &json!({"features":{"ticket_mutation":false,"ticket_dispatch":false}}),
    )
    .await;
    let scope = get(&f.client, &f.base, "/tickets/scope").await;
    assert_eq!(scope["mutation_enabled"], false);
    assert_eq!(scope["dispatch_enabled"], false);
    let refused = f
        .client
        .post(format!("{}/tickets/dispatch", f.base))
        .json(&start)
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status().as_u16(), 503);
    f.probe.release.notify_waiters();
    let result = submitted(&f, &work).await;
    assert_eq!(
        result["data"][0]["submissions"].as_array().unwrap().len(),
        1
    );
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);
    f.restart().await;
    let saved = submitted(&f, &work).await;
    assert_eq!(
        saved["data"][0]["submissions"],
        result["data"][0]["submissions"]
    );
    assert_eq!(
        get(&f.client, &f.base, "/tickets/scope").await["mutation_enabled"],
        false
    );
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);
    f.finish().await;
}
