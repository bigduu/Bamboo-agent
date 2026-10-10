//! Actual Host/native processes; synthetic provider, no production data.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use bamboo_agent::ticket_cli;
use bamboo_domain::{
    ActorActivationStatus, ActorDirectoryEntry, ActorLogicalState, Role, Storage, TaskList,
};
use bamboo_engine::{ticket_runtime, ticket_worker_plan::tickets::*};
use bamboo_storage::SessionStoreV2;
use fixture::{command, get, post, Fixture};
use serde_json::{json, Value};
use std::{process::Command, sync::atomic::Ordering, time::Duration};

#[actix_web::test]
async fn actual_completed_plain_root_import_preserves_full_proof_and_replays_without_acceptance() {
    let mut f = Fixture::new().await;
    let id = "ticket-1481-historical-root";
    post(
        &f.client,
        &f.base,
        "/chat",
        &json!({
            "session_id":id,"message":"Return one plain acknowledgement without tools.",
            "provider":"openai","model":"ticket-model","root_orchestration_only":false
        }),
    )
    .await;
    // This reader loads the Host-created Root's current derived index. A
    // reader initialized before /chat would retain an empty cached index.
    let storage = SessionStoreV2::new(f.data.clone()).await.unwrap();
    let execution = post(
        &f.client,
        &f.base,
        &format!("/execute/{id}"),
        &json!({"max_rounds":2}),
    )
    .await;
    let directory = f.data.join("sessions").join(id);
    let actor_path = directory.join("actor-authority.json");
    let (mut finished, actor) = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let session = storage.load_session(id).await.unwrap().unwrap();
            let actor = std::fs::read(&actor_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<ActorDirectoryEntry>(&bytes).ok());
            if let Some(actor) = actor {
                if actor
                    .activation
                    .as_ref()
                    .is_some_and(|a| a.status == ActorActivationStatus::Succeeded)
                    && session
                        .agent_runtime_state
                        .as_ref()
                        .is_some_and(|r| r.status == bamboo_domain::AgentStatusState::Completed)
                {
                    break (session, actor);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("actual historical Root completion");
    actor.validate().unwrap();
    assert_eq!(actor.actor.state, ActorLogicalState::Cold);
    let activation = actor.activation.as_ref().unwrap();
    let runtime = finished.agent_runtime_state.as_ref().unwrap();
    assert_eq!(activation.run_id, execution["run_id"].as_str().unwrap());
    assert_eq!(runtime.run_id, finished.id);
    assert_ne!(
        activation.run_id, runtime.run_id,
        "physical Run and legacy logical loop addresses remain separate"
    );
    assert_eq!(runtime.round.total_tool_calls, 0);
    assert!(finished.messages.iter().any(|m| m.role == Role::Assistant));
    // Add only old Task data through the existing exact-context metadata
    // writer after the real plain Root response. Seeding an all-completed
    // legacy list before execution would end that old loop before its model
    // turn. No Actor claim or Runtime completion is manufactured here.
    finished.task_list = Some(serde_json::from_value::<TaskList>(json!({
        "session_id":id,"title":"Legacy completed work","items":[{
            "id":"old-completed","description":"Legacy reviewed delivery","status":"completed","notes":"Old history, not new acceptance"
        }],"created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now()
    })).unwrap());
    storage.save_session(&finished).await.unwrap();
    finished = storage.load_session(id).await.unwrap().unwrap();
    let live_preview = ticket_cli::preview(&f.data, id).await.unwrap();
    assert_eq!(
        live_preview["source_snapshot_format"],
        "complete_root_files_v1"
    );
    assert_eq!(live_preview["read_only"], false);
    assert!(
        ticket_cli::legacy_commit(
            &f.data,
            id,
            live_preview["source_snapshot_hash"].as_str().unwrap(),
            "historical-import",
            &["old-completed".into()],
            false
        )
        .await
        .is_err(),
        "live scope writer must prevent offline import"
    );
    drop(f.host.take()); // Kill and wait the actual fixture Host, never infer stop from a lease.

    // Negative proof cases alter only stopped synthetic fixture files. They
    // do not manufacture the production activation used by the positive case.
    let main_path = directory.join("session.json");
    let original_main = std::fs::read(&main_path).unwrap();
    let runtime_path = directory.join("runtime.json");
    let original_runtime = std::fs::read(&runtime_path).unwrap();
    for field in ["role", "compressed", "tool_call_id"] {
        let mut unsupported = finished.clone();
        match field {
            "role" => unsupported.messages[1].role = Role::Tool,
            "compressed" => unsupported.messages[1].compressed = true,
            _ => unsupported.messages[1].tool_call_id = Some("historical-tool-result".into()),
        }
        // Alter only this stopped synthetic fixture's transcript suffix.
        // Keep the real compact authority prefix byte-identical: production
        // correctly refuses context mutation after an Actor activation.
        let frame_end = original_main
            .iter()
            .position(|byte| *byte == b'\n')
            .unwrap();
        let mut negative_main = original_main[..frame_end].to_vec();
        negative_main.extend_from_slice(&serde_json::to_vec(&unsupported).unwrap()[1..]);
        std::fs::write(&main_path, negative_main).unwrap();
        assert_eq!(
            ticket_cli::preview(&f.data, id).await.unwrap()["read_only"],
            true,
            "full Main {field} history must not pass through an empty authority projection"
        );
        std::fs::write(&main_path, &original_main).unwrap();
        std::fs::write(&runtime_path, &original_runtime).unwrap();
    }
    let original_actor = std::fs::read(&actor_path).unwrap();
    let marker = directory.join("actor-authority.initialized.json");
    let original_marker = std::fs::read(&marker).unwrap();
    std::fs::remove_file(&marker).unwrap();
    assert!(ticket_cli::preview(&f.data, id).await.is_err());
    std::fs::write(&marker, &original_marker).unwrap();
    let mut mismatched = actor.clone();
    mismatched.actor.session_created_at += chrono::Duration::seconds(1);
    std::fs::write(&actor_path, serde_json::to_vec(&mismatched).unwrap()).unwrap();
    assert!(ticket_cli::preview(&f.data, id).await.is_err());
    std::fs::write(&actor_path, &original_actor).unwrap();
    let mut wrong_loop: Value = serde_json::from_slice(&original_runtime).unwrap();
    wrong_loop["agent_runtime_state"]["run_id"] = json!("different-logical-session");
    std::fs::write(&runtime_path, serde_json::to_vec(&wrong_loop).unwrap()).unwrap();
    assert_eq!(
        ticket_cli::preview(&f.data, id).await.unwrap()["read_only"],
        true
    );
    std::fs::write(&runtime_path, &original_runtime).unwrap();
    let mut expired = actor.clone();
    expired.actor.state = ActorLogicalState::Active;
    let live = expired.activation.as_mut().unwrap();
    live.status = ActorActivationStatus::Running;
    live.finished_at = None;
    live.lease_expires_at = chrono::Utc::now() - chrono::Duration::seconds(1);
    std::fs::write(&actor_path, serde_json::to_vec(&expired).unwrap()).unwrap();
    assert_eq!(
        ticket_cli::preview(&f.data, id).await.unwrap()["read_only"],
        true
    );
    std::fs::write(&actor_path, &original_actor).unwrap();

    let preview = ticket_cli::preview(&f.data, id).await.unwrap();
    let hash = preview["source_snapshot_hash"].as_str().unwrap();
    let only_session_hash = content_hash(&canonical_bytes(&finished).unwrap());
    assert_ne!(
        hash, only_session_hash,
        "historical import binds the complete canonical tree"
    );
    assert!(ticket_cli::legacy_commit(
        &f.data,
        id,
        &only_session_hash,
        "wrong-historical-snapshot",
        &["old-completed".into()],
        false
    )
    .await
    .is_err());
    let cli = || {
        let output = Command::new(env!("CARGO_BIN_EXE_bamboo"))
            .args(["tickets", "import", "--data-dir"])
            .arg(&f.data)
            .args([
                "--source-session",
                id,
                "--expected-snapshot",
                hash,
                "--operation-id",
                "historical-import",
                "--task-id",
                "old-completed",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "CLI import: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let first = cli();
    let replay = cli();
    assert_eq!(first["receipt"], replay["receipt"]);
    assert_eq!(replay["replayed"], true);
    let work = first["receipt"]["ids"]["legacy-old-completed"]
        .as_str()
        .unwrap()
        .to_owned();
    let (binding, supervisor) = ticket_runtime::verified_scope_binding(
        &storage,
        bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID,
    )
    .await
    .unwrap();
    let bamboo_domain::SessionAuthorityIdentity::Supervisor { incarnation_id } =
        supervisor.authority_identity
    else {
        panic!("canonical Supervisor")
    };
    let service = TicketService::open_offline(
        f.data.join("tickets").join(incarnation_id.to_string()),
        binding.clone(),
    )
    .unwrap();
    let snapshot = service.published().unwrap().1;
    let imported = &snapshot.tickets[&work];
    assert_eq!(imported.state, WorkState::Blocked);
    assert!(imported.current_submission.is_none() && imported.accepted_submission.is_none());
    let source = imported.import_source.as_ref().unwrap();
    assert_eq!(source.original_state, "completed");
    let authority = Authority::from_verified_host(
        binding,
        Principal::User {
            user_id: "offline-host-owner".into(),
        },
    );
    let bytes = service
        .read_artifact(&authority, source.artifact.as_ref().unwrap(), 1024 * 1024)
        .unwrap();
    assert_eq!(content_hash(&bytes), hash);
    let archive: Value = serde_json::from_slice(&bytes).unwrap();
    for required in [
        "session.json",
        "runtime.json",
        "root-tool-authority.json",
        "actor-authority.json",
        "actor-authority.initialized.json",
    ] {
        assert!(
            archive[required]["bytes"].is_array(),
            "missing full snapshot member: {required}"
        );
    }
    let preserved: Vec<u8> =
        serde_json::from_value(archive["actor-authority.json"]["bytes"].clone()).unwrap();
    assert_eq!(preserved, original_actor);
    drop(service);
    assert_eq!(
        canonical_bytes(&storage.load_session(id).await.unwrap().unwrap()).unwrap(),
        canonical_bytes(&finished).unwrap()
    );
    assert_eq!(std::fs::read(&actor_path).unwrap(), original_actor);

    let mut host = fixture::start(&f.data, f.port);
    fixture::ready(&f.client, &f.base, &mut host, &f.data).await;
    f.host = Some(host);
    let view = post(
        &f.client,
        &f.base,
        "/tickets/inspect",
        &json!({"ids":[work],"depth":0,"budget_bytes":65536,"fixed_commit":null}),
    )
    .await;
    assert_eq!(view["data"][0]["ticket"]["state"], "blocked");
    assert!(view["data"][0]["ticket"]["accepted_submission"].is_null());
    assert!(view["data"][0]["assignments"]
        .as_array()
        .unwrap()
        .is_empty());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        std::fs::read(&actor_path).unwrap(),
        original_actor,
        "restart/import must not activate the old Root"
    );
    eprintln!(
        "A12 historical Root: {}",
        json!({"source_snapshot_hash":hash,"format":"complete_root_files_v1","run_id":activation.run_id,"work_id":work,"receipt_replayed":true,"old_completed":"blocked_needs_review","restart_preserved":true})
    );
    f.finish().await;
}

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
