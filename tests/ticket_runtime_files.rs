//! Actual Host, native Worker and Git worktree; controlled provider, no live
//! model quality or production #1488 writer-fence claim.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use fixture::{command, post, Fixture};
use serde_json::json;
use std::{path::Path, process::Command, sync::atomic::Ordering, time::Duration};

#[cfg(feature = "ticket-runtime-fixtures")]
#[actix_web::test]
async fn actual_stopped_native_unknown_file_is_reconciled_offline_without_rewriting_or_acceptance()
{
    use std::os::unix::fs::MetadataExt;
    let mut f = Fixture::with_fault(Some(("worker-file/", "before_head_enospc"))).await;
    let repo = f.temp.join("repo");
    let tree = f.temp.join("assignment");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-b", "main"]);
    std::fs::write(repo.join("answer.rs"), "pub fn answer() -> u8 { 0 }\n").unwrap();
    git(&repo, &["add", "answer.rs"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Ticket fixture",
            "-c",
            "user.email=ticket@fixture.invalid",
            "commit",
            "-m",
            "fixture baseline",
        ],
    );
    let base = git(&repo, &["rev-parse", "HEAD"]);
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "ticket-code",
            tree.to_str().unwrap(),
        ],
    );
    let tree = tree.canonicalize().unwrap();
    let file = tree.join("answer.rs");
    *f.probe.coding_path.lock().unwrap() = Some((
        file.to_string_lossy().into_owned(),
        repo.join("answer.rs").to_string_lossy().into_owned(),
    ));
    f.probe.hold_code.store(true, Ordering::SeqCst);
    let create = command(&f.client, &f.base, "unknown-create", json!([
        {"op":"create","temp_id":"w","kind":"work","parent":null,"depends_on":[],"contract":{"title":"Unknown file fixture","objective":"Patch own answer.rs","constraints":["Own worktree only"],"acceptance":["answer returns 42"],"user_acceptance_required":true,"allowed_tools":["Task","Read","Write"]}},
        {"op":"ready","work_id":"w"}
    ])).await;
    let created = post(&f.client, &f.base, "/tickets/update", &create).await;
    let work = created["ids"]["w"].as_str().unwrap().to_owned();
    let workspace = json!({"repo":repo.canonicalize().unwrap(),"base_commit":base,"branch":"ticket-code","worktree":tree,"write_roots":[tree],"claims":[format!("worktree:{}",tree.display())]});
    let start = command(
        &f.client,
        &f.base,
        "unknown-start",
        json!([
            {"op":"start","work_id":work,"temp_id":"a","workspace":workspace}
        ]),
    )
    .await;
    let started = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
    assert_eq!(started["errors"], json!([]), "{started}");
    let assignment = started["receipt"]["ids"]["a"]
        .as_str()
        .or_else(|| started["ids"]["a"].as_str())
        .unwrap()
        .to_owned();
    tokio::time::timeout(Duration::from_secs(60), async {
        while f.probe.held.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let system = sysinfo::System::new_all();
    let host_pid = f.host.as_ref().unwrap().0.id();
    let pids: Vec<_> = system
        .processes()
        .iter()
        .filter(|(_, p)| {
            p.parent().is_some_and(|id| id.as_u32() == host_pid)
                && p.cmd().iter().any(|arg| arg == "subagent-worker")
        })
        .map(|(id, _)| id.as_u32())
        .collect();
    assert_eq!(pids.len(), 1, "actual owned native PID");
    let pid = pids[0];
    assert_eq!(unsafe { libc::kill(pid as i32, 0) }, 0);
    f.probe.release.notify_waiters();
    let inspect = json!({"ids":[work],"depth":0,"sections":["assignments","submissions"],"budget_bytes":65536,"fixed_commit":null});
    let view = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let view = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
            let a = &view["data"][0]["assignments"][0];
            if a["state"] == "outcome_unknown" && a["process_stopped"] == true {
                break view;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_ne!(
        unsafe { libc::kill(pid as i32, 0) },
        0,
        "native PID actually reaped"
    );
    assert_eq!(view["data"][0]["ticket"]["state"], "blocked");
    assert!(view["data"][0]["submissions"]
        .as_array()
        .unwrap()
        .is_empty());
    let effects = view["data"][0]["assignments"][0]["effects"]
        .as_object()
        .unwrap();
    assert_eq!(effects.len(), 1);
    let (effect, detail) = effects.iter().next().unwrap();
    assert_eq!(detail["state"], "started");
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "pub fn answer() -> u8 { 42 }\n"
    );
    let inode = std::fs::metadata(&file).unwrap().ino();
    let cli = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_bamboo"))
            .arg("tickets")
            .args(args)
            .arg("--data-dir")
            .arg(&f.data)
            .current_dir(&f.temp)
            .env("BAMBOO_JIANDU_DATA_DIR", f.data.join("jiandu"))
            .output()
            .unwrap()
    };
    let plan_args = [
        "file-reconcile-plan",
        "--assignment-id",
        &assignment,
        "--effect-id",
        effect,
        "--operation-id",
        "file-reconcile/native",
        "--evidence",
        "Verified exact intended code after owned native PID reap",
    ];
    let locked = cli(&plan_args);
    assert!(
        !locked.status.success(),
        "live Host writer lock denies offline operation"
    );
    drop(f.host.take());
    let planned = cli(&plan_args);
    assert!(
        planned.status.success(),
        "{}",
        String::from_utf8_lossy(&planned.stderr)
    );
    let plan: serde_json::Value = serde_json::from_slice(&planned.stdout).unwrap();
    assert_eq!(plan["process_stopped"], true);
    assert_eq!(plan["observed_sha256"], plan["intended_sha256"]);
    assert!(plan["request"].is_object(), "{plan}");
    let request = f.temp.join("file-reconciliation-request.json");
    std::fs::write(
        &request,
        serde_json::to_vec_pretty(&plan["request"]).unwrap(),
    )
    .unwrap();
    let args = ["file-reconcile", "--request", request.to_str().unwrap()];
    let acknowledged = cli(&args);
    assert!(
        acknowledged.status.success(),
        "{}",
        String::from_utf8_lossy(&acknowledged.stderr)
    );
    let receipt: serde_json::Value = serde_json::from_slice(&acknowledged.stdout).unwrap();
    assert_eq!(receipt["ids"]["resource_released"], "true");
    let replay = cli(&args);
    assert!(
        replay.status.success(),
        "{}",
        String::from_utf8_lossy(&replay.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&replay.stdout).unwrap(),
        receipt
    );
    assert_eq!(
        std::fs::metadata(&file).unwrap().ino(),
        inode,
        "plan/ack/replay never rewrite"
    );
    let calls = f.probe.calls.load(Ordering::SeqCst);
    f.restart().await;
    let reconciled = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
    assert_eq!(reconciled["data"][0]["ticket"]["state"], "blocked");
    assert_eq!(reconciled["data"][0]["ticket"]["paused"], true);
    assert!(reconciled["data"][0]["submissions"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(reconciled["data"][0]["assignments"][0]["state"], "failed");
    assert_eq!(
        f.probe.calls.load(Ordering::SeqCst),
        calls,
        "no automatic retry"
    );
    let next = command(
        &f.client,
        &f.base,
        "explicit-file-retry",
        json!([
            {"op":"ready","work_id":work},
            {"op":"start","work_id":work,"temp_id":"next","workspace":workspace}
        ]),
    )
    .await;
    let admitted = post(&f.client, &f.base, "/tickets/dispatch", &next).await;
    assert_eq!(admitted["errors"], json!([]), "{admitted}");
    let completed = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let view = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
            if view["data"][0]["ticket"]["state"] == "submitted" {
                break view;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(completed["data"][0]["ticket"]["generation"], 2);
    let submission = &completed["data"][0]["submissions"][0];
    assert_eq!(submission["generation"], 2);
    let accept = command(&f.client, &f.base, "file-retry-accept", json!([
        {"op":"accept","work_id":work,"submission_id":submission["id"],"evidence":["Verified generation 2 exact managed code"]}
    ])).await;
    post(&f.client, &f.base, "/tickets/update", &accept).await;
    f.restart().await;
    let accepted = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
    assert_eq!(accepted["data"][0]["ticket"]["state"], "accepted");
    assert_eq!(accepted["data"][0]["ticket"]["generation"], 2);
    assert_eq!(
        accepted["data"][0]["submissions"].as_array().unwrap().len(),
        1
    );
    f.finish().await;
}

fn git(dir: &Path, args: &[&str]) -> String {
    let result = Command::new("git")
        .args(["-C"])
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    String::from_utf8(result.stdout).unwrap().trim().into()
}

#[actix_web::test]
async fn expired_capacity_lease_cannot_admit_same_worktree_while_native_pid_is_alive() {
    use bamboo_domain::{
        ActorPlacementClass, HostPlacementIntent, HostPlacementRequest, WorkerHostCapabilities,
        WorkerHostRegistration,
    };
    use bamboo_storage::v2::FileHostRegistry;
    use std::collections::BTreeSet;
    let f = Fixture::new().await;
    let repo = f.temp.join("repo");
    let tree = f.temp.join("assignment");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-b", "main"]);
    std::fs::write(repo.join("answer.rs"), "pub fn answer() -> u8 { 0 }\n").unwrap();
    git(&repo, &["add", "answer.rs"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Ticket fixture",
            "-c",
            "user.email=ticket@fixture.invalid",
            "commit",
            "-m",
            "fixture baseline",
        ],
    );
    let base = git(&repo, &["rev-parse", "HEAD"]);
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "ticket-code",
            tree.to_str().unwrap(),
        ],
    );
    let tree = tree.canonicalize().unwrap();
    *f.probe.coding_path.lock().unwrap() = Some((
        tree.join("answer.rs").to_string_lossy().into_owned(),
        repo.join("answer.rs").to_string_lossy().into_owned(),
    ));
    f.probe.hold_code.store(true, Ordering::SeqCst);
    let contract = json!({"title":"Lease alive fixture","objective":"Patch own answer.rs","constraints":["Own worktree only"],"acceptance":["exact code"],"user_acceptance_required":true,"allowed_tools":["Task","Read","Write"]});
    let create=command(&f.client,&f.base,"two-code",json!([
        {"op":"create","temp_id":"a","kind":"work","parent":null,"depends_on":[],"contract":contract},{"op":"ready","work_id":"a"},
        {"op":"create","temp_id":"b","kind":"work","parent":null,"depends_on":[],"contract":contract},{"op":"ready","work_id":"b"}])).await;
    let created = post(&f.client, &f.base, "/tickets/update", &create).await;
    let a = created["ids"]["a"].as_str().unwrap();
    let b = created["ids"]["b"].as_str().unwrap();
    let workspace = json!({"repo":repo,"base_commit":base,"branch":"ticket-code","worktree":tree,"write_roots":[tree],"claims":[format!("worktree:{}",tree.display())]});
    let start = command(
        &f.client,
        &f.base,
        "start-a",
        json!([{"op":"start","work_id":a,"temp_id":"aa","workspace":workspace}]),
    )
    .await;
    let dispatched = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
    assert_eq!(dispatched["errors"], json!([]), "{dispatched}");
    tokio::time::timeout(Duration::from_secs(30), async {
        while f.probe.held.load(Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let host_pid = f.host.as_ref().unwrap().0.id();
    let system = sysinfo::System::new_all();
    let pids: Vec<_> = system
        .processes()
        .iter()
        .filter(|(_, p)| {
            p.parent().is_some_and(|id| id.as_u32() == host_pid)
                && p.cmd().iter().any(|arg| arg == "subagent-worker")
        })
        .map(|(id, _)| id.as_u32())
        .collect();
    assert_eq!(
        pids.len(),
        1,
        "one actual native Worker owned by the isolated Host"
    );
    let pid = pids[0];
    assert_eq!(unsafe { libc::kill(pid as i32, 0) }, 0);
    // This is the existing capacity API with a controlled clock, bound to the
    // actual live Run. It is not the pending #1488 activation lease protocol.
    let registry = FileHostRegistry::new(f.temp.join("capacity"))
        .await
        .unwrap();
    let now = chrono::Utc::now();
    let key = dispatched["runtime"][0]["dispatch_key"].as_str().unwrap();
    let observed = fixture::get(&f.client, &f.base, &format!("/tickets/dispatch/{key}")).await;
    let run = observed["receipt"]["run_id"].as_str().unwrap();
    registry
        .observe_host(WorkerHostRegistration {
            host_ref: "fixture".into(),
            mailbox: "fixture".into(),
            role: Some("worker".into()),
            connection_generation: "one".into(),
            credential_expires_at: now + chrono::Duration::minutes(5),
            observed_at: now,
            lease_expires_at: now + chrono::Duration::minutes(2),
            expected_connection_generation: None,
            max_slots: 1,
            capabilities: WorkerHostCapabilities {
                placement_class: ActorPlacementClass::Local,
                project_ids: BTreeSet::new(),
                allow_unscoped_project: true,
                trust_zone: "fixture".into(),
                workspace_labels: BTreeSet::new(),
                executors: BTreeSet::from(["bamboo-runtime".into()]),
                tools: BTreeSet::from(["Write".into()]),
                network_zones: BTreeSet::new(),
                network_isolation: false,
            },
        })
        .await
        .unwrap();
    let request = HostPlacementRequest {
        intent: HostPlacementIntent::Auto,
        actor_id: "a".into(),
        run_id: run.into(),
        project_id: None,
        trust_zone: "fixture".into(),
        workspace_label: None,
        executor: "bamboo-runtime".into(),
        required_tools: BTreeSet::from(["Write".into()]),
        network_zone: None,
        require_network_isolation: false,
        preferred_host_ref: None,
        now,
        lease_expires_at: now + chrono::Duration::seconds(1),
    };
    let old = registry.reserve_slot(request.clone()).await.unwrap();
    let mut next = request;
    next.actor_id = "b".into();
    next.run_id = "candidate".into();
    next.now = now + chrono::Duration::seconds(2);
    next.lease_expires_at = now + chrono::Duration::seconds(30);
    let reused = registry.reserve_slot(next).await.unwrap();
    assert_eq!(reused.slot, old.slot);
    assert!(reused.epoch > old.epoch);
    assert_eq!(unsafe { libc::kill(pid as i32, 0) }, 0);
    let blocked = command(
        &f.client,
        &f.base,
        "start-b-blocked",
        json!([{"op":"start","work_id":b,"temp_id":"bb","workspace":workspace}]),
    )
    .await;
    let response = f
        .client
        .post(format!("{}/tickets/dispatch", f.base))
        .json(&blocked)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 423);
    assert!(response.text().await.unwrap().contains("resource_blocked"));
    assert_eq!(
        std::fs::read_to_string(tree.join("answer.rs")).unwrap(),
        "pub fn answer() -> u8 { 0 }\n"
    );
    let cancel = command(
        &f.client,
        &f.base,
        "cancel-a",
        json!([{"op":"cancel","work_id":a}]),
    )
    .await;
    post(&f.client, &f.base, "/tickets/update", &cancel).await;
    let inspect = json!({"ids":[a],"depth":0,"sections":["assignments"],"budget_bytes":65536,"fixed_commit":null});
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let view = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
            if view["data"][0]["assignments"][0]["process_stopped"] == true {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_ne!(
        unsafe { libc::kill(pid as i32, 0) },
        0,
        "owned native process reaped before same-worktree resource release"
    );
    f.probe.release.notify_waiters();
    let start_b = command(
        &f.client,
        &f.base,
        "start-b-after-reap",
        json!([{"op":"start","work_id":b,"temp_id":"bb","workspace":workspace}]),
    )
    .await;
    let admitted = post(&f.client, &f.base, "/tickets/dispatch", &start_b).await;
    assert_eq!(admitted["errors"], json!([]), "{admitted}");
    let inspect = json!({"ids":[b],"depth":0,"sections":["assignments","submissions"],"budget_bytes":65536,"fixed_commit":null});
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let view = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
            if view["data"][0]["ticket"]["state"] == "submitted" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(tree.join("answer.rs")).unwrap(),
        "pub fn answer() -> u8 { 42 }\n"
    );
    f.finish().await;
}

#[actix_web::test]
async fn actual_native_worker_writes_only_its_git_worktree_and_submits_managed_code() {
    let mut f = Fixture::new().await;
    let repo = f.temp.join("repo");
    let workspace = f.temp.join("assignment");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-b", "main"]);
    std::fs::write(repo.join("answer.rs"), "pub fn answer() -> u8 { 0 }\n").unwrap();
    git(&repo, &["add", "answer.rs"]);
    git(
        &repo,
        &[
            "-c",
            "user.name=Ticket fixture",
            "-c",
            "user.email=ticket@fixture.invalid",
            "commit",
            "-m",
            "fixture baseline",
        ],
    );
    let base = git(&repo, &["rev-parse", "HEAD"]);
    git(
        &repo,
        &[
            "worktree",
            "add",
            "-b",
            "ticket-code",
            workspace.to_str().unwrap(),
        ],
    );
    let workspace = workspace.canonicalize().unwrap();
    let file = workspace.join("answer.rs");
    let outside = repo.join("answer.rs");
    let cache = workspace.join(".bamboo/private-state.json");
    std::fs::create_dir(cache.parent().unwrap()).unwrap();
    std::fs::write(&cache, "private runtime state").unwrap();
    *f.probe.coding_path.lock().unwrap() = Some((
        file.to_string_lossy().into_owned(),
        outside.to_string_lossy().into_owned(),
    ));
    let create=command(&f.client,&f.base,"code-create",json!([
        {"op":"create","temp_id":"w","kind":"work","parent":null,"depends_on":[],"contract":{"title":"Code fixture","objective":"Patch and verify own answer.rs, then submit exact code evidence.","constraints":["Never write the sibling repo or TicketStore"],"acceptance":["answer returns 42"],"user_acceptance_required":true,"allowed_tools":["Task","Read","Write"]}},
        {"op":"ready","work_id":"w"}])).await;
    let created = post(&f.client, &f.base, "/tickets/update", &create).await;
    let work = created["ids"]["w"].as_str().unwrap();
    let start=command(&f.client,&f.base,"code-start",json!([{"op":"start","work_id":work,"temp_id":"a","workspace":{"repo":repo.canonicalize().unwrap(),"base_commit":base,"branch":"ticket-code","worktree":workspace,"write_roots":[workspace],"claims":[format!("worktree:{}",workspace.display())]}}])).await;
    let started = post(&f.client, &f.base, "/tickets/dispatch", &start).await;
    assert_eq!(started["errors"], json!([]), "{started}");
    let inspect = json!({"ids":[work],"depth":0,"sections":["assignments","submissions"],"budget_bytes":65536,"fixed_commit":null});
    let view = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let view = post(&f.client, &f.base, "/tickets/inspect", &inspect).await;
            if view["data"][0]["ticket"]["state"] == "submitted" {
                break view;
            }
            assert_ne!(
                view["data"][0]["assignments"][0]["state"],
                "failed",
                "{}",
                std::fs::read_to_string(f.data.join("host.log")).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{}",
            std::fs::read_to_string(f.data.join("host.log")).unwrap()
        )
    });
    assert_eq!(
        std::fs::read_to_string(file).unwrap(),
        "pub fn answer() -> u8 { 42 }\n"
    );
    assert_eq!(
        std::fs::read_to_string(outside).unwrap(),
        "pub fn answer() -> u8 { 0 }\n"
    );
    assert_eq!(
        std::fs::read_to_string(cache).unwrap(),
        "private runtime state"
    );
    assert_eq!(view["data"][0]["assignments"][0]["process_stopped"], true);
    assert_eq!(
        view["data"][0]["assignments"][0]["plan"]["steps"][0]["id"],
        "code-step"
    );
    let submission = &view["data"][0]["submissions"][0];
    assert_eq!(submission["artifacts"].as_array().unwrap().len(), 2);
    assert_eq!(submission["effects"].as_object().unwrap().len(), 1);
    let artifacts = submission["artifacts"].as_array().unwrap();
    let mut contents = Vec::new();
    for artifact in artifacts {
        let response = f
            .client
            .get(format!(
                "{}/tickets/artifacts/{}",
                f.base,
                artifact["sha256"].as_str().unwrap()
            ))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        contents.push(response.text().await.unwrap());
    }
    assert!(contents.contains(&"pub fn answer() -> u8 { 42 }\n".into()));
    assert!(contents.contains(&"TICKET_CODE_DONE".into()));
    let completed_calls = f.probe.calls.load(Ordering::SeqCst);
    let accept=command(&f.client,&f.base,"code-accept",json!([{"op":"accept","work_id":work,"submission_id":submission["id"],"evidence":["Read exact managed code; own worktree changed and sibling unchanged"]}])).await;
    post(&f.client, &f.base, "/tickets/update", &accept).await;
    f.restart().await;
    assert_eq!(
        post(&f.client, &f.base, "/tickets/inspect", &inspect).await["data"][0]["ticket"]["state"],
        "accepted"
    );
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), completed_calls);
    f.finish().await;
}
