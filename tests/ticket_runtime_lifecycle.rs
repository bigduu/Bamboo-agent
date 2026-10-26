//! Actual isolated Host/native processes; controlled HTTP provider only.
#![cfg(unix)]
#[path = "support/ticket_runtime.rs"]
mod fixture;
use bamboo_domain::{Session, Storage};
use bamboo_storage::SessionStoreV2;
use fixture::{command, get, post, Fixture};
use serde_json::{json, Value};
use std::time::Duration;
use std::{os::unix::process::ExitStatusExt, path::Path, sync::atomic::Ordering};

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
    let (project, workspace, fresh_workspace) = f.project_workspaces().await;
    let work = f.ready_project_work("crashed", &project, true).await;
    let start_request = f.start_request("start-held", &work, &workspace).await;
    let dispatched = post(&f.client, &f.base, "/tickets/dispatch", &start_request).await;
    assert_eq!(dispatched["errors"], json!([]), "{dispatched}");
    let before = f
        .until(&work, |_| f.probe.held.load(Ordering::SeqCst) == 1)
        .await;
    let old = &before["data"][0]["assignments"][0];
    assert_eq!(before["data"][0]["ticket"]["state"], "active");
    assert_eq!(old["workspace"], workspace);
    assert_eq!(old["plan"]["plan_revision"], 1);
    assert_eq!(old["plan"]["steps"][0]["id"], "own-step");
    let original_plan = old["plan"].clone();
    let assignment = old["id"].as_str().unwrap().to_owned();
    let key = dispatched["runtime"][0]["dispatch_key"].as_str().unwrap();
    let running = get(&f.client, &f.base, &format!("/tickets/dispatch/{key}")).await;
    assert_eq!(running["status"], "admitted");
    let child_id = running["receipt"]["session_id"].as_str().unwrap();
    let child = f.canonical_child(child_id).await;
    assert_eq!(child.metadata["runtime.local_task.state"], "running");
    assert!(!child.metadata.contains_key("ticket.runtime.owned_stop.v1"));
    let original_context: Value =
        serde_json::from_str(&child.metadata["ticket.work_contract_ref.v1"]).unwrap();
    assert_eq!(original_context["contract_ref"], work);
    assert_eq!(
        original_context["project_context"]["project_id"],
        project["id"]
    );
    assert_eq!(
        original_context["project_context"]["workspace_binding"],
        project["workspace_bindings"][0]
    );
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);
    // Reuse the existing owned Host fixture, but record the actual abrupt exit
    // before restart. No Cancel, graceful shutdown or stop receipt comes first.
    let mut previous = f.host.take().unwrap();
    let previous_pid = previous.0.id();
    assert!(previous.0.try_wait().unwrap().is_none());
    previous.0.kill().unwrap();
    let exited = previous.0.wait().unwrap();
    assert_eq!(
        exited.signal(),
        Some(9),
        "owned Host must really lose its process"
    );
    drop(previous);
    f.restart().await;
    let recovered_pid = f.host.as_ref().unwrap().0.id();
    assert_ne!(previous_pid, recovered_pid);
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
    assert_eq!(view["data"][0]["ticket"]["active_assignment"], assignment);
    assert_eq!(view["data"][0]["assignments"][0]["plan"], original_plan);
    assert_eq!(view["data"][0]["assignments"][0]["workspace"], workspace);
    assert!(view["data"][0]["submissions"]
        .as_array()
        .unwrap()
        .is_empty());
    let recovered_child = f.canonical_child(child_id).await;
    assert_eq!(recovered_child.created_at, child.created_at);
    assert_eq!(
        recovered_child.metadata["ticket.work_contract_ref.v1"],
        child.metadata["ticket.work_contract_ref.v1"]
    );
    assert!(!recovered_child
        .metadata
        .contains_key("ticket.runtime.owned_stop.v1"));
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

    // An explicit owner revocation is still not a Runtime stop proof.
    let cancel = command(
        &f.client,
        &f.base,
        "cancel-after-loss",
        json!([
            {"op":"cancel","work_id":work}
        ]),
    )
    .await;
    post(&f.client, &f.base, "/tickets/update", &cancel).await;
    let revoked = f
        .until(&work, |v| v["data"][0]["ticket"]["state"] == "cancelled")
        .await;
    assert_eq!(
        revoked["data"][0]["assignments"][0]["state"],
        "outcome_unknown"
    );
    assert_ne!(
        revoked["data"][0]["assignments"][0]["process_stopped"],
        true
    );

    let fresh = f.ready_project_work("fresh", &project, false).await;
    let blocked = f
        .start_request("same-claim-after-loss", &fresh, &workspace)
        .await;
    let prior = f.view(&fresh).await;
    let response = f
        .client
        .post(format!("{}/tickets/dispatch", f.base))
        .json(&blocked)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let denied: Value = response.json().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::LOCKED, "{denied}");
    assert!(denied["error"]["message"]
        .as_str()
        .unwrap()
        .starts_with("resource_blocked:"));
    let unchanged = f.view(&fresh).await;
    assert_eq!(unchanged["snapshot"], prior["snapshot"]);
    assert_eq!(unchanged["data"], prior["data"]);
    assert!(unchanged["data"][0]["assignments"]
        .as_array()
        .unwrap()
        .is_empty());
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 2);

    // Positive control: only the unconfirmed old claim is fenced. Explicitly
    // authorized work in the same Project on another worktree can still run.
    let fresh_start = f
        .start_request("disjoint-claim-after-loss", &fresh, &fresh_workspace)
        .await;
    let fresh_dispatch = post(&f.client, &f.base, "/tickets/dispatch", &fresh_start).await;
    assert_eq!(fresh_dispatch["errors"], json!([]), "{fresh_dispatch}");
    let finished = f
        .until(&fresh, |v| {
            v["data"][0]["ticket"]["state"] == "submitted"
                && v["data"][0]["assignments"][0]["process_stopped"] == true
        })
        .await;
    assert!(finished["data"][0]["ticket"]["accepted_submission"].is_null());
    assert_eq!(
        finished["data"][0]["assignments"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        finished["data"][0]["submissions"].as_array().unwrap().len(),
        1
    );
    assert_eq!(f.probe.calls.load(Ordering::SeqCst), 4);
    let final_old = f.view(&work).await;
    let old = &final_old["data"][0];
    assert_eq!(old["ticket"]["state"], "cancelled");
    assert!(old["ticket"]["current_submission"].is_null());
    assert!(old["ticket"]["accepted_submission"].is_null());
    assert_eq!(old["assignments"][0]["state"], "outcome_unknown");
    assert_ne!(old["assignments"][0]["process_stopped"], true);
    assert_eq!(old["assignments"][0]["runtime"], running["receipt"]);
    assert_eq!(old["assignments"][0]["workspace"], workspace);
    assert_eq!(old["assignments"][0]["plan"], original_plan);
    assert!(old["submissions"].as_array().unwrap().is_empty());
    assert!(!f
        .canonical_child(child_id)
        .await
        .metadata
        .contains_key("ticket.runtime.owned_stop.v1"));
    assert_eq!(
        get(
            &f.client,
            &f.base,
            &format!("/projects/{}", project["id"].as_str().unwrap())
        )
        .await,
        project
    );
    f.finish().await;
    eprintln!("native Project crash: Host {previous_pid} -> {recovered_pid}; real Task then SIGKILL, unknown stop/claims retained through owner Cancel, same claim denied, disjoint Work submitted; Work provider calls=4 (existing probe), external=0");
}

impl Fixture {
    async fn canonical_child(&self, id: &str) -> Session {
        // Read-only canonical observation; never opens another Ticket writer or
        // supplies synthetic authority, result, process-stop or Actor metadata.
        SessionStoreV2::new(self.data.clone())
            .await
            .unwrap()
            .load_session(id)
            .await
            .unwrap()
            .unwrap()
    }

    async fn view(&self, work: &str) -> Value {
        post(
            &self.client,
            &self.base,
            "/tickets/inspect",
            &json!({
                "ids":[work],"depth":0,"budget_bytes":65536,"fixed_commit":null
            }),
        )
        .await
    }

    async fn ready_project_work(&self, label: &str, project: &Value, held: bool) -> String {
        let objective = if held {
            "WAIT_FOR_CANCEL TICKET_E2E_1481"
        } else {
            "Return TICKET_E2E_1481_DONE using only the private Task plan"
        };
        let contract = json!({"title":"TICKET_E2E_1481","objective":objective,
            "constraints":["Own Project and private plan only"],"acceptance":["Exact output"],
            "user_acceptance_required":true,"allowed_tools":["Task"]});
        let create = command(&self.client, &self.base, &format!("create-{label}"), json!([
            {"op":"create","temp_id":"work","kind":"work","parent":null,"depends_on":[],"contract":contract},
            {"op":"associate_project","ticket_id":"work","project_id":project["id"]}
        ])).await;
        let created = post(&self.client, &self.base, "/tickets/update", &create).await;
        let work = created["ids"]["work"].as_str().unwrap().to_owned();
        let current = self.view(&work).await;
        let ready = command(&self.client, &self.base, &format!("ready-{label}"), json!([
            {"op":"set_project_context","work_id":work,"context":{
                "project_id":project["id"],"project_revision":project["revision"],
                "resource_revision":project["resource_revision"],"workspace_binding":project["workspace_bindings"][0],
                "contract_revision":current["data"][0]["ticket"]["contract_revision"],"contract":contract,"sources":{}
            }},
            {"op":"ready","work_id":work}
        ])).await;
        post(&self.client, &self.base, "/tickets/update", &ready).await;
        work
    }

    async fn start_request(&self, id: &str, work: &str, workspace: &Value) -> Value {
        command(
            &self.client,
            &self.base,
            id,
            json!([
                {"op":"start","work_id":work,"temp_id":"assignment","workspace":workspace}
            ]),
        )
        .await
    }

    async fn project_workspaces(&self) -> (Value, Value, Value) {
        // Project's primary directory and additional repository binding must
        // be distinct roots. The real registry rejects overlapping roots.
        let project_home = self.temp.join("project-home");
        std::fs::create_dir(&project_home).unwrap();
        let repo = self.temp.join("project-repo");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-b", "fixture"]);
        std::fs::write(repo.join("fixture.txt"), "disposable crash acceptance\n").unwrap();
        git(&repo, &["add", "fixture.txt"]);
        git(
            &repo,
            &[
                "-c",
                "user.name=Native fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-m",
                "fixture",
            ],
        );
        let base = git(&repo, &["rev-parse", "HEAD"]);
        let repo = repo.canonicalize().unwrap();
        let workspace = |branch: &str| {
            let path = self.temp.join(branch);
            git(
                &repo,
                &[
                    "worktree",
                    "add",
                    "-b",
                    branch,
                    path.to_str().unwrap(),
                    &base,
                ],
            );
            let path = path.canonicalize().unwrap();
            json!({"repo":repo,"base_commit":base,"branch":branch,"worktree":path,
                "write_roots":[path],"claims":[format!("worktree:{}", path.display())]})
        };
        let old = workspace("old-claim");
        let fresh = workspace("new-claim");
        let project = post(&self.client, &self.base, "/projects", &json!({
            "name":"Native crash Project","description":"Disposable process-loss fixture",
            "project_path":project_home,"workspace_bindings":[{"path":repo,"label":"fixture","git_common_dir":null}]
        })).await;
        assert_eq!(project["workspace_bindings"].as_array().unwrap().len(), 1);
        let primary = Path::new(project["project_path"].as_str().unwrap());
        let binding = Path::new(project["workspace_bindings"][0]["path"].as_str().unwrap());
        assert!(!primary.starts_with(binding) && !binding.starts_with(primary));
        (project, old, fresh)
    }

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

fn git(repo: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
            "-C",
        ])
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
