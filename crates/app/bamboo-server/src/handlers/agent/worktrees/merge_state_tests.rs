//! Route-level contract for `GET /projects/{id}/worktrees/merge-state`.
//!
//! These use real temporary repositories: the endpoint's whole value is Git's
//! actual answer, which a canned fixture would not exercise.
use super::*;
use actix_web::{test, App};
use std::fs;
use std::process::Command;

fn git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args([
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
        ])
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn head(repo: &Path) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn commit(repo: &Path, file: &str, body: &str) {
    fs::write(repo.join(file), body).unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", file]);
}

fn init_repository_with_base(repo: &Path) {
    fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "-b", "main"]);
    commit(repo, "tracked.txt", "one\n");
    git(repo, &["update-ref", "refs/remotes/origin/main", "HEAD"]);
}

async fn register_container(base: &Path, container: &Path) -> (web::Data<AppState>, String) {
    let state = web::Data::new(AppState::new(base.join("data")).await.unwrap());
    let project = state
        .project_store
        .create_with_project_path("Container", None, container.to_str().unwrap(), vec![])
        .unwrap();
    let uri = format!("/api/v1/projects/{}/worktrees", project.id);
    (state, uri)
}

#[actix_web::test]
async fn merge_state_discovers_the_same_deep_repository_as_inventory() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let container = base.join("project");
    let repo = container.join("a/b/c/d/repo");
    init_repository_with_base(&repo);
    let (state, uri) = register_container(&base, &container).await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;

    let inventory: Value =
        test::call_and_read_body_json(&app, test::TestRequest::get().uri(&uri).to_request()).await;
    let merge_state: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri(&format!("{uri}/merge-state?forge=off"))
            .to_request(),
    )
    .await;
    state.shutdown().await;

    let path = repo.to_str().unwrap();
    let inventory_repositories = inventory["repositories"].as_array().unwrap();
    assert_eq!(inventory_repositories.len(), 1, "{inventory}");
    assert_eq!(inventory_repositories[0]["path"], path);
    assert!(inventory_repositories[0]["error"].is_null());

    let repositories = merge_state["repositories"].as_array().unwrap();
    assert_eq!(repositories.len(), 1, "{merge_state}");
    let repository = &repositories[0];
    assert_eq!(repository["path"], path);
    assert_eq!(repository["base_ref"], "origin/main");
    let commit = head(&repo);
    assert_eq!(repository["base_commit"], commit.as_str());
    assert!(repository["error"].is_null());
    assert_eq!(repository["worktrees"].as_array().unwrap().len(), 1);
    let checkout = &repository["worktrees"][0];
    assert_eq!(
        Path::new(checkout["path"].as_str().unwrap())
            .canonicalize()
            .unwrap(),
        repo
    );
    assert_eq!(checkout["branch"], "main");
    assert_eq!(checkout["head"], commit.as_str());
    assert_eq!(checkout["state"], "primary");
    assert_eq!(checkout["reason"], "main_checkout");
}

#[actix_web::test]
async fn merge_state_reports_depth_truncation_without_losing_known_repositories() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let container = base.join("project");
    let known = container.join("known");
    init_repository_with_base(&known);
    let mut boundary = container.clone();
    for _ in 0..12 {
        boundary.push("nested");
    }
    init_repository_with_base(&boundary.join("omitted"));
    let (state, uri) = register_container(&base, &container).await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;

    let response: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri(&format!("{uri}/merge-state?forge=off"))
            .to_request(),
    )
    .await;
    state.shutdown().await;

    let repositories = response["repositories"].as_array().unwrap();
    assert_eq!(repositories.len(), 1, "{response}");
    assert_eq!(repositories[0]["path"], known.to_str().unwrap());
    assert_eq!(repositories[0]["worktrees"][0]["state"], "primary");
    let diagnostics = response["discovery_diagnostics"].as_array().unwrap();
    assert_eq!(diagnostics.len(), 1, "{response}");
    assert_eq!(diagnostics[0].as_object().unwrap().len(), 2);
    assert_eq!(diagnostics[0]["path"], boundary.to_str().unwrap());
    assert_eq!(
        diagnostics[0]["error"],
        "Repository discovery truncated at depth 12"
    );
}

#[actix_web::test]
async fn merge_state_uses_physical_primary_when_linked_source_is_discovered_first() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let container = base.join("project");
    let primary = container.join("Z");
    let linked = container.join("A");
    init_repository_with_base(&primary);
    git(
        &primary,
        &[
            "worktree",
            "add",
            "-b",
            "linked",
            linked.to_str().unwrap(),
            "HEAD",
        ],
    );
    commit(&linked, "linked.txt", "unique work\n");
    let (state, uri) = register_container(&base, &container).await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(crate::routes::configure_routes),
    )
    .await;

    let response: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri(&format!("{uri}/merge-state?forge=off"))
            .to_request(),
    )
    .await;
    state.shutdown().await;

    let repositories = response["repositories"].as_array().unwrap();
    assert_eq!(repositories.len(), 1, "{response}");
    let repository = &repositories[0];
    assert_eq!(repository["path"], linked.to_str().unwrap());
    let checkouts = repository["worktrees"].as_array().unwrap();
    assert_eq!(checkouts.len(), 2, "{response}");
    for (path, branch, status, reason) in [
        (&primary, "main", "primary", "main_checkout"),
        (&linked, "linked", "in_progress", "unique_commits"),
    ] {
        let checkout = checkouts
            .iter()
            .find(|entry| {
                entry["path"]
                    .as_str()
                    .and_then(|path| Path::new(path).canonicalize().ok())
                    .as_ref()
                    == Some(path)
            })
            .unwrap();
        assert_eq!(checkout["branch"], branch);
        assert_eq!(checkout["head"], head(path).as_str());
        assert_eq!(checkout["state"], status, "{checkout}");
        assert_eq!(checkout["reason"], reason, "{checkout}");
    }
}

#[actix_web::test]
async fn merge_state_reports_landed_and_outstanding_checkouts_then_degrades_without_a_base() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let repo = base.join("repo");
    fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-b", "main"]);
    commit(&repo, "tracked.txt", "one\n");

    let roots = base.join("roots");
    fs::create_dir(&roots).unwrap();
    let merged = roots.join("merged");
    let pending = roots.join("pending");
    for (name, path) in [("merged", &merged), ("pending", &pending)] {
        git(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                name,
                path.to_str().unwrap(),
                "HEAD",
            ],
        );
    }
    commit(&merged, "landed.txt", "landed\n");
    commit(&pending, "wip.txt", "wip\n");

    // Land only the `merged` checkout, then advance the integration branch so its
    // head is a strict ancestor of the base rather than the base tip itself.
    let landed = head(&merged);
    git(&repo, &["merge", "--ff-only", &landed]);
    git(&repo, &["commit", "--allow-empty", "-m", "follow-up"]);
    git(&repo, &["update-ref", "refs/remotes/origin/main", "HEAD"]);

    let state = web::Data::new(AppState::new(base.join("data")).await.unwrap());
    let project = state
        .project_store
        .create_with_project_path("Merge", None, repo.to_str().unwrap(), vec![])
        .unwrap();
    let uri = format!("/projects/{}/worktrees/merge-state", project.id);
    let app = test::init_service(App::new().app_data(state.clone()).route(
        "/projects/{project_id}/worktrees/merge-state",
        web::get().to(merge_state),
    ))
    .await;

    let response = test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;
    assert_eq!(response.status(), actix_web::http::StatusCode::OK);
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["project_id"], project.id.to_string());
    let repository = &body["repositories"][0];
    assert_eq!(repository["base_ref"], "origin/main");
    assert_eq!(repository["base_commit"], head(&repo).as_str());
    let find = |path: &Path| {
        repository["worktrees"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["path"] == path.to_str().unwrap())
            .unwrap()
            .clone()
    };
    assert_eq!(find(&repo)["state"], "primary");
    assert_eq!(find(&repo)["reason"], "main_checkout");
    assert_eq!(find(&merged)["state"], "merged");
    assert_eq!(find(&merged)["reason"], "is_ancestor");
    assert_eq!(find(&pending)["state"], "in_progress");
    assert_eq!(find(&pending)["reason"], "unique_commits");
    assert_eq!(find(&pending)["branch"], "pending");

    // With the upstream ref gone every checkout degrades to one explicit
    // repository-level reason instead of guessing, and the request still
    // succeeds: an unavailable answer must never become an HTTP failure.
    git(&repo, &["update-ref", "-d", "refs/remotes/origin/main"]);
    let response = test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;
    assert_eq!(response.status(), actix_web::http::StatusCode::OK);
    let body: Value = test::read_body_json(response).await;
    let repository = &body["repositories"][0];
    assert_eq!(repository["error"], "no_base_ref");
    assert!(repository["base_ref"].is_null());
    assert!(repository["worktrees"].as_array().unwrap().is_empty());
}
