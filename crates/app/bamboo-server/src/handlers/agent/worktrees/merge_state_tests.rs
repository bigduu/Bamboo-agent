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
