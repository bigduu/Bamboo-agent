use super::*;
use actix_web::{test, App};
use std::fs;
use std::process::Command;

fn run_git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[actix_web::test]
async fn directory_preferences_native_create_discovery_and_session_association_round_trip() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let repo = base.join("repo");
    let global = base.join("global-root");
    let override_root = base.join("project-root");
    for path in [&repo, &global, &override_root] {
        fs::create_dir(path).unwrap();
    }
    run_git(&repo, &["init", "-b", "main"]);
    run_git(
        &repo,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "initial",
        ],
    );
    let state = web::Data::new(AppState::new(base.join("data")).await.unwrap());
    let project = state
        .project_store
        .create_with_project_path("Test", None, repo.to_str().unwrap(), vec![])
        .unwrap();
    let uri = format!("/projects/{}/worktrees", project.id);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/worktrees/preferences", web::get().to(get_preferences))
            .route("/worktrees/preferences", web::put().to(set_preferences))
            .route("/projects/{project_id}/worktrees", web::get().to(list))
            .route("/projects/{project_id}/worktrees", web::post().to(create)),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::put()
            .uri("/worktrees/preferences")
            .set_json(json!({"root": global}))
            .to_request(),
    )
    .await;
    assert!(response.status().is_success());
    let response = test::call_service(
        &app,
        test::TestRequest::put()
            .uri("/worktrees/preferences")
            .set_json(json!({"root": override_root, "project_id": project.id}))
            .to_request(),
    )
    .await;
    assert!(response.status().is_success());
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri(&uri)
            .set_json(json!({"repository": repo, "name": "task", "branch": "feature/task"}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
    let body: Value = test::read_body_json(response).await;
    let checkout = override_root.join("task");
    assert_eq!(body["path"], checkout.to_str().unwrap());
    fs::write(checkout.join("uncommitted.txt"), "user content").unwrap();
    let mut session = bamboo_agent_core::Session::new("worktree-session", "test-model");
    session.set_project_id_meta(project.id.to_string());
    session.set_workspace_path_meta(checkout.to_str().unwrap());
    session.set_last_run_status("running");
    state.storage.save_session(&session).await.unwrap();
    let response = test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;
    assert!(response.status().is_success());
    let body: Value = test::read_body_json(response).await;
    assert_eq!(body["default_root"], override_root.to_str().unwrap());
    let entries = body["repositories"][0]["worktrees"].as_array().unwrap();
    let entry = entries
        .iter()
        .find(|entry| entry["path"] == checkout.to_str().unwrap())
        .unwrap();
    assert_eq!(entry["dirty"], true);
    assert_eq!(entry["occupied"], true);
    assert_eq!(entry["sessions"][0]["id"], "worktree-session");
    // Clearing a preference does not move or remove any Git checkout.
    let response = test::call_service(
        &app,
        test::TestRequest::put()
            .uri("/worktrees/preferences")
            .set_json(json!({"root": null, "project_id": project.id}))
            .to_request(),
    )
    .await;
    assert!(response.status().is_success());
    assert!(checkout.join("uncommitted.txt").exists());
    assert_eq!(
        state.config.read().await.worktrees.default_root.as_deref(),
        global.to_str()
    );
    assert!(state.config.read().await.worktrees.project_roots.is_empty());
}

#[actix_web::test]
async fn rejects_unbound_repository_and_offline_root_without_fallback_or_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let repo = base.join("repo");
    let other = base.join("other");
    let root = base.join("worktrees");
    for path in [&repo, &other, &root] {
        fs::create_dir(path).unwrap();
    }
    for path in [&repo, &other] {
        run_git(path, &["init", "-b", "main"]);
        run_git(
            path,
            &[
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--allow-empty",
                "-m",
                "initial",
            ],
        );
    }
    let state = web::Data::new(AppState::new(base.join("data")).await.unwrap());
    let project = state
        .project_store
        .create_with_project_path("Test", None, repo.to_str().unwrap(), vec![])
        .unwrap();
    let uri = format!("/projects/{}/worktrees", project.id);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/worktrees/preferences", web::put().to(set_preferences))
            .route("/projects/{project_id}/worktrees", web::post().to(create)),
    )
    .await;
    for body in [
        json!({"repository": other, "root": root, "name": "task", "branch": "task"}),
        json!({"repository": repo, "root": base.join("offline"), "name": "task", "branch": "task"}),
        json!({"repository": repo, "root": repo.join(".git"), "name": "metadata", "branch": "metadata"}),
        json!({"repository": repo, "root": repo.join(".git/objects"), "name": "objects", "branch": "objects"}),
    ] {
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri(&uri)
                .set_json(body)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
    let response = test::call_service(
        &app,
        test::TestRequest::put()
            .uri("/worktrees/preferences")
            .set_json(json!({"root": base.join("offline")}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    for metadata in [repo.join(".git"), repo.join(".git/objects")] {
        let response = test::call_service(
            &app,
            test::TestRequest::put()
                .uri("/worktrees/preferences")
                .set_json(json!({"root": metadata}))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
    assert!(state.config.read().await.worktrees.is_empty());
    assert!(!base.join("offline").exists());
    assert!(!root.join("task").exists());
    assert_eq!(git::list(&repo).unwrap().len(), 1);
    assert_eq!(git::list(&other).unwrap().len(), 1);
}
