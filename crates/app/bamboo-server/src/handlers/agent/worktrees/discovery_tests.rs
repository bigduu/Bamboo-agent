use super::*;
use actix_web::{test, App};
use std::fs;
use std::process::Command;

fn committed_repo(path: &Path) {
    fs::create_dir_all(path).unwrap();
    for args in [
        vec!["init", "-b", "main"],
        vec![
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "initial",
        ],
    ] {
        let output = Command::new("git")
            .arg("-C")
            .arg(path)
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
}

#[actix_web::test]
async fn container_project_discovers_and_creates_from_selected_repo_without_rebinding() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let container = base.join("project");
    let frontend = container.join("frontend");
    let backend = container.join("services/backend");
    let destination = base.join("worktrees");
    committed_repo(&frontend);
    committed_repo(&backend);
    fs::create_dir(&destination).unwrap();
    fs::write(frontend.join("draft.txt"), "keep this uncommitted work").unwrap();
    let state = web::Data::new(AppState::new(base.join("data")).await.unwrap());
    let project = state
        .project_store
        .create_with_project_path("Container", None, container.to_str().unwrap(), vec![])
        .unwrap();
    let manifest = state.project_store.paths().manifest_path(&project.id);
    let before = fs::read(&manifest).unwrap();
    let uri = format!("/projects/{}/worktrees", project.id);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/projects/{id}/worktrees", web::get().to(list))
            .route("/projects/{id}/worktrees", web::post().to(create)),
    )
    .await;
    let body: Value =
        test::call_and_read_body_json(&app, test::TestRequest::get().uri(&uri).to_request()).await;
    let repositories = body["repositories"].as_array().unwrap();
    let paths: HashSet<_> = repositories
        .iter()
        .map(|repo| repo["path"].as_str().unwrap())
        .collect();
    assert_eq!(
        paths,
        HashSet::from([frontend.to_str().unwrap(), backend.to_str().unwrap()])
    );
    assert!(repositories.iter().all(|repo| repo["error"].is_null()));
    assert!(repositories
        .iter()
        .all(|repo| repo["worktrees"][0]["project_owned"] == true));
    assert!(repositories
        .iter()
        .all(|repo| repo["worktrees"][0]["openable"] == true));
    let response = test::call_service(&app, test::TestRequest::post().uri(&uri)
        .set_json(json!({"repository": backend, "root": destination, "name": "backend-task", "branch": "feature/backend-task"}))
        .to_request()).await;
    assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
    let result: Value = test::read_body_json(response).await;
    let checkout = destination.join("backend-task");
    assert_eq!(result["path"], checkout.to_str().unwrap());
    assert_eq!(
        git::repository(&checkout).unwrap(),
        git::repository(&backend).unwrap()
    );
    assert_eq!(git::list(&frontend).unwrap().len(), 1);
    let body: Value =
        test::call_and_read_body_json(&app, test::TestRequest::get().uri(&uri).to_request()).await;
    let created = body["repositories"]
        .as_array()
        .unwrap()
        .iter()
        .find(|repo| repo["path"] == backend.to_str().unwrap())
        .unwrap()["worktrees"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["path"] == checkout.to_str().unwrap())
        .unwrap();
    assert_eq!(created["branch"], "feature/backend-task");
    assert_eq!(created["project_owned"], false);
    assert_eq!(
        fs::read(&manifest).unwrap(),
        before,
        "discovery and creation must not mutate Project bindings"
    );
    assert!(!container.join(".git").exists());
    assert_eq!(
        fs::read_to_string(frontend.join("draft.txt")).unwrap(),
        "keep this uncommitted work"
    );
    state.shutdown().await;
}

#[actix_web::test]
async fn broken_and_excluded_sources_do_not_hide_valid_repositories_or_gain_create_authority() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let container = base.join("project");
    let good = container.join("good");
    let ignored = container.join("node_modules/ignored");
    let broken = container.join("broken");
    let forged = container.join("forged");
    let outside = base.join("outside");
    let destination = base.join("worktrees");
    for repo in [&good, &ignored, &outside] {
        committed_repo(repo);
    }
    fs::create_dir(&broken).unwrap();
    fs::write(
        broken.join(".git"),
        "gitdir: /nonexistent/bamboo-2231-metadata\n",
    )
    .unwrap();
    fs::create_dir(&forged).unwrap();
    fs::write(
        forged.join(".git"),
        format!("gitdir: {}\n", outside.join(".git").display()),
    )
    .unwrap();
    fs::create_dir(&destination).unwrap();
    fs::create_dir(good.join("ordinary-subdirectory")).unwrap();
    // A native linked checkout outside the Project shares Git identity, but is
    // not a source unless independently bound to this Project.
    let linked = git::create(&good, &destination, "native", "native").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, container.join("alias")).unwrap();
    let state = web::Data::new(AppState::new(base.join("data")).await.unwrap());
    let project = state
        .project_store
        .create_with_project_path("Container", None, container.to_str().unwrap(), vec![])
        .unwrap();
    let uri = format!("/projects/{}/worktrees", project.id);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/projects/{id}/worktrees", web::get().to(list))
            .route("/projects/{id}/worktrees", web::post().to(create)),
    )
    .await;
    let body: Value =
        test::call_and_read_body_json(&app, test::TestRequest::get().uri(&uri).to_request()).await;
    let repositories = body["repositories"].as_array().unwrap();
    assert_eq!(repositories.len(), 3);
    assert!(repositories
        .iter()
        .any(|repo| repo["path"] == good.to_str().unwrap() && repo["error"].is_null()));
    assert!(repositories
        .iter()
        .any(|repo| repo["path"] == broken.to_str().unwrap() && repo["error"].is_string()));
    assert!(repositories
        .iter()
        .any(|repo| repo["path"] == forged.to_str().unwrap() && repo["error"].is_string()));
    let mut rejected = vec![
        ignored,
        outside.clone(),
        linked,
        good.join("ordinary-subdirectory"),
        broken,
        forged,
    ];
    #[cfg(unix)]
    rejected.push(container.join("alias"));
    for (index, repository) in rejected.into_iter().enumerate() {
        let name = format!("denied-{index}");
        let response = test::call_service(&app, test::TestRequest::post().uri(&uri)
            .set_json(json!({"repository": repository, "root": destination, "name": name, "branch": name}))
            .to_request()).await;
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
        assert!(!destination.join(&name).exists());
    }
    #[cfg(unix)]
    {
        let relocated = base.join("relocated");
        fs::rename(&good, &relocated).unwrap();
        std::os::unix::fs::symlink(&relocated, &good).unwrap();
        let response = test::call_service(&app, test::TestRequest::post().uri(&uri)
            .set_json(json!({"repository": good, "root": destination, "name": "stale", "branch": "stale"}))
            .to_request()).await;
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
        assert!(!destination.join("stale").exists());
        assert!(!git::list(&relocated)
            .unwrap()
            .iter()
            .any(|entry| entry.branch.as_deref() == Some("stale")));
    }
    assert_eq!(git::list(&outside).unwrap().len(), 1);
    let outside_branches: Vec<_> = fs::read_dir(outside.join(".git/refs/heads"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(outside_branches, vec![std::ffi::OsString::from("main")]);
    state.shutdown().await;
}

#[actix_web::test]
async fn explicitly_bound_repository_subdirectory_remains_the_only_selected_source() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let repo = base.join("repo");
    committed_repo(&repo);
    let binding = repo.join("subdirectory");
    fs::create_dir(&binding).unwrap();
    let destination = base.join("worktrees");
    fs::create_dir(&destination).unwrap();
    let state = web::Data::new(AppState::new(base.join("data")).await.unwrap());
    let project = state
        .project_store
        .create_with_project_path("Subdirectory", None, binding.to_str().unwrap(), vec![])
        .unwrap();
    let uri = format!("/projects/{}/worktrees", project.id);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/projects/{id}/worktrees", web::get().to(list))
            .route("/projects/{id}/worktrees", web::post().to(create)),
    )
    .await;
    let body: Value =
        test::call_and_read_body_json(&app, test::TestRequest::get().uri(&uri).to_request()).await;
    assert_eq!(body["repositories"].as_array().unwrap().len(), 1);
    assert_eq!(body["repositories"][0]["path"], binding.to_str().unwrap());
    for (source, name, expected) in [
        (&repo, "ancestor", actix_web::http::StatusCode::BAD_REQUEST),
        (&binding, "selected", actix_web::http::StatusCode::CREATED),
    ] {
        let response = test::call_service(&app, test::TestRequest::post().uri(&uri)
            .set_json(json!({"repository": source, "root": destination, "name": name, "branch": name}))
            .to_request()).await;
        assert_eq!(response.status(), expected);
    }
    assert!(!destination.join("ancestor").exists());
    state.shutdown().await;
}

#[actix_web::test]
async fn truncated_descendants_warn_without_duplicate_or_unselectable_repository_rows() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let container = base.join("project");
    let mut repository = container.clone();
    for _ in 0..12 {
        repository.push("nested");
    }
    committed_repo(&repository);
    fs::create_dir(repository.join("beyond-scan-depth")).unwrap();
    let destination = base.join("worktrees");
    fs::create_dir(&destination).unwrap();
    let state = web::Data::new(AppState::new(base.join("data")).await.unwrap());
    let project = state
        .project_store
        .create_with_project_path("Container", None, container.to_str().unwrap(), vec![])
        .unwrap();
    let uri = format!("/projects/{}/worktrees", project.id);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/projects/{id}/worktrees", web::get().to(list))
            .route("/projects/{id}/worktrees", web::post().to(create)),
    )
    .await;
    let body: Value =
        test::call_and_read_body_json(&app, test::TestRequest::get().uri(&uri).to_request()).await;
    let repositories = body["repositories"].as_array().unwrap();
    assert_eq!(repositories.len(), 1);
    assert_eq!(repositories[0]["path"], repository.to_str().unwrap());
    assert!(repositories[0]["error"].is_null());
    assert!(repositories[0]["warnings"][0]
        .as_str()
        .unwrap()
        .contains("truncated at depth"));
    let response = test::call_service(&app, test::TestRequest::post().uri(&uri)
        .set_json(json!({"repository": repository, "root": destination, "name": "selected", "branch": "selected"}))
        .to_request()).await;
    assert_eq!(response.status(), actix_web::http::StatusCode::CREATED);
    state.shutdown().await;
}

#[actix_web::test]
async fn discovered_submodule_opens_its_owned_checkout_instead_of_git_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let container = base.join("project");
    let parent = container.join("parent");
    let source = base.join("submodule-source");
    committed_repo(&parent);
    committed_repo(&source);
    let output = Command::new("git")
        .arg("-C")
        .arg(&parent)
        .args(["-c", "protocol.file.allow=always", "submodule", "add"])
        .arg(&source)
        .arg("modules/child")
        .env("GIT_ALLOW_PROTOCOL", "file")
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
    let submodule = parent.join("modules/child");
    let state = web::Data::new(AppState::new(base.join("data")).await.unwrap());
    let project = state
        .project_store
        .create_with_project_path("Container", None, container.to_str().unwrap(), vec![])
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .route("/projects/{id}/worktrees", web::get().to(list)),
    )
    .await;
    let body: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri(&format!("/projects/{}/worktrees", project.id))
            .to_request(),
    )
    .await;
    let repository = body["repositories"]
        .as_array()
        .unwrap()
        .iter()
        .find(|repo| repo["path"] == submodule.to_str().unwrap())
        .unwrap();
    assert!(repository["error"].is_null());
    let entries = repository["worktrees"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["path"], submodule.to_str().unwrap());
    assert_eq!(entries[0]["project_owned"], true);
    assert_eq!(entries[0]["openable"], true);
    assert_eq!(entries[0]["dirty"], false);
    assert!(entries[0]["error"].is_null());
    state.shutdown().await;
}
