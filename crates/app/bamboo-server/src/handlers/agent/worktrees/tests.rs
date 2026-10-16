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
    #[cfg(unix)]
    {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        let registered = global.join("registered");
        run_git(
            &repo,
            &[
                "worktree",
                "add",
                "-b",
                "bytes",
                registered.to_str().unwrap(),
                "HEAD",
            ],
        );
        let metadata = Command::new("git")
            .arg("-C")
            .arg(&registered)
            .args(["rev-parse", "--absolute-git-dir"])
            .output()
            .unwrap();
        assert!(metadata.status.success());
        let metadata = String::from_utf8(metadata.stdout).unwrap();
        let raw = global.join(std::ffi::OsString::from_vec(b"invalid-\xff".to_vec()));
        let gitdir = [raw.as_os_str().as_bytes(), b"/.git\n"].concat();
        let record = Path::new(metadata.trim()).join("gitdir");
        fs::write(&record, &gitdir).unwrap();
        let response =
            test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;
        assert!(response.status().is_success());
        let body: Value = test::read_body_json(response).await;
        assert!(body["repositories"][0]["error"].is_null());
        let entries = body["repositories"][0]["worktrees"].as_array().unwrap();
        assert_eq!(
            entries.len(),
            3,
            "Unsupported raw paths must not drop valid inventory"
        );
        assert!(entries
            .iter()
            .any(|e| e["path"] == checkout.to_str().unwrap() && e["dirty"] == true));
        let unsupported = entries.iter().find(|e| e["branch"] == "bytes").unwrap();
        assert_eq!(unsupported["path"], format!("{:?}", raw.as_os_str()));
        assert_eq!(unsupported["prunable"], true);
        assert_eq!(unsupported["path_encoding"], "escaped");
        assert!(unsupported["dirty"].is_null());
        assert!(unsupported["error"].as_str().unwrap().contains("non-UTF-8"));
        assert_eq!(fs::read(record).unwrap(), gitdir);
        assert!(registered.join(".git").is_file());
    }
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

async fn worktree_preferences_json(state: web::Data<AppState>) -> Value {
    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/worktrees/preferences", web::get().to(get_preferences)),
    )
    .await;
    test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri("/worktrees/preferences")
            .to_request(),
    )
    .await
}

fn fixture_worktree_settings(base: &Path) -> Value {
    let global = base.join("global");
    let project = base.join("project");
    fs::create_dir_all(&global).unwrap();
    fs::create_dir_all(&project).unwrap();
    json!({"default_root": global, "project_roots": {
        "10000000-0000-4000-8000-000000000001": project
    }})
}

#[actix_web::test]
async fn typed_core_worktree_update_is_live_and_survives_compatibility_write() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let data = base.join("data");
    let mut raw = AppState::new(data.clone()).await.unwrap();
    raw.stop_config_watcher_for_test();
    let state = web::Data::new(raw);
    let expected = fixture_worktree_settings(&base);
    let mut core = state
        .config_facade
        .as_ref()
        .unwrap()
        .registry()
        .envelope_value(bamboo_config::SectionId::Core)
        .unwrap();
    core.data["worktrees"] = expected.clone();
    state
        .put_ordinary_section(bamboo_config::SectionId::Core, core.revision, core.data)
        .await
        .unwrap();
    let durable = bamboo_config::ConfigFacade::open(&data)
        .unwrap()
        .effective_config();
    assert_eq!(serde_json::to_value(&durable.worktrees).unwrap(), expected);
    assert_eq!(
        worktree_preferences_json(state.clone()).await,
        expected,
        "a successful typed Core write must immediately change the manager's live preferences"
    );
    state
        .update_config(
            |candidate| {
                candidate.server.port = 54321;
                Ok(())
            },
            ConfigUpdateEffects::default(),
        )
        .await
        .unwrap();
    let durable = bamboo_config::ConfigFacade::open(&data)
        .unwrap()
        .effective_config();
    assert_eq!(durable.server.port, 54321);
    assert_eq!(
        serde_json::to_value(&durable.worktrees).unwrap(),
        expected,
        "an unrelated compatibility write must not overwrite the accepted directory settings"
    );
    assert_eq!(worktree_preferences_json(state).await, expected);
}

#[actix_web::test]
async fn external_core_worktree_reload_publishes_and_clears_live_preferences() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let data = base.join("data");
    let mut raw = AppState::new(data.clone()).await.unwrap();
    raw.stop_config_watcher_for_test();
    let state = web::Data::new(raw);
    let path = data.join("core.json");
    for expected in [fixture_worktree_settings(&base), json!({})] {
        let mut document: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        document["revision"] = json!(document["revision"].as_u64().unwrap() + 1);
        document["data"]["worktrees"] = expected.clone();
        fs::write(&path, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
        state
            .reload_ordinary_section_for_test(bamboo_config::SectionId::Core)
            .await;
        assert_eq!(
            worktree_preferences_json(state.clone()).await,
            expected,
            "external Core publication must replace both global and Project roots without restart"
        );
        let durable = bamboo_config::ConfigFacade::open(&data)
            .unwrap()
            .effective_config();
        assert_eq!(serde_json::to_value(&durable.worktrees).unwrap(), expected);
    }
}

#[actix_web::test]
async fn inventory_scopes_and_deduplicates_session_git_queries_without_losing_shared_checkouts() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().canonicalize().unwrap();
    let repo = base.join("repo");
    let unrelated = base.join("repo-history");
    let embedded = repo.join("embedded");
    for path in [&repo, &unrelated, &embedded] {
        fs::create_dir_all(path.join("subdir")).unwrap();
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
    #[cfg(unix)]
    let binding = {
        let alias = base.join("bound-alias");
        std::os::unix::fs::symlink(&repo, &alias).unwrap();
        alias
    };
    #[cfg(not(unix))]
    let binding = repo.clone();
    let state = web::Data::new(AppState::new(base.join("data")).await.unwrap());
    let project = state
        .project_store
        .create_with_project_path("Requested", None, binding.to_str().unwrap(), vec![])
        .unwrap();
    #[cfg(unix)]
    {
        // Current writers canonicalize aliases. Seed an explicitly bound
        // persisted alias to exercise legacy metadata, rather than assuming
        // that creating a new Project preserved the spelling of its input.
        assert_eq!(project.project_path.as_deref(), repo.to_str());
        let path = state.project_store.paths().manifest_path(&project.id);
        let mut stored: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        stored["project_path"] = json!(binding.to_str().unwrap());
        fs::write(&path, serde_json::to_vec_pretty(&stored).unwrap()).unwrap();
        assert_eq!(
            state
                .project_store
                .get(&project.id)
                .unwrap()
                .project_path
                .as_deref(),
            binding.to_str()
        );
    }
    let other = state
        .project_store
        .create_with_project_path("Unrelated", None, unrelated.to_str().unwrap(), vec![])
        .unwrap();
    let selected = Some(project.id.to_string());
    let foreign = Some(other.id.to_string());
    let nested = repo.join("subdir");
    let unrelated_nested = unrelated.join("subdir");
    let escaped = repo.join("../repo-history/subdir");
    let mut sessions = vec![
        ("current-a".to_string(), selected.clone(), nested.clone()),
        ("current-b".to_string(), selected.clone(), nested.clone()),
        (
            "shared-other-project".to_string(),
            foreign.clone(),
            repo.clone(),
        ),
        ("legacy-projectless".to_string(), None, repo.clone()),
        (
            "misplaced-current-project".to_string(),
            selected.clone(),
            unrelated_nested.clone(),
        ),
        (
            "parent-escape".to_string(),
            foreign.clone(),
            escaped.clone(),
        ),
        (
            "embedded-repository".to_string(),
            foreign.clone(),
            embedded.join("subdir"),
        ),
    ];
    #[cfg(unix)]
    sessions.push(("bound-alias".to_string(), selected, binding.join("subdir")));
    for index in 0..24 {
        sessions.push((
            format!("unrelated-history-{index}"),
            foreign.clone(),
            unrelated_nested.clone(),
        ));
    }
    for (id, project_id, workspace) in sessions {
        let mut session = bamboo_agent_core::Session::new(id, "local-test-no-execution");
        if let Some(project_id) = project_id {
            session.set_project_id_meta(project_id);
        }
        session.set_workspace_path_meta(workspace.to_str().unwrap());
        session.set_last_run_status("running");
        state.storage.save_session(&session).await.unwrap();
    }
    let app = test::init_service(
        App::new()
            .app_data(state)
            .route("/projects/{project_id}/worktrees", web::get().to(list)),
    )
    .await;
    let body: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri(&format!("/projects/{}/worktrees", project.id))
            .to_request(),
    )
    .await;
    let entries = body["repositories"][0]["worktrees"].as_array().unwrap();
    let entry = entries
        .iter()
        .find(|entry| entry["path"] == repo.to_str().unwrap())
        .unwrap();
    let ids: HashSet<_> = entry["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|session| session["id"].as_str().unwrap())
        .collect();
    let expected = HashSet::from([
        "current-a",
        "current-b",
        "shared-other-project",
        "legacy-projectless",
    ]);
    #[cfg(unix)]
    let expected: HashSet<_> = expected.into_iter().chain(["bound-alias"]).collect();
    assert_eq!(
        ids, expected,
        "shared/legacy and nested workspaces remain associated; nested repositories do not"
    );
    assert_eq!(entry["occupied"], true);
    assert_eq!(
        git::workspace_root_queries_for_test(&unrelated_nested),
        0,
        "unrelated historical and misplaced-current-Project paths must not invoke Git"
    );
    assert_eq!(
        git::workspace_root_queries_for_test(&escaped),
        0,
        "lexical parent components must not escape the candidate scope"
    );
    assert_eq!(
        git::workspace_root_queries_for_test(&nested),
        1,
        "repeated Session workspace paths need only one real Git resolution"
    );
}
