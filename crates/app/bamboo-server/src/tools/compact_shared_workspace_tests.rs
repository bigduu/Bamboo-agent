//! Compact callers delegate in their canonical parent's selected checkout.
use super::*;
use bamboo_engine::project_context::WORKSPACE_SOURCE_METADATA_KEY;

fn native_git(repo: &std::path::Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn recording_tool(h: &TestHarness) -> (SubAgentTool, Arc<WaitOrderPort>) {
    let port = Arc::new(WaitOrderPort::new(h.adapter.clone(), h.storage.clone()));
    port.expect_wait_on_enqueue.store(false, Ordering::SeqCst);
    port.skip_successful_enqueue.store(true, Ordering::SeqCst);
    (SubAgentTool::new(port.clone(), h.adapter.clone()), port)
}

async fn compact_create(tool: &SubAgentTool, parent: &str) -> Result<ToolResult, ToolError> {
    let call = format!("shared-workspace-{}", Uuid::new_v4());
    let diagnostic = bamboo_server_tools::sub_agent::test_diagnostics::install(parent, &call);
    let result = invoke_completed(
        tool,
        json!({"message":"Inspect this checkout without changing it"}),
        subagent_test_ctx(parent, &call),
    )
    .await;
    if result.is_err() {
        eprintln!("fixture-only SubAgent diagnostic: {:?}", diagnostic.take());
    }
    result
}

async fn assert_no_creation(
    h: &TestHarness,
    port: &WaitOrderPort,
    parent: &Session,
    before: usize,
) {
    assert_eq!(
        h.adapter.session_store.list_index_entries().await.len(),
        before
    );
    assert!(port.last_admit_child_id.read().unwrap().is_none());
    assert!(port.last_wait_child_id.read().unwrap().is_none());
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        serde_json::to_value(h.storage.load_session(&parent.id).await.unwrap().unwrap()).unwrap(),
        serde_json::to_value(parent).unwrap()
    );
}

#[tokio::test]
async fn compact_keeps_selected_native_worktree_after_project_default_changes() {
    let h = build_test_harness_with_storage(None, None, true).await;
    let fixture = tempfile::tempdir().unwrap();
    let repo = fixture.path().join("selected");
    std::fs::create_dir(&repo).unwrap();
    native_git(&repo, &["init", "-b", "main"]);
    native_git(
        &repo,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "fixture",
        ],
    );
    let other = fixture.path().join("new-default");
    native_git(
        &repo,
        &["worktree", "add", "-b", "other", other.to_str().unwrap()],
    );
    let selected = repo.canonicalize().unwrap();
    let other = other.canonicalize().unwrap();
    assert!(other.join(".git").is_file(), "real linked Git worktree");
    let project = h
        .project_store
        .create_with_project_path(
            "Shared checkout",
            None,
            selected.to_str().unwrap(),
            Vec::new(),
        )
        .unwrap();
    h.project_store
        .update_with_project_path(
            &project.id,
            project.revision,
            other.to_str().unwrap(),
            |_| Ok(()),
        )
        .unwrap();
    let (tool, port) = recording_tool(&h);
    for (version, ultra) in [(Some(1), false), (None, true)] {
        let mut parent = Session::new(format!("shared-parent-{}", Uuid::new_v4()), "gpt-5");
        parent.session_version = version;
        parent.set_root_orchestration_only(ultra).unwrap();
        parent.set_project_id_meta(project.id.to_string());
        parent.workspace = Some(other.to_string_lossy().into_owned());
        parent.set_workspace_path_meta(selected.to_string_lossy());
        parent.metadata.insert(
            WORKSPACE_SOURCE_METADATA_KEY.into(),
            "project_default".into(),
        );
        h.storage.save_session(&parent).await.unwrap();
        // Neither the legacy field nor this process publication may beat the
        // freshly loaded durable selection.
        bamboo_agent_core::workspace_state::publish_resolved_workspace(&parent.id, other.clone());
        let result = compact_create(&tool, &parent.id).await.unwrap();
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        let id = payload["actor_id"].as_str().unwrap();
        let child = h.storage.load_session(id).await.unwrap().unwrap();
        assert_eq!(
            child.workspace.as_deref(),
            selected.to_str(),
            "version={version:?}, ultra={ultra}"
        );
        assert_eq!(child.workspace_path_meta().as_deref(), selected.to_str());
        assert_eq!(child.metadata[WORKSPACE_SOURCE_METADATA_KEY], "session");
        assert_eq!(child.parent_session_id.as_deref(), Some(parent.id.as_str()));
        assert_eq!(child.root_session_id, parent.id);
        assert!(child.authority_identity.is_ordinary());
        let reopened = SessionStoreV2::new(h.workspace_path.parent().unwrap().to_path_buf())
            .await
            .unwrap();
        assert_eq!(
            reopened
                .load_session(id)
                .await
                .unwrap()
                .unwrap()
                .workspace_path_meta(),
            child.workspace_path_meta()
        );
        let retained = reopened.load_session(&parent.id).await.unwrap().unwrap();
        assert_eq!(retained.session_version, version);
        assert_eq!(retained.workspace_path_meta().as_deref(), selected.to_str());
        assert_eq!(
            port.last_admit_child_id.read().unwrap().as_deref(),
            Some(id)
        );
    }
    assert_eq!(
        h.activation.calls.load(Ordering::SeqCst),
        0,
        "recording Host, no provider activation"
    );
}

#[tokio::test]
async fn compact_does_not_replace_missing_or_foreign_parent_workspace_with_project_default() {
    for fault in ["absent", "empty", "missing", "file", "foreign"] {
        let h = build_test_harness_with_storage(None, None, true).await;
        let defaults = tempfile::tempdir().unwrap();
        let project = h
            .project_store
            .create_with_project_path(
                "Current default",
                None,
                defaults.path().to_str().unwrap(),
                Vec::new(),
            )
            .unwrap();
        let foreign = tempfile::tempdir().unwrap();
        h.project_store
            .create_with_project_path(
                "Other Project",
                None,
                foreign.path().to_str().unwrap(),
                Vec::new(),
            )
            .unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut parent = h
            .storage
            .load_session(&h.parent_session_id)
            .await
            .unwrap()
            .unwrap();
        parent.workspace = None;
        parent.set_project_id_meta(project.id.to_string());
        parent.metadata.insert(
            WORKSPACE_SOURCE_METADATA_KEY.into(),
            "project_default".into(),
        );
        match fault {
            "absent" => {}
            "empty" => parent.set_workspace_path_meta(" "),
            "missing" => {
                parent.set_workspace_path_meta(defaults.path().join("gone").to_string_lossy())
            }
            "file" => parent.set_workspace_path_meta(file.path().to_string_lossy()),
            "foreign" => parent.set_workspace_path_meta(foreign.path().to_string_lossy()),
            _ => unreachable!(),
        }
        parent.metadata_version += 1;
        h.storage.save_session(&parent).await.unwrap();
        let before = h.adapter.session_store.list_index_entries().await.len();
        let (tool, port) = recording_tool(&h);
        let result = compact_create(&tool, &parent.id).await;
        assert!(
            result.is_err(),
            "{fault}: must not choose another checkout: {result:?}"
        );
        assert_no_creation(&h, &port, &parent, before).await;
    }
}

#[tokio::test]
async fn compact_refuses_confinement_relocation_instead_of_creating_another_checkout() {
    let root = tempfile::tempdir().unwrap();
    let resolver = bamboo_agent_core::workspace_state::WorkspaceResolver::new(|| None, {
        let root = root.path().canonicalize().unwrap();
        move || bamboo_agent_core::workspace_state::WorkspaceRootConfig {
            root: root.clone(),
            confine: true,
        }
    });
    let h = build_test_harness_with_storage(None, Some(resolver), true).await;
    let mut parent = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    parent.set_workspace_path_meta(h.workspace_path.to_string_lossy());
    h.storage.save_session(&parent).await.unwrap();
    let before = h.adapter.session_store.list_index_entries().await.len();
    let before_root = std::fs::read_dir(root.path()).unwrap().count();
    let (tool, port) = recording_tool(&h);
    let result = compact_create(&tool, &parent.id).await;
    assert!(
        result.is_err(),
        "shared workspace must not be relocated: {result:?}"
    );
    assert_no_creation(&h, &port, &parent, before).await;
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), before_root);
}

#[tokio::test]
async fn compact_workspace_preserves_supervisor_legacy_and_direct_child_boundaries() {
    for (kind, ultra) in [
        ("supervisor", false),
        ("supervisor", true),
        ("child", false),
        ("legacy_root", false),
    ] {
        let h = build_test_harness_with_storage(None, None, true).await;
        let mut parent = match kind {
            "supervisor" => {
                let receipt = h
                    .storage
                    .get_or_create_default_supervisor("gpt-5")
                    .await
                    .unwrap();
                let current = h
                    .storage
                    .load_session(&receipt.session_id)
                    .await
                    .unwrap()
                    .unwrap();
                h.storage
                    .root_mode_operation(&bamboo_domain::RootModeOperationRequest {
                        session_id: current.id.clone(),
                        operation_id: format!(
                            "{}:{}",
                            current.root_mode_transition_epoch,
                            Uuid::new_v4()
                        ),
                        birth_token: current.root_mode_birth_token(),
                        expected_epoch: current.root_mode_transition_epoch,
                        requested_enabled: ultra,
                        action: bamboo_domain::RootModeOperationAction::Select,
                    })
                    .await
                    .unwrap();
                h.storage
                    .load_session(&receipt.session_id)
                    .await
                    .unwrap()
                    .unwrap()
            }
            "child" => h
                .storage
                .load_session(&h.child_session_id)
                .await
                .unwrap()
                .unwrap(),
            "legacy_root" => {
                let mut session =
                    Session::new(format!("legacy-parent-{}", Uuid::new_v4()), "gpt-5");
                session.session_version = None;
                session
            }
            _ => unreachable!(),
        };
        if kind == "supervisor" {
            assert_eq!(parent.root_orchestration_only, ultra);
        }
        // A usable legacy parent field remains sufficient without publishing
        // a new metadata plane or changing its behavior stamp.
        parent.workspace = Some(h.workspace_path.to_string_lossy().into_owned());
        parent.metadata.remove("workspace_path");
        if let Some(metadata) = &mut parent.runtime_metadata {
            metadata.workspace_path = None;
        }
        parent.metadata_version += 1;
        h.storage.save_session(&parent).await.unwrap();
        let (tool, port) = recording_tool(&h);
        if kind == "child" {
            // Direct Server-tool calls have an existing Root-only gate.
            // Nested Child execution requires its separately owned Host route;
            // a workspace policy change must not grant that route implicitly.
            let before = h.adapter.session_store.list_index_entries().await.len();
            assert!(compact_create(&tool, &parent.id).await.is_err());
            assert_no_creation(&h, &port, &parent, before).await;
            continue;
        }
        let result = compact_create(&tool, &parent.id)
            .await
            .unwrap_or_else(|error| panic!("{kind}/{ultra}: {error}"));
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        let child = h
            .storage
            .load_session(payload["actor_id"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            child.workspace_path_meta().as_deref(),
            h.workspace_path.to_str(),
            "{kind}/{ultra}"
        );
        assert_eq!(child.parent_session_id.as_deref(), Some(parent.id.as_str()));
        assert_eq!(child.root_session_id, parent.root_session_id);
        assert_eq!(child.spawn_depth, parent.spawn_depth + 1);
        assert!(child.authority_identity.is_ordinary());
        let retained = h.storage.load_session(&parent.id).await.unwrap().unwrap();
        assert_eq!(retained.authority_identity, parent.authority_identity);
        assert_eq!(retained.session_version, parent.session_version);
        assert_eq!(
            retained.root_orchestration_only,
            parent.root_orchestration_only
        );
        assert!(retained.workspace_path_meta().is_none());
        assert_eq!(h.activation.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn compact_shared_workspace_rechecks_policy_before_child_publication() {
    let root = tempfile::tempdir().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let resolver = bamboo_agent_core::workspace_state::WorkspaceResolver::new(|| None, {
        let root = root.path().canonicalize().unwrap();
        let calls = calls.clone();
        move || bamboo_agent_core::workspace_state::WorkspaceRootConfig {
            root: root.clone(),
            confine: calls.fetch_add(1, Ordering::SeqCst) >= 2,
        }
    });
    let h = build_test_harness_with_storage(None, Some(resolver), true).await;
    let mut parent = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    parent.set_workspace_path_meta(h.workspace_path.to_string_lossy());
    h.storage.save_session(&parent).await.unwrap();
    let before = h.adapter.session_store.list_index_entries().await.len();
    let (tool, port) = recording_tool(&h);
    let result = compact_create(&tool, &parent.id).await;
    assert!(
        result.is_err(),
        "a newly confined parent must not publish a child: {result:?}"
    );
    assert!(calls.load(Ordering::SeqCst) >= 3);
    assert_no_creation(&h, &port, &parent, before).await;
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
