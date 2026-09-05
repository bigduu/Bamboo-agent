//! Real session_history export -> immutable files -> bounded Read coverage.

use std::{io, path::Path, sync::Arc};

use async_trait::async_trait;
use bamboo_agent_core::{
    ConversationSummary, Message, Session, Storage, Tool, ToolCtx, ToolOutcome,
};
use bamboo_domain::{
    session::task::{TaskItem, TaskItemStatus, TaskList},
    ProjectId,
};
use bamboo_server_tools::SessionInspectorTool;
use bamboo_storage::SessionStoreV2;
use bamboo_tools::tools::ReadTool;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn ctx(caller: &str) -> ToolCtx {
    let mut ctx = ToolCtx::none("context-export-test");
    ctx.session_id = Some(Arc::from(caller));
    ctx
}

fn completed(outcome: ToolOutcome) -> String {
    let ToolOutcome::Completed(result) = outcome else {
        panic!("expected completed tool")
    };
    assert!(result.success, "{}", result.result);
    result.result
}

async fn export(tool: &SessionInspectorTool, caller: &str, target: &str) -> Value {
    let outcome = tool
        .invoke(
            json!({"action": "export_context", "session_id": target}),
            ctx(caller),
        )
        .await
        .expect("context export succeeds");
    serde_json::from_str(&completed(outcome)).unwrap()
}

struct Fixture {
    home: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    tool: SessionInspectorTool,
    root: Session,
    child: Session,
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(
            SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let root = Session::new("context-root", "test-model");
        let child = Session::new_child_of("context-child", &root, "test-model", "Child preview");
        for session in [&root, &child] {
            store.save_session(session).await.unwrap();
        }
        let tool =
            SessionInspectorTool::new(store.clone(), Arc::new(ControlPlanePortOnly(store.clone())));
        Self {
            home,
            store,
            tool,
            root,
            child,
        }
    }

    fn cache(&self) -> std::path::PathBuf {
        self.home.path().join("coordination/session-context/v1")
    }

    async fn session_dir(&self, id: &str) -> std::path::PathBuf {
        self.home
            .path()
            .join(self.store.resolve_rel_path(id).await.unwrap())
    }
}

/// Export requests the control-plane port, never an explicit full load or write.
/// V2 may internally read session.json when its runtime sidecar is unavailable.
struct ControlPlanePortOnly(Arc<SessionStoreV2>);

#[async_trait]
impl Storage for ControlPlanePortOnly {
    async fn load_runtime_control_plane(&self, id: &str) -> io::Result<Option<Session>> {
        self.0.load_runtime_control_plane(id).await
    }
    async fn load_session(&self, _: &str) -> io::Result<Option<Session>> {
        panic!("export must not explicitly request a full session load")
    }
    async fn save_session(&self, _: &Session) -> io::Result<()> {
        panic!("export must not save session state")
    }
    async fn delete_session(&self, _: &str) -> io::Result<bool> {
        panic!("export must not delete session state")
    }
}

fn contents(receipt: &Value, name: &str) -> String {
    std::fs::read_to_string(receipt[format!("{name}_path")].as_str().unwrap()).unwrap()
}

fn verify_bundle(receipt: &Value, home: &Path) {
    assert_eq!(receipt["schema_version"], "session-context-view.v1");
    let revision = receipt["revision"].as_str().unwrap();
    assert_eq!(revision.len(), 64);
    assert!(revision.bytes().all(|b| b.is_ascii_hexdigit()));
    let manifest_path = Path::new(receipt["manifest_path"].as_str().unwrap());
    assert!(manifest_path.is_absolute());
    assert!(manifest_path.starts_with(
        home.canonicalize()
            .unwrap()
            .join("coordination/session-context/v1")
    ));
    assert_eq!(
        manifest_path.parent().unwrap().file_name().unwrap(),
        revision
    );
    let manifest: Value =
        serde_json::from_str(&std::fs::read_to_string(manifest_path).unwrap()).unwrap();
    assert_eq!(manifest["revision"], revision);
    assert_eq!(manifest["observation"], "last_persisted");
    assert!(manifest["source_observed_at"].as_str().is_some());
    assert_eq!(manifest["source_digest"].as_str().unwrap().len(), 64);
    for (name, max_bytes, max_lines) in [("status", 8192, 40), ("brief", 16384, 120)] {
        let body = contents(receipt, name);
        assert!(body.len() <= max_bytes, "{name} exceeds byte budget");
        assert!(
            body.lines().count() <= max_lines,
            "{name} exceeds line budget"
        );
        assert!(
            body.lines().all(|line| line.len() <= 512),
            "{name} has oversized UTF-8 line"
        );
        assert_eq!(receipt["files"][name]["bytes"], body.len());
        assert_eq!(receipt["files"][name]["lines"], body.lines().count());
        assert_eq!(
            receipt["files"][name]["sha256"],
            hex::encode(Sha256::digest(body.as_bytes()))
        );
        assert_eq!(
            Path::new(receipt[format!("{name}_path")].as_str().unwrap()).parent(),
            manifest_path.parent()
        );
    }
}

#[tokio::test]
async fn root_exports_self_and_child_then_reads_exact_bounded_continuations() {
    let f = Fixture::new().await;
    for target in [&f.root.id, &f.child.id] {
        let receipt = export(&f.tool, &f.root.id, target).await;
        verify_bundle(&receipt, f.home.path());
        assert_eq!(receipt["scope"]["caller_session_id"], f.root.id);
        assert_eq!(receipt["scope"]["root_session_id"], f.root.id);
        assert_eq!(receipt["scope"]["target_session_id"], *target);
        for (name, offset) in [("status", 0), ("status", 2), ("brief", 0), ("brief", 2)] {
            let read = completed(ReadTool::new().invoke(json!({
                "file_path": receipt[format!("{name}_path")], "offset": offset, "limit": 2
            }), ctx(&f.root.id)).await.unwrap());
            let body = contents(&receipt, name);
            let lines: Vec<_> = body.lines().collect();
            assert!(lines.len() > offset + 2, "fixture needs a continuation");
            let expected = lines[offset..offset + 2]
                .iter()
                .enumerate()
                .map(|(i, line)| format!("{:>6}\t{line}", offset + i + 1))
                .collect::<Vec<_>>()
                .join("\n");
            assert!(read.starts_with(&expected), "{read}");
            assert_eq!(read.lines().filter(|line| line.contains('\t')).count(), 2);
            assert!(read.contains(&format!("Continue with offset={}", offset + 2)));
        }
    }
}

#[tokio::test]
async fn readonly_export_requests_control_plane_and_never_leaks_private_payloads() {
    let mut f = Fixture::new().await;
    f.child
        .add_message(Message::system("PRIVATE-LEAK-SYSTEM-PROMPT"));
    f.child
        .add_message(Message::user("PRIVATE-LEAK-TRANSCRIPT"));
    f.child.conversation_summary = Some(ConversationSummary::new("PRIVATE-LEAK-SUMMARY", 2, 20));
    f.child
        .metadata
        .insert("private".into(), "PRIVATE-LEAK-METADATA".into());
    f.child
        .metadata
        .insert("workspace_path".into(), "/PRIVATE-LEAK-WORKSPACE".into());
    f.child.set_pending_question(
        "PRIVATE-LEAK-CALL".into(),
        "PRIVATE-LEAK-TOOL".into(),
        "PRIVATE-LEAK-QUESTION".into(),
        vec!["PRIVATE-LEAK-OPTION".into()],
        true,
    );
    f.child.set_last_run_status("completed");
    f.child.task_list = Some(TaskList {
        session_id: f.child.id.clone(),
        title: "Visible task list".into(),
        items: vec![TaskItem {
            id: "visible-task".into(),
            description: "Visible task description".into(),
            status: TaskItemStatus::InProgress,
            notes: "PRIVATE-LEAK-TASK-NOTES".into(),
            ..Default::default()
        }],
        created_at: f.child.created_at,
        updated_at: f.child.updated_at,
    });
    f.store.save_session(&f.child).await.unwrap();
    let before = serde_json::to_value(f.store.load_session(&f.child.id).await.unwrap()).unwrap();
    let mut readonly = ctx(&f.root.id);
    readonly.plan_read_only = true;
    let receipt: Value = serde_json::from_str(&completed(
        f.tool
            .invoke(
                json!({"action":"export_context", "session_id":f.child.id}),
                readonly,
            )
            .await
            .unwrap(),
    ))
    .unwrap();
    let all = format!(
        "{}{}{}{}",
        receipt,
        contents(&receipt, "manifest"),
        contents(&receipt, "status"),
        contents(&receipt, "brief")
    );
    assert!(
        !all.contains("PRIVATE-LEAK-"),
        "private payload escaped projection"
    );
    assert!(contents(&receipt, "brief").contains("Visible task description"));
    assert!(contents(&receipt, "status").contains("completed"));
    assert!(contents(&receipt, "status").contains("pending question: true"));
    assert_eq!(
        before,
        serde_json::to_value(f.store.load_session(&f.child.id).await.unwrap()).unwrap()
    );
}

#[tokio::test]
async fn valid_runtime_sidecars_allow_export_with_corrupt_full_session_files() {
    let f = Fixture::new().await;
    for id in [&f.root.id, &f.child.id] {
        let directory = f.session_dir(id).await;
        assert!(directory.join("runtime.json").is_file());
        std::fs::write(directory.join("session.json"), "invalid full Session JSON").unwrap();
        assert!(f.store.load_session(id).await.is_err());
    }
    let receipt = export(&f.tool, &f.root.id, &f.child.id).await;
    verify_bundle(&receipt, f.home.path());
    assert_eq!(receipt["scope"]["target_session_id"], f.child.id);
    for id in [&f.root.id, &f.child.id] {
        assert_eq!(
            std::fs::read_to_string(f.session_dir(id).await.join("session.json")).unwrap(),
            "invalid full Session JSON"
        );
    }
}

#[tokio::test]
async fn legacy_missing_sidecars_fall_back_without_exporting_or_rewriting_transcript() {
    let mut f = Fixture::new().await;
    f.child
        .add_message(Message::user("PRIVATE-LEAK-LEGACY-TRANSCRIPT"));
    f.store.save_session(&f.child).await.unwrap();
    let mut originals = Vec::new();
    for id in [&f.root.id, &f.child.id] {
        let directory = f.session_dir(id).await;
        std::fs::remove_file(directory.join("runtime.json")).unwrap();
        originals.push((
            directory.clone(),
            std::fs::read(directory.join("session.json")).unwrap(),
        ));
    }
    let receipt = export(&f.tool, &f.root.id, &f.child.id).await;
    verify_bundle(&receipt, f.home.path());
    let published = format!(
        "{receipt}{}{}{}",
        contents(&receipt, "manifest"),
        contents(&receipt, "status"),
        contents(&receipt, "brief")
    );
    assert!(!published.contains("PRIVATE-LEAK-LEGACY-TRANSCRIPT"));
    for (directory, original) in originals {
        assert!(!directory.join("runtime.json").exists());
        assert_eq!(
            std::fs::read(directory.join("session.json")).unwrap(),
            original
        );
    }
}

#[tokio::test]
async fn missing_caller_and_cross_root_requests_fail_authorization_before_publication() {
    let f = Fixture::new().await;
    let other = Session::new("other-root", "test-model");
    f.store.save_session(&other).await.unwrap();
    for (caller, target, reason) in [
        (
            None,
            f.child.id.as_str(),
            "requires a session_id in tool context",
        ),
        (Some("missing"), f.child.id.as_str(), "session not found"),
        (
            Some(f.child.id.as_str()),
            f.child.id.as_str(),
            "a persisted Root caller is required",
        ),
        (
            Some(f.root.id.as_str()),
            other.id.as_str(),
            "target must share the caller's root",
        ),
        (Some(f.root.id.as_str()), "missing", "session not found"),
        (
            Some("other-root"),
            f.child.id.as_str(),
            "target must share the caller's root",
        ),
    ] {
        let context = caller
            .map(ctx)
            .unwrap_or_else(|| ToolCtx::none("missing-caller"));
        let error = f
            .tool
            .invoke(
                json!({"action":"export_context", "session_id":target}),
                context,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains(reason),
            "caller={caller:?}, target={target}: {error}"
        );
        assert!(!f.cache().exists(), "denied export published a directory");
    }
    for (args, reason) in [
        (
            json!({"action":"export_context"}),
            "missing field `session_id`",
        ),
        (
            json!({"action":"export_context", "session_id":"../escape"}),
            "invalid session identity",
        ),
    ] {
        let error = f.tool.invoke(args, ctx(&f.root.id)).await.unwrap_err();
        assert!(error.to_string().contains(reason), "{error}");
    }
    assert!(!f.cache().exists());
}

#[tokio::test]
async fn caller_root_and_output_arguments_are_rejected_before_publication() {
    let f = Fixture::new().await;
    for key in ["caller_session_id", "root_session_id", "output_path"] {
        let mut args = json!({"action":"export_context", "session_id":f.child.id});
        args[key] = json!(f.home.path().join("forged-output"));
        let error = f.tool.invoke(args, ctx(&f.root.id)).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("only accepts action and session_id"),
            "{key}: {error}"
        );
        assert!(
            !f.cache().exists(),
            "unknown argument published a directory"
        );
        assert!(!f.home.path().join("forged-output").exists());
    }
}

#[tokio::test]
async fn optional_project_identity_must_match_exactly_and_be_valid() {
    let project_a = ProjectId::new().to_string();
    let project_b = ProjectId::new().to_string();
    for (root_project, child_project, allowed) in [
        (Some(project_a.as_str()), Some(project_a.as_str()), true),
        (Some(project_a.as_str()), Some(project_b.as_str()), false),
        (Some(project_a.as_str()), None, false),
        (None, Some(project_a.as_str()), false),
        (Some("../invalid"), Some("../invalid"), false),
        (Some(""), Some(""), false),
    ] {
        let mut f = Fixture::new().await;
        if let Some(project) = root_project {
            f.root.set_project_id_meta(project);
        }
        if let Some(project) = child_project {
            f.child.set_project_id_meta(project);
        }
        for session in [&f.root, &f.child] {
            f.store.save_session(session).await.unwrap();
        }
        let result = f
            .tool
            .invoke(
                json!({"action":"export_context", "session_id":f.child.id}),
                ctx(&f.root.id),
            )
            .await;
        if allowed {
            let receipt: Value = serde_json::from_str(&completed(result.unwrap())).unwrap();
            assert_eq!(receipt["scope"]["project_id"], project_a);
        } else {
            let error = result.unwrap_err();
            let reason = if matches!(root_project, Some("../invalid" | "")) {
                "invalid persisted Project identity"
            } else {
                "target must share the caller's root and exact optional Project identity"
            };
            assert!(
                error.to_string().contains(reason),
                "{root_project:?} -> {child_project:?}: {error}"
            );
            assert!(!f.cache().exists());
        }
    }
}

#[tokio::test]
async fn snapshot_revision_survives_restart_and_source_updates_preserve_old_files() {
    let mut f = Fixture::new().await;
    let first = export(&f.tool, &f.root.id, &f.child.id).await;
    let old = [
        contents(&first, "manifest"),
        contents(&first, "status"),
        contents(&first, "brief"),
    ];
    let reopened = Arc::new(
        SessionStoreV2::new(f.home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let restarted =
        SessionInspectorTool::new(reopened.clone(), Arc::new(ControlPlanePortOnly(reopened)));
    let same = export(&restarted, &f.root.id, &f.child.id).await;
    assert_eq!(same["revision"], first["revision"]);
    assert_eq!(same["reused"], true);
    f.child.title = "Updated persisted title".into();
    f.store.save_session(&f.child).await.unwrap();
    let changed = export(&restarted, &f.root.id, &f.child.id).await;
    assert_ne!(changed["revision"], first["revision"]);
    verify_bundle(&changed, f.home.path());
    assert_eq!(
        old,
        [
            contents(&first, "manifest"),
            contents(&first, "status"),
            contents(&first, "brief")
        ]
    );
}

#[tokio::test]
async fn snapshot_quota_preserves_references_and_still_reuses_existing_revision() {
    let mut f = Fixture::new().await;
    let mut snapshots = Vec::new();
    for i in 0..64 {
        f.child.title = format!("Snapshot {i}");
        f.store.save_session(&f.child).await.unwrap();
        snapshots.push(export(&f.tool, &f.root.id, &f.child.id).await);
    }
    let reused = export(&f.tool, &f.root.id, &f.child.id).await;
    assert_eq!(reused["reused"], true);
    assert_eq!(reused["revision"], snapshots[63]["revision"]);
    f.child.title = "Snapshot 65 exceeds quota".into();
    f.store.save_session(&f.child).await.unwrap();
    let error = f
        .tool
        .invoke(
            json!({"action":"export_context", "session_id":f.child.id}),
            ctx(&f.root.id),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("context_snapshot_quota"),
        "{error}"
    );
    for receipt in snapshots {
        verify_bundle(&receipt, f.home.path());
    }
}

#[tokio::test]
async fn concurrent_identical_exports_return_one_complete_snapshot() {
    let f = Fixture::new().await;
    let receipts =
        futures::future::join_all((0..8).map(|_| export(&f.tool, &f.root.id, &f.child.id))).await;
    for receipt in &receipts {
        assert_eq!(receipt["revision"], receipts[0]["revision"]);
        verify_bundle(receipt, f.home.path());
    }
    let snapshot = Path::new(receipts[0]["manifest_path"].as_str().unwrap())
        .parent()
        .unwrap();
    assert_eq!(std::fs::read_dir(snapshot).unwrap().count(), 3);
    assert_eq!(
        std::fs::read_dir(snapshot.parent().unwrap())
            .unwrap()
            .filter(|entry| entry.as_ref().unwrap().file_type().unwrap().is_dir())
            .count(),
        1
    );
}

#[tokio::test]
async fn oversized_multiline_unicode_input_keeps_bounded_files_lines_and_task_count() {
    let mut f = Fixture::new().await;
    f.child.title = "中文🪷\r\n\t".repeat(4000);
    f.child.task_list = Some(TaskList {
        session_id: f.child.id.clone(),
        title: "任务".repeat(2000),
        items: (0..100)
            .map(|i| TaskItem {
                id: format!("task-{i:03}"),
                description: format!("TASK-MARKER-{i:03} {}", "描述🌱\n".repeat(1000)),
                ..Default::default()
            })
            .collect(),
        created_at: f.child.created_at,
        updated_at: f.child.updated_at,
    });
    f.store.save_session(&f.child).await.unwrap();
    let receipt = export(&f.tool, &f.root.id, &f.child.id).await;
    verify_bundle(&receipt, f.home.path());
    let brief = contents(&receipt, "brief");
    assert!(brief.contains("TASK-MARKER-000"));
    assert!(!brief.contains("TASK-MARKER-032"));
    assert!(
        format!("{receipt}{brief}")
            .to_lowercase()
            .contains("truncat"),
        "truncation must be explicit"
    );
}

#[tokio::test]
async fn corrupt_or_partial_committed_snapshot_is_never_reported_as_success() {
    for name in ["manifest", "status", "brief"] {
        let f = Fixture::new().await;
        let first = export(&f.tool, &f.root.id, &f.child.id).await;
        let path = Path::new(first[format!("{name}_path")].as_str().unwrap());
        std::fs::write(path, "corrupt snapshot").unwrap();
        assert!(f
            .tool
            .invoke(
                json!({"action":"export_context", "session_id":f.child.id}),
                ctx(&f.root.id)
            )
            .await
            .is_err());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "corrupt snapshot");
        std::fs::remove_file(path).unwrap();
        assert!(f
            .tool
            .invoke(
                json!({"action":"export_context", "session_id":f.child.id}),
                ctx(&f.root.id)
            )
            .await
            .is_err());
        assert!(!path.exists());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_output_component_or_file_cannot_redirect_export() {
    use std::os::unix::fs::symlink;
    let f = Fixture::new().await;
    let outside = tempfile::tempdir().unwrap();
    symlink(outside.path(), f.home.path().join("coordination")).unwrap();
    assert!(f
        .tool
        .invoke(
            json!({"action":"export_context", "session_id":f.child.id}),
            ctx(&f.root.id)
        )
        .await
        .is_err());
    assert_eq!(std::fs::read_dir(outside.path()).unwrap().count(), 0);
    for name in ["manifest", "status", "brief"] {
        let f = Fixture::new().await;
        let receipt = export(&f.tool, &f.root.id, &f.child.id).await;
        let path = Path::new(receipt[format!("{name}_path")].as_str().unwrap());
        let outside_file = outside.path().join(format!("{name}-sentinel"));
        std::fs::write(&outside_file, "untouched sentinel").unwrap();
        std::fs::remove_file(path).unwrap();
        symlink(&outside_file, path).unwrap();
        assert!(f
            .tool
            .invoke(
                json!({"action":"export_context", "session_id":f.child.id}),
                ctx(&f.root.id)
            )
            .await
            .is_err());
        assert_eq!(
            std::fs::read_to_string(&outside_file).unwrap(),
            "untouched sentinel"
        );
    }
}
