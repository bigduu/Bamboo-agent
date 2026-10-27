//! Real canonical scopes, persisted Roots, immutable files and retained reads.
#![cfg(any(target_os = "linux", target_os = "macos"))]

use bamboo_agent_core::tools::ExecutingSupervisorObservation;
use bamboo_agent_core::{Message, Session, Storage, Tool, ToolCtx, ToolOutcome};
use bamboo_domain::session::task::{TaskItem, TaskItemStatus, TaskList};
use bamboo_domain::{
    ActorAncestorObservation, HostScopeSessionObservation, HostSessionRight as Right,
    HostSessionScopeBinding, HostSessionTargetScope, SessionCallerReference,
    SupervisorManagementMutation as Mutation, SupervisorManagementRequest, SupervisorReference,
    HOST_SESSION_SCOPE_VERSION,
};
use bamboo_server_tools::SessionInspectorTool;
use bamboo_storage::SessionStoreV2;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

struct Fixture {
    home: tempfile::TempDir,
    store: Arc<SessionStoreV2>,
    supervisor: Session,
    reference: SupervisorReference,
    targets: Vec<Session>,
}

fn ctx(caller: &Session) -> ToolCtx {
    let mut ctx = ToolCtx::none("scoped-context");
    ctx.session_id = Some(Arc::from(caller.id.as_str()));
    ctx.executing_caller = Some(Arc::new(SessionCallerReference::from(caller)));
    ctx.executing_supervisor =
        ExecutingSupervisorObservation::capture_from_executing_session(caller);
    ctx
}

fn result(outcome: ToolOutcome) -> Value {
    let ToolOutcome::Completed(value) = outcome else {
        panic!("completed read")
    };
    assert!(value.success, "{}", value.result);
    serde_json::from_str(&value.result).unwrap()
}

impl Fixture {
    async fn new() -> Self {
        let home = tempfile::tempdir().unwrap();
        let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
        let bootstrap = store
            .get_or_create_default_supervisor("model")
            .await
            .unwrap();
        let reference: SupervisorReference = (&bootstrap).into();
        let mut targets = Vec::new();
        for (id, project) in [("existing-a", "project-a"), ("existing-b", "project-b")] {
            let mut target = Session::new(id, "PRIVATE-MODEL");
            target.set_project_id_meta(project);
            target.set_workspace_path_meta(format!("/private/{id}"));
            target.add_message(Message::system("PRIVATE-SYSTEM"));
            target.add_message(Message::user("PRIVATE-TRANSCRIPT"));
            target.add_message(Message::assistant("PRIVATE-ASSISTANT", None));
            target.set_last_run_status("running");
            target.task_list = Some(TaskList {
                session_id: target.id.clone(),
                title: "Task list detail".into(),
                items: vec![TaskItem {
                    id: "task-preview-id".into(),
                    description: "Task description detail".into(),
                    status: TaskItemStatus::Blocked,
                    notes: "PRIVATE-TASK-NOTES".into(),
                    ..Default::default()
                }],
                created_at: target.created_at,
                updated_at: target.updated_at,
            });
            store.save_session(&target).await.unwrap();
            targets.push(target);
        }
        store
            .mutate_supervisor_management(&SupervisorManagementRequest {
                supervisor: reference.clone(),
                expected_state_revision: 0,
                mutation: Mutation::ConfigureProjectScope {
                    allowed_projects: ["project-a".parse().unwrap(), "project-b".parse().unwrap()]
                        .into(),
                },
            })
            .await
            .unwrap();
        for (index, target) in targets.iter().enumerate() {
            store
                .mutate_supervisor_management(&SupervisorManagementRequest {
                    supervisor: reference.clone(),
                    expected_state_revision: index as u64 + 1,
                    mutation: Mutation::Attach {
                        target_session_id: target.id.clone(),
                    },
                })
                .await
                .unwrap();
        }
        let supervisor = store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        Self {
            home,
            store,
            supervisor,
            reference,
            targets,
        }
    }

    fn tool(&self) -> SessionInspectorTool {
        SessionInspectorTool::host_routed(self.store.clone(), self.store.clone())
    }

    async fn export(&self, caller: &Session, index: usize) -> Value {
        result(
            self.tool()
                .invoke(
                    json!({"action":"export_context", "session_id":self.targets[index].id}),
                    ctx(caller),
                )
                .await
                .unwrap(),
        )
    }

    async fn read(&self, caller: &Session, index: usize, receipt: &Value) -> Value {
        result(
            self.tool()
                .invoke(request(&self.targets[index], receipt), ctx(caller))
                .await
                .unwrap(),
        )
    }

    fn directory(&self, caller: &Session, receipt: &Value) -> PathBuf {
        self.home
            .path()
            .join("coordination/session-context/v1")
            .join(hex::encode(Sha256::digest(caller.id.as_bytes())))
            .join(receipt["revision"].as_str().unwrap())
    }

    async fn install(
        &self,
        caller: &Session,
        ancestors: Vec<ActorAncestorObservation>,
        revision: u64,
    ) {
        self.install_rights(caller, ancestors, revision, &[Right::ReadContextView])
            .await;
    }

    async fn install_rights(
        &self,
        caller: &Session,
        ancestors: Vec<ActorAncestorObservation>,
        revision: u64,
        rights: &[Right],
    ) {
        self.store
            .install_host_session_scope(
                revision,
                HostSessionScopeBinding {
                    schema_version: HOST_SESSION_SCOPE_VERSION,
                    grant_id: uuid::Uuid::new_v4(),
                    revision: revision + 1,
                    caller: HostScopeSessionObservation::capture(caller, ancestors).unwrap(),
                    targets: self
                        .targets
                        .iter()
                        .map(|target| {
                            (
                                target.id.clone(),
                                HostSessionTargetScope {
                                    observation: HostScopeSessionObservation::capture(
                                        target,
                                        vec![],
                                    )
                                    .unwrap(),
                                    rights: rights.iter().copied().collect(),
                                },
                            )
                        })
                        .collect(),
                    creates: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
    }
}

fn request(target: &Session, receipt: &Value) -> Value {
    json!({"action":"read_context", "session_id":target.id, "revision":receipt["revision"],
        "file":"status", "offset":0, "limit":2})
}

#[tokio::test]
async fn two_project_supervisor_views_are_bounded_immutable_and_survive_reopen() {
    let f = Fixture::new().await;
    for (index, target) in f.targets.iter().enumerate() {
        let before = serde_json::to_value(f.store.load_session(&target.id).await.unwrap()).unwrap();
        let receipt = f.export(&f.supervisor, index).await;
        assert_eq!(receipt["schema_version"], "session-context-view.scoped.v1");
        assert_eq!(
            receipt["scope"]["project_id"],
            target.project_id_meta().unwrap()
        );
        assert!(receipt.get("status_path").is_none());
        let bundle = f.directory(&f.supervisor, &receipt);
        let status = std::fs::read_to_string(bundle.join("status.md")).unwrap();
        let source_lines: Vec<_> = status.lines().collect();
        assert!(
            source_lines.len() > 4,
            "fixture needs two bounded continuations"
        );
        assert_eq!(
            source_lines[1], "",
            "keep the empty source line in this regression"
        );
        let view = f.read(&f.supervisor, index, &receipt).await;
        // A final empty selected line becomes a trailing newline after join;
        // String::lines() on that result does not count the empty selected line.
        assert_eq!(view["content"], source_lines[..2].join("\n"));
        assert_eq!(view["offset"], 0);
        assert_eq!(view["total_lines"], source_lines.len());
        assert_eq!(view["next_offset"], 2);
        let mut header = request(target, &receipt);
        header["limit"] = json!(40);
        let header = result(f.tool().invoke(header, ctx(&f.supervisor)).await.unwrap());
        let content = header["content"].as_str().unwrap();
        assert_eq!(receipt["scope"]["caller_session_id"], f.supervisor.id);
        assert_eq!(
            receipt["scope"]["root_session_id"],
            f.supervisor.root_session_id
        );
        assert_eq!(
            content
                .lines()
                .find_map(|line| line.strip_prefix("- Root: ")),
            Some(target.root_session_id.as_str()),
            "status Root describes the target, not caller provenance"
        );
        assert!(content.contains("- Schema: session-context-view.scoped.v1"));
        assert!(content.contains(&format!(
            "- Source digest: {}",
            receipt["source_digest"].as_str().unwrap()
        )));
        assert!(!content.contains("- Revision:"));
        assert_ne!(receipt["revision"], receipt["source_digest"]);
        let mut next = request(target, &receipt);
        next["offset"] = json!(2);
        let later = result(
            f.tool()
                .invoke(next.clone(), ctx(&f.supervisor))
                .await
                .unwrap(),
        );
        assert_eq!(later["content"], source_lines[2..4].join("\n"));
        assert_eq!(later["offset"], 2);
        assert_eq!(later["total_lines"], source_lines.len());
        assert_eq!(later["next_offset"], 4);
        for file in ["status.md", "brief.md", "manifest.json"] {
            let content = std::fs::read_to_string(bundle.join(file)).unwrap();
            for private in [
                "PRIVATE-SYSTEM",
                "PRIVATE-TRANSCRIPT",
                "PRIVATE-ASSISTANT",
                "PRIVATE-MODEL",
                "PRIVATE-TASK-NOTES",
                "/private/",
            ] {
                assert!(!content.contains(private), "{private} leaked in {file}");
            }
        }
        let reopened = Arc::new(SessionStoreV2::new(f.home.path().into()).await.unwrap());
        let reader = SessionInspectorTool::host_routed(reopened.clone(), reopened.clone());
        assert_eq!(
            result(reader.invoke(next, ctx(&f.supervisor)).await.unwrap()),
            later
        );
        assert_eq!(
            before,
            serde_json::to_value(f.store.load_session(&target.id).await.unwrap()).unwrap()
        );
    }
}

#[tokio::test]
async fn child_read_scope_does_not_inherit_supervisor_or_send_authority() {
    let f = Fixture::new().await;
    let child = Session::new_child_of("scoped-observer", &f.supervisor, "model", "observe only");
    f.store.save_session(&child).await.unwrap();
    let export = json!({"action":"export_context", "session_id":f.targets[0].id});
    assert!(f.tool().invoke(export.clone(), ctx(&child)).await.is_err());
    let mut impersonated = ctx(&child);
    impersonated.executing_supervisor =
        ExecutingSupervisorObservation::capture_from_executing_session(&f.supervisor);
    assert!(f.tool().invoke(export.clone(), impersonated).await.is_err());
    f.install(
        &child,
        vec![ActorAncestorObservation {
            actor_id: f.supervisor.id.clone(),
            session_created_at: f.supervisor.created_at,
            metadata_version: f.supervisor.metadata_version,
        }],
        0,
    )
    .await;
    let receipt = f.export(&child, 0).await;
    f.read(&child, 0, &receipt).await;
    for (index, target) in f.targets.iter().enumerate() {
        let child_receipt = f.export(&child, index).await;
        assert_eq!(child_receipt["scope"]["caller_session_id"], child.id);
        assert_eq!(
            child_receipt["scope"]["root_session_id"],
            child.root_session_id
        );
        let mut header = request(target, &child_receipt);
        header["limit"] = json!(40);
        let header = result(f.tool().invoke(header, ctx(&child)).await.unwrap());
        assert_eq!(
            header["content"]
                .as_str()
                .unwrap()
                .lines()
                .find_map(|line| line.strip_prefix("- Root: ")),
            Some(target.root_session_id.as_str()),
            "a Child reader cannot relabel an independent target's Root"
        );
    }
    let token = f
        .store
        .find_host_session_scope(&SessionCallerReference::from(&child))
        .await
        .unwrap()
        .unwrap();
    assert!(token
        .binding()
        .target(&f.targets[0].id, Right::SendPeer)
        .is_err());
    assert!(child.authority_identity.is_ordinary());
    assert!(f
        .tool()
        .invoke(
            json!({"action":"followup", "session_id":f.targets[0].id, "message":"unauthorized"}),
            ctx(&child)
        )
        .await
        .is_err());
    let reopened = Arc::new(SessionStoreV2::new(f.home.path().into()).await.unwrap());
    let reader = SessionInspectorTool::host_routed(reopened.clone(), reopened);
    assert!(
        reader
            .invoke(request(&f.targets[0], &receipt), ctx(&child))
            .await
            .is_err(),
        "ordinary observer needs explicit Host restoration after restart"
    );
}

#[tokio::test]
async fn supervisor_default_views_survive_legacy_acl_changes_but_not_another_incarnation() {
    let f = Fixture::new().await;
    let receipt = f.export(&f.supervisor, 0).await;
    f.store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: f.reference.clone(),
            expected_state_revision: 3,
            mutation: Mutation::Detach {
                target_session_id: f.targets[0].id.clone(),
            },
        })
        .await
        .unwrap();
    assert!(f
        .tool()
        .invoke(request(&f.targets[0], &receipt), ctx(&f.supervisor))
        .await
        .is_ok());
    f.store
        .mutate_supervisor_management(&SupervisorManagementRequest {
            supervisor: f.reference.clone(),
            expected_state_revision: 4,
            mutation: Mutation::Attach {
                target_session_id: f.targets[0].id.clone(),
            },
        })
        .await
        .unwrap();
    assert!(f
        .tool()
        .invoke(request(&f.targets[0], &receipt), ctx(&f.supervisor))
        .await
        .is_ok());
    let current = f.export(&f.supervisor, 0).await;
    f.read(&f.supervisor, 0, &current).await;
    f.install(&f.supervisor, vec![], 0).await;
    let host = f.export(&f.supervisor, 0).await;
    f.store
        .revoke_host_session_scope(&f.supervisor.id, 1)
        .await
        .unwrap();
    assert!(f
        .tool()
        .invoke(request(&f.targets[0], &host), ctx(&f.supervisor))
        .await
        .is_ok());
    assert!(
        f.tool()
            .invoke(
                json!({"action":"export_context", "session_id":f.targets[0].id}),
                ctx(&f.supervisor)
            )
            .await
            .is_ok(),
        "legacy Host revocation cannot remove canonical Supervisor role access"
    );
    f.store.delete_session(&f.supervisor.id).await.unwrap();
    f.store
        .get_or_create_default_supervisor("replacement")
        .await
        .unwrap();
    let replacement = f
        .store
        .load_session(&f.supervisor.id)
        .await
        .unwrap()
        .unwrap();
    for caller in [&f.supervisor, &replacement] {
        assert!(
            f.tool()
                .invoke(request(&f.targets[0], &receipt), ctx(caller))
                .await
                .is_err(),
            "old view selectors cannot cross a canonical caller lifetime/incarnation"
        );
    }
}

#[tokio::test]
async fn corrupted_content_and_rewritten_manifest_cannot_keep_a_revision() {
    let f = Fixture::new().await;
    let receipt = f.export(&f.supervisor, 0).await;
    let directory = f.directory(&f.supervisor, &receipt);
    let path = directory.join("status.md");
    let bytes = std::fs::read(&path).unwrap();
    let replacement = b"FORGED OBSERVATION\n";
    std::fs::write(&path, replacement).unwrap();
    assert!(f
        .tool()
        .invoke(request(&f.targets[0], &receipt), ctx(&f.supervisor))
        .await
        .is_err());
    let manifest = directory.join("manifest.json");
    let mut value: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    value["files"]["status"] = json!({"bytes":replacement.len(), "lines":1,
        "sha256":hex::encode(Sha256::digest(replacement))});
    std::fs::write(&manifest, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    assert!(
        f.tool()
            .invoke(request(&f.targets[0], &receipt), ctx(&f.supervisor))
            .await
            .is_err(),
        "selector commits to scope and file hashes"
    );
    std::fs::write(path, bytes).unwrap();
}

#[tokio::test]
async fn retained_reader_rejects_symlinks_missing_files_and_oversized_actual_reads() {
    let f = Fixture::new().await;
    let receipt = f.export(&f.supervisor, 0).await;
    let directory = f.directory(&f.supervisor, &receipt);
    let path = directory.join("brief.md");
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert!(f
        .tool()
        .invoke(request(&f.targets[0], &receipt), ctx(&f.supervisor))
        .await
        .is_err());
    let external = f.home.path().join("private-target");
    std::fs::write(&external, "PRIVATE-SYMLINK").unwrap();
    std::os::unix::fs::symlink(&external, &path).unwrap();
    assert!(f
        .tool()
        .invoke(request(&f.targets[0], &receipt), ctx(&f.supervisor))
        .await
        .is_err());
    std::fs::remove_file(&path).unwrap();
    std::fs::write(&path, vec![b'x'; 16 * 1024 + 1]).unwrap();
    assert!(f
        .tool()
        .invoke(request(&f.targets[0], &receipt), ctx(&f.supervisor))
        .await
        .is_err());
    std::fs::write(&path, bytes).unwrap();
    f.read(&f.supervisor, 0, &receipt).await;
}

#[tokio::test]
async fn arguments_and_original_caller_generation_are_not_grants() {
    let f = Fixture::new().await;
    let receipt = f.export(&f.supervisor, 0).await;
    for (key, value) in [
        ("revision", json!("../escape")),
        ("file", json!("manifest")),
        ("offset", json!(121)),
        ("limit", json!(41)),
        ("path", json!("/tmp/secret")),
        ("caller_session_id", json!(f.supervisor.id)),
        ("session_id", json!(f.targets[1].id)),
    ] {
        let mut args = request(&f.targets[0], &receipt);
        args[key] = value;
        assert!(
            f.tool().invoke(args, ctx(&f.supervisor)).await.is_err(),
            "{key}"
        );
    }
    let mut stale = ctx(&f.supervisor);
    let mut caller = SessionCallerReference::from(&f.supervisor);
    caller.created_at += chrono::Duration::seconds(1);
    stale.executing_caller = Some(Arc::new(caller));
    assert!(f
        .tool()
        .invoke(request(&f.targets[0], &receipt), stale)
        .await
        .is_err());
}

#[tokio::test]
async fn source_updates_keep_old_versions_readable_under_the_same_exact_link() {
    let f = Fixture::new().await;
    let old = f.export(&f.supervisor, 0).await;
    let old_bytes = std::fs::read(f.directory(&f.supervisor, &old).join("status.md")).unwrap();
    let mut changed = f
        .store
        .load_session(&f.targets[0].id)
        .await
        .unwrap()
        .unwrap();
    changed.title = "Updated public observation".into();
    f.store.save_session(&changed).await.unwrap();
    let current = f.export(&f.supervisor, 0).await;
    assert_ne!(old["revision"], current["revision"]);
    f.read(&f.supervisor, 0, &old).await;
    assert_eq!(
        std::fs::read(f.directory(&f.supervisor, &old).join("status.md")).unwrap(),
        old_bytes
    );
}

#[tokio::test]
async fn target_replacement_and_missing_authority_never_read_old_views() {
    for damaged in [false, true] {
        let f = Fixture::new().await;
        let receipt = f.export(&f.supervisor, 0).await;
        if damaged {
            let relative = f.store.resolve_rel_path(&f.targets[0].id).await.unwrap();
            std::fs::remove_file(f.home.path().join(relative).join("runtime.json")).unwrap();
        } else {
            f.store.delete_session(&f.targets[0].id).await.unwrap();
            f.store
                .recreate_root_session(&f.targets[0].id, "replacement")
                .await
                .unwrap();
        }
        assert!(f
            .tool()
            .invoke(request(&f.targets[0], &receipt), ctx(&f.supervisor))
            .await
            .is_err());
    }
}

#[tokio::test]
async fn selectors_cannot_transfer_between_callers_or_host_grant_epochs() {
    let f = Fixture::new().await;
    let mut reader = Session::new("separate-reader", "model");
    reader.set_project_id_meta("reader-project");
    f.store.save_session(&reader).await.unwrap();
    f.install(&reader, vec![], 0).await;
    let own = f.export(&reader, 0).await;
    f.read(&reader, 0, &own).await;
    let supervisor = f.export(&f.supervisor, 0).await;
    assert!(f
        .tool()
        .invoke(request(&f.targets[0], &supervisor), ctx(&reader))
        .await
        .is_err());
    f.install(&reader, vec![], 1).await;
    assert!(f
        .tool()
        .invoke(request(&f.targets[0], &own), ctx(&reader))
        .await
        .is_err());
    let current = f.export(&reader, 0).await;
    f.read(&reader, 0, &current).await;
}

#[tokio::test]
async fn ordinary_root_self_export_preserves_the_existing_host_routed_contract() {
    let f = Fixture::new().await;
    let caller = &f.targets[0];
    let receipt = result(
        f.tool()
            .invoke(
                json!({"action":"export_context", "session_id":caller.id}),
                ctx(caller),
            )
            .await
            .unwrap(),
    );
    assert_eq!(receipt["schema_version"], "session-context-view.v1");
    assert!(receipt["status_path"].as_str().is_some());
    assert!(f
        .tool()
        .invoke(
            json!({"action":"export_context", "session_id":f.targets[1].id}),
            ctx(caller)
        )
        .await
        .is_err());
}

#[tokio::test]
async fn ordinary_metadata_rights_stay_narrow_while_supervisor_role_can_read_bounded_context() {
    let f = Fixture::new().await;
    let mut reader = Session::new("ordinary-metadata-reader", "model");
    reader.set_project_id_meta("reader-project");
    f.store.save_session(&reader).await.unwrap();
    f.install(&reader, vec![], 0).await;
    let receipt = f.export(&reader, 0).await;
    let mut brief = request(&f.targets[0], &receipt);
    brief["file"] = json!("brief");
    brief["limit"] = json!(40);
    let admitted = result(f.tool().invoke(brief.clone(), ctx(&reader)).await.unwrap());
    assert!(admitted["content"]
        .as_str()
        .unwrap()
        .contains("Task description detail"));
    let default_view = f.export(&f.supervisor, 0).await;
    let mut default_brief = request(&f.targets[0], &default_view);
    default_brief["file"] = json!("brief");
    let current = result(
        f.tool()
            .invoke(default_brief, ctx(&f.supervisor))
            .await
            .unwrap(),
    );
    assert!(current["content"]
        .as_str()
        .unwrap()
        .contains("Task description detail"));
    for (revision, right) in [(1, Right::InspectBoundedProgress), (2, Right::ListMetadata)] {
        f.install_rights(&reader, vec![], revision, &[right]).await;
        let denied = f
            .tool()
            .invoke(brief.clone(), ctx(&reader))
            .await
            .unwrap_err();
        assert!(
            denied.to_string().contains("host_session_scope_denied"),
            "the new right is checked before selecting old files: {denied}"
        );
        let denied = f
            .tool()
            .invoke(
                json!({"action":"export_context", "session_id":f.targets[0].id}),
                ctx(&reader),
            )
            .await
            .unwrap_err();
        assert!(
            denied.to_string().contains("host_session_scope_denied"),
            "ordinary metadata grants cannot borrow Supervisor context rights: {denied}"
        );
        if right == Right::InspectBoundedProgress {
            let old = result(
                f.tool()
                    .invoke(
                        json!({"action":"inspect", "session_id":f.targets[0].id}),
                        ctx(&reader),
                    )
                    .await
                    .unwrap(),
            );
            assert_eq!(old["task_counts"]["blocked"], 1);
            assert!(!old.to_string().contains("Task description detail"));
            assert!(!old.to_string().contains("Task list detail"));
            assert!(!old.to_string().contains("task-preview-id"));
        }
    }
}

#[tokio::test]
async fn context_right_grants_neither_progress_history_listing_nor_actual_inbox_send() {
    use bamboo_domain::{
        SessionInboxPort, SessionMessageBody, SessionMessageContent, SessionMessageDeliveryIntent,
        SessionMessageEnvelope, SessionMessageKind, SessionMessageSource,
    };
    let f = Fixture::new().await;
    let mut caller = Session::new("view-only-reader", "model");
    caller.set_project_id_meta("reader-project");
    f.store.save_session(&caller).await.unwrap();
    f.install(&caller, vec![], 0).await;
    let receipt = f.export(&caller, 0).await;
    f.read(&caller, 0, &receipt).await;
    assert_eq!(
        serde_json::to_value(Right::ReadContextView).unwrap(),
        json!("read_context_view")
    );
    for action in [
        "inspect",
        "read_messages",
        "get_meta",
        "read_compressed_cache",
    ] {
        assert!(
            f.tool()
                .invoke(
                    json!({"action":action,"session_id":f.targets[0].id}),
                    ctx(&caller)
                )
                .await
                .is_err(),
            "{action}"
        );
    }
    let page = result(
        f.tool()
            .invoke(json!({"action":"list"}), ctx(&caller))
            .await
            .unwrap(),
    );
    assert!(page["sessions"].as_array().unwrap().is_empty());
    let original = SessionCallerReference::from(&caller);
    let token = f
        .store
        .find_host_session_scope(&original)
        .await
        .unwrap()
        .unwrap();
    assert!(f
        .store
        .load_host_scoped_target(&token, &original, &f.targets[0].id)
        .await
        .is_err());
    let operation = "view-right-cannot-send";
    let id = bamboo_domain::session_peer_message_id(
        &caller.id,
        caller.created_at,
        &f.targets[0].id,
        f.targets[0].created_at,
        operation,
    );
    let envelope = SessionMessageEnvelope {
        thread_id: Some(id.to_string()),
        id,
        source: SessionMessageSource::Session {
            session_id: caller.id.clone(),
        },
        target_session_id: f.targets[0].id.clone(),
        kind: SessionMessageKind::PeerMessage,
        body: SessionMessageBody::Content(SessionMessageContent::text("must not be sent")),
        created_at: chrono::Utc::now(),
        in_reply_to: None,
        attempt: None,
        correlation_id: Some(operation.into()),
    };
    let inbox = bamboo_storage::FileSessionInbox::new(f.store.clone(), Default::default());
    let before = inbox.inspect(&f.targets[0].id).await.unwrap();
    let denied = inbox
        .deliver_host_scoped_peer_message(
            &token,
            &original,
            &envelope,
            SessionMessageDeliveryIntent::QueueOnly,
        )
        .await
        .unwrap_err();
    assert!(
        denied.to_string().contains("host_session_scope_denied"),
        "{denied}"
    );
    assert_eq!(inbox.inspect(&f.targets[0].id).await.unwrap(), before);
}

#[tokio::test]
async fn upgrade_reuses_the_original_legacy_manifest_byte_encoding() {
    let f = Fixture::new().await;
    let caller = &f.targets[0];
    let args = json!({"action":"export_context", "session_id":caller.id});
    let receipt = result(f.tool().invoke(args.clone(), ctx(caller)).await.unwrap());
    let path = receipt["manifest_path"].as_str().unwrap();
    let produced: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    // This is the pre-#2295 encoder's exact construction order. The locked
    // serde_json graph enables preserve_order, and immutable reuse compares bytes.
    let old_encoding = json!({
        "schema_version":produced["schema_version"], "revision":produced["revision"],
        "source_digest":produced["source_digest"], "scope":produced["scope"],
        "source_observed_at":produced["source_observed_at"],
        "observation":produced["observation"], "files":produced["files"],
        "filenames":produced["filenames"],
    });
    let bytes = serde_json::to_vec_pretty(&old_encoding).unwrap();
    std::fs::write(path, &bytes).unwrap();
    let reused = result(f.tool().invoke(args, ctx(caller)).await.unwrap());
    assert_eq!(reused["revision"], receipt["revision"]);
    assert_eq!(reused["reused"], true);
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}
