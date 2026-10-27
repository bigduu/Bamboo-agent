//! The current-user contract uses canonical storage, not fixture ACL grants.
//! Legacy mutation below only constructs old on-disk data for migration tests.

use bamboo_domain::{
    AgentRuntimeState, Message, Session, SessionCallerReference, SessionInboxLimits,
    SessionInboxPort, SessionMessageEnvelope, SessionMessageKind, SessionMessageSource, Storage,
    SupervisorManagementMutation, SupervisorManagementRequest, SupervisorReference,
};
use bamboo_storage::{FileSessionInbox, SessionStoreV2};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

async fn fixture() -> (tempfile::TempDir, SessionStoreV2, SupervisorReference) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let reference = (&store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap())
        .into();
    (home, store, reference)
}

async fn root(store: &SessionStoreV2, id: &str, project: Option<&str>) -> Session {
    let mut session = Session::new(id, "model");
    if let Some(project) = project {
        session.set_project_id_meta(project);
    }
    session.agent_runtime_state = Some(AgentRuntimeState::new(id));
    session.add_message(Message::user(format!("synthetic history for {id}")));
    store.save_session(&session).await.unwrap();
    session
}

async fn canonical_bytes(store: &SessionStoreV2, id: &str) -> (Vec<u8>, Vec<u8>) {
    let directory = store.sessions_root_dir().join(id);
    (
        tokio::fs::read(directory.join("session.json"))
            .await
            .unwrap(),
        tokio::fs::read(directory.join("runtime.json"))
            .await
            .unwrap(),
    )
}

async fn assert_default_read(
    store: &SessionStoreV2,
    supervisor: &SupervisorReference,
    target: &Session,
) {
    let observation = store
        .inspect_supervisor_link(supervisor, &target.id)
        .await
        .unwrap();
    assert!(
        observation.authorized,
        "current-user target {} must not require configure/attach",
        target.id
    );
    let read = store
        .load_supervisor_managed_session(supervisor, &target.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.id, target.id);
    assert_eq!(read.created_at, target.created_at);
    assert_eq!(read.authority_identity, target.authority_identity);
    assert_eq!(read.messages.len(), target.messages.len());
    assert_eq!(read.messages[0].content, target.messages[0].content);
    let (observed, selector) = store
        .load_supervisor_current_user_observed_session(supervisor, &target.id)
        .await
        .unwrap();
    assert_eq!(observed.created_at, selector.created_at);
    assert_eq!(observed.metadata_version, selector.metadata_version);
    assert_eq!(observed.id, selector.session_id);
    assert_eq!(observed.kind, selector.kind);
}

#[tokio::test]
async fn current_user_two_projects_and_later_roots_need_no_manual_grants() {
    let (_home, store, reference) = fixture().await;
    let a = root(&store, "current-a", Some("project-a")).await;
    let b = root(&store, "current-b", Some("project-b")).await;
    let before_a = canonical_bytes(&store, &a.id).await;
    let before_b = canonical_bytes(&store, &b.id).await;
    let supervisor = canonical_bytes(&store, &reference.session_id).await;
    for target in [&a, &b] {
        assert_default_read(&store, &reference, target).await;
    }
    let later = root(&store, "current-later", Some("project-later")).await;
    assert_default_read(&store, &reference, &later).await;
    assert_eq!(canonical_bytes(&store, &a.id).await, before_a);
    assert_eq!(canonical_bytes(&store, &b.id).await, before_b);
    assert_eq!(
        canonical_bytes(&store, &reference.session_id).await,
        supervisor
    );
    // Directory control observations do not decode transcript content. A
    // malformed private message still fails explicit history inspection; the
    // metadata observation neither repairs it nor grants file/runner access.
    let path = store.sessions_root_dir().join(&a.id).join("session.json");
    let mut raw = tokio::fs::read_to_string(&path).await.unwrap();
    let fields: std::collections::BTreeMap<String, &serde_json::value::RawValue> =
        serde_json::from_str(&raw).unwrap();
    let history = fields["messages"].get();
    let start = history.as_ptr() as usize - raw.as_ptr() as usize;
    let end = start + history.len();
    raw.replace_range(
        start..end,
        r#"[{"invalid_private_transcript_fixture":true}]"#,
    );
    tokio::fs::write(&path, &raw).await.unwrap();
    let page = store
        .list_supervisor_current_user_targets(&reference, None, None, 20)
        .await
        .unwrap();
    assert!(page.targets.iter().any(|target| target.session_id == a.id));
    let filtered = store
        .list_supervisor_current_user_targets(&reference, Some("project-b"), None, 1)
        .await
        .unwrap();
    assert_eq!(filtered.targets.len(), 1);
    assert_eq!(filtered.targets[0].session_id, b.id);
    assert!(store
        .load_supervisor_current_user_observed_session(&reference, &a.id)
        .await
        .is_err());
    assert_eq!(tokio::fs::read_to_string(&path).await.unwrap(), raw);
}

#[tokio::test]
async fn legacy_scope_and_detached_link_are_preserved_without_restricting_default_access() {
    let (_home, store, reference) = fixture().await;
    let a = root(&store, "legacy-a", Some("project-a")).await;
    let b = root(&store, "legacy-b", Some("project-b")).await;
    for (revision, mutation) in [
        (
            0,
            SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: ["project-a".parse().unwrap()].into(),
            },
        ),
        (
            1,
            SupervisorManagementMutation::Attach {
                target_session_id: a.id.clone(),
            },
        ),
        (
            2,
            SupervisorManagementMutation::Detach {
                target_session_id: a.id.clone(),
            },
        ),
        (
            3,
            SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: Default::default(),
            },
        ),
    ] {
        store
            .mutate_supervisor_management(&SupervisorManagementRequest {
                supervisor: reference.clone(),
                expected_state_revision: revision,
                mutation,
            })
            .await
            .unwrap();
    }
    let before = canonical_bytes(&store, &reference.session_id).await;
    assert_default_read(&store, &reference, &a).await;
    assert_default_read(&store, &reference, &b).await;
    let historical = store
        .load_root_authority(&reference.session_id)
        .await
        .unwrap()
        .unwrap()
        .supervisor_management
        .unwrap();
    assert_eq!(historical.revision, 4);
    assert!(historical.allowed_projects.is_empty());
    assert!(!historical.links[&a.id].enabled);
    assert_eq!(canonical_bytes(&store, &reference.session_id).await, before);
}

#[tokio::test]
async fn current_user_no_project_root_and_child_history_need_no_fabricated_project() {
    let (_home, store, reference) = fixture().await;
    let parent = root(&store, "unbound-root", None).await;
    let mut child = Session::new_child_of("unbound-child", &parent, "model", "bounded work");
    child.agent_runtime_state = Some(AgentRuntimeState::new("child"));
    child.add_message(Message::assistant("synthetic child evidence", None));
    store.save_session(&child).await.unwrap();
    assert_default_read(&store, &reference, &parent).await;
    assert_default_read(&store, &reference, &child).await;
    assert!(store
        .load_session(&child.id)
        .await
        .unwrap()
        .unwrap()
        .project_id_meta()
        .is_none());
}

#[tokio::test]
async fn default_domain_access_does_not_trust_other_store_or_ordinary_identity() {
    let (_home, store, reference) = fixture().await;
    let (_other_home, _other, other_reference) = fixture().await;
    let target = root(&store, "local-target", Some("project-local")).await;
    let ordinary = root(&store, "ordinary", Some("project-local")).await;
    let before = canonical_bytes(&store, &target.id).await;
    for invalid in [
        other_reference,
        SupervisorReference {
            session_id: ordinary.id,
            incarnation_id: reference.incarnation_id,
        },
    ] {
        assert!(store
            .load_supervisor_managed_session(&invalid, &target.id)
            .await
            .is_err());
        assert!(store
            .inspect_supervisor_link(&invalid, &target.id)
            .await
            .is_err());
    }
    assert_eq!(canonical_bytes(&store, &target.id).await, before);
}

#[tokio::test]
async fn canonical_supervisor_publication_works_in_both_modes_without_changing_worker_permissions()
{
    let (_home, store, reference) = fixture().await;
    let worker = root(&store, "worker", Some("project-worker")).await;
    let before = canonical_bytes(&store, &worker.id).await;
    let effects = Arc::new(AtomicUsize::new(0));
    for ultra in [false, true, false, true] {
        let mut supervisor = store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap();
        supervisor.set_root_orchestration_only(ultra).unwrap();
        store.save_session(&supervisor).await.unwrap();
        let effect = effects.clone();
        // Mode changes do not revoke the fixed business role. Its epoch is
        // distinct from the original-caller raw tool counter/generation.
        let revision = 1;
        store
            .with_supervisor_orchestration_authority(
                &reference,
                revision,
                Box::new(move || {
                    effect.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            )
            .await
            .expect("trusted Supervisor business publication must not depend on Ultra");
    }
    assert_eq!(effects.load(Ordering::SeqCst), 4);
    assert_eq!(canonical_bytes(&store, &worker.id).await, before);
}

#[tokio::test]
async fn current_user_observer_keeps_only_selected_targets_and_explicit_revocation() {
    let (_home, store, reference) = fixture().await;
    let a = root(&store, "observer-a", Some("project-a")).await;
    let b = root(&store, "observer-b", Some("project-b")).await;
    let unrelated = root(&store, "observer-unselected", Some("project-c")).await;
    let parent = store
        .load_session(&reference.session_id)
        .await
        .unwrap()
        .unwrap();
    let mut child = Session::new_child_of("finite-observer", &parent, "model", "observe");
    child
        .agent_runtime_state
        .get_or_insert_with(Default::default)
        .read_only = true;
    child.advance_child_launch_generation().unwrap();
    child.set_last_run_status("pending");
    store.save_session(&child).await.unwrap();
    let operation = uuid::Uuid::new_v4();
    let targets: std::collections::BTreeSet<String> = [a.id.clone(), b.id.clone()].into();
    assert!(store
        .register_supervisor_observer(&reference, 1, operation, &child.id, targets.clone())
        .await
        .is_err());
    let registered = store
        .register_supervisor_observer(&reference, 0, operation, &child.id, targets.clone())
        .await
        .unwrap();
    assert!(registered.changed);
    let replay = store
        .register_supervisor_observer(&reference, 0, operation, &child.id, targets)
        .await
        .unwrap();
    assert!(!replay.changed);
    let caller =
        SessionCallerReference::from(&store.load_session(&child.id).await.unwrap().unwrap());
    for target in [&a, &b] {
        assert!(store
            .load_supervisor_observer_context_target(&caller, &target.id)
            .await
            .is_ok());
    }
    assert!(store
        .load_supervisor_observer_context_target(&caller, &unrelated.id)
        .await
        .is_err());
    let state = store
        .load_root_authority(&parent.id)
        .await
        .unwrap()
        .unwrap()
        .supervisor_management
        .unwrap();
    assert_eq!(state.schema_version, 4);
    assert!(
        state.links.is_empty(),
        "default access must not populate old ACL links"
    );
    assert!(state.allowed_projects.is_empty());
    assert!(state.observers[&child.id].current_user_targets);
    store
        .revoke_supervisor_observer(&reference, 1, &child.id)
        .await
        .unwrap();
    assert!(store
        .load_supervisor_observer_context_target(&caller, &a.id)
        .await
        .is_err());
}

#[tokio::test]
async fn current_user_followup_replays_after_restart_and_rejects_stale_target_lifetimes() {
    let (home, store, reference) = fixture().await;
    let target = root(&store, "followup-target", Some("project-a")).await;
    let store = Arc::new(store);
    let observed = store
        .inspect_supervisor_current_user_target(&reference, &target.id)
        .await
        .unwrap();
    let caller = SessionCallerReference::from(
        &store
            .load_session(&reference.session_id)
            .await
            .unwrap()
            .unwrap(),
    );
    let operation = uuid::Uuid::new_v4().to_string();
    let mut envelope = SessionMessageEnvelope::user_input(&target.id, "synthetic bounded followup");
    envelope.id =
        bamboo_domain::supervisor_current_user_message_id(&reference, &observed, &operation);
    envelope.source = SessionMessageSource::Session {
        session_id: reference.session_id.clone(),
    };
    envelope.kind = SessionMessageKind::PeerMessage;
    envelope.correlation_id = Some(operation);
    let inbox = FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    let first = inbox
        .deliver_supervisor_current_user_followup(&caller, &reference, &observed, &envelope)
        .await
        .unwrap();
    let restarted = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let inbox = FileSessionInbox::new(restarted.clone(), SessionInboxLimits::default());
    let replay = inbox
        .deliver_supervisor_current_user_followup(&caller, &reference, &observed, &envelope)
        .await
        .unwrap();
    assert_eq!(first.generation, replay.generation);
    assert_eq!(first.id, replay.id);
    let mut changed = restarted.load_session(&target.id).await.unwrap().unwrap();
    changed.set_project_id_meta("project-b");
    changed.metadata_version += 1;
    restarted.save_session(&changed).await.unwrap();
    assert!(inbox
        .deliver_supervisor_current_user_followup(&caller, &reference, &observed, &envelope)
        .await
        .is_err());
    assert_eq!(inbox.inspect(&target.id).await.unwrap().pending, 1);
    restarted.delete_session(&target.id).await.unwrap();
    let replacement = restarted
        .recreate_root_session(&target.id, "model")
        .await
        .unwrap();
    assert_ne!(replacement.created_at, observed.created_at);
    assert!(inbox
        .deliver_supervisor_current_user_followup(&caller, &reference, &observed, &envelope)
        .await
        .is_err());
}
