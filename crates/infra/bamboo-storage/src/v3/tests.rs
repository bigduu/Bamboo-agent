use super::*;
use bamboo_domain::{Message, SessionMessageEnvelope};
use tempfile::TempDir;

fn fixture() -> (TempDir, SessionStoreV3, Session) {
    let temp = TempDir::new().unwrap();
    let store = SessionStoreV3::open(temp.path().join("shadow.sqlite")).unwrap();
    let mut session = Session::new("root", "model");
    session.add_message(Message::user("first"));
    session.add_message(Message::assistant("answer", None));
    (temp, store, session)
}

#[test]
fn round_trip_runtime_updates_and_reopen_preserve_history_proof() {
    let (temp, store, mut session) = fixture();
    let envelope = SessionMessageEnvelope::user_input("root", "additional guidance");
    session.add_message(envelope.to_provider_message().unwrap());
    session
        .runtime_metadata
        .get_or_insert_with(Default::default)
        .session_inbox_admission
        .get_or_insert_with(Default::default)
        .record(envelope.id.clone(), 7);
    let revision = store.put(&session, None).unwrap();
    assert!(store.verify_shadow(&session).unwrap());
    let mut runtime = store.load_runtime("root").unwrap().unwrap();
    assert!(runtime.session.messages.is_empty());
    assert!(runtime
        .session
        .runtime_metadata
        .as_ref()
        .unwrap()
        .session_inbox_admission
        .is_none());
    runtime.session.title = "updated title".into();
    let updated = store
        .put_runtime(&runtime.session, revision.runtime)
        .unwrap();
    assert_eq!(updated.history, revision.history);
    assert_eq!(updated.runtime, revision.runtime + 1);
    drop(store);
    let reopened = SessionStoreV3::open(temp.path().join("shadow.sqlite")).unwrap();
    let snapshot = reopened.load("root").unwrap().unwrap();
    assert_eq!(snapshot.session.messages.len(), 3);
    assert_eq!(snapshot.session.title, "updated title");
    assert!(snapshot
        .session
        .runtime_metadata
        .unwrap()
        .session_inbox_admission
        .unwrap()
        .contains(&envelope.id));
}

#[test]
fn edits_touch_only_changed_rows_and_stale_writers_fail() {
    let (_temp, store, mut session) = fixture();
    let first = store.put(&session, None).unwrap();
    assert_eq!(store.put(&session, Some(first)).unwrap(), first);
    let before = store.connection().unwrap().total_changes();
    session.messages[0].content = "edited".into();
    let second = store.put(&session, Some(first)).unwrap();
    let after = store.connection().unwrap().total_changes();
    assert_eq!(after - before, 2, "one header and one message row");
    assert_eq!(second.history, first.history + 1);
    assert_eq!(second.runtime, first.runtime);
    assert!(matches!(
        store.put(&session, Some(first)),
        Err(V3Error::Conflict)
    ));
    assert!(matches!(
        store.message_page("root", 0, 1, first.history),
        Err(V3Error::Conflict)
    ));
    assert_eq!(
        store.message_page("root", 1, 1, second.history).unwrap()[0].content,
        "answer"
    );
    session.messages.pop();
    let third = store.put(&session, Some(second)).unwrap();
    assert_eq!(
        store
            .message_page("root", 0, 10, third.history)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn independent_connections_have_one_cas_winner() {
    let (temp, first, session) = fixture();
    let revision = first.put(&session, None).unwrap();
    let second = SessionStoreV3::open(temp.path().join("shadow.sqlite")).unwrap();
    let session = first.load_runtime(&session.id).unwrap().unwrap().session;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let handles = [first, second]
        .into_iter()
        .enumerate()
        .map(|(index, store)| {
            let barrier = barrier.clone();
            let mut session = session.clone();
            std::thread::spawn(move || {
                session.title = format!("writer {index}");
                barrier.wait();
                store.put_runtime(&session, revision.runtime)
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(V3Error::Conflict)))
            .count(),
        1
    );
}

#[test]
fn import_is_atomic_and_validates_the_whole_tree() {
    let (_temp, store, root) = fixture();
    let mut child = Session::new("child", "model");
    child.kind = bamboo_domain::SessionKind::Child;
    child.parent_session_id = Some(root.id.clone());
    child.root_session_id = root.id.clone();
    store.put(&root, None).unwrap();
    assert!(matches!(
        store.import_tree_shadow(&[child.clone(), root.clone()]),
        Err(V3Error::Conflict)
    ));
    assert!(
        store.load("child").unwrap().is_none(),
        "first insert rolls back when root conflicts"
    );
    child.parent_session_id = Some(child.id.clone());
    assert!(matches!(
        store.import_tree_shadow(&[child, root]),
        Err(V3Error::Invalid(_))
    ));
}

#[test]
fn project_and_creation_identity_cannot_be_replaced_by_runtime_cas() {
    let (_temp, store, mut root) = fixture();
    let first = store.put(&root, None).unwrap();
    root = store.load_runtime(&root.id).unwrap().unwrap().session;
    root.set_project_id_meta("project-b");
    assert!(matches!(
        store.put_runtime(&root, first.runtime),
        Err(V3Error::Conflict)
    ));
    root.metadata_version += 1;
    let second = store.put_runtime(&root, first.runtime).unwrap();
    root.created_at += chrono::Duration::seconds(1);
    assert!(matches!(
        store.put_runtime(&root, second.runtime),
        Err(V3Error::Conflict)
    ));
}

#[test]
fn unrelated_databases_and_duplicate_message_ids_are_rejected() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("other.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE unrelated (value TEXT)")
        .unwrap();
    assert!(matches!(
        SessionStoreV3::open(&path),
        Err(V3Error::UnsupportedDatabase)
    ));
    let (_temp, store, mut session) = fixture();
    session.messages[1].id = session.messages[0].id.clone();
    assert!(matches!(
        store.put(&session, None),
        Err(V3Error::Invalid(_))
    ));
    assert!(store.load("root").unwrap().is_none());
}

#[tokio::test]
async fn v2_captured_tree_import_matches_every_session_and_preserves_source() {
    use bamboo_domain::Storage;
    let home = TempDir::new().unwrap();
    let v2 = crate::SessionStoreV2::new(home.path().join("v2"))
        .await
        .unwrap();
    let mut root = Session::new("tree", "model");
    root.add_message(Message::user("root history"));
    let mut child = Session::new("tree-child", "model");
    child.kind = bamboo_domain::SessionKind::Child;
    child.parent_session_id = Some(root.id.clone());
    child.root_session_id = root.id.clone();
    child.add_message(Message::assistant("child history", None));
    v2.save_session(&root).await.unwrap();
    v2.save_session(&child).await.unwrap();
    let source = vec![
        v2.load_session(&child.id).await.unwrap().unwrap(),
        v2.load_session(&root.id).await.unwrap().unwrap(),
    ];
    let v3 = SessionStoreV3::open(home.path().join("v3.sqlite")).unwrap();
    v3.import_tree_shadow(&source).unwrap();
    for snapshot in &source {
        assert!(v3.verify_shadow(snapshot).unwrap());
        assert_eq!(
            serde_json::to_value(v2.load_session(&snapshot.id).await.unwrap().unwrap()).unwrap(),
            serde_json::to_value(snapshot).unwrap()
        );
    }
    v2.flush_search_index().await;
}
