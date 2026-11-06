use super::init_account_event_sink;
use bamboo_agent_core::AgentEvent;
use bamboo_engine::events::journal;
use fs2::FileExt;
use std::fs::OpenOptions;
use std::time::Duration;

// The OS thread's finite fallback also bounds the synchronous, broken path:
// a timeout on this current-thread runtime cannot wake while flock blocks it.
#[tokio::test(flavor = "current_thread")]
async fn account_journal_startup_yields_to_runtime_and_preserves_durable_events() {
    let dir = tempfile::tempdir().unwrap();
    let events_dir = dir.path().join("events");
    let first = init_account_event_sink(events_dir.clone()).await.unwrap();
    let original = AgentEvent::ConfigChanged {
        section: "core".to_string(),
        revision: 7,
    };
    assert!(first.record_confirmed(None, &original).await);
    assert_eq!(first.latest_seq(), 1);

    let lock_path = events_dir.join(".account-journal.lock");
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .unwrap();
        lock.lock_exclusive().unwrap();
        held_tx.send(()).unwrap();
        let runtime_released_lock = release_rx.recv_timeout(Duration::from_secs(3)).is_ok();
        FileExt::unlock(&lock).unwrap();
        runtime_released_lock
    });
    held_rx.await.unwrap();

    let (start_tx, start_rx) = tokio::sync::oneshot::channel();
    let release = tokio::spawn(async move {
        start_rx.await.unwrap();
        // A failed send is expected on the broken path after the OS fallback.
        let _ = release_tx.send(());
    });
    start_tx.send(()).unwrap();
    let reopened = init_account_event_sink(events_dir.clone()).await.unwrap();
    release.await.unwrap();
    assert!(
        holder.join().unwrap(),
        "account journal startup blocked the runtime; only the OS watchdog released the lock"
    );

    assert_eq!(reopened.latest_seq(), 1);
    assert!(reopened.record_confirmed(None, &original).await);
    let next = AgentEvent::ConfigChanged {
        section: "core".to_string(),
        revision: 8,
    };
    assert!(reopened.record_confirmed(None, &next).await);
    let durable = journal::read_since(&events_dir, 0).unwrap();
    assert_eq!(durable.len(), 2);
    assert_eq!(
        durable.iter().map(|event| event.seq).collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(reopened.latest_seq(), 2);
    assert!(first.record_confirmed(None, &original).await);
    assert_eq!(journal::read_since(&events_dir, 0).unwrap().len(), 2);
    let later = AgentEvent::ConfigChanged {
        section: "core".to_string(),
        revision: 9,
    };
    assert!(first.record_confirmed(None, &later).await);
    let durable = journal::read_since(&events_dir, 0).unwrap();
    assert_eq!(
        durable.iter().map(|event| event.seq).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(first.latest_seq(), 3);
}

#[tokio::test]
async fn account_journal_startup_preserves_filesystem_errors() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("not-a-directory");
    std::fs::write(&file, b"preserve this file").unwrap();

    assert!(init_account_event_sink(file.clone()).await.is_err());
    assert_eq!(std::fs::read(file).unwrap(), b"preserve this file");
}
