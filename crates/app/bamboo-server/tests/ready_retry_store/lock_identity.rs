//! Lock replacement must not let an old writer overwrite a consumed attempt.
#![cfg(unix)]

use super::*;
use bamboo_orchestration::ready_recovery::VersionedReadyRetry;
use std::{fs, path::Path};

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Remove,
    Replace,
    HardLink,
}

fn mutate_lock(root: &Path, mutation: Mutation) {
    let path = root.join("ready-retry-v1.lock");
    match mutation {
        Mutation::Remove => fs::remove_file(path).unwrap(),
        Mutation::Replace => {
            let replacement = root.join("replacement.lock");
            fs::write(&replacement, b"").unwrap();
            fs::rename(replacement, path).unwrap();
        }
        Mutation::HardLink => fs::hard_link(path, root.join("aliased.lock")).unwrap(),
    }
}

fn record_path(root: &Path, stable: &ReadyRetryKey) -> std::path::PathBuf {
    root.join("ready-retry-v1/entries")
        .join(content_hash(&canonical_bytes(stable).unwrap()))
        .join("state.json")
}

fn second_charge() -> ReadyRetryRecord {
    ReadyRetryRecord {
        attempts: 2,
        request: Some(ReadyRetryRequest::Intent {
            dispatch_key: "fixture/competing/2".into(),
            spec_hash: "c".repeat(64),
        }),
        ..reserved()
    }
}

struct ReleaseWriter(Option<std::sync::mpsc::Sender<()>>);

impl Drop for ReleaseWriter {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

async fn replacement_during_publication(mutation: Mutation) {
    let root = tempfile::tempdir().unwrap();
    let active = Arc::new(AtomicBool::new(false));
    let flag = active.clone();
    let (entered, receive) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, wait) = std::sync::mpsc::channel();
    let release = ReleaseWriter(Some(release));
    let wait = Mutex::new(wait);
    let store = FileReadyRetryStore::with_publication_fault(
        root.path(),
        Arc::new(move |phase| {
            if flag.load(Ordering::SeqCst) && phase == ReadyRetryFilePhase::BeforeReplace {
                entered.lock().unwrap().take().unwrap().send(()).unwrap();
                wait.lock().unwrap().recv().unwrap();
            }
            Ok(())
        }),
    );
    let stable = key("lock-identity");
    let original = store
        .compare_exchange(&stable, None, initial())
        .await
        .unwrap();
    let before = fs::read(record_path(root.path(), &stable)).unwrap();
    active.store(true, Ordering::SeqCst);
    let first_key = stable.clone();
    let writer = tokio::spawn(async move {
        store
            .compare_exchange(&first_key, Some(original.revision), reserved())
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), receive)
        .await
        .unwrap()
        .unwrap();
    mutate_lock(root.path(), mutation);
    let successor = FileReadyRetryStore::new(root.path());
    let competing = tokio::time::timeout(
        Duration::from_secs(5),
        successor.compare_exchange(&stable, Some(1), second_charge()),
    )
    .await;
    drop(release);
    let stale = tokio::time::timeout(Duration::from_secs(5), writer)
        .await
        .unwrap()
        .unwrap();
    let competing = competing.expect("a contender must finish without waiting for the held writer");
    let persisted = FileReadyRetryStore::new(root.path()).load(&stable).await;
    assert!(
        stale.is_err(),
        "{mutation:?}: retired lock writer reported {stale:?}; contender={competing:?}; persisted={persisted:?}"
    );
    match competing {
        Ok(charged) => {
            assert_eq!(charged.record, second_charge());
            assert_eq!(persisted.unwrap(), Some(charged));
        }
        Err(_) => {
            assert_eq!(fs::read(record_path(root.path(), &stable)).unwrap(), before);
            if let Ok(value) = persisted {
                assert_eq!(
                    value,
                    Some(VersionedReadyRetry {
                        revision: 1,
                        record: initial()
                    })
                );
            }
        }
    }
}

#[tokio::test]
async fn removed_lock_cannot_let_the_retired_writer_overwrite_a_competing_charge() {
    replacement_during_publication(Mutation::Remove).await;
}

#[tokio::test]
async fn replaced_lock_cannot_let_the_retired_writer_overwrite_a_competing_charge() {
    replacement_during_publication(Mutation::Replace).await;
}

#[tokio::test]
async fn cloned_store_cannot_reopen_a_changed_or_aliased_lock_between_transactions() {
    for mutation in [Mutation::Remove, Mutation::Replace, Mutation::HardLink] {
        let root = tempfile::tempdir().unwrap();
        let store = FileReadyRetryStore::new(root.path());
        let stable = key("cached-lock");
        store
            .compare_exchange(&stable, None, reserved())
            .await
            .unwrap();
        let before = fs::read(record_path(root.path(), &stable)).unwrap();
        let retained = store.clone();
        drop(store);
        mutate_lock(root.path(), mutation);
        let read = retained.load(&stable).await;
        let exchange = retained
            .compare_exchange(&stable, Some(1), second_charge())
            .await;
        assert!(
            read.is_err() && exchange.is_err(),
            "{mutation:?}: load={read:?}, CAS={exchange:?}"
        );
        assert_eq!(fs::read(record_path(root.path(), &stable)).unwrap(), before);
    }
}
