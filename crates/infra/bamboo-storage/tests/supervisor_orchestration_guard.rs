use bamboo_domain::{Storage, SupervisorReference, DEFAULT_SUPERVISOR_SESSION_ID};
use bamboo_storage::SessionStoreV2;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};

#[tokio::test]
async fn transient_supervisor_publication_requires_current_mode_revision_and_incarnation() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let receipt = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let reference: SupervisorReference = (&receipt).into();
    let mut supervisor = store
        .load_session(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    let effects = Arc::new(AtomicUsize::new(0));
    for (ultra, stale_revision, wrong_incarnation) in [
        (false, false, false),
        (true, true, false),
        (true, false, true),
        (true, false, false),
    ] {
        supervisor.set_root_orchestration_only(ultra).unwrap();
        store.save_session(&supervisor).await.unwrap();
        let mut observed = reference.clone();
        if wrong_incarnation {
            observed.incarnation_id = uuid::Uuid::new_v4();
        }
        let revision = supervisor.root_tool_authority_revision + u64::from(stale_revision);
        let effect = effects.clone();
        let result = store
            .with_supervisor_orchestration_authority(
                &observed,
                revision,
                Box::new(move || {
                    effect.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            )
            .await;
        assert_eq!(
            result.is_ok(),
            ultra && !stale_revision && !wrong_incarnation
        );
    }
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}

struct Release(Arc<(Mutex<bool>, Condvar)>);
impl Drop for Release {
    fn drop(&mut self) {
        let (latch, wake) = &*self.0;
        *latch.lock().unwrap() = true;
        wake.notify_all();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_initializer_retains_physical_authority_until_its_job_finishes() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let receipt = store
        .get_or_create_default_supervisor("model")
        .await
        .unwrap();
    let reference: SupervisorReference = (&receipt).into();
    let mut supervisor = store
        .load_session(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap();
    supervisor.set_root_orchestration_only(true).unwrap();
    store.save_session(&supervisor).await.unwrap();
    let revision = supervisor.root_tool_authority_revision;
    supervisor.set_root_orchestration_only(false).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release = Release(gate.clone());
    let (entered, started) = tokio::sync::oneshot::channel();
    let writer_store = store.clone();
    let job = tokio::spawn(async move {
        writer_store
            .with_supervisor_orchestration_authority(
                &reference,
                revision,
                Box::new(move || {
                    entered.send(()).unwrap();
                    let (latch, wake) = &*gate;
                    let mut released = latch.lock().unwrap();
                    while !*released {
                        released = wake.wait(released).unwrap();
                    }
                    Ok(())
                }),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), started)
        .await
        .unwrap()
        .unwrap();
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    let mode_store = store.clone();
    let mut mode_change = tokio::spawn(async move { mode_store.save_session(&supervisor).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut mode_change)
            .await
            .is_err(),
        "cancel released authority before the physical job finished"
    );
    drop(release);
    tokio::time::timeout(std::time::Duration::from_secs(10), mode_change)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!store
        .load_root_authority(DEFAULT_SUPERVISOR_SESSION_ID)
        .await
        .unwrap()
        .unwrap()
        .root_orchestration_only_enabled());
}
