//! The unchanged production initializer must use the fenced migration entrypoint.
use bamboo_domain::{
    ActorActivationClaim, ActorDirectoryPort, ConversationSummary, Session, Storage,
};
use bamboo_storage::SessionStoreV2;
use chrono::{Duration, Utc};

#[tokio::test]
async fn init_storage_does_not_reconstruct_activated_child_runtime_and_stays_nonfatal() {
    let home = tempfile::tempdir().unwrap();
    let original = SessionStoreV2::new(home.path().into()).await.unwrap();
    let root = Session::new("startup-root", "model");
    original.save_session(&root).await.unwrap();
    let mut child = Session::new_child_of("startup-child", &root, "model", "child");
    child.conversation_summary = Some(ConversationSummary::new("old main summary", 1, 10));
    original.save_session(&child).await.unwrap();
    let now = Utc::now();
    let activation = original
        .claim_activation(&ActorActivationClaim {
            actor_id: child.id.clone(),
            run_id: "run".into(),
            lease_owner: "owner".into(),
            lease_expires_at: now + Duration::minutes(5),
            inbox_generation: 0,
            placement_ref: None,
            now,
        })
        .await
        .unwrap();
    let directory = home
        .path()
        .join("sessions/startup-root/children/startup-child");
    std::fs::remove_file(directory.join("runtime.json")).unwrap();
    let files = [
        "session.json",
        "actor-authority.json",
        "actor-authority.initialized.json",
    ];
    let before = files.map(|file| std::fs::read(directory.join(file)).unwrap());
    drop(original);
    let (store, _) = bamboo_server::app_state::init::init_storage(home.path())
        .await
        .unwrap();
    assert!(!directory.join("runtime.json").exists());
    assert!(!home.path().join(".runtime_sidecar_migrated").exists());
    assert_eq!(
        files.map(|file| std::fs::read(directory.join(file)).unwrap()),
        before
    );
    store
        .validate_fence(&activation.fence(), now)
        .await
        .unwrap();
    // Existing Child history reads may still fall back to main. This writer
    // does not turn runtime absence into a global read-path rejection.
    assert!(store.load_session(&child.id).await.unwrap().is_some());
    tokio::task::yield_now().await;
    store.flush_search_index().await;
    assert!(!directory.join("runtime.json").exists());
}
