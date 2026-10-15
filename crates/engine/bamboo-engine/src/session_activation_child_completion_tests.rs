use super::*;

#[tokio::test]
async fn child_control_fence_survives_placeholder_adoption_and_resets_with_owner() {
    let router = SessionActivationRouter::new();
    router
        .install_owner_placeholder_for_test("parent", "run-a")
        .await;
    assert_eq!(
        router
            .child_completion_successor_run("parent", false, None)
            .await,
        None
    );
    assert_eq!(
        router
            .child_completion_successor_run("parent", true, None)
            .await
            .as_deref(),
        Some("run-a")
    );
    let mut owner = router.register_run("parent", "run-a").await.unwrap();
    assert_eq!(
        router
            .child_completion_successor_run("parent", false, None)
            .await
            .as_deref(),
        Some("run-a")
    );
    owner.begin_finalization().await;
    assert!(owner.finish(0).await.unwrap().is_none());

    let mut successor = router.register_run("parent", "run-b").await.unwrap();
    assert_eq!(
        router
            .child_completion_successor_run("parent", false, Some("run-a"))
            .await,
        None
    );
    assert_eq!(
        router
            .child_completion_successor_run("parent", false, None)
            .await,
        None
    );
    successor.begin_finalization().await;
    assert!(successor.finish(0).await.unwrap().is_none());
}

#[tokio::test]
async fn only_a_replayed_fence_for_this_owner_is_retained() {
    let router = SessionActivationRouter::new();
    let mut owner = router.register_run("parent", "run-a").await.unwrap();
    assert_eq!(
        router
            .child_completion_successor_run("parent", false, Some("foreign-run"))
            .await,
        None
    );
    assert_eq!(
        router
            .child_completion_successor_run("parent", false, Some("run-a"))
            .await
            .as_deref(),
        Some("run-a")
    );
    assert_eq!(
        router
            .child_completion_successor_run("parent", false, None)
            .await
            .as_deref(),
        Some("run-a")
    );
    owner.begin_finalization().await;
    assert!(owner.finish(0).await.unwrap().is_none());
}

#[tokio::test]
async fn finalizing_completions_stay_deferred_and_absent_owners_are_not_created() {
    let router = SessionActivationRouter::new();
    assert_eq!(
        router
            .child_completion_successor_run("absent", true, Some("old-run"))
            .await,
        None
    );
    assert!(router.states.lock().await.is_empty());
    let mut owner = router.register_run("parent", "run-a").await.unwrap();
    assert_eq!(
        router
            .child_completion_successor_run("parent", false, None)
            .await,
        None
    );
    owner.begin_finalization().await;
    assert_eq!(
        router
            .child_completion_successor_run("parent", false, None)
            .await
            .as_deref(),
        Some("run-a")
    );
    assert!(owner.finish(0).await.unwrap().is_none());
    assert_eq!(
        router
            .child_completion_successor_run("parent", false, None)
            .await,
        None
    );
}
