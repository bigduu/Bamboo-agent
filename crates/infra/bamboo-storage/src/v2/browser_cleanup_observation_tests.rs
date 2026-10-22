//! Browser cleanup is an observation of existing placement, not birth authority.
use super::*;

async fn large_history_fixture() -> (
    tempfile::TempDir,
    SessionStoreV2,
    Session,
    Session,
    Session,
    Session,
) {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let root = Session::new("cleanup-large-history-root", "model");
    let child = Session::new_child_of("cleanup-large-history-child", &root, "model", "child");
    let survivor = Session::new("cleanup-large-history-survivor", "model");
    let surviving_child =
        Session::new_child_of("cleanup-surviving-child", &survivor, "model", "child");
    for session in [&root, &child, &survivor, &surviving_child] {
        store.save_session(session).await.unwrap();
    }
    store
        .update_index(|index| {
            index.sessions.remove(&root.id);
            index.sessions.remove(&child.id);
            index.sessions.remove(&surviving_child.id);
            Ok(())
        })
        .await
        .unwrap();
    // Empty unrelated history slots exercise locator metadata only. Do not
    // create thousands of Sessions or invoke models/providers for this fixture.
    for number in 0..=canonical_birth_census::Limits::default().roots {
        std::fs::create_dir(
            home.path()
                .join("sessions")
                .join(format!("unrelated-history-{number:05}")),
        )
        .unwrap();
    }
    (home, store, root, child, survivor, surviving_child)
}

fn lock_names(home: &Path) -> Vec<std::ffi::OsString> {
    let mut names = std::fs::read_dir(home.join(".session-write-locks"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[tokio::test]
async fn cleanup_observations_support_history_beyond_the_birth_census_budget() {
    let (home, store, root, child, _, _) = large_history_fixture().await;
    assert_eq!(
        store.canonical_birth_census(&root.id).await,
        Err(canonical_birth_census::CensusError::BudgetExceeded),
        "birth authority retains its existing bounded full census"
    );
    for session in [&root, &child] {
        assert!(
            store
                .session_has_canonical_cleanup_evidence(&session.id)
                .await
                .expect("unrelated history must not disable cleanup admission"),
            "canonical placement remains evidence even without its index row"
        );
        assert!(!store
            .session_is_canonically_absent(&session.id)
            .await
            .expect("live placement remains observable in a large store"));
    }
    let before = lock_names(home.path());
    assert!(!store
        .session_has_canonical_cleanup_evidence("never-created-large-history-target")
        .await
        .unwrap());
    assert_eq!(lock_names(home.path()), before);
}

#[tokio::test]
async fn large_history_recursive_deletion_keeps_post_delete_observations_available() {
    let (home, store, root, child, survivor, surviving_child) = large_history_fixture().await;
    let result = store
        .delete_session_recursive_with_progress(&root.id, true)
        .await
        .unwrap();
    assert!(result.deleted);
    let mut expected = vec![root.id.clone(), child.id.clone()];
    expected.sort();
    assert_eq!(result.candidate_session_ids, expected);
    for id in result.candidate_session_ids {
        assert!(store
            .session_is_canonically_absent(&id)
            .await
            .expect("confirmed deletion must not fail its subsequent observation"));
    }
    assert!(store
        .session_has_canonical_cleanup_evidence(&root.id)
        .await
        .unwrap());
    for live in [&survivor, &surviving_child] {
        assert!(!store.session_is_canonically_absent(&live.id).await.unwrap());
    }
    assert!(home.path().join("sessions").join(&survivor.id).exists());
    assert!(home
        .path()
        .join("sessions")
        .join(&survivor.id)
        .join("children")
        .join(&surviving_child.id)
        .exists());
}

#[tokio::test]
async fn cleanup_observations_reject_ambiguous_and_linked_placements() {
    let home = tempfile::tempdir().unwrap();
    let store = SessionStoreV2::new(home.path().into()).await.unwrap();
    let root = Session::new("cleanup-observation-target", "model");
    let other = Session::new("cleanup-observation-other-root", "model");
    store.save_session(&root).await.unwrap();
    store.save_session(&other).await.unwrap();
    let duplicate = home
        .path()
        .join("sessions")
        .join(&other.id)
        .join("children")
        .join(&root.id);
    std::fs::create_dir(&duplicate).unwrap();
    assert!(store
        .session_has_canonical_cleanup_evidence(&root.id)
        .await
        .is_err());
    assert!(store.session_is_canonically_absent(&root.id).await.is_err());
    std::fs::remove_dir(&duplicate).unwrap();
    #[cfg(unix)]
    {
        let outside = home.path().join("synthetic-outside");
        std::fs::create_dir(&outside).unwrap();
        let marker = outside.join("preserve-marker");
        std::fs::write(&marker, b"synthetic state must survive").unwrap();
        std::os::unix::fs::symlink(&outside, &duplicate).unwrap();
        assert!(store
            .session_has_canonical_cleanup_evidence(&root.id)
            .await
            .is_err());
        assert!(store.session_is_canonically_absent(&root.id).await.is_err());
        assert_eq!(
            std::fs::read(&marker).unwrap(),
            b"synthetic state must survive"
        );
    }
}
