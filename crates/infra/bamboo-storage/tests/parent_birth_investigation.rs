//! #1318 parent birth regressions using real V2 save/delete/claim APIs.
//! Deterministic birth timestamps model an imported snapshot or remote clock skew.
//! Legacy compatibility fixtures rewrite Session encoding while retaining genuine
//! Actor record and marker bytes; the replacement regression uses normal APIs only.
use bamboo_domain::{
    ActorActivationClaim, ActorDirectoryError, ActorDirectoryPort, ActorLogicalState, Session,
    SessionKind, Storage,
};
use bamboo_storage::SessionStoreV2;
use chrono::{Duration, Utc};
use std::path::Path;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn lineage() -> (Session, Session, Session) {
    let birth = Utc::now() - Duration::minutes(5);
    let mut root = Session::new("birth-root", "test-model");
    root.created_at = birth;
    root.updated_at = birth;
    root.set_project_id_meta("birth-project");
    let mut child = Session::new_child_of("birth-child", &root, "test-model", "Child");
    child.created_at = birth + Duration::seconds(10);
    child.updated_at = child.created_at;
    child.set_project_id_meta("birth-project");
    let mut grandchild =
        Session::new_child_of("birth-grandchild", &child, "test-model", "Grandchild");
    grandchild.created_at = birth + Duration::seconds(30);
    grandchild.updated_at = grandchild.created_at;
    grandchild.set_project_id_meta("birth-project");
    (root, child, grandchild)
}

async fn publish(home: &Path) -> Result<(SessionStoreV2, Session, Session, Session)> {
    let store = SessionStoreV2::new(home.into()).await?;
    let (root, child, grandchild) = lineage();
    store.save_session(&root).await?;
    store.save_session(&child).await?;
    store.save_session(&grandchild).await?;
    assert_uninitialized(home, &root.id, &grandchild.id);
    Ok((store, root, child, grandchild))
}

fn descendant_path(home: &Path, root: &str, id: &str) -> std::path::PathBuf {
    home.join("sessions").join(root).join("children").join(id)
}

fn assert_uninitialized(home: &Path, root: &str, id: &str) {
    let directory = descendant_path(home, root, id);
    assert!(directory.join("session.json").is_file());
    assert!(directory.join("runtime.json").is_file());
    assert!(!directory.join("actor-authority.json").exists());
    assert!(!directory.join("actor-authority.initialized.json").exists());
}

fn claim(id: &str) -> ActorActivationClaim {
    let now = Utc::now();
    ActorActivationClaim {
        actor_id: id.into(),
        run_id: "local-birth-reproduction".into(),
        lease_owner: "fake-local-owner".into(),
        lease_expires_at: now + Duration::minutes(5),
        inbox_generation: 0,
        placement_ref: None,
        now,
    }
}

async fn replace_parent(
    home: &Path,
    store: SessionStoreV2,
    root: &Session,
    child: &Session,
    birth_delta: i64,
) -> Result<Session> {
    assert!(store.delete_session(&child.id).await?);
    assert!(!descendant_path(home, &root.id, &child.id).exists());
    drop(store);
    let replacement_store = SessionStoreV2::new(home.into()).await?;
    let mut replacement = Session::new_child_of(&child.id, root, "test-model", "Replacement");
    replacement.created_at = child.created_at + Duration::seconds(birth_delta);
    replacement.updated_at = replacement.created_at;
    replacement.set_project_id_meta("birth-project");
    assert_ne!(replacement.created_at, child.created_at);
    // New directory publication uses the normal Store API. No raw file rewrite.
    replacement_store.save_session(&replacement).await?;
    let saved = replacement_store.load_session(&child.id).await?.unwrap();
    assert_eq!(saved.created_at, replacement.created_at);
    drop(replacement_store);
    Ok(replacement)
}

#[tokio::test]
async fn unchanged_nested_lineage_first_claim_survives_independent_reopen() -> Result {
    let home = tempfile::tempdir()?;
    let (store, root, child, grandchild) = publish(home.path()).await?;
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    let loaded = reopened.load_session(&grandchild.id).await?.unwrap();
    assert_eq!(loaded.parent_session_id.as_deref(), Some(child.id.as_str()));
    assert_eq!(loaded.root_session_id, root.id);
    assert_eq!(loaded.spawn_depth, 2);
    assert_eq!(loaded.created_at, grandchild.created_at);
    assert_uninitialized(home.path(), &root.id, &grandchild.id);
    let first = reopened.claim_activation(&claim(&grandchild.id)).await?;
    assert_eq!(first.attempt, 1);
    let actor = reopened.inspect_actor(&grandchild.id).await?.actor;
    assert_eq!(actor.state, ActorLogicalState::Active);
    assert_eq!(actor.ancestor_observations[0].actor_id, child.id);
    assert_eq!(
        actor.ancestor_observations[0].session_created_at,
        child.created_at
    );
    assert_eq!(actor.ancestor_observations[1].actor_id, root.id);
    println!("GREEN unchanged nested parent exact birth; independent Store reopen; attempt=1");
    Ok(())
}

#[tokio::test]
async fn never_activated_grandchild_must_reject_different_parent_birth_even_with_ordered_clocks(
) -> Result {
    let home = tempfile::tempdir()?;
    let (store, root, child, grandchild) = publish(home.path()).await?;
    let replacement = replace_parent(home.path(), store, &root, &child, 10).await?;
    assert!(root.created_at < child.created_at);
    assert!(child.created_at < replacement.created_at);
    assert!(replacement.created_at < grandchild.created_at);
    assert_uninitialized(home.path(), &root.id, &grandchild.id);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    let loaded = reopened.load_session(&grandchild.id).await?.unwrap();
    assert_eq!(loaded.created_at, grandchild.created_at);
    assert_eq!(loaded.parent_session_id, grandchild.parent_session_id);
    let result = reopened.claim_activation(&claim(&grandchild.id)).await;
    println!("EXPECTED REJECT exact parent birth mismatch; actual first claim={result:?}");
    if result.is_ok() {
        let actor = reopened.inspect_actor(&grandchild.id).await?.actor;
        println!(
            "ACTUAL parent observation captured at first activation: {:?}; creation parent birth={}",
            actor.ancestor_observations[0], child.created_at
        );
        assert_eq!(
            actor.ancestor_observations[0].session_created_at,
            replacement.created_at
        );
    }
    assert!(
        matches!(result, Err(ActorDirectoryError::InvalidIdentity)),
        "An unactivated descendant must not bind to a different direct parent incarnation"
    );
    assert_uninitialized(home.path(), &root.id, &grandchild.id);
    Ok(())
}

#[tokio::test]
async fn different_parent_birth_after_grandchild_is_rejected_by_current_clock_order() -> Result {
    let home = tempfile::tempdir()?;
    let (store, root, child, grandchild) = publish(home.path()).await?;
    let replacement = replace_parent(home.path(), store, &root, &child, 40).await?;
    assert!(replacement.created_at > grandchild.created_at);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    assert_eq!(
        reopened
            .claim_activation(&claim(&grandchild.id))
            .await
            .unwrap_err(),
        ActorDirectoryError::InvalidIdentity
    );
    assert_uninitialized(home.path(), &root.id, &grandchild.id);
    println!("GREEN later replacement rejected; ordering guard control");
    Ok(())
}

#[tokio::test]
async fn already_activated_grandchild_rejects_changed_parent_birth_after_reopen() -> Result {
    let home = tempfile::tempdir()?;
    let (store, root, child, grandchild) = publish(home.path()).await?;
    let old = store.claim_activation(&claim(&grandchild.id)).await?;
    store.start_activation(&old.fence(), Utc::now()).await?;
    let replacement = replace_parent(home.path(), store, &root, &child, 10).await?;
    assert!(replacement.created_at < grandchild.created_at);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    assert_eq!(
        reopened
            .claim_activation(&claim(&grandchild.id))
            .await
            .unwrap_err(),
        ActorDirectoryError::InvalidIdentity
    );
    println!("GREEN existing Actor observation fences changed parent birth");
    Ok(())
}

#[tokio::test]
async fn id_only_constructor_and_plain_serde_roundtrip_have_no_creation_birth_proof() -> Result {
    let home = tempfile::tempdir()?;
    let (root, _, _) = lineage();
    let mut flat = Session::new_child("id-only-child", &root.id, "test-model", "Flat");
    flat.set_project_id_meta("birth-project");
    let encoded = serde_json::to_value(&flat)?;
    assert!(encoded.get("parent_created_at").is_none());
    assert!(encoded.get("parent_birth").is_none());
    let loaded: Session = serde_json::from_value(encoded)?;
    assert_eq!(loaded.parent_session_id.as_deref(), Some(root.id.as_str()));
    let store = SessionStoreV2::new(home.path().into()).await?;
    store.save_session(&root).await?;
    store.save_session(&loaded).await?;
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    assert_eq!(
        reopened
            .claim_activation(&claim(&loaded.id))
            .await
            .unwrap_err(),
        ActorDirectoryError::InvalidIdentity
    );
    assert_uninitialized(home.path(), &root.id, &loaded.id);
    println!(
        "GREEN explicit compatibility: ID-only/serde child readable; first activation rejected"
    );
    Ok(())
}

#[tokio::test]
async fn copy_of_nested_child_is_an_independent_root() -> Result {
    let home = tempfile::tempdir()?;
    let (store, root, _, grandchild) = publish(home.path()).await?;
    let copied = store
        .copy_session(&grandchild.id, "copied-birth-root")
        .await?
        .unwrap();
    assert_eq!(copied.kind, SessionKind::Root);
    assert!(copied.parent_session_id.is_none());
    assert_eq!(copied.root_session_id, copied.id);
    assert_eq!(copied.spawn_depth, 0);
    assert_ne!(copied.created_at, grandchild.created_at);
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    assert_eq!(
        reopened.claim_activation(&claim(&copied.id)).await?.attempt,
        1
    );
    assert_uninitialized(home.path(), &root.id, &grandchild.id);
    println!("GREEN native copy detaches ancestry and creates an independent Root");
    Ok(())
}

#[tokio::test]
async fn creation_birth_is_direct_parent_and_durable_in_both_files() -> Result {
    let home = tempfile::tempdir()?;
    let (store, root, child, grandchild) = publish(home.path()).await?;
    assert_eq!(root.parent_created_at, None);
    assert_eq!(child.parent_created_at, Some(root.created_at));
    assert_eq!(grandchild.parent_created_at, Some(child.created_at));
    let directory = descendant_path(home.path(), &root.id, &grandchild.id);
    for name in ["session.json", "runtime.json"] {
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(directory.join(name))?)?;
        let loaded: Session = serde_json::from_value(value.clone())?;
        assert_eq!(loaded.parent_created_at, Some(child.created_at));
        if name == "session.json" {
            assert_eq!(value["_bamboo_main_authority"]["version"], 2);
            assert_eq!(
                value["_bamboo_main_authority"]["payload"]["parent_created_at"],
                value["parent_created_at"]
            );
        }
    }
    let control = store
        .load_runtime_control_plane(&grandchild.id)
        .await?
        .unwrap();
    assert_eq!(control.parent_created_at, Some(child.created_at));
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    assert_eq!(
        reopened
            .load_session(&grandchild.id)
            .await?
            .unwrap()
            .parent_created_at,
        Some(child.created_at)
    );
    Ok(())
}

fn pair_bytes(directory: &Path) -> Result<[Vec<u8>; 2]> {
    Ok([
        std::fs::read(directory.join("session.json"))?,
        std::fs::read(directory.join("runtime.json"))?,
    ])
}

#[tokio::test]
async fn child_parent_birth_cannot_be_removed_or_changed_by_full_or_runtime_save() -> Result {
    let home = tempfile::tempdir()?;
    let (store, root, child, grandchild) = publish(home.path()).await?;
    let directory = descendant_path(home.path(), &root.id, &grandchild.id);
    let before = pair_bytes(&directory)?;
    let other = SessionStoreV2::new(home.path().into()).await?;
    for proof in [None, Some(child.created_at + Duration::seconds(1))] {
        let mut changed = grandchild.clone();
        changed.parent_created_at = proof;
        assert!(other.save_session(&changed).await.is_err());
        assert_eq!(pair_bytes(&directory)?, before);
        assert!(other.save_runtime_state(&changed).await.is_err());
        assert_eq!(pair_bytes(&directory)?, before);
    }
    store.save_runtime_state(&grandchild).await?;
    assert_eq!(
        store
            .load_session(&grandchild.id)
            .await?
            .unwrap()
            .parent_created_at,
        grandchild.parent_created_at
    );
    Ok(())
}

#[tokio::test]
async fn missing_runtime_birth_cannot_be_adopted_by_later_full_writer() -> Result {
    let home = tempfile::tempdir()?;
    let (store, root, _, grandchild) = publish(home.path()).await?;
    let directory = descendant_path(home.path(), &root.id, &grandchild.id);
    // Model an older runtime-only writer that ignores this optional Session
    // field. No Actor files exist or are fabricated.
    let path = directory.join("runtime.json");
    let mut value: serde_json::Value = serde_json::from_slice(&std::fs::read(&path)?)?;
    value.as_object_mut().unwrap().remove("parent_created_at");
    std::fs::write(path, serde_json::to_vec(&value)?)?;
    let before = pair_bytes(&directory)?;
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    assert!(reopened
        .claim_activation(&claim(&grandchild.id))
        .await
        .is_err());
    assert!(reopened.save_session(&grandchild).await.is_err());
    let mut missing = grandchild.clone();
    missing.parent_created_at = None;
    assert!(reopened.save_session(&missing).await.is_err());
    assert!(reopened.save_runtime_state(&missing).await.is_err());
    assert_eq!(pair_bytes(&directory)?, before);
    assert_uninitialized(home.path(), &root.id, &grandchild.id);
    Ok(())
}

fn legacy_session_pair(home: &Path, root: &str, session: &Session) -> Result {
    // Compatibility fixture only: simulate the pre-upgrade Session encoding.
    // Existing genuine Actor record/marker bytes are retained unchanged.
    let mut value = serde_json::to_value(session)?;
    value.as_object_mut().unwrap().remove("parent_created_at");
    let directory = descendant_path(home, root, &session.id);
    std::fs::write(directory.join("session.json"), serde_json::to_vec(&value)?)?;
    value["messages"] = serde_json::json!([]);
    std::fs::write(directory.join("runtime.json"), serde_json::to_vec(&value)?)?;
    Ok(())
}

#[tokio::test]
async fn old_child_without_actor_remains_readable_but_first_activation_cannot_backfill() -> Result {
    let home = tempfile::tempdir()?;
    let store = SessionStoreV2::new(home.path().into()).await?;
    let (root, mut child, mut grandchild) = lineage();
    child.parent_created_at = None;
    grandchild.parent_created_at = None;
    store.save_session(&root).await?;
    store.save_session(&child).await?;
    store.save_session(&grandchild).await?;
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    let loaded = reopened.load_session(&grandchild.id).await?.unwrap();
    assert!(loaded.parent_created_at.is_none());
    let directory = descendant_path(home.path(), &root.id, &grandchild.id);
    let before = pair_bytes(&directory)?;
    assert_eq!(
        reopened.ensure_actor(&grandchild.id).await.unwrap_err(),
        ActorDirectoryError::InvalidIdentity
    );
    assert_eq!(
        reopened
            .claim_activation(&claim(&grandchild.id))
            .await
            .unwrap_err(),
        ActorDirectoryError::InvalidIdentity
    );
    assert_eq!(pair_bytes(&directory)?, before);
    assert_uninitialized(home.path(), &root.id, &grandchild.id);
    Ok(())
}

#[tokio::test]
async fn legacy_already_activated_actor_uses_original_births_without_backfill() -> Result {
    use bamboo_domain::ActorActivationFinish;
    let home = tempfile::tempdir()?;
    let (store, root, child, grandchild) = publish(home.path()).await?;
    let first = store.claim_activation(&claim(&grandchild.id)).await?;
    store.start_activation(&first.fence(), Utc::now()).await?;
    store
        .finish_activation(&first.fence(), Utc::now(), ActorActivationFinish::Succeeded)
        .await?;
    let directory = descendant_path(home.path(), &root.id, &grandchild.id);
    let actor_before = std::fs::read(directory.join("actor-authority.json"))?;
    let marker_before = std::fs::read(directory.join("actor-authority.initialized.json"))?;
    legacy_session_pair(home.path(), &root.id, &child)?;
    legacy_session_pair(home.path(), &root.id, &grandchild)?;
    assert_eq!(
        std::fs::read(directory.join("actor-authority.json"))?,
        actor_before
    );
    assert_eq!(
        std::fs::read(directory.join("actor-authority.initialized.json"))?,
        marker_before
    );
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    let before = pair_bytes(&directory)?;
    let second = reopened.claim_activation(&claim(&grandchild.id)).await?;
    assert_eq!(second.attempt, first.attempt + 1);
    assert_eq!(pair_bytes(&directory)?, before);
    assert!(reopened
        .load_session(&grandchild.id)
        .await?
        .unwrap()
        .parent_created_at
        .is_none());
    let replacement = replace_parent(home.path(), reopened, &root, &child, 10).await?;
    assert!(replacement.created_at < grandchild.created_at);
    let third = SessionStoreV2::new(home.path().into()).await?;
    assert_eq!(
        third
            .claim_activation(&claim(&grandchild.id))
            .await
            .unwrap_err(),
        ActorDirectoryError::InvalidIdentity
    );
    Ok(())
}

#[tokio::test]
async fn legacy_inert_actor_row_does_not_substitute_for_creation_proof() -> Result {
    let home = tempfile::tempdir()?;
    let (store, root, child, grandchild) = publish(home.path()).await?;
    assert_eq!(
        store
            .ensure_actor(&grandchild.id)
            .await?
            .actor
            .current_attempt,
        0
    );
    legacy_session_pair(home.path(), &root.id, &child)?;
    legacy_session_pair(home.path(), &root.id, &grandchild)?;
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    assert_eq!(
        reopened
            .claim_activation(&claim(&grandchild.id))
            .await
            .unwrap_err(),
        ActorDirectoryError::InvalidIdentity
    );
    Ok(())
}

#[tokio::test]
async fn manual_title_refresh_preserves_activated_legacy_child_authority() -> Result {
    use bamboo_domain::ActorActivationFinish;
    let home = tempfile::tempdir()?;
    let (store, mut root, child, grandchild) = publish(home.path()).await?;
    let first = store.claim_activation(&claim(&grandchild.id)).await?;
    store.start_activation(&first.fence(), Utc::now()).await?;
    store
        .finish_activation(&first.fence(), Utc::now(), ActorActivationFinish::Succeeded)
        .await?;
    legacy_session_pair(home.path(), &root.id, &child)?;
    legacy_session_pair(home.path(), &root.id, &grandchild)?;
    drop(store);
    let reopened = SessionStoreV2::new(home.path().into()).await?;
    let directory = descendant_path(home.path(), &root.id, &grandchild.id);
    let before = pair_bytes(&directory)?;
    root.title = "Updated title".into();
    root.title_version += 1;
    root.metadata_version += 1;
    root.updated_at = Utc::now();
    reopened.save_manual_title(&root).await?;
    assert_eq!(
        reopened.load_session(&root.id).await?.unwrap().title,
        root.title
    );
    assert_eq!(pair_bytes(&directory)?, before);
    let second = reopened.claim_activation(&claim(&grandchild.id)).await?;
    assert_eq!(second.attempt, first.attempt + 1);
    assert!(reopened
        .load_session(&grandchild.id)
        .await?
        .unwrap()
        .parent_created_at
        .is_none());
    Ok(())
}
