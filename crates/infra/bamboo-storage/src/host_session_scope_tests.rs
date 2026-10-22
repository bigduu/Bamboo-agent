//! Publication fencing fixtures. No real-account Sessions or filesystem writes.

use super::*;
use bamboo_domain::{
    HostScopeSessionObservation, HostSessionRight, HostSessionTargetScope, Session,
    HOST_SESSION_SCOPE_VERSION,
};
use std::{collections::BTreeSet, future::Future, task::Poll};

fn binding(caller: &Session, revision: u64) -> HostSessionScopeBinding {
    let mut target = Session::new("explicit-target", "model");
    target.set_project_id_meta("explicit-project");
    HostSessionScopeBinding {
        schema_version: HOST_SESSION_SCOPE_VERSION,
        grant_id: uuid::Uuid::new_v4(),
        revision,
        caller: HostScopeSessionObservation::capture(caller, vec![]).unwrap(),
        targets: BTreeMap::from([(
            "explicit-target".into(),
            HostSessionTargetScope {
                observation: HostScopeSessionObservation::capture(&target, vec![]).unwrap(),
                rights: BTreeSet::from([HostSessionRight::InspectBoundedProgress]),
            },
        )]),
        creates: BTreeMap::new(),
    }
}

async fn install(
    registry: &HostSessionScopeRegistry,
    caller: &Session,
    revision: u64,
) -> HostSessionScopeToken {
    registry
        .begin_install(&caller.id, revision - 1)
        .await
        .unwrap()
        .install(binding(caller, revision))
        .unwrap()
}

#[tokio::test]
async fn default_registry_is_empty_and_copied_observations_cannot_mint_installed_authority() {
    let registry = HostSessionScopeRegistry::default();
    let caller = Session::new("caller", "model");
    let observed = SessionCallerReference::from(&caller);
    assert!(registry.lookup(&observed).await.unwrap().is_none());
    let token = install(&registry, &caller, 1).await;
    let copied = serde_json::from_value(serde_json::to_value(token.binding()).unwrap()).unwrap();
    let forged = HostSessionScopeIssuer::default().issue(copied).unwrap();
    assert!(registry.retain(&forged, &observed).await.is_err());
    assert!(registry.retain(&token, &observed).await.is_ok());
    let mut recreated = caller;
    recreated.created_at += chrono::Duration::seconds(1);
    assert!(registry
        .lookup(&SessionCallerReference::from(&recreated))
        .await
        .is_err());
    assert!(registry
        .retain(&token, &SessionCallerReference::from(&recreated))
        .await
        .is_err());
}

#[tokio::test]
async fn revoke_waits_for_retained_physical_authority_then_invalidates_old_token() {
    let registry = HostSessionScopeRegistry::default();
    let caller = Session::new("caller", "model");
    let observed = SessionCallerReference::from(&caller);
    let token = install(&registry, &caller, 1).await;
    let held = registry.retain(&token, &observed).await.unwrap();
    assert!(held.token().same_installation(&token));
    let revoke = registry.revoke(&caller.id, 1);
    tokio::pin!(revoke);
    let pending =
        std::future::poll_fn(|cx| Poll::Ready(revoke.as_mut().poll(cx).is_pending())).await;
    assert!(
        pending,
        "revocation cannot pass an owned final-operation guard"
    );
    drop(held);
    assert_eq!(revoke.await.unwrap(), 2);
    assert!(registry.lookup(&observed).await.is_err());
    assert!(registry.retain(&token, &observed).await.is_err());
}

#[tokio::test]
async fn replacement_uses_one_publication_fence_and_never_reactivates_an_old_token() {
    let registry = HostSessionScopeRegistry::default();
    let caller = Session::new("caller", "model");
    let observed = SessionCallerReference::from(&caller);
    let old = install(&registry, &caller, 1).await;
    let held = registry.retain(&old, &observed).await.unwrap();
    let publication = registry.begin_install(&caller.id, 1);
    tokio::pin!(publication);
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(publication.as_mut().poll(cx).is_pending())).await
    );
    drop(held);
    let new = publication
        .await
        .unwrap()
        .install(binding(&caller, 2))
        .unwrap();
    assert!(!new.same_installation(&old));
    assert!(registry.retain(&old, &observed).await.is_err());
    assert!(registry.retain(&new, &observed).await.is_ok());
    assert!(registry.begin_install(&caller.id, 1).await.is_err());
    assert!(registry.revoke(&caller.id, 1).await.is_err());
    assert!(registry
        .lookup(&observed)
        .await
        .unwrap()
        .unwrap()
        .same_installation(&new));
}

#[tokio::test]
async fn dropping_uncommitted_publication_or_wrong_caller_changes_no_scope() {
    let registry = HostSessionScopeRegistry::default();
    let caller = Session::new("caller", "model");
    let observed = SessionCallerReference::from(&caller);
    drop(registry.begin_install(&caller.id, 0).await.unwrap());
    assert!(registry.lookup(&observed).await.unwrap().is_none());
    let other = Session::new("other-caller", "model");
    assert!(registry
        .begin_install(&caller.id, 0)
        .await
        .unwrap()
        .install(binding(&other, 1))
        .is_err());
    assert!(registry.lookup(&observed).await.unwrap().is_none());
    assert!(registry
        .lookup(&SessionCallerReference::from(&other))
        .await
        .unwrap()
        .is_none());
    let token = install(&registry, &caller, 1).await;
    assert!(registry.retain(&token, &observed).await.is_ok());
}

#[tokio::test]
async fn absent_namespace_holds_publication_without_consuming_caller_slots() {
    let registry = HostSessionScopeRegistry::default();
    let held = registry
        .retain_never_installed("legacy-caller")
        .await
        .unwrap();
    assert!(registry.slots.read().await.is_empty());
    let other = Session::new("another-caller", "model");
    let publication = registry.begin_install(&other.id, 0);
    tokio::pin!(publication);
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(publication.as_mut().poll(cx).is_pending())).await
    );
    drop(held);
    publication
        .await
        .unwrap()
        .install(binding(&other, 1))
        .unwrap();
    assert_eq!(registry.slots.read().await.len(), 1);
    assert!(registry.retain_never_installed(&other.id).await.is_err());
    registry.revoke(&other.id, 1).await.unwrap();
    assert!(registry.retain_never_installed(&other.id).await.is_err());
}

#[tokio::test]
async fn uncommitted_slot_absence_retains_the_actual_slot_publication_guard() {
    let registry = HostSessionScopeRegistry::default();
    let caller = Session::new("legacy-caller", "model");
    drop(registry.begin_install(&caller.id, 0).await.unwrap());
    let existing = registry.slot(&caller.id).await.unwrap();
    let held = registry.retain_never_installed(&caller.id).await.unwrap();
    // This writer already selected the slot before the map read. Holding the
    // map alone would not prevent it from publishing while admission runs.
    let write = existing.write_owned();
    tokio::pin!(write);
    assert!(std::future::poll_fn(|cx| Poll::Ready(write.as_mut().poll(cx).is_pending())).await);
    drop(held);
    drop(write.await);
    install(&registry, &caller, 1).await;
    assert!(registry.retain_never_installed(&caller.id).await.is_err());
}
