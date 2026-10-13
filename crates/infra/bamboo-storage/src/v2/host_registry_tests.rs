use std::collections::BTreeSet;

use bamboo_domain::{
    ActorPlacementClass, ActorPlacementRef, HostPlacementIntent, HostPlacementRequest,
    HostRegistryError, ProjectId, WorkerHostCapabilities, WorkerHostRegistration,
    WorkerRunReservationCheck, WorkerSlotLease,
};
use chrono::{DateTime, Duration, Utc};

use super::FileHostRegistry;

fn set(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn capabilities(class: ActorPlacementClass) -> WorkerHostCapabilities {
    WorkerHostCapabilities {
        placement_class: class,
        project_ids: [ProjectId::parse("project-a").unwrap()].into(),
        allow_unscoped_project: false,
        trust_zone: "trusted".into(),
        workspace_labels: set(&["clean-git"]),
        executors: set(&["bamboo-runtime"]),
        tools: set(&["Glob"]),
        network_zones: set(&["internal"]),
        network_isolation: true,
    }
}

fn registration(
    host_ref: &str,
    generation: &str,
    previous: Option<&str>,
    now: DateTime<Utc>,
    max_slots: u16,
    class: ActorPlacementClass,
) -> WorkerHostRegistration {
    WorkerHostRegistration {
        host_ref: host_ref.into(),
        mailbox: format!("mailbox-{host_ref}"),
        role: Some("worker-pool".into()),
        connection_generation: generation.into(),
        credential_expires_at: now + Duration::minutes(5),
        observed_at: now,
        lease_expires_at: now + Duration::minutes(2),
        expected_connection_generation: previous.map(str::to_owned),
        max_slots,
        capabilities: capabilities(class),
    }
}

fn request(now: DateTime<Utc>) -> HostPlacementRequest {
    HostPlacementRequest {
        intent: HostPlacementIntent::Auto,
        actor_id: "child-a".into(),
        run_id: "run-a".into(),
        project_id: Some(ProjectId::parse("project-a").unwrap()),
        trust_zone: "trusted".into(),
        workspace_label: Some("clean-git".into()),
        executor: "bamboo-runtime".into(),
        required_tools: set(&["Glob"]),
        network_zone: Some("internal".into()),
        require_network_isolation: true,
        preferred_host_ref: None,
        now,
        lease_expires_at: now + Duration::seconds(30),
    }
}

async fn registry(home: &std::path::Path) -> FileHostRegistry {
    FileHostRegistry::new(home.to_path_buf()).await.unwrap()
}

fn run_check(lease: &WorkerSlotLease, now: DateTime<Utc>) -> WorkerRunReservationCheck {
    WorkerRunReservationCheck {
        placement_ref: ActorPlacementRef {
            class: ActorPlacementClass::Remote,
            lease_id: lease.lease_id.clone(),
            slot_epoch: Some(lease.epoch),
        },
        actor_id: lease.actor_id.clone(),
        run_id: lease.run_id.clone(),
        observed_host_ref: lease.host_ref.clone(),
        observed_connection_generation: lease.connection_generation.clone(),
        now,
    }
}

#[tokio::test]
async fn independent_stores_atomically_reserve_one_slot() {
    let dir = tempfile::tempdir().unwrap();
    let first = registry(dir.path()).await;
    let second = registry(dir.path()).await;
    let now = Utc::now();
    first
        .observe_host(registration(
            "host-a",
            "generation-a",
            None,
            now,
            1,
            ActorPlacementClass::Remote,
        ))
        .await
        .unwrap();

    let (a, b) = tokio::join!(
        first.reserve_slot(request(now)),
        second.reserve_slot(request(now))
    );
    assert_eq!(a.is_ok() as u8 + b.is_ok() as u8, 1);
    assert!(matches!(
        a.err().or_else(|| b.err()),
        Some(HostRegistryError::Busy)
    ));
}

#[tokio::test]
async fn new_connection_fences_old_slot_and_stale_observation() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry(dir.path()).await;
    let now = Utc::now();
    registry
        .observe_host(registration(
            "host-a",
            "generation-a",
            None,
            now,
            1,
            ActorPlacementClass::Remote,
        ))
        .await
        .unwrap();
    let old = registry.reserve_slot(request(now)).await.unwrap();

    registry
        .observe_host(registration(
            "host-a",
            "generation-b",
            Some("generation-a"),
            now + Duration::seconds(1),
            1,
            ActorPlacementClass::Remote,
        ))
        .await
        .unwrap();
    assert!(matches!(
        registry
            .validate_slot(&old, now + Duration::seconds(1))
            .await,
        Err(HostRegistryError::StaleLease)
    ));
    assert!(matches!(
        registry.release_slot(&old).await,
        Err(HostRegistryError::StaleLease)
    ));
    let mut next_request = request(now + Duration::seconds(1));
    next_request.run_id = "run-b".into();
    let next = registry.reserve_slot(next_request).await.unwrap();
    assert_eq!(next.slot, old.slot);
    assert!(next.epoch > old.epoch);
    assert_ne!(next.lease_id, old.lease_id);

    let stale = registration(
        "host-a",
        "generation-a",
        Some("generation-a"),
        now + Duration::seconds(2),
        1,
        ActorPlacementClass::Remote,
    );
    assert!(matches!(
        registry.observe_host(stale).await,
        Err(HostRegistryError::StaleGeneration)
    ));
    assert_eq!(
        registry
            .inspect_host("host-a")
            .await
            .unwrap()
            .unwrap()
            .connection_generation,
        "generation-b"
    );
}

#[tokio::test]
async fn hard_constraints_reject_pinned_host_without_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry(dir.path()).await;
    let now = Utc::now();
    registry
        .observe_host(registration(
            "host-local",
            "generation-local",
            None,
            now,
            1,
            ActorPlacementClass::Local,
        ))
        .await
        .unwrap();
    let mut remote = registration(
        "host-remote",
        "generation-remote",
        None,
        now,
        4,
        ActorPlacementClass::Remote,
    );
    remote.capabilities.trust_zone = "public".into();
    remote.capabilities.workspace_labels.clear();
    registry.observe_host(remote).await.unwrap();

    let mut pinned = request(now);
    pinned.intent = HostPlacementIntent::PinnedHost {
        host_ref: "host-remote".into(),
    };
    assert!(matches!(
        registry.reserve_slot(pinned).await,
        Err(HostRegistryError::PinnedUnavailable)
    ));

    let mut wrong_executor = request(now);
    wrong_executor.executor = "other-runtime".into();
    assert!(matches!(
        registry.reserve_slot(wrong_executor).await,
        Err(HostRegistryError::NoEligibleHost)
    ));
    let mut wrong_tool = request(now);
    wrong_tool.required_tools.insert("Shell".into());
    assert!(matches!(
        registry.reserve_slot(wrong_tool).await,
        Err(HostRegistryError::NoEligibleHost)
    ));
    let mut wrong_network = request(now);
    wrong_network.network_zone = Some("public".into());
    assert!(matches!(
        registry.reserve_slot(wrong_network).await,
        Err(HostRegistryError::NoEligibleHost)
    ));
    let mut wrong_project = request(now);
    wrong_project.project_id = Some(ProjectId::parse("project-b").unwrap());
    assert!(matches!(
        registry.reserve_slot(wrong_project).await,
        Err(HostRegistryError::NoEligibleHost)
    ));
    assert_eq!(
        registry.reserve_slot(request(now)).await.unwrap().host_ref,
        "host-local"
    );
}

#[tokio::test]
async fn renewal_replaces_exact_token_and_revocation_fences_current_generation() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry(dir.path()).await;
    let now = Utc::now();
    registry
        .observe_host(registration(
            "host-a",
            "generation-a",
            None,
            now,
            1,
            ActorPlacementClass::Remote,
        ))
        .await
        .unwrap();
    let original = registry.reserve_slot(request(now)).await.unwrap();
    let mut weakened = registration(
        "host-a",
        "generation-a",
        Some("generation-a"),
        now + Duration::seconds(5),
        1,
        ActorPlacementClass::Remote,
    );
    weakened.capabilities.trust_zone = "public".into();
    assert!(matches!(
        registry.observe_host(weakened).await,
        Err(HostRegistryError::Busy)
    ));
    let renewed = registry
        .renew_slot(
            &original,
            now + Duration::seconds(5),
            now + Duration::seconds(45),
        )
        .await
        .unwrap();
    assert!(matches!(
        registry
            .validate_slot(&original, now + Duration::seconds(6))
            .await,
        Err(HostRegistryError::StaleLease)
    ));
    registry
        .validate_slot(&renewed, now + Duration::seconds(6))
        .await
        .unwrap();
    assert!(matches!(
        registry
            .revoke_host("host-a", "generation-older", now + Duration::seconds(7))
            .await,
        Err(HostRegistryError::StaleGeneration)
    ));
    registry
        .revoke_host("host-a", "generation-a", now + Duration::seconds(7))
        .await
        .unwrap();
    assert!(matches!(
        registry
            .validate_slot(&renewed, now + Duration::seconds(7))
            .await,
        Err(HostRegistryError::StaleLease)
    ));
}

#[tokio::test]
async fn auto_selection_is_deterministic_and_respects_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry(dir.path()).await;
    let now = Utc::now();
    for host in ["host-b", "host-a"] {
        registry
            .observe_host(registration(
                host,
                &format!("generation-{host}"),
                None,
                now,
                1,
                ActorPlacementClass::Remote,
            ))
            .await
            .unwrap();
    }
    let first = registry.reserve_slot(request(now)).await.unwrap();
    assert_eq!(first.host_ref, "host-a");
    let mut second_request = request(now);
    second_request.run_id = "run-b".into();
    assert!(matches!(
        registry.reserve_slot(second_request.clone()).await,
        Err(HostRegistryError::Busy)
    ));
    second_request.actor_id = "child-b".into();
    let second = registry.reserve_slot(second_request).await.unwrap();
    assert_eq!(second.host_ref, "host-b");
}

#[tokio::test]
async fn expired_slot_can_be_reused_but_old_owner_cannot_renew_or_release_it() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry(dir.path()).await;
    let now = Utc::now();
    registry
        .observe_host(registration(
            "host-a",
            "generation-a",
            None,
            now,
            1,
            ActorPlacementClass::Remote,
        ))
        .await
        .unwrap();
    let mut short = request(now);
    short.lease_expires_at = now + Duration::seconds(1);
    let old = registry.reserve_slot(short).await.unwrap();
    let later = now + Duration::seconds(2);
    let mut retry = request(later);
    retry.run_id = "run-b".into();
    let current = registry.reserve_slot(retry).await.unwrap();
    assert!(current.epoch > old.epoch);
    assert!(matches!(
        registry
            .renew_slot(&old, later, later + Duration::seconds(20))
            .await,
        Err(HostRegistryError::StaleLease)
    ));
    assert!(matches!(
        registry.release_slot(&old).await,
        Err(HostRegistryError::StaleLease)
    ));
    assert_eq!(
        registry
            .inspect_slot_by_lease_id(&current.lease_id, "child-a", "run-b", later)
            .await
            .unwrap(),
        current
    );
}

#[tokio::test]
async fn run_preflight_requires_exact_live_placement_and_observed_connection() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry(dir.path()).await;
    let now = Utc::now();
    registry
        .observe_host(registration(
            "host-a",
            "generation-a",
            None,
            now,
            1,
            ActorPlacementClass::Remote,
        ))
        .await
        .unwrap();
    let lease = registry.reserve_slot(request(now)).await.unwrap();
    let check = run_check(&lease, now + Duration::seconds(1));
    let preflight = registry
        .validate_run_reservation(check.clone())
        .await
        .unwrap();
    assert_eq!(preflight.lease, lease);
    assert_eq!(preflight.mailbox, "mailbox-host-a");
    assert_eq!(preflight.role.as_deref(), Some("worker-pool"));

    let mut missing_epoch = check.clone();
    missing_epoch.placement_ref.slot_epoch = None;
    assert!(matches!(
        registry.validate_run_reservation(missing_epoch).await,
        Err(HostRegistryError::Invalid)
    ));
    let mut wrong_epoch = check.clone();
    wrong_epoch.placement_ref.slot_epoch = Some(lease.epoch + 1);
    assert!(matches!(
        registry.validate_run_reservation(wrong_epoch).await,
        Err(HostRegistryError::StaleLease)
    ));

    let mut wrong = check.clone();
    wrong.actor_id = "other-actor".into();
    assert!(matches!(
        registry.validate_run_reservation(wrong).await,
        Err(HostRegistryError::StaleLease)
    ));
    let mut wrong = check.clone();
    wrong.run_id = "other-run".into();
    assert!(matches!(
        registry.validate_run_reservation(wrong).await,
        Err(HostRegistryError::StaleLease)
    ));
    let mut wrong = check.clone();
    wrong.observed_host_ref = "host-b".into();
    assert!(matches!(
        registry.validate_run_reservation(wrong).await,
        Err(HostRegistryError::StaleLease)
    ));
    let mut wrong = check.clone();
    wrong.observed_connection_generation = "generation-b".into();
    assert!(matches!(
        registry.validate_run_reservation(wrong).await,
        Err(HostRegistryError::StaleLease)
    ));
    let mut wrong = check.clone();
    wrong.placement_ref.class = ActorPlacementClass::Local;
    assert!(matches!(
        registry.validate_run_reservation(wrong).await,
        Err(HostRegistryError::StaleLease)
    ));
    let mut wrong = check.clone();
    wrong.placement_ref.lease_id = "other-lease".into();
    assert!(matches!(
        registry.validate_run_reservation(wrong).await,
        Err(HostRegistryError::StaleLease)
    ));
    let mut unbounded = check.clone();
    unbounded.placement_ref.lease_id = "x".repeat(129);
    assert!(matches!(
        registry.validate_run_reservation(unbounded).await,
        Err(HostRegistryError::Invalid)
    ));
    let mut expired = check;
    expired.now = lease.expires_at;
    assert!(matches!(
        registry.validate_run_reservation(expired).await,
        Err(HostRegistryError::StaleLease)
    ));
}

#[tokio::test]
async fn run_preflight_follows_renewal_but_fences_replacement_and_release() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry(dir.path()).await;
    let now = Utc::now();
    registry
        .observe_host(registration(
            "host-a",
            "generation-a",
            None,
            now,
            1,
            ActorPlacementClass::Remote,
        ))
        .await
        .unwrap();
    let old = registry.reserve_slot(request(now)).await.unwrap();
    let renewed = registry
        .renew_slot(
            &old,
            now + Duration::seconds(5),
            now + Duration::seconds(45),
        )
        .await
        .unwrap();
    assert_eq!(old.lease_id, renewed.lease_id);
    assert_eq!(
        registry
            .validate_run_reservation(run_check(&old, now + Duration::seconds(6)))
            .await
            .unwrap()
            .lease,
        renewed
    );
    registry
        .observe_host(registration(
            "host-a",
            "generation-b",
            Some("generation-a"),
            now + Duration::seconds(7),
            1,
            ActorPlacementClass::Remote,
        ))
        .await
        .unwrap();
    assert!(matches!(
        registry
            .validate_run_reservation(run_check(&old, now + Duration::seconds(8)))
            .await,
        Err(HostRegistryError::StaleLease)
    ));
    let mut next_request = request(now + Duration::seconds(8));
    next_request.run_id = "run-b".into();
    let replacement = registry.reserve_slot(next_request).await.unwrap();
    registry
        .validate_run_reservation(run_check(&replacement, now + Duration::seconds(9)))
        .await
        .unwrap();
    registry.release_slot(&replacement).await.unwrap();
    assert!(matches!(
        registry
            .validate_run_reservation(run_check(&replacement, now + Duration::seconds(9)))
            .await,
        Err(HostRegistryError::StaleLease)
    ));
}

#[tokio::test]
async fn initialized_marker_prevents_lost_registry_from_resetting_leases() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry(dir.path()).await;
    let now = Utc::now();
    registry
        .observe_host(registration(
            "host-a",
            "generation-a",
            None,
            now,
            1,
            ActorPlacementClass::Remote,
        ))
        .await
        .unwrap();
    std::fs::remove_file(dir.path().join("worker-host-registry.json")).unwrap();
    assert!(matches!(
        registry.inspect_host("host-a").await,
        Err(HostRegistryError::Corrupt)
    ));
}
