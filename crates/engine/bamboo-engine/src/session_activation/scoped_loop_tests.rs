use super::*;
use bamboo_domain::{
    LoopControlAction, ScopedLoopControlBinding, Session, SessionActivationPort,
    SupervisorReference,
};
use std::collections::BTreeSet;

fn binding(session: &Session, run: &str) -> ScopedLoopControlBinding {
    ScopedLoopControlBinding {
        grant_id: uuid::Uuid::new_v4(),
        supervisor: SupervisorReference {
            session_id: bamboo_domain::DEFAULT_SUPERVISOR_SESSION_ID.into(),
            incarnation_id: uuid::Uuid::new_v4(),
        },
        supervisor_tool_authority_revision: 0,
        target_session_id: session.id.clone(),
        target_created_at: session.created_at,
        target_metadata_version: session.metadata_version,
        target_project_id: "project-a".parse().unwrap(),
        link_revision: 7,
        owner_run_id: run.into(),
        execution_id: uuid::Uuid::new_v4().to_string(),
        allowed_actions: BTreeSet::from([
            LoopControlAction::Status,
            LoopControlAction::Start,
            LoopControlAction::Stop,
        ]),
        max_iterations: 2,
        max_total_rounds: 8,
    }
}

#[tokio::test]
async fn scoped_grant_requires_the_same_real_router_owner_and_bound_lifetime() {
    let router = SessionActivationRouter::new();
    let foreign = SessionActivationRouter::new();
    let target = Session::new("scoped-owner", "model");
    let registration = router.register_run(&target.id, "run-a").await.unwrap();
    let other = foreign
        .register_session_run(&target, "run-a")
        .await
        .unwrap();
    let grant = binding(&target, "run-a");
    assert!(router
        .bind_supervisor_loop_control(
            &other.ownership(),
            grant.clone(),
            tokio_util::sync::CancellationToken::new(),
            tokio_util::sync::CancellationToken::new()
        )
        .await
        .is_err());
    assert!(router
        .bind_supervisor_loop_control(
            &registration.ownership(),
            grant.clone(),
            tokio_util::sync::CancellationToken::new(),
            tokio_util::sync::CancellationToken::new()
        )
        .await
        .is_err());
    assert!(
        router
            .bind_run_lifetime(&registration.ownership(), &target)
            .await
    );
    router
        .bind_supervisor_loop_control(
            &registration.ownership(),
            grant.clone(),
            tokio_util::sync::CancellationToken::new(),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    router
        .bind_supervisor_loop_control(
            &registration.ownership(),
            grant.clone(),
            tokio_util::sync::CancellationToken::new(),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    let mut replacement = grant.clone();
    replacement.grant_id = uuid::Uuid::new_v4();
    assert!(router
        .bind_supervisor_loop_control(
            &registration.ownership(),
            replacement,
            tokio_util::sync::CancellationToken::new(),
            tokio_util::sync::CancellationToken::new()
        )
        .await
        .is_err());
    assert_eq!(registration.ownership().run_id(), "run-a");
    assert!(Arc::ptr_eq(registration.ownership().router(), &router));
    other.abandon().await;
    registration.abandon().await;
}

#[tokio::test]
async fn scoped_observation_bootstraps_status_only_and_never_survives_owner_exit() {
    let router = SessionActivationRouter::new();
    let target = Session::new("scoped-observe", "model");
    let registration = router.register_session_run(&target, "run-a").await.unwrap();
    let grant = binding(&target, "run-a");
    assert!(router
        .observe_supervisor_loop(
            &grant.supervisor,
            &target.id,
            None,
            LoopControlAction::Status
        )
        .await
        .is_err());
    router
        .bind_supervisor_loop_control(
            &registration.ownership(),
            grant.clone(),
            tokio_util::sync::CancellationToken::new(),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        router
            .observe_supervisor_loop(
                &grant.supervisor,
                &target.id,
                None,
                LoopControlAction::Status
            )
            .await
            .unwrap(),
        grant
    );
    assert!(router
        .observe_supervisor_loop(
            &grant.supervisor,
            &target.id,
            None,
            LoopControlAction::Start
        )
        .await
        .is_err());
    assert!(router
        .observe_supervisor_loop(
            &grant.supervisor,
            &target.id,
            Some(6),
            LoopControlAction::Start
        )
        .await
        .is_err());
    assert!(router
        .observe_supervisor_loop(
            &grant.supervisor,
            &target.id,
            Some(7),
            LoopControlAction::Update
        )
        .await
        .is_err());
    assert_eq!(
        router
            .observe_supervisor_loop(
                &grant.supervisor,
                &target.id,
                Some(7),
                LoopControlAction::Start
            )
            .await
            .unwrap(),
        grant
    );
    let mut impostor = grant.supervisor.clone();
    impostor.incarnation_id = uuid::Uuid::new_v4();
    assert!(router
        .observe_supervisor_loop(&impostor, &target.id, None, LoopControlAction::Status)
        .await
        .is_err());
    registration.abandon().await;
    let successor = router.register_session_run(&target, "run-b").await.unwrap();
    assert!(router
        .observe_supervisor_loop(
            &grant.supervisor,
            &target.id,
            None,
            LoopControlAction::Status
        )
        .await
        .is_err());
    let retained_router = router.clone();
    let old_grant = grant.clone();
    assert!(tokio::task::spawn_blocking(
        move || retained_router.retain_supervisor_loop_blocking(&old_grant)
    )
    .await
    .unwrap()
    .is_err());
    successor.abandon().await;
}

#[tokio::test]
async fn scoped_revoke_waits_for_the_actual_target_publication_guard() {
    let router = SessionActivationRouter::new();
    let target = Session::new("scoped-retain", "model");
    let registration = router.register_session_run(&target, "run-a").await.unwrap();
    let grant = binding(&target, "run-a");
    router
        .bind_supervisor_loop_control(
            &registration.ownership(),
            grant.clone(),
            tokio_util::sync::CancellationToken::new(),
            tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap();
    let (retained_tx, retained_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocking_router = router.clone();
    let saved = grant.clone();
    let physical = tokio::task::spawn_blocking(move || {
        let guard = blocking_router
            .retain_supervisor_loop_blocking(&saved)
            .unwrap();
        assert!(guard.matches(
            &saved.target_session_id,
            saved.target_created_at,
            &saved.owner_run_id
        ));
        retained_tx.send(()).unwrap();
        release_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        assert!(guard.notify_committed_delivery(1));
    });
    retained_rx.await.unwrap();
    // The fence itself provides deterministic evidence; no sleep/yield-based
    // claim that a spawned revoker has or has not run is needed.
    assert!(router
        .publication_fence(&target.id)
        .try_lock_owned()
        .is_err());
    release_tx.send(()).unwrap();
    physical.await.unwrap();
    assert!(
        !router
            .revoke_supervisor_loop_control(&target.id, &uuid::Uuid::new_v4())
            .await
    );
    assert!(
        router
            .revoke_supervisor_loop_control(&target.id, &grant.grant_id)
            .await
    );
    let blocked_router = router.clone();
    let old = grant.clone();
    assert!(tokio::task::spawn_blocking(
        move || blocked_router.retain_supervisor_loop_blocking(&old)
    )
    .await
    .unwrap()
    .is_err());
    registration.abandon().await;
}

#[tokio::test]
async fn scoped_real_cancel_and_host_stop_invalidate_even_a_retained_guard() {
    for user_cancel in [true, false] {
        let router = SessionActivationRouter::new();
        let target = Session::new("scoped-stop", "model");
        let registration = router.register_session_run(&target, "run-a").await.unwrap();
        let grant = binding(&target, "run-a");
        let cancel = tokio_util::sync::CancellationToken::new();
        let stop = tokio_util::sync::CancellationToken::new();
        router
            .bind_supervisor_loop_control(
                &registration.ownership(),
                grant.clone(),
                cancel.clone(),
                stop.clone(),
            )
            .await
            .unwrap();
        let blocked_router = router.clone();
        let held_binding = grant.clone();
        tokio::task::spawn_blocking(move || {
            let guard = blocked_router
                .retain_supervisor_loop_blocking(&held_binding)
                .unwrap();
            assert!(guard.matches(
                &held_binding.target_session_id,
                held_binding.target_created_at,
                &held_binding.owner_run_id
            ));
            if user_cancel {
                cancel.cancel();
            } else {
                stop.cancel();
            }
            assert!(!guard.matches(
                &held_binding.target_session_id,
                held_binding.target_created_at,
                &held_binding.owner_run_id
            ));
            assert!(!guard.notify_committed_delivery(9));
        })
        .await
        .unwrap();
        assert!(router
            .observe_supervisor_loop(
                &grant.supervisor,
                &target.id,
                None,
                LoopControlAction::Status
            )
            .await
            .is_err());
        assert!(router
            .bind_supervisor_loop_control(
                &registration.ownership(),
                grant,
                tokio_util::sync::CancellationToken::new(),
                tokio_util::sync::CancellationToken::new()
            )
            .await
            .is_err());
        registration.abandon().await;
    }
}
