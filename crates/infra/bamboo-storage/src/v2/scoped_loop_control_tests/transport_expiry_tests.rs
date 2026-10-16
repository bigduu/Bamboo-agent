//! Old-API integration contracts. No new retirement method is called directly.
//! Positive rows must free the original transport before ordinary BacklogFull.
use super::*;

fn target_dir(f: &Fixture) -> PathBuf {
    f._home.path().join("sessions").join(&f.target.id)
}
fn queued_files(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut found = Vec::new();
    for queue in ["new", "cur"] {
        for entry in std::fs::read_dir(dir.join("inbox").join(queue)).unwrap() {
            let path = entry.unwrap().path();
            if path
                .extension()
                .is_some_and(|extension| extension == "json")
            {
                found.push((path.clone(), std::fs::read(path).unwrap()));
            }
        }
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
}
fn limit_one(f: &mut Fixture) {
    f.inbox = Arc::new(crate::FileSessionInbox::new(
        f.store.clone(),
        SessionInboxLimits {
            max_backlog: 1,
            ..Default::default()
        },
    ));
}
async fn canonical(f: &Fixture) -> Session {
    f.store
        .load_execution_admission_snapshot(&f.target.id)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn native_terminal_reclaims_pending_claimed_and_committed_cur_before_backlog_limit() {
    // The last row is a *rejected* command committed by the real writer, not
    // a fabricated successful operation. Its cur remnant must be ACKed.
    // Expiry is injected into an actually issued native census entry. This
    // does not model normal lease renewal or claim any physical work stopped.
    for (evidence, stage) in ["expired", "terminal"]
        .into_iter()
        .flat_map(|evidence| ["pending", "claimed", "committed_cur"].map(|stage| (evidence, stage)))
    {
        let mut f = Fixture::new(true).await;
        limit_one(&mut f);
        let envelope = f.envelope();
        f.deliver(&envelope).await;
        if stage != "pending" {
            let (inbox, claim) = f.claim().await;
            if stage == "committed_cur" {
                let mut candidate = f.candidate(&claim);
                let live = f.activation.live.clone();
                assert!(f
                    .checkpoint(
                        &mut candidate,
                        inbox,
                        claim,
                        Arc::new(move |_| {
                            // Main + typed consumption exist, but physical ACK cannot
                            // retain the Supervisor anymore. Err is not rollback.
                            live.store(false, Ordering::SeqCst);
                        })
                    )
                    .await
                    .is_err());
            }
        }
        let before = canonical(&f).await;
        let cursor = serde_json::to_value(before.session_inbox_admission()).unwrap();
        let records = before
            .metadata
            .get(bamboo_domain::SCOPED_LOOP_CONSUMPTION_KEY)
            .cloned();
        assert_eq!(
            before
                .session_inbox_admission()
                .is_some_and(|c| c.contains(&envelope.id)),
            stage == "committed_cur"
        );
        let dir = target_dir(&f);
        let main_before = std::fs::read(dir.join("session.json")).unwrap();
        let runtime_before = std::fs::read(dir.join(RUNTIME_SIDECAR_FILE)).unwrap();
        if evidence == "terminal" {
            f.store
                .finish_activation(
                    &f.owner.as_ref().unwrap().fence,
                    Utc::now(),
                    bamboo_domain::ActorActivationFinish::Cancelled,
                )
                .await
                .unwrap();
        } else {
            let path = dir.join("actor-authority.json");
            let mut entry: bamboo_domain::ActorDirectoryEntry =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            let activation = entry.activation.as_mut().unwrap();
            assert_eq!(
                activation.status,
                bamboo_domain::ActorActivationStatus::Running
            );
            activation.lease_expires_at = Utc::now() - chrono::Duration::seconds(1);
            let expired_at = activation.lease_expires_at;
            entry.validate().unwrap();
            std::fs::write(&path, serde_json::to_vec(&entry).unwrap()).unwrap();
            // The existing strict directory read accepts the valid expired
            // entry; no renew/finish API or new runtime capability is invoked.
            let observed = f.store.inspect_actor(&f.target.id).await.unwrap();
            observed.validate().unwrap();
            let observed = observed.activation.unwrap();
            assert_eq!(
                observed.status,
                bamboo_domain::ActorActivationStatus::Running
            );
            assert_eq!(observed.lease_expires_at, expired_at);
            assert!(observed.lease_expires_at <= Utc::now());
        }
        let user = SessionMessageEnvelope::user_input(
            &f.target.id,
            "independent genuine User after terminal native run",
        );
        let result = f.inbox.deliver(&user).await;
        assert!(
            result.is_ok(),
            "{evidence}/{stage}: inert transport must not cause BacklogFull: {result:?}"
        );
        let after = canonical(&f).await;
        assert_eq!(
            std::fs::read(dir.join("session.json")).unwrap(),
            main_before
        );
        assert_eq!(
            std::fs::read(dir.join(RUNTIME_SIDECAR_FILE)).unwrap(),
            runtime_before
        );
        assert_eq!(
            serde_json::to_value(after.session_inbox_admission()).unwrap(),
            cursor
        );
        assert_eq!(
            after
                .metadata
                .get(bamboo_domain::SCOPED_LOOP_CONSUMPTION_KEY),
            records.as_ref()
        );
        assert_eq!(
            f.inbox
                .was_admitted(&f.target.id, &envelope.id)
                .await
                .unwrap(),
            stage == "committed_cur"
        );
        let backlog = f.inbox.inspect(&f.target.id).await.unwrap();
        assert_eq!(
            (backlog.pending, backlog.claimed),
            (1, 0),
            "{evidence}/{stage}"
        );
        assert_eq!(f.activation.activations.load(Ordering::SeqCst), 0);
        // Owned mode is permanent once claimed. Observe the actual queued
        // User without calling the forbidden legacy claim API or minting a
        // new Actor writer merely to make the assertion pass.
        let queue = queued_files(&target_dir(&f));
        assert_eq!(queue.len(), 1);
        let wrapper: serde_json::Value = serde_json::from_slice(&queue[0].1).unwrap();
        let remaining: SessionMessageEnvelope =
            serde_json::from_value(wrapper["body"].clone()).unwrap();
        assert_eq!(remaining, user);
    }
}

#[tokio::test]
async fn scoped_transport_unknown_legacy_and_live_or_broken_native_never_guess_dead() {
    for fault in [
        "legacy_none",
        "live",
        "missing_marker",
        "corrupt_record",
        "missing_execution",
        "epoch_only",
    ] {
        let mut f = Fixture::new(fault != "legacy_none").await;
        limit_one(&mut f);
        let envelope = f.envelope();
        f.deliver(&envelope).await;
        let dir = target_dir(&f);
        let original_transport = queued_files(&dir);
        let record = dir.join("actor-authority.json");
        match fault {
            "missing_marker" => {
                std::fs::remove_file(dir.join("actor-authority.initialized.json")).unwrap()
            }
            "corrupt_record" => std::fs::write(&record, b"{corrupt native census").unwrap(),
            "missing_execution" => {
                let path = dir.join(RUNTIME_SIDECAR_FILE);
                let mut side: Session =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                side.agent_runtime_state.as_mut().unwrap().execution_id = None;
                std::fs::write(path, serde_json::to_vec(&side).unwrap()).unwrap();
            }
            "epoch_only" => {
                let mut entry: bamboo_domain::ActorDirectoryEntry =
                    serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
                entry.activation.as_mut().unwrap().lease_epoch += 1;
                entry.validate().unwrap();
                std::fs::write(&record, serde_json::to_vec(&entry).unwrap()).unwrap();
            }
            _ => {}
        }
        let main_before = std::fs::read(dir.join("session.json")).unwrap();
        let runtime_before = std::fs::read(dir.join(RUNTIME_SIDECAR_FILE)).unwrap();
        let result = f
            .inbox
            .deliver(&SessionMessageEnvelope::user_input(
                &f.target.id,
                "must not guess the old owner dead",
            ))
            .await;
        assert!(
            result.is_err(),
            "{fault}: unknown/live scoped transport cannot be reclaimed: {result:?}"
        );
        assert_eq!(
            queued_files(&dir),
            original_transport,
            "{fault}: transport must remain byte-for-byte"
        );
        assert_eq!(
            std::fs::read(dir.join("session.json")).unwrap(),
            main_before
        );
        assert_eq!(
            std::fs::read(dir.join(RUNTIME_SIDECAR_FILE)).unwrap(),
            runtime_before
        );
        assert_eq!(f.activation.activations.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn scoped_delivery_rechecks_actual_native_expiry_after_expected_run_retained() {
    let f = Fixture::new(true).await;
    let record_path = target_dir(&f).join("actor-authority.json");
    let original = std::fs::read(&record_path).unwrap();
    let fired = Arc::new(AtomicBool::new(false));
    let seen = fired.clone();
    let path = record_path.clone();
    let inbox = f
        .inbox
        .with_owned_filesystem_hook_for_test(Arc::new(move |phase, _| {
            if phase == "expected_run_matched" && !seen.swap(true, Ordering::SeqCst) {
                // Physical publication barrier: mutation is an explicit witness
                // fault injection, not an attempt to reenter locked Actor APIs.
                let mut entry: bamboo_domain::ActorDirectoryEntry =
                    serde_json::from_slice(&std::fs::read(&path)?)?;
                entry.activation.as_mut().unwrap().lease_expires_at =
                    Utc::now() - chrono::Duration::seconds(1);
                entry.validate().map_err(io::Error::other)?;
                std::fs::write(&path, serde_json::to_vec(&entry)?)?;
            }
            Ok(())
        }));
    let envelope = f.envelope();
    let result = inbox
        .deliver_supervisor_loop_control(&f.binding, &envelope, f.activation.clone())
        .await;
    assert!(
        fired.load(Ordering::SeqCst),
        "actual final rename barrier must be reached"
    );
    assert!(result.is_err(), "expired actual native lease must prevent rename even while logical grant is live: {result:?}");
    assert!(queued_files(&target_dir(&f)).is_empty());
    assert_eq!(f.activation.notifications.load(Ordering::SeqCst), 0);
    assert!(!f
        .inbox
        .was_admitted(&f.target.id, &envelope.id)
        .await
        .unwrap());
    assert!(!canonical(&f)
        .await
        .session_inbox_admission()
        .is_some_and(|cursor| cursor.contains(&envelope.id)));
    std::fs::write(record_path, original).unwrap();
}

#[tokio::test]
async fn ordinary_root_and_child_without_scoped_carrier_do_not_require_runtime_or_census() {
    let home = tempfile::tempdir().unwrap();
    let store = Arc::new(SessionStoreV2::new(home.path().into()).await.unwrap());
    let root = Session::new("ordinary-root-no-scoped", "model");
    let child = Session::new_child_of("ordinary-child-no-scoped", &root, "model", "child");
    store.save_session(&root).await.unwrap();
    store.save_session(&child).await.unwrap();
    let inbox = crate::FileSessionInbox::new(store.clone(), SessionInboxLimits::default());
    for session in [&root, &child] {
        let directory = store
            .session_json_path(&session.id)
            .await
            .unwrap()
            .unwrap()
            .parent()
            .unwrap()
            .to_owned();
        for filename in [
            RUNTIME_SIDECAR_FILE,
            "actor-authority.json",
            "actor-authority.initialized.json",
        ] {
            match std::fs::remove_file(directory.join(filename)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => panic!("{error}"),
            }
        }
        let envelope = SessionMessageEnvelope::user_input(
            &session.id,
            "ordinary input without any scoped carrier",
        );
        inbox.deliver(&envelope).await.unwrap();
        assert!(!directory.join("actor-authority.json").exists());
        let claims = inbox.claim(&session.id, 1).await.unwrap();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].envelope, envelope);
    }
}
