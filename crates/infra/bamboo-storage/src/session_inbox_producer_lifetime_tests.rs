//! Compatible producer jobs retain the same original physical scope as v3 consumers.
use super::owned_lifetime_tests::{probe, Barrier, Fixture, Parked};
use super::*;
use bamboo_domain::{
    AdmissionGate, SessionMessageKind, Storage, SupervisorManagementMutation as Mutation,
    SupervisorManagementRequest,
};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

fn enable(f: &Fixture) {
    f.runtime
        .block_on(f.inbox.claim_owned("target", 0, None, &f.request(0)))
        .unwrap();
}
fn message_path(f: &Fixture, message: &SessionMessageEnvelope, generation: u64) -> PathBuf {
    let wrapper = FileSessionInbox::wrapper(message, generation, None);
    f.dir
        .join("new")
        .join(format!("{generation:020}-{}.json", wrapper.id.0))
}
fn messages(f: &Fixture) -> Vec<SessionMessageEnvelope> {
    let mut result = Vec::new();
    for queue in ["new", "cur"] {
        if let Ok(entries) = std::fs::read_dir(f.dir.join(queue)) {
            for entry in entries {
                let path = entry.unwrap().path();
                if path.file_name().unwrap().to_string_lossy().starts_with('.') {
                    continue;
                }
                let (wrapper, _) =
                    FileSessionInbox::decode_owned_wrapper(&std::fs::read(path).unwrap()).unwrap();
                let mut body = wrapper.body;
                body.as_object_mut().unwrap().remove(ACTIVATION_INTENT_KEY);
                result.push(serde_json::from_value(body).unwrap());
            }
        }
    }
    result
}
fn no_hidden_temps(f: &Fixture) {
    assert!(!std::fs::read_dir(f.dir.join("new")).unwrap().any(|e| e
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with('.')));
}

#[test]
fn compatible_maildir_publication_owns_capacity_and_gate_through_abort_and_shutdown() {
    for mode in ["ungated_abort", "gated_shutdown", "cancelled_shutdown"] {
        let mut f = Fixture::new();
        enable(&f);
        let limits = SessionInboxLimits {
            max_backlog: 1,
            ..SessionInboxLimits::default()
        };
        f.inbox = FileSessionInbox::new(f.store.clone(), limits);
        f.other = FileSessionInbox::new(f.other_store.clone(), limits);
        let first = SessionMessageEnvelope::user_input("target", "first exact input");
        let next = SessionMessageEnvelope::user_input("target", "successor input");
        let gate = AdmissionGate::default();
        let job_gate = gate.clone();
        let original = first.clone();
        let path = message_path(&f, &first, 1);
        let mut parked = Parked::start(
            f.inbox.clone(),
            "maildir_replace",
            path,
            move |inbox| async move {
                let gate = (mode != "ungated_abort").then_some(&job_gate);
                inbox
                    .deliver_with_activation_intent(
                        &original,
                        SessionActivationPolicy::RespectSpecificWait,
                        gate,
                    )
                    .await
                    .map(|_| ())
            },
        );
        if mode == "ungated_abort" {
            parked.caller.abort();
            let deadline = Instant::now() + Duration::from_secs(10);
            while !parked.dropped.load(Ordering::SeqCst) && Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert!(parked.dropped.load(Ordering::SeqCst));
        } else {
            parked.shutdown();
        }
        if mode == "cancelled_shutdown" {
            gate.cancel_if_pending();
            assert!(gate.is_cancelled());
        }
        f.assert_physical_locks(true);
        let producer = f.other.clone();
        let successor = next.clone();
        let pending = f.runtime.spawn(async move {
            producer
                .deliver_with_activation_intent(
                    &successor,
                    SessionActivationPolicy::RespectSpecificWait,
                    None,
                )
                .await
        });
        let consumer = f.successor(0);
        f.assert_waiting(&pending);
        f.assert_waiting(&consumer);
        parked.barrier.release();
        let outcome = f.finish(pending);
        let claimed = f.finish(consumer);
        let expected = if mode == "cancelled_shutdown" {
            assert_eq!(outcome.unwrap().generation, 2); // preserve the allocated GEN hole
            assert!(gate.is_cancelled());
            next
        } else {
            assert!(matches!(
                outcome,
                Err(SessionInboxError::BacklogFull {
                    current: 1,
                    limit: 1
                })
            ));
            assert_eq!(claimed[0].claim.envelope, first);
            assert_eq!(claimed[0].lease.epoch, 1);
            if mode != "ungated_abort" {
                assert!(gate.is_committed());
                gate.cancel_if_pending();
                assert!(gate.is_committed());
                assert!(!gate.is_cancelled());
            }
            first.clone()
        };
        assert_eq!(messages(&f), vec![expected]);
        no_hidden_temps(&f);
        if mode != "cancelled_shutdown" {
            let retry = f
                .runtime
                .block_on(f.inbox.deliver_with_activation_intent(
                    &first,
                    SessionActivationPolicy::RespectSpecificWait,
                    Some(&gate),
                ))
                .unwrap();
            assert_eq!(retry.generation, 1); // exact retry precedes gate/backlog checks
            let mut mismatch = SessionMessageEnvelope::user_input("target", "changed semantics");
            mismatch.id = first.id.clone();
            assert!(matches!(
                f.runtime.block_on(f.inbox.deliver_with_activation_intent(
                    &mismatch,
                    SessionActivationPolicy::RespectSpecificWait,
                    Some(&gate)
                )),
                Err(SessionInboxError::InvalidClaim(_))
            ));
        }
        let cold = f
            .runtime
            .block_on(SessionStoreV2::new(f._temp.path().into()))
            .unwrap();
        let cold_inbox = FileSessionInbox::new(Arc::new(cold), limits);
        assert_eq!(
            f.runtime
                .block_on(cold_inbox.inspect("target"))
                .unwrap()
                .pending
                + f.runtime
                    .block_on(cold_inbox.inspect("target"))
                    .unwrap()
                    .claimed,
            1
        );
    }
}

#[test]
fn producer_acquisition_job_cannot_create_after_independent_target_deletion() {
    let f = Fixture::new();
    let original = SessionMessageEnvelope::user_input("target", "cancelled setup");
    let mut parked = Parked::start(
        f.inbox.clone(),
        "acquire",
        f.dir.clone(),
        move |inbox| async move { inbox.deliver(&original).await.map(|_| ()) },
    );
    parked.shutdown();
    f.assert_physical_locks(false);
    let store = f.other_store.clone();
    let delete = f
        .runtime
        .spawn(async move { store.delete_session("target").await.unwrap() });
    f.assert_waiting(&delete);
    parked.barrier.release();
    assert!(f.finish(delete));
    assert!(!f.dir.parent().unwrap().exists());
}

#[test]
fn producer_gen_int_act_jobs_keep_existing_holes_and_partial_monotonic_authority() {
    for selected in [
        GENERATION_FILE,
        INTERRUPT_GENERATION_FILE,
        ACTIVATION_GENERATION_FILE,
        "legacy_upgrade",
    ] {
        let f = Fixture::new();
        let original = SessionMessageEnvelope::user_input("target", "staged input");
        if selected != "legacy_upgrade" {
            enable(&f);
        }
        if matches!(
            selected,
            INTERRUPT_GENERATION_FILE | ACTIVATION_GENERATION_FILE
        ) {
            f.runtime.block_on(f.inbox.deliver(&original)).unwrap();
        }
        let path = f.dir.join(if selected == "legacy_upgrade" {
            ACTIVATION_GENERATION_FILE
        } else {
            selected
        });
        let job_message = original.clone();
        let mut parked = Parked::start(f.inbox.clone(), "replace", path, move |inbox| async move {
            match selected {
                GENERATION_FILE => inbox.deliver(&job_message).await.map(|_| ()),
                "legacy_upgrade" => inbox
                    .deliver_with_activation_intent(
                        &job_message,
                        SessionActivationPolicy::RespectSpecificWait,
                        None,
                    )
                    .await
                    .map(|_| ()),
                _ => {
                    inbox
                        .mark_activation_eligible(
                            "target",
                            1,
                            SessionActivationPolicy::InterruptSpecificWait,
                        )
                        .await
                }
            }
        });
        parked.shutdown();
        f.assert_physical_locks(true);
        let waiter = f.process_waiter();
        f.assert_waiting(&waiter);
        parked.barrier.release();
        f.finish(waiter);
        f.runtime.block_on(async {
            if matches!(selected, GENERATION_FILE | "legacy_upgrade") {
                assert!(messages(&f).is_empty());
                assert_eq!(FileSessionInbox::read_generation(&f.dir).await.unwrap(), 1);
                let next = SessionMessageEnvelope::user_input("target", "after allocated hole");
                assert_eq!(f.other.deliver(&next).await.unwrap().generation, 2);
                if selected == "legacy_upgrade" {
                    let value: serde_json::Value = serde_json::from_slice(
                        &std::fs::read(f.dir.join(ACTIVATION_GENERATION_FILE)).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(value["version"], 2); // holder reuse never enables v3
                }
            } else {
                assert_eq!(
                    FileSessionInbox::read_interrupt_generation(&f.dir)
                        .await
                        .unwrap(),
                    1
                );
                assert_eq!(
                    FileSessionInbox::read_activation_generation(&f.dir)
                        .await
                        .unwrap(),
                    u64::from(selected == ACTIVATION_GENERATION_FILE)
                );
                if selected == INTERRUPT_GENERATION_FILE {
                    assert!(f
                        .other
                        .claim_owned("target", 1, None, &f.request(0))
                        .await
                        .unwrap()
                        .is_empty());
                    f.other
                        .mark_activation_eligible(
                            "target",
                            1,
                            SessionActivationPolicy::RespectSpecificWait,
                        )
                        .await
                        .unwrap();
                }
                let claim = f
                    .other
                    .claim_owned("target", 1, None, &f.request(0))
                    .await
                    .unwrap()
                    .remove(0);
                assert_eq!(claim.lease.epoch, 1);
                assert_eq!(
                    claim.claim.activation_policy,
                    SessionActivationPolicy::InterruptSpecificWait
                );
                assert_eq!(claim.claim.envelope, original);
            }
        });
    }
}

#[test]
fn guidance_cancel_and_each_producer_visible_quarantine_retain_actual_scope() {
    for action in [
        "cancel",
        "deliver_scan",
        "pending_scan",
        "inspect_scan",
        "cancel_scan",
    ] {
        let f = Fixture::new();
        enable(&f);
        let mut guidance = SessionMessageEnvelope::user_input("target", "guidance");
        guidance.correlation_id = Some("session-guidance".into());
        f.runtime
            .block_on(f.inbox.deliver_with_activation_intent(
                &guidance,
                SessionActivationPolicy::RespectSpecificWait,
                None,
            ))
            .unwrap();
        let (phase, path) = if action == "cancel" {
            ("cancel", message_path(&f, &guidance, 1))
        } else {
            let path = f.dir.join("new/malformed.json");
            std::fs::write(&path, b"malformed").unwrap();
            ("quarantine", path)
        };
        let job_guidance = guidance.clone();
        let mut parked = Parked::start(
            f.inbox.clone(),
            phase,
            path.clone(),
            move |inbox| async move {
                match action {
                    "deliver_scan" => inbox
                        .deliver(&SessionMessageEnvelope::user_input(
                            "target",
                            "unstarted producer",
                        ))
                        .await
                        .map(|_| ()),
                    "pending_scan" => inbox.pending_guidance("target").await.map(|_| ()),
                    "inspect_scan" => inbox.inspect("target").await.map(|_| ()),
                    _ => inbox
                        .cancel_guidance("target", &job_guidance.id)
                        .await
                        .map(|_| ()),
                }
            },
        );
        parked.shutdown();
        f.assert_physical_locks(true);
        let consumer = f.successor(0);
        f.assert_waiting(&consumer);
        parked.barrier.release();
        let result = f.finish(consumer);
        if action == "cancel" {
            assert!(result.is_empty());
            assert!(f
                .dir
                .join("cancelled")
                .join(path.file_name().unwrap())
                .exists());
            assert_eq!(
                f.runtime
                    .block_on(f.other.deliver_with_activation_intent(
                        &guidance,
                        SessionActivationPolicy::RespectSpecificWait,
                        None
                    ))
                    .unwrap()
                    .generation,
                1
            );
        } else {
            assert_eq!(result[0].lease.epoch, 1);
            assert_eq!(result[0].claim.envelope, guidance);
            assert!(f.dir.join("corrupt/malformed.json").exists());
            assert!(!f
                .runtime
                .block_on(f.other.cancel_guidance("target", &guidance.id))
                .unwrap());
        }
        assert!(!path.exists());
    }
}

#[test]
fn maildir_fault_cleanup_keeps_physical_holder_and_primary_outcome() {
    for failure in ["injected", "os_error", "shutdown_cleanup"] {
        let f = Fixture::new();
        enable(&f);
        let gate = AdmissionGate::default();
        let job_gate = gate.clone();
        let original = SessionMessageEnvelope::user_input("target", "failed producer");
        let path = message_path(&f, &original, 1);
        let barrier = Arc::new(Barrier::default());
        let selected = barrier.clone();
        let expected_path = path.clone();
        let mut inbox = f.inbox.clone();
        inbox.owned_fs_hook = Some(Arc::new(move |phase, actual| {
            if actual == expected_path {
                if phase == "maildir_replace" {
                    if failure == "os_error" {
                        std::fs::create_dir(actual)?;
                    } else {
                        return Err(std::io::Error::other("primary Maildir fault"));
                    }
                }
                if phase == "maildir_cleanup" {
                    selected.park();
                    if failure == "os_error" {
                        std::fs::remove_dir(actual)?;
                    } // remove only injected fixture fault
                    return Err(std::io::Error::other("secondary cleanup observer"));
                }
            }
            Ok(())
        }));
        let mut parked = Parked::with_hook(inbox, barrier, move |inbox| async move {
            inbox
                .deliver_with_gate(&original, &job_gate)
                .await
                .map(|_| ())
        });
        if failure == "shutdown_cleanup" {
            parked.shutdown();
        }
        f.assert_physical_locks(true);
        let producer = f.other.clone();
        let replacement = SessionMessageEnvelope::user_input("target", "after failure");
        let expected = replacement.clone();
        let next = f
            .runtime
            .spawn(async move { producer.deliver(&replacement).await.unwrap() });
        f.assert_waiting(&next);
        if failure == "shutdown_cleanup" {
            parked.barrier.release();
        } else {
            let error = parked.result().unwrap_err().to_string();
            assert!(!error.contains("secondary"));
            if failure == "injected" {
                assert!(error.contains("primary Maildir fault"));
            } else {
                assert!(!error.contains("primary Maildir fault"));
            }
        }
        assert_eq!(f.finish(next).generation, 2);
        assert!(!gate.is_committed());
        assert!(!path.exists());
        no_hidden_temps(&f);
        assert_eq!(messages(&f), vec![expected]);
    }
}

#[test]
fn supervisor_followup_started_job_retains_full_original_relationship_lockset() {
    for change in ["detach", "scope", "project", "version", "recreate"] {
        let mut f = Fixture::new();
        let reference: bamboo_domain::SupervisorReference = f.runtime.block_on(async {
            let reference: bamboo_domain::SupervisorReference = (&f
                .store
                .get_or_create_default_supervisor("model")
                .await
                .unwrap())
                .into();
            let mut target = f.store.load_session("target").await.unwrap().unwrap();
            target.set_project_id_meta("project-a");
            target.metadata_version += 1;
            f.store.save_session(&target).await.unwrap();
            for (revision, mutation) in [
                (
                    0,
                    Mutation::ConfigureProjectScope {
                        allowed_projects: ["project-a".parse().unwrap()].into(),
                    },
                ),
                (
                    1,
                    Mutation::Attach {
                        target_session_id: "target".into(),
                    },
                ),
            ] {
                f.store
                    .mutate_supervisor_management(&SupervisorManagementRequest {
                        supervisor: reference.clone(),
                        expected_state_revision: revision,
                        mutation,
                    })
                    .await
                    .unwrap();
            }
            reference
        });
        // Fresh independent authority/index observations exist BEFORE the job parks.
        f.other_store = Arc::new(
            f.runtime
                .block_on(SessionStoreV2::new(f._temp.path().into()))
                .unwrap(),
        );
        let mut target = f
            .runtime
            .block_on(f.other_store.load_session("target"))
            .unwrap()
            .unwrap();
        let mut message = SessionMessageEnvelope::user_input("target", "authorized followup");
        message.source = SessionMessageSource::Session {
            session_id: reference.session_id.clone(),
        };
        message.kind = SessionMessageKind::PeerMessage;
        let job_ref = reference.clone();
        let job_message = message.clone();
        let mut parked = Parked::start(
            f.inbox.clone(),
            "maildir_replace",
            message_path(&f, &message, 1),
            move |inbox| async move {
                inbox
                    .deliver_supervisor_followup(&job_ref, &job_message)
                    .await
                    .map(|_| ())
            },
        );
        parked.shutdown();
        f.assert_physical_locks(true);
        assert!(!probe(
            &f.store
                .bamboo_home_dir()
                .join(".runtime-task-transactions.lock")
        ));
        for id in [reference.session_id.as_str(), "target"] {
            let digest = Sha256::digest(id.as_bytes());
            let hex = digest
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<_>>()
                .join("");
            assert!(!probe(
                &f.store
                    .bamboo_home_dir()
                    .join(".session-write-locks")
                    .join(format!("{hex}.lock"))
            ));
        }
        let store = f.other_store.clone();
        let mutate_ref = reference.clone();
        let mutation = f.runtime.spawn(async move {
            match change {
                "detach" | "scope" => {
                    let mutation = if change == "detach" {
                        Mutation::Detach {
                            target_session_id: "target".into(),
                        }
                    } else {
                        Mutation::ConfigureProjectScope {
                            allowed_projects: Default::default(),
                        }
                    };
                    store
                        .mutate_supervisor_management(&SupervisorManagementRequest {
                            supervisor: mutate_ref,
                            expected_state_revision: 2,
                            mutation,
                        })
                        .await
                        .unwrap();
                }
                "recreate" => {
                    store.delete_session("target").await.unwrap();
                    store
                        .recreate_root_session("target", "new model")
                        .await
                        .unwrap();
                }
                _ => {
                    if change == "project" {
                        target.set_project_id_meta("project-b");
                    }
                    target.metadata_version += 1;
                    store.save_session(&target).await.unwrap();
                }
            }
        });
        f.assert_waiting(&mutation);
        parked.barrier.release();
        f.finish(mutation);
        assert!(
            f.runtime
                .block_on(f.inbox.deliver_supervisor_followup(&reference, &message))
                .is_err(),
            "authority recheck must precede exact-id retry: {change}"
        );
    }
}
