mod background_outcomes {
    use super::*;
    use crate::runtime::runner::state_bridge::refresh_turn_boundary_with_inbox_for_run;

    async fn active_parent(
        store: &bamboo_storage::SessionStoreV2,
        supervisor: bool,
        ultra: bool,
    ) -> Session {
        let mut parent = if supervisor {
            let reference = store
                .get_or_create_default_supervisor("model")
                .await
                .unwrap();
            store
                .load_session(&reference.session_id)
                .await
                .unwrap()
                .unwrap()
        } else {
            Session::new("background-parent", "model")
        };
        parent.set_root_orchestration_only(ultra).unwrap();
        parent.add_message(Message::user(
            "Read the assigned fixture and report its evidence",
        ));
        let mut runtime = AgentRuntimeState::new("active-parent-run");
        runtime.status = AgentStatusState::Running;
        write_runtime_state(&mut parent, &runtime);
        store.save_session(&parent).await.unwrap();
        parent
    }

    async fn admit_at_safe_point(
        coordinator: &ChildCompletionCoordinator,
        inbox: &Arc<dyn SessionInboxPort>,
        parent: &mut Session,
        run_id: &str,
    ) -> u64 {
        let refresh = refresh_turn_boundary_with_inbox_for_run(
            parent,
            Some(coordinator.agent.storage()),
            Some(coordinator.agent.persistence()),
            Some(inbox),
            Some(run_id),
        )
        .await;
        assert!(refresh.admission_error.is_none(), "{refresh:?}");
        assert_eq!(
            refresh.merged, 1,
            "the next safe point must receive this outcome"
        );
        parent
            .session_inbox_admission()
            .unwrap()
            .last_admitted_sequence
    }

    #[tokio::test]
    async fn background_completion_is_consumed_before_summary_without_a_successor() {
        for supervisor in [false, true] {
            for ultra in [false, true] {
                for prose in [false, true] {
                    let (_home, store, inbox, coordinator, reservations, launches) =
                        completion_inbox_fixture().await;
                    let mut parent = active_parent(&store, supervisor, ultra).await;
                    let report = if prose {
                        "The fixture names apple and pear.".to_string()
                    } else {
                        typed_test_report().to_string()
                    };
                    let child = typed_test_child(&parent, "background-child", &report);
                    store.save_session(&child).await.unwrap();
                    let completion = typed_test_completion(&child);
                    let router = coordinator.agent.activation_router().unwrap();
                    router.set_inbox(inbox.clone());
                    let mut owner = router
                        .register_run(&parent.id, "parent-turn")
                        .await
                        .unwrap();

                    coordinator.on_child_completed(completion.clone()).await;
                    let pending = inbox
                        .pending_child_outcomes(&parent.id, &child.id)
                        .await
                        .unwrap();
                    assert_eq!(pending.len(), 1);
                    let envelope = pending[0].clone();
                    assert!(!envelope.child_outcome_waits_for_run("parent-turn"));
                    let generation =
                        admit_at_safe_point(&coordinator, &inbox, &mut parent, "parent-turn").await;
                    let notice = parent.messages.last().unwrap();
                    assert!(bamboo_domain::is_matching_session_message(
                        notice, &envelope
                    ));
                    assert_eq!(notice.content.contains("report_malformed"), prose);
                    assert!(notice.content.contains("Child typed result:"));
                    assert!(inbox.was_admitted(&parent.id, &envelope.id).await.unwrap());
                    parent.add_message(Message::assistant("Summary after the child outcome", None));
                    coordinator
                        .persistence
                        .save_runtime_session(&mut parent)
                        .await
                        .unwrap();

                    // A repeated callback after durable admission cannot recreate
                    // the notification or schedule an otherwise empty successor.
                    coordinator.on_child_completed(completion.clone()).await;
                    owner.begin_finalization().await;
                    assert!(owner.finish(generation).await.unwrap().is_none());
                    assert!(router.current_run_id(&parent.id).await.is_none());
                    coordinator.on_child_completed(completion).await;
                    assert_eq!(inbox.inspect(&parent.id).await.unwrap().pending, 0);
                    assert_eq!(reservations.load(Ordering::SeqCst), 0);
                    assert_eq!(launches.load(Ordering::SeqCst), 0);
                    let saved = store.load_session(&parent.id).await.unwrap().unwrap();
                    assert_eq!(
                        saved
                            .messages
                            .iter()
                            .filter(|message| bamboo_domain::is_matching_session_message(
                                message, &envelope
                            ))
                            .count(),
                        1
                    );
                    assert_eq!(
                        saved.messages.last().unwrap().content,
                        "Summary after the child outcome"
                    );
                    assert_eq!(saved.supervisor_incarnation_id().is_some(), supervisor);
                    assert_eq!(saved.root_orchestration_only_enabled(), ultra);
                }
            }
        }
    }

    #[tokio::test]
    async fn background_completion_remains_deliverable_after_inbox_reopen() {
        let (home, store, inbox, coordinator, reservations, launches) =
            completion_inbox_fixture().await;
        let mut parent = active_parent(&store, false, false).await;
        let completion =
            save_completion_test_child(&store, &parent.id, "reopened-child", "completed").await;
        let router = coordinator.agent.activation_router().unwrap();
        router.set_inbox(inbox.clone());
        let mut owner = router
            .register_run(&parent.id, "parent-turn")
            .await
            .unwrap();
        coordinator.on_child_completed(completion.clone()).await;

        let reopened_store = Arc::new(
            bamboo_storage::SessionStoreV2::new(home.path().to_path_buf())
                .await
                .unwrap(),
        );
        let reopened: Arc<dyn SessionInboxPort> = Arc::new(bamboo_storage::FileSessionInbox::new(
            reopened_store,
            Default::default(),
        ));
        let generation =
            admit_at_safe_point(&coordinator, &reopened, &mut parent, "parent-turn").await;
        coordinator.on_child_completed(completion).await;
        owner.begin_finalization().await;
        assert!(owner.finish(generation).await.unwrap().is_none());
        assert_eq!(reopened.inspect(&parent.id).await.unwrap().pending, 0);
        assert_eq!(reservations.load(Ordering::SeqCst), 0);
        assert_eq!(launches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn background_completion_after_last_safe_point_still_gets_one_successor() {
        let (_home, store, inbox, coordinator, reservations, launches) =
            completion_inbox_fixture().await;
        let mut parent = active_parent(&store, false, false).await;
        parent.add_message(Message::assistant("Earlier work is complete", None));
        store.save_session(&parent).await.unwrap();
        let completion =
            save_completion_test_child(&store, &parent.id, "late-background-child", "completed")
                .await;
        let router = coordinator.agent.activation_router().unwrap();
        router.set_inbox(inbox.clone());
        let mut owner = router
            .register_run(&parent.id, "parent-turn")
            .await
            .unwrap();
        owner.begin_finalization().await;

        coordinator.on_child_completed(completion.clone()).await;
        assert_eq!(reservations.load(Ordering::SeqCst), 0);
        owner.finish(0).await.unwrap();
        assert_eq!(reservations.load(Ordering::SeqCst), 1);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        let successor = router.current_run_id(&parent.id).await.unwrap();
        let generation = admit_at_safe_point(&coordinator, &inbox, &mut parent, &successor).await;
        assert!(parent
            .messages
            .last()
            .unwrap()
            .content
            .contains("late-background-child"));
        coordinator.on_child_completed(completion).await;
        router.begin_finalization(&parent.id, &successor).await;
        assert!(router
            .finish_finalization(&parent.id, &successor, generation)
            .await
            .unwrap()
            .is_none());
        assert_eq!(reservations.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn background_outcome_does_not_steal_an_unrelated_child_wait() {
        for policy in [
            ChildWaitPolicy::All,
            ChildWaitPolicy::Any,
            ChildWaitPolicy::FirstError,
        ] {
            let (_home, store, inbox, coordinator, reservations, launches) =
                completion_inbox_fixture().await;
            let parent_id = "unrelated-wait-parent";
            save_waiting_test_parent(
                &store,
                parent_id,
                vec!["still-running-child".into()],
                policy,
            )
            .await;
            save_completion_test_child(&store, parent_id, "still-running-child", "running").await;
            let completion =
                save_completion_test_child(&store, parent_id, "background-child", "completed")
                    .await;
            let before = store.load_session(parent_id).await.unwrap().unwrap();
            let router = coordinator.agent.activation_router().unwrap();
            router.set_inbox(inbox.clone());
            let mut owner = router.register_run(parent_id, "parent-turn").await.unwrap();

            coordinator.on_child_completed(completion).await;
            assert!(inbox
                .claim_for_turn(parent_id, 8, Some("parent-turn"))
                .await
                .unwrap()
                .is_empty());
            let after = store.load_session(parent_id).await.unwrap().unwrap();
            assert_eq!(read_runtime_state(&before), read_runtime_state(&after));
            assert_eq!(after.updated_at, before.updated_at);
            owner.begin_finalization().await;
            owner.finish(0).await.unwrap();
            assert_eq!(reservations.load(Ordering::SeqCst), 1);
            assert_eq!(launches.load(Ordering::SeqCst), 1);
            let successor = router.current_run_id(parent_id).await.unwrap();
            let claims = inbox
                .claim_for_turn(parent_id, 8, Some(&successor))
                .await
                .unwrap();
            assert_eq!(claims.len(), 1);
            assert_eq!(
                read_runtime_state(&store.load_session(parent_id).await.unwrap().unwrap()),
                read_runtime_state(&before)
            );
        }
    }

    #[tokio::test]
    async fn waited_completion_keeps_its_successor_fence_after_clearing_wait() {
        for (policy, status) in [
            (ChildWaitPolicy::All, "completed"),
            (ChildWaitPolicy::Any, "completed"),
            (ChildWaitPolicy::FirstError, "error"),
        ] {
            let (_home, store, inbox, coordinator, reservations, launches) =
                completion_inbox_fixture().await;
            let parent_id = "waited-parent";
            save_waiting_test_parent(&store, parent_id, vec!["waited-child".into()], policy).await;
            let completion =
                save_completion_test_child(&store, parent_id, "waited-child", status).await;
            let router = coordinator.agent.activation_router().unwrap();
            router.set_inbox(inbox.clone());
            let mut owner = router.register_run(parent_id, "parent-turn").await.unwrap();

            coordinator.on_child_completed(completion.clone()).await;
            // Retrying after the wait was cleared reuses the original durable
            // envelope; the new no-wait observation cannot remove its fence.
            coordinator.on_child_completed(completion).await;
            assert!(
                read_runtime_state(&store.load_session(parent_id).await.unwrap().unwrap())
                    .waiting_for_children
                    .is_none()
            );
            assert!(inbox
                .claim_for_turn(parent_id, 8, Some("parent-turn"))
                .await
                .unwrap()
                .is_empty());
            owner.begin_finalization().await;
            owner.finish(0).await.unwrap();
            assert_eq!(reservations.load(Ordering::SeqCst), 1);
            assert_eq!(launches.load(Ordering::SeqCst), 1);
            let successor = router.current_run_id(parent_id).await.unwrap();
            let claims = inbox
                .claim_for_turn(parent_id, 8, Some(&successor))
                .await
                .unwrap();
            assert_eq!(claims.len(), 1);
            assert!(claims[0]
                .envelope
                .child_outcome_waits_for_run("parent-turn"));
        }
    }

    #[tokio::test]
    async fn released_wait_keeps_late_siblings_on_the_same_owners_successor() {
        for (policy, first_status) in [
            (ChildWaitPolicy::Any, "completed"),
            (ChildWaitPolicy::FirstError, "error"),
        ] {
            let (_home, store, inbox, coordinator, reservations, launches) =
                completion_inbox_fixture().await;
            let parent_id = "released-wait-parent";
            save_waiting_test_parent(
                &store,
                parent_id,
                vec!["first-child".into(), "late-sibling".into()],
                policy,
            )
            .await;
            save_completion_test_child(&store, parent_id, "late-sibling", "running").await;
            let first =
                save_completion_test_child(&store, parent_id, "first-child", first_status).await;
            let router = coordinator.agent.activation_router().unwrap();
            router.set_inbox(inbox.clone());
            let mut owner = router.register_run(parent_id, "parent-turn").await.unwrap();

            coordinator.on_child_completed(first).await;
            assert!(
                read_runtime_state(&store.load_session(parent_id).await.unwrap().unwrap())
                    .waiting_for_children
                    .is_none()
            );
            // The durable wait is gone, but the current reasoning owner still
            // predates its release. A later sibling must not overtake the first
            // outcome merely because a fresh control-plane read now says None.
            let late =
                save_completion_test_child(&store, parent_id, "late-sibling", "completed").await;
            coordinator.on_child_completed(late).await;
            assert!(inbox
                .claim_for_turn(parent_id, 8, Some("parent-turn"))
                .await
                .unwrap()
                .is_empty());
            owner.begin_finalization().await;
            owner.finish(0).await.unwrap();
            assert_eq!(reservations.load(Ordering::SeqCst), 1);
            assert_eq!(launches.load(Ordering::SeqCst), 1);
            let successor = router.current_run_id(parent_id).await.unwrap();
            let claims = inbox
                .claim_for_turn(parent_id, 8, Some(&successor))
                .await
                .unwrap();
            assert_eq!(claims.len(), 2);
            assert!(claims
                .iter()
                .all(|claim| claim.envelope.child_outcome_waits_for_run("parent-turn")));
        }
    }

    #[tokio::test]
    async fn guardian_control_without_a_child_wait_still_waits_for_successor() {
        let (_home, store, inbox, coordinator, reservations, launches) =
            completion_inbox_fixture().await;
        let mut parent = active_parent(&store, false, false).await;
        let mut guardian = crate::runtime::guardian_state::ensure_guardian_state(&parent);
        guardian.record_spawn("guardian-child");
        write_guardian_state(&mut parent, guardian);
        store.save_session(&parent).await.unwrap();
        let mut child = Session::new_child("guardian-child", &parent.id, "model", "Guardian");
        child
            .metadata
            .insert("subagent_type".into(), "guardian".into());
        child.add_message(Message::assistant(
            r#"{"approve":true,"findings":[]}"#,
            None,
        ));
        child.set_last_run_status("completed");
        save_sourced_test_child(&store, &child).await;
        let completion = ChildCompletion {
            source: test_child_source(&store, &child.id).await,
            ..typed_test_completion(&child)
        };
        let router = coordinator.agent.activation_router().unwrap();
        router.set_inbox(inbox.clone());
        let mut owner = router
            .register_run(&parent.id, "parent-turn")
            .await
            .unwrap();

        coordinator.on_child_completed(completion).await;
        assert!(
            read_guardian_state(&store.load_session(&parent.id).await.unwrap().unwrap())
                .unwrap()
                .phase
                .is_reviewed()
        );
        assert!(inbox
            .claim_for_turn(&parent.id, 8, Some("parent-turn"))
            .await
            .unwrap()
            .is_empty());
        let background =
            save_completion_test_child(&store, &parent.id, "after-guardian-child", "completed")
                .await;
        coordinator.on_child_completed(background).await;
        assert!(inbox
            .claim_for_turn(&parent.id, 8, Some("parent-turn"))
            .await
            .unwrap()
            .is_empty());
        owner.begin_finalization().await;
        owner.finish(0).await.unwrap();
        assert_eq!(reservations.load(Ordering::SeqCst), 1);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        let successor = router.current_run_id(&parent.id).await.unwrap();
        let claims = inbox
            .claim_for_turn(&parent.id, 8, Some(&successor))
            .await
            .unwrap();
        assert_eq!(claims.len(), 2);
        assert!(claims
            .iter()
            .all(|claim| claim.envelope.child_outcome_waits_for_run("parent-turn")));
    }

    #[tokio::test]
    async fn background_source_rejection_has_no_delivery_or_parent_state_effect() {
        for foreign in [false, true] {
            let (_home, store, inbox, coordinator, reservations, launches) =
                completion_inbox_fixture().await;
            let parent = active_parent(&store, false, false).await;
            let child_parent = if foreign {
                "foreign-parent"
            } else {
                &parent.id
            };
            if foreign {
                store
                    .save_session(&Session::new(child_parent, "model"))
                    .await
                    .unwrap();
            }
            let mut completion =
                save_completion_test_child(&store, child_parent, "rejected-child", "completed")
                    .await;
            completion.parent_session_id = parent.id.clone();
            if !foreign {
                let mut successor = store.load_session("rejected-child").await.unwrap().unwrap();
                successor.add_message(Message::assistant("Newer child result", None));
                ChildCompletionSource::prepare(
                    &mut successor,
                    "newer-child-run",
                    &Default::default(),
                );
                store.save_session(&successor).await.unwrap();
            }
            let before =
                serde_json::to_value(store.load_session(&parent.id).await.unwrap().unwrap())
                    .unwrap();
            let router = coordinator.agent.activation_router().unwrap();
            router.set_inbox(inbox.clone());
            let mut owner = router
                .register_run(&parent.id, "parent-turn")
                .await
                .unwrap();

            coordinator.on_child_completed(completion).await;
            assert_eq!(inbox.inspect(&parent.id).await.unwrap().pending, 0);
            assert_eq!(
                serde_json::to_value(store.load_session(&parent.id).await.unwrap().unwrap())
                    .unwrap(),
                before
            );
            owner.begin_finalization().await;
            assert!(owner.finish(0).await.unwrap().is_none());
            assert_eq!(reservations.load(Ordering::SeqCst), 0);
            assert_eq!(launches.load(Ordering::SeqCst), 0);
        }
    }
}
