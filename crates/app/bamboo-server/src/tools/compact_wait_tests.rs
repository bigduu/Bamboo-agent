//! Compact caller acceptance against the existing durable Host wait port.
use super::*;

#[tokio::test]
async fn compact_wait_terminal_child_is_immediate_and_preserves_parent() {
    let h = build_test_harness_with_storage(None, None, true).await;
    let before = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    let result = invoke_completed(
        &h.tool,
        json!({"intent":"wait", "target":h.child_session_id}),
        subagent_test_ctx(&h.parent_session_id, "compact-terminal"),
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(
        payload,
        json!({"status":"no_active_children", "waiting_on":0, "already_terminal_count":1})
    );
    assert_ne!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children")
    );
    let after = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn compact_wait_registers_both_children_and_deduplicates_completion_inputs() {
    let h = build_test_harness_with_storage(None, None, true).await;
    seed_pending_children(&h).await;
    let result = invoke_completed(
        &h.tool,
        json!({"intent":"wait"}),
        subagent_test_ctx(&h.parent_session_id, "compact-wait-both"),
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    assert_eq!(payload["status"], "waiting");
    assert_eq!(payload["waiting_on"], 2);
    assert_eq!(payload["runtime_control"], "waiting_for_children");
    assert_eq!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children")
    );
    let armed = settled_views(&h, "compact wait before completion").await;
    let wait = waiting(&armed).unwrap();
    assert_eq!(wait.wait_for, ChildWaitPolicy::All);
    assert_eq!(wait.child_session_ids.len(), 2);
    assert_eq!(
        wait.registered_by_tool_call_id.as_deref(),
        Some("compact-wait-both")
    );
    assert_eq!(
        h.activation.calls.load(Ordering::SeqCst),
        0,
        "wait cannot launch a child"
    );
    let coordinator =
        coordinator_with_storage(&h, h.storage.clone(), h.adapter.persistence.clone()).await;
    let a = sealed_completion(&h).await;
    coordinator.on_child_completed(a.clone()).await;
    let partial = settled_views(&h, "compact wait first callback").await;
    assert!(waiting(&partial)
        .unwrap()
        .child_session_ids
        .contains(&"proof-child-b".into()));
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 1);
    let b = sealed_completion_for(&h, "proof-child-b").await;
    coordinator.on_child_completed(b.clone()).await;
    assert!(waiting(&settled_views(&h, "compact wait all completed").await).is_none());
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 2);
    // A retry may re-notify activation while these durable inputs remain
    // pending. It must not append another completion or resurrect the wait.
    let inbox = h.session_inbox.inspect(&h.parent_session_id).await.unwrap();
    coordinator.on_child_completed(a).await;
    coordinator.on_child_completed(b).await;
    assert_eq!(
        h.session_inbox
            .inspect(&h.parent_session_id)
            .await
            .unwrap()
            .generation,
        inbox.generation
    );
    assert_eq!(
        h.session_inbox
            .inspect(&h.parent_session_id)
            .await
            .unwrap()
            .pending,
        2
    );
    assert!(waiting(&settled_views(&h, "compact duplicate completion").await).is_none());
}

#[tokio::test]
async fn compact_wait_rejects_foreign_missing_and_suspension_disabled_without_parent_mutation() {
    let h = build_test_harness_with_storage(None, None, true).await;
    let foreign = Session::new_child("foreign-child", "other-root", "model", "foreign");
    h.storage.save_session(&foreign).await.unwrap();
    let before = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    for target in ["foreign-child", "missing", h.parent_session_id.as_str()] {
        assert!(invoke_completed(
            &h.tool,
            json!({"intent":"wait", "target":target}),
            subagent_test_ctx(&h.parent_session_id, "compact-invalid")
        )
        .await
        .is_err());
    }
    let error = bamboo_agent_core::tools::with_suspension_disabled(h.tool.invoke(
        json!({"intent":"wait", "target":h.child_session_id}),
        subagent_test_ctx(&h.parent_session_id, "compact-no-suspension"),
    ))
    .await
    .unwrap_err();
    assert!(error.to_string().contains("suspension_disabled"));
    let after = h
        .storage
        .load_session(&h.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    assert_eq!(h.activation.calls.load(Ordering::SeqCst), 0);
}
