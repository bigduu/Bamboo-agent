use super::*;

#[test]
fn compact_wait_routes_to_the_existing_native_wait_without_launching() {
    for (input, expected) in [
        (
            json!({"intent":"wait", "target":"logical-child"}),
            json!({"action":"wait", "child_session_ids":["logical-child"], "wait_for":"all"}),
        ),
        (
            json!({"intent":"wait"}),
            json!({"action":"wait", "wait_for":"all"}),
        ),
    ] {
        let normalized = normalize(input).expect("the compact caller must expose native wait");
        assert_eq!(normalized.args, expected);
        assert!(normalized.projection.is_some());
    }
}

#[test]
fn compact_wait_rejects_launch_reply_and_policy_arguments() {
    for extra in [
        json!({"message":"retry"}),
        json!({"message":" "}),
        json!({"role":"worker"}),
        json!({"model":"provider:model"}),
        json!({"reasoning_effort":"max"}),
        json!({"reply_to":"request"}),
        json!({"action":"wait"}),
        json!({"wait_for":"any"}),
        json!({"child_session_ids":["other"]}),
        json!({"target":" child"}),
    ] {
        let mut args = json!({"intent":"wait"});
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert!(normalize(args.clone()).is_err(), "{args}");
    }
    for sentinel in [Value::Null, json!("")] {
        let args = json!({"intent":"wait", "target":sentinel, "role":sentinel,
            "model":sentinel, "reasoning_effort":sentinel, "reply_to":sentinel,
            "message":sentinel});
        assert_eq!(
            normalize(args).unwrap().args,
            json!({"action":"wait", "wait_for":"all"})
        );
    }
}

#[test]
fn compact_wait_preserves_native_suspension_and_bounds_many_child_observations() {
    let native = json!({"status":"waiting", "waiting_on":4096,
        "child_session_ids":vec!["private-runtime-identity";4096],
        "already_terminal_child_ids":[], "runtime_control":"waiting_for_children",
        "wait_for":"all", "endpoint":"private-endpoint"});
    let ToolOutcome::Completed(result) = finish(
        Some(Projection::Wait),
        Ok(ToolOutcome::Completed(ToolResult {
            success: true,
            result: native.to_string(),
            images: vec![],
            display_preference: Some("runtime_control:waiting_for_children".into()),
        })),
    )
    .unwrap() else {
        panic!("wait observation must complete")
    };
    assert_eq!(
        serde_json::from_str::<Value>(&result.result).unwrap(),
        json!({
        "status":"waiting", "waiting_on":4096, "already_terminal_count":0,
        "runtime_control":"waiting_for_children", "wait_for":"all"})
    );
    assert_eq!(
        result.display_preference.as_deref(),
        Some("runtime_control:waiting_for_children")
    );
    assert!(serde_json::to_vec(&result).unwrap().len() < 512);
    let legacy =
        normalize(json!({"action":"wait", "child_session_ids":["legacy"], "wait_for":"any"}))
            .unwrap();
    assert_eq!(legacy.projection, None);
    assert_eq!(legacy.args["wait_for"], "any");
}
