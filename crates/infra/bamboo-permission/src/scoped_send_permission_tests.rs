use crate::*;
use serde_json::json;

fn send() -> PermissionEvaluation {
    PermissionEvaluation {
        request_id: "message-call".into(),
        session_id: "sender".into(),
        workspace_path: Some("/workspace".into()),
        tool_name: "session_message".into(),
        tool_args: json!({
            "action":"send", "target_session_id":"private-target",
            "operation_id":"private-operation", "message":"private-message",
            "delivery_intent":"trigger_turn",
        }),
        permission_type: PermissionType::ExecuteCommand,
        resource: "session_message send".into(),
        operation_summary: "send collaboration input".into(),
        risk_level: RiskLevel::High,
        bypass_requested: false,
        auto_approve_requested: false,
        platform_hard_deny: None,
        consume_once: true,
        supported_decisions: PermissionDecisionKind::all_supported(),
    }
}

fn legacy_rule(effect: PermissionRuleEffect) -> DurablePermissionRule {
    DurablePermissionRule {
        id: "legacy-followup-rule".into(),
        permission_type: PermissionType::ExecuteCommand,
        effect,
        scope: PermissionRuleScope::Global,
        workspace_path: None,
        matcher: PermissionMatcher {
            id: "exact-legacy-followup".into(),
            kind: PermissionMatcherKind::ExactResource,
            value: "session_control followup private-target".into(),
        },
        source: PermissionRuleSource::User,
        expires_at: None,
    }
}

#[test]
fn legacy_deny_sources_precede_once_disabled_auto_and_bypass_without_consuming_it() {
    for source in ["durable", "session", "typed-session", "whitelist"] {
        for posture in ["default", "disabled", "auto", "bypass"] {
            let config = PermissionConfig::new();
            let mut input = send();
            match posture {
                "disabled" => config.set_enabled(false),
                "auto" => input.auto_approve_requested = true,
                "bypass" => input.bypass_requested = true,
                _ => {}
            }
            match source {
                "durable" => config
                    .add_durable_rule(legacy_rule(PermissionRuleEffect::Deny))
                    .unwrap(),
                "session" => config.deny_scoped_session_permission(
                    "sender",
                    PermissionType::ExecuteCommand,
                    "session_control followup private-target",
                ),
                "typed-session" => config
                    .deny_typed_scoped_session_permission(
                        "sender",
                        PermissionType::ExecuteCommand,
                        legacy_rule(PermissionRuleEffect::Deny).matcher,
                    )
                    .unwrap(),
                _ => config.add_rule(PermissionRule::new(
                    PermissionType::ExecuteCommand,
                    "session_control followup private-target",
                    false,
                )),
            }
            config.grant_once(
                "sender",
                "message-call",
                PermissionType::ExecuteCommand,
                "session_message send".into(),
            );
            let outcome = config.evaluate_framework_scoped_session_send(input.clone());
            assert!(
                matches!(&outcome, PermissionOutcome::Deny { reason, .. }
                if reason.code == PermissionReasonCode::ExplicitDeny),
                "{source}/{posture}"
            );
            assert!(!format!("{outcome:?}").contains("private-target"));
            // The old narrowing gate must run before consuming the actual once
            // receipt. A second evaluate is a test observation, not production.
            assert!(matches!(
                config.evaluate(input),
                PermissionOutcome::Allow {
                    source: PermissionDecisionSource::OneShot,
                    ..
                }
            ));
        }
    }
}

#[test]
fn legacy_ask_keeps_real_owner_and_private_resource_before_remembered_new_allow() {
    for source in ["ask-rule", "typed-rule"] {
        let config = PermissionConfig::new();
        config.set_enabled(false);
        config.add_rule(PermissionRule::new(
            PermissionType::ExecuteCommand,
            "session_message send",
            true,
        ));
        if source == "ask-rule" {
            config.set_ask_rules(["session_control".into()]);
        } else {
            config
                .add_durable_rule(legacy_rule(PermissionRuleEffect::AlwaysAsk))
                .unwrap();
        }
        let mut input = send();
        input.bypass_requested = true;
        let original = input.clone();
        let PermissionOutcome::Ask(request) = config.evaluate_framework_scoped_session_send(input)
        else {
            panic!("legacy ask must narrow disabled/bypass/new allow: {source}");
        };
        assert_eq!(request.tool_name, "session_message");
        assert_eq!(request.resource, "session_message send");
        assert_eq!(request.request_id, original.request_id);
        assert_eq!(
            request.reason_code,
            PermissionReasonCode::ConfiguredAlwaysAsk
        );
        let wire = serde_json::to_string(&request).unwrap();
        for private in ["private-target", "private-operation", "private-message"] {
            assert!(!wire.contains(private));
        }
        // Preserve the existing Auto no-prompt posture after explicit Deny.
        let mut auto = original;
        auto.auto_approve_requested = true;
        assert!(matches!(
            config.evaluate_framework_scoped_session_send(auto),
            PermissionOutcome::Allow {
                source: PermissionDecisionSource::Auto,
                ..
            }
        ));
    }
}

#[test]
fn legacy_allow_and_legacy_once_never_grant_the_new_sender() {
    let config = PermissionConfig::new();
    config.set_mode(PermissionMode::DontAsk);
    config
        .add_durable_rule(legacy_rule(PermissionRuleEffect::Allow))
        .unwrap();
    config.add_rule(PermissionRule::new(
        PermissionType::ExecuteCommand,
        "session_control followup *",
        true,
    ));
    config.grant_scoped_session_permission(
        "sender",
        PermissionType::ExecuteCommand,
        "session_control followup *",
    );
    config.grant_once(
        "sender",
        "message-call",
        PermissionType::ExecuteCommand,
        "session_control followup private-target".into(),
    );
    assert!(matches!(
        config.evaluate_framework_scoped_session_send(send()),
        PermissionOutcome::Deny { .. }
    ));
    assert!(config.consume_once(
        "sender",
        "message-call",
        PermissionType::ExecuteCommand,
        "session_control followup private-target"
    ));
}

#[tokio::test]
async fn exact_typed_generation_is_consumed_once_by_the_single_new_owner_evaluation() {
    let config = PermissionConfig::new();
    config.set_ask_rules(["session_control".into()]);
    config
        .grant_once_for_generation(
            "sender",
            "message-call",
            "generation-original",
            PermissionType::ExecuteCommand,
            "session_message send".into(),
        )
        .unwrap();
    with_permission_replay_generation("sender", "message-call", Some("generation-other"), async {
        assert!(matches!(
            config.evaluate_framework_scoped_session_send(send()),
            PermissionOutcome::Ask(_)
        ));
    })
    .await;
    with_permission_replay_generation("sender", "message-call", Some("generation-original"), async {
        assert!(matches!(config.evaluate_framework_scoped_session_send(send()), PermissionOutcome::Allow {
            source: PermissionDecisionSource::OneShot, ..
        }));
        assert!(matches!(config.evaluate_framework_scoped_session_send(send()), PermissionOutcome::Ask(request)
            if request.tool_name == "session_message" && request.resource == "session_message send"));
    }).await;
}

#[test]
fn invalid_or_changed_projection_never_reinterprets_another_owner_or_resource() {
    let config = PermissionConfig::new();
    let original = send();
    let (_, private_context) =
        tool_permissions::framework_scoped_session_send_restriction(&original.tool_args).unwrap();
    assert_eq!(
        private_context.resource,
        "session_control followup private-target"
    );
    for field in ["tool", "resource", "target", "action"] {
        let mut changed = original.clone();
        changed.auto_approve_requested = true;
        match field {
            "tool" => changed.tool_name = "custom::session_message".into(),
            "resource" => changed.resource = private_context.resource.clone(),
            "target" => changed.tool_args["target_session_id"] = json!("../unsafe"),
            _ => changed.tool_args["action"] = json!("reply"),
        }
        assert!(
            matches!(config.evaluate_framework_scoped_session_send(changed), PermissionOutcome::Deny {
            reason, ..
        } if reason.code == PermissionReasonCode::PlatformHardDeny)
        );
    }
    assert_eq!(original, send());
}
