//! Exercise saved model roles through the real tool and child admission path.
use super::*;
use bamboo_domain::ProviderModelRef;
use bamboo_llm::{Config, ProviderRegistry};

fn routing_registry() -> Arc<ProviderRegistry> {
    let providers = ["chat-a", "fast-b", "child-c"]
        .into_iter()
        .map(|id| (id.into(), Arc::new(NoopProvider) as Arc<dyn LLMProvider>))
        .collect();
    let registry = Arc::new(ProviderRegistry::new(providers, "chat-a".into()));
    registry.set_runtime_models(HashMap::from([
        (
            "chat-a".into(),
            vec!["gpt-5".into(), "explicit-child".into()],
        ),
        ("fast-b".into(), vec!["cheap-b".into()]),
        ("child-c".into(), vec!["child-model".into()]),
    ]));
    registry
}

async fn routing_harness(flag: Option<bool>) -> TestHarness {
    let slot = Arc::new(StdRwLock::new(None::<Arc<RwLock<Config>>>));
    let registry = routing_registry();
    let resolver_slot = slot.clone();
    let resolver: crate::tools::SubagentModelResolver = Arc::new(move |kind| {
        let config = resolver_slot.read().unwrap().as_ref().unwrap().clone();
        let registry = registry.clone();
        Box::pin(async move {
            let config = config.read().await;
            bamboo_engine::model_config_helper::resolve_subagent_model_ref(
                &config,
                config.effective_default_provider(),
                &registry,
                &kind,
            )
        })
    });
    let harness = build_test_harness_with_storage(Some(resolver), None, true).await;
    *slot.write().unwrap() = Some(harness.adapter.config.clone());
    let mut config = harness.adapter.config.write().await;
    config.provider = "chat-a".into();
    config.default_provider_instance = Some("chat-a".into());
    config.features = serde_json::from_value(match flag {
        Some(enabled) => json!({"provider_model_ref":enabled}),
        None => json!({}),
    })
    .unwrap();
    for (id, models) in [
        ("chat-a", vec!["gpt-5", "explicit-child"]),
        ("fast-b", vec!["cheap-b"]),
        ("child-c", vec!["child-model"]),
    ] {
        config.provider_instances.insert(id.into(), serde_json::from_value(json!({
            "provider_type":"openai", "enabled":true, "model":models[0], "runtime_models":models,
        })).unwrap());
    }
    config.defaults = Some(
        serde_json::from_value(json!({
            "chat":{"provider":"chat-a","model":"gpt-5"},
            "fast":{"provider":"fast-b","model":"cheap-b"},
            "sub_agent":{"provider":"child-c","model":"child-model"},
        }))
        .unwrap(),
    );
    drop(config);
    harness
}

fn create_args(harness: &TestHarness) -> serde_json::Value {
    json!({
        "action":"create", "title":"Synthetic model routing", "responsibility":"Validate routing",
        "prompt":"Return only the synthetic routing witness", "subagent_type":"coder",
        "workspace":harness.workspace_path, "auto_run":false,
    })
}

#[tokio::test]
async fn routing_contract_subagent_and_plan_reject_explicit_bad_roles_before_persist_or_launch() {
    for flag in [None, Some(false), Some(true)] {
        for bad in ["unknown", "disabled", "unadmitted"] {
            let harness = routing_harness(flag).await;
            {
                let mut config = harness.adapter.config.write().await;
                let target = match bad {
                    "unknown" => ProviderModelRef::new("missing-account", "child-model"),
                    "disabled" => {
                        config
                            .provider_instances
                            .get_mut("child-c")
                            .unwrap()
                            .enabled = false;
                        ProviderModelRef::new("child-c", "child-model")
                    }
                    _ => ProviderModelRef::new("child-c", "unadmitted-child"),
                };
                config.defaults.as_mut().unwrap().sub_agent = Some(target);
            }
            let before = harness
                .adapter
                .session_store
                .list_index_entries()
                .await
                .len();
            let error = invoke_completed(
                &harness.tool,
                create_args(&harness),
                subagent_test_ctx(&harness.parent_session_id, "synthetic-rejected-child-role"),
            )
            .await
            .expect_err("a rejected role must not inherit the parent's valid model");
            assert!(error.to_string().contains("not admitted"), "{bad}: {error}");
            assert_eq!(
                harness
                    .adapter
                    .session_store
                    .list_index_entries()
                    .await
                    .len(),
                before
            );
            assert!(harness.agent_runners.read().await.is_empty());
            let plan = PlanTool::new(harness.adapter.clone(), harness.adapter.clone());
            let error = invoke_plan_completed(
                &plan, json!({"task":"Inspect synthetic routing only", "workspace":harness.workspace_path}),
                subagent_test_ctx(&harness.parent_session_id, "synthetic-rejected-planner-role"),
            ).await.expect_err("Plan must retain the same rejected explicit role");
            assert!(error.to_string().contains("not admitted"), "{bad}: {error}");
            assert_eq!(
                harness
                    .adapter
                    .session_store
                    .list_index_entries()
                    .await
                    .len(),
                before
            );
            assert!(harness.agent_runners.read().await.is_empty());
        }
    }
}

#[tokio::test]
async fn routing_contract_subagent_precedence_and_hot_changes_preserve_exact_refs() {
    for flag in [None, Some(false), Some(true)] {
        let harness = routing_harness(flag).await;
        for (step, expected) in [
            (0, ProviderModelRef::new("child-c", "child-model")),
            (1, ProviderModelRef::new("fast-b", "cheap-b")),
            (2, ProviderModelRef::new("chat-a", "gpt-5")),
            (3, ProviderModelRef::new("child-c", "child-model")),
        ] {
            {
                let mut config = harness.adapter.config.write().await;
                let defaults = config.defaults.as_mut().unwrap();
                if step == 1 {
                    defaults.sub_agent = None;
                }
                if step == 2 {
                    defaults.fast = None;
                }
                if step == 3 {
                    defaults
                        .subagent_models
                        .insert("coder".into(), expected.clone());
                }
            }
            let mut args = create_args(&harness);
            args["prompt"] = json!(format!("Synthetic routing witness for step {step}"));
            args["title"] = json!(format!("Synthetic routing step {step}"));
            let result = invoke_completed(
                &harness.tool,
                args,
                subagent_test_ctx(&harness.parent_session_id, "synthetic-valid-child-role"),
            )
            .await
            .unwrap();
            let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
            let child = harness
                .storage
                .load_session(payload["child_session_id"].as_str().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(child.model_ref, Some(expected));
        }
    }
}

#[tokio::test]
async fn routing_contract_saved_provider_alias_does_not_select_another_account() {
    for flag in [None, Some(false), Some(true)] {
        let harness = routing_harness(flag).await;
        {
            let mut config = harness.adapter.config.write().await;
            config.default_provider_instance = Some("fast-b".into());
            config.defaults.as_mut().unwrap().sub_agent =
                Some(ProviderModelRef::new("openai", "cheap-b"));
        }
        let registry = routing_registry();
        let router = bamboo_llm::ProviderModelRouter::new(registry);
        assert!(router
            .route(&ProviderModelRef::new("openai", "cheap-b"))
            .is_err());
        assert!(router
            .route(&ProviderModelRef::new("fast-b", "cheap-b"))
            .is_ok());

        for hybrid_legacy in [false, true] {
            if hybrid_legacy {
                let mut config = harness.adapter.config.write().await;
                let mut legacy = bamboo_config::OpenAIConfig::default();
                legacy.model = Some("cheap-b".into());
                config.providers_mut().openai = Some(legacy);
            }
            let before = harness
                .adapter
                .session_store
                .list_index_entries()
                .await
                .len();
            let error = invoke_completed(
                &harness.tool,
                create_args(&harness),
                subagent_test_ctx(&harness.parent_session_id, "synthetic-provider-alias"),
            )
            .await
            .expect_err("a saved alias must not silently select a different provider account");
            assert!(error.to_string().contains("not admitted"), "{error}");
            assert_eq!(
                harness
                    .adapter
                    .session_store
                    .list_index_entries()
                    .await
                    .len(),
                before
            );
            assert!(harness.agent_runners.read().await.is_empty());
            let plan = PlanTool::new(harness.adapter.clone(), harness.adapter.clone());
            let error = invoke_plan_completed(
                &plan,
                json!({"task":"Inspect synthetic routing only", "workspace":harness.workspace_path}),
                subagent_test_ctx(&harness.parent_session_id, "synthetic-planner-provider-alias"),
            )
            .await
            .expect_err("Plan must not use a different provider account for the alias");
            assert!(error.to_string().contains("not admitted"), "{error}");
            assert_eq!(
                harness
                    .adapter
                    .session_store
                    .list_index_entries()
                    .await
                    .len(),
                before
            );
            assert!(harness.agent_runners.read().await.is_empty());
        }

        harness
            .adapter
            .config
            .write()
            .await
            .defaults
            .as_mut()
            .unwrap()
            .sub_agent = Some(
            ProviderModelRef::new("fast-b", "cheap-b")
                .with_reasoning_effort(bamboo_domain::ReasoningEffort::Low),
        );
        let result = invoke_completed(
            &harness.tool,
            create_args(&harness),
            subagent_test_ctx(&harness.parent_session_id, "synthetic-exact-provider"),
        )
        .await
        .expect("the exact admitted instance should still create a child");
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        let child = harness
            .storage
            .load_session(payload["child_session_id"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            child.model_ref,
            Some(
                ProviderModelRef::new("fast-b", "cheap-b")
                    .with_reasoning_effort(bamboo_domain::ReasoningEffort::Low)
            )
        );
        assert_eq!(
            child.reasoning_effort,
            Some(bamboo_domain::ReasoningEffort::Low)
        );
    }
}

#[tokio::test]
async fn routing_contract_hybrid_legacy_alias_requires_the_effective_default() {
    for flag in [None, Some(false), Some(true)] {
        let harness = routing_harness(flag).await;
        {
            let mut config = harness.adapter.config.write().await;
            config.provider_instances.clear();
            config.provider_instances.insert(
                "anthropic-work".into(),
                serde_json::from_value(json!({
                    "provider_type":"anthropic", "enabled":true,
                    "model":"other-model", "runtime_models":["other-model"]
                }))
                .unwrap(),
            );
            config.default_provider_instance = Some("anthropic-work".into());
            let mut legacy = bamboo_config::OpenAIConfig::default();
            legacy.model = Some("legacy-model".into());
            config.providers_mut().openai = Some(legacy);
            config.defaults.as_mut().unwrap().sub_agent =
                Some(ProviderModelRef::new("openai", "legacy-model"));
        }
        let before = harness
            .adapter
            .session_store
            .list_index_entries()
            .await
            .len();
        let error = invoke_completed(
            &harness.tool,
            create_args(&harness),
            subagent_test_ctx(&harness.parent_session_id, "synthetic-stale-legacy-alias"),
        )
        .await
        .expect_err("a nondefault legacy alias must not be persisted as a live route");
        assert!(error.to_string().contains("not admitted"), "{error}");
        assert_eq!(
            harness
                .adapter
                .session_store
                .list_index_entries()
                .await
                .len(),
            before
        );
        assert!(harness.agent_runners.read().await.is_empty());

        {
            let mut config = harness.adapter.config.write().await;
            config.default_provider_instance = None;
            config.provider = "openai".into();
        }
        let result = invoke_completed(
            &harness.tool,
            create_args(&harness),
            subagent_test_ctx(&harness.parent_session_id, "synthetic-live-legacy-alias"),
        )
        .await
        .expect("the effective legacy default remains an admitted routing key");
        let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
        let child = harness
            .storage
            .load_session(payload["child_session_id"].as_str().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            child.model_ref,
            Some(ProviderModelRef::new("openai", "legacy-model"))
        );
    }
}

#[tokio::test]
async fn routing_contract_explicit_child_override_wins_invalid_default_role() {
    let harness = routing_harness(Some(false)).await;
    harness
        .adapter
        .config
        .write()
        .await
        .defaults
        .as_mut()
        .unwrap()
        .sub_agent = Some(ProviderModelRef::new("missing-account", "bad-model"));
    let mut args = create_args(&harness);
    args["model"] = json!("chat-a:explicit-child");
    let result = invoke_completed(
        &harness.tool,
        args,
        subagent_test_ctx(&harness.parent_session_id, "synthetic-explicit-child-model"),
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
    let child = harness
        .storage
        .load_session(payload["child_session_id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        child.model_ref,
        Some(ProviderModelRef::new("chat-a", "explicit-child"))
    );
}

#[tokio::test]
async fn routing_contract_plan_rejects_unknown_explicit_type_role_before_parent_inheritance() {
    let harness = routing_harness(None).await;
    harness
        .adapter
        .config
        .write()
        .await
        .defaults
        .as_mut()
        .unwrap()
        .subagent_models
        .insert(
            "planner".into(),
            ProviderModelRef::new("missing-planner-account", "unadmitted-planner"),
        );
    let before = harness
        .adapter
        .session_store
        .list_index_entries()
        .await
        .len();
    let plan = PlanTool::new(harness.adapter.clone(), harness.adapter.clone());
    let result = invoke_plan_completed(
        &plan,
        json!({"task":"Inspect synthetic routing only", "workspace":harness.workspace_path}),
        subagent_test_ctx(&harness.parent_session_id, "synthetic-unknown-planner-role"),
    )
    .await;
    let error = result
        .expect_err("the explicitly rejected planner route must not inherit the parent's model");
    assert!(error.to_string().contains("not admitted"), "{error}");
    assert_eq!(
        harness
            .adapter
            .session_store
            .list_index_entries()
            .await
            .len(),
        before
    );
    assert!(harness.agent_runners.read().await.is_empty());
}

#[tokio::test]
async fn routing_contract_legacy_instance_normalizes_and_falls_back_before_child_admission() {
    for flag in [None, Some(false), Some(true)] {
        let harness = routing_harness(flag).await;
        for (fast, default_model, expected_model) in [
            (Some(""), "child-model", "child-model"),
            (Some("  "), "child-model", "child-model"),
            (Some("  child-fast  "), "child-model", "child-fast"),
            (None, "  child-model  ", "child-model"),
        ] {
            {
                let mut config = harness.adapter.config.write().await;
                config.defaults = None;
                config.default_provider_instance = Some("child-c".into());
                let instance = config.provider_instances.get_mut("child-c").unwrap();
                instance.fast_model = fast.map(str::to_string);
                instance.model = Some(default_model.into());
                instance.extra.insert(
                    "runtime_models".into(),
                    json!(["child-model", "child-fast"]),
                );
            }
            let result = invoke_completed(
                &harness.tool,
                create_args(&harness),
                subagent_test_ctx(&harness.parent_session_id, "synthetic-legacy-fallback"),
            )
            .await
            .expect("the configured legacy child model should be admitted");
            let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
            let child = harness
                .storage
                .load_session(payload["child_session_id"].as_str().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(child.model, expected_model, "flag {flag:?}, fast {fast:?}");
            assert_eq!(
                child.model_ref,
                Some(ProviderModelRef::new("child-c", expected_model)),
                "flag {flag:?}, fast {fast:?}"
            );
        }
    }
}

#[tokio::test]
async fn routing_contract_legacy_provider_blank_fast_inherits_parent_but_keeps_real_selections() {
    for flag in [None, Some(false), Some(true)] {
        let harness = routing_harness(flag).await;
        {
            let mut config = harness.adapter.config.write().await;
            config.defaults = None;
            config.provider_instances.clear();
            config.default_provider_instance = None;
            config.provider = "openai".into();
            *config.providers_mut() = Default::default();
            let mut openai = bamboo_config::OpenAIConfig::default();
            openai.model = Some("legacy-default".into());
            openai.extra.insert(
                "runtime_models".into(),
                json!(["gpt-5", "legacy-default", "legacy-fast"]),
            );
            config.providers_mut().openai = Some(openai);
        }
        for (fast, expected_model, expected_ref) in [
            (Some(""), "gpt-5", None),
            (Some("  "), "gpt-5", None),
            (None, "legacy-default", Some("legacy-default")),
            (Some("legacy-fast"), "legacy-fast", Some("legacy-fast")),
        ] {
            harness
                .adapter
                .config
                .write()
                .await
                .providers_mut()
                .openai
                .as_mut()
                .unwrap()
                .fast_model = fast.map(str::to_string);
            let result = invoke_completed(
                &harness.tool,
                create_args(&harness),
                subagent_test_ctx(&harness.parent_session_id, "synthetic-legacy-provider"),
            )
            .await
            .expect("the legacy selection or inherited parent must be admitted");
            let payload: serde_json::Value = serde_json::from_str(&result.result).unwrap();
            let child = harness
                .storage
                .load_session(payload["child_session_id"].as_str().unwrap())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(child.model, expected_model, "flag {flag:?}, fast {fast:?}");
            assert_eq!(
                child.model_ref,
                expected_ref.map(|model| ProviderModelRef::new("openai", model)),
                "flag {flag:?}, fast {fast:?}"
            );
        }
        harness
            .adapter
            .config
            .write()
            .await
            .providers_mut()
            .openai
            .as_mut()
            .unwrap()
            .fast_model = Some("unadmitted-legacy".into());
        let before = harness
            .adapter
            .session_store
            .list_index_entries()
            .await
            .len();
        let error = invoke_completed(
            &harness.tool,
            create_args(&harness),
            subagent_test_ctx(
                &harness.parent_session_id,
                "synthetic-unadmitted-legacy-provider",
            ),
        )
        .await
        .expect_err("an explicit unadmitted legacy model must not inherit the parent");
        assert!(error.to_string().contains("not admitted"), "{error}");
        assert_eq!(
            harness
                .adapter
                .session_store
                .list_index_entries()
                .await
                .len(),
            before
        );
        assert!(harness.agent_runners.read().await.is_empty());
    }
}

#[tokio::test]
async fn routing_contract_disabled_legacy_instance_does_not_inherit_parent() {
    let harness = routing_harness(Some(false)).await;
    {
        let mut config = harness.adapter.config.write().await;
        config.defaults = None;
        config.default_provider_instance = Some("child-c".into());
        let instance = config.provider_instances.get_mut("child-c").unwrap();
        instance.enabled = false;
        instance.fast_model = Some("child-model".into());
    }
    let before = harness
        .adapter
        .session_store
        .list_index_entries()
        .await
        .len();
    let error = invoke_completed(
        &harness.tool,
        create_args(&harness),
        subagent_test_ctx(
            &harness.parent_session_id,
            "synthetic-disabled-legacy-child",
        ),
    )
    .await
    .expect_err("a disabled configured instance must not inherit the parent's model");
    assert!(error.to_string().contains("not admitted"), "{error}");
    assert_eq!(
        harness
            .adapter
            .session_store
            .list_index_entries()
            .await
            .len(),
        before
    );
    assert!(harness.agent_runners.read().await.is_empty());
}

#[tokio::test]
async fn routing_contract_legacy_child_model_rejection_is_not_parent_inheritance() {
    let harness = routing_harness(Some(false)).await;
    {
        let mut config = harness.adapter.config.write().await;
        config.defaults = None;
        config
            .provider_instances
            .get_mut("chat-a")
            .unwrap()
            .fast_model = Some("unadmitted-legacy-fast".into());
    }
    let before = harness
        .adapter
        .session_store
        .list_index_entries()
        .await
        .len();
    let error = invoke_completed(
        &harness.tool,
        create_args(&harness),
        subagent_test_ctx(
            &harness.parent_session_id,
            "synthetic-legacy-bad-child-model",
        ),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("unadmitted-legacy-fast"),
        "{error}"
    );
    assert_eq!(
        harness
            .adapter
            .session_store
            .list_index_entries()
            .await
            .len(),
        before
    );
    assert!(harness.agent_runners.read().await.is_empty());
}

#[tokio::test]
async fn routing_contract_guardian_override_keeps_parent_account_and_child_admission() {
    use bamboo_engine::GuardianSpawner;
    let harness = routing_harness(Some(true)).await;
    let mut parent = harness
        .storage
        .load_session(&harness.parent_session_id)
        .await
        .unwrap()
        .unwrap();
    parent.model_ref = Some(ProviderModelRef::new("chat-a", "gpt-5"));
    parent.set_workspace_path_meta(harness.workspace_path.to_string_lossy());
    harness.storage.save_session(&parent).await.unwrap();
    harness
        .adapter
        .session_store
        .save_session(&parent)
        .await
        .unwrap();
    let before = harness
        .adapter
        .session_store
        .list_index_entries()
        .await
        .len();
    let error = harness
        .adapter
        .spawn_guardian_review(
            &parent,
            "Synthetic routing review".into(),
            "unadmitted-guardian-model".into(),
            None,
        )
        .await
        .unwrap_err();
    assert!(error.contains("not admitted"), "{error}");
    assert_eq!(
        harness
            .adapter
            .session_store
            .list_index_entries()
            .await
            .len(),
        before
    );
    let child_id = harness
        .adapter
        .spawn_guardian_review(
            &parent,
            "Synthetic routing review".into(),
            "explicit-child".into(),
            None,
        )
        .await
        .unwrap();
    let child = harness
        .storage
        .load_session(&child_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        child.model_ref,
        Some(ProviderModelRef::new("chat-a", "explicit-child"))
    );
}
