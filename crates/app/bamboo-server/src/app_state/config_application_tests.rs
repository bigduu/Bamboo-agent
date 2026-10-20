use super::*;

fn consumer<'a>(report: &'a Value, id: &str) -> &'a Value {
    report["consumers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|consumer| consumer["id"] == id)
        .unwrap()
}

#[test]
fn user_named_instances_servers_and_environment_fields_remain_runtime_inputs() {
    let mut config = Config::default();
    config.provider_instances = serde_json::from_value(
        json!({"team-encrypted": {"provider_type":"openai", "api_key":"old-key"}}),
    )
    .unwrap();
    config.mcp = serde_json::from_value(
        json!({"svc-encrypted": {"command":"true", "env":{"TOKEN_encrypted":"old-value"}}}),
    )
    .unwrap();
    let before_provider = input_digest(ConfigConsumer::ProviderRuntime, &config);
    let before_mcp = input_digest(ConfigConsumer::McpRuntime, &config);
    config
        .provider_instances
        .get_mut("team-encrypted")
        .unwrap()
        .api_key = "new-key".into();
    if let bamboo_mcp::TransportConfig::Stdio(stdio) = &mut config.mcp.servers[0].transport {
        stdio
            .env
            .insert("TOKEN_encrypted".into(), "new-value".into());
    }
    assert!(before_provider != input_digest(ConfigConsumer::ProviderRuntime, &config));
    assert!(before_mcp != input_digest(ConfigConsumer::McpRuntime, &config));
}

#[test]
fn mcp_credential_proof_excludes_topology_but_tracks_referenced_values() {
    let mut config = Config::default();
    let empty_credentials = credential_digest(ConfigConsumer::McpRuntime, &config);
    let empty_inputs = input_digest(ConfigConsumer::McpRuntime, &config);
    config.mcp = serde_json::from_value(json!({
        "stdio": {"command": "true", "enabled": false, "env": {"EMPTY": ""}},
        "http": {"url": "http://127.0.0.1:19879/mcp", "enabled": false,
            "headers": {"X-Optional": ""}}
    }))
    .unwrap();
    assert!(credential_digest(ConfigConsumer::McpRuntime, &config) == empty_credentials);
    assert!(input_digest(ConfigConsumer::McpRuntime, &config) != empty_inputs);

    for server in &mut config.mcp.servers {
        match &mut server.transport {
            bamboo_mcp::TransportConfig::Stdio(stdio) => {
                stdio
                    .env
                    .insert("TOKEN".into(), "stdio-old-sentinel".into());
                stdio
                    .env_credential_refs
                    .insert("TOKEN".into(), "mcp.stdio.token".into());
            }
            bamboo_mcp::TransportConfig::StreamableHttp(http) => {
                http.headers[0].credential_ref = Some("mcp.http.header".into());
                http.headers[0].value = "http-old-sentinel".into();
            }
        }
    }
    assert!(credential_digest(ConfigConsumer::McpRuntime, &config) != empty_credentials);
    for index in 0..config.mcp.servers.len() {
        let before = credential_digest(ConfigConsumer::McpRuntime, &config);
        match &mut config.mcp.servers[index].transport {
            bamboo_mcp::TransportConfig::Stdio(stdio) => {
                stdio
                    .env
                    .insert("TOKEN".into(), "stdio-new-sentinel".into());
            }
            bamboo_mcp::TransportConfig::StreamableHttp(http) => {
                http.headers[0].value = "http-new-sentinel".into();
            }
        }
        assert!(credential_digest(ConfigConsumer::McpRuntime, &config) != before);
    }
}

#[test]
fn legacy_and_uncaptured_consumers_do_not_claim_application() {
    let config = Config::default();
    let state = ConfigApplicationState::default();
    let report =
        serde_json::to_value(state.diagnostics(None, &config, &config, &BTreeSet::new())).unwrap();
    for status in report["consumers"].as_array().unwrap() {
        assert_eq!(status["state"], "unknown");
        assert_eq!(status["applied_dependencies"], Value::Null);
    }
}

#[test]
fn object_order_does_not_change_private_input_comparison() {
    let left = json!({"b": [1, 2], "a": {"x": true, "y": "text"}});
    let right: Value = serde_json::from_str(r#"{"a":{"y":"text","x":true},"b":[1,2]}"#).unwrap();
    let mut left_hash = Sha256::new();
    let mut right_hash = Sha256::new();
    hash_value(&mut left_hash, &left);
    hash_value(&mut right_hash, &right);
    assert_eq!(left_hash.finalize(), right_hash.finalize());
}

thread_local! {
    static APPLICATION_THREAD_KEY: std::cell::RefCell<Option<bamboo_config::encryption::TestKeyGuard>> =
        const { std::cell::RefCell::new(None) };
}

// Exact transactions decrypt in spawn_blocking. Give this test's owned pool
// the same synthetic thread-local key as its async driver, without touching
// process environment or the user's encryption key.
fn with_application_runtime(test: impl std::future::Future<Output = ()>) {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x71; 32]);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .on_thread_start(|| {
            APPLICATION_THREAD_KEY.with(|key| {
                *key.borrow_mut() = Some(bamboo_config::encryption::set_test_encryption_key(
                    [0x71; 32],
                ));
            });
        })
        .on_thread_stop(|| {
            APPLICATION_THREAD_KEY.with(|key| {
                key.borrow_mut().take();
            });
        })
        .build()
        .unwrap()
        .block_on(test);
}

async fn application_state() -> (tempfile::TempDir, AppState) {
    application_state_with_core(json!({"provider":"openai"})).await
}

async fn application_state_with_core(document: Value) -> (tempfile::TempDir, AppState) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("config.json"),
        serde_json::to_vec(&document).unwrap(),
    )
    .unwrap();
    std::fs::write(dir.path().join("providers.json"), serde_json::to_vec(&json!({"openai": {
        "api_key_encrypted": bamboo_config::encryption::encrypt("application-secret-sentinel").unwrap(),
        "base_url": "http://127.0.0.1:9876/v1", "model": "old-model"
    }})).unwrap()).unwrap();
    let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
    state.stop_config_watcher_for_test();
    let mcp = application_section(&state, SectionId::Mcp);
    state
        .put_mcp_section(mcp.revision, bamboo_mcp::McpConfig::default())
        .await
        .unwrap();
    (dir, state)
}

fn application_section(state: &AppState, id: SectionId) -> bamboo_config::SectionEnvelope<Value> {
    state
        .config_facade
        .as_ref()
        .unwrap()
        .registry()
        .envelope_value(id)
        .unwrap()
}

async fn application_report(state: &AppState) -> Value {
    let _io = state.config_io_lock.lock().await;
    serde_json::to_value(state.config_application_diagnostics_locked().await).unwrap()
}

#[test]
fn environment_backed_inputs_remain_available_without_hiding_other_missing_credentials() {
    with_application_runtime(async {
        let _provider =
            bamboo_config::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
        let _openai = bamboo_config::test_support::override_runtime_env_var(
            "BAMBOO_OPENAI_API_KEY",
            Some("application-environment-secret-sentinel"),
        );
        let _anthropic =
            bamboo_config::test_support::override_runtime_env_var("BAMBOO_ANTHROPIC_API_KEY", None);
        let _gemini =
            bamboo_config::test_support::override_runtime_env_var("BAMBOO_GEMINI_API_KEY", None);

        for stored_proxy in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut document = json!({
                "provider": "openai",
                "default_provider_instance": "env-backed",
                "provider_instances": {"env-backed": {
                    "provider_type": "openai",
                    "base_url": "http://127.0.0.1:9876/v1",
                    "credential_ref": "provider.missing.env-fallback",
                    "api_key_from_env": true
                }}
            });
            if stored_proxy {
                let reference =
                    bamboo_config::CredentialRef::parse("proxy.application.auth").unwrap();
                bamboo_config::CredentialStore::open(dir.path())
                    .replace(
                        reference.clone(),
                        &serde_json::to_string(&bamboo_config::ProxyAuth {
                            username: "application-proxy-user".into(),
                            password: "application-proxy-secret-sentinel".into(),
                        })
                        .unwrap(),
                        bamboo_config::CredentialSource::User,
                        0,
                    )
                    .unwrap();
                document["http_proxy"] = json!("http://127.0.0.1:19877");
                document["proxy_auth_credential_ref"] = json!(reference);
            }
            std::fs::write(
                dir.path().join("config.json"),
                serde_json::to_vec(&document).unwrap(),
            )
            .unwrap();
            let mut state = AppState::new(dir.path().to_path_buf()).await.unwrap();
            state.stop_config_watcher_for_test();
            let report = application_report(&state).await;
            let provider = consumer(&report, "provider-runtime");
            assert_eq!(provider["state"], "in_sync");
            assert_eq!(provider["last_apply_error"], Value::Null);
            assert_eq!(
                provider["credential_source"]["status"],
                if stored_proxy {
                    "observed_inputs"
                } else {
                    "not_required"
                }
            );
            let serialized = serde_json::to_string(&report).unwrap();
            assert!(!serialized.contains("application-environment-secret-sentinel"));
            assert!(!serialized.contains("application-proxy-secret-sentinel"));

            if stored_proxy {
                // A sibling credential writer rotates Core's proxy secret.
                // MCP's owned transaction adopts that document but leaves the
                // running proxy inputs unchanged, so it cannot prove a source.
                let store = bamboo_config::CredentialStore::open(dir.path());
                store
                    .replace(
                        bamboo_config::CredentialRef::parse("proxy.application.auth").unwrap(),
                        &serde_json::to_string(&bamboo_config::ProxyAuth {
                            username: "application-proxy-user".into(),
                            password: "application-proxy-rotated-sentinel".into(),
                        })
                        .unwrap(),
                        bamboo_config::CredentialSource::User,
                        store.revision().unwrap(),
                    )
                    .unwrap();
                let candidate = serde_json::from_value(json!({"proof-only": {
                    "command": "true", "enabled": false,
                    "env": {"TOKEN": "application-mcp-secret-sentinel"}
                }}))
                .unwrap();
                state
                    .update_legacy_mcp_config(BTreeSet::new(), move |mcp| {
                        *mcp = candidate;
                        Ok(())
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    state
                        .config
                        .read()
                        .await
                        .proxy_auth
                        .as_ref()
                        .unwrap()
                        .password,
                    "application-proxy-secret-sentinel"
                );
                let adopted = application_report(&state).await;
                let mcp = consumer(&adopted, "mcp-runtime");
                assert_eq!(mcp["credential_source"]["status"], "unknown");
                assert_eq!(mcp["applied_dependencies"]["credentials"], Value::Null);
                assert_eq!(
                    mcp["applied_dependencies"]["mcp"],
                    application_section(&state, SectionId::Mcp).revision
                );
                assert_eq!(
                    adopted["sections"]["credentials"]["revision"],
                    store.revision().unwrap()
                );
                let serialized = serde_json::to_string(&adopted).unwrap();
                for secret in [
                    "application-proxy-secret-sentinel",
                    "application-proxy-rotated-sentinel",
                    "application-mcp-secret-sentinel",
                ] {
                    assert!(!serialized.contains(secret));
                }
            }

            let facade = state.config_facade.as_ref().unwrap();
            let mut providers = application_section(&state, SectionId::Providers);
            providers.data["provider_instances"]["still-missing"] = json!({
                "provider_type": "anthropic",
                "credential_ref": "provider.missing.anthropic",
                "api_key_from_env": true
            });
            facade
                .registry()
                .providers
                .commit(
                    providers.revision,
                    serde_json::from_value(providers.data).unwrap(),
                )
                .unwrap();
            let unavailable = application_report(&state).await;
            assert_eq!(
                consumer(&unavailable, "provider-runtime")["state"],
                "failed"
            );
            assert_eq!(
                consumer(&unavailable, "provider-runtime")["last_apply_error"]["code"],
                "runtime_input_unavailable"
            );
            assert_eq!(
                consumer(&unavailable, "provider-runtime")["applied_dependencies"],
                provider["applied_dependencies"]
            );
        }
    });
}

#[test]
fn actual_provider_and_mcp_publications_preserve_unrelated_receipts() {
    with_application_runtime(async {
        let (_dir, state) = application_state().await;
        let before = application_report(&state).await;
        assert_eq!(consumer(&before, "provider-runtime")["state"], "in_sync");
        assert_eq!(consumer(&before, "mcp-runtime")["state"], "in_sync");
        let mut core = application_section(&state, SectionId::Core);
        core.data["server"]["port"] = json!(19876);
        state
            .put_ordinary_section(SectionId::Core, core.revision, core.data)
            .await
            .unwrap();
        let after = application_report(&state).await;
        for id in ["provider-runtime", "mcp-runtime"] {
            assert_eq!(consumer(&after, id)["state"], "in_sync");
            assert_eq!(
                consumer(&after, id)["applied_dependencies"],
                consumer(&before, id)["applied_dependencies"]
            );
            assert_eq!(
                consumer(&after, id)["last_applied_at"],
                consumer(&before, id)["last_applied_at"]
            );
        }
        let rev = application_section(&state, SectionId::Providers).revision;
        state
            .put_provider_settings(rev, |_old, candidate| {
                candidate
                    .provider_instances
                    .values_mut()
                    .next()
                    .unwrap()
                    .model = Some("next-model".into());
                candidate.provider_instances.insert(
                    "disabled-peer".into(),
                    serde_json::from_value(json!({"provider_type":"openai", "enabled":false}))
                        .unwrap(),
                );
                Ok((BTreeSet::new(), BTreeSet::new()))
            })
            .await
            .unwrap();
        let defaults = application_report(&state).await;
        assert_eq!(consumer(&defaults, "provider-runtime")["state"], "in_sync");
        for id in [
            "connect-manager",
            "agent-loop-defaults",
            "live-tool-filters",
            "auxiliary-models",
            "notification-sinks",
            "listener",
            "memory-background-cadence",
            "broker-and-cluster",
        ] {
            assert_eq!(consumer(&defaults, id)["state"], "unknown");
            assert_eq!(consumer(&defaults, id)["applied_dependencies"], Value::Null);
        }
        let serialized = serde_json::to_string(&defaults).unwrap();
        for forbidden in [
            "application-secret-sentinel",
            "old-model",
            "next-model",
            "credential_ref",
            "digest",
            "9876",
        ] {
            assert!(!serialized.contains(forbidden));
        }
    });
}

#[test]
fn credential_free_mcp_topology_preserves_proxy_proof_without_claiming_rotation() {
    with_application_runtime(async {
        for typed in [true, false] {
            let (dir, state) = application_state_with_core(json!({
                "provider": "openai", "http_proxy": "http://127.0.0.1:19877",
                "proxy_auth": {"username": "proxy-user", "password": "topology-proxy-original-sentinel"}
            }))
            .await;
            let store = bamboo_config::CredentialStore::open(dir.path());
            let credentials = store.revision().unwrap();
            let provider_before =
                consumer(&application_report(&state).await, "provider-runtime").clone();
            let stdio = json!({"credential-free-stdio": {"command": "true", "enabled": false}});
            // An empty, unreferenced header is metadata, not a credential input.
            let http = json!({"credential-free-http": {
                "url": "http://127.0.0.1:19879/mcp", "enabled": false,
                "headers": {"X-Optional": ""}
            }});
            for document in [stdio.clone(), json!({}), http, json!({})] {
                let candidate: bamboo_mcp::McpConfig = serde_json::from_value(document).unwrap();
                let expected = serde_json::to_value(&candidate).unwrap();
                let previous = application_section(&state, SectionId::Mcp).revision;
                if typed {
                    state.put_mcp_section(previous, candidate).await.unwrap();
                } else {
                    state
                        .update_legacy_mcp_config(BTreeSet::new(), move |mcp| {
                            *mcp = candidate;
                            Ok(())
                        })
                        .await
                        .unwrap();
                }
                let revision = application_section(&state, SectionId::Mcp).revision;
                assert!(revision > previous);
                assert_eq!(
                    serde_json::to_value(&state.config.read().await.mcp).unwrap(),
                    expected
                );
                assert_eq!(store.revision().unwrap(), credentials);
                let report = application_report(&state).await;
                let mcp = consumer(&report, "mcp-runtime");
                assert_eq!(mcp["state"], "in_sync");
                assert_eq!(mcp["credential_source"]["status"], "observed_inputs");
                assert_eq!(mcp["credential_source"]["revision"], credentials);
                assert_eq!(mcp["applied_dependencies"]["mcp"], revision);
                assert_eq!(mcp["applied_dependencies"]["credentials"], credentials);
                assert_eq!(mcp["last_apply_error"], Value::Null);
                assert_eq!(consumer(&report, "provider-runtime"), &provider_before);
                assert_eq!(
                    consumer(&application_report(&state).await, "mcp-runtime"),
                    mcp
                );
                assert!(!serde_json::to_string(&report)
                    .unwrap()
                    .contains("topology-proxy-original-sentinel"));
            }

            // Observe a sibling writer's new credential document without
            // replacing the already-running Core proxy inputs.
            let reference = state
                .config
                .read()
                .await
                .proxy_auth_credential_ref
                .clone()
                .unwrap();
            store.replace(reference,
                &json!({"username": "proxy-user", "password": "topology-proxy-rotated-sentinel"}).to_string(),
                bamboo_config::CredentialSource::User, credentials).unwrap();
            state
                .config_facade
                .as_ref()
                .unwrap()
                .registry()
                .credentials
                .reload()
                .unwrap();
            let candidate = serde_json::from_value(stdio).unwrap();
            if typed {
                state
                    .put_mcp_section(
                        application_section(&state, SectionId::Mcp).revision,
                        candidate,
                    )
                    .await
                    .unwrap();
            } else {
                state
                    .update_legacy_mcp_config(BTreeSet::new(), move |mcp| {
                        *mcp = candidate;
                        Ok(())
                    })
                    .await
                    .unwrap();
            }
            assert_eq!(
                state
                    .config
                    .read()
                    .await
                    .proxy_auth
                    .as_ref()
                    .unwrap()
                    .password,
                "topology-proxy-original-sentinel"
            );
            let report = application_report(&state).await;
            let mcp = consumer(&report, "mcp-runtime");
            assert_eq!(mcp["state"], "unknown");
            assert_eq!(mcp["credential_source"]["status"], "unknown");
            assert_eq!(mcp["applied_dependencies"]["credentials"], Value::Null);
            assert_eq!(
                mcp["applied_dependencies"]["mcp"],
                application_section(&state, SectionId::Mcp).revision
            );
            assert_eq!(
                report["sections"]["credentials"]["revision"],
                store.revision().unwrap()
            );
            assert_eq!(
                consumer(&application_report(&state).await, "mcp-runtime"),
                mcp
            );
            let serialized = serde_json::to_string(&report).unwrap();
            for secret in [
                "topology-proxy-original-sentinel",
                "topology-proxy-rotated-sentinel",
            ] {
                assert!(!serialized.contains(secret));
            }
        }
    });
}

#[test]
fn mcp_reset_binds_committed_credentials_without_claiming_rotated_core_proxy() {
    with_application_runtime(async {
        for rotate_proxy in [false, true] {
            let (dir, state) = application_state_with_core(json!({
                "provider": "openai", "http_proxy": "http://127.0.0.1:19877",
                "proxy_auth": {"username": "proxy-user", "password": "reset-proxy-original-sentinel"}
            }))
            .await;
            let candidate = serde_json::from_value(json!({"reset-proof": {
                "command": "true", "enabled": false,
                "env": {"TOKEN": "reset-mcp-secret-sentinel"}
            }}))
            .unwrap();
            state
                .update_legacy_mcp_config(BTreeSet::new(), move |mcp| {
                    *mcp = candidate;
                    Ok(())
                })
                .await
                .unwrap();
            let before = application_report(&state).await;
            assert_eq!(consumer(&before, "mcp-runtime")["state"], "in_sync");
            assert!(!state.config.read().await.mcp.servers.is_empty());

            let store = bamboo_config::CredentialStore::open(dir.path());
            if rotate_proxy {
                let reference = state
                    .config
                    .read()
                    .await
                    .proxy_auth_credential_ref
                    .clone()
                    .unwrap();
                store
                    .replace(
                        reference,
                        &json!({"username": "proxy-user", "password": "reset-proxy-rotated-sentinel"})
                            .to_string(),
                        bamboo_config::CredentialSource::User,
                        store.revision().unwrap(),
                    )
                    .unwrap();
            }

            let revision = state
                .reset_mcp_section(application_section(&state, SectionId::Mcp).revision)
                .await
                .unwrap();
            assert!(state.config.read().await.mcp.servers.is_empty());
            assert!(state.mcp_manager.list_servers().is_empty());
            assert!(state.mcp_manager.tool_index().all_aliases().is_empty());
            assert_eq!(
                state
                    .config
                    .read()
                    .await
                    .proxy_auth
                    .as_ref()
                    .unwrap()
                    .password,
                "reset-proxy-original-sentinel"
            );
            let reset = application_report(&state).await;
            let mcp = consumer(&reset, "mcp-runtime");
            assert_eq!(mcp["applied_dependencies"]["mcp"], revision);
            assert_eq!(mcp["last_apply_error"], Value::Null);
            if rotate_proxy {
                assert_eq!(mcp["state"], "unknown");
                assert_eq!(mcp["credential_source"]["status"], "unknown");
                assert_eq!(mcp["applied_dependencies"]["credentials"], Value::Null);
            } else {
                assert_eq!(mcp["state"], "in_sync");
                assert_eq!(mcp["credential_source"]["status"], "observed_inputs");
                assert_eq!(
                    mcp["credential_source"]["revision"],
                    store.revision().unwrap()
                );
                assert_eq!(
                    mcp["applied_dependencies"]["credentials"],
                    reset["sections"]["credentials"]["revision"]
                );
            }
            let repeated = application_report(&state).await;
            assert_eq!(consumer(&repeated, "mcp-runtime"), mcp);
            for secret in [
                "reset-proxy-original-sentinel",
                "reset-proxy-rotated-sentinel",
                "reset-mcp-secret-sentinel",
            ] {
                assert!(!serde_json::to_string(&repeated).unwrap().contains(secret));
            }
        }
    });
}

#[test]
fn actual_typed_provider_failure_keeps_receipt_and_repair_converges() {
    with_application_runtime(async {
        let (_dir, state) = application_state().await;
        let before = application_report(&state).await;
        let rev = application_section(&state, SectionId::Providers).revision;
        assert!(state
            .put_provider_settings(rev, |_old, candidate| {
                candidate.provider = "unavailable-provider".into();
                candidate.default_provider_instance = Some("unavailable-instance".into());
                Ok((BTreeSet::new(), BTreeSet::new()))
            })
            .await
            .is_err());
        let failed = application_report(&state).await;
        let provider = consumer(&failed, "provider-runtime");
        assert_eq!(
            provider["applied_dependencies"],
            consumer(&before, "provider-runtime")["applied_dependencies"]
        );
        // The rejected attempt did not change desired storage or its active runtime.
        assert_eq!(provider["state"], "in_sync");
        assert_eq!(
            provider["last_apply_error"]["code"],
            "provider_initialization_failed"
        );
        let mut core = application_section(&state, SectionId::Core);
        core.data["server"]["port"] = json!(19878);
        state
            .put_ordinary_section(SectionId::Core, core.revision, core.data)
            .await
            .unwrap();
        assert_eq!(
            consumer(&application_report(&state).await, "provider-runtime")["last_apply_error"]
                ["code"],
            "provider_initialization_failed"
        );
        let revision = state
            .put_provider_settings(rev, |_old, candidate| {
                candidate
                    .provider_instances
                    .values_mut()
                    .next()
                    .unwrap()
                    .model = Some("repaired-model".into());
                Ok((BTreeSet::new(), BTreeSet::new()))
            })
            .await
            .unwrap();
        let repaired = application_report(&state).await;
        assert_eq!(consumer(&repaired, "provider-runtime")["state"], "in_sync");
        assert_eq!(
            consumer(&repaired, "provider-runtime")["last_apply_error"],
            Value::Null
        );
        assert_eq!(
            consumer(&repaired, "provider-runtime")["applied_dependencies"]["providers"],
            revision
        );
        assert_eq!(
            consumer(&repaired, "mcp-runtime")["applied_dependencies"],
            consumer(&before, "mcp-runtime")["applied_dependencies"]
        );
    });
}

#[test]
fn provider_owned_publication_cannot_attribute_old_core_proxy_to_new_credentials() {
    with_application_runtime(async {
        let (dir, state) = application_state_with_core(json!({
            "provider": "openai", "http_proxy": "http://127.0.0.1:19877",
            "proxy_auth": {"username": "proxy-user", "password": "proxy-original-sentinel"}
        }))
        .await;
        let store = bamboo_config::CredentialStore::open(dir.path());
        let before = application_report(&state).await;
        let old_revision = store.revision().unwrap();
        assert_eq!(
            consumer(&before, "provider-runtime")["credential_source"]["revision"],
            old_revision
        );
        let reference = state
            .config
            .read()
            .await
            .proxy_auth_credential_ref
            .clone()
            .unwrap();
        store
            .replace(
                reference,
                &json!({"username":"proxy-user", "password":"proxy-rotated-sentinel"}).to_string(),
                bamboo_config::CredentialSource::User,
                old_revision,
            )
            .unwrap();
        assert!(store.revision().unwrap() > old_revision);
        let observed = application_report(&state).await;
        assert_eq!(
            consumer(&observed, "provider-runtime")["applied_dependencies"],
            consumer(&before, "provider-runtime")["applied_dependencies"]
        );

        // Provider owns only its metadata/keys. Adopting the rotated credential
        // document cannot update or prove the running sibling Core proxy.
        let revision = state
            .put_provider_settings(
                application_section(&state, SectionId::Providers).revision,
                |_old, candidate| {
                    candidate
                        .provider_instances
                        .values_mut()
                        .next()
                        .unwrap()
                        .model = Some("rotated-provider-model".into());
                    Ok((BTreeSet::new(), BTreeSet::new()))
                },
            )
            .await
            .unwrap();
        assert_eq!(
            state
                .config
                .read()
                .await
                .proxy_auth
                .as_ref()
                .unwrap()
                .password,
            "proxy-original-sentinel"
        );
        let adopted = application_report(&state).await;
        let provider = consumer(&adopted, "provider-runtime");
        assert_eq!(
            adopted["sections"]["credentials"]["revision"],
            store.revision().unwrap()
        );
        assert_eq!(provider["applied_dependencies"]["providers"], revision);
        assert_eq!(provider["credential_source"]["status"], "unknown");
        assert_eq!(provider["applied_dependencies"]["credentials"], Value::Null);
        let repeated = application_report(&state).await;
        assert_eq!(
            consumer(&repeated, "provider-runtime")["applied_dependencies"],
            provider["applied_dependencies"]
        );
        assert_eq!(
            consumer(&repeated, "provider-runtime")["credential_source"]["status"],
            "unknown"
        );
        for secret in [
            "proxy-original-sentinel",
            "proxy-rotated-sentinel",
            "application-secret-sentinel",
        ] {
            assert!(!serde_json::to_string(&repeated).unwrap().contains(secret));
        }
    });
}

#[test]
fn actual_direct_provider_reload_failure_retains_receipt_and_reports_error() {
    with_application_runtime(async {
        let (_dir, state) = application_state().await;
        let before = application_report(&state).await;
        let original = state.config.read().await.clone();
        state.config.write().await.default_provider_instance =
            Some("unavailable-default-instance".into());
        assert!(state.reload_provider().await.is_err());
        let failed = application_report(&state).await;
        let provider = consumer(&failed, "provider-runtime");
        assert_eq!(
            provider["applied_dependencies"],
            consumer(&before, "provider-runtime")["applied_dependencies"]
        );
        assert_eq!(
            provider["last_applied_at"],
            consumer(&before, "provider-runtime")["last_applied_at"]
        );
        assert_eq!(
            provider["last_apply_error"]["code"],
            "provider_initialization_failed"
        );
        *state.config.write().await = original;
        state.reload_provider().await.unwrap();
        let repaired = application_report(&state).await;
        assert_eq!(consumer(&repaired, "provider-runtime")["state"], "in_sync");
        assert_eq!(
            consumer(&repaired, "provider-runtime")["last_apply_error"],
            Value::Null
        );
    });
}

#[test]
fn actual_dependent_publication_retains_unapplied_policy_source() {
    with_application_runtime(async {
        let (dir, state) = application_state().await;
        let before = application_report(&state).await;
        let policy = application_section(&state, SectionId::ModelPolicy);
        let mut document: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("model-policy.json")).unwrap())
                .unwrap();
        document["data"]["keyword_masking"] =
            json!({"entries":[{"pattern":"secret-pattern", "match_type":"exact", "enabled":true}]});
        std::fs::write(
            dir.path().join("model-policy.json"),
            serde_json::to_vec(&document).unwrap(),
        )
        .unwrap();
        state
            .config_facade
            .as_ref()
            .unwrap()
            .registry()
            .reload_if_changed(SectionId::ModelPolicy)
            .unwrap();
        let mut core = application_section(&state, SectionId::Core);
        core.data["http_proxy"] = json!("http://127.0.0.1:19877");
        let core = state
            .put_ordinary_section(SectionId::Core, core.revision, core.data)
            .await
            .unwrap();
        let staged = application_report(&state).await;
        let provider = consumer(&staged, "provider-runtime");
        assert_eq!(provider["applied_dependencies"]["core"], core.revision);
        assert_eq!(
            provider["applied_dependencies"]["model-policy"],
            policy.revision
        );
        assert_ne!(
            provider["desired_dependencies"]["model-policy"],
            policy.revision
        );
        assert_eq!(provider["state"], "pending_apply");
        // Publishing the remaining policy adopts its own revision and the live Core.
        let policy = application_section(&state, SectionId::ModelPolicy);
        state
            .put_ordinary_section(SectionId::ModelPolicy, policy.revision, policy.data)
            .await
            .unwrap();
        let repaired = application_report(&state).await;
        assert_eq!(consumer(&repaired, "provider-runtime")["state"], "in_sync");
        assert_eq!(
            consumer(&repaired, "provider-runtime")["applied_dependencies"]["core"],
            core.revision
        );
        assert_ne!(
            consumer(&repaired, "provider-runtime")["applied_dependencies"],
            consumer(&before, "provider-runtime")["applied_dependencies"]
        );
    });
}

#[test]
fn safe_observed_receipts_exclude_digests_and_credential_bindings() {
    let mut state = ConfigApplicationState::default();
    state.receipts.insert(
        ConfigConsumer::ProviderRuntime,
        ConsumerReceipt {
            applied: Some(AppliedReceipt {
                revisions: BTreeMap::from([(SectionId::Providers, 7)]),
                digest: InputDigest([123; 32]),
                applied_at: Utc::now(),
                credentials: CredentialBinding::Observed(987654321),
            }),
            last_error: Some(ConfigApplyError::ProviderInitializationFailed),
            failed_section: Some(SectionId::Providers),
        },
    );
    let report = state.safe_observed_receipts();
    let text = report.to_string();
    assert!(!text.contains("987654321"));
    assert!(!text.contains("digest"));
    assert!(!text.contains("credentials"));
    assert_eq!(report["desired_vs_applied"], "not_evaluated");
    let provider = report["consumers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["consumer"] == "provider-runtime")
        .unwrap();
    assert_eq!(provider["receipt_status"], "recorded");
    assert_eq!(provider["applied_revisions"]["providers"], 7);
    assert_eq!(
        provider["last_error_code"],
        "provider_initialization_failed"
    );
    assert_eq!(report, state.safe_observed_receipts());
}
