//! All fixtures are synthetic in-memory values. No real credentials or config,
//! files, network endpoints, process environment or subprocesses are accessed.
use super::*;
use crate::{Config, EnvVarEntry};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde_json::json;

const FAKE_KEY: &str = "synthetic-provider-key-never-live";
const FAKE_ENV: &str = "synthetic-env-token-never-live";
const PASSWORD: &str = "synthetic demo passphrase";

fn fixture() -> Config {
    let mut config = Config::default();
    config.provider_instances.clear();
    config.providers_mut().openai = None;
    config.providers_mut().anthropic = None;
    config.providers_mut().gemini = None;
    config.providers_mut().copilot = None;
    config.providers_mut().bodhi = None;
    config.provider_instances.insert("work".into(), serde_json::from_value(json!({
        "provider_type":"openai", "label":"Demo provider", "api_key":FAKE_KEY,
        "api_key_encrypted":"synthetic-ciphertext", "credential_ref":"provider.work.api_key",
        "base_url":"https://example.invalid/v1", "model":"demo-model", "fast_model":"demo-fast",
        "vision_model":"demo-vision", "reasoning_effort":"medium", "responses_only_models":["demo-model"],
        "runtime_models":["demo-model","demo-fast","demo-vision"], "machine_path":"/synthetic/private",
        "request_overrides":{"common":{"headers":{"X-Synthetic-Secret":"never-export"}}}
    })).unwrap());
    let reference = json!({"provider":"work","model":"demo-model","reasoning_effort":"high"});
    config.defaults = Some(
        serde_json::from_value(json!({
            "chat":reference, "fast":reference, "task_summary":reference, "vision":reference,
            "memory_background":reference, "planning":reference, "search":reference,
            "code_review":reference, "sub_agent":reference,
            "subagent_models":{"researcher":reference,"coder":reference}
        }))
        .unwrap(),
    );
    config.env_vars = vec![EnvVarEntry {
        name: "DEMO_TOKEN".into(),
        value: FAKE_ENV.into(),
        secret: true,
        value_encrypted: None,
        credential_ref: None,
        configured: true,
        description: Some("Never export metadata by accident".into()),
    }];
    config
}

fn selection() -> ShareSelection {
    ShareSelection {
        providers: BTreeSet::from(["work".into()]),
        scenarios: selection::scenario_refs(&fixture())
            .keys()
            .copied()
            .collect(),
        subagent_models: BTreeSet::from(["researcher".into(), "coder".into()]),
        env: BTreeMap::from([("DEMO_TOKEN".into(), false)]),
        ..Default::default()
    }
}

fn plain_json(bundle: serde_json::Value) -> String {
    outer(json!({"mode":"plain","bundle":bundle}))
}
fn outer(envelope: serde_json::Value) -> String {
    format!(
        "{SHARE_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&envelope).unwrap())
    )
}

#[test]
fn defaults_and_selective_roundtrip_exclude_unselected_sensitive_and_host_fields() {
    let source = fixture();
    assert_eq!(source.provider_instances["work"].api_key, FAKE_KEY);
    assert_eq!(
        source.provider_instances["work"]
            .request_overrides
            .as_ref()
            .unwrap()
            .common
            .headers
            .get("X-Synthetic-Secret"),
        Some(&crate::TemplateExpr::Literal("never-export".into()))
    );
    assert_eq!(
        select_bundle(&source, &ShareSelection::default()).unwrap(),
        ShareBundle::default()
    );
    let selected = select_bundle(&source, &selection()).unwrap();
    assert_eq!(selected.scenarios.len(), 9);
    assert_eq!(selected.subagent_models.len(), 2);
    assert_eq!(selected.providers[0].api_key, None);
    assert_eq!(selected.providers[0].base_url, None);
    assert_eq!(selected.env[0].value, None);
    let json = serde_json::to_string(&selected).unwrap();
    for excluded in [
        FAKE_KEY,
        FAKE_ENV,
        "credential_ref",
        "api_key_encrypted",
        "/synthetic/private",
        "request_overrides",
        "never-export",
        "description",
        "revision",
    ] {
        assert!(
            !json.contains(excluded),
            "excluded field escaped projection"
        );
    }
    let encoded = encode_bundle(&selected, None).unwrap();
    assert_eq!(decode_bundle(&encoded, None).unwrap(), selected);
    assert!(encoded.starts_with(SHARE_PREFIX));
}

#[test]
fn explicit_values_require_encryption_even_for_nonsecret_and_empty_env_values() {
    let mut selected = select_bundle(&fixture(), &selection()).unwrap();
    selected.env[0].secret = false;
    selected.env[0].value = Some(String::new());
    assert!(selected.contains_secrets());
    assert_eq!(
        encode_bundle(&selected, None),
        Err(ShareError::PassphraseRequired)
    );
    assert_eq!(
        decode_bundle(&plain_json(serde_json::to_value(&selected).unwrap()), None),
        Err(ShareError::Invalid)
    );
    selected.env[0].value = None;
    selected.providers[0].api_key = Some(FAKE_KEY.into());
    assert_eq!(
        encode_bundle(&selected, None),
        Err(ShareError::PassphraseRequired)
    );
}

#[test]
fn authenticated_roundtrip_wrong_password_and_tampering_are_fail_closed() {
    let mut selection = selection();
    selection.provider_api_keys.insert("work".into());
    selection.provider_base_urls.insert("work".into());
    selection.env.insert("DEMO_TOKEN".into(), true);
    let bundle = select_bundle(&fixture(), &selection).unwrap();
    let encoded = encode_bundle(&bundle, Some(PASSWORD)).unwrap();
    assert!(!encoded.contains(PASSWORD));
    assert!(!encoded.contains(FAKE_KEY));
    assert_eq!(decode_bundle(&encoded, Some(PASSWORD)).unwrap(), bundle);
    assert_eq!(
        decode_bundle(&encoded, None),
        Err(ShareError::PassphraseRequired)
    );
    assert_eq!(
        decode_bundle(&encoded, Some("wrong demo passphrase")),
        Err(ShareError::Authentication)
    );
    let mut envelope: serde_json::Value = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(encoded.strip_prefix(SHARE_PREFIX).unwrap())
            .unwrap(),
    )
    .unwrap();
    let mut cipher = URL_SAFE_NO_PAD
        .decode(envelope["ciphertext"].as_str().unwrap())
        .unwrap();
    cipher[0] ^= 1;
    envelope["ciphertext"] = URL_SAFE_NO_PAD.encode(cipher).into();
    assert_eq!(
        decode_bundle(&outer(envelope), Some(PASSWORD)),
        Err(ShareError::Authentication)
    );
    let preview =
        serde_json::to_string(&preview_bundle(&bundle, &Config::default()).unwrap()).unwrap();
    for secret in [FAKE_KEY, FAKE_ENV, "https://example.invalid/v1"] {
        assert!(!preview.contains(secret));
    }
    for debug in [
        format!("{bundle:?}"),
        format!("{:?}", bundle.providers[0]),
        format!("{:?}", bundle.env[0]),
    ] {
        assert!(!debug.contains(FAKE_KEY));
        assert!(!debug.contains(FAKE_ENV));
    }
}

#[test]
fn malformed_unknown_fields_versions_duplicates_and_depth_are_rejected() {
    let bundle = serde_json::to_value(select_bundle(&fixture(), &selection()).unwrap()).unwrap();
    for field in ["mcp", "hooks", "permissions", "session", "revision", "path"] {
        let mut changed = bundle.clone();
        changed[field] = json!({});
        assert_eq!(
            decode_bundle(&plain_json(changed), None),
            Err(ShareError::Invalid)
        );
    }
    for field in [
        "extra",
        "credential_ref",
        "request_overrides",
        "api_key_encrypted",
        "enabled",
    ] {
        let mut changed = bundle.clone();
        changed["providers"][0][field] = json!("synthetic");
        assert_eq!(
            decode_bundle(&plain_json(changed), None),
            Err(ShareError::Invalid)
        );
    }
    let mut nested_ref = bundle.clone();
    nested_ref["scenarios"]["chat"]["path"] = json!("synthetic");
    assert_eq!(
        decode_bundle(&plain_json(nested_ref), None),
        Err(ShareError::Invalid)
    );
    assert_eq!(
        decode_bundle("BAMBOO-SHARE:2:e30", None),
        Err(ShareError::Unsupported)
    );
    assert_eq!(
        decode_bundle("BAMBOO-SHARE:1:broken!", None),
        Err(ShareError::Invalid)
    );
    let duplicate = br#"{"mode":"plain","bundle":{"scenarios":{"chat":{"provider":"a","model":"m"},"chat":{"provider":"b","model":"n"}}}}"#;
    assert_eq!(
        decode_bundle(
            &format!("{SHARE_PREFIX}{}", URL_SAFE_NO_PAD.encode(duplicate)),
            None
        ),
        Err(ShareError::Invalid)
    );
    let deep = format!("{}{}", "[".repeat(17), "]".repeat(17));
    assert_eq!(
        decode_bundle(
            &format!("{SHARE_PREFIX}{}", URL_SAFE_NO_PAD.encode(deep)),
            None
        ),
        Err(ShareError::Limit)
    );
    let encoded = encode_bundle(&ShareBundle::default(), None).unwrap();
    assert!(decode_bundle(&encoded[..encoded.len() - 2], None).is_err());
}

#[test]
fn oversized_data_and_unsupported_kdf_are_rejected_before_kdf_work() {
    let calls = codec::test_kdf_calls();
    assert_eq!(
        decode_bundle(&"x".repeat(MAX_SHARE_STRING_BYTES + 1), Some(PASSWORD)),
        Err(ShareError::Limit)
    );
    let mut bundle = select_bundle(&fixture(), &selection()).unwrap();
    bundle.env[0].value = Some("x".repeat(MAX_VALUE_BYTES + 1));
    assert_eq!(
        encode_bundle(&bundle, Some(PASSWORD)),
        Err(ShareError::Limit)
    );
    let mut envelope = json!({"mode":"encrypted","algorithm":"scrypt-attacker-chosen-work","salt":"", "nonce":"", "ciphertext":""});
    assert_eq!(
        decode_bundle(&outer(envelope.clone()), Some(PASSWORD)),
        Err(ShareError::Unsupported)
    );
    envelope["algorithm"] = "scrypt-n17-r8-p1-aes256gcm".into();
    envelope["salt"] = URL_SAFE_NO_PAD.encode([0u8; 16]).into();
    envelope["nonce"] = URL_SAFE_NO_PAD.encode([0u8; 12]).into();
    envelope["ciphertext"] = URL_SAFE_NO_PAD
        .encode(vec![0u8; MAX_SHARE_BUNDLE_BYTES + 17])
        .into();
    assert_eq!(
        decode_bundle(&outer(envelope), Some(PASSWORD)),
        Err(ShareError::Limit)
    );
    assert_eq!(codec::test_kdf_calls(), calls);
}

#[test]
fn all_scenario_and_subagent_references_follow_provider_rename() {
    let bundle = select_bundle(&fixture(), &selection()).unwrap();
    let target = Config::default();
    let mut choices = ShareDecisions::default();
    choices.providers.insert(
        "work".into(),
        ShareConflictChoice::Rename {
            target: "shared-work".into(),
        },
    );
    choices.env.insert(
        "DEMO_TOKEN".into(),
        ShareConflictChoice::Rename {
            target: "SHARED_DEMO_TOKEN".into(),
        },
    );
    choices.subagent_models.insert(
        "coder".into(),
        ShareConflictChoice::Rename {
            target: "shared-coder".into(),
        },
    );
    let plan = plan_import(&bundle, &target, &choices).unwrap();
    assert_eq!(plan.providers[0].id, "shared-work");
    assert_eq!(plan.scenarios.len(), 9);
    assert!(plan
        .scenarios
        .values()
        .chain(plan.subagent_models.values())
        .all(|r| r.provider == "shared-work"));
    assert!(plan.subagent_models.contains_key("shared-coder"));
    assert_eq!(plan.env[0].name, "SHARED_DEMO_TOKEN");
    assert_eq!(plan.env[0].value, None);
    assert_eq!(plan.providers[0].api_key, None);
    assert_eq!(plan_import(&bundle, &target, &choices).unwrap(), plan);
    assert_eq!(bundle.providers[0].id, "work");
}

#[test]
fn explicit_conflicts_skip_replace_and_cancel_leave_snapshots_unchanged() {
    let source = fixture();
    let bundle = select_bundle(&source, &selection()).unwrap();
    let before = serde_json::to_vec(&source).unwrap();
    let preview = preview_bundle(&bundle, &source).unwrap();
    assert!(preview.providers[0].conflict);
    assert!(preview.scenarios.iter().all(|s| s.conflict));
    assert!(preview.subagent_models.iter().all(|s| s.conflict));
    assert!(preview.env[0].conflict);
    assert_eq!(
        plan_import(&bundle, &source, &ShareDecisions::default()),
        Err(ShareError::Conflict)
    );
    let mut choices = ShareDecisions::default();
    choices
        .providers
        .insert("work".into(), ShareConflictChoice::Skip);
    choices
        .env
        .insert("DEMO_TOKEN".into(), ShareConflictChoice::Skip);
    let skipped = plan_import(&bundle, &source, &choices).unwrap();
    assert_eq!(skipped, ShareBundle::default());
    choices
        .providers
        .insert("work".into(), ShareConflictChoice::Replace);
    choices
        .env
        .insert("DEMO_TOKEN".into(), ShareConflictChoice::Replace);
    choices.scenarios = bundle
        .scenarios
        .keys()
        .map(|s| (*s, ShareConflictChoice::Replace))
        .collect();
    choices.subagent_models = bundle
        .subagent_models
        .keys()
        .map(|s| (s.clone(), ShareConflictChoice::Replace))
        .collect();
    assert_eq!(plan_import(&bundle, &source, &choices).unwrap(), bundle);
    assert_eq!(serde_json::to_vec(&source).unwrap(), before);
}

#[test]
fn collisions_unknown_decisions_and_scenario_rename_are_rejected() {
    let mut source = fixture();
    let bundle = select_bundle(&source, &selection()).unwrap();
    let other = source.provider_instances["work"].clone();
    source.provider_instances.insert("occupied".into(), other);
    let mut choices = ShareDecisions::default();
    choices.providers.insert(
        "work".into(),
        ShareConflictChoice::Rename {
            target: "occupied".into(),
        },
    );
    assert_eq!(
        plan_import(&bundle, &source, &choices),
        Err(ShareError::Conflict)
    );
    choices.providers.insert(
        "work".into(),
        ShareConflictChoice::Rename {
            target: "new-work".into(),
        },
    );
    choices.scenarios.insert(
        ShareScenario::Chat,
        ShareConflictChoice::Rename {
            target: "new-chat".into(),
        },
    );
    assert_eq!(
        plan_import(&bundle, &source, &choices),
        Err(ShareError::Conflict)
    );
    choices.scenarios.clear();
    choices
        .providers
        .insert("not-in-bundle".into(), ShareConflictChoice::Skip);
    assert_eq!(
        plan_import(&bundle, &source, &choices),
        Err(ShareError::Conflict)
    );
}

#[test]
fn provider_type_aliases_resolve_before_selection_without_environment_lookup() {
    let mut source = fixture();
    source.default_provider_instance = Some("work".into());
    let defaults = source.defaults.as_mut().unwrap();
    defaults.chat.provider = "openai".into();
    defaults.subagent_models.get_mut("coder").unwrap().provider = "openai".into();
    let selected = select_bundle(&source, &selection()).unwrap();
    assert_eq!(selected.scenarios[&ShareScenario::Chat].provider, "work");
    assert_eq!(selected.subagent_models["coder"].provider, "work");
}

#[test]
fn missing_selection_reference_and_masked_keys_fail_without_clear_semantics() {
    let source = fixture();
    let mut selected = selection();
    selected.providers.clear();
    assert_eq!(
        select_bundle(&source, &selected),
        Err(ShareError::Reference)
    );
    selected = selection();
    selected.provider_api_keys.insert("unselected".into());
    assert_eq!(
        select_bundle(&source, &selected),
        Err(ShareError::Selection)
    );
    selected = selection();
    selected.providers.insert("missing".into());
    assert_eq!(
        select_bundle(&source, &selected),
        Err(ShareError::Selection)
    );
    let mut bundle = select_bundle(&source, &selection()).unwrap();
    for placeholder in ["****...****", "*****", "...", ""] {
        bundle.providers[0].api_key = Some(placeholder.into());
        assert_eq!(bundle.validate(), Err(ShareError::Invalid));
    }
}

#[test]
fn sensitive_urls_and_dangerous_environment_names_are_rejected() {
    let mut bundle = select_bundle(&fixture(), &selection()).unwrap();
    for url in [
        "https://user:token@example.invalid/v1",
        "https://example.invalid/v1?key=synthetic",
        "https://example.invalid/v1#synthetic",
        "file:///synthetic",
        "javascript:synthetic",
    ] {
        bundle.providers[0].base_url = Some(url.into());
        assert_eq!(bundle.validate(), Err(ShareError::Unsafe));
    }
    for name in [
        "PATH",
        "home",
        "LD_PRELOAD",
        "DYLD_INSERT_LIBRARIES",
        "NODE_OPTIONS",
        "PYTHONPATH",
        "BAMBOO_DATA_DIR",
        "OPENAI_BASE_URL",
        "RUSTC_WRAPPER",
        "BASH_ENV",
        "GIT_SSH_COMMAND",
        "JAVA_TOOL_OPTIONS",
        "PHPRC",
        "ELECTRON_RUN_AS_NODE",
        "XDG_CONFIG_HOME",
    ] {
        assert_eq!(validate_env_name(name), Err(ShareError::Unsafe));
    }
    for name in ["bad-name", "1BAD", "", "A\0B"] {
        assert_eq!(validate_env_name(name), Err(ShareError::Invalid));
    }
    assert_eq!(validate_env_name("DEMO_TOKEN"), Ok(()));
}

fn env_bundle(names: &[&str]) -> ShareBundle {
    ShareBundle {
        env: names
            .iter()
            .map(|name| ShareEnvVar {
                name: (*name).into(),
                value: None,
                secret: false,
            })
            .collect(),
        ..Default::default()
    }
}

fn env_target(names: &[&str]) -> Config {
    let mut target = Config::default();
    target.env_vars = names
        .iter()
        .map(|name| EnvVarEntry {
            name: (*name).into(),
            value: "synthetic-saved-value".into(),
            secret: false,
            value_encrypted: None,
            credential_ref: None,
            configured: true,
            description: None,
        })
        .collect();
    target
}

#[test]
fn git_control_environment_names_are_rejected_as_data_without_execution() {
    // These strings are never used as commands or passed to a child process.
    for name in [
        "GIT_ASKPASS",
        "git_askpass",
        "Git_AskPass",
        "GIT_SYNTHETIC_FUTURE_CONTROL",
    ] {
        assert_eq!(validate_env_name(name), Err(ShareError::Unsafe));
        assert_eq!(env_bundle(&[name]).validate(), Err(ShareError::Unsafe));
        let source = env_target(&[name]);
        let selection = ShareSelection {
            env: BTreeMap::from([(name.into(), false)]),
            ..Default::default()
        };
        assert_eq!(select_bundle(&source, &selection), Err(ShareError::Unsafe));
        let choices = ShareDecisions {
            env: BTreeMap::from([(
                "Token".into(),
                ShareConflictChoice::Rename {
                    target: name.into(),
                },
            )]),
            ..Default::default()
        };
        assert_eq!(
            plan_import(&env_bundle(&["Token"]), &Config::default(), &choices),
            Err(ShareError::Unsafe)
        );
    }
}

#[test]
fn portable_environment_bundle_identity_rejects_case_only_duplicates_on_every_platform() {
    for name in ["Token", "TOKEN", "token", "tOkEn"] {
        assert_eq!(canonical_env_name(name), "TOKEN");
        assert_eq!(validate_env_name(name), Ok(()));
    }
    let bundle = env_bundle(&["Token", "TOKEN"]);
    assert_eq!(bundle.validate(), Err(ShareError::Invalid));
    assert_eq!(
        decode_bundle(&plain_json(serde_json::to_value(&bundle).unwrap()), None),
        Err(ShareError::Invalid)
    );
    let source = env_target(&["Token", "TOKEN"]);
    let before = serde_json::to_vec(&source).unwrap();
    let selection = ShareSelection {
        env: BTreeMap::from([("Token".into(), false), ("TOKEN".into(), false)]),
        ..Default::default()
    };
    assert_eq!(select_bundle(&source, &selection), Err(ShareError::Invalid));
    assert_eq!(serde_json::to_vec(&source).unwrap(), before);
}

#[test]
fn portable_environment_conflicts_require_choices_and_replace_uses_target_spelling() {
    let bundle = env_bundle(&["Token"]);
    let original_bundle = bundle.clone();
    for spelling in ["TOKEN", "tOkEn"] {
        let target = env_target(&[spelling, "Other"]);
        let before = serde_json::to_vec(&target).unwrap();
        let preview = preview_bundle(&bundle, &target).unwrap();
        assert_eq!(preview.env[0].name, "Token");
        assert!(preview.env[0].conflict);
        assert_eq!(
            plan_import(&bundle, &target, &ShareDecisions::default()),
            Err(ShareError::Conflict)
        );
        let mut decisions = ShareDecisions {
            // Decision names use the same portable identity as bundle names.
            env: BTreeMap::from([("toKEN".into(), ShareConflictChoice::Skip)]),
            ..Default::default()
        };
        assert!(plan_import(&bundle, &target, &decisions)
            .unwrap()
            .env
            .is_empty());
        decisions
            .env
            .insert("toKEN".into(), ShareConflictChoice::Replace);
        let replaced = plan_import(&bundle, &target, &decisions).unwrap();
        assert_eq!(replaced.env.len(), 1);
        assert_eq!(replaced.env[0].name, spelling);
        assert_eq!(replaced.env[0].value, None); // omission is still not Clear
        decisions.env.insert(
            "toKEN".into(),
            ShareConflictChoice::Rename {
                target: "New_Token".into(),
            },
        );
        assert_eq!(
            plan_import(&bundle, &target, &decisions).unwrap().env[0].name,
            "New_Token"
        );
        assert_eq!(serde_json::to_vec(&target).unwrap(), before);
    }
    assert_eq!(bundle, original_bundle);
}

#[test]
fn portable_environment_rename_rejects_source_target_assigned_and_choice_collisions() {
    let bundle = env_bundle(&["Token", "Second"]);
    let target = env_target(&["EXISTING"]);
    let before = serde_json::to_vec(&target).unwrap();
    for name in ["TOKEN", "tOkEn", "second", "existing"] {
        let decisions = ShareDecisions {
            env: BTreeMap::from([(
                "Token".into(),
                ShareConflictChoice::Rename {
                    target: name.into(),
                },
            )]),
            ..Default::default()
        };
        assert_eq!(
            plan_import(&bundle, &target, &decisions),
            Err(ShareError::Conflict)
        );
    }
    let decisions = ShareDecisions {
        env: BTreeMap::from([
            (
                "Token".into(),
                ShareConflictChoice::Rename {
                    target: "Shared".into(),
                },
            ),
            (
                "Second".into(),
                ShareConflictChoice::Rename {
                    target: "sHaReD".into(),
                },
            ),
        ]),
        ..Default::default()
    };
    assert_eq!(
        plan_import(&bundle, &target, &decisions),
        Err(ShareError::Conflict)
    );
    let decisions = ShareDecisions {
        env: BTreeMap::from([
            ("Token".into(), ShareConflictChoice::Skip),
            ("TOKEN".into(), ShareConflictChoice::Replace),
        ]),
        ..Default::default()
    };
    assert_eq!(
        plan_import(&bundle, &target, &decisions),
        Err(ShareError::Conflict)
    );
    assert_eq!(serde_json::to_vec(&target).unwrap(), before);
}

#[test]
fn portable_environment_target_duplicates_fail_closed_without_selecting_a_spelling() {
    let target = env_target(&["Token", "TOKEN"]);
    let before = serde_json::to_vec(&target).unwrap();
    let bundle = env_bundle(&["Fresh"]);
    assert_eq!(preview_bundle(&bundle, &target), Err(ShareError::Conflict));
    for choice in [ShareConflictChoice::Skip, ShareConflictChoice::Replace] {
        let decisions = ShareDecisions {
            env: BTreeMap::from([("Fresh".into(), choice)]),
            ..Default::default()
        };
        assert_eq!(
            plan_import(&bundle, &target, &decisions),
            Err(ShareError::Conflict)
        );
    }
    assert_eq!(serde_json::to_vec(&target).unwrap(), before);
}

fn provider_only_bundle() -> ShareBundle {
    select_bundle(
        &fixture(),
        &ShareSelection {
            providers: BTreeSet::from(["work".into()]),
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn provider_preview_shows_replace_origins_and_retained_key_risk_without_secret_paths() {
    let mut target = fixture();
    target.provider_instances.get_mut("work").unwrap().base_url = Some(
        "https://synthetic-user:synthetic-password@OLD.invalid:8443/private-target-path?target-query=synthetic#target-fragment".into()
    );
    let before = target.provider_instances.clone();
    let mut bundle = provider_only_bundle();
    bundle.providers[0].base_url = Some("https://NEW.invalid:9443/private-source-path".into());
    let preview = preview_bundle(&bundle, &target).unwrap();
    let provider = &preview.providers[0];
    assert_eq!(
        provider.source_origin.as_deref(),
        Some("https://new.invalid:9443")
    );
    assert_eq!(
        provider.target_origin.as_deref(),
        Some("https://old.invalid:8443")
    );
    assert!(provider.retains_existing_api_key);
    assert!(provider.credential_origin_risk);
    for output in [
        serde_json::to_string(&preview).unwrap(),
        format!("{preview:?}"),
    ] {
        for omitted in [
            FAKE_KEY,
            "synthetic-user",
            "synthetic-password",
            "private-target-path",
            "target-query",
            "target-fragment",
            "private-source-path",
            "provider.work.api_key",
        ] {
            assert!(!output.contains(omitted));
        }
    }
    assert_eq!(target.provider_instances, before);
}

#[test]
fn provider_preview_marks_unknown_target_origins_for_all_retained_key_metadata() {
    let mut bundle = provider_only_bundle();
    bundle.providers[0].base_url = Some("https://source.invalid/v1".into());
    for target_url in [
        None,
        Some("not-a-url"),
        Some("file:///synthetic/private"),
        Some("ftp://target.invalid/private"),
    ] {
        for key_kind in ["reference", "encrypted", "plaintext"] {
            let mut target = fixture();
            let instance = target.provider_instances.get_mut("work").unwrap();
            instance.base_url = target_url.map(str::to_string);
            instance.api_key.clear();
            instance.api_key_encrypted = None;
            instance.credential_ref = None;
            match key_kind {
                "reference" => {
                    instance.credential_ref =
                        Some(crate::CredentialRef::parse("provider.synthetic.key").unwrap())
                }
                "encrypted" => {
                    instance.api_key_encrypted = Some("synthetic-ciphertext-metadata".into())
                }
                _ => instance.api_key = FAKE_KEY.into(),
            }
            let before = target.provider_instances.clone();
            let preview = preview_bundle(&bundle, &target).unwrap();
            let provider = &preview.providers[0];
            assert_eq!(
                provider.source_origin.as_deref(),
                Some("https://source.invalid")
            );
            assert_eq!(provider.target_origin, None);
            assert!(provider.retains_existing_api_key);
            assert!(provider.credential_origin_risk);
            let serialized = serde_json::to_value(provider).unwrap();
            assert!(serialized.get("target_origin").is_none());
            assert_eq!(target.provider_instances, before);
        }
    }
}

#[test]
fn provider_preview_origin_risk_requires_a_changed_url_and_a_conflicting_target() {
    let mut target = fixture();
    target.provider_instances.get_mut("work").unwrap().base_url =
        Some("https://source.invalid:443/target-private".into());
    let mut bundle = provider_only_bundle();
    bundle.providers[0].base_url = Some("https://SOURCE.invalid/source-private".into());
    let preview = preview_bundle(&bundle, &target).unwrap();
    assert!(preview.providers[0].retains_existing_api_key);
    assert!(!preview.providers[0].credential_origin_risk); // equal normalized origins
    bundle.providers[0].base_url = None;
    let preview = preview_bundle(&bundle, &target).unwrap();
    let provider = &preview.providers[0];
    assert!(provider.retains_existing_api_key);
    assert!(!provider.credential_origin_risk); // omitted URL preserves the host
    assert!(serde_json::to_value(provider)
        .unwrap()
        .get("source_origin")
        .is_none());
    bundle.providers[0].base_url = Some("https://different.invalid/v1".into());
    bundle.providers[0].api_key = Some("synthetic-incoming-key".into());
    let preview = preview_bundle(&bundle, &target).unwrap();
    assert!(!preview.providers[0].retains_existing_api_key);
    assert!(preview.providers[0].credential_origin_risk);
    bundle.providers[0].api_key = None;
    let instance = target.provider_instances.get_mut("work").unwrap();
    instance.api_key.clear();
    instance.api_key_encrypted = None;
    instance.credential_ref = None;
    let preview = preview_bundle(&bundle, &target).unwrap();
    assert!(!preview.providers[0].retains_existing_api_key);
    assert!(preview.providers[0].credential_origin_risk);
}

#[test]
fn provider_rename_does_not_reuse_the_previous_identity_key_or_replace_risk() {
    let target = fixture();
    let before = target.provider_instances.clone();
    let mut bundle = provider_only_bundle();
    bundle.providers[0].base_url = Some("https://different.invalid/private".into());
    let decisions = ShareDecisions {
        providers: BTreeMap::from([(
            "work".into(),
            ShareConflictChoice::Rename {
                target: "renamed-work".into(),
            },
        )]),
        ..Default::default()
    };
    let plan = plan_import(&bundle, &target, &decisions).unwrap();
    assert_eq!(plan.providers[0].id, "renamed-work");
    assert_eq!(plan.providers[0].api_key, None);
    let preview = preview_bundle(&plan, &target).unwrap();
    assert!(!preview.providers[0].retains_existing_api_key);
    assert!(!preview.providers[0].credential_origin_risk);
    assert_eq!(preview.providers[0].target_origin, None);
    assert_eq!(target.provider_instances, before);
}

#[test]
fn provider_preview_includes_possible_env_binding_reuse_with_or_without_an_incoming_key() {
    let mut target = fixture();
    let instance = target.provider_instances.get_mut("work").unwrap();
    instance.api_key.clear();
    instance.api_key_encrypted = None;
    instance.credential_ref = None;
    instance.extra.insert(
        crate::PROVIDER_INSTANCE_API_KEY_FROM_ENV_CONFIG_KEY.into(),
        json!(true),
    );
    let before = target.provider_instances.clone();
    for incoming_key in [None, Some("synthetic-incoming-key")] {
        let mut bundle = provider_only_bundle();
        bundle.providers[0].base_url = Some("https://different.invalid/private".into());
        bundle.providers[0].api_key = incoming_key.map(str::to_string);
        let preview = preview_bundle(&bundle, &target).unwrap();
        let provider = &preview.providers[0];
        assert_eq!(provider.has_api_key, incoming_key.is_some());
        assert!(provider.retains_existing_api_key); // binding metadata, not an env-value probe
        assert!(provider.credential_origin_risk);
        let decisions = ShareDecisions {
            providers: BTreeMap::from([(
                "work".into(),
                ShareConflictChoice::Rename {
                    target: "fresh-work".into(),
                },
            )]),
            ..Default::default()
        };
        let plan = plan_import(&bundle, &target, &decisions).unwrap();
        assert!(serde_json::to_value(&plan.providers[0])
            .unwrap()
            .get("api_key_from_env")
            .is_none());
        let renamed = preview_bundle(&plan, &target).unwrap();
        assert!(!renamed.providers[0].retains_existing_api_key);
        assert!(!renamed.providers[0].credential_origin_risk);
        assert_eq!(target.provider_instances, before);
    }
}

#[test]
fn provider_origin_change_warns_about_retained_overrides_even_with_a_new_api_key() {
    let mut target = fixture();
    let instance = target.provider_instances.get_mut("work").unwrap();
    instance.api_key.clear();
    instance.api_key_encrypted = None;
    instance.credential_ref = None;
    let overrides = instance.request_overrides.as_mut().unwrap();
    overrides.common.headers.insert(
        "Authorization".into(),
        crate::TemplateExpr::Literal("synthetic-private-override".into()),
    );
    assert_eq!(
        overrides.common.headers.get("Authorization"),
        Some(&crate::TemplateExpr::Literal(
            "synthetic-private-override".into()
        ))
    );
    let before = target.provider_instances.clone();
    let mut bundle = provider_only_bundle();
    bundle.providers[0].base_url = Some("https://different.invalid/private-source-path".into());
    bundle.providers[0].api_key = Some("synthetic-new-import-key".into());
    let preview = preview_bundle(&bundle, &target).unwrap();
    assert!(!preview.providers[0].retains_existing_api_key);
    assert!(preview.providers[0].credential_origin_risk);
    for output in [
        serde_json::to_string(&preview).unwrap(),
        format!("{preview:?}"),
    ] {
        for omitted in [
            "Authorization",
            "synthetic-private-override",
            "synthetic-new-import-key",
            "private-source-path",
        ] {
            assert!(!output.contains(omitted));
        }
    }
    assert_eq!(target.provider_instances, before);
}

#[test]
fn native_loader_environment_selectors_are_rejected_as_names_without_loading_modules() {
    // No set_var, module loading, command execution or filesystem lookup.
    for name in [
        "GCONV_PATH",
        "LOCPATH",
        "NLSPATH",
        "LIBPATH",
        "SHLIB_PATH",
        "GLIBC_TUNABLES",
        "OPENSSL_MODULES",
        "OPENSSL_CONF",
        "CORECLR_PROFILER_PATH",
        "COR_ENABLE_PROFILING",
        "GIO_EXTRA_MODULES",
        "GTK_PATH",
        "QT_PLUGIN_PATH",
        "LUA_CPATH",
        "TCL_LIBRARY",
        "TCLLIBPATH",
        "JULIA_LOAD_PATH",
        "R_PROFILE_USER",
        "R_ENVIRON_USER",
        "COMPlus_JitName",
        "MONO_PATH",
        "MONO_GAC_PREFIX",
    ] {
        for variant in [name.to_string(), name.to_ascii_lowercase()] {
            assert_eq!(validate_env_name(&variant), Err(ShareError::Unsafe));
            assert_eq!(env_bundle(&[&variant]).validate(), Err(ShareError::Unsafe));
            let source = env_target(&[&variant]);
            let selection = ShareSelection {
                env: BTreeMap::from([(variant.clone(), false)]),
                ..Default::default()
            };
            assert_eq!(select_bundle(&source, &selection), Err(ShareError::Unsafe));
            let choices = ShareDecisions {
                env: BTreeMap::from([(
                    "AppToken".into(),
                    ShareConflictChoice::Rename { target: variant },
                )]),
                ..Default::default()
            };
            assert_eq!(
                plan_import(&env_bundle(&["AppToken"]), &Config::default(), &choices),
                Err(ShareError::Unsafe)
            );
        }
    }
    for name in ["APP_LOCALE", "APP_MODULES", "RANGE_LIMIT"] {
        assert_eq!(validate_env_name(name), Ok(()));
    }
}

#[test]
fn provider_type_changes_require_skip_or_fresh_rename_without_mutating_snapshots() {
    let target = fixture();
    let before = serde_json::to_vec(&target).unwrap();
    for base_url in [None, Some("https://synthetic-other.invalid/private")] {
        for api_key in [None, Some("synthetic-incoming-key")] {
            let mut bundle = provider_only_bundle();
            bundle.providers[0].provider_type = ShareProviderType::Anthropic;
            bundle.providers[0].base_url = base_url.map(str::to_string);
            bundle.providers[0].api_key = api_key.map(str::to_string);
            let original = bundle.clone();
            let preview = preview_bundle(&bundle, &target).unwrap();
            let provider = &preview.providers[0];
            assert!(provider.conflict);
            assert!(!provider.replace_allowed);
            assert_eq!(provider.credential_origin_risk, base_url.is_some());
            let mut serialized = serde_json::to_value(provider).unwrap();
            assert_eq!(serialized["replace_allowed"], false);
            serialized
                .as_object_mut()
                .unwrap()
                .remove("replace_allowed");
            assert!(serde_json::from_value::<ShareProviderPreview>(serialized).is_err());
            assert_eq!(
                plan_import(&bundle, &target, &ShareDecisions::default()),
                Err(ShareError::Conflict)
            );
            let mut decisions = ShareDecisions {
                providers: BTreeMap::from([("work".into(), ShareConflictChoice::Replace)]),
                ..Default::default()
            };
            assert_eq!(
                plan_import(&bundle, &target, &decisions),
                Err(ShareError::Invalid)
            );
            decisions
                .providers
                .insert("work".into(), ShareConflictChoice::Skip);
            assert_eq!(
                plan_import(&bundle, &target, &decisions).unwrap(),
                ShareBundle::default()
            );
            decisions.providers.insert(
                "work".into(),
                ShareConflictChoice::Rename {
                    target: "fresh-work".into(),
                },
            );
            let renamed = plan_import(&bundle, &target, &decisions).unwrap();
            let mut expected = bundle.clone();
            expected.providers[0].id = "fresh-work".into();
            assert_eq!(renamed, expected); // only imported fields, no old key/settings
            let renamed_preview = preview_bundle(&renamed, &target).unwrap();
            let provider = &renamed_preview.providers[0];
            assert!(!provider.conflict);
            assert!(provider.replace_allowed);
            assert!(!provider.retains_existing_api_key);
            assert!(!provider.credential_origin_risk);
            assert_eq!(provider.target_origin, None);
            let mut same_type = bundle.clone();
            same_type.providers[0].provider_type = ShareProviderType::Openai;
            assert!(preview_bundle(&same_type, &target).unwrap().providers[0].replace_allowed);
            decisions
                .providers
                .insert("work".into(), ShareConflictChoice::Replace);
            assert_eq!(
                plan_import(&same_type, &target, &decisions).unwrap(),
                same_type
            );
            assert_eq!(bundle, original);
            assert_eq!(serde_json::to_vec(&target).unwrap(), before);
        }
    }
}

#[test]
fn compiler_controls_are_rejected_as_strings_without_running_tools() {
    // The fixture supplies names only: no environment mutation or tool probe.
    let mut names: Vec<String> = [
        "RUSTC",
        "RUSTDOC",
        "RUSTC_LINKER",
        "RUSTDOCFLAGS",
        "CROSS_COMPILE",
        "LDSHARED",
        "CL",
        "_CL_",
        "LINK",
        "_LINK_",
        "GCC_EXEC_PREFIX",
        "GCC_COMPARE_DEBUG",
        "COMPILER_PATH",
        "LIBRARY_PATH",
        "MAKE",
        "MAKEFLAGS",
        "GNUMAKEFLAGS",
        "MAKEFILES",
        "CONFIG_SHELL",
        "CONFIG_SITE",
        "CMAKE_TOOLCHAIN_FILE",
        "CMAKE_PROGRAM_PATH",
        "CMAKE_PREFIX_PATH",
        "CMAKE_GENERATOR_TOOLSET",
        "CMAKE_C_COMPILER_LAUNCHER",
        "CMAKE_CXX_LINKER_LAUNCHER",
        "CCACHE_PREFIX",
        "CCACHE_PREFIX_CPP",
        "CCACHE_PATH",
        "CCACHE_CONFIGPATH",
        "SCCACHE_CONF",
        "DISTCC_SSH",
        "ICECC_CC",
        "ICECC_CXX",
        "ICECC_VERSION",
        // Reserve each known control family, independent of individual spelling.
        "CCACHE_SYNTHETIC_RESERVED",
        "SCCACHE_SYNTHETIC_RESERVED",
        "DISTCC_SYNTHETIC_RESERVED",
        "ICECC_SYNTHETIC_RESERVED",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    for tool in [
        "CC",
        "CXX",
        "CPP",
        "AR",
        "AS",
        "LD",
        "FC",
        "F77",
        "F90",
        "RANLIB",
        "NM",
        "STRIP",
        "PKG_CONFIG",
        "NVCC",
        "CC_MASM_ASM",
        "CFLAGS",
        "CXXFLAGS",
        "CPPFLAGS",
        "LDFLAGS",
        "ARFLAGS",
        "RANLIBFLAGS",
        "FCFLAGS",
        "FFLAGS",
        "F77FLAGS",
        "F90FLAGS",
        "ASFLAGS",
    ] {
        names.extend([
            tool.to_string(),
            format!("HOST_{tool}"),
            format!("TARGET_{tool}"),
            format!("{tool}_x86_64_unknown_linux_gnu"),
        ]);
    }
    for name in names {
        for variant in [name.clone(), name.to_ascii_lowercase()] {
            assert_eq!(validate_env_name(&variant), Err(ShareError::Unsafe));
            assert_eq!(env_bundle(&[&variant]).validate(), Err(ShareError::Unsafe));
            let source = env_target(&[&variant]);
            let before = serde_json::to_vec(&source).unwrap();
            let selection = ShareSelection {
                env: BTreeMap::from([(variant.clone(), false)]),
                ..Default::default()
            };
            assert_eq!(select_bundle(&source, &selection), Err(ShareError::Unsafe));
            let choices = ShareDecisions {
                env: BTreeMap::from([(
                    "AppToken".into(),
                    ShareConflictChoice::Rename { target: variant },
                )]),
                ..Default::default()
            };
            assert_eq!(
                plan_import(&env_bundle(&["AppToken"]), &Config::default(), &choices),
                Err(ShareError::Unsafe)
            );
            assert_eq!(serde_json::to_vec(&source).unwrap(), before);
        }
    }
    for name in [
        "HOST_LABEL",
        "TARGET_REGION",
        "ACCOUNT_ID",
        "CCOUNT",
        "RUSTCURRENCY",
        "APP_BUILD_FLAVOR",
        "APP_CFLAGS",
        "LANG",
        "LANGUAGE",
        "MAKEUP",
        "GCCOUNT",
        "CMAKEAPP",
        "CCACHEAPP",
        "SCCACHEAPP",
        "DISTCCAPP",
        "ICECCAPP",
        "APP_CCACHE_PREFIX",
    ] {
        for variant in [name.to_string(), name.to_ascii_lowercase()] {
            assert_eq!(validate_env_name(&variant), Ok(()));
            let source = env_target(&[&variant]);
            let before = serde_json::to_vec(&source).unwrap();
            let selection = ShareSelection {
                env: BTreeMap::from([(variant.clone(), false)]),
                ..Default::default()
            };
            let selected = select_bundle(&source, &selection).unwrap();
            assert_eq!(selected.env[0].name, variant);
            let choices = ShareDecisions {
                env: BTreeMap::from([(
                    "AppToken".into(),
                    ShareConflictChoice::Rename {
                        target: variant.clone(),
                    },
                )]),
                ..Default::default()
            };
            let plan =
                plan_import(&env_bundle(&["AppToken"]), &Config::default(), &choices).unwrap();
            assert_eq!(plan.env[0].name, variant);
            assert_eq!(serde_json::to_vec(&source).unwrap(), before);
        }
    }
}

#[test]
fn package_controls_are_rejected_as_inert_names_without_running_tools() {
    // No Ruby/package manager, interpreter, environment mutation, file or process
    // probe. Source values are synthetic and excluded from the plain roundtrip.
    let kdf_calls = codec::test_kdf_calls();
    let benign = env_bundle(&["AppToken"]);
    let original_benign = benign.clone();
    let mut names: Vec<String> = [
        "BUNDLE_GEMFILE",
        "BUNDLE_APP_CONFIG",
        "BUNDLE_PATH",
        "GEM_HOME",
        "GEM_PATH",
        "GEMRC",
        "RAKEOPT",
        "RAKE_SYSTEM",
        "RBENV_VERSION",
        "RBENV_ROOT",
        "RBENV_HOOK_PATH",
        "RUBYGEMS_HOST",
        "RUBYOPT",
        "PIP_EDITOR",
        "PIP_CONFIG_FILE",
        "UV_PYTHON",
        "UV_ENV_FILE",
        "UV_CONFIG_FILE",
        "PIPENV_PYTHON",
        "PIPENV_VIRTUALENV",
        "PIPENV_DOTENV_LOCATION",
        "POETRY_CONFIG_DIR",
        "POETRY_VIRTUALENVS_PATH",
        "GRADLE_USER_HOME",
        "GRADLE_OPTS",
        "MAVEN_OPTS",
        "MAVEN_ARGS",
        "ANT_OPTS",
        "ANT_ARGS",
        "JAVACMD",
        "COMPOSER",
        "COMPOSER_HOME",
        "COMPOSER_VENDOR_DIR",
        "YARN_YARN_PATH",
        "BUN_OPTIONS",
        "COREPACK_HOME",
        "COREPACK_ENV_FILE",
        "COREPACK_INTEGRITY_KEYS",
        "PNPM_CONFIG_SCRIPT_SHELL",
        "DENO_V8_FLAGS",
        "DENO_CONDITIONS",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    for prefix in [
        "BUNDLE_",
        "GEM_",
        "RBENV_",
        "RAKE_",
        "PIP_",
        "UV_",
        "PIPENV_",
        "POETRY_",
        "GRADLE_",
        "MAVEN_",
        "ANT_",
        "COMPOSER_",
        "YARN_",
        "BUN_",
        "COREPACK_",
        "PNPM_",
        "DENO_",
    ] {
        names.push(format!("{prefix}SYNTHETIC_RESERVED"));
    }
    for name in names {
        for variant in [name.clone(), name.to_ascii_lowercase()] {
            assert_eq!(validate_env_name(&variant), Err(ShareError::Unsafe));
            let bundle = env_bundle(&[&variant]);
            assert_eq!(bundle.validate(), Err(ShareError::Unsafe));
            assert_eq!(encode_bundle(&bundle, None), Err(ShareError::Unsafe));
            let encoded = plain_json(serde_json::to_value(&bundle).unwrap());
            assert_eq!(decode_bundle(&encoded, None), Err(ShareError::Unsafe));
            assert_eq!(
                preview_bundle(&bundle, &Config::default()),
                Err(ShareError::Unsafe)
            );
            let source = env_target(&[&variant]);
            let before = serde_json::to_vec(&source).unwrap();
            let selection = ShareSelection {
                env: BTreeMap::from([(variant.clone(), false)]),
                ..Default::default()
            };
            assert_eq!(select_bundle(&source, &selection), Err(ShareError::Unsafe));
            let choices = ShareDecisions {
                env: BTreeMap::from([(
                    "AppToken".into(),
                    ShareConflictChoice::Rename { target: variant },
                )]),
                ..Default::default()
            };
            assert_eq!(
                plan_import(&benign, &Config::default(), &choices),
                Err(ShareError::Unsafe)
            );
            assert_eq!(serde_json::to_vec(&source).unwrap(), before);
        }
    }
    for name in [
        "APP_BUNDLE_GEMFILE",
        "APP_GEM_PATH",
        "APP_PACKAGE_MODE",
        "APP_PIP_EDITOR",
        "APP_UV_PYTHON",
        "BUNDLECOUNT",
        "GEMSTONE",
        "GEMRC_LABEL",
        "RBENVLABEL",
        "RAKELIB", // Current Rake reads the CLI option, not this env name.
        "RAKEOPTIONAL",
        "PIPCOUNT",
        "UVCOUNT",
        "PIPENVCOUNT",
        "POETRYCOUNT",
        "GRADLECOUNT",
        "MAVENCOUNT",
        "ANTCOUNT",
        "COMPOSERCOUNT",
        "JAVACMD_LABEL",
        "YARNCOUNT",
        "BUNCOUNT",
        "COREPACKCOUNT",
        "PNPMCOUNT",
        "DENOCOUNT",
        "LANG",
    ] {
        for variant in [name.to_string(), name.to_ascii_lowercase()] {
            assert_eq!(validate_env_name(&variant), Ok(()));
            let source = env_target(&[&variant]);
            let before = serde_json::to_vec(&source).unwrap();
            let selection = ShareSelection {
                env: BTreeMap::from([(variant.clone(), false)]),
                ..Default::default()
            };
            let selected = select_bundle(&source, &selection).unwrap();
            assert_eq!(selected.env[0].name, variant);
            assert_eq!(selected.env[0].value, None);
            let encoded = encode_bundle(&selected, None).unwrap();
            let decoded = decode_bundle(&encoded, None).unwrap();
            assert_eq!(decoded, selected);
            assert_eq!(
                plan_import(&decoded, &Config::default(), &ShareDecisions::default()).unwrap(),
                selected
            );
            let choices = ShareDecisions {
                env: BTreeMap::from([(
                    "AppToken".into(),
                    ShareConflictChoice::Rename {
                        target: variant.clone(),
                    },
                )]),
                ..Default::default()
            };
            let renamed = plan_import(&benign, &Config::default(), &choices).unwrap();
            assert_eq!(renamed.env[0].name, variant);
            assert_eq!(serde_json::to_vec(&source).unwrap(), before);
        }
    }
    assert_eq!(benign, original_benign);
    assert_eq!(codec::test_kdf_calls(), kdf_calls);
}
