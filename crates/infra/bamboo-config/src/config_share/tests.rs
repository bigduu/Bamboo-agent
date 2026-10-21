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
