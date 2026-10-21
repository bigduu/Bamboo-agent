use super::*;
use std::collections::BTreeMap;

use crate::config_share::{ShareConflictChoice, ShareEnvVar, ShareProvider, ShareProviderType};

fn bytes(data: serde_json::Value, revision: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"schema_version":1,"revision":revision,"data":data}))
        .unwrap()
}

fn snapshot() -> ShareTransactionSnapshot {
    ShareTransactionSnapshot {
        providers: Some(bytes(
            serde_json::json!({
                "provider":"relay", "default_provider_instance":"relay",
                "provider_instances":{"relay":{
                    "provider_type":"openai", "label":"Saved label", "base_url":"https://saved.invalid/v1",
                    "enabled":true, "model":"good", "runtime_models":["good"], "saved_custom_option":true
                }}
            }),
            7,
        )),
        env: Some(bytes(serde_json::json!([]), 3)),
        credentials: Some(bytes(serde_json::json!({"entries":{}}), 2)),
        revisions: ShareRevisions {
            providers: 7,
            env: 3,
            credentials: 2,
        },
    }
}

fn provider() -> ShareProvider {
    ShareProvider {
        id: "relay".into(),
        provider_type: ShareProviderType::Openai,
        label: None,
        base_url: None,
        api_key: None,
        model: None,
        fast_model: None,
        vision_model: None,
        reasoning_effort: None,
        responses_only_models: Vec::new(),
        runtime_models: None,
    }
}

fn bundle() -> ShareBundle {
    ShareBundle {
        providers: vec![provider()],
        ..Default::default()
    }
}

fn choices() -> ShareDecisions {
    ShareDecisions {
        providers: BTreeMap::from([("relay".into(), ShareConflictChoice::Replace)]),
        ..Default::default()
    }
}

#[test]
fn omitted_fields_preserve_destination_and_revision() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let original = snapshot();
    let candidate = prepare_import(&bundle(), &choices(), &original).unwrap();
    let providers: serde_json::Value = serde_json::from_slice(&candidate.providers).unwrap();
    let saved: serde_json::Value =
        serde_json::from_slice(original.providers.as_deref().unwrap()).unwrap();
    assert_eq!(providers, saved);
    let env: serde_json::Value = serde_json::from_slice(&candidate.env).unwrap();
    assert_eq!(env["revision"], 3);
    let credentials: serde_json::Value = serde_json::from_slice(&candidate.credentials).unwrap();
    assert_eq!(credentials["revision"], 2);
}

#[test]
fn adding_env_preserves_saved_unmodeled_fields_without_restoring_upgraded_plaintext() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x48; 32]);
    let mut original = snapshot();
    let saved = serde_json::json!([
        {"name":"APP_MODE", "value":"synthetic-mode", "secret":false, "configured":true,
            "description":null, "future_hint":{"source":"synthetic", "enabled":true}},
        {"name":"APP_UPGRADE", "value":"synthetic-old-cleartext", "secret":false,
            "configured":true, "future_hint":["synthetic-retained-hint"]}
    ]);
    original.env = Some(bytes(saved.clone(), 3));
    let imported = ShareBundle {
        env: vec![
            ShareEnvVar {
                name: "APP_OTHER".into(),
                value: Some("synthetic-added".into()),
                secret: false,
            },
            ShareEnvVar {
                name: "APP_UPGRADE".into(),
                value: Some("synthetic-upgraded-secret".into()),
                secret: true,
            },
        ],
        ..Default::default()
    };
    let decisions = ShareDecisions {
        env: BTreeMap::from([("APP_UPGRADE".into(), ShareConflictChoice::Replace)]),
        ..Default::default()
    };
    let candidate = prepare_import(&imported, &decisions, &original).unwrap();
    let env: serde_json::Value = serde_json::from_slice(&candidate.env).unwrap();
    assert_eq!(env["revision"], 4);
    assert_eq!(env["data"].as_array().unwrap().len(), 3);
    assert_eq!(env["data"][0], saved[0]);
    assert_eq!(env["data"][1]["future_hint"], saved[1]["future_hint"]);
    assert_eq!(env["data"][1]["name"], "APP_UPGRADE");
    assert_eq!(env["data"][1]["secret"], true);
    assert!(env["data"][1].get("value").is_none());
    assert_eq!(env["data"][2]["name"], "APP_OTHER");
    assert_eq!(env["data"][2]["value"], "synthetic-added");
    assert_eq!(candidate.providers, original.providers.unwrap());
    assert!(!String::from_utf8_lossy(&candidate.env).contains("synthetic-old-cleartext"));
    assert!(!String::from_utf8_lossy(&candidate.env).contains("synthetic-upgraded-secret"));
    let credentials = ShareCredentialSnapshot::from_bytes(Some(&candidate.credentials)).unwrap();
    let reference =
        crate::CredentialRef::parse(env["data"][1]["credential_ref"].as_str().unwrap()).unwrap();
    assert_eq!(
        credentials.resolve_selected(&reference).unwrap(),
        "synthetic-upgraded-secret"
    );
}

#[test]
fn provider_label_edit_preserves_saved_feature_default_and_modelref_metadata() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let mut original = snapshot();
    let mut saved: serde_json::Value =
        serde_json::from_slice(original.providers.as_deref().unwrap()).unwrap();
    saved["data"]["features"] = serde_json::json!({
        "provider_model_ref":true, "future_toggle":{"mode":"synthetic"}
    });
    saved["data"]["defaults"] = serde_json::json!({
        "chat":{"provider":"relay", "model":"good", "future_route":{"hint":"synthetic"}},
        "fast":null, "future_policy":{"enabled":true},
        "sub_session":{"provider":"relay", "model":"good", "future_agent_route":"synthetic"}
    });
    original.providers = Some(serde_json::to_vec(&saved).unwrap());
    // Normal sparse/default/null data remains admissible, with exact no-op bytes.
    let unchanged = prepare_import(&bundle(), &choices(), &original).unwrap();
    assert_eq!(unchanged.providers, original.providers.clone().unwrap());
    let mut imported = bundle();
    imported.providers[0].label = Some("Imported synthetic label".into());
    let candidate = prepare_import(&imported, &choices(), &original).unwrap();
    let providers: serde_json::Value = serde_json::from_slice(&candidate.providers).unwrap();
    saved["revision"] = serde_json::json!(8);
    saved["data"]["provider_instances"]["relay"]["label"] =
        serde_json::json!("Imported synthetic label");
    assert_eq!(providers, saved);
    assert_eq!(candidate.env.as_slice(), original.env.as_deref().unwrap());
    assert_eq!(
        candidate.credentials.as_slice(),
        original.credentials.as_deref().unwrap()
    );

    // A later role edit must not reinsert the legacy alias beside sub_agent.
    imported.scenarios.insert(
        ShareScenario::Fast,
        ShareModelRef {
            provider: "relay".into(),
            model: "good".into(),
            reasoning_effort: None,
        },
    );
    let candidate = prepare_import(&imported, &choices(), &original).unwrap();
    let providers: serde_json::Value = serde_json::from_slice(&candidate.providers).unwrap();
    let defaults = saved["data"]["defaults"].as_object_mut().unwrap();
    let sub_agent = defaults.remove("sub_session").unwrap();
    defaults.insert("sub_agent".into(), sub_agent);
    defaults.insert(
        "fast".into(),
        serde_json::json!({"provider":"relay", "model":"good"}),
    );
    assert_eq!(providers, saved);
}

#[test]
fn provider_key_replacement_never_restores_read_only_plaintext() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x49; 32]);
    let mut original = snapshot();
    let mut saved: serde_json::Value =
        serde_json::from_slice(original.providers.as_deref().unwrap()).unwrap();
    let relay = &mut saved["data"]["provider_instances"]["relay"];
    relay["api_key"] = serde_json::json!("synthetic-old-provider-plaintext");
    relay["api_key_encrypted"] = serde_json::json!("synthetic-old-provider-inline");
    relay["api_key_from_env"] = serde_json::json!(true);
    relay["future_hint"] = serde_json::json!({"api_key":false});
    for name in ["openai", "anthropic", "gemini", "bodhi"] {
        saved["data"][name] = serde_json::json!({"api_key":""});
        if name != "bodhi" {
            saved["data"][name]["api_key_from_env"] = serde_json::json!(true);
        }
    }
    original.providers = Some(serde_json::to_vec(&saved).unwrap());
    let mut imported = bundle();
    imported.providers[0].api_key = Some("synthetic-new-provider-secret".into());
    let candidate = prepare_import(&imported, &choices(), &original).unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("providers.json"), &candidate.providers).unwrap();
    std::fs::write(dir.path().join("credentials.json"), &candidate.credentials).unwrap();
    let persisted = std::fs::read(dir.path().join("providers.json")).unwrap();
    let providers: serde_json::Value = serde_json::from_slice(&persisted).unwrap();
    let relay = &providers["data"]["provider_instances"]["relay"];
    assert!(relay.get("api_key").is_none());
    assert!(relay.get("api_key_encrypted").is_none());
    assert_eq!(relay["api_key_from_env"], true);
    assert_eq!(relay["future_hint"]["api_key"], false);
    for name in ["openai", "anthropic", "gemini", "bodhi"] {
        assert!(providers["data"][name].get("api_key").is_none());
        assert!(providers["data"][name].get("api_key_from_env").is_none());
    }
    for value in [
        "synthetic-old-provider-plaintext",
        "synthetic-old-provider-inline",
        "synthetic-new-provider-secret",
    ] {
        assert!(!String::from_utf8_lossy(&persisted).contains(value));
    }
    let reference = crate::CredentialRef::parse(relay["credential_ref"].as_str().unwrap()).unwrap();
    let credential_bytes = std::fs::read(dir.path().join("credentials.json")).unwrap();
    let credentials = ShareCredentialSnapshot::from_bytes(Some(&credential_bytes)).unwrap();
    assert_eq!(
        credentials.resolve_selected(&reference).unwrap(),
        "synthetic-new-provider-secret"
    );
}

#[test]
fn env_value_replacement_never_restores_inline_ciphertext_or_previous_binding() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x50; 32]);
    let mut original = snapshot();
    let old_reference = crate::CredentialRef::parse("env.synthetic.previous.value").unwrap();
    let mut credentials =
        ShareCredentialSnapshot::from_bytes(original.credentials.as_deref()).unwrap();
    credentials
        .insert_share_secret(old_reference.clone(), "synthetic-old-stored-secret")
        .unwrap();
    original.credentials = Some(credentials.encode().unwrap());
    original.revisions.credentials += 1;
    original.env = Some(bytes(
        serde_json::json!([{
            "name":"APP_TOKEN", "value":"synthetic-old-env-plaintext", "secret":true,
            "configured":true, "credential_ref":old_reference,
            "value_encrypted":"synthetic-old-env-inline-ciphertext",
            "future_hint":{"value_encrypted":false}
        }]),
        3,
    ));
    let imported = ShareBundle {
        env: vec![ShareEnvVar {
            name: "APP_TOKEN".into(),
            value: Some("synthetic-new-env-secret".into()),
            secret: false,
        }],
        ..Default::default()
    };
    let decisions = ShareDecisions {
        env: BTreeMap::from([("APP_TOKEN".into(), ShareConflictChoice::Replace)]),
        ..Default::default()
    };
    let candidate = prepare_import(&imported, &decisions, &original).unwrap();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("env.json"), &candidate.env).unwrap();
    std::fs::write(dir.path().join("credentials.json"), &candidate.credentials).unwrap();
    let persisted = std::fs::read(dir.path().join("env.json")).unwrap();
    let env: serde_json::Value = serde_json::from_slice(&persisted).unwrap();
    let entry = &env["data"][0];
    assert!(entry.get("value").is_none());
    assert!(entry.get("value_encrypted").is_none());
    assert_eq!(entry["future_hint"]["value_encrypted"], false);
    assert_eq!(entry["secret"], true);
    assert_eq!(entry["configured"], true);
    for value in [
        "synthetic-old-env-plaintext",
        "synthetic-old-env-inline-ciphertext",
        "synthetic-new-env-secret",
        old_reference.as_str(),
    ] {
        assert!(!String::from_utf8_lossy(&persisted).contains(value));
    }
    let reference = crate::CredentialRef::parse(entry["credential_ref"].as_str().unwrap()).unwrap();
    assert_ne!(reference, old_reference);
    let credential_bytes = std::fs::read(dir.path().join("credentials.json")).unwrap();
    let credentials = ShareCredentialSnapshot::from_bytes(Some(&credential_bytes)).unwrap();
    assert_eq!(
        credentials.resolve_selected(&reference).unwrap(),
        "synthetic-new-env-secret"
    );
    // Removing an old config binding never deletes its stored credential.
    assert_eq!(
        credentials.resolve_selected(&old_reference).unwrap(),
        "synthetic-old-stored-secret"
    );
}

#[test]
fn provider_type_change_is_rejected_without_reusing_old_key() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let mut imported = bundle();
    imported.providers[0].provider_type = ShareProviderType::Anthropic;
    assert!(matches!(
        prepare_import(&imported, &choices(), &snapshot()),
        Err(ShareApiError::Share(ShareError::Invalid))
    ));
}

#[test]
fn type_changing_replace_preview_disallows_apply_without_changing_saved_bytes() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x51; 32]);
    let dir = fixture();
    let mut imported = bundle();
    imported.providers[0].provider_type = ShareProviderType::Anthropic;
    // Omitting a URL must not allow a new provider type to reuse its target.
    assert!(imported.providers[0].base_url.is_none());
    assert!(matches!(
        config_share::plan_import(&imported, &target(&snapshot()).unwrap(), &choices()),
        Err(ShareError::Invalid)
    ));
    let string = config_share::encode_bundle(&imported, None).unwrap();
    let before = tree(dir.path());
    let preview = preview_config_share(dir.path(), &string, None).unwrap();
    assert!(!preview.apply_blocked); // Skip and Rename remain valid choices.
    assert_eq!(preview.preview.providers.len(), 1);
    let provider = &preview.preview.providers[0];
    assert!(provider.conflict);
    assert!(!provider.has_base_url);
    assert!(!provider.credential_origin_risk);
    assert!(!provider.replace_allowed);
    assert_tree_unchanged(&before, dir.path());
    let operation_id = Uuid::new_v4();
    assert!(matches!(
        apply_config_share(
            dir.path(),
            &string,
            None,
            &choices(),
            preview.expected_revisions,
            &preview.preview_token,
            operation_id,
        ),
        Err(ShareApiError::Invalid)
    ));
    assert!(matches!(
        share_transaction::read_share_operation(dir.path(), operation_id).unwrap(),
        ShareOperationStatus::NotFound { operation_id: id } if id == operation_id
    ));
    assert_tree_unchanged(&before, dir.path());
}

#[test]
fn merged_roles_must_still_be_admitted_to_runtime_models() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let mut imported = bundle();
    imported.scenarios.insert(
        ShareScenario::Chat,
        ShareModelRef {
            provider: "relay".into(),
            model: "unadmitted".into(),
            reasoning_effort: None,
        },
    );
    assert!(matches!(
        prepare_import(&imported, &choices(), &snapshot()),
        Err(ShareApiError::Invalid)
    ));
}

#[test]
fn generated_credential_reference_and_destination_encryption_are_local() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x27; 32]);
    let mut imported = bundle();
    imported.providers[0].api_key = Some("synthetic-api-key-canary".into());
    let candidate = prepare_import(&imported, &choices(), &snapshot()).unwrap();
    let providers: serde_json::Value = serde_json::from_slice(&candidate.providers).unwrap();
    let raw_ref = providers["data"]["provider_instances"]["relay"]["credential_ref"]
        .as_str()
        .unwrap();
    assert!(raw_ref.starts_with("provider."));
    assert!(!String::from_utf8(candidate.providers.clone())
        .unwrap()
        .contains("synthetic-api-key-canary"));
    assert!(!String::from_utf8(candidate.credentials.clone())
        .unwrap()
        .contains("synthetic-api-key-canary"));
    let credentials = ShareCredentialSnapshot::from_bytes(Some(&candidate.credentials)).unwrap();
    assert_eq!(
        credentials
            .resolve_selected(&crate::CredentialRef::parse(raw_ref).unwrap())
            .unwrap(),
        "synthetic-api-key-canary"
    );
    let stored: serde_json::Value = serde_json::from_slice(&candidate.credentials).unwrap();
    assert_eq!(stored["revision"], 3);
}

#[test]
fn missing_import_values_never_clear_existing_secret_or_create_one() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x28; 32]);
    let mut original = snapshot();
    let reference = crate::CredentialRef::parse("env.saved.value").unwrap();
    let mut credentials =
        ShareCredentialSnapshot::from_bytes(original.credentials.as_deref()).unwrap();
    credentials
        .insert_share_secret(reference.clone(), "synthetic-env-canary")
        .unwrap();
    original.credentials = Some(credentials.encode().unwrap());
    original.revisions.credentials += 1;
    original.env = Some(bytes(
        serde_json::json!([{"name":"APP_TOKEN","secret":true,"configured":true,"credential_ref":reference}]),
        3,
    ));
    let imported = ShareBundle {
        env: vec![
            ShareEnvVar {
                name: "APP_TOKEN".into(),
                value: None,
                secret: false,
            },
            ShareEnvVar {
                name: "APP_EMPTY".into(),
                value: None,
                secret: true,
            },
        ],
        ..Default::default()
    };
    let decisions = ShareDecisions {
        env: BTreeMap::from([("APP_TOKEN".into(), ShareConflictChoice::Replace)]),
        ..Default::default()
    };
    let candidate = prepare_import(&imported, &decisions, &original).unwrap();
    let env: serde_json::Value = serde_json::from_slice(&candidate.env).unwrap();
    assert_eq!(env["data"][0]["credential_ref"], "env.saved.value");
    assert_eq!(env["data"][0]["secret"], true);
    assert_eq!(env["data"][1]["configured"], false);
    assert!(env["data"][1].get("credential_ref").is_none());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&candidate.credentials).unwrap(),
        serde_json::from_slice::<serde_json::Value>(original.credentials.as_deref().unwrap())
            .unwrap()
    );
}

#[test]
fn portable_environment_case_replace_preserves_one_secret_destination_and_key_precheck() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x29; 32]);
    let mut original = snapshot();
    let reference = crate::CredentialRef::parse("env.synthetic.case.value").unwrap();
    let mut credentials =
        ShareCredentialSnapshot::from_bytes(original.credentials.as_deref()).unwrap();
    credentials
        .insert_share_secret(reference.clone(), "synthetic-saved-case-canary")
        .unwrap();
    original.credentials = Some(credentials.encode().unwrap());
    original.revisions.credentials += 1;
    original.env = Some(bytes(
        serde_json::json!([{
            "name":"Token", "secret":true, "configured":true,
            "credential_ref":reference, "description":"Saved description"
        }]),
        3,
    ));
    let before = (
        original.providers.clone(),
        original.env.clone(),
        original.credentials.clone(),
    );
    let imported = ShareBundle {
        env: vec![ShareEnvVar {
            name: "TOKEN".into(),
            value: Some("synthetic-imported-case-canary".into()),
            // The destination's classification must survive this input.
            secret: false,
        }],
        ..Default::default()
    };
    assert!(requires_local_key(&imported, &target(&original).unwrap()));
    let decisions = ShareDecisions {
        env: BTreeMap::from([("tOkEn".into(), ShareConflictChoice::Replace)]),
        ..Default::default()
    };
    let candidate = prepare_import(&imported, &decisions, &original).unwrap();
    assert_eq!(candidate.providers, original.providers.clone().unwrap());
    let env: serde_json::Value = serde_json::from_slice(&candidate.env).unwrap();
    let entries = env["data"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], "Token");
    assert_eq!(entries[0]["secret"], true);
    assert_eq!(entries[0]["configured"], true);
    assert_eq!(entries[0]["description"], "Saved description");
    assert!(entries[0].get("value").is_none());
    let new_reference = entries[0]["credential_ref"].as_str().unwrap();
    assert_ne!(new_reference, "env.synthetic.case.value");
    let credentials = ShareCredentialSnapshot::from_bytes(Some(&candidate.credentials)).unwrap();
    assert_eq!(
        credentials
            .resolve_selected(&crate::CredentialRef::parse(new_reference).unwrap())
            .unwrap(),
        "synthetic-imported-case-canary"
    );
    for bytes in [&candidate.env, &candidate.credentials] {
        assert!(!String::from_utf8_lossy(bytes).contains("synthetic-imported-case-canary"));
    }
    assert!(
        before
            == (
                original.providers.clone(),
                original.env.clone(),
                original.credentials.clone(),
            ),
        "planning changed its supplied snapshot"
    );
}

#[test]
fn runtime_boundary_rejects_legacy_unchanged_copilot_and_provider_override() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let mut current = target(&snapshot()).unwrap();
    assert!(reject_network_initialization(&current).is_ok());
    current.provider_instances.clear();
    assert!(matches!(
        reject_network_initialization(&current),
        Err(ShareApiError::RuntimeBoundary)
    ));
    current = target(&snapshot()).unwrap();
    current
        .provider_instances
        .get_mut("relay")
        .unwrap()
        .provider_type = "copilot".into();
    assert!(matches!(
        reject_network_initialization(&current),
        Err(ShareApiError::RuntimeBoundary)
    ));
    current = target(&snapshot()).unwrap();
    current.provider = "copilot".into();
    current.default_provider_instance = None;
    assert!(matches!(
        reject_network_initialization(&current),
        Err(ShareApiError::RuntimeBoundary)
    ));
    let _override = crate::test_support::override_runtime_env_var(
        "BAMBOO_PROVIDER",
        Some("synthetic-selector"),
    );
    assert!(matches!(
        reject_network_initialization(&target(&snapshot()).unwrap()),
        Err(ShareApiError::RuntimeBoundary)
    ));
}

#[test]
fn preview_binding_detects_same_revision_edits_and_missing_present_aba() {
    let initial = snapshot();
    let initial_token = preview_token("synthetic-share-string", &initial);
    let mut edited = snapshot();
    edited.env = Some(bytes(
        serde_json::json!([{"name":"APP_MODE","value":"changed"}]),
        3,
    ));
    assert_ne!(
        initial_token,
        preview_token("synthetic-share-string", &edited)
    );
    let mut absent = snapshot();
    absent.env = None;
    assert_ne!(
        initial_token,
        preview_token("synthetic-share-string", &absent)
    );
    assert_ne!(
        initial_token,
        preview_token("different-share-string", &initial)
    );
}

#[test]
fn operation_fingerprint_excludes_password_and_binds_choices_and_revisions() {
    let initial = snapshot();
    let a =
        operation_fingerprint("encrypted-string", &choices(), &initial.revisions, "token").unwrap();
    let b =
        operation_fingerprint("encrypted-string", &choices(), &initial.revisions, "token").unwrap();
    assert_eq!(a, b);
    let mut different = choices();
    different
        .providers
        .insert("relay".into(), ShareConflictChoice::Skip);
    assert_ne!(
        a,
        operation_fingerprint("encrypted-string", &different, &initial.revisions, "token").unwrap()
    );
    assert_ne!(
        a,
        operation_fingerprint(
            "different-encrypted-string",
            &choices(),
            &initial.revisions,
            "token"
        )
        .unwrap()
    );
}

#[test]
fn initialized_crypto_has_no_implicit_fallback_and_round_trips_synthetic_values() {
    assert!(crate::encryption::require_initialized_key().is_err());
    assert!(crate::encryption::encrypt_initialized("synthetic-value").is_err());
    let _key = crate::encryption::set_test_encryption_key([0x39; 32]);
    let encoded = crate::encryption::encrypt_initialized("synthetic-value").unwrap();
    assert_eq!(
        crate::encryption::decrypt_initialized(&encoded).unwrap(),
        "synthetic-value"
    );
}

#[test]
fn unchanged_missing_empty_env_stays_revision_zero() {
    let section = Stored {
        schema_version: 1,
        revision: 0,
        data: EnvSection::default(),
    };
    let candidate = encode_changed_section(ShareStoredSection::Env, section, None).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&candidate).unwrap();
    assert_eq!(value["revision"], 0);
}

#[test]
fn non_chat_mapping_without_saved_defaults_is_rejected_without_inventing_chat() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let mut imported = bundle();
    imported.scenarios.insert(
        ShareScenario::Fast,
        ShareModelRef {
            provider: "relay".into(),
            model: "good".into(),
            reasoning_effort: None,
        },
    );
    assert!(matches!(
        prepare_import(&imported, &choices(), &snapshot()),
        Err(ShareApiError::Invalid)
    ));
    assert!(target(&snapshot()).unwrap().defaults.is_none());
}

fn fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("config.json"), b"{}").unwrap();
    drop(crate::ConfigFacade::open_or_migrate(dir.path()).unwrap());
    let saved = snapshot();
    for (name, bytes) in [
        ("providers.json", saved.providers),
        ("env.json", saved.env),
        ("credentials.json", saved.credentials),
    ] {
        std::fs::write(dir.path().join(name), bytes.unwrap()).unwrap();
    }
    dir
}

fn tree(dir: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn walk(base: &Path, current: &Path, files: &mut BTreeMap<std::path::PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(current).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                walk(base, &entry.path(), files);
            } else {
                files.insert(
                    entry.path().strip_prefix(base).unwrap().to_path_buf(),
                    std::fs::read(entry.path()).unwrap(),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    walk(dir, dir, &mut files);
    files
}

fn assert_tree_unchanged(before: &BTreeMap<std::path::PathBuf, Vec<u8>>, dir: &Path) {
    assert!(
        before == &tree(dir),
        "share API changed the saved configuration tree"
    );
}

#[test]
fn unsafe_or_case_duplicate_environment_bundles_are_rejected_before_any_api_write() {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};

    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x45; 32]);
    let dir = fixture();
    let before = tree(dir.path());
    for (names, unsafe_name) in [
        (vec!["GIT_ASKPASS"], true),
        (vec!["git_askpass"], true),
        (vec!["Git_AskPass"], true),
        (vec!["CCACHE_PREFIX"], true),
        (vec!["ccache_prefix_cpp"], true),
        (vec!["CCACHE_PATH"], true),
        (vec!["SCCACHE_CONF"], true),
        (vec!["sccache_conf"], true),
        (vec!["DISTCC_SSH"], true),
        (vec!["ICECC_CC"], true),
        (vec!["icecc_cxx"], true),
        (vec!["BUNDLE_GEMFILE"], true),
        (vec!["bundle_gemfile"], true),
        (vec!["BUNDLE_APP_CONFIG"], true),
        (vec!["GEM_HOME"], true),
        (vec!["gem_path"], true),
        (vec!["GEMRC"], true),
        (vec!["RBENV_HOOK_PATH"], true),
        (vec!["RAKEOPT"], true),
        (vec!["rake_system"], true),
        (vec!["PIP_EDITOR"], true),
        (vec!["PIPENV_PYTHON"], true),
        (vec!["POETRY_CONFIG_DIR"], true),
        (vec!["UV_PYTHON"], true),
        (vec!["UV_ENV_FILE"], true),
        (vec!["GRADLE_USER_HOME"], true),
        (vec!["MAVEN_OPTS"], true),
        (vec!["ANT_OPTS"], true),
        (vec!["JAVACMD"], true),
        (vec!["COMPOSER"], true),
        (vec!["COMPOSER_HOME"], true),
        (vec!["YARN_YARN_PATH"], true),
        (vec!["BUN_OPTIONS"], true),
        (vec!["COREPACK_HOME"], true),
        (vec!["PNPM_CONFIG_SCRIPT_SHELL"], true),
        (vec!["DENO_V8_FLAGS"], true),
        (vec!["Token", "TOKEN"], false),
    ] {
        // Names are inert data. Construct an untrusted envelope directly so
        // this exercises the inbound boundary rather than the safe exporter.
        let imported = ShareBundle {
            env: names
                .into_iter()
                .map(|name| ShareEnvVar {
                    name: name.into(),
                    value: None,
                    secret: false,
                })
                .collect(),
            ..Default::default()
        };
        let envelope = serde_json::json!({"mode":"plain", "bundle":imported});
        let string = format!(
            "{}{}",
            config_share::SHARE_PREFIX,
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&envelope).unwrap())
        );
        let preview = preview_config_share(dir.path(), &string, None);
        let apply = apply_config_share(
            dir.path(),
            &string,
            None,
            &ShareDecisions::default(),
            snapshot().revisions,
            &"0".repeat(64),
            Uuid::new_v4(),
        );
        if unsafe_name {
            assert!(matches!(
                preview,
                Err(ShareApiError::Share(ShareError::Unsafe))
            ));
            assert!(matches!(
                apply,
                Err(ShareApiError::Share(ShareError::Unsafe))
            ));
        } else {
            assert!(matches!(
                preview,
                Err(ShareApiError::Share(ShareError::Invalid))
            ));
            assert!(matches!(
                apply,
                Err(ShareApiError::Share(ShareError::Invalid))
            ));
        }
        assert_tree_unchanged(&before, dir.path());
    }
}

#[test]
fn portable_environment_preview_warns_when_case_replace_requires_uninitialized_key() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let dir = {
        let _key = crate::encryption::set_test_encryption_key([0x47; 32]);
        fixture()
    };
    assert!(crate::encryption::require_initialized_key().is_err());
    std::fs::write(
        dir.path().join("env.json"),
        bytes(
            serde_json::json!([{"name":"Token", "secret":true, "configured":false}]),
            3,
        ),
    )
    .unwrap();
    let imported = ShareBundle {
        env: vec![ShareEnvVar {
            name: "TOKEN".into(),
            value: Some("synthetic-preview-case-canary".into()),
            secret: false,
        }],
        ..Default::default()
    };
    let passphrase = "synthetic preview case passphrase";
    let string = config_share::encode_bundle(&imported, Some(passphrase)).unwrap();
    let before = tree(dir.path());
    let preview = preview_config_share(dir.path(), &string, Some(passphrase)).unwrap();
    assert!(preview.preview.env[0].conflict);
    assert!(preview.apply_blocked);
    assert!(preview
        .warnings
        .iter()
        .any(|warning| matches!(warning, ShareWarning::CredentialKeyUninitialized)));
    assert!(!serde_json::to_string(&preview)
        .unwrap()
        .contains("synthetic-preview-case-canary"));
    assert_tree_unchanged(&before, dir.path());
}

#[test]
fn portable_environment_api_requires_case_conflict_choice_and_preserves_target_spelling() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x46; 32]);
    let dir = fixture();
    std::fs::write(
        dir.path().join("env.json"),
        bytes(
            serde_json::json!([{"name":"Token", "value":"synthetic-saved-case-value", "secret":false, "configured":true}]),
            3,
        ),
    )
    .unwrap();
    let imported = ShareBundle {
        env: vec![ShareEnvVar {
            name: "TOKEN".into(),
            value: None,
            secret: false,
        }],
        ..Default::default()
    };
    let string = config_share::encode_bundle(&imported, None).unwrap();
    let before = tree(dir.path());
    let preview = preview_config_share(dir.path(), &string, None).unwrap();
    assert!(!preview.apply_blocked);
    assert_eq!(preview.preview.env.len(), 1);
    assert!(preview.preview.env[0].conflict);
    assert_tree_unchanged(&before, dir.path());
    assert!(matches!(
        apply_config_share(
            dir.path(),
            &string,
            None,
            &ShareDecisions::default(),
            preview.expected_revisions,
            &preview.preview_token,
            Uuid::new_v4(),
        ),
        Err(ShareApiError::Invalid)
    ));
    assert_tree_unchanged(&before, dir.path());
    let decisions = ShareDecisions {
        env: BTreeMap::from([("token".into(), ShareConflictChoice::Replace)]),
        ..Default::default()
    };
    assert!(matches!(
        apply_config_share(
            dir.path(),
            &string,
            None,
            &decisions,
            preview.expected_revisions,
            &preview.preview_token,
            Uuid::new_v4(),
        )
        .unwrap(),
        ShareOperationStatus::Committed { .. }
    ));
    assert_eq!(
        std::fs::read(dir.path().join("env.json")).unwrap(),
        before[Path::new("env.json")]
    );
}

#[test]
fn preview_cancel_is_read_only_and_apply_replay_returns_original_receipt() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x41; 32]);
    let dir = fixture();
    let mut imported = bundle();
    imported.providers[0].label = Some("Imported synthetic label".into());
    let string = config_share::encode_bundle(&imported, None).unwrap();
    let before = tree(dir.path());
    let preview = preview_config_share(dir.path(), &string, None).unwrap();
    assert!(!preview.apply_blocked);
    assert_tree_unchanged(&before, dir.path()); // cancelling here needs no rollback
    let id = Uuid::new_v4();
    let receipt = apply_config_share(
        dir.path(),
        &string,
        None,
        &choices(),
        preview.expected_revisions,
        &preview.preview_token,
        id,
    )
    .unwrap();
    assert!(matches!(receipt, ShareOperationStatus::Committed { .. }));
    let after = tree(dir.path());
    let replay = apply_config_share(
        dir.path(),
        &string,
        None,
        &choices(),
        preview.expected_revisions,
        &preview.preview_token,
        id,
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        serde_json::to_value(&replay).unwrap()
    );
    assert_tree_unchanged(&after, dir.path());
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        serde_json::to_value(share_transaction::read_share_operation(dir.path(), id).unwrap())
            .unwrap()
    );
}

#[test]
fn same_revision_edit_rejects_preview_token_before_any_import_write() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x42; 32]);
    let dir = fixture();
    let mut imported = bundle();
    imported.providers[0].label = Some("Imported synthetic label".into());
    let string = config_share::encode_bundle(&imported, None).unwrap();
    let preview = preview_config_share(dir.path(), &string, None).unwrap();
    let mut changed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("providers.json")).unwrap()).unwrap();
    changed["data"]["provider_instances"]["relay"]["label"] = serde_json::json!("Edited locally");
    std::fs::write(
        dir.path().join("providers.json"),
        serde_json::to_vec(&changed).unwrap(),
    )
    .unwrap();
    let before = tree(dir.path());
    assert!(matches!(
        apply_config_share(
            dir.path(),
            &string,
            None,
            &choices(),
            preview.expected_revisions,
            &preview.preview_token,
            Uuid::new_v4()
        ),
        Err(ShareApiError::Conflict)
    ));
    assert_tree_unchanged(&before, dir.path());
}

#[test]
fn explicit_sensitive_export_preview_apply_replay_and_query_use_only_synthetic_store() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x43; 32]);
    let dir = fixture();
    let reference = crate::CredentialRef::parse("provider.synthetic.api_key").unwrap();
    let credential_bytes = std::fs::read(dir.path().join("credentials.json")).unwrap();
    let mut credentials = ShareCredentialSnapshot::from_bytes(Some(&credential_bytes)).unwrap();
    credentials
        .insert_share_secret(reference.clone(), "synthetic-export-canary")
        .unwrap();
    std::fs::write(
        dir.path().join("credentials.json"),
        credentials.encode().unwrap(),
    )
    .unwrap();
    let mut providers: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("providers.json")).unwrap()).unwrap();
    providers["data"]["provider_instances"]["relay"]["credential_ref"] =
        serde_json::json!(reference);
    std::fs::write(
        dir.path().join("providers.json"),
        serde_json::to_vec(&providers).unwrap(),
    )
    .unwrap();
    let selection = ShareSelection {
        providers: std::collections::BTreeSet::from(["relay".into()]),
        provider_api_keys: std::collections::BTreeSet::from(["relay".into()]),
        ..Default::default()
    };
    assert!(matches!(
        export_config_share(
            dir.path(),
            &selection,
            false,
            Some("synthetic strong passphrase")
        ),
        Err(ShareApiError::Invalid)
    ));
    let string = export_config_share(
        dir.path(),
        &selection,
        true,
        Some("synthetic strong passphrase"),
    )
    .unwrap();
    assert!(!string.contains("synthetic-export-canary"));
    let decoded =
        config_share::decode_bundle(&string, Some("synthetic strong passphrase")).unwrap();
    assert_eq!(
        decoded.providers[0].api_key.as_deref(),
        Some("synthetic-export-canary")
    );
    let before = tree(dir.path());
    let preview =
        preview_config_share(dir.path(), &string, Some("synthetic strong passphrase")).unwrap();
    assert!(!preview.apply_blocked);
    assert_tree_unchanged(&before, dir.path());
    assert!(!serde_json::to_string(&preview)
        .unwrap()
        .contains("synthetic-export-canary"));
    let id = Uuid::new_v4();
    let receipt = apply_config_share(
        dir.path(),
        &string,
        Some("synthetic strong passphrase"),
        &choices(),
        preview.expected_revisions,
        &preview.preview_token,
        id,
    )
    .unwrap();
    assert!(matches!(receipt, ShareOperationStatus::Committed { .. }));
    let after = tree(dir.path());
    let replay = apply_config_share(
        dir.path(),
        &string,
        Some("synthetic strong passphrase"),
        &choices(),
        preview.expected_revisions,
        &preview.preview_token,
        id,
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        serde_json::to_value(replay).unwrap()
    );
    assert_tree_unchanged(&after, dir.path());
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        serde_json::to_value(share_transaction::read_share_operation(dir.path(), id).unwrap())
            .unwrap()
    );
}

#[test]
fn preserved_credentials_require_supported_version_authenticated_non_mask_values() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let _key = crate::encryption::set_test_encryption_key([0x44; 32]);
    for (ciphertext, version) in [
        ("synthetic-broken-ciphertext".into(), 1),
        (
            crate::encryption::encrypt_initialized("synthetic-value").unwrap(),
            99,
        ),
        (
            crate::encryption::encrypt_initialized("********").unwrap(),
            1,
        ),
    ] {
        let mut original = snapshot();
        let mut providers: serde_json::Value =
            serde_json::from_slice(original.providers.as_deref().unwrap()).unwrap();
        providers["data"]["provider_instances"]["relay"]["credential_ref"] =
            serde_json::json!("provider.existing.key");
        original.providers = Some(serde_json::to_vec(&providers).unwrap());
        original.credentials = Some(bytes(
            serde_json::json!({"entries":{"provider.existing.key":{"ciphertext":ciphertext,"source":"user","updated_at":"2026-10-21T00:00:00Z","key_version":version}}}),
            2,
        ));
        assert!(prepare_import(&bundle(), &choices(), &original).is_err());
    }
}

#[test]
fn credential_documents_are_lossless_when_unchanged_and_fail_closed_on_unknown_or_duplicate_fields()
{
    let unchanged = b"{ \"schema_version\":1, \"revision\":4, \"data\":{\"entries\":{}} }";
    assert_eq!(
        ShareCredentialSnapshot::from_bytes(Some(unchanged))
            .unwrap()
            .encode()
            .unwrap()
            .as_slice(),
        unchanged.as_slice()
    );
    for invalid in [
        br#"{"schema_version":1,"revision":1,"data":{"entries":{},"future":1}}"#.as_slice(),
        br#"{"schema_version":1,"revision":1,"data":{"entries":{"a":{"ciphertext":"a","source":"user","updated_at":"2026-10-21T00:00:00Z","key_version":1,"future":"unknown"}}}}"#.as_slice(),
        br#"{"schema_version":1,"revision":1,"data":{"entries":{"a":{"ciphertext":"a","source":"user","updated_at":"2026-10-21T00:00:00Z","key_version":1},"a":{"ciphertext":"b","source":"user","updated_at":"2026-10-21T00:00:00Z","key_version":1}}}}"#.as_slice(),
    ] { assert!(ShareCredentialSnapshot::from_bytes(Some(invalid)).is_err()); }
}

#[test]
fn known_precommit_transaction_rejections_do_not_become_uncertain_outcomes() {
    for message in [
        share_transaction::SHARE_TRANSACTION_INVALID,
        share_transaction::SHARE_OPERATION_ID_REUSED,
        share_transaction::SHARE_OPERATION_CAPACITY_REACHED,
        "share candidate invalid",
    ] {
        assert!(matches!(
            map_store(ConfigStoreError::Validation(message.into())),
            ShareApiError::Invalid
        ));
    }
}
