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
fn provider_type_change_is_rejected_without_reusing_old_key() {
    let _provider = crate::test_support::override_runtime_env_var("BAMBOO_PROVIDER", None);
    let mut imported = bundle();
    imported.providers[0].provider_type = ShareProviderType::Anthropic;
    assert!(matches!(
        prepare_import(&imported, &choices(), &snapshot()),
        Err(ShareApiError::Invalid)
    ));
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
    let candidate = encode_changed_section(section, None).unwrap();
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
    assert_eq!(before, tree(dir.path())); // cancelling here needs no rollback
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
    assert_eq!(after, tree(dir.path()));
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
    assert_eq!(before, tree(dir.path()));
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
    assert_eq!(before, tree(dir.path()));
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
    assert_eq!(after, tree(dir.path()));
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
