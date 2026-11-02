use super::*;
use crate::{dot_path::apply_dot_path_set, Config, ConfigFacade, SectionId};
use serde_json::json;

fn custom() -> TrustedKey {
    TrustedKey {
        label: "custom".into(),
        algorithm: "ed25519".into(),
        public_key: "11".repeat(32),
    }
}

#[test]
fn absent_policy_follows_defaults_but_every_legacy_list_is_explicit() {
    for value in [json!({}), json!({"enforcement":"strict"})] {
        let policy: PluginTrustConfig = serde_json::from_value(value).unwrap();
        assert_eq!(policy.publisher_mode, PublisherMode::FollowDefaults);
        assert!(policy.trusted_keys.is_empty());
        assert_eq!(policy.effective_trusted_keys().len(), 3);
        let saved = serde_json::to_value(&policy).unwrap();
        assert_eq!(saved["trusted_keys"], json!([]));
        assert_eq!(
            serde_json::from_value::<PluginTrustConfig>(saved).unwrap(),
            policy
        );
    }
    for keys in [
        json!([]),
        json!([custom()]),
        json!(official_plugin_publishers()
            .into_iter()
            .map(|(_, key)| key)
            .collect::<Vec<_>>()),
    ] {
        let policy: PluginTrustConfig =
            serde_json::from_value(json!({"trusted_keys":keys})).unwrap();
        assert_eq!(policy.publisher_mode, PublisherMode::Explicit);
        assert_eq!(
            serde_json::to_value(policy.effective_trusted_keys()).unwrap(),
            keys
        );
    }
    for invalid in [
        json!({"trusted_keys":null}),
        json!({"publisher_mode":null}),
        json!({"publisher_mode":"unknown"}),
        json!({"revoked_publishers":null}),
    ] {
        assert!(serde_json::from_value::<PluginTrustConfig>(invalid).is_err());
    }
}

#[test]
fn revocation_survives_duplicate_keys_rotation_retirement_modes_and_round_trips() {
    let mut policy = PluginTrustConfig::default();
    let catalog = official_plugin_publishers();
    let mut duplicate = catalog[0].1.clone();
    duplicate.label = "different label".into();
    duplicate.public_key = duplicate.public_key.to_uppercase();
    policy.trusted_keys.extend([custom(), duplicate.clone()]);
    assert_eq!(policy.effective_trusted_keys().len(), 4);
    assert!(policy.revoke_official_publisher("jiandu"));
    policy.trusted_keys.push(duplicate);
    let denied = publisher_key_id(&catalog[0].1);
    let rotated = vec![(
        "jiandu",
        TrustedKey {
            public_key: "22".repeat(32),
            ..catalog[0].1.clone()
        },
    )];
    for mode in [PublisherMode::Explicit, PublisherMode::FollowDefaults] {
        policy.set_publisher_mode(mode);
        let loaded: PluginTrustConfig =
            serde_json::from_value(serde_json::to_value(&policy).unwrap()).unwrap();
        for keys in [
            loaded.effective_trusted_keys(),
            loaded.resolve_with_catalog(&rotated),
            loaded.resolve_with_catalog(&[]),
        ] {
            assert!(!keys
                .iter()
                .any(|key| publisher_key_id(key) == denied || key.public_key == "22".repeat(32)));
            assert!(keys.contains(&custom()));
        }
        assert!(loaded.revoked_publishers.contains_key("jiandu"));
    }
    assert!(policy.trust_official_publisher("jiandu"));
    assert_eq!(policy.effective_trusted_keys().len(), 4);
    assert!(!policy.trust_official_publisher("not-a-publisher"));
}

#[test]
fn mode_switch_freezes_effective_keys_and_keeps_custom_additions_without_copying_defaults() {
    let mut policy = PluginTrustConfig::default();
    policy.trusted_keys.push(custom());
    policy.revoke_official_publisher("jiandu");
    let effective = policy.effective_trusted_keys();
    policy.set_publisher_mode(PublisherMode::Explicit);
    assert_eq!(policy.trusted_keys, effective);
    policy.set_publisher_mode(PublisherMode::FollowDefaults);
    assert_eq!(policy.trusted_keys, vec![custom()]);
    assert_eq!(policy.effective_trusted_keys(), effective);
}

#[test]
fn compatibility_and_cli_updates_preserve_denials_and_explicit_empty_lists() {
    let mut config = Config::default();
    config.plugin_trust.revoke_official_publisher("jiandu");
    config.plugin_trust.trusted_keys.push(custom());
    for invalid in [
        json!({"publiser_mode":"follow_defaults"}),
        json!({"trusted_keys":[{"label":"custom", "algorithm":"ed25519", "public_key":"11".repeat(32), "typo":true}]}),
    ] {
        assert!(
            apply_dot_path_set(&config, "plugin_trust", invalid).is_err(),
            "policy compatibility must preserve the CLI's unknown-field guard"
        );
    }
    for value in [
        json!({"trusted_keys":[]}),
        json!({"trusted_keys":[custom()]}),
    ] {
        let result = apply_dot_path_set(&config, "plugin_trust", value)
            .unwrap()
            .config;
        assert_eq!(result.plugin_trust.publisher_mode, PublisherMode::Explicit);
        assert_eq!(
            result.plugin_trust.revoked_publishers,
            config.plugin_trust.revoked_publishers
        );
    }
    let result = apply_dot_path_set(&config, "plugin_trust.trusted_keys", json!([]))
        .unwrap()
        .config;
    assert!(result.plugin_trust.effective_trusted_keys().is_empty());
    let result = apply_dot_path_set(&config, "plugin_trust.publisher_mode", json!("explicit"))
        .unwrap()
        .config;
    assert_eq!(
        result.plugin_trust.trusted_keys,
        config.plugin_trust.effective_trusted_keys()
    );
    assert_eq!(
        result.plugin_trust.revoked_publishers,
        config.plugin_trust.revoked_publishers
    );
}

#[test]
fn typed_section_legacy_writes_and_unrelated_reset_preserve_policy_under_cas() {
    let dir = tempfile::tempdir().unwrap();
    let facade = ConfigFacade::open_or_migrate(dir.path()).unwrap();
    let registry = facade.registry();
    let initial_revision = registry.tools_skills.snapshot().revision;
    let mut value = serde_json::to_value(registry.tools_skills.snapshot().data.as_ref()).unwrap();
    let mut policy = PluginTrustConfig::default();
    policy.revoke_official_publisher("jiandu");
    value["plugin_trust"] = serde_json::to_value(&policy).unwrap();
    registry
        .commit_value(SectionId::ToolsSkills, initial_revision, value)
        .unwrap();
    registry
        .commit_value(
            SectionId::ToolsSkills,
            initial_revision + 1,
            json!({"plugin_trust":{"trusted_keys":[]}}),
        )
        .unwrap();
    let snapshot = registry.tools_skills.snapshot();
    assert_eq!(
        snapshot.data.plugin_trust.publisher_mode,
        PublisherMode::Explicit
    );
    assert!(snapshot
        .data
        .plugin_trust
        .effective_trusted_keys()
        .is_empty());
    assert_eq!(
        snapshot.data.plugin_trust.revoked_publishers,
        policy.revoked_publishers
    );
    registry
        .commit_value(SectionId::ToolsSkills, initial_revision + 2, json!({}))
        .unwrap();
    assert_eq!(
        registry.tools_skills.snapshot().data.plugin_trust,
        snapshot.data.plugin_trust
    );
    assert!(registry
        .commit_value(
            SectionId::ToolsSkills,
            initial_revision + 2,
            json!({"plugin_trust":{"publisher_mode":"follow_defaults"}})
        )
        .is_err());
    let reopened = ConfigFacade::open(dir.path()).unwrap();
    assert_eq!(
        reopened
            .registry()
            .tools_skills
            .snapshot()
            .data
            .plugin_trust,
        snapshot.data.plugin_trust
    );
}
