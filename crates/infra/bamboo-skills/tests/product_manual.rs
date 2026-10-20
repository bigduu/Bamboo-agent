use bamboo_config::{Config, SectionId, CONFIG_VALUE_FIELD_OWNERS};
use bamboo_skills::store::builtin::{load_builtin_skill_bundles, BUILTIN_SKILL_FILES};
use serde_json::{json, Value};
use std::collections::BTreeSet;

#[allow(dead_code)]
#[path = "../build/product_manual.rs"]
mod packaging;

#[path = "../manual/config_catalog.rs"]
mod config_catalog;

fn resource(path: &str) -> &'static [u8] {
    let full = format!("bamboo-manual/{path}");
    BUILTIN_SKILL_FILES
        .iter()
        .find(|(name, _)| *name == full)
        .unwrap_or_else(|| panic!("missing embedded {full}"))
        .1
}

#[test]
fn embedded_manifest_binds_complete_exact_content() {
    let manifest: Value = serde_json::from_slice(resource("manifest.json")).unwrap();
    assert_eq!(manifest["package_version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(
        manifest["configuration_coverage"],
        "bounded_catalog_not_full_schema"
    );
    if manifest["source"]["identity_source"] == "git" {
        assert_eq!(manifest["source"]["status"], "checkout_unverified");
    }
    let resources = manifest["resources"].as_array().unwrap();
    assert_eq!(resources.len(), packaging::RESOURCES.len());
    let mut content = Vec::new();
    for (entry, expected) in resources.iter().zip(packaging::RESOURCES) {
        assert_eq!(entry["path"], *expected);
        let bytes = resource(expected);
        assert_eq!(entry["bytes"], bytes.len());
        assert_eq!(entry["sha256"], packaging::sha256(bytes));
        content.push((*expected, bytes.to_vec()));
    }
    assert_eq!(
        manifest["content_sha256"],
        packaging::content_digest(&content)
    );
    let bundles = load_builtin_skill_bundles().unwrap();
    let bundle = bundles
        .iter()
        .find(|bundle| bundle.skill.id == "bamboo-manual")
        .unwrap();
    assert!(
        bundle.skill.tool_refs.is_empty(),
        "manual must not grant tools"
    );
    assert_eq!(bundle.files.len(), packaging::RESOURCES.len());
    assert!(bundle.skill.prompt.contains("next_cursor"));
    assert!(bundle.skill.prompt.contains("not a complete JSON Schema"));
}

#[test]
fn missing_empty_invalid_and_non_utf8_assets_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    assert!(packaging::collect(root.path()).is_err());
    for path in packaging::RESOURCES {
        let target = root.path().join(path);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(target, resource(path)).unwrap();
    }
    assert!(packaging::collect(root.path()).is_ok());
    for bytes in [Vec::new(), vec![0xff]] {
        std::fs::write(root.path().join("SKILL.md"), bytes).unwrap();
        assert!(packaging::collect(root.path()).is_err());
    }
    std::fs::write(root.path().join("SKILL.md"), resource("SKILL.md")).unwrap();
    std::fs::write(root.path().join("references/config-keys.json"), b"{broken").unwrap();
    assert!(packaging::collect(root.path()).is_err());
}

#[test]
fn source_archive_identity_is_explicit_and_invalid_identity_stays_unknown() {
    let root = tempfile::tempdir().unwrap();
    assert_eq!(packaging::source_identity(root.path())["status"], "unknown");
    let vcs = root.path().join(".cargo_vcs_info.json");
    let revision = "0123456789abcdef0123456789abcdef01234567";
    std::fs::write(
        &vcs,
        json!({"git": {"sha1": revision, "dirty": true}}).to_string(),
    )
    .unwrap();
    let identity = packaging::source_identity(root.path());
    assert_eq!(identity["revision"], revision);
    assert_eq!(identity["dirty"], true);
    assert_eq!(identity["status"], "source_archive");
    for bytes in ["{broken", "{}", r#"{"git":{"sha1":"latest"}}"#] {
        std::fs::write(&vcs, bytes).unwrap();
        let identity = packaging::source_identity(root.path());
        assert_eq!(identity["status"], "unknown");
        assert!(identity["revision"].is_null());
    }
}

#[test]
fn native_hook_example_uses_the_real_configuration_contract() {
    let chapter = std::str::from_utf8(resource("references/hooks.md")).unwrap();
    let example = chapter
        .split("```json\n")
        .nth(1)
        .unwrap()
        .split("```")
        .next()
        .unwrap();
    let config: Config = serde_json::from_str(example).unwrap();
    assert!(config.lifecycle_hooks.enabled);
    let serialized = serde_json::to_value(&config.lifecycle_hooks).unwrap();
    let handler = &serialized["SessionStart"][0]["hooks"][0];
    assert_eq!(handler["type"], "script");
    assert_eq!(handler["runner"], "python");
    assert_eq!(handler["timeout_ms"], 5000);
    assert!(chapter.contains("    \"additional_context\""));
}

fn typed_value(config: &Config, key: &str) -> Value {
    let entry = config_catalog::entries()
        .into_iter()
        .find(|entry| entry.key == key)
        .unwrap_or_else(|| panic!("catalog entry lacks a typed accessor: {key}"));
    (entry.read)(config)
}

fn input(key: &str, value: Value) -> Value {
    if let Some(field) = key.strip_prefix("server.tls.") {
        let mut tls = json!({"cert_file":"example-cert.pem", "key_file":"example-key.pem"});
        tls[field] = value;
        return json!({"server":{"tls":tls}});
    }
    key.split('.')
        .rev()
        .fold(value, |child, field| json!({field: child}))
}

#[test]
fn catalog_is_reproducible_and_root_coverage_cannot_drift_silently() {
    assert_eq!(
        std::str::from_utf8(resource("references/config-keys.json")).unwrap(),
        config_catalog::render(),
        "run the catalog generator --write and review the diff"
    );
    let catalog = config_catalog::catalog();
    let roots: BTreeSet<_> = catalog["root_coverage"]
        .as_array()
        .unwrap()
        .iter()
        .map(|root| root["root"].as_str().unwrap())
        .collect();
    let expected: BTreeSet<_> = CONFIG_VALUE_FIELD_OWNERS
        .iter()
        .map(|(root, _)| *root)
        .chain(["memory"])
        .collect();
    assert_eq!(roots, expected);
    assert!(catalog["root_coverage"]
        .as_array()
        .unwrap()
        .iter()
        .any(|root| root["coverage"] == "not_documented"));
    assert_eq!(catalog["coverage"], "bounded_typed_catalog_not_full_schema");
    let entries = catalog["entries"].as_array().unwrap();
    for key in [
        "memory.background_model",
        "server.tls",
        "server.static_dir",
        "worktrees.default_root",
    ] {
        let entry = entries.iter().find(|entry| entry["key"] == key).unwrap();
        assert!(
            entry["default"].is_null(),
            "omitted Option still needs documentation"
        );
    }
    let extra = catalog["root_coverage"]
        .as_array()
        .unwrap()
        .iter()
        .find(|root| root["root"] == "extra")
        .unwrap();
    assert_eq!(extra["kind"], "flattened_dynamic_map_not_literal_key");
    assert_eq!(
        entries
            .iter()
            .find(|e| e["key"] == "memory.dream_refine_mode")
            .unwrap()["status"],
        "deprecated_ignored"
    );
}

#[test]
fn catalog_defaults_examples_aliases_and_owners_match_real_typed_config() {
    let catalog: Value = serde_json::from_slice(resource("references/config-keys.json")).unwrap();
    let entries = catalog["entries"].as_array().unwrap();
    assert_eq!(entries.len(), config_catalog::entries().len());
    assert!(entries.len() > 11, "expand the original bounded slice");
    let mut keys = BTreeSet::new();
    for entry in entries {
        let key = entry["key"].as_str().unwrap();
        assert!(keys.insert(key));
        if entry.get("required_when").is_some() {
            assert!(key.starts_with("server.tls."));
            assert!(
                entry.get("default").is_none(),
                "required TLS fields must not imply a fallback"
            );
        } else {
            assert_eq!(
                typed_value(&Config::default(), key),
                entry["default"],
                "{key}"
            );
        }
        assert_ne!(
            entry["default"], entry["example"],
            "must exercise a non-default value"
        );
        let section = SectionId::from_name(entry["section"].as_str().unwrap()).unwrap();
        let root = key.split('.').next().unwrap();
        let owner = if root == "memory" {
            SectionId::Memory
        } else {
            CONFIG_VALUE_FIELD_OWNERS
                .iter()
                .find(|(field, _)| *field == root)
                .unwrap()
                .1
        };
        assert_eq!(section, owner, "{key}");
        for spelling in std::iter::once(key).chain(
            entry["aliases"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|alias| alias.as_str().unwrap()),
        ) {
            let config: Config =
                serde_json::from_value(input(spelling, entry["example"].clone())).unwrap();
            assert_eq!(typed_value(&config, key), entry["example"], "{spelling}");
            let roundtrip: Config =
                serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
            assert_eq!(typed_value(&roundtrip, key), entry["example"]);
        }
        if let Some(values) = entry["values"].as_array() {
            for value in values {
                let config: Config = serde_json::from_value(input(key, value.clone())).unwrap();
                assert_eq!(typed_value(&config, key), *value);
            }
        }
    }
    // Exercise an actual validator, not only serde shape or flattened unknowns.
    assert!(
        serde_json::from_value::<Config>(json!({"context_management": {
            "strategy": "retrieval_window", "retrieval_window": {"history_tool_required": false}
        }}))
        .is_err()
    );
    assert!(serde_json::from_value::<Config>(json!({"server": {"port": 65536}})).is_err());
    for field in [
        "transport_idle_timeout_secs",
        "first_semantic_timeout_secs",
        "semantic_idle_timeout_secs",
    ] {
        for invalid in [0, 86401] {
            assert!(
                serde_json::from_value::<Config>(json!({"stream_timeout": {field: invalid}}))
                    .is_err()
            );
        }
    }
    for missing_child in [
        json!({}),
        json!({"cert_file":"example-cert.pem"}),
        json!({"key_file":"example-key.pem"}),
    ] {
        assert!(serde_json::from_value::<Config>(json!({"server":{"tls":missing_child}})).is_err());
    }
    for key in ["server.tls.cert_file", "server.tls.key_file"] {
        let entry = entries.iter().find(|entry| entry["key"] == key).unwrap();
        assert_eq!(entry["required_when"], "server.tls is present");
        assert!(entry.get("default").is_none());
    }
    // These are preserved dynamic values, never catalog entries or typed keys.
    let preserved: Config =
        serde_json::from_value(json!({"server":{"future_unknown":42}})).unwrap();
    assert_eq!(preserved.server.extra["future_unknown"], 42);
    assert!(!entries
        .iter()
        .any(|entry| entry["key"] == "server.future_unknown"));
}

#[tokio::test]
async fn builtin_manual_reads_without_repository_via_existing_paginated_reader() {
    use bamboo_skills::progressive::SkillCatalogEligibility;
    use bamboo_skills::{SkillStore, SkillStoreConfig};
    let directory = tempfile::tempdir().unwrap();
    let store = SkillStore::new(SkillStoreConfig {
        skills_dir: directory.path().join("empty-user-skills"),
        ..Default::default()
    });
    store.initialize().await.unwrap();
    let access = SkillCatalogEligibility {
        ceiling: None,
        explicit: BTreeSet::new(),
        disabled: BTreeSet::new(),
        deny_all: false,
    };
    let catalog = store
        .progressive_catalog_for_mode(None, &access)
        .await
        .unwrap();
    let entry = catalog
        .entries
        .iter()
        .find(|entry| entry.name == "bamboo-manual")
        .unwrap();
    for path in [
        "manifest.json",
        "references/configuration.md",
        "references/config-keys.json",
        "references/memory.md",
        "references/hooks.md",
        "references/troubleshooting.md",
    ] {
        let selected = store
            .selected_source_for_mode(None, &access, &catalog, &entry.package, path)
            .await
            .unwrap();
        let mut offset = 0;
        let mut reconstructed = String::new();
        loop {
            let page = selected
                .page_response(
                    offset,
                    512,
                    |text, next| Ok(serde_json::to_vec(&(text, next)).unwrap().len()),
                    |text, next, writer| {
                        serde_json::to_writer(writer, &(text, next)).map_err(|error| {
                            bamboo_skills::SkillError::Validation(error.to_string())
                        })
                    },
                )
                .unwrap();
            let (text, next): (String, Option<usize>) =
                serde_json::from_str(page.as_str()).unwrap();
            reconstructed.push_str(&text);
            match next {
                Some(next) => {
                    assert!(next > offset);
                    offset = next;
                }
                None => break,
            }
        }
        assert_eq!(
            reconstructed.as_bytes(),
            resource(path),
            "must preserve every byte, code indent and newline"
        );
    }
    let denied = SkillCatalogEligibility {
        disabled: BTreeSet::from([entry.package.clone()]),
        ..access
    };
    assert!(store
        .selected_source_for_mode(None, &denied, &catalog, &entry.package, "manifest.json")
        .await
        .is_err());
    assert!(store
        .selected_source_for_mode(None, &denied, &catalog, &entry.package, "../config.json")
        .await
        .is_err());
}
