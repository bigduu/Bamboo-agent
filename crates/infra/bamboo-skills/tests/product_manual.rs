use bamboo_config::{Config, MemoryConfig, SectionId, CONFIG_VALUE_FIELD_OWNERS};
use bamboo_skills::store::builtin::{load_builtin_skill_bundles, BUILTIN_SKILL_FILES};
use serde_json::{json, Value};
use std::collections::BTreeSet;

#[allow(dead_code)]
#[path = "../build/product_manual.rs"]
mod packaging;

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
    let memory = config
        .memory()
        .clone()
        .unwrap_or_else(MemoryConfig::default);
    match key {
        "server.port" => json!(config.server.port),
        "server.workers" => json!(config.server.workers),
        "tools.disabled" => json!(config.tools.disabled),
        "skills.disabled" => json!(config.skills.disabled),
        "memory.auto_dream_enabled" => json!(memory.auto_dream_enabled),
        "memory.project_prompt_injection" => json!(memory.project_prompt_injection),
        "memory.relevant_recall" => json!(memory.relevant_recall),
        "memory.relevant_recall_rerank" => json!(memory.relevant_recall_rerank),
        "memory.ledger_agenda_injection" => json!(memory.ledger_agenda_injection),
        "lifecycle_hooks.enabled" => json!(config.lifecycle_hooks.enabled),
        "context_management.strategy" => json!(config.context_management.strategy),
        _ => panic!("catalog entry lacks a typed accessor: {key}"),
    }
}

fn input(key: &str, value: Value) -> Value {
    key.split('.')
        .rev()
        .fold(value, |child, field| json!({field: child}))
}

#[test]
fn catalog_defaults_examples_aliases_and_owners_match_real_typed_config() {
    let catalog: Value = serde_json::from_slice(resource("references/config-keys.json")).unwrap();
    let entries = catalog["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 11, "update typed coverage deliberately");
    let mut keys = BTreeSet::new();
    for entry in entries {
        let key = entry["key"].as_str().unwrap();
        assert!(keys.insert(key));
        assert_eq!(
            typed_value(&Config::default(), key),
            entry["default"],
            "{key}"
        );
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
        "references/memory.md",
        "references/hooks.md",
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
