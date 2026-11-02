use super::super::tests::isolated_state;
use super::*;
use actix_web::{test, App};
use bamboo_config::{ConfigFacade, PluginTrustEnforcement};
use serde_json::{json, Value};

fn policy_change(state: &AppState, change: Value) -> Value {
    json!({"expected_revision": snapshot(state).revision,
        "expected_publishers": catalog_identity(), "change": change})
}

#[actix_web::test]
async fn policy_mode_revoke_legacy_save_and_reviewed_restore_keep_one_cas_authority() {
    let custom = json!({"label":"private", "algorithm":"ed25519", "public_key":"11".repeat(32)});
    let (_dir, state) = legacy_state(
        Some(json!([custom])),
        json!(["github.com/bigduu/", "example.invalid/"]),
    )
    .await;
    let before = snapshot(&state);
    let memory_before = tree_bytes(&state.memory_store.data_dir());
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .service(web::scope("/api/v1").service(crate::routes::plugin_scope()))
            .route(
                "/sections/{section}/reset",
                web::post().to(crate::handlers::settings::reset_typed_section),
            ),
    )
    .await;
    let report: Value = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/plugins/publishers")
            .to_request(),
    )
    .await;
    assert_eq!(report["publisher_mode"], "explicit");
    assert_eq!(snapshot(&state).revision, before.revision);
    for (change, mode, trusted, revoked) in [
        (
            json!({"type":"set_mode", "publisher_mode":"follow_defaults"}),
            "follow_defaults",
            true,
            false,
        ),
        (
            json!({"type":"revoke", "publisher_id":"jiandu"}),
            "follow_defaults",
            false,
            true,
        ),
        (
            json!({"type":"set_mode", "publisher_mode":"explicit"}),
            "explicit",
            false,
            true,
        ),
        (
            json!({"type":"set_mode", "publisher_mode":"follow_defaults"}),
            "follow_defaults",
            false,
            true,
        ),
        (
            json!({"type":"trust", "publisher_id":"jiandu"}),
            "follow_defaults",
            true,
            false,
        ),
    ] {
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/plugins/publishers")
                .set_json(policy_change(&state, change))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), 204);
        let report: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/plugins/publishers")
                .to_request(),
        )
        .await;
        assert_eq!(report["publisher_mode"], mode);
        let jiandu = report["publishers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["id"] == "jiandu")
            .unwrap();
        assert_eq!(jiandu["trusted"], trusted);
        assert_eq!(jiandu["revoked"], revoked);
        assert_eq!(report["enforcement"], "strict");
        assert_eq!(report["custom_key_count"], 1);
        assert_eq!(
            snapshot(&state).data.plugin_trust.trusted_hosts,
            before.data.plugin_trust.trusted_hosts
        );
        assert!(!state.app_data_dir.join("plugins").exists());
    }
    // Legacy list-only save must select explicit, without erasing the deny record.
    let mut candidate = snapshot(&state).data.clone();
    candidate.plugin_trust.revoke_official_publisher("jiandu");
    state
        .put_ordinary_section(
            SectionId::ToolsSkills,
            snapshot(&state).revision,
            serde_json::to_value(candidate).unwrap(),
        )
        .await
        .unwrap();
    state
        .put_ordinary_section(
            SectionId::ToolsSkills,
            snapshot(&state).revision,
            json!({"plugin_trust":{"trusted_keys":[]}}),
        )
        .await
        .unwrap();
    assert!(snapshot(&state)
        .data
        .plugin_trust
        .effective_trusted_keys()
        .is_empty());
    assert!(snapshot(&state)
        .data
        .plugin_trust
        .revoked_publishers
        .contains_key("jiandu"));
    let saved_policy = snapshot(&state).data.plugin_trust.clone();
    let reset = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/sections/tools-skills/reset")
            .set_json(json!({"expected_revision":snapshot(&state).revision}))
            .to_request(),
    )
    .await;
    assert_eq!(reset.status(), 200);
    assert_eq!(snapshot(&state).data.plugin_trust, saved_policy);
    let reopened = ConfigFacade::open(&state.app_data_dir).unwrap();
    assert_eq!(
        reopened
            .registry()
            .tools_skills
            .snapshot()
            .data
            .plugin_trust,
        snapshot(&state).data.plugin_trust
    );
    assert_eq!(tree_bytes(&state.memory_store.data_dir()), memory_before);
}

#[actix_web::test]
async fn policy_rejects_stale_revision_catalog_replacement_and_unknown_actions_without_writes() {
    let (_dir, state) = legacy_state(Some(json!([])), json!(["github.com/bigduu/"])).await;
    let before = snapshot(&state);
    let bytes = tokio::fs::read(state.app_data_dir.join("tools-skills.json"))
        .await
        .unwrap();
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .service(web::scope("/api/v1").service(crate::routes::plugin_scope())),
    )
    .await;
    let valid = policy_change(&state, json!({"type":"trust", "publisher_id":"jiandu"}));
    let mut stale = valid.clone();
    stale["expected_revision"] = json!(0);
    let mut replaced = valid.clone();
    replaced["expected_publishers"]["jiandu"] = json!("ed25519:00");
    let mut unknown = valid.clone();
    unknown["change"]["publisher_id"] = json!("arbitrary");
    let mut extra = valid.clone();
    extra["enforcement"] = json!("off");
    let mut invalid = valid.clone();
    invalid["change"] = json!({"type":"set_mode", "publisher_mode":null});
    for (body, status) in [
        (stale, 409),
        (replaced, 400),
        (unknown, 400),
        (extra, 400),
        (invalid, 400),
    ] {
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/plugins/publishers")
                .set_json(body)
                .to_request(),
        )
        .await;
        assert_eq!(response.status().as_u16(), status);
        assert_eq!(snapshot(&state).revision, before.revision);
        assert_eq!(
            tokio::fs::read(state.app_data_dir.join("tools-skills.json"))
                .await
                .unwrap(),
            bytes
        );
    }
}

#[actix_web::test]
async fn fixed_jiandu_adoption_can_restore_a_revoked_publisher_without_relaxing_enforcement() {
    for mode in [PublisherMode::Explicit, PublisherMode::FollowDefaults] {
        let (_dir, state) = legacy_state(Some(json!([])), json!(["github.com/bigduu/"])).await;
        let mut candidate = snapshot(&state).data.clone();
        candidate.plugin_trust.set_publisher_mode(mode);
        candidate.plugin_trust.revoke_official_publisher("jiandu");
        state
            .put_ordinary_section(
                SectionId::ToolsSkills,
                snapshot(&state).revision,
                serde_json::to_value(candidate).unwrap(),
            )
            .await
            .unwrap();
        let response = trust_official_jiandu(
            state.clone(),
            web::Json(PublisherTrustRequest {
                expected_revision: snapshot(&state).revision,
                expected_public_key: JIANDU_PLUGIN_PUBLIC_KEY.into(),
            }),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), 204);
        let policy = snapshot(&state).data.plugin_trust.clone();
        assert!(policy
            .effective_trusted_keys()
            .iter()
            .any(|key| key.public_key == JIANDU_PLUGIN_PUBLIC_KEY));
        assert!(!policy.revoked_publishers.contains_key("jiandu"));
        assert_eq!(policy.enforcement, PluginTrustEnforcement::Strict);
        assert_eq!(policy.publisher_mode, mode);
    }
}

async fn legacy_state(
    keys: Option<Value>,
    hosts: Value,
) -> (tempfile::TempDir, web::Data<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("bamboo");
    tokio::fs::create_dir_all(&data).await.unwrap();
    let mut policy = json!({"enforcement":"strict", "trusted_hosts":hosts});
    if let Some(keys) = keys {
        policy["trusted_keys"] = keys;
    }
    tokio::fs::write(
        data.join("config.json"),
        // Keep independent startup gardeners from writing to the fixture's
        // store while measuring this configuration-only operation.
        serde_json::to_vec(&json!({"plugin_trust":policy, "memory": {
            "auto_dream_enabled":false, "gardener_enabled":false,
            "dedup_gardener_enabled":false, "ledger_gardener_enabled":false,
            "ledger_distillation_enabled":false, "granularity_freshness_gardener_enabled":false
        }}))
        .unwrap(),
    )
    .await
    .unwrap();
    let state = isolated_state(data, dir.path().join("independent-memory")).await;
    (dir, state)
}

fn snapshot(state: &AppState) -> bamboo_config::SectionEnvelope<bamboo_config::ToolsSkillsSection> {
    state
        .config_facade
        .as_ref()
        .unwrap()
        .registry()
        .tools_skills
        .snapshot()
        .envelope()
}

fn tree_bytes(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    let mut result = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        if !path.exists() {
            continue;
        }
        let relative = path.strip_prefix(root).unwrap().to_path_buf();
        if path.is_dir() {
            result.insert(relative, None);
            pending.extend(
                std::fs::read_dir(path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path()),
            );
        } else {
            result.insert(relative, Some(std::fs::read(path).unwrap()));
        }
    }
    result
}

#[actix_web::test]
async fn explicit_adoption_preserves_legacy_custom_and_empty_lists_and_survives_reload() {
    let old_keys: Vec<_> = PluginTrustConfig::default()
        .effective_trusted_keys()
        .into_iter()
        .filter(|key| key.public_key != JIANDU_PLUGIN_PUBLIC_KEY)
        .collect();
    for keys in [
        json!(old_keys),
        json!([]),
        json!([{"label":"custom", "algorithm":"ed25519", "public_key":"11".repeat(32)}]),
    ] {
        let (_dir, state) = legacy_state(
            Some(keys),
            json!(["github.com/bigduu/", "example.invalid/private/"]),
        )
        .await;
        let before = snapshot(&state);
        let memory_before = tree_bytes(&state.memory_store.data_dir());
        let bytes = tokio::fs::read(state.app_data_dir.join("tools-skills.json"))
            .await
            .unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .service(web::scope("/api/v1").service(crate::routes::plugin_scope())),
        )
        .await;
        let report: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/plugins/jiandu/setup")
                .to_request(),
        )
        .await;
        assert_eq!(report["trust"]["reason"], "publisher_not_trusted");
        assert_eq!(
            tokio::fs::read(state.app_data_dir.join("tools-skills.json"))
                .await
                .unwrap(),
            bytes,
            "GET must not adopt a publisher"
        );
        assert_eq!(
            report["trust_action"]["publisher"]["public_key"],
            JIANDU_PLUGIN_PUBLIC_KEY
        );
        if !Host::detect().supported {
            assert_eq!(report["trust_action"]["available"], false);
            continue;
        }
        assert_eq!(report["trust_action"]["available"], true);
        let request = json!({"expected_revision":before.revision,"expected_public_key":JIANDU_PLUGIN_PUBLIC_KEY});
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/plugins/official/jiandu/trust")
                .set_json(&request)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), 204);
        let mut expected = before.data.clone();
        expected.plugin_trust.trust_official_publisher("jiandu");
        let after = snapshot(&state);
        assert_eq!(
            serde_json::to_value(&after.data).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        assert_eq!(after.revision, before.revision + 1);
        assert_eq!(
            after.data.plugin_trust.enforcement,
            PluginTrustEnforcement::Strict
        );
        assert!(!state.app_data_dir.join("plugins/installed.json").exists());
        assert_eq!(tree_bytes(&state.memory_store.data_dir()), memory_before);
        let loaded = ConfigFacade::open_or_migrate(&state.app_data_dir).unwrap();
        assert_eq!(
            loaded.registry().tools_skills.snapshot().data.plugin_trust,
            expected.plugin_trust
        );
        let fresh: Value = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/plugins/jiandu/setup")
                .to_request(),
        )
        .await;
        assert_eq!(fresh["trust"]["trusted"], true);
        assert_eq!(fresh["install"]["available"], true);
        // A repeat with the current revision is a no-op, never a duplicate key.
        let response = test::call_service(&app, test::TestRequest::post().uri("/api/v1/plugins/official/jiandu/trust").set_json(json!({"expected_revision":after.revision,"expected_public_key":JIANDU_PLUGIN_PUBLIC_KEY})).to_request()).await;
        assert_eq!(response.status(), 200);
        assert_eq!(snapshot(&state).revision, after.revision);
    }
}

#[actix_web::test]
async fn absent_key_field_keeps_reviewed_defaults_without_adoption() {
    let (_dir, state) = legacy_state(None, json!(["github.com/bigduu/"])).await;
    let before = snapshot(&state);
    let (_, action) = observe_trust(
        &state,
        &super::super::tests::mac(),
        accepted_release().as_ref(),
        None,
        false,
    )
    .await;
    assert_eq!(action.unwrap().reason, Some("publisher_already_trusted"));
    assert!(before
        .data
        .plugin_trust
        .effective_trusted_keys()
        .iter()
        .any(|key| key.public_key == JIANDU_PLUGIN_PUBLIC_KEY));
    assert_eq!(snapshot(&state).revision, before.revision);
}

#[actix_web::test]
async fn adoption_rejects_stale_revisions_wrong_identity_and_caller_authority() {
    let (_dir, state) = legacy_state(Some(json!([])), json!(["github.com/bigduu/"])).await;
    let before = snapshot(&state);
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .service(web::scope("/api/v1").service(crate::routes::plugin_scope())),
    )
    .await;
    for request in [
        json!({"expected_revision":before.revision,"expected_public_key":"00".repeat(32)}),
        json!({"expected_revision":before.revision,"expected_public_key":JIANDU_PLUGIN_PUBLIC_KEY,"insecure":true}),
        json!({"expected_revision":before.revision,"expected_public_key":JIANDU_PLUGIN_PUBLIC_KEY,"url":"https://evil.invalid"}),
        json!({"expected_public_key":JIANDU_PLUGIN_PUBLIC_KEY}),
    ] {
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/plugins/official/jiandu/trust")
                .set_json(request)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), 400);
        assert_eq!(snapshot(&state).revision, before.revision);
    }
    // Another process can update the durable section before this facade has
    // observed it. The existing physical CAS must preserve that newer policy.
    let external = ConfigFacade::open_or_migrate(&state.app_data_dir).unwrap();
    let mut candidate = serde_json::to_value(&before.data).unwrap();
    candidate["plugin_trust"]["trusted_hosts"] = json!(["example.invalid/private/"]);
    external
        .registry()
        .commit_value_with_envelope(SectionId::ToolsSkills, before.revision, candidate.clone())
        .unwrap();
    let response = test::call_service(&app, test::TestRequest::post().uri("/api/v1/plugins/official/jiandu/trust").set_json(json!({"expected_revision":before.revision,"expected_public_key":JIANDU_PLUGIN_PUBLIC_KEY})).to_request()).await;
    assert_eq!(response.status(), 409);
    let disk: Value = serde_json::from_slice(
        &tokio::fs::read(state.app_data_dir.join("tools-skills.json"))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(disk["data"], candidate);
}

#[actix_web::test]
async fn source_platform_artifact_and_install_state_restrictions_are_not_repaired_by_trust() {
    let (_dir, state) = legacy_state(Some(json!([])), json!(["example.invalid/private/"])).await;
    let before = snapshot(&state);
    let release = accepted_release();
    let mac = super::super::tests::mac();
    for (host, release, memory, installing, reason) in [
        (
            mac.clone(),
            release.as_ref(),
            None,
            false,
            "source_not_trusted",
        ),
        (
            Host::for_platform("unknown", "unknown", None),
            release.as_ref(),
            None,
            false,
            "unsupported_host",
        ),
        (mac.clone(), None, None, false, "artifact_unavailable"),
        (
            mac,
            release.as_ref(),
            Some("memory_root_overlaps_plugin"),
            true,
            "memory_root_overlaps_plugin",
        ),
    ] {
        let (_, action) = observe_trust(&state, &host, release, memory, installing).await;
        let action = action.unwrap();
        assert!(!action.available);
        assert_eq!(action.reason, Some(reason));
    }
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .service(web::scope("/api/v1").service(crate::routes::plugin_scope())),
    )
    .await;
    let response = test::call_service(&app, test::TestRequest::post().uri("/api/v1/plugins/official/jiandu/trust").set_json(json!({"expected_revision":before.revision,"expected_public_key":JIANDU_PLUGIN_PUBLIC_KEY})).to_request()).await;
    assert_eq!(response.status(), 409);
    assert_eq!(snapshot(&state).revision, before.revision);
}
