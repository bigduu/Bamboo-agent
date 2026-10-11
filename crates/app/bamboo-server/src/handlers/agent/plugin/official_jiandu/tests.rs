use super::*;
use actix_web::{test, App};
use bamboo_plugin::RegisteredCapabilities;
use chrono::Utc;

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

async fn isolated_state(data_dir: PathBuf, memory_root: PathBuf) -> web::Data<AppState> {
    let mut state = AppState::new_with_memory_store(
        data_dir,
        bamboo_memory::memory_store::MemoryStore::new(memory_root),
    )
    .await
    .unwrap();
    state.wait_for_boot_reconcile_services().await;
    state.stop_config_watcher_for_test();
    web::Data::new(state)
}

async fn fixture() -> (
    tempfile::TempDir,
    web::Data<AppState>,
    Release,
    InstalledPlugin,
) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("separate-canonical-store");
    let state = isolated_state(dir.path().join("bamboo"), root).await;
    let plugin_dir = state.app_data_dir.join("plugins/jiandu");
    tokio::fs::create_dir_all(plugin_dir.join("bin/macos"))
        .await
        .unwrap();
    let manifest = b"{\"id\":\"jiandu\",\"version\":\"0.3.2\",\"provides\":{}}\n";
    let binary = b"fixture executable bytes; never launched";
    tokio::fs::write(plugin_dir.join("plugin.json"), manifest)
        .await
        .unwrap();
    tokio::fs::write(plugin_dir.join("bin/macos/jiandu"), binary)
        .await
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(
            plugin_dir.join("bin/macos/jiandu"),
            std::fs::Permissions::from_mode(0o755),
        )
        .await
        .unwrap();
    }
    let release = Release {
        version: "0.3.2".into(),
        manifest_url:
            "https://github.com/bigduu/Jiandu/releases/download/v0.3.2/jiandu-plugin-v0.3.2.tar.gz"
                .into(),
        sha256: "a".repeat(64),
        manifest_sha256: digest(manifest),
        executable_sha256: BTreeMap::from([("macos".into(), digest(binary))]),
    };
    let entry = InstalledPlugin {
        id: "jiandu".into(),
        version: release.version.clone(),
        source: PluginSource::Url {
            url: release.manifest_url.clone(),
            sha256: Some(release.sha256.clone()),
            signed_by: Some("jiandu (bigduu official)".into()),
            allow_unverified: false,
            allow_untrusted_host: false,
            allow_unsigned: false,
            insecure: false,
        },
        plugin_dir,
        installed_at: Utc::now(),
        status: PluginInstallStatus::Installed,
        registered: RegisteredCapabilities::default(),
    };
    save(&state, entry.clone()).await;
    (dir, state, release, entry)
}

async fn save(state: &AppState, entry: InstalledPlugin) {
    InstalledPlugins {
        plugins: vec![entry],
    }
    .save(&state.app_data_dir.join("plugins/installed.json"))
    .await
    .unwrap();
}
fn mac() -> Host {
    Host::for_platform("macos", "aarch64", None)
}

#[test]
async fn compatibility_uses_process_abi_and_runtime_glibc() {
    for (os, arch, libc, supported) in [
        ("macos", "aarch64", None, true),
        ("macos", "x86_64", None, true),
        ("windows", "x86_64", None, true),
        ("windows", "aarch64", None, false),
        ("linux", "x86_64", Some("2.35"), true),
        ("linux", "x86_64", Some("2.34"), false),
        ("linux", "x86_64", Some("2.40"), true),
        ("linux", "aarch64", Some("2.40"), false),
        ("linux", "x86_64", None, false),
        ("linux", "x86_64", Some("musl"), false),
        ("unknown", "x86_64", None, false),
    ] {
        assert_eq!(
            Host::for_platform(os, arch, libc.map(String::from)).supported,
            supported,
            "{os}/{arch}/{libc:?}"
        );
    }
}

#[actix_web::test]
async fn passive_http_metadata_and_official_action_reject_caller_sources() {
    let dir = tempfile::tempdir().unwrap();
    let state = isolated_state(dir.path().join("bamboo"), dir.path().join("memory")).await;
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .service(web::scope("/api/v1").service(crate::routes::plugin_scope())),
    )
    .await;
    let response = test::call_service(
        &app,
        test::TestRequest::get()
            .uri("/api/v1/plugins/jiandu/setup")
            .to_request(),
    )
    .await;
    assert!(response.status().is_success());
    let report: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(report["plugin_id"], "jiandu");
    assert_eq!(report["host"]["os"], std::env::consts::OS);
    assert_eq!(report["host"]["arch"], std::env::consts::ARCH);
    assert_eq!(report["installed"]["state"], "missing");
    assert!(report["external_client"].is_null());
    assert!(!state.app_data_dir.join("plugins/installed.json").exists());
    for body in [
        serde_json::json!({"url":"https://evil.invalid/plugin.json"}),
        serde_json::json!({"insecure":true}),
        serde_json::json!({"command":"/tmp/jiandu"}),
    ] {
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/plugins/official/jiandu/install")
                .set_json(body)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
    // Removing publisher trust blocks the official action even in generic manual Off mode.
    {
        let mut config = state.config.write().await;
        config.plugin_trust.trusted_keys.clear();
        config.plugin_trust.enforcement = PluginTrustEnforcement::Off;
    }
    let response = test::call_service(
        &app,
        test::TestRequest::post()
            .uri("/api/v1/plugins/official/jiandu/install")
            .set_json(serde_json::json!({}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);
    let body: serde_json::Value = test::read_body_json(response).await;
    assert_eq!(body["error"]["reason"], "publisher_not_trusted");
    assert!(!state.app_data_dir.join("plugins/installed.json").exists());
}

#[actix_web::test]
async fn ready_configuration_shares_actual_custom_root_without_registering_mcp() {
    let (_dir, state, release, _entry) = fixture().await;
    let before = serde_json::to_value(&state.config.read().await.mcp).unwrap();
    let report = setup(&state, mac(), Some(release)).await.unwrap();
    assert_eq!(report.installed.state, "ready");
    let client = report.external_client.unwrap();
    assert!(Path::new(&client.command).is_absolute());
    assert_eq!(
        client.args,
        [
            "--data-dir",
            state.memory_store.data_dir().to_str().unwrap()
        ]
    );
    assert!(client.args[1].ends_with("separate-canonical-store"));
    assert_eq!(
        serde_json::to_value(&state.config.read().await.mcp).unwrap(),
        before
    );
    assert!(!report.install.available);
}

#[actix_web::test]
async fn incomplete_untrusted_foreign_or_unsigned_provenance_never_exports_a_command() {
    let (_dir, state, release, original) = fixture().await;
    let mut cases = vec![];
    let mut entry = original.clone();
    entry.status = PluginInstallStatus::Installing;
    cases.push(entry);
    let mut entry = original.clone();
    entry.source = PluginSource::LocalDir {
        path: original.plugin_dir.clone(),
    };
    cases.push(entry);
    let mut entry = original.clone();
    entry.version = "0.0.1".into();
    cases.push(entry);
    let mut entry = original.clone();
    entry.plugin_dir = state.app_data_dir.clone();
    cases.push(entry);
    let mut entry = original.clone();
    entry
        .registered
        .mcp_server_ids
        .push("duplicate-memory".into());
    cases.push(entry);
    for (field, value) in [
        ("insecure", serde_json::json!(true)),
        ("allow_unsigned", serde_json::json!(true)),
        ("allow_unverified", serde_json::json!(true)),
        ("allow_untrusted_host", serde_json::json!(true)),
        ("signed_by", serde_json::Value::Null),
        ("sha256", serde_json::json!("b".repeat(64))),
    ] {
        let mut entry = serde_json::to_value(&original).unwrap();
        entry["source"][field] = value;
        cases.push(serde_json::from_value(entry).unwrap());
    }
    for entry in cases {
        save(&state, entry).await;
        assert!(setup(&state, mac(), Some(release.clone()))
            .await
            .unwrap()
            .external_client
            .is_none());
    }
    save(&state, original).await;
    state.config.write().await.plugin_trust.trusted_keys.clear();
    assert!(setup(&state, mac(), Some(release.clone()))
        .await
        .unwrap()
        .external_client
        .is_none());
    assert!(setup(
        &state,
        Host::for_platform("windows", "aarch64", None),
        Some(release)
    )
    .await
    .unwrap()
    .external_client
    .is_none());
}

#[actix_web::test]
async fn tampered_manifest_binary_and_duplicate_registry_are_refused() {
    let (_dir, state, release, entry) = fixture().await;
    let manifest = entry.plugin_dir.join("plugin.json");
    let original = tokio::fs::read(&manifest).await.unwrap();
    tokio::fs::write(&manifest, b"tampered").await.unwrap();
    assert!(setup(&state, mac(), Some(release.clone()))
        .await
        .unwrap()
        .external_client
        .is_none());
    tokio::fs::write(&manifest, original).await.unwrap();
    tokio::fs::write(entry.plugin_dir.join("bin/macos/jiandu"), b"tampered")
        .await
        .unwrap();
    assert!(setup(&state, mac(), Some(release.clone()))
        .await
        .unwrap()
        .external_client
        .is_none());
    InstalledPlugins {
        plugins: vec![entry.clone(), entry],
    }
    .save(&state.app_data_dir.join("plugins/installed.json"))
    .await
    .unwrap();
    assert!(setup(&state, mac(), Some(release)).await.is_err());
}

#[cfg(unix)]
#[actix_web::test]
async fn symlinked_directory_and_executable_are_refused_even_with_matching_bytes() {
    let (dir, state, release, entry) = fixture().await;
    let executable = entry.plugin_dir.join("bin/macos/jiandu");
    let outside = dir.path().join("outside");
    tokio::fs::rename(&executable, &outside).await.unwrap();
    std::os::unix::fs::symlink(&outside, &executable).unwrap();
    assert!(setup(&state, mac(), Some(release.clone()))
        .await
        .unwrap()
        .external_client
        .is_none());
    tokio::fs::remove_file(&executable).await.unwrap();
    tokio::fs::rename(&outside, &executable).await.unwrap();
    let bin = entry.plugin_dir.join("bin/macos");
    tokio::fs::rename(&bin, &outside).await.unwrap();
    std::os::unix::fs::symlink(&outside, &bin).unwrap();
    assert!(setup(&state, mac(), Some(release))
        .await
        .unwrap()
        .external_client
        .is_none());
}

#[actix_web::test]
async fn equal_or_nested_memory_root_blocks_setup_and_official_install_without_mutation() {
    for suffix in ["", "canonical-store"] {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("bamboo");
        let plugin_dir = data_dir.join("plugins/jiandu");
        let state = isolated_state(data_dir, plugin_dir.join(suffix)).await;
        let mcp_before = serde_json::to_value(&state.config.read().await.mcp).unwrap();
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .service(web::scope("/api/v1").service(crate::routes::plugin_scope())),
        )
        .await;
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri("/api/v1/plugins/jiandu/setup")
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::OK);
        let report: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(report["install"]["available"], false);
        assert_eq!(report["install"]["reason"], "memory_root_overlaps_plugin");
        assert_eq!(report["installed"]["reason"], "memory_root_overlaps_plugin");
        assert!(report["external_client"].is_null());
        let response = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/plugins/official/jiandu/install")
                .set_json(serde_json::json!({}))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body["error"]["reason"], "memory_root_overlaps_plugin");
        assert!(!plugin_dir.exists());
        assert!(!state.app_data_dir.join("plugins/installed.json").exists());
        assert_eq!(
            serde_json::to_value(&state.config.read().await.mcp).unwrap(),
            mcp_before
        );
    }
}

#[actix_web::test]
async fn completed_connector_never_exports_an_overlapping_active_store() {
    let (_dir, state, release, entry) = fixture().await;
    let state = isolated_state(
        state.app_data_dir.clone(),
        entry.plugin_dir.join("canonical-store"),
    )
    .await;
    let registry = tokio::fs::read(state.app_data_dir.join("plugins/installed.json"))
        .await
        .unwrap();
    let report = setup(&state, mac(), Some(release)).await.unwrap();
    assert_eq!(report.installed.state, "unverified");
    assert_eq!(report.installed.reason, Some("memory_root_overlaps_plugin"));
    assert_eq!(report.install.reason, Some("memory_root_overlaps_plugin"));
    assert!(report.external_client.is_none());
    assert_eq!(
        tokio::fs::read(state.app_data_dir.join("plugins/installed.json"))
            .await
            .unwrap(),
        registry
    );
}

#[cfg(unix)]
#[actix_web::test]
async fn memory_alias_overlap_and_unresolvable_alias_are_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("bamboo");
    let plugin_dir = data_dir.join("plugins/jiandu");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    let alias = dir.path().join("selected-memory");
    std::os::unix::fs::symlink(&plugin_dir, &alias).unwrap();
    let state = isolated_state(data_dir.clone(), alias.clone()).await;
    let report = setup(&state, mac(), accepted_release()).await.unwrap();
    assert_eq!(report.install.reason, Some("memory_root_overlaps_plugin"));
    assert!(report.external_client.is_none());
    tokio::fs::remove_file(&alias).await.unwrap();
    std::os::unix::fs::symlink(dir.path().join("absent-target"), &alias).unwrap();
    let state = isolated_state(data_dir, alias).await;
    let report = setup(&state, mac(), accepted_release()).await.unwrap();
    assert_eq!(report.install.reason, Some("memory_root_unavailable"));
    assert!(report.external_client.is_none());
}
