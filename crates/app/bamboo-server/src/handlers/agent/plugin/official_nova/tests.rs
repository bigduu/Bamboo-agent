use super::*;
use actix_web::{test as http_test, App};
use bamboo_plugin::{Platform, RegisteredCapabilities};
use chrono::Utc;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn mac() -> Host {
    Host::for_platform("macos", "aarch64")
}

fn missing() -> &'static str {
    r#"{"success":true,"state":"missing","message":"No app","next_action":"Install explicitly"}"#
}

fn artifact() -> AppArtifact {
    AppArtifact {
        artifact_id: "nova-0.3.1-macos-development".into(),
        version: "0.3.1".into(),
        url: "https://github.com/bigduu/Nova/releases/download/v0.3.1/nova-v0.3.1-universal-apple-darwin-development-app.zip".into(),
        sha256: digest(b"fixture archive"),
        signing: "ad_hoc".into(),
        install_mode: "local_ad_hoc".into(),
        development: true,
    }
}

async fn state(dir: &Path) -> web::Data<AppState> {
    let mut state = AppState::new_with_memory_store(
        dir.join("bamboo"),
        bamboo_memory::memory_store::MemoryStore::new(dir.join("isolated-memory")),
    )
    .await
    .unwrap();
    // These transport/provenance fixtures deliberately own in-memory config;
    // unrelated live reload must not replace it or start fake MCP commands.
    state.stop_config_watcher_for_test();
    web::Data::new(state)
}

async fn save(state: &AppState, entry: InstalledPlugin) {
    InstalledPlugins {
        plugins: vec![entry],
    }
    .save(&state.app_data_dir.join("plugins/installed.json"))
    .await
    .unwrap();
}

#[cfg(unix)]
async fn script(path: &Path, body: &str) -> Vec<u8> {
    use std::os::unix::fs::PermissionsExt;
    let bytes = format!("#!/bin/sh\nset -eu\n{body}\n").into_bytes();
    tokio::fs::write(path, &bytes).await.unwrap();
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .await
        .unwrap();
    bytes
}

#[cfg(unix)]
fn quoted(path: &Path) -> String {
    format!("'{}'", path.to_str().unwrap().replace('\'', "'\\''"))
}

#[cfg(unix)]
async fn run_fixture(
    path: &Path,
    args: &[String],
    timeout: Duration,
    legacy: bool,
) -> Result<AppReport, &'static str> {
    let temporary = tempfile::tempdir().unwrap();
    let command = if path.exists() {
        VerifiedCli::snapshot(&tokio::fs::read(path).await.unwrap(), temporary)
            .await
            .unwrap()
    } else {
        VerifiedCli {
            executable: temporary.path().join("missing"),
            _temporary: temporary,
        }
    };
    let private = command._temporary.path().to_path_buf();
    let arguments: Vec<OsString> = args.iter().map(OsString::from).collect();
    let result = run_cli(&command, &arguments, timeout, legacy).await;
    drop(command);
    assert!(!private.exists());
    result
}

#[cfg(unix)]
async fn fixture(
    body: &str,
) -> (
    tempfile::TempDir,
    web::Data<AppState>,
    Catalog,
    InstalledPlugin,
) {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path()).await;
    let plugin_dir = state.app_data_dir.join("plugins/nova");
    tokio::fs::create_dir_all(plugin_dir.join("bin/macos"))
        .await
        .unwrap();
    let manifest = serde_json::to_vec(&serde_json::json!({
        "id":"nova", "name":"Fixture Nova", "version":"0.3.1", "platforms":["macos","windows"],
        "provides":{"mcp_servers":[
            {"id":"nova", "transport":{"type":"stdio","command":"${platform_bin}","args":["mcp"]}},
            {"id":"nova-chrome-devtools", "enabled":false,
             "transport":{"type":"stdio","command":"${platform_bin}","args":["chrome"]}}
        ]}
    }))
    .unwrap();
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest)
        .await
        .unwrap();
    let binary = script(&plugin_dir.join("bin/macos/nova"), body).await;
    let release = ConnectorRelease {
        version: "0.3.1".into(),
        bundle_url:
            "https://github.com/bigduu/Nova/releases/download/v0.3.1/nova-plugin-v0.3.1.tar.gz"
                .into(),
        bundle_sha256: "a".repeat(64),
        manifest_sha256: digest(&manifest),
        executable_sha256: BTreeMap::from([("macos".into(), digest(&binary))]),
        legacy_cli: false,
    };
    let entry = InstalledPlugin {
        id: "nova".into(),
        version: release.version.clone(),
        source: PluginSource::Url {
            url: release.bundle_url.clone(),
            sha256: Some(release.bundle_sha256.clone()),
            signed_by: Some("nova (bigduu official)".into()),
            allow_unverified: false,
            allow_unsigned: false,
            allow_untrusted_host: false,
            insecure: false,
        },
        plugin_dir,
        installed_at: Utc::now(),
        status: PluginInstallStatus::Installed,
        registered: RegisteredCapabilities {
            mcp_server_ids: vec!["nova".into(), "nova-chrome-devtools".into()],
            ..Default::default()
        },
    };
    let parsed: PluginManifest = serde_json::from_slice(&manifest).unwrap();
    state.config.write().await.mcp.servers = parsed
        .provides
        .mcp_servers
        .iter()
        .map(|server| {
            server
                .resolve(&entry.plugin_dir, "nova", Platform::Macos)
                .unwrap()
        })
        .collect();
    save(&state, entry.clone()).await;
    (
        dir,
        state,
        Catalog {
            connectors: vec![release],
            app: Some(artifact()),
        },
        entry,
    )
}

#[test]
fn host_matrix_does_not_call_windows_missing_app_a_failure() {
    for (os, arch, supported, reason) in [
        ("macos", "aarch64", true, None),
        ("macos", "x86_64", true, None),
        ("macos", "unknown", false, Some("unsupported_host")),
        ("windows", "x86_64", false, Some("app_not_required")),
        ("windows", "aarch64", false, Some("app_not_required")),
        ("linux", "x86_64", false, Some("unsupported_host")),
    ] {
        let host = Host::for_platform(os, arch);
        assert_eq!((host.supported, host.reason), (supported, reason));
    }
}

#[test]
fn reviewed_catalog_uses_immutable_release_urls_and_valid_digests() {
    let catalog = accepted_catalog();
    assert!(!catalog.connectors.is_empty());
    for release in catalog.connectors {
        assert_eq!(
            release.bundle_url,
            format!(
                "https://github.com/bigduu/Nova/releases/download/v{0}/nova-plugin-v{0}.tar.gz",
                release.version
            )
        );
        for hash in std::iter::once(&release.bundle_sha256)
            .chain(std::iter::once(&release.manifest_sha256))
            .chain(release.executable_sha256.values())
        {
            assert_eq!(hash.len(), 64);
            assert!(hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }
    if let Some(artifact) = catalog.app {
        assert!(valid_artifact(&artifact));
    }
    let mut invalid = artifact();
    invalid.url = "https://example.invalid/latest.zip".into();
    assert!(!valid_artifact(&invalid));
    invalid = artifact();
    invalid.signing = "local_self_signed".into();
    assert!(!valid_artifact(&invalid));
    invalid = artifact();
    invalid.version = "0.3.1-rc.1".into();
    assert!(!valid_artifact(&invalid));
}

#[test]
fn desktop_proof_matches_installer_resolution_without_accepting_path_aliases() {
    let manifest: PluginManifest = serde_json::from_value(serde_json::json!({
        "id":"nova","name":"Fixture Nova","version":"0.3.1","provides":{"mcp_servers":[
            {"id":"nova","transport":{"type":"stdio","command":"${platform_bin}","args":["mcp"]}}
        ]}
    }))
    .unwrap();
    for (os, root, alias) in [
        (
            "windows",
            r"C:\Bamboo\plugins\nova",
            r"\\?\C:\Bamboo\plugins\nova",
        ),
        (
            "macos",
            "/var/bamboo/plugins/nova",
            "/private/var/bamboo/plugins/nova",
        ),
    ] {
        let entry=InstalledPlugin {
            id:"nova".into(),version:"0.3.1".into(),plugin_dir:PathBuf::from(root),
            source:PluginSource::Url {url:"https://github.com/bigduu/Nova/releases/download/v0.3.1/nova-plugin-v0.3.1.tar.gz".into(),sha256:Some("a".repeat(64)),signed_by:Some("nova".into()),allow_unverified:false,allow_unsigned:false,allow_untrusted_host:false,insecure:false},
            installed_at:Utc::now(),status:PluginInstallStatus::Installed,
            registered:RegisteredCapabilities {mcp_server_ids:vec!["nova".into()],..Default::default()},
        };
        let desktop = desktop_entry(&manifest).unwrap();
        let mut server = desktop
            .resolve(&entry.plugin_dir, "nova", Platform::parse(os).unwrap())
            .unwrap();
        assert_eq!(
            desktop_server_id(&entry, &manifest, os, &[server.clone()]).as_deref(),
            Some("nova")
        );
        let foreign = desktop
            .resolve(Path::new(alias), "nova", Platform::parse(os).unwrap())
            .unwrap();
        server.transport = foreign.transport;
        assert!(desktop_server_id(&entry, &manifest, os, &[server]).is_none());
    }
}

#[actix_web::test]
async fn http_is_passive_and_rejects_caller_commands_paths_urls_or_flags() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path()).await;
    let app = http_test::init_service(
        App::new()
            .app_data(state.clone())
            .service(web::scope("/api/v1").service(crate::routes::plugin_scope())),
    )
    .await;
    let response = http_test::call_service(
        &app,
        http_test::TestRequest::get()
            .uri("/api/v1/plugins/nova/app")
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::OK);
    let report: serde_json::Value = http_test::read_body_json(response).await;
    let expected_unavailable = report["install"]["reason"].clone();
    assert_eq!(report["plugin_id"], "nova");
    assert_eq!(report["connector"]["state"], "missing");
    assert!(report["app"].is_null());
    assert!(report["desktop_server_id"].is_null());
    assert_eq!(report["desktop_readiness"], "not_tested");
    for body in [
        serde_json::json!({"command":"/tmp/nova"}),
        serde_json::json!({"path":"/tmp/Nova.app"}),
        serde_json::json!({"url":"https://example.invalid/app.zip"}),
        serde_json::json!({"insecure":true}),
    ] {
        let response = http_test::call_service(
            &app,
            http_test::TestRequest::post()
                .uri("/api/v1/plugins/nova/app/install")
                .set_json(body)
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), actix_web::http::StatusCode::BAD_REQUEST);
    }
    let response = http_test::call_service(
        &app,
        http_test::TestRequest::post()
            .uri("/api/v1/plugins/nova/app/install")
            .set_json(serde_json::json!({}))
            .to_request(),
    )
    .await;
    assert_eq!(response.status(), actix_web::http::StatusCode::CONFLICT);
    let error: serde_json::Value = http_test::read_body_json(response).await;
    assert_eq!(error["error"]["reason"], expected_unavailable);
    assert!(!state.app_data_dir.join("plugins/installed.json").exists());
}

#[cfg(unix)]
#[actix_web::test]
async fn missing_success_is_capability_and_install_eligibility_not_desktop_readiness() {
    let (_dir, state, catalog, _entry) = fixture(&format!("printf '%s\\n' '{}'", missing())).await;
    let before = serde_json::to_value(&state.config.read().await.mcp).unwrap();
    let report = inspect_locked(&state, mac(), &catalog)
        .await
        .unwrap()
        .report;
    assert_eq!(report.connector.state, "verified");
    assert!(report.capability.supported);
    assert!(report.install.available);
    assert_eq!(report.app.unwrap().state, AppStateKind::Missing);
    assert_eq!(report.desktop_server_id.as_deref(), Some("nova"));
    assert_eq!(report.desktop_readiness, "not_tested");
    assert_eq!(
        serde_json::to_value(&state.config.read().await.mcp).unwrap(),
        before
    );
}

#[actix_web::test]
async fn cancelled_queued_operation_drops_work_without_acquiring_the_plugin_lock() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path()).await;
    let installer = ServerPluginInstaller::new(state.clone());
    let held = installer.begin_operation().await;
    let (started, mut start) = tokio::sync::oneshot::channel();
    let (finish, completion) = tokio::sync::oneshot::channel::<()>();
    let mut request = Box::pin(run_owned_operation(state, async move {
        started.send(()).unwrap();
        completion.await.unwrap();
    }));
    assert!(futures::poll!(request.as_mut()).is_pending());
    drop(request);
    assert!(finish.is_closed(), "cancelled queued work must be dropped");
    assert!(matches!(
        start.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    ));
    drop(held);
    let _following = tokio::time::timeout(Duration::from_secs(2), installer.begin_operation())
        .await
        .expect("cancelled queued work must not retain a plugin-lock waiter");
}

#[actix_web::test]
async fn started_operation_keeps_plugin_lock_after_request_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path()).await;
    let installer = ServerPluginInstaller::new(state.clone());
    // Force the first poll to queue on the real operation lock.
    let held = installer.begin_operation().await;
    let (started, start) = tokio::sync::oneshot::channel();
    let (finish, completion) = tokio::sync::oneshot::channel::<()>();
    let mut request = Box::pin(run_owned_operation(state, async move {
        started.send(()).unwrap();
        completion.await.unwrap();
    }));
    assert!(futures::poll!(request.as_mut()).is_pending());
    drop(held);
    // A queued request must keep being polled until its operation starts;
    // waiting only on `start` would leave it suspended at lock acquisition.
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            started = start => started.unwrap(),
            result = request.as_mut() => panic!("operation completed before its start handshake: {result:?}"),
        }
    })
        .await
        .unwrap();
    drop(request);
    assert!(
        !finish.is_closed(),
        "started work must retain owned cleanup"
    );
    let mut following = Box::pin(installer.begin_operation());
    assert!(futures::poll!(following.as_mut()).is_pending());
    finish.send(()).unwrap();
    let _following = tokio::time::timeout(Duration::from_secs(2), following)
        .await
        .expect("completed owned work must release the plugin lock");
}

#[cfg(unix)]
#[actix_web::test]
async fn existing_app_or_absent_accepted_artifact_never_enables_installation() {
    let report = r#"{"success":true,"state":"installed","message":"Existing","next_action":"Open manually","path":"/fixture/Nova.app","version":"0.3.1","signing":"ad_hoc","development":true,"connector_compatible":true}"#;
    let (_dir, state, mut catalog, _entry) = fixture(&format!("printf '%s\\n' '{report}'")).await;
    let status = inspect_locked(&state, mac(), &catalog)
        .await
        .unwrap()
        .report;
    assert!(status.capability.supported);
    assert!(!status.install.available);
    assert_eq!(status.install.reason, Some("app_not_missing"));
    assert_eq!(status.desktop_readiness, "not_tested");
    assert_eq!(
        install_downloaded(
            &state,
            mac(),
            &catalog,
            catalog.app.as_ref().unwrap(),
            &state.app_data_dir.join("unused.zip")
        )
        .await
        .err()
        .unwrap(),
        "app_not_missing"
    );
    catalog.app = None;
    let status = inspect_locked(&state, mac(), &catalog)
        .await
        .unwrap()
        .report;
    assert_eq!(status.install.reason, Some("artifact_unavailable"));
}

#[cfg(unix)]
#[actix_web::test]
async fn unsupported_host_never_executes_even_an_accepted_package() {
    let (_dir, state, catalog, _entry) = fixture("exit 66").await;
    for host in [
        Host::for_platform("windows", "x86_64"),
        Host::for_platform("linux", "x86_64"),
    ] {
        let status = inspect_locked(&state, host, &catalog).await.unwrap().report;
        assert!(!status.capability.supported);
        assert!(status.app.is_none());
        assert!(!status.install.available);
        assert_eq!(status.capability.reason, status.host.reason);
    }
}

#[cfg(unix)]
#[actix_web::test]
async fn incomplete_foreign_unsigned_or_bypassed_provenance_never_executes() {
    let (_dir, state, catalog, original) = fixture("exit 66").await;
    let mut variants = Vec::new();
    let mut entry = original.clone();
    entry.status = PluginInstallStatus::Installing;
    variants.push(entry);
    let mut entry = original.clone();
    entry.plugin_dir = entry.plugin_dir.join("foreign");
    variants.push(entry);
    for key in [
        "url",
        "sha256",
        "signed_by",
        "allow_unverified",
        "allow_unsigned",
        "allow_untrusted_host",
        "insecure",
    ] {
        let mut entry = original.clone();
        if let PluginSource::Url {
            url,
            sha256,
            signed_by,
            allow_unverified,
            allow_unsigned,
            allow_untrusted_host,
            insecure,
        } = &mut entry.source
        {
            match key {
                "url" => *url = "https://example.invalid/foreign.tar.gz".into(),
                "sha256" => *sha256 = None,
                "signed_by" => *signed_by = None,
                "allow_unverified" => *allow_unverified = true,
                "allow_unsigned" => *allow_unsigned = true,
                "allow_untrusted_host" => *allow_untrusted_host = true,
                _ => *insecure = true,
            }
        }
        variants.push(entry);
    }
    for entry in variants {
        save(&state, entry).await;
        let status = inspect_locked(&state, mac(), &catalog)
            .await
            .unwrap()
            .report;
        assert_ne!(status.connector.state, "verified");
        assert!(status.app.is_none());
        assert!(status.desktop_server_id.is_none());
        assert!(!status.install.available);
    }
}

#[cfg(unix)]
#[actix_web::test]
async fn tampered_managed_manifest_or_executable_prevents_cli_execution() {
    for filename in ["plugin.json", "bin/macos/nova"] {
        let (_dir, state, catalog, entry) = fixture("exit 66").await;
        tokio::fs::write(entry.plugin_dir.join(filename), b"tampered")
            .await
            .unwrap();
        let status = inspect_locked(&state, mac(), &catalog)
            .await
            .unwrap()
            .report;
        assert_eq!(status.connector.state, "unverified");
        assert!(status.app.is_none());
        assert!(!status.install.available);
    }
}

#[cfg(unix)]
#[actix_web::test]
async fn verified_snapshot_executes_original_bytes_despite_managed_path_replacement() {
    use std::os::unix::fs::PermissionsExt;
    for atomic in [true, false] {
        let (_dir, state, catalog, entry) =
            fixture(&format!("printf '%s\\n' '{}'", missing())).await;
        let (verified, _) = validated_cli(&state, &entry, &catalog.connectors[0], "macos")
            .await
            .unwrap();
        let snapshot = verified.executable.clone();
        let private = verified._temporary.path().to_path_buf();
        assert_eq!(
            std::fs::metadata(&private).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(&snapshot).unwrap().permissions().mode() & 0o777,
            0o500
        );
        let command = entry.plugin_dir.join("bin/macos/nova");
        let marker = state.app_data_dir.join("unverified-executed");
        let replacement = if atomic {
            command.with_extension("replacement")
        } else {
            command.clone()
        };
        script(
            &replacement,
            &format!(": > {}; printf '%s\\n' '{}'", quoted(&marker), missing()),
        )
        .await;
        if atomic {
            tokio::fs::rename(&replacement, &command).await.unwrap();
        }
        let report = run_cli(
            &verified,
            &["app".into(), "status".into()],
            STATUS_TIMEOUT,
            false,
        )
        .await
        .unwrap();
        assert_eq!(report.state, AppStateKind::Missing);
        assert!(
            !marker.exists(),
            "the replaced managed executable must never run"
        );
        assert!(
            validated_cli(&state, &entry, &catalog.connectors[0], "macos")
                .await
                .is_err()
        );
        drop(verified);
        assert!(!snapshot.exists());
        assert!(!private.exists());
    }
}

#[cfg(unix)]
#[actix_web::test]
async fn nonexecutable_file_and_fifo_fail_validation_without_blocking() {
    use std::os::unix::{ffi::OsStrExt, fs::PermissionsExt};
    let (_dir, state, catalog, entry) = fixture("exit 66").await;
    let command = entry.plugin_dir.join("bin/macos/nova");
    tokio::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o600))
        .await
        .unwrap();
    assert!(
        validated_cli(&state, &entry, &catalog.connectors[0], "macos")
            .await
            .is_err()
    );
    tokio::fs::remove_file(&command).await.unwrap();
    let fifo = std::ffi::CString::new(command.as_os_str().as_bytes()).unwrap();
    // SAFETY: fifo is a live NUL-terminated path inside this test's temporary directory.
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    assert!(tokio::time::timeout(
        Duration::from_secs(1),
        validated_cli(&state, &entry, &catalog.connectors[0], "macos")
    )
    .await
    .expect("FIFO open must not block")
    .is_err());
}

#[tokio::test]
async fn snapshot_creation_failure_removes_its_owned_private_files() {
    let temporary = tempfile::tempdir().unwrap();
    let private = temporary.path().to_path_buf();
    tokio::fs::write(
        private.join(if cfg!(windows) { "nova.exe" } else { "nova" }),
        b"occupied",
    )
    .await
    .unwrap();
    assert!(VerifiedCli::snapshot(b"must not overwrite", temporary)
        .await
        .is_err());
    assert!(!private.exists());
}

#[cfg(unix)]
#[test]
fn install_arguments_preserve_non_utf8_temporary_archive_paths() {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    let bytes = b"/private/tmp/tmp-\xff/Nova.zip";
    let archive = PathBuf::from(OsString::from_vec(bytes.to_vec()));
    assert!(archive.to_str().is_none());

    let arguments = install_arguments(&artifact(), &archive);
    assert_eq!(arguments[3].as_os_str().as_bytes(), bytes);
}

#[cfg(unix)]
#[actix_web::test]
async fn symlinked_managed_directory_or_binary_is_not_package_owned() {
    use std::os::unix::fs::symlink;
    for relative in ["bin/macos", "bin/macos/nova"] {
        let (dir, state, catalog, entry) = fixture("exit 66").await;
        let original = entry.plugin_dir.join(relative);
        let foreign = dir.path().join("foreign");
        tokio::fs::rename(&original, &foreign).await.unwrap();
        symlink(&foreign, &original).unwrap();
        let status = inspect_locked(&state, mac(), &catalog)
            .await
            .unwrap()
            .report;
        assert_eq!(status.connector.state, "unverified");
        assert!(status.app.is_none());
    }
}

#[cfg(unix)]
#[actix_web::test]
async fn removed_publisher_or_source_trust_fails_closed_even_if_generic_enforcement_is_off() {
    let (_dir, state, catalog, _entry) = fixture("exit 66").await;
    let original = state.config.read().await.plugin_trust.clone();
    for publisher in [true, false] {
        let mut trust = original.clone();
        trust.enforcement = bamboo_config::PluginTrustEnforcement::Off;
        if publisher {
            trust.trusted_keys.clear();
        } else {
            trust.trusted_hosts.clear();
        }
        state.config.write().await.plugin_trust = trust;
        let status = inspect_locked(&state, mac(), &catalog)
            .await
            .unwrap()
            .report;
        assert_eq!(
            status.connector.reason,
            Some(if publisher {
                "publisher_not_trusted"
            } else {
                "source_not_trusted"
            })
        );
        assert!(status.app.is_none());
        assert!(!status.install.available);
    }
}

#[cfg(unix)]
#[actix_web::test]
async fn only_exact_registered_desktop_server_is_returned_for_reconnect() {
    let (_dir, state, catalog, mut entry) =
        fixture(&format!("printf '%s\\n' '{}'", missing())).await;
    let original = state.config.read().await.mcp.servers.clone();
    for variant in 0..6 {
        let mut servers = original.clone();
        match variant {
            0 => servers.retain(|server| server.id != "nova"),
            1 => servers.push(servers[0].clone()),
            2 => servers[0].enabled = false,
            3 => {
                if let TransportConfig::Stdio(config) = &mut servers[0].transport {
                    config.command = "/tmp/foreign/nova".into();
                }
            }
            4 => {
                if let TransportConfig::Stdio(config) = &mut servers[0].transport {
                    config.args = vec!["chrome".into()];
                }
            }
            _ => {
                if let TransportConfig::Stdio(config) = &mut servers[0].transport {
                    config.env.insert("INJECT".into(), "value".into());
                }
            }
        }
        state.config.write().await.mcp.servers = servers;
        let report = inspect_locked(&state, mac(), &catalog)
            .await
            .unwrap()
            .report;
        assert!(report.capability.supported);
        assert!(report.desktop_server_id.is_none());
    }
    state.config.write().await.mcp.servers = original;
    entry.registered.mcp_server_ids = vec!["nova-chrome-devtools".into()];
    save(&state, entry).await;
    assert!(inspect_locked(&state, mac(), &catalog)
        .await
        .unwrap()
        .report
        .desktop_server_id
        .is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn legacy_requires_known_legacy_receipt_and_actual_app_rejection() {
    let dir = tempfile::tempdir().unwrap();
    let command = dir.path().join("nova");
    script(
        &command,
        "printf \"error: unrecognized subcommand 'app'\\n\" >&2; exit 2",
    )
    .await;
    assert_eq!(
        run_fixture(&command, &[], STATUS_TIMEOUT, true)
            .await
            .unwrap_err(),
        "legacy_cli_unavailable"
    );
    assert_eq!(
        run_fixture(&command, &[], STATUS_TIMEOUT, false)
            .await
            .unwrap_err(),
        "invalid_report"
    );
    script(&command, "printf 'not JSON'; exit 0").await;
    assert_eq!(
        run_fixture(&command, &[], STATUS_TIMEOUT, true)
            .await
            .unwrap_err(),
        "invalid_report"
    );
    tokio::fs::remove_file(&command).await.unwrap();
    assert_eq!(
        run_fixture(&command, &[], STATUS_TIMEOUT, true)
            .await
            .unwrap_err(),
        "status_failed"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn malformed_inconsistent_or_permission_claiming_reports_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let command = dir.path().join("nova");
    for report in [
        r#"{"success":true,"state":"installed","message":"yes","next_action":"open"}"#,
        r#"{"success":true,"state":"missing","message":"yes","next_action":"open","permission_ready":true}"#,
        r#"{"success":true,"state":"error","message":"yes","next_action":"open"}"#,
        r#"{"success":true,"state":"missing","message":"","next_action":"open"}"#,
        r#"{"success":true,"state":"missing","message":"yes","next_action":"open","signing":"notarized"}"#,
    ] {
        script(&command, &format!("printf '%s\\n' '{report}'")).await;
        assert_eq!(
            run_fixture(&command, &[], STATUS_TIMEOUT, false)
                .await
                .unwrap_err(),
            "invalid_report"
        );
    }
    script(&command, &format!("printf '%s\\n' '{}'; exit 1", missing())).await;
    assert_eq!(
        run_fixture(&command, &[], STATUS_TIMEOUT, false)
            .await
            .unwrap_err(),
        "invalid_report"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn exit_observation_holds_the_leader_and_nonchild_is_an_error() {
    let mut command = tokio::process::Command::new("/bin/sh");
    command
        .args(["-c", "exit 7"])
        .process_group(0)
        .kill_on_drop(false);
    let mut owned = ProcessGroup {
        child: command.spawn().unwrap(),
        armed: true,
    };
    tokio::time::timeout(Duration::from_secs(2), owned.wait_without_reaping())
        .await
        .unwrap()
        .unwrap();
    assert!(observe_exit(owned.child.id().unwrap()).unwrap());
    owned.stop().unwrap();
    assert!(!owned.armed);
    assert_eq!(owned.child.wait().await.unwrap().code(), Some(7));
    assert!(owned.child.id().is_none());
    assert_eq!(
        observe_exit(std::process::id()).unwrap_err().raw_os_error(),
        Some(libc::ECHILD)
    );
}

#[cfg(unix)]
#[tokio::test]
async fn successful_leader_exit_still_cleans_up_its_background_child() {
    let dir = tempfile::tempdir().unwrap();
    let command = dir.path().join("nova");
    let marker = dir.path().join("surviving-child");
    script(
        &command,
        &format!(
            "(sleep 1; printf survived > {}) >/dev/null 2>&1 &\nprintf '%s\\n' '{}'\nexit 0",
            quoted(&marker),
            missing()
        ),
    )
    .await;
    assert!(
        run_fixture(&command, &[], STATUS_TIMEOUT, false)
            .await
            .unwrap()
            .success
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(!marker.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn cancellation_cleans_up_the_unreaped_leader_and_child() {
    let dir = tempfile::tempdir().unwrap();
    let command = dir.path().join("nova");
    let started = dir.path().join("started");
    let marker = dir.path().join("surviving-child");
    script(
        &command,
        &format!(
            "(sleep 1; printf survived > {}) &\nprintf '%s' \"$$\" > {}\nexit 0",
            quoted(&marker),
            quoted(&started)
        ),
    )
    .await;
    let task = tokio::spawn(async move { run_fixture(&command, &[], STATUS_TIMEOUT, false).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let pid = tokio::fs::read_to_string(&started)
                .await
                .ok()
                .and_then(|raw| raw.parse::<u32>().ok());
            if pid.is_some_and(|pid| observe_exit(pid).unwrap()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(!marker.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn output_limits_and_timeout_clean_up_the_owned_process_group() {
    let dir = tempfile::tempdir().unwrap();
    let command = dir.path().join("nova");
    script(&command,"while :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; done").await;
    assert_eq!(
        run_fixture(&command, &[], STATUS_TIMEOUT, false)
            .await
            .unwrap_err(),
        "invalid_report"
    );
    let marker = dir.path().join("surviving-child");
    script(
        &command,
        &format!("(sleep 1; printf 'survived' > {}) &\nwait", quoted(&marker)),
    )
    .await;
    assert_eq!(
        run_fixture(&command, &[], Duration::from_millis(100), false)
            .await
            .unwrap_err(),
        "status_timeout"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert!(!marker.exists());
}

#[tokio::test]
async fn bounded_download_verifies_bytes_and_keeps_the_archive_private_and_temporary() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"fixture archive".to_vec()))
        .mount(&server)
        .await;
    let response = reqwest::get(server.uri()).await.unwrap();
    let (temporary, archive) = save_archive(response, &artifact().sha256, 64)
        .await
        .unwrap();
    assert_eq!(tokio::fs::read(&archive).await.unwrap(), b"fixture archive");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(temporary.path())
                .unwrap()
                .permissions()
                .mode()
                & 0o077,
            0
        );
        assert_eq!(
            std::fs::metadata(&archive).unwrap().permissions().mode() & 0o077,
            0
        );
    }
    drop(temporary);
    assert!(!archive.exists());
    assert_eq!(
        save_archive(
            reqwest::get(server.uri()).await.unwrap(),
            &"0".repeat(64),
            64
        )
        .await
        .unwrap_err(),
        "archive_checksum_mismatch"
    );
    assert_eq!(
        save_archive(
            reqwest::get(server.uri()).await.unwrap(),
            &artifact().sha256,
            4
        )
        .await
        .unwrap_err(),
        "download_too_large"
    );
}

#[tokio::test]
async fn chunked_download_cannot_evade_the_size_limit() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let sender = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0u8; 1024];
        stream.read(&mut request).await.unwrap();
        stream.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n8\r\nabcdefgh\r\n8\r\nijklmnop\r\n0\r\n\r\n").await.unwrap();
    });
    let response = reqwest::get(format!("http://{address}")).await.unwrap();
    assert!(response.content_length().is_none());
    assert_eq!(
        save_archive(response, &digest(b"abcdefghijklmnop"), 10)
            .await
            .unwrap_err(),
        "download_too_large"
    );
    sender.await.unwrap();
}

#[cfg(unix)]
#[actix_web::test]
async fn fixed_install_arguments_are_followed_by_fresh_structured_confirmation() {
    let (_dir, state, mut catalog, entry) = fixture("exit 66").await;
    let marker = state.app_data_dir.join("installed-marker");
    let arguments = state.app_data_dir.join("install-arguments");
    let installed = r#"{"success":true,"state":"installed","message":"Installed","next_action":"Open manually and grant permissions","path":"/fixture/Nova.app","version":"0.3.1","signing":"ad_hoc","development":true,"connector_compatible":true}"#;
    let body=format!("if [ \"$2\" = install ]; then\n  printf '%s\\n' \"$@\" > {}\n  : > {}\nfi\nif [ -e {} ]; then printf '%s\\n' '{}'; else printf '%s\\n' '{}'; fi",quoted(&arguments),quoted(&marker),quoted(&marker),installed,missing());
    let binary = script(&entry.plugin_dir.join("bin/macos/nova"), &body).await;
    catalog.connectors[0]
        .executable_sha256
        .insert("macos".into(), digest(&binary));
    let archive = state.app_data_dir.join("private-fixture.zip");
    tokio::fs::write(&archive, b"fixture archive")
        .await
        .unwrap();
    let installer = ServerPluginInstaller::new(state.clone());
    let _guard = installer.begin_operation().await;
    let report = install_downloaded(
        &state,
        mac(),
        &catalog,
        catalog.app.as_ref().unwrap(),
        &archive,
    )
    .await
    .unwrap();
    let args = tokio::fs::read_to_string(arguments).await.unwrap();
    assert_eq!(
        args.lines().collect::<Vec<_>>(),
        vec![
            "app",
            "install",
            "--archive",
            archive.to_str().unwrap(),
            "--sha256",
            &artifact().sha256,
            "--local-sign"
        ]
    );
    assert_eq!(report.app.unwrap().state, AppStateKind::Installed);
    assert_eq!(report.desktop_server_id.as_deref(), Some("nova"));
    assert_eq!(report.desktop_readiness, "not_tested");
    assert!(!report.install.available);
}

#[cfg(unix)]
#[actix_web::test]
async fn download_time_policy_and_binary_changes_are_rechecked_before_install() {
    let (_dir, state, catalog, entry) = fixture(&format!("printf '%s\\n' '{}'", missing())).await;
    let artifact = catalog.app.as_ref().unwrap();
    let archive = state.app_data_dir.join("fixture.zip");
    state.config.write().await.plugin_trust.trusted_keys.clear();
    assert_eq!(
        install_downloaded(&state, mac(), &catalog, artifact, &archive)
            .await
            .err()
            .unwrap(),
        "publisher_not_trusted"
    );
    state.config.write().await.plugin_trust = PluginTrustConfig::default();
    tokio::fs::write(
        entry.plugin_dir.join("bin/macos/nova"),
        b"tampered after download",
    )
    .await
    .unwrap();
    assert_eq!(
        install_downloaded(&state, mac(), &catalog, artifact, &archive)
            .await
            .err()
            .unwrap(),
        "unverified_install"
    );
}

#[cfg(unix)]
#[actix_web::test]
async fn install_exit_success_cannot_replace_fresh_installed_confirmation() {
    let installed = r#"{"success":true,"state":"installed","message":"Installed","next_action":"Open manually","path":"/fixture/Nova.app","version":"0.3.1","signing":"ad_hoc","development":true,"connector_compatible":true}"#;
    let body = format!(
        "if [ \"$2\" = install ]; then printf '%s\\n' '{installed}'; else printf '%s\\n' '{}'; fi",
        missing()
    );
    let (_dir, state, catalog, _entry) = fixture(&body).await;
    assert_eq!(
        install_downloaded(
            &state,
            mac(),
            &catalog,
            catalog.app.as_ref().unwrap(),
            &state.app_data_dir.join("fixture.zip")
        )
        .await
        .err()
        .unwrap(),
        "install_unconfirmed"
    );
}

#[cfg(unix)]
#[actix_web::test]
async fn already_installed_is_reported_without_retrying_or_replacing_the_app() {
    let existing = r#"{"success":false,"state":"already_installed","message":"Existing app","next_action":"Inspect manually"}"#;
    let body=format!("if [ \"$2\" = install ]; then printf '%s\\n' '{existing}'; exit 1; else printf '%s\\n' '{}'; fi",missing());
    let (_dir, state, catalog, _entry) = fixture(&body).await;
    assert_eq!(
        install_downloaded(
            &state,
            mac(),
            &catalog,
            catalog.app.as_ref().unwrap(),
            &state.app_data_dir.join("fixture.zip")
        )
        .await
        .err()
        .unwrap(),
        "already_installed"
    );
}

mod lifecycle_tests;

#[cfg(target_os = "linux")]
mod spawn_busy;
