//! Actix integration tests for `/api/v1/plugins` — install / list / update /
//! remove, and the error->status mapping end to end through the real HTTP
//! handlers (not just `plugin_error_response` in isolation — see
//! `super::errors::tests` for that). Mirrors the `App::new().app_data(...)
//! .route(...)` + `test::call_service` pattern used in
//! `handlers/agent/stream/tests.rs`.
//!
//! Every test installs from the checked-in
//! `crates/infra/bamboo-plugin/examples/hello-plugin` fixture as a
//! `local_dir` source — `prepare_plugin_source` COPIES a `LocalDir` source
//! (see `plugin_source.rs::stage_into`), so the checked-in fixture is never
//! mutated — and a throwaway `tempfile::tempdir()` `AppState`, never
//! `~/.bamboo`.

use std::path::{Path, PathBuf};

use actix_web::http::StatusCode;
use actix_web::{test, web, App};
use bamboo_plugin::manifest::{McpServerManifestEntry, McpTransportManifest, Platform};
use bamboo_plugin::{
    InstalledPlugin, InstalledPlugins, PluginInstallStatus, PluginSource, RegisteredCapabilities,
};
use bamboo_plugin_protocol::{
    FILE_CHANGED_SUBSCRIPTION_ID_V1, TOOL_EVENT_PROTOCOL_NAME, TOOL_EVENT_V1_SCHEMA_VERSION,
};
use chrono::Utc;

use crate::app_state::AppState;

use super::handlers::{install_plugin, list_plugins, remove_plugin, update_plugin};

fn hello_plugin_example_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../infra/bamboo-plugin/examples/hello-plugin")
}

async fn test_state(data_dir: &Path) -> web::Data<AppState> {
    web::Data::new(
        AppState::new(data_dir.to_path_buf())
            .await
            .expect("app state should initialize"),
    )
}

/// Registers the same 4 routes `routes::agent::plugin_scope` wires under
/// `/api/v1/plugins` (see that module) directly on a bare `App`, matching
/// `handlers/agent/stream/tests.rs`'s inline-`App`-per-test style — factoring
/// this into a shared fn would need to name `App`'s hairy service-factory
/// generic, which isn't worth it for 4 `.route()` calls repeated 6 times.
macro_rules! plugin_test_app {
    ($state:expr) => {
        App::new()
            .app_data($state)
            .route("/api/v1/plugins", web::get().to(list_plugins))
            .route("/api/v1/plugins/install", web::post().to(install_plugin))
            .route("/api/v1/plugins/{id}/update", web::post().to(update_plugin))
            .route("/api/v1/plugins/{id}", web::delete().to(remove_plugin))
    };
}

fn local_dir_source(path: &Path) -> serde_json::Value {
    serde_json::json!({
        "source": { "type": "local_dir", "path": path.to_string_lossy() }
    })
}

/// Writes a minimal, syntactically-valid-but-`validate()`-rejected manifest
/// (an id with a space and `!`, which `is_valid_plugin_id` forbids) to a
/// fresh tempdir plugin bundle. Mirrors
/// `plugin_source::tests::stages_local_dir_rejects_invalid_manifest_...`'s
/// fixture shape.
async fn write_bad_manifest_plugin_dir(root: &Path) -> PathBuf {
    let dir = root.join("bad-plugin-source");
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(
        dir.join("plugin.json"),
        serde_json::json!({
            "id": "Bad Id!",
            "name": "Bad",
            "version": "1.0.0"
        })
        .to_string(),
    )
    .await
    .unwrap();
    dir
}

/// Writes a plugin bundle declaring one MCP server with the given id (stdio,
/// pointed at a nonexistent binary so `mcp_manager.start_server` fails fast
/// rather than hanging — the config write/registration is what these tests
/// care about, not a real handshake).
async fn write_mcp_plugin_dir(root: &Path, plugin_id: &str, mcp_id: &str) -> PathBuf {
    let dir = root.join(format!("{plugin_id}-source"));
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(
        dir.join("plugin.json"),
        serde_json::json!({
            "id": plugin_id,
            "name": "Test Plugin",
            "version": "1.0.0",
            "provides": {
                "mcp_servers": [
                    {
                        "id": mcp_id,
                        "transport": {
                            "type": "stdio",
                            "command": "/nonexistent/bamboo-test-mcp-binary-does-not-exist"
                        }
                    }
                ]
            }
        })
        .to_string(),
    )
    .await
    .unwrap();
    dir
}

async fn write_event_sink_plugin_dir(
    root: &Path,
    source_name: &str,
    plugin_id: &str,
    version: &str,
    marker: &str,
) -> PathBuf {
    let dir = root.join(source_name);
    tokio::fs::create_dir_all(&dir).await.unwrap();
    tokio::fs::write(
        dir.join("plugin.json"),
        serde_json::json!({
            "id": plugin_id,
            "name": "Event Plugin",
            "version": version,
            "provides": {
                "services": [{
                    "id": "audit-service",
                    "enabled": true,
                    "command": "${platform_bin}"
                }],
                "event_sinks": [{
                    "id": "shared-sink",
                    "service_id": "audit-service",
                    "protocol": {
                        "name": TOOL_EVENT_PROTOCOL_NAME,
                        "version": TOOL_EVENT_V1_SCHEMA_VERSION
                    },
                    "subscriptions": [{"id": FILE_CHANGED_SUBSCRIPTION_ID_V1}],
                    "requested_permissions": ["metadata"]
                }]
            }
        })
        .to_string(),
    )
    .await
    .unwrap();
    tokio::fs::write(dir.join("MARKER"), marker).await.unwrap();
    dir
}

async fn body_json(response: actix_web::dev::ServiceResponse) -> serde_json::Value {
    let bytes = test::read_body(response).await;
    serde_json::from_slice(&bytes).expect("valid json body")
}

fn error_message(body: &serde_json::Value) -> &str {
    assert_eq!(body["error"]["type"], "api_error");
    body["error"]["message"]
        .as_str()
        .expect("canonical error.message string")
}

// ---------------------------------------------------------------------
// install -> 201, list shows it, second install -> 409 AlreadyInstalled,
// delete -> gone.
// ---------------------------------------------------------------------

#[actix_web::test]
async fn install_list_reinstall_conflict_then_delete() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    // POST /install -> 201 with the InstalledPluginView.
    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(local_dir_source(&hello_plugin_example_dir()))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let view = body_json(resp).await;
    assert_eq!(view["id"], "hello-plugin");
    assert_eq!(view["name"], "Hello Plugin");
    assert_eq!(view["version"], "0.1.0");
    assert_eq!(view["status"], "installed");
    assert_eq!(view["source"]["type"], "local_dir");
    assert_eq!(
        view["registered"]["skill_dirs"],
        serde_json::json!(["hello-world"])
    );
    assert_eq!(
        view["registered"]["preset_ids"],
        serde_json::json!(["hello_plugin_greeter"])
    );

    // GET /plugins -> shows it.
    let req = test::TestRequest::get().uri("/api/v1/plugins").to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let listed = body_json(resp).await;
    let plugins = listed["plugins"].as_array().expect("plugins array");
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0]["id"], "hello-plugin");
    assert_eq!(plugins[0]["name"], "Hello Plugin");

    // POST /install again (same id) -> 409 AlreadyInstalled.
    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(local_dir_source(&hello_plugin_example_dir()))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let error = body_json(resp).await;
    assert!(
        error_message(&error).contains("already installed"),
        "error message should mention already installed: {error}"
    );

    // DELETE -> gone.
    let req = test::TestRequest::delete()
        .uri("/api/v1/plugins/hello-plugin")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let deleted = body_json(resp).await;
    assert_eq!(deleted["id"], "hello-plugin");
    assert_eq!(deleted["removed"], true);

    let req = test::TestRequest::get().uri("/api/v1/plugins").to_request();
    let resp = test::call_service(&app, req).await;
    let listed = body_json(resp).await;
    assert!(listed["plugins"].as_array().unwrap().is_empty());

    // The real checked-in example fixture must be untouched.
    assert!(hello_plugin_example_dir().join("plugin.json").exists());
}

// ---------------------------------------------------------------------
// A bad manifest (fails PluginManifest::validate) -> 400.
// ---------------------------------------------------------------------

#[actix_web::test]
async fn install_with_invalid_manifest_returns_400() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let plugin_source_dir = write_bad_manifest_plugin_dir(data_dir.path()).await;

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(local_dir_source(&plugin_source_dir))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let error = body_json(resp).await;
    assert!(
        error_message(&error).contains("invalid"),
        "error message should mention the manifest is invalid: {error}"
    );
}

// ---------------------------------------------------------------------
// A declared mcp server id colliding with a NON-plugin ("foreign") entry
// already in config.json -> 409 Conflict, and the user's entry is untouched.
// ---------------------------------------------------------------------

#[actix_web::test]
async fn install_with_foreign_mcp_conflict_returns_409() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;

    // Seed a user's own mcp server "shared-tool" directly into config.json,
    // as if added by hand via the MCP settings UI (not by any plugin).
    let user_entry = McpServerManifestEntry {
        id: "shared-tool".to_string(),
        name: None,
        enabled: false,
        transport: McpTransportManifest::Stdio {
            command: "/usr/bin/true".to_string(),
            args: vec![],
            cwd: None,
            env: Default::default(),
        },
        allowed_tools: vec![],
        denied_tools: vec![],
    };
    let user_server = user_entry
        .resolve(
            Path::new("/tmp"),
            "not-a-plugin",
            Platform::current().unwrap_or(Platform::Linux),
        )
        .expect("resolve a user mcp server config");
    state
        .update_config(
            move |cfg| {
                cfg.mcp.servers.push(user_server.clone());
                Ok(())
            },
            Default::default(),
        )
        .await
        .expect("seed user mcp server");

    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let plugin_source_dir =
        write_mcp_plugin_dir(data_dir.path(), "conflicting-plugin", "shared-tool").await;

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(local_dir_source(&plugin_source_dir))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let error = body_json(resp).await;
    let message = error_message(&error);
    assert!(message.contains("mcp server"), "{message}");
    assert!(message.contains("shared-tool"), "{message}");

    // Never installed (no provenance row).
    let req = test::TestRequest::get().uri("/api/v1/plugins").to_request();
    let resp = test::call_service(&app, req).await;
    let listed = body_json(resp).await;
    assert!(listed["plugins"].as_array().unwrap().is_empty());

    // The user's entry is untouched.
    let config = state.config.read().await;
    let servers: Vec<_> = config
        .mcp
        .servers
        .iter()
        .filter(|s| s.id == "shared-tool")
        .collect();
    assert_eq!(servers.len(), 1);
    assert!(!servers[0].enabled);
}

// ---------------------------------------------------------------------
// update: same body shape as install, Upgrade disposition, 200.
// ---------------------------------------------------------------------

#[actix_web::test]
async fn update_upgrades_an_installed_plugin() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(local_dir_source(&hello_plugin_example_dir()))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/hello-plugin/update")
        .set_json(local_dir_source(&hello_plugin_example_dir()))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let view = body_json(resp).await;
    assert_eq!(view["id"], "hello-plugin");
    assert_eq!(view["status"], "installed");
}

#[actix_web::test]
async fn update_event_sink_conflict_is_rejected_before_service_stop_or_bundle_swap() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    state.wait_for_boot_reconcile_services().await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let old_source = write_event_sink_plugin_dir(
        data_dir.path(),
        "old-source",
        "event-plugin",
        "1.0.0",
        "old-bundle",
    )
    .await;
    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(local_dir_source(&old_source))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    assert!(state.service_manager.is_running("audit-service"));

    // Corrupt provenance to model a foreign row also claiming the current
    // plugin's sink id. The current row still owns it too: foreign must win.
    let installed_json = data_dir.path().join("plugins").join("installed.json");
    let mut store = InstalledPlugins::load(&installed_json).await.unwrap();
    store.add(InstalledPlugin {
        id: "foreign-plugin".to_string(),
        version: "1.0.0".to_string(),
        source: PluginSource::LocalDir {
            path: PathBuf::from("/tmp/foreign-plugin"),
        },
        plugin_dir: PathBuf::from("/tmp/foreign-plugin"),
        installed_at: Utc::now(),
        status: PluginInstallStatus::Installed,
        registered: RegisteredCapabilities {
            event_sink_ids: vec!["shared-sink".to_string()],
            ..Default::default()
        },
    });
    store.save(&installed_json).await.unwrap();
    let provenance_before = store.plugins.clone();

    let new_source = write_event_sink_plugin_dir(
        data_dir.path(),
        "new-source",
        "event-plugin",
        "2.0.0",
        "new-bundle",
    )
    .await;
    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/event-plugin/update")
        .set_json(local_dir_source(&new_source))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let error = body_json(resp).await;
    assert!(error_message(&error).contains("shared-sink"));

    assert!(
        state.service_manager.is_running("audit-service"),
        "preflight rejection must not stop the old service"
    );
    let live_dir = data_dir.path().join("plugins").join("event-plugin");
    assert_eq!(
        tokio::fs::read_to_string(live_dir.join("MARKER"))
            .await
            .unwrap(),
        "old-bundle",
        "preflight rejection must not activate the candidate bundle"
    );
    let live_manifest: serde_json::Value = serde_json::from_str(
        &tokio::fs::read_to_string(live_dir.join("plugin.json"))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(live_manifest["version"], "1.0.0");
    assert_eq!(
        InstalledPlugins::load(&installed_json)
            .await
            .unwrap()
            .plugins,
        provenance_before
    );

    let mut entries = tokio::fs::read_dir(data_dir.path().join("plugins"))
        .await
        .unwrap();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        let name = entry.file_name().to_string_lossy().into_owned();
        assert!(!name.starts_with(".staging-"), "leftover {name}");
        assert!(!name.starts_with(".backup-"), "leftover {name}");
    }
}

#[actix_web::test]
async fn update_with_mismatched_path_id_returns_400_and_rolls_back() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    // Nothing installed yet under "some-other-id" -- the source's manifest id
    // ("hello-plugin") will never match the URL's path id.
    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/some-other-id/update")
        .set_json(local_dir_source(&hello_plugin_example_dir()))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let error = body_json(resp).await;
    let message = error_message(&error);
    assert!(message.contains("some-other-id"), "{message}");
    assert!(message.contains("hello-plugin"), "{message}");

    // Nothing was left behind under either id.
    let req = test::TestRequest::get().uri("/api/v1/plugins").to_request();
    let resp = test::call_service(&app, req).await;
    let listed = body_json(resp).await;
    assert!(listed["plugins"].as_array().unwrap().is_empty());
}

// ---------------------------------------------------------------------
// DELETE of an unknown id -> 404.
// ---------------------------------------------------------------------

#[actix_web::test]
async fn delete_unknown_id_returns_404() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let req = test::TestRequest::delete()
        .uri("/api/v1/plugins/does-not-exist")
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------
// URL source: secure-by-default checksum policy, exercised end to end
// through the real HTTP handlers (unit coverage of the same policy at the
// `plugin_source::fetch_manifest_bundle` level lives in
// `plugin_source::tests`).
// ---------------------------------------------------------------------

/// A minimal, `validate()`-passing manifest with NO declared capabilities —
/// unlike `plugin_source::tests`' `hello_manifest_json` fixture (which
/// declares a `hello-world` skill), these tests drive the real
/// `ServerPluginInstaller::install()` end to end (not just staging), and a
/// bare `plugin.json` fetched from a URL has no bundled `skills/` directory
/// alongside it — declaring a skill here would fail registration with "no
/// SKILL.md", unrelated to the checksum behavior under test.
fn hello_manifest_json(id: &str) -> String {
    serde_json::json!({
        "id": id,
        "name": "Hello",
        "version": "0.1.0",
    })
    .to_string()
}

fn sha256_hex_of(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Checksum-layer-only test helper (see the module docs on the three trust
/// layers): bypasses the host-allowlist + signature layers
/// (`allow_untrusted_host: true, allow_unsigned: true`), since every
/// `wiremock` server in this file is plain `http://127.0.0.1:<port>` (never
/// `https`, never in `plugin_trust.trusted_hosts`) and never mounts a `.sig`
/// route. The host-allowlist layer gets its own dedicated test below using
/// `url_source_full` (which does NOT bypass it).
fn url_source(url: &str, sha256: Option<&str>, allow_unverified: bool) -> serde_json::Value {
    url_source_full(url, sha256, allow_unverified, true, true)
}

fn url_source_full(
    url: &str,
    sha256: Option<&str>,
    allow_unverified: bool,
    allow_untrusted_host: bool,
    allow_unsigned: bool,
) -> serde_json::Value {
    let mut source = serde_json::json!({ "type": "url", "url": url });
    if let Some(sha) = sha256 {
        source["sha256"] = serde_json::Value::String(sha.to_string());
    }
    if allow_unverified {
        source["allow_unverified"] = serde_json::Value::Bool(true);
    }
    if allow_untrusted_host {
        source["allow_untrusted_host"] = serde_json::Value::Bool(true);
    }
    if allow_unsigned {
        source["allow_unsigned"] = serde_json::Value::Bool(true);
    }
    serde_json::json!({ "source": source })
}

/// Source-TRUST layer 1 (host allowlist): `POST /install` with a `url` source
/// whose host is not in `plugin_trust.trusted_hosts` (the default is
/// `["github.com/bigduu/"]`; a local `wiremock` server never matches) must be
/// refused with an actionable 403 — BEFORE the URL is ever fetched. Uses a
/// `wiremock` server with no mounted responder at all, so a bug that fetched
/// before refusing would surface as an unmatched-request panic instead of
/// quietly passing.
#[actix_web::test]
async fn install_url_with_untrusted_host_returns_403_before_fetch() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let server = wiremock::MockServer::start().await;
    let url = format!("{}/plugin.json", server.uri());

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(url_source_full(&url, None, false, false, false))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    let error = body_json(resp).await;
    let message = error_message(&error);
    assert!(message.contains("trusted_hosts"), "{message}");
    assert!(
        message.contains("allow_untrusted_host") || message.contains("allow-untrusted-host"),
        "{message}"
    );

    // Nothing installed.
    let req = test::TestRequest::get().uri("/api/v1/plugins").to_request();
    let resp = test::call_service(&app, req).await;
    let listed = body_json(resp).await;
    assert!(listed["plugins"].as_array().unwrap().is_empty());

    // The refusal happened before the URL was ever fetched.
    let received = server.received_requests().await;
    assert_eq!(received.map(|r| r.len()), Some(0));
}

/// Source-TRUST layer 3 (checksum), isolated from layers 1/2 via
/// `allow_untrusted_host`/`allow_unsigned`: `POST /install` with neither
/// `sha256` nor `allow_unverified` on a genuinely unsigned bundle must still
/// be refused with an actionable 400 — the core "secure by default" behavior
/// this whole feature started with. Unlike the host-layer test above, this
/// one DOES fetch the bundle (and attempts its `.sig`) before refusing — see
/// `plugin_source.rs`'s module docs on why the checksum gate can no longer
/// run before any network access now that a valid signature can supersede it.
#[actix_web::test]
async fn install_url_with_no_checksum_or_allow_unverified_returns_400_after_host_and_signature_pass(
) {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(url_source(&url, None, false))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let error = body_json(resp).await;
    let message = error_message(&error);
    assert!(message.contains("sha256"), "{message}");
    assert!(message.contains("allow_unverified"), "{message}");

    // Nothing installed.
    let req = test::TestRequest::get().uri("/api/v1/plugins").to_request();
    let resp = test::call_service(&app, req).await;
    let listed = body_json(resp).await;
    assert!(listed["plugins"].as_array().unwrap().is_empty());
}

#[actix_web::test]
async fn install_url_with_wrong_bundle_sha256_returns_400_and_installs_nothing() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());
    let wrong_sha256 = "b".repeat(64);

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(url_source(&url, Some(&wrong_sha256), false))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let error = body_json(resp).await;
    assert!(error_message(&error).contains("mismatch"), "{error}");

    let req = test::TestRequest::get().uri("/api/v1/plugins").to_request();
    let resp = test::call_service(&app, req).await;
    let listed = body_json(resp).await;
    assert!(listed["plugins"].as_array().unwrap().is_empty());
}

#[actix_web::test]
async fn install_url_with_correct_bundle_sha256_succeeds() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    let bundle_sha256 = sha256_hex_of(manifest_body.as_bytes());
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(url_source(&url, Some(&bundle_sha256), false))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let view = body_json(resp).await;
    assert_eq!(view["id"], "hello-plugin");
    assert_eq!(view["source"]["type"], "url");
    assert_eq!(view["source"]["sha256"], bundle_sha256);
}

#[actix_web::test]
async fn install_url_with_allow_unverified_and_no_sha256_succeeds() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(url_source(&url, None, true))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let view = body_json(resp).await;
    assert_eq!(view["id"], "hello-plugin");
    assert!(view["source"]["sha256"].is_null());
}

// ---------------------------------------------------------------------
// `insecure` — the SourceSpec aggregate — and `plugin_trust.enforcement`
// (the config-level form), exercised end to end through the real HTTP
// handlers. Unit coverage of the same aggregate at the
// `plugin_source::fetch_manifest_bundle` level lives in
// `plugin_source::tests`.
// ---------------------------------------------------------------------

/// `SourceSpec` with `"insecure": true` and every individual `allow_*`
/// omitted — proves the aggregate ALONE (not any per-layer flag) is what
/// lets an untrusted-host, unsigned, unchecksummed install through.
fn url_source_insecure(url: &str) -> serde_json::Value {
    serde_json::json!({ "source": { "type": "url", "url": url, "insecure": true } })
}

#[actix_web::test]
async fn install_url_with_insecure_true_bypasses_all_three_layers() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    // A plain wiremock server: never in `plugin_trust.trusted_hosts`, never
    // https, no `.sig` mounted, no `sha256` given — every one of the three
    // layers would refuse this under Strict with no flags.
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(url_source_insecure(&url))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let view = body_json(resp).await;
    assert_eq!(view["id"], "hello-plugin");
    assert_eq!(view["source"]["type"], "url");
    assert!(view["source"]["sha256"].is_null());
    // Provenance records the aggregate.
    assert_eq!(view["source"]["insecure"], true);
}

#[actix_web::test]
async fn install_url_with_insecure_true_still_refuses_a_wrong_sha256() {
    // Precedence: `insecure: true` must not downgrade a caller-supplied
    // checksum — a WRONG sha256 alongside it still refuses the install.
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());
    let wrong_sha256 = "d".repeat(64);

    let mut source = url_source_insecure(&url);
    source["source"]["sha256"] = serde_json::Value::String(wrong_sha256);

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(source)
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let error = body_json(resp).await;
    assert!(error_message(&error).contains("mismatch"), "{error}");

    let req = test::TestRequest::get().uri("/api/v1/plugins").to_request();
    let resp = test::call_service(&app, req).await;
    let listed = body_json(resp).await;
    assert!(listed["plugins"].as_array().unwrap().is_empty());
}

#[actix_web::test]
async fn install_url_with_plugin_trust_enforcement_off_needs_no_per_request_flags() {
    // The PERSISTENT, config-level form: flip `plugin_trust.enforcement` to
    // `off` on this server's config, then install with a BARE `url` source —
    // no `insecure`, no individual `allow_*` flags at all.
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    state
        .update_config(
            |cfg| {
                cfg.plugin_trust.enforcement = bamboo_config::PluginTrustEnforcement::Off;
                Ok(())
            },
            Default::default(),
        )
        .await
        .expect("set plugin_trust.enforcement = off");
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(serde_json::json!({ "source": { "type": "url", "url": url } }))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let view = body_json(resp).await;
    assert_eq!(view["id"], "hello-plugin");
    assert_eq!(view["source"]["insecure"], true);
}

// ---------------------------------------------------------------------
// Status surface (issue #479): `InstalledPluginView.service_status` is
// populated from the live `ServiceManager`, keyed off
// `registered.service_ids`.
// ---------------------------------------------------------------------

fn service_plugin_manifest_json(id: &str) -> String {
    serde_json::json!({
        "id": id,
        "name": "Service Plugin",
        "version": "1.0.0",
        "provides": {
            "services": [{"id": "svc", "command": "${platform_bin}"}]
        }
    })
    .to_string()
}

async fn write_service_plugin_dir(dir: &Path, id: &str) {
    tokio::fs::create_dir_all(dir).await.unwrap();
    tokio::fs::write(dir.join("plugin.json"), service_plugin_manifest_json(id))
        .await
        .unwrap();
}

#[actix_web::test]
async fn install_and_list_surface_service_status() {
    let data_dir = tempfile::tempdir().unwrap();
    let state = test_state(data_dir.path()).await;
    let app = test::init_service(plugin_test_app!(state.clone())).await;

    let source_dir = data_dir.path().join("svc-plugin-source");
    write_service_plugin_dir(&source_dir, "svc-plugin").await;

    let req = test::TestRequest::post()
        .uri("/api/v1/plugins/install")
        .set_json(local_dir_source(&source_dir))
        .to_request();
    let resp = test::call_service(&app, req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let view = body_json(resp).await;
    assert_eq!(
        view["registered"]["service_ids"],
        serde_json::json!(["svc"])
    );
    let service_status = view["service_status"]
        .as_array()
        .expect("service_status array");
    assert_eq!(service_status.len(), 1);
    assert_eq!(service_status[0]["id"], "svc");
    // The binary doesn't exist on disk in this fixture, so the runtime
    // never reaches `running` — but it MUST be present (best-effort start,
    // ownership recorded regardless — matches the mcp contract) and report
    // SOME state, not be silently absent from the response.
    assert!(service_status[0]["state"].is_string());

    // GET /plugins reflects the same live status.
    let req = test::TestRequest::get().uri("/api/v1/plugins").to_request();
    let resp = test::call_service(&app, req).await;
    let listed = body_json(resp).await;
    let plugins = listed["plugins"].as_array().unwrap();
    assert_eq!(plugins.len(), 1);
    assert_eq!(
        plugins[0]["service_status"][0]["id"],
        serde_json::json!("svc")
    );
}
