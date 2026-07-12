//! Actix integration tests for `/api/v1/plugins` — install / list / update /
//! remove, and the error->status mapping end to end through the real HTTP
//! handlers (not just `plugin_error_response` in isolation — see
//! `super::errors::tests` for that). Mirrors the `App::new().app_data(...)
//! .route(...)` + `test::call_service` pattern used in
//! `handlers/agent/stream/tests.rs`.
//!
//! Every test installs from the checked-in
//! `crates/infra/bamboo-plugin/examples/hello-plugin` fixture as a
//! `local_dir` source — `stage_plugin_source` COPIES a `LocalDir` source
//! (see `plugin_source.rs::stage_into`), so the checked-in fixture is never
//! mutated — and a throwaway `tempfile::tempdir()` `AppState`, never
//! `~/.bamboo`.

use std::path::{Path, PathBuf};

use actix_web::http::StatusCode;
use actix_web::{test, web, App};
use bamboo_plugin::manifest::{McpServerManifestEntry, McpTransportManifest, Platform};

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

async fn body_json(response: actix_web::dev::ServiceResponse) -> serde_json::Value {
    let bytes = test::read_body(response).await;
    serde_json::from_slice(&bytes).expect("valid json body")
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
        error["error"]
            .as_str()
            .unwrap()
            .contains("already installed"),
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
        error["error"].as_str().unwrap().contains("invalid"),
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
    let message = error["error"].as_str().unwrap();
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
    let message = error["error"].as_str().unwrap();
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
