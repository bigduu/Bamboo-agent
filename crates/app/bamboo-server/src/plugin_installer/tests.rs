use std::path::{Path, PathBuf};
use std::sync::Arc;

use actix_web::web;
use bamboo_plugin::{
    InstallDisposition, InstalledPlugin, InstalledPlugins, McpServerManifestEntry,
    McpTransportManifest, ObservationPermissionId, Platform, PluginError, PluginInstallStatus,
    PluginInstaller, PluginManifest, PluginSource, RegisteredCapabilities,
};
use bamboo_plugin_protocol::{
    FILE_CHANGED_SUBSCRIPTION_ID_V1, TOOL_EVENT_PROTOCOL_NAME, TOOL_EVENT_V1_SCHEMA_VERSION,
};
use chrono::Utc;

use super::{boot_reconcile_services, ServerPluginInstaller, PLUGIN_OP_LOCK};
use crate::app_state::AppState;
use crate::tool_event_policy::canonicalize_persisted_event_sink_grants;
use crate::tool_event_router::ToolEventSinkState;

/// A never-resolves stdio command: `Command::spawn` fails immediately (ENOENT)
/// so `mcp_manager.start_server` returns a fast `Err` instead of hanging on a
/// handshake timeout — exactly the "best-effort start, config write still
/// counts as registered" path these tests want to exercise quickly.
const NONEXISTENT_COMMAND: &str = "/nonexistent/bamboo-test-mcp-binary-does-not-exist";

async fn new_installer(data_dir: &Path) -> (web::Data<AppState>, ServerPluginInstaller) {
    let state = AppState::new(data_dir.to_path_buf())
        .await
        .expect("app state should initialize");
    // `AppState::new` fires the boot-time service reconcile pass
    // (`plugin_installer::boot_reconcile_services`) in the background. It now
    // shares `PLUGIN_OP_LOCK` with installer mutations, preventing a stale
    // service/sink plan from racing a newer plugin generation. Tests still
    // drain the one-shot pass here so each fixture starts from deterministic
    // completed boot state rather than depending on lock-waiter scheduling.
    state.wait_for_boot_reconcile_services().await;
    let data = web::Data::new(state);
    let installer = ServerPluginInstaller::new(data.clone());
    (data, installer)
}

fn mcp_manifest_json(id: &str, version: &str, mcp_ids: &[&str]) -> String {
    let servers: Vec<serde_json::Value> = mcp_ids
        .iter()
        .map(|mcp_id| {
            serde_json::json!({
                "id": mcp_id,
                "transport": {"type": "stdio", "command": NONEXISTENT_COMMAND}
            })
        })
        .collect();
    serde_json::json!({
        "id": id,
        "name": "Test Plugin",
        "version": version,
        "provides": {
            "mcp_servers": servers,
        }
    })
    .to_string()
}

fn restartable_mcp_manifest_json(id: &str, version: &str, python: &str) -> String {
    serde_json::json!({
        "id": id,
        "name": "Restartable MCP Plugin",
        "version": version,
        "provides": {
            "mcp_servers": [{
                "id": "restartable-mcp",
                "transport": {
                    "type": "stdio",
                    "command": python,
                    "args": [
                        "${plugin_dir}/mcp-fixture.py",
                        "${plugin_dir}/starts.log",
                        "${plugin_dir}/generation.txt"
                    ]
                }
            }]
        }
    })
    .to_string()
}

/// `command: "${platform_bin}"` resolves to `<plugin_dir>/bin/<platform>/<id>`,
/// which none of these tests ever create on disk — `ServiceManager::start_service`
/// therefore fails fast (`ENOENT`) exactly like `NONEXISTENT_COMMAND` does for
/// MCP above, exercising the same "best-effort start, ownership still
/// recorded" contract without spawning a real long-running process.
fn service_manifest_json(id: &str, version: &str, service_ids: &[&str]) -> String {
    let services: Vec<serde_json::Value> = service_ids
        .iter()
        .map(|service_id| {
            serde_json::json!({
                "id": service_id,
                "command": "${platform_bin}"
            })
        })
        .collect();
    serde_json::json!({
        "id": id,
        "name": "Test Service Plugin",
        "version": version,
        "provides": {
            "services": services,
        }
    })
    .to_string()
}

fn event_sink_manifest_json(
    id: &str,
    version: &str,
    service_id: &str,
    sink_versions: &[(&str, u16)],
) -> String {
    let sinks: Vec<serde_json::Value> = sink_versions
        .iter()
        .map(|(sink_id, protocol_version)| {
            serde_json::json!({
                "id": sink_id,
                "service_id": service_id,
                "protocol": {
                    "name": TOOL_EVENT_PROTOCOL_NAME,
                    "version": protocol_version
                },
                "subscriptions": [{"id": FILE_CHANGED_SUBSCRIPTION_ID_V1}],
                "requested_permissions": ["metadata"]
            })
        })
        .collect();
    serde_json::json!({
        "id": id,
        "name": "Test Event Sink Plugin",
        "version": version,
        "provides": {
            // Disabled is a valid #479 declaration. It avoids process spawn in
            // these provenance-only #903 tests and must not erase sink ownership.
            "services": [{
                "id": service_id,
                "enabled": false,
                "command": "${platform_bin}",
                "input_protocol": "ndjson_v1"
            }],
            "event_sinks": sinks
        }
    })
    .to_string()
}

fn hello_plugin_example_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../infra/bamboo-plugin/examples/hello-plugin")
}

/// Copies the real `crates/infra/bamboo-plugin/examples/hello-plugin` fixture
/// into `dest` (a tempdir), so `uninstall()`'s `remove_dir_all(plugin_dir)`
/// never touches the checked-in example.
async fn copy_hello_plugin_fixture(dest: &Path) -> PluginManifest {
    let source = hello_plugin_example_dir();
    let manifest_raw = tokio::fs::read_to_string(source.join("plugin.json"))
        .await
        .expect("read example plugin.json");
    let skill_raw =
        tokio::fs::read_to_string(source.join("skills").join("hello-world").join("SKILL.md"))
            .await
            .expect("read example SKILL.md");

    tokio::fs::create_dir_all(dest.join("skills").join("hello-world"))
        .await
        .unwrap();
    tokio::fs::write(dest.join("plugin.json"), &manifest_raw)
        .await
        .unwrap();
    tokio::fs::write(
        dest.join("skills").join("hello-world").join("SKILL.md"),
        &skill_raw,
    )
    .await
    .unwrap();

    PluginManifest::parse_str(&manifest_raw).expect("parse example manifest")
}

// ---------------------------------------------------------------------
// End-to-end: install the hello-plugin example, then uninstall it.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_registers_skill_and_prompt_and_records_provenance() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    let plugin_dir = root.path().join("plugins").join("hello-plugin");
    let manifest = copy_hello_plugin_fixture(&plugin_dir).await;

    let entry = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("install hello-plugin");

    assert_eq!(entry.id, "hello-plugin");
    assert_eq!(entry.registered.skill_dirs, vec!["hello-world".to_string()]);
    assert_eq!(
        entry.registered.preset_ids,
        vec!["hello_plugin_greeter".to_string()]
    );
    assert!(entry.registered.mcp_server_ids.is_empty());
    assert!(entry.registered.workflow_filenames.is_empty());

    // prompt-presets.json actually has the preset.
    let presets_path = state.app_data_dir.join("prompt-presets.json");
    let presets_raw = tokio::fs::read_to_string(&presets_path).await.unwrap();
    assert!(presets_raw.contains("hello_plugin_greeter"));
    assert!(presets_raw.contains("Hello Plugin Greeter"));

    // installed.json has the provenance entry.
    let installed_raw =
        tokio::fs::read_to_string(state.app_data_dir.join("plugins").join("installed.json"))
            .await
            .unwrap();
    assert!(installed_raw.contains("\"hello-plugin\""));

    // Skill file is discoverable in place (no copy into a shared skills dir).
    assert!(plugin_dir
        .join("skills")
        .join("hello-world")
        .join("SKILL.md")
        .exists());

    // list() surfaces it too.
    let listed = installer.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "hello-plugin");

    // --- Now uninstall and assert everything is gone. ---
    installer
        .uninstall("hello-plugin")
        .await
        .expect("uninstall");

    let presets_raw_after = tokio::fs::read_to_string(&presets_path).await.unwrap();
    assert!(!presets_raw_after.contains("hello_plugin_greeter"));

    let installed_after = installer.list().await.unwrap();
    assert!(installed_after.is_empty());

    assert!(
        !plugin_dir.exists(),
        "uninstall should remove the plugin's own directory"
    );

    // The real checked-in example fixture must be untouched.
    assert!(hello_plugin_example_dir().join("plugin.json").exists());
}

#[tokio::test]
async fn uninstall_unknown_id_is_not_found() {
    let root = tempfile::tempdir().unwrap();
    let (_state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    let error = installer
        .uninstall("does-not-exist")
        .await
        .expect_err("unknown id should be not-found");
    assert!(matches!(error, PluginError::NotFound(_)));
}

#[tokio::test]
async fn uninstall_duplicate_plugin_rows_fails_before_any_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let installed_json = state.app_data_dir.join("plugins").join("installed.json");
    let mut store = InstalledPlugins::default();
    for index in 0..2 {
        let plugin_dir = state
            .app_data_dir
            .join("plugins")
            .join(format!("duplicate-dir-{index}"));
        tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
        tokio::fs::write(plugin_dir.join("MARKER"), format!("row-{index}"))
            .await
            .unwrap();
        store.plugins.push(InstalledPlugin {
            id: "duplicate-plugin".to_string(),
            version: format!("1.0.{index}"),
            source: PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            plugin_dir,
            installed_at: Utc::now(),
            status: PluginInstallStatus::Installed,
            registered: RegisteredCapabilities {
                service_ids: vec![format!("service-{index}")],
                event_sink_ids: vec![format!("sink-{index}")],
                ..Default::default()
            },
        });
    }
    store.save(&installed_json).await.unwrap();
    let before = store.plugins.clone();

    let error = installer
        .uninstall("duplicate-plugin")
        .await
        .expect_err("ambiguous plugin identity must fail closed");
    assert!(matches!(error, PluginError::Registration(_)));
    assert_eq!(
        InstalledPlugins::load(&installed_json)
            .await
            .unwrap()
            .plugins,
        before
    );
    for entry in before {
        assert!(entry.plugin_dir.join("MARKER").exists());
    }
}

#[tokio::test]
async fn second_install_under_fail_if_installed_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let (_state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    let plugin_dir = root.path().join("plugins").join("hello-plugin");
    let manifest = copy_hello_plugin_fixture(&plugin_dir).await;

    installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("first install succeeds");

    let error = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect_err("second install under FailIfInstalled should be rejected");
    assert!(matches!(error, PluginError::AlreadyInstalled(_)));
}

// ---------------------------------------------------------------------
// Ownership pre-check: a foreign (non-plugin) mcp server entry is never
// clobbered, and the whole install is refused.
// ---------------------------------------------------------------------

#[tokio::test]
async fn foreign_mcp_conflict_refuses_install_and_does_not_touch_the_users_entry() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    // Seed a user's own mcp server, "shared-tool", directly into config.json
    // (as if the user had added it via the MCP settings UI). Built via the
    // same `McpServerManifestEntry::resolve` the installer itself uses, just
    // to get a structurally-valid `McpServerConfig` without hand-rolling
    // every serde field.
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

    let plugin_dir = root.path().join("plugins").join("conflicting-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    let manifest_json = mcp_manifest_json("conflicting-plugin", "1.0.0", &["shared-tool"]);
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let error = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect_err("a foreign mcp id collision must refuse the install");
    assert!(matches!(
        error,
        PluginError::Conflict {
            kind: "mcp server",
            ..
        }
    ));

    // The user's entry must be untouched (same id, still present, still
    // disabled — a clobber would have flipped `enabled`/replaced the config).
    let config = state.config.read().await;
    let servers: Vec<_> = config
        .mcp
        .servers
        .iter()
        .filter(|s| s.id == "shared-tool")
        .collect();
    assert_eq!(
        servers.len(),
        1,
        "exactly the user's original entry, no duplicate"
    );
    assert!(!servers[0].enabled, "the user's entry must be unmodified");
    drop(config);

    // The install must not have been recorded as provenance either.
    let listed = installer.list().await.unwrap();
    assert!(listed.is_empty());
}

// ---------------------------------------------------------------------
// Upgrade drop-diff: installing v1 with 2 mcp servers, then "upgrading" to a
// v2 that only declares 1, must de-register the dropped one.
// ---------------------------------------------------------------------

#[tokio::test]
async fn upgrade_deregisters_mcp_server_dropped_by_the_new_version() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    let plugin_dir = root.path().join("plugins").join("multi-mcp-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();

    let v1_json = mcp_manifest_json("multi-mcp-plugin", "1.0.0", &["alpha", "beta"]);
    tokio::fs::write(plugin_dir.join("plugin.json"), &v1_json)
        .await
        .unwrap();
    let v1_manifest = PluginManifest::parse_str(&v1_json).unwrap();

    let v1_entry = installer
        .install(
            &v1_manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("install v1");
    assert_eq!(
        v1_entry.registered.mcp_server_ids,
        vec!["alpha".to_string(), "beta".to_string()]
    );
    {
        let config = state.config.read().await;
        let ids: Vec<&str> = config.mcp.servers.iter().map(|s| s.id.as_str()).collect();
        assert!(ids.contains(&"alpha"));
        assert!(ids.contains(&"beta"));
    }

    // "Upgrade" to v2, which only declares alpha.
    let v2_json = mcp_manifest_json("multi-mcp-plugin", "2.0.0", &["alpha"]);
    tokio::fs::write(plugin_dir.join("plugin.json"), &v2_json)
        .await
        .unwrap();
    let v2_manifest = PluginManifest::parse_str(&v2_json).unwrap();

    let v2_entry = installer
        .install(
            &v2_manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::Upgrade,
            Utc::now(),
        )
        .await
        .expect("upgrade to v2");

    assert_eq!(v2_entry.version, "2.0.0");
    assert_eq!(
        v2_entry.registered.mcp_server_ids,
        vec!["alpha".to_string()]
    );

    let config = state.config.read().await;
    let ids: Vec<&str> = config.mcp.servers.iter().map(|s| s.id.as_str()).collect();
    assert!(ids.contains(&"alpha"), "alpha must still be registered");
    assert!(
        !ids.contains(&"beta"),
        "beta was dropped by v2 and must have been de-registered"
    );
    drop(config);

    // Provenance reflects only the v2 (upgraded) entry.
    let listed = installer.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].version, "2.0.0");
    assert_eq!(
        listed[0].registered.mcp_server_ids,
        vec!["alpha".to_string()]
    );
}

#[tokio::test]
async fn upgrade_restarts_owned_mcp_when_effective_config_is_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let python = ["python3", "python"]
        .into_iter()
        .find(|command| {
            std::process::Command::new(command)
                .arg("--version")
                .output()
                .is_ok_and(|output| output.status.success())
        })
        .expect("a Python interpreter is required for the MCP restart fixture");
    let plugin_dir = root.path().join("plugins").join("restartable-mcp-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    tokio::fs::write(
        plugin_dir.join("mcp-fixture.py"),
        r#"import json
import sys

generation = open(sys.argv[2], encoding="utf-8").read().strip()
with open(sys.argv[1], "a", encoding="utf-8") as marker:
    marker.write(generation + "\n")

for line in sys.stdin:
    request = json.loads(line)
    request_id = request.get("id")
    if request_id is None:
        continue
    if request.get("method") == "server/discover":
        print(json.dumps({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": "Method not found"},
        }), flush=True)
        continue
    if request.get("method") == "initialize":
        result = {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "plugin-restart-fixture", "version": generation},
        }
    elif request.get("method") == "tools/list":
        result = {"tools": []}
    else:
        result = {}
    print(json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result}), flush=True)
"#,
    )
    .await
    .unwrap();
    tokio::fs::write(plugin_dir.join("generation.txt"), "v1")
        .await
        .unwrap();

    let v1_json = restartable_mcp_manifest_json("restartable-mcp-plugin", "1.0.0", python);
    tokio::fs::write(plugin_dir.join("plugin.json"), &v1_json)
        .await
        .unwrap();
    let v1_manifest = PluginManifest::parse_str(&v1_json).unwrap();
    installer
        .install(
            &v1_manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("install v1 and start its MCP runtime");
    assert_eq!(
        tokio::fs::read_to_string(plugin_dir.join("starts.log"))
            .await
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        vec!["v1"]
    );

    tokio::fs::write(plugin_dir.join("generation.txt"), "v2")
        .await
        .unwrap();
    let v2_json = restartable_mcp_manifest_json("restartable-mcp-plugin", "2.0.0", python);
    tokio::fs::write(plugin_dir.join("plugin.json"), &v2_json)
        .await
        .unwrap();
    let v2_manifest = PluginManifest::parse_str(&v2_json).unwrap();
    installer
        .install(
            &v2_manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::Upgrade,
            Utc::now(),
        )
        .await
        .expect("upgrade must replace the same-config MCP runtime");

    assert_eq!(
        tokio::fs::read_to_string(plugin_dir.join("starts.log"))
            .await
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        vec!["v1", "v2"],
        "the upgraded bundle must be loaded by a fresh MCP process"
    );
    assert!(state
        .mcp_manager
        .get_server_info("restartable-mcp")
        .is_some());
    state.mcp_manager.shutdown_all().await;
}

// ---------------------------------------------------------------------
// Legacy plugin workflows: validate and discover in place, never copy.
// ---------------------------------------------------------------------

#[tokio::test]
async fn plugin_workflow_stays_in_place_and_does_not_conflict_with_user_source() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    // Seed a user's own workflow file directly in workflows_dir.
    let workflows_dir = state.app_data_dir.join("workflows");
    tokio::fs::create_dir_all(&workflows_dir).await.unwrap();
    tokio::fs::write(workflows_dir.join("daily-report.md"), "# my own workflow\n")
        .await
        .unwrap();

    let plugin_dir = root.path().join("plugins").join("workflow-plugin");
    tokio::fs::create_dir_all(plugin_dir.join("workflows"))
        .await
        .unwrap();
    tokio::fs::write(
        plugin_dir.join("workflows").join("daily-report.md"),
        "# plugin's workflow\n",
    )
    .await
    .unwrap();
    let manifest_json = serde_json::json!({
        "id": "workflow-plugin",
        "name": "Workflow Plugin",
        "version": "1.0.0",
        "provides": {
            "workflows": ["daily-report.md"],
        }
    })
    .to_string();
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let entry = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("plugin workflows are isolated in place");
    assert!(entry.registered.workflow_filenames.is_empty());

    // The user's workflow content must be untouched.
    let content = tokio::fs::read_to_string(workflows_dir.join("daily-report.md"))
        .await
        .unwrap();
    assert_eq!(content, "# my own workflow\n");
    let plugin_content =
        tokio::fs::read_to_string(plugin_dir.join("workflows").join("daily-report.md"))
            .await
            .unwrap();
    assert_eq!(plugin_content, "# plugin's workflow\n");
}

/// A manifest can declare 2+ workflows where the second fails
/// `bamboo_config::paths::is_safe_workflow_name`'s stricter charset check
/// (bamboo-plugin's own manifest validation is looser — it only rejects path
/// separators/`..`/control chars, not e.g. `!`). Validation must fail without
/// copying either file into the user's global legacy-workflow directory.
#[tokio::test]
async fn unsafe_plugin_workflow_name_is_rejected_without_global_writes() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    let plugin_dir = root.path().join("plugins").join("multi-workflow-plugin");
    tokio::fs::create_dir_all(plugin_dir.join("workflows"))
        .await
        .unwrap();
    tokio::fs::write(plugin_dir.join("workflows").join("good-one.md"), "# good\n")
        .await
        .unwrap();
    tokio::fs::write(plugin_dir.join("workflows").join("bad!name.md"), "# bad\n")
        .await
        .unwrap();
    let manifest_json = serde_json::json!({
        "id": "multi-workflow-plugin",
        "name": "Multi Workflow Plugin",
        "version": "1.0.0",
        "provides": {
            "workflows": ["good-one.md", "bad!name.md"],
        }
    })
    .to_string();
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let error = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect_err("the second (unsafe-named) workflow must fail registration");
    assert!(matches!(error, PluginError::InvalidManifest(_)));

    // Neither source is moved or copied into the global legacy directory.
    let workflows_dir = state.app_data_dir.join("workflows");
    assert!(
        !workflows_dir.join("good-one.md").exists(),
        "plugin workflow must never be copied into the user's legacy directory"
    );
    assert!(!workflows_dir.join("bad!name.md").exists());
    assert!(plugin_dir.join("workflows/good-one.md").exists());
    assert!(plugin_dir.join("workflows/bad!name.md").exists());

    // And nothing was committed to provenance.
    assert!(installer.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn undeclared_plugin_workflow_is_rejected_before_publication() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let plugin_dir = root.path().join("plugins/undeclared-workflow-plugin");
    tokio::fs::create_dir_all(plugin_dir.join("workflows"))
        .await
        .unwrap();
    tokio::fs::write(plugin_dir.join("workflows/declared.md"), "Declared.\n")
        .await
        .unwrap();
    tokio::fs::write(plugin_dir.join("workflows/hidden.md"), "Undeclared.\n")
        .await
        .unwrap();
    let manifest_json = serde_json::json!({
        "id": "undeclared-workflow-plugin",
        "name": "Undeclared Workflow Plugin",
        "version": "1.0.0",
        "provides": {"workflows": ["declared.md"]}
    })
    .to_string();
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let error = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect_err("undeclared workflow must fail installation");
    assert!(matches!(error, PluginError::InvalidManifest(_)));
    assert!(!state.app_data_dir.join("workflows/declared.md").exists());
    assert!(!state.app_data_dir.join("workflows/hidden.md").exists());
    assert!(installer.list().await.unwrap().is_empty());
}

// ---------------------------------------------------------------------
// Concurrency: two installs of DIFFERENT ids run concurrently under the
// process-wide install lock; neither drops the other's provenance row or
// prompt preset (the load/modify/save lost-update races the lock closes).
// ---------------------------------------------------------------------

/// Write a plugin bundle declaring one distinct skill + one distinct prompt
/// preset, so concurrent installs each touch BOTH installed.json AND
/// prompt-presets.json (the two lost-update-prone stores).
async fn write_skill_and_preset_plugin(dir: &Path, id: &str, preset_id: &str) -> PluginManifest {
    tokio::fs::create_dir_all(dir.join("skills").join(id))
        .await
        .unwrap();
    tokio::fs::write(
        dir.join("skills").join(id).join("SKILL.md"),
        format!("---\nname: {id}\ndescription: demo\n---\nHi\n"),
    )
    .await
    .unwrap();
    let manifest_json = serde_json::json!({
        "id": id,
        "name": id,
        "version": "1.0.0",
        "provides": {
            "skills": [id],
            "prompts": [
                {"id": preset_id, "name": preset_id, "content": "hello from a preset"}
            ]
        }
    })
    .to_string();
    tokio::fs::write(dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    PluginManifest::parse_str(&manifest_json).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_installs_of_different_ids_both_persist() {
    let root = tempfile::tempdir().unwrap();
    let (_state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let installer = Arc::new(installer);

    let dir_a = root.path().join("src-a");
    let dir_b = root.path().join("src-b");
    let manifest_a = write_skill_and_preset_plugin(&dir_a, "plug-a", "preset_a").await;
    let manifest_b = write_skill_and_preset_plugin(&dir_b, "plug-b", "preset_b").await;

    let inst_a = installer.clone();
    let inst_b = installer.clone();
    let handle_a = tokio::spawn(async move {
        inst_a
            .install(
                &manifest_a,
                &dir_a,
                PluginSource::LocalDir {
                    path: dir_a.clone(),
                },
                InstallDisposition::FailIfInstalled,
                Utc::now(),
            )
            .await
    });
    let handle_b = tokio::spawn(async move {
        inst_b
            .install(
                &manifest_b,
                &dir_b,
                PluginSource::LocalDir {
                    path: dir_b.clone(),
                },
                InstallDisposition::FailIfInstalled,
                Utc::now(),
            )
            .await
    });

    handle_a.await.unwrap().expect("install plug-a");
    handle_b.await.unwrap().expect("install plug-b");

    // Neither install dropped the other's provenance row...
    let mut listed = installer.list().await.unwrap();
    listed.sort_by(|l, r| l.id.cmp(&r.id));
    let ids: Vec<&str> = listed.iter().map(|p| p.id.as_str()).collect();
    assert_eq!(
        ids,
        vec!["plug-a", "plug-b"],
        "both provenance rows present"
    );
    assert!(listed
        .iter()
        .all(|p| p.status == PluginInstallStatus::Installed));

    // ...nor the other's prompt preset (no lost update on prompt-presets.json).
    let presets_raw = tokio::fs::read_to_string(_state.app_data_dir.join("prompt-presets.json"))
        .await
        .unwrap();
    assert!(presets_raw.contains("preset_a"), "preset_a survived");
    assert!(presets_raw.contains("preset_b"), "preset_b survived");
}

// ---------------------------------------------------------------------
// Crash recovery: a prior install killed mid-flight left an `installing`
// provenance row + a leftover mcp entry in config.json. The next install of
// that id must recover cleanly (no false Conflict, ends `installed`), not
// treat the plugin's own leftover as a foreign conflict.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_recovers_from_a_crashed_installing_leftover() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    // Simulate a crashed install: config.json already has the mcp entry the
    // install had begun to register...
    let leftover_entry = McpServerManifestEntry {
        id: "leftover-mcp".to_string(),
        name: None,
        enabled: false,
        transport: McpTransportManifest::Stdio {
            command: NONEXISTENT_COMMAND.to_string(),
            args: vec![],
            cwd: None,
            env: Default::default(),
        },
        allowed_tools: vec![],
        denied_tools: vec![],
    };
    let leftover_cfg = leftover_entry
        .resolve(
            Path::new("/tmp"),
            "crashed-plugin",
            Platform::current().unwrap_or(Platform::Linux),
        )
        .unwrap();
    state
        .update_config(
            move |cfg| {
                cfg.mcp.servers.push(leftover_cfg.clone());
                Ok(())
            },
            Default::default(),
        )
        .await
        .unwrap();

    // ...and installed.json has an `installing` journal row recording that id
    // as its intended owner (this is what the pre-registration journal write
    // leaves behind on a hard kill).
    let installed_json = state.app_data_dir.join("plugins").join("installed.json");
    let plugin_dir = state.app_data_dir.join("plugins").join("crashed-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    let mut store = InstalledPlugins::default();
    store.add(InstalledPlugin {
        id: "crashed-plugin".to_string(),
        version: "1.0.0".to_string(),
        source: PluginSource::LocalDir {
            path: plugin_dir.clone(),
        },
        plugin_dir: plugin_dir.clone(),
        installed_at: Utc::now(),
        status: PluginInstallStatus::Installing,
        registered: RegisteredCapabilities {
            mcp_server_ids: vec!["leftover-mcp".to_string()],
            service_ids: vec!["leftover-event-service".to_string()],
            event_sink_ids: vec!["leftover-event-sink".to_string()],
            event_sink_grants: std::collections::BTreeMap::from([(
                "leftover-event-sink".to_string(),
                vec![
                    ObservationPermissionId::new("metadata"),
                    ObservationPermissionId::new("paths"),
                ],
            )]),
            ..Default::default()
        },
    });
    store.save(&installed_json).await.unwrap();

    // Re-run the install (plain `install` verb → FailIfInstalled). It must NOT
    // fail AlreadyInstalled (the row is `installing`, not a completed install),
    // and must NOT false-Conflict on `leftover-mcp` (recorded as the plugin's
    // own intended entry).
    let manifest_json = mcp_manifest_json("crashed-plugin", "1.0.0", &["leftover-mcp"]);
    let mut manifest_value: serde_json::Value = serde_json::from_str(&manifest_json).unwrap();
    manifest_value["provides"]["services"] = serde_json::json!([{
        "id": "leftover-event-service",
        "enabled": false,
        "command": "${platform_bin}",
        "input_protocol": "ndjson_v1"
    }]);
    manifest_value["provides"]["event_sinks"] = serde_json::json!([{
        "id": "leftover-event-sink",
        "service_id": "leftover-event-service",
        "protocol": {
            "name": TOOL_EVENT_PROTOCOL_NAME,
            "version": TOOL_EVENT_V1_SCHEMA_VERSION
        },
        "subscriptions": [{"id": FILE_CHANGED_SUBSCRIPTION_ID_V1}],
        "requested_permissions": ["metadata", "paths"]
    }]);
    let manifest_json = manifest_value.to_string();
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let entry = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("a crashed `installing` leftover must recover, not conflict");

    assert_eq!(entry.status, PluginInstallStatus::Installed);
    assert_eq!(
        entry.registered.mcp_server_ids,
        vec!["leftover-mcp".to_string()]
    );
    assert_eq!(
        entry.registered.event_sink_grants["leftover-event-sink"]
            .iter()
            .map(ObservationPermissionId::as_str)
            .collect::<Vec<_>>(),
        vec!["metadata", "paths"],
        "Installing recovery must preserve only the exact still-requested host authority"
    );

    // Provenance flipped to `installed`; the mcp entry is still owned.
    let listed = installer.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].status, PluginInstallStatus::Installed);
    let config = state.config.read().await;
    assert!(config.mcp.servers.iter().any(|s| s.id == "leftover-mcp"));
}

// ---------------------------------------------------------------------
// Services (issue #479, prereq for epic #477). Same shapes as the MCP
// section above: REFUSE-on-foreign-conflict, best-effort start, upgrade
// drop-diff — but reconciled against `installed.json` (via
// `existing_service_ids`) rather than `config.json`, since there is no
// single shared document for services. See `register_services`'s doc
// comment.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_registers_service_with_provenance_even_when_the_binary_is_missing() {
    let root = tempfile::tempdir().unwrap();
    let (_state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    let plugin_dir = root.path().join("plugins").join("svc-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    let manifest_json = service_manifest_json("svc-plugin", "1.0.0", &["svc"]);
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let entry = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("install with a service entry (binary missing) must still succeed");

    // Ownership recorded regardless of the (missing-binary) start outcome —
    // matches `register_mcp`'s best-effort contract.
    assert_eq!(entry.registered.service_ids, vec!["svc".to_string()]);

    let listed = installer.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].registered.service_ids, vec!["svc".to_string()]);
}

#[tokio::test]
async fn foreign_service_conflict_refuses_install_and_does_not_touch_the_owner() {
    let root = tempfile::tempdir().unwrap();
    let (_state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    // Seed provenance for a DIFFERENT, already-installed plugin that owns
    // service id "shared-svc" (the services analog of "config.json already
    // has this mcp server id" — there is no shared config document for
    // services, so ownership lives entirely in `installed.json`).
    let installed_json = root
        .path()
        .join("bamboo-home")
        .join("plugins")
        .join("installed.json");
    let mut store = InstalledPlugins::default();
    store.add(InstalledPlugin {
        id: "owner-plugin".to_string(),
        version: "1.0.0".to_string(),
        source: PluginSource::LocalDir {
            path: PathBuf::from("/tmp/owner"),
        },
        plugin_dir: root.path().join("plugins").join("owner-plugin"),
        installed_at: Utc::now(),
        status: PluginInstallStatus::Installed,
        registered: RegisteredCapabilities {
            service_ids: vec!["shared-svc".to_string()],
            ..Default::default()
        },
    });
    store.save(&installed_json).await.unwrap();

    let plugin_dir = root.path().join("plugins").join("conflicting-svc-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    let manifest_json = service_manifest_json("conflicting-svc-plugin", "1.0.0", &["shared-svc"]);
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let error = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect_err("a foreign service id collision must refuse the install");
    assert!(matches!(
        error,
        PluginError::Conflict {
            kind: "service",
            ..
        }
    ));

    // The original owner's provenance is untouched; the conflicting install
    // never got recorded.
    let listed = installer.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "owner-plugin");
    assert_eq!(
        listed[0].registered.service_ids,
        vec!["shared-svc".to_string()]
    );
}

#[tokio::test]
async fn upgrade_deregisters_service_dropped_by_the_new_version_and_frees_the_id() {
    let root = tempfile::tempdir().unwrap();
    let (_state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    let plugin_dir = root.path().join("plugins").join("multi-svc-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();

    let v1_json = service_manifest_json("multi-svc-plugin", "1.0.0", &["alpha", "beta"]);
    tokio::fs::write(plugin_dir.join("plugin.json"), &v1_json)
        .await
        .unwrap();
    let v1_manifest = PluginManifest::parse_str(&v1_json).unwrap();

    let v1_entry = installer
        .install(
            &v1_manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("install v1");
    assert_eq!(
        v1_entry.registered.service_ids,
        vec!["alpha".to_string(), "beta".to_string()]
    );

    // "Upgrade" to v2, which only declares alpha.
    let v2_json = service_manifest_json("multi-svc-plugin", "2.0.0", &["alpha"]);
    tokio::fs::write(plugin_dir.join("plugin.json"), &v2_json)
        .await
        .unwrap();
    let v2_manifest = PluginManifest::parse_str(&v2_json).unwrap();

    let v2_entry = installer
        .install(
            &v2_manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::Upgrade,
            Utc::now(),
        )
        .await
        .expect("upgrade to v2");
    assert_eq!(v2_entry.registered.service_ids, vec!["alpha".to_string()]);

    // `beta` was dropped and de-registered — a DIFFERENT plugin can now
    // claim that id without a foreign conflict, proving it was actually
    // freed (not just absent from THIS plugin's own provenance row).
    let other_plugin_dir = root.path().join("plugins").join("other-plugin");
    tokio::fs::create_dir_all(&other_plugin_dir).await.unwrap();
    let other_json = service_manifest_json("other-plugin", "1.0.0", &["beta"]);
    tokio::fs::write(other_plugin_dir.join("plugin.json"), &other_json)
        .await
        .unwrap();
    let other_manifest = PluginManifest::parse_str(&other_json).unwrap();
    let other_entry = installer
        .install(
            &other_manifest,
            &other_plugin_dir,
            PluginSource::LocalDir {
                path: other_plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("beta must be free for a different plugin to claim after the upgrade dropped it");
    assert_eq!(other_entry.registered.service_ids, vec!["beta".to_string()]);
}

// ---------------------------------------------------------------------
// Event sink manifest/provenance foundation (#903). This slice records exact
// ownership and validates before mutation; it deliberately creates no live
// router, queue, or service-input channel (#905/#906).
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_records_supported_and_future_event_sink_ownership() {
    let root = tempfile::tempdir().unwrap();
    let (_state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let plugin_dir = root.path().join("plugins").join("event-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    let manifest_json = event_sink_manifest_json(
        "event-plugin",
        "1.0.0",
        "audit-service",
        &[
            ("audit-v1", TOOL_EVENT_V1_SCHEMA_VERSION),
            ("audit-future", TOOL_EVENT_V1_SCHEMA_VERSION + 1),
        ],
    );
    let mut manifest_value: serde_json::Value =
        serde_json::from_str(&manifest_json).expect("manifest json");
    manifest_value["provides"]["event_sinks"][1]["subscriptions"] =
        serde_json::json!([{"id": "tool.symbol_changed.v2", "tool_names": ["FutureTool"]}]);
    manifest_value["provides"]["event_sinks"][1]["requested_permissions"] =
        serde_json::json!(["symbol_metadata_v2"]);
    let manifest_json = manifest_value.to_string();
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let entry = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("supported and future sinks should both install");
    assert_eq!(
        entry.registered.event_sink_ids,
        vec!["audit-v1".to_string(), "audit-future".to_string()]
    );
    assert_eq!(
        entry.registered.service_ids,
        vec!["audit-service".to_string()]
    );

    let listed = installer.list().await.unwrap();
    assert_eq!(
        listed[0].registered.event_sink_ids,
        entry.registered.event_sink_ids
    );

    installer
        .uninstall("event-plugin")
        .await
        .expect("uninstall clears sink provenance with the plugin row");
    assert!(installer.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn foreign_event_sink_conflict_fails_before_candidate_provenance_mutation() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let installed_json = state.app_data_dir.join("plugins").join("installed.json");
    let mut store = InstalledPlugins::default();
    store.add(InstalledPlugin {
        id: "owner-plugin".to_string(),
        version: "1.0.0".to_string(),
        source: PluginSource::LocalDir {
            path: PathBuf::from("/tmp/owner-plugin"),
        },
        plugin_dir: PathBuf::from("/tmp/owner-plugin"),
        installed_at: Utc::now(),
        status: PluginInstallStatus::Installed,
        registered: RegisteredCapabilities {
            event_sink_ids: vec!["shared-sink".to_string()],
            ..Default::default()
        },
    });
    store.save(&installed_json).await.unwrap();

    let plugin_dir = root.path().join("plugins").join("candidate-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    let manifest_json = event_sink_manifest_json(
        "candidate-plugin",
        "1.0.0",
        "candidate-service",
        &[("shared-sink", TOOL_EVENT_V1_SCHEMA_VERSION)],
    );
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let error = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect_err("foreign sink ownership must fail before registration");
    assert!(matches!(
        error,
        PluginError::Conflict {
            kind: "event sink",
            ..
        }
    ));
    let listed = installer.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "owner-plugin");
    assert_eq!(listed[0].registered.event_sink_ids, vec!["shared-sink"]);
}

#[tokio::test]
async fn corrupt_self_and_foreign_sink_ownership_fails_closed_for_installed_and_installing_rows() {
    for current_status in [
        PluginInstallStatus::Installed,
        PluginInstallStatus::Installing,
    ] {
        let root = tempfile::tempdir().unwrap();
        let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;
        let installed_json = state.app_data_dir.join("plugins").join("installed.json");
        let plugin_dir = root.path().join("plugins").join("candidate-plugin");
        tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
        let manifest_json = event_sink_manifest_json(
            "candidate-plugin",
            "2.0.0",
            "candidate-service",
            &[("shared-sink", TOOL_EVENT_V1_SCHEMA_VERSION)],
        );
        tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
            .await
            .unwrap();
        let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

        let current = InstalledPlugin {
            id: "candidate-plugin".to_string(),
            version: "1.0.0".to_string(),
            source: PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            plugin_dir: plugin_dir.clone(),
            installed_at: Utc::now(),
            status: current_status,
            registered: RegisteredCapabilities {
                service_ids: vec!["candidate-service".to_string()],
                event_sink_ids: vec!["shared-sink".to_string()],
                ..Default::default()
            },
        };
        let foreign = InstalledPlugin {
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
        };
        let mut store = InstalledPlugins::default();
        store.add(current.clone());
        store.add(foreign.clone());
        store.save(&installed_json).await.unwrap();
        let before = store.plugins.clone();

        let error = installer
            .install(
                &manifest,
                &plugin_dir,
                PluginSource::LocalDir {
                    path: plugin_dir.clone(),
                },
                InstallDisposition::Upgrade,
                Utc::now(),
            )
            .await
            .expect_err("foreign row must outrank corrupt self ownership");
        assert!(matches!(
            error,
            PluginError::Conflict {
                kind: "event sink",
                ..
            }
        ));
        let after = InstalledPlugins::load(&installed_json).await.unwrap();
        assert_eq!(after.plugins, before, "status={current_status:?}");
    }
}

#[tokio::test]
async fn corrupt_self_and_foreign_backing_service_ownership_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let installed_json = state.app_data_dir.join("plugins").join("installed.json");
    let plugin_dir = root.path().join("plugins").join("candidate-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    let manifest_json = event_sink_manifest_json(
        "candidate-plugin",
        "2.0.0",
        "shared-service",
        &[("candidate-sink", TOOL_EVENT_V1_SCHEMA_VERSION)],
    );
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let mut store = InstalledPlugins::default();
    for id in ["candidate-plugin", "foreign-plugin"] {
        store.plugins.push(InstalledPlugin {
            id: id.to_string(),
            version: "1.0.0".to_string(),
            source: PluginSource::LocalDir {
                path: PathBuf::from(format!("/tmp/{id}")),
            },
            plugin_dir: if id == "candidate-plugin" {
                plugin_dir.clone()
            } else {
                PathBuf::from("/tmp/foreign-plugin")
            },
            installed_at: Utc::now(),
            status: PluginInstallStatus::Installed,
            registered: RegisteredCapabilities {
                service_ids: vec!["shared-service".to_string()],
                event_sink_ids: if id == "candidate-plugin" {
                    vec!["candidate-sink".to_string()]
                } else {
                    Vec::new()
                },
                ..Default::default()
            },
        });
    }
    store.save(&installed_json).await.unwrap();
    let before = store.plugins.clone();

    let error = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::Upgrade,
            Utc::now(),
        )
        .await
        .expect_err("foreign service owner must outrank corrupt self ownership");
    assert!(matches!(
        error,
        PluginError::Conflict {
            kind: "event sink service",
            ..
        }
    ));
    assert_eq!(
        InstalledPlugins::load(&installed_json)
            .await
            .unwrap()
            .plugins,
        before
    );
}

#[tokio::test]
async fn boot_global_audit_blocks_duplicate_sink_backing_services_but_starts_safe_plugins() {
    let data_dir = tempfile::tempdir().unwrap();
    let plugins_root = data_dir.path().join("plugins");
    tokio::fs::create_dir_all(&plugins_root).await.unwrap();
    let mut store = InstalledPlugins::default();

    for (plugin_id, service_id, sink_id) in [
        ("first-plugin", "first-service", "shared-sink"),
        ("second-plugin", "second-service", "shared-sink"),
        ("safe-plugin", "safe-service", "safe-sink"),
    ] {
        let plugin_dir = plugins_root.join(plugin_id);
        tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
        let raw = event_sink_manifest_json(
            plugin_id,
            "1.0.0",
            service_id,
            &[(sink_id, TOOL_EVENT_V1_SCHEMA_VERSION)],
        );
        let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        value["provides"]["services"][0]["enabled"] = serde_json::json!(true);
        tokio::fs::write(plugin_dir.join("plugin.json"), value.to_string())
            .await
            .unwrap();
        store.plugins.push(InstalledPlugin {
            id: plugin_id.to_string(),
            version: "1.0.0".to_string(),
            source: PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            plugin_dir,
            installed_at: Utc::now(),
            status: PluginInstallStatus::Installed,
            registered: RegisteredCapabilities {
                service_ids: vec![service_id.to_string()],
                event_sink_ids: vec![sink_id.to_string()],
                ..Default::default()
            },
        });
    }
    store
        .save(&plugins_root.join("installed.json"))
        .await
        .unwrap();

    let state = AppState::new(data_dir.path().to_path_buf())
        .await
        .expect("app state");
    state.wait_for_boot_reconcile_services().await;
    assert!(!state.service_manager.is_running("first-service"));
    assert!(!state.service_manager.is_running("second-service"));
    assert!(
        state.service_manager.is_running("safe-service"),
        "a corrupt row must not stop the independent boot reconciliation pass"
    );
}

#[tokio::test]
async fn boot_global_audit_blocks_duplicate_plugin_ids_with_distinct_capabilities() {
    let data_dir = tempfile::tempdir().unwrap();
    let plugins_root = data_dir.path().join("plugins");
    tokio::fs::create_dir_all(&plugins_root).await.unwrap();
    let mut store = InstalledPlugins::default();

    for (dir_name, plugin_id, service_id, sink_id) in [
        ("duplicate-a", "duplicate-plugin", "service-a", "sink-a"),
        ("duplicate-b", "duplicate-plugin", "service-b", "sink-b"),
        ("safe-dir", "safe-plugin", "safe-service", "safe-sink"),
    ] {
        let plugin_dir = plugins_root.join(dir_name);
        tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
        let raw = event_sink_manifest_json(
            plugin_id,
            "1.0.0",
            service_id,
            &[(sink_id, TOOL_EVENT_V1_SCHEMA_VERSION)],
        );
        let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        value["provides"]["services"][0]["enabled"] = serde_json::json!(true);
        tokio::fs::write(plugin_dir.join("plugin.json"), value.to_string())
            .await
            .unwrap();
        store.plugins.push(InstalledPlugin {
            id: plugin_id.to_string(),
            version: "1.0.0".to_string(),
            source: PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            plugin_dir,
            installed_at: Utc::now(),
            status: PluginInstallStatus::Installed,
            registered: RegisteredCapabilities {
                service_ids: vec![service_id.to_string()],
                event_sink_ids: vec![sink_id.to_string()],
                ..Default::default()
            },
        });
    }
    store
        .save(&plugins_root.join("installed.json"))
        .await
        .unwrap();

    let state = AppState::new(data_dir.path().to_path_buf())
        .await
        .expect("app state");
    state.wait_for_boot_reconcile_services().await;
    assert!(!state.service_manager.is_running("service-a"));
    assert!(!state.service_manager.is_running("service-b"));
    assert!(
        state.service_manager.is_running("safe-service"),
        "duplicate plugin identity must not poison an unrelated boot candidate"
    );
}

#[tokio::test]
async fn invalid_sink_policy_fails_before_mcp_or_service_registration() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let plugin_dir = root.path().join("plugins").join("invalid-event-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();

    let manifest_json = event_sink_manifest_json(
        "invalid-event-plugin",
        "1.0.0",
        "must-not-register-service",
        &[("invalid-sink", TOOL_EVENT_V1_SCHEMA_VERSION)],
    );
    let mut manifest_value: serde_json::Value =
        serde_json::from_str(&manifest_json).expect("manifest json");
    manifest_value["provides"]["event_sinks"][0]["requested_permissions"] =
        serde_json::json!(["metadata", "unknown-v1-permission"]);
    manifest_value["provides"]["mcp_servers"] = serde_json::json!([{
        "id": "must-not-register-mcp",
        "transport": {"type": "stdio", "command": NONEXISTENT_COMMAND}
    }]);
    let manifest_json = manifest_value.to_string();
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();

    let error = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect_err("invalid sink policy must fail in pure preflight");
    assert!(matches!(error, PluginError::InvalidManifest(_)));
    assert!(installer.list().await.unwrap().is_empty());
    assert!(!state
        .config
        .read()
        .await
        .mcp
        .servers
        .iter()
        .any(|server| server.id == "must-not-register-mcp"));
    assert!(!state
        .service_manager
        .is_running("must-not-register-service"));
}

#[tokio::test]
async fn tool_event_v1_without_ndjson_input_fails_before_runtime_or_provenance_mutation() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let plugin_dir = root.path().join("plugins").join("null-stdin-event-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();

    let raw = event_sink_manifest_json(
        "null-stdin-event-plugin",
        "1.0.0",
        "must-not-start-service",
        &[("must-not-register-sink", TOOL_EVENT_V1_SCHEMA_VERSION)],
    );
    let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    value["provides"]["services"][0]
        .as_object_mut()
        .expect("service object")
        .remove("input_protocol");
    value["provides"]["services"][0]["enabled"] = serde_json::json!(true);
    value["provides"]["mcp_servers"] = serde_json::json!([{
        "id": "must-not-register-mcp",
        "transport": {"type": "stdio", "command": NONEXISTENT_COMMAND}
    }]);
    let raw = value.to_string();
    tokio::fs::write(plugin_dir.join("plugin.json"), &raw)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&raw).unwrap();

    let error = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect_err("a V1 sink backed by null stdin must fail pure preflight");
    assert!(error.to_string().contains("input_protocol 'ndjson_v1'"));
    assert!(installer.list().await.unwrap().is_empty());
    assert!(!state
        .config
        .read()
        .await
        .mcp
        .servers
        .iter()
        .any(|server| server.id == "must-not-register-mcp"));
    assert!(!state.service_manager.is_running("must-not-start-service"));
    assert_eq!(
        state
            .tool_event_router
            .status_for_ids(&["must-not-register-sink".to_string()])
            .await[0]
            .state,
        ToolEventSinkState::Unavailable
    );
}

#[tokio::test]
async fn upgrade_replaces_event_sink_provenance_exactly_and_frees_removed_id() {
    let root = tempfile::tempdir().unwrap();
    let (_state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let plugin_dir = root.path().join("plugins").join("event-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();

    let v1_json = event_sink_manifest_json(
        "event-plugin",
        "1.0.0",
        "audit-service",
        &[
            ("retained", TOOL_EVENT_V1_SCHEMA_VERSION),
            ("removed", TOOL_EVENT_V1_SCHEMA_VERSION),
        ],
    );
    tokio::fs::write(plugin_dir.join("plugin.json"), &v1_json)
        .await
        .unwrap();
    let v1 = PluginManifest::parse_str(&v1_json).unwrap();
    installer
        .install(
            &v1,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("install event sink v1");

    let v2_json = event_sink_manifest_json(
        "event-plugin",
        "2.0.0",
        "audit-service",
        &[
            ("retained", TOOL_EVENT_V1_SCHEMA_VERSION),
            ("added", TOOL_EVENT_V1_SCHEMA_VERSION),
        ],
    );
    tokio::fs::write(plugin_dir.join("plugin.json"), &v2_json)
        .await
        .unwrap();
    let v2 = PluginManifest::parse_str(&v2_json).unwrap();
    let upgraded = installer
        .install(
            &v2,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::Upgrade,
            Utc::now(),
        )
        .await
        .expect("upgrade event sink plugin");
    assert_eq!(
        upgraded.registered.event_sink_ids,
        vec!["retained".to_string(), "added".to_string()]
    );

    let other_dir = root.path().join("plugins").join("other-event-plugin");
    tokio::fs::create_dir_all(&other_dir).await.unwrap();
    let other_json = event_sink_manifest_json(
        "other-event-plugin",
        "1.0.0",
        "other-service",
        &[("removed", TOOL_EVENT_V1_SCHEMA_VERSION)],
    );
    tokio::fs::write(other_dir.join("plugin.json"), &other_json)
        .await
        .unwrap();
    let other = PluginManifest::parse_str(&other_json).unwrap();
    installer
        .install(
            &other,
            &other_dir,
            PluginSource::LocalDir {
                path: other_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("removed sink id must be free for another plugin");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_backing_service_revokes_retained_sink_before_old_service_stop() {
    use std::time::Duration;

    use bamboo_domain::mcp_config::ReconnectConfig;
    use bamboo_plugin::manifest::{GracefulShutdown, HealthCheckSpec, ServiceInputProtocol};
    use bamboo_plugin::reconcile_event_sinks;

    use crate::service_manager::ServiceRuntimeConfig;

    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let raw = event_sink_manifest_json(
        "changed-service-plugin",
        "1.0.0",
        "old-service",
        &[("retained-sink", TOOL_EVENT_V1_SCHEMA_VERSION)],
    );
    let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    value["provides"]["services"][0]["enabled"] = serde_json::json!(true);
    let old_manifest = PluginManifest::parse_str(&value.to_string()).unwrap();
    old_manifest.validate().unwrap();

    let previous = RegisteredCapabilities {
        service_ids: vec!["old-service".to_string()],
        event_sink_ids: vec!["retained-sink".to_string()],
        ..Default::default()
    };
    let replacement = RegisteredCapabilities {
        service_ids: vec!["new-service".to_string()],
        event_sink_ids: vec!["retained-sink".to_string()],
        ..Default::default()
    };
    let dropped = replacement.removed_since(&previous);
    assert!(dropped.event_sink_ids.is_empty());
    assert_eq!(dropped.service_ids, vec!["old-service".to_string()]);

    state
        .service_manager
        .start_service(ServiceRuntimeConfig {
            id: "old-service".to_string(),
            plugin_id: "changed-service-plugin".to_string(),
            name: None,
            command: PathBuf::from("/bin/sh"),
            args: vec![
                "-c".to_string(),
                "while IFS= read -r line; do :; done".to_string(),
            ],
            cwd: None,
            env: Default::default(),
            health_check: HealthCheckSpec::default(),
            restart_policy: ReconnectConfig {
                enabled: false,
                ..ReconnectConfig::default()
            },
            graceful_shutdown: GracefulShutdown::default(),
            input_protocol: ServiceInputProtocol::NdjsonV1,
            user_config_path: root.path().join("service-config.json"),
        })
        .await
        .unwrap();
    let plan = reconcile_event_sinks(
        &old_manifest,
        &previous,
        PluginInstallStatus::Installed,
        Platform::current(),
    )
    .unwrap();
    let grants =
        canonicalize_persisted_event_sink_grants(&old_manifest, &previous.event_sink_grants)
            .unwrap();
    state
        .tool_event_router
        .apply_plugin_plan("changed-service-plugin", &old_manifest, &plan, &grants)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = state
                .tool_event_router
                .status_for_ids(&["retained-sink".to_string()])
                .await;
            if status[0].state == ToolEventSinkState::Live {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("old sink worker becomes live");

    installer
        .deregister_upgrade_drop_diff("changed-service-plugin", &previous, &dropped)
        .await;
    assert_eq!(
        state
            .tool_event_router
            .status_for_ids(&["retained-sink".to_string()])
            .await[0]
            .state,
        ToolEventSinkState::Unavailable
    );
    assert!(!state.service_manager.is_running("old-service"));
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_upgrade_dropping_unrelated_service_preserves_retained_live_route() {
    use std::time::Duration;

    use bamboo_domain::mcp_config::ReconnectConfig;
    use bamboo_plugin::manifest::{GracefulShutdown, HealthCheckSpec, ServiceInputProtocol};
    use bamboo_plugin::reconcile_event_sinks;

    use crate::service_manager::ServiceRuntimeConfig;

    let root = tempfile::tempdir().unwrap();
    let data_dir = root.path().join("bamboo-home");
    let (state, installer) = new_installer(&data_dir).await;
    let plugin_dir = data_dir.join("plugins").join("rollback-route-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();

    let old_raw = event_sink_manifest_json(
        "rollback-route-plugin",
        "1.0.0",
        "retained-service",
        &[("retained-sink", TOOL_EVENT_V1_SCHEMA_VERSION)],
    );
    let mut old_value: serde_json::Value = serde_json::from_str(&old_raw).unwrap();
    old_value["provides"]["services"][0]["enabled"] = serde_json::json!(true);
    old_value["provides"]["services"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "id": "dropped-service",
            "enabled": true,
            "command": "${platform_bin}",
            "input_protocol": "ndjson_v1"
        }));
    let old_manifest = PluginManifest::parse_str(&old_value.to_string()).unwrap();
    old_manifest.validate().unwrap();

    let mut new_value = old_value.clone();
    new_value["version"] = serde_json::json!("2.0.0");
    new_value["provides"]["services"]
        .as_array_mut()
        .unwrap()
        .retain(|service| service["id"] == "retained-service");
    let new_raw = new_value.to_string();
    tokio::fs::write(plugin_dir.join("plugin.json"), &new_raw)
        .await
        .unwrap();
    let new_manifest = PluginManifest::parse_str(&new_raw).unwrap();
    new_manifest.validate().unwrap();

    let previous_registered = RegisteredCapabilities {
        service_ids: vec![
            "retained-service".to_string(),
            "dropped-service".to_string(),
        ],
        event_sink_ids: vec!["retained-sink".to_string()],
        ..Default::default()
    };
    let previous_entry = InstalledPlugin {
        id: "rollback-route-plugin".to_string(),
        version: "1.0.0".to_string(),
        source: PluginSource::LocalDir {
            path: plugin_dir.clone(),
        },
        plugin_dir: plugin_dir.clone(),
        installed_at: Utc::now(),
        status: PluginInstallStatus::Installed,
        registered: previous_registered.clone(),
    };
    let mut store = InstalledPlugins::default();
    store.add(previous_entry.clone());
    store
        .save(&data_dir.join("plugins").join("installed.json"))
        .await
        .unwrap();

    for service_id in ["retained-service", "dropped-service"] {
        state
            .service_manager
            .start_service(ServiceRuntimeConfig {
                id: service_id.to_string(),
                plugin_id: "rollback-route-plugin".to_string(),
                name: None,
                command: PathBuf::from("/bin/sh"),
                args: vec![
                    "-c".to_string(),
                    "while IFS= read -r line; do :; done".to_string(),
                ],
                cwd: None,
                env: Default::default(),
                health_check: HealthCheckSpec::default(),
                restart_policy: ReconnectConfig {
                    enabled: false,
                    ..ReconnectConfig::default()
                },
                graceful_shutdown: GracefulShutdown::default(),
                input_protocol: ServiceInputProtocol::NdjsonV1,
                user_config_path: root.path().join(format!("{service_id}-config.json")),
            })
            .await
            .unwrap();
    }
    let old_plan = reconcile_event_sinks(
        &old_manifest,
        &previous_registered,
        PluginInstallStatus::Installed,
        Platform::current(),
    )
    .unwrap();
    let old_grants = canonicalize_persisted_event_sink_grants(
        &old_manifest,
        &previous_registered.event_sink_grants,
    )
    .unwrap();
    state
        .tool_event_router
        .apply_plugin_plan(
            "rollback-route-plugin",
            &old_manifest,
            &old_plan,
            &old_grants,
        )
        .await
        .unwrap();
    let prior_generation = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = state
                .tool_event_router
                .status_for_ids(&["retained-sink".to_string()])
                .await;
            if status[0].state == ToolEventSinkState::Live {
                break status[0].generation.expect("live route generation");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retained route becomes live");

    let guard = installer.begin_operation().await;
    let error = installer
        .install_with_operation_failing_before_service_replacement(
            &new_manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::Upgrade,
            Utc::now(),
            &guard,
        )
        .await
        .expect_err("injected failure must abort before service replacement");
    drop(guard);
    assert!(error
        .to_string()
        .contains("injected failure before service replacement"));

    let retained_status = state
        .tool_event_router
        .status_for_ids(&["retained-sink".to_string()])
        .await;
    assert_eq!(retained_status[0].state, ToolEventSinkState::Live);
    assert_eq!(retained_status[0].generation, Some(prior_generation));
    assert!(state.service_manager.is_running("retained-service"));
    assert!(!state.service_manager.is_running("dropped-service"));

    let restored = InstalledPlugins::load(&data_dir.join("plugins").join("installed.json"))
        .await
        .unwrap();
    assert_eq!(
        restored
            .get_unique("rollback-route-plugin")
            .unwrap()
            .expect("previous provenance restored"),
        &previous_entry
    );

    state
        .tool_event_router
        .unregister_sinks(&["retained-sink".to_string()])
        .await;
    state
        .service_manager
        .stop_service("retained-service")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------
// Same-id upgrade ordering (issue #479): `stop_services_for_upgrade` is the
// seam the HTTP update path uses after prepared-candidate preflight and before
// bundle activation. A later failure deliberately leaves services stopped;
// only the stop ordering belongs in this installer-level test section.
// ---------------------------------------------------------------------

#[tokio::test]
async fn stop_services_for_upgrade_stops_the_running_service_and_returns_its_id() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    let plugin_dir = root.path().join("plugins").join("svc-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    let manifest_json = service_manifest_json("svc-plugin", "1.0.0", &["svc"]);
    tokio::fs::write(plugin_dir.join("plugin.json"), &manifest_json)
        .await
        .unwrap();
    let manifest = PluginManifest::parse_str(&manifest_json).unwrap();
    installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("install");
    assert!(
        state.service_manager.is_running("svc"),
        "start_service must have registered a runtime even though the binary is missing \
         (best-effort start, matches mcp)"
    );

    let stopped = installer.stop_services_for_upgrade("svc-plugin").await;
    assert_eq!(stopped, vec!["svc".to_string()]);
    assert!(
        !state.service_manager.is_running("svc"),
        "stop_services_for_upgrade must have actually stopped it before returning"
    );
}

#[tokio::test]
async fn stop_services_for_upgrade_on_a_plugin_with_no_services_is_a_harmless_noop() {
    let root = tempfile::tempdir().unwrap();
    let (_state, installer) = new_installer(&root.path().join("bamboo-home")).await;
    let stopped = installer.stop_services_for_upgrade("never-installed").await;
    assert!(stopped.is_empty());
}

#[tokio::test]
async fn boot_reconcile_takes_plugin_op_lock_before_reading_its_generation_plan() {
    let root = tempfile::tempdir().unwrap();
    let data_dir = root.path().join("bamboo-home");
    let (state, _installer) = new_installer(&data_dir).await;
    let plugins_root = data_dir.join("plugins");
    tokio::fs::create_dir_all(&plugins_root).await.unwrap();

    let old_dir = plugins_root.join("generation-old");
    let new_dir = plugins_root.join("generation-new");
    tokio::fs::create_dir_all(&old_dir).await.unwrap();
    tokio::fs::create_dir_all(&new_dir).await.unwrap();
    let old_manifest = event_sink_manifest_json(
        "generation-plugin",
        "1.0.0",
        "generation-service",
        &[("old-sink", TOOL_EVENT_V1_SCHEMA_VERSION)],
    );
    let new_manifest = event_sink_manifest_json(
        "generation-plugin",
        "2.0.0",
        "generation-service",
        &[("new-sink", TOOL_EVENT_V1_SCHEMA_VERSION)],
    );
    let add_paths_request = |raw: String| {
        let mut value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        value["provides"]["event_sinks"][0]["requested_permissions"] =
            serde_json::json!(["metadata", "paths"]);
        value.to_string()
    };
    tokio::fs::write(old_dir.join("plugin.json"), add_paths_request(old_manifest))
        .await
        .unwrap();
    tokio::fs::write(new_dir.join("plugin.json"), add_paths_request(new_manifest))
        .await
        .unwrap();

    let entry = |version: &str, plugin_dir: PathBuf, sink_id: &str| InstalledPlugin {
        id: "generation-plugin".to_string(),
        version: version.to_string(),
        source: PluginSource::LocalDir {
            path: plugin_dir.clone(),
        },
        plugin_dir,
        installed_at: Utc::now(),
        status: PluginInstallStatus::Installed,
        registered: RegisteredCapabilities {
            service_ids: vec!["generation-service".to_string()],
            event_sink_ids: vec![sink_id.to_string()],
            event_sink_grants: std::collections::BTreeMap::from([(
                sink_id.to_string(),
                vec![
                    ObservationPermissionId::new("metadata"),
                    ObservationPermissionId::new("paths"),
                ],
            )]),
            ..Default::default()
        },
    };
    let installed_path = plugins_root.join("installed.json");
    let op_guard = PLUGIN_OP_LOCK.lock().await;
    let mut old_store = InstalledPlugins::default();
    old_store.plugins.push(entry("1.0.0", old_dir, "old-sink"));
    old_store.save(&installed_path).await.unwrap();

    let boot = {
        let data_dir = data_dir.clone();
        let service_manager = state.service_manager.clone();
        let router = state.tool_event_router.clone();
        tokio::spawn(async move {
            boot_reconcile_services(&data_dir, &service_manager, &router).await;
        })
    };
    tokio::task::yield_now().await;
    assert!(
        !boot.is_finished(),
        "boot must wait for the plugin operation generation lock"
    );

    let mut new_store = InstalledPlugins::default();
    new_store.plugins.push(entry("2.0.0", new_dir, "new-sink"));
    new_store.save(&installed_path).await.unwrap();
    drop(op_guard);
    boot.await.unwrap();

    let status = state
        .tool_event_router
        .status_for_ids(&["old-sink".to_string(), "new-sink".to_string()])
        .await;
    assert_eq!(status[0].state, ToolEventSinkState::Unavailable);
    assert_eq!(status[1].state, ToolEventSinkState::Inactive);
    assert_eq!(
        status[1]
            .granted_permissions
            .iter()
            .map(ObservationPermissionId::as_str)
            .collect::<Vec<_>>(),
        vec!["metadata", "paths"]
    );
    assert!(status[1].policy_generation.is_some());
}

#[tokio::test]
async fn boot_rejects_corrupt_nonempty_grants_before_starting_plugin_service() {
    let data_dir = tempfile::tempdir().unwrap();
    let plugins_root = data_dir.path().join("plugins");
    let plugin_dir = plugins_root.join("corrupt-grants-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();

    let raw = event_sink_manifest_json(
        "corrupt-grants-plugin",
        "1.0.0",
        "must-not-start-service",
        &[("must-not-route-sink", TOOL_EVENT_V1_SCHEMA_VERSION)],
    );
    let mut manifest: serde_json::Value = serde_json::from_str(&raw).unwrap();
    manifest["provides"]["services"][0]["enabled"] = serde_json::json!(true);
    manifest["provides"]["event_sinks"][0]["requested_permissions"] =
        serde_json::json!(["metadata", "paths"]);
    tokio::fs::write(plugin_dir.join("plugin.json"), manifest.to_string())
        .await
        .unwrap();

    let mut store = InstalledPlugins::default();
    store.plugins.push(InstalledPlugin {
        id: "corrupt-grants-plugin".to_string(),
        version: "1.0.0".to_string(),
        source: PluginSource::LocalDir {
            path: plugin_dir.clone(),
        },
        plugin_dir,
        installed_at: Utc::now(),
        status: PluginInstallStatus::Installed,
        registered: RegisteredCapabilities {
            service_ids: vec!["must-not-start-service".to_string()],
            event_sink_ids: vec!["must-not-route-sink".to_string()],
            event_sink_grants: std::collections::BTreeMap::from([(
                "must-not-route-sink".to_string(),
                vec![
                    ObservationPermissionId::new("metadata"),
                    ObservationPermissionId::new("content"),
                ],
            )]),
            ..Default::default()
        },
    });
    store
        .save(&plugins_root.join("installed.json"))
        .await
        .unwrap();

    let state = AppState::new(data_dir.path().to_path_buf())
        .await
        .expect("app state");
    state.wait_for_boot_reconcile_services().await;
    assert!(
        !state.service_manager.is_running("must-not-start-service"),
        "grant authority must validate before any plugin-owned process starts"
    );
    let status = state
        .tool_event_router
        .status_for_ids(&["must-not-route-sink".to_string()])
        .await;
    assert_eq!(status[0].state, ToolEventSinkState::Unavailable);
    assert!(status[0].granted_permissions.is_empty());
}
