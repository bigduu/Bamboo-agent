use std::path::{Path, PathBuf};
use std::sync::Arc;

use actix_web::web;
use bamboo_plugin::{
    InstallDisposition, InstalledPlugin, InstalledPlugins, McpServerManifestEntry,
    McpTransportManifest, Platform, PluginError, PluginInstallStatus, PluginInstaller,
    PluginManifest, PluginSource, RegisteredCapabilities,
};
use chrono::Utc;

use super::ServerPluginInstaller;
use crate::app_state::AppState;

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
    // (`plugin_installer::boot_reconcile_services`) in the background,
    // unsynchronized against `PLUGIN_OP_LOCK` (see that function's doc
    // comment). On a fresh `data_dir` it is a same-tick no-op (nothing in
    // `installed.json` yet) — UNLESS it is still in flight when a
    // service-lifecycle test below writes `installed.json` and starts/stops
    // a service moments later, in which case it can race back in and
    // resurrect (or fail to see) a service the test just
    // installed/stopped, producing exactly the `is_running` flakes tracked
    // by issue #486. Draining it here (once, before any test touches
    // `installed.json`) removes that race entirely: by construction it can
    // only observe an empty store at this point, so this always resolves
    // near-instantly.
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
            ..Default::default()
        },
    });
    store.save(&installed_json).await.unwrap();

    // Re-run the install (plain `install` verb → FailIfInstalled). It must NOT
    // fail AlreadyInstalled (the row is `installing`, not a completed install),
    // and must NOT false-Conflict on `leftover-mcp` (recorded as the plugin's
    // own intended entry).
    let manifest_json = mcp_manifest_json("crashed-plugin", "1.0.0", &["leftover-mcp"]);
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
// Same-id upgrade ordering (issue #479): `stop_services_for_upgrade` /
// `restart_services_after_failed_upgrade` are the seam the HTTP
// `update_plugin` handler uses to stop a plugin's services BEFORE
// `stage_plugin_source` swaps `plugin_dir`, and to restart them if the
// upgrade subsequently fails and rolls back to the old bundle. Unit-tested
// directly here (rather than only through the full HTTP+staging pipeline)
// so the ordering contract is pinned precisely.
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
async fn restart_services_after_failed_upgrade_restarts_from_the_still_installed_manifest() {
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

    // Simulate the handler's pre-stage stop (see `update_plugin`).
    let stopped = installer.stop_services_for_upgrade("svc-plugin").await;
    assert_eq!(stopped, vec!["svc".to_string()]);
    assert!(!state.service_manager.is_running("svc"));

    // Simulate a FAILED upgrade whose `StagedPlugin::rollback()` restored
    // `plugin_dir` to the pre-upgrade bundle (here: nothing ever changed
    // `plugin_dir`'s on-disk `plugin.json`, which is exactly what a
    // successful rollback leaves behind).
    installer
        .restart_services_after_failed_upgrade("svc-plugin", &stopped)
        .await;
    assert!(
        state.service_manager.is_running("svc"),
        "the previously-stopped service must be running again after a failed upgrade"
    );
}

#[tokio::test]
async fn restart_services_after_failed_upgrade_skips_a_service_the_rolled_back_manifest_disabled() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer) = new_installer(&root.path().join("bamboo-home")).await;

    let plugin_dir = root.path().join("plugins").join("svc-plugin");
    tokio::fs::create_dir_all(&plugin_dir).await.unwrap();
    // Declare "svc" as DISABLED from the start.
    let manifest_json = serde_json::json!({
        "id": "svc-plugin",
        "name": "Svc",
        "version": "1.0.0",
        "provides": {
            "services": [{"id": "svc", "command": "${platform_bin}", "enabled": false}]
        }
    })
    .to_string();
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
    assert!(!state.service_manager.is_running("svc"));

    // Nothing was running to begin with, so `stopped` is empty here — but
    // exercise `restart_services_after_failed_upgrade` with a fabricated
    // non-empty `stopped` list to prove it still respects `enabled: false`
    // in the manifest it reads back, rather than blindly restarting
    // whatever it's told.
    installer
        .restart_services_after_failed_upgrade("svc-plugin", &["svc".to_string()])
        .await;
    assert!(
        !state.service_manager.is_running("svc"),
        "a disabled service must never be started by the restart-after-rollback path"
    );
}
