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
// Workflow ownership: same REFUSE-on-conflict shape as MCP.
// ---------------------------------------------------------------------

#[tokio::test]
async fn foreign_workflow_conflict_refuses_install() {
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
        .expect_err("a foreign workflow filename collision must refuse the install");
    assert!(matches!(
        error,
        PluginError::Conflict {
            kind: "workflow",
            ..
        }
    ));

    // The user's workflow content must be untouched.
    let content = tokio::fs::read_to_string(workflows_dir.join("daily-report.md"))
        .await
        .unwrap();
    assert_eq!(content, "# my own workflow\n");
}

/// A manifest can declare 2+ workflows where the SECOND fails
/// `bamboo_config::paths::is_safe_workflow_name`'s stricter charset check
/// (bamboo-plugin's own manifest validation is looser — it only rejects path
/// separators/`..`/control chars, not e.g. `!`). This proves that when the
/// second file's copy fails mid-loop, the FIRST file (already really written
/// to `workflows_dir()`) is rolled back too — not just the batch as a whole.
#[tokio::test]
async fn partial_workflow_copy_failure_rolls_back_the_files_already_written() {
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

    // The first file must have been rolled back, not left orphaned.
    let workflows_dir = state.app_data_dir.join("workflows");
    assert!(
        !workflows_dir.join("good-one.md").exists(),
        "the first workflow's copy must be rolled back when the second fails"
    );
    assert!(!workflows_dir.join("bad!name.md").exists());

    // And nothing was committed to provenance.
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
