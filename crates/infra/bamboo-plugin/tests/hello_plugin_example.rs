//! End-to-end exercise of the `examples/hello-plugin` reference bundle: parse
//! its real `plugin.json` from disk, validate it, and confirm `install()`
//! gets all the way to the (expected, later-agent) `NotImplemented` step —
//! proving the manifest + on-disk skill layout are consistent with the
//! schema this crate defines.

use bamboo_plugin::{
    InstallDisposition, LocalPluginInstaller, PluginError, PluginInstaller, PluginManifest,
    PluginSource,
};

fn example_plugin_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("hello-plugin")
}

#[test]
fn hello_plugin_manifest_parses_and_validates() {
    let plugin_dir = example_plugin_dir();
    let raw = std::fs::read_to_string(plugin_dir.join("plugin.json")).expect("read plugin.json");
    let manifest = PluginManifest::parse_str(&raw).expect("parse plugin.json");
    manifest.validate().expect("hello-plugin manifest is valid");

    assert_eq!(manifest.id, "hello-plugin");
    assert_eq!(manifest.provides.skills, vec!["hello-world".to_string()]);
    assert_eq!(manifest.provides.prompts.len(), 1);
    assert_eq!(manifest.provides.prompts[0].id, "hello_plugin_greeter");
    assert!(manifest.provides.mcp_servers.is_empty());
    assert!(manifest.provides.workflows.is_empty());
    assert!(manifest.artifacts.is_empty());

    let skill_md = plugin_dir
        .join("skills")
        .join("hello-world")
        .join("SKILL.md");
    assert!(skill_md.exists(), "example SKILL.md must exist on disk");
}

#[tokio::test]
async fn hello_plugin_install_reaches_registration_todo() {
    let plugin_dir = example_plugin_dir();
    let raw = std::fs::read_to_string(plugin_dir.join("plugin.json")).expect("read plugin.json");
    let manifest = PluginManifest::parse_str(&raw).expect("parse plugin.json");

    let bamboo_home = tempfile::tempdir().expect("tempdir");
    let installer = LocalPluginInstaller::new(bamboo_home.path().to_path_buf());

    let error = installer
        .install(
            &manifest,
            &plugin_dir,
            PluginSource::LocalDir {
                path: plugin_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            chrono::Utc::now(),
        )
        .await
        .expect_err("registration wiring is a later-agent TODO");

    // Confirms manifest validation + on-disk skill-dir check both passed —
    // the ONLY remaining step is the capability-registration wiring a later
    // agent implements.
    assert!(matches!(error, PluginError::NotImplemented(_)));
}
