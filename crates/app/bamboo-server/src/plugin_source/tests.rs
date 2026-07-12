use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bamboo_plugin::{
    InstallDisposition, InstalledPlugin, PluginError, PluginInstaller, PluginManifest,
    PluginResult, PluginSource,
};
use chrono::{DateTime, Utc};

use super::*;

fn hello_manifest_json(id: &str) -> String {
    serde_json::json!({
        "id": id,
        "name": "Hello",
        "version": "0.1.0",
        "provides": {
            "skills": ["hello-world"],
        }
    })
    .to_string()
}

async fn write_hello_plugin_dir(dir: &Path, id: &str) {
    tokio::fs::create_dir_all(dir.join("skills").join("hello-world"))
        .await
        .unwrap();
    tokio::fs::write(dir.join("plugin.json"), hello_manifest_json(id))
        .await
        .unwrap();
    tokio::fs::write(
        dir.join("skills").join("hello-world").join("SKILL.md"),
        "---\nname: hello-world\ndescription: demo\n---\nHi\n",
    )
    .await
    .unwrap();
}

// ---------------------------------------------------------------------
// LocalDir
// ---------------------------------------------------------------------

#[tokio::test]
async fn stages_local_dir_and_parses_manifest() {
    let root = tempfile::tempdir().unwrap();
    let source_dir = root.path().join("source");
    write_hello_plugin_dir(&source_dir, "hello-plugin").await;

    let plugins_root = root.path().join("plugins");
    let staged = stage_plugin_source(
        PluginSourceInput::LocalDir(source_dir.clone()),
        &plugins_root,
    )
    .await
    .expect("stage local dir");

    assert_eq!(staged.manifest.id, "hello-plugin");
    assert_eq!(staged.plugin_dir, plugins_root.join("hello-plugin"));
    assert!(staged.plugin_dir.join("plugin.json").exists());
    assert!(staged
        .plugin_dir
        .join("skills")
        .join("hello-world")
        .join("SKILL.md")
        .exists());
    assert_eq!(staged.source, PluginSource::LocalDir { path: source_dir });
    staged.commit().await;
}

#[tokio::test]
async fn stages_local_dir_rejects_invalid_manifest_before_touching_plugins_root() {
    let root = tempfile::tempdir().unwrap();
    let source_dir = root.path().join("source");
    tokio::fs::create_dir_all(&source_dir).await.unwrap();
    tokio::fs::write(
        source_dir.join("plugin.json"),
        serde_json::json!({"id": "Bad Id!", "name": "Bad", "version": "1.0.0"}).to_string(),
    )
    .await
    .unwrap();

    let plugins_root = root.path().join("plugins");
    let error = stage_plugin_source(PluginSourceInput::LocalDir(source_dir), &plugins_root)
        .await
        .expect_err("invalid manifest should fail staging");
    assert!(matches!(error, PluginError::InvalidManifest(_)));
    assert!(
        !plugins_root.join("Bad Id!").exists(),
        "an invalid manifest must never be committed to plugins_root"
    );
}

// ---------------------------------------------------------------------
// LocalArchive
// ---------------------------------------------------------------------

fn build_targz(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_bytes);
        for (name, content) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, name, *content).unwrap();
        }
        builder.finish().unwrap();
    }
    let mut gz_bytes = Vec::new();
    {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let mut encoder = GzEncoder::new(&mut gz_bytes, Compression::default());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap();
    }
    gz_bytes
}

fn build_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    use std::io::{Cursor, Write};
    let mut buffer = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(Cursor::new(&mut buffer));
        let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
        for (name, content) in entries {
            writer.start_file(*name, options).unwrap();
            writer.write_all(content).unwrap();
        }
        writer.finish().unwrap();
    }
    buffer
}

#[tokio::test]
async fn stages_local_targz_archive_with_nested_top_level_dir() {
    let root = tempfile::tempdir().unwrap();
    let manifest = hello_manifest_json("hello-plugin");
    let archive_bytes = build_targz(&[
        ("hello-plugin/plugin.json", manifest.as_bytes()),
        (
            "hello-plugin/skills/hello-world/SKILL.md",
            b"---\nname: hello-world\ndescription: demo\n---\nHi\n",
        ),
    ]);
    let archive_path = root.path().join("bundle.tar.gz");
    tokio::fs::write(&archive_path, &archive_bytes)
        .await
        .unwrap();

    let plugins_root = root.path().join("plugins");
    let staged = stage_plugin_source(
        PluginSourceInput::LocalArchive(archive_path.clone()),
        &plugins_root,
    )
    .await
    .expect("stage tar.gz archive");

    assert_eq!(staged.manifest.id, "hello-plugin");
    assert!(staged.plugin_dir.join("plugin.json").exists());
    assert!(staged
        .plugin_dir
        .join("skills")
        .join("hello-world")
        .join("SKILL.md")
        .exists());
    staged.commit().await;
}

#[tokio::test]
async fn stages_local_zip_archive_at_root() {
    let root = tempfile::tempdir().unwrap();
    let manifest = hello_manifest_json("hello-plugin");
    let archive_bytes = build_zip(&[
        ("plugin.json", manifest.as_bytes()),
        (
            "skills/hello-world/SKILL.md",
            b"---\nname: hello-world\ndescription: demo\n---\nHi\n",
        ),
    ]);
    let archive_path = root.path().join("bundle.zip");
    tokio::fs::write(&archive_path, &archive_bytes)
        .await
        .unwrap();

    let plugins_root = root.path().join("plugins");
    let staged = stage_plugin_source(
        PluginSourceInput::LocalArchive(archive_path.clone()),
        &plugins_root,
    )
    .await
    .expect("stage zip archive");

    assert_eq!(staged.manifest.id, "hello-plugin");
    assert!(staged.plugin_dir.join("plugin.json").exists());
    staged.commit().await;
}

/// `tar::Builder::append_data` validates paths itself (rejects `..`) — a
/// real malicious archive wouldn't go through that safe API, so this builds
/// the header's raw name bytes directly to simulate a genuinely crafted
/// archive, then uses the unchecked `Builder::append`.
fn build_malicious_targz(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_bytes);
        for (name, content) in entries {
            let mut header = tar::Header::new_gnu();
            if let Some(gnu) = header.as_gnu_mut() {
                let name_bytes = name.as_bytes();
                gnu.name[..name_bytes.len()].copy_from_slice(name_bytes);
            }
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append(&header, *content).unwrap();
        }
        builder.finish().unwrap();
    }
    let mut gz_bytes = Vec::new();
    {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let mut encoder = GzEncoder::new(&mut gz_bytes, Compression::default());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap();
    }
    gz_bytes
}

#[tokio::test]
async fn tar_archive_with_traversal_entry_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let archive_bytes = build_malicious_targz(&[
        (
            "plugin.json",
            hello_manifest_json("hello-plugin").as_bytes(),
        ),
        ("../../evil.txt", b"pwned"),
    ]);
    let archive_path = root.path().join("evil.tar.gz");
    tokio::fs::write(&archive_path, &archive_bytes)
        .await
        .unwrap();

    let plugins_root = root.path().join("plugins");
    let error = stage_plugin_source(PluginSourceInput::LocalArchive(archive_path), &plugins_root)
        .await
        .expect_err("traversal entry must be rejected");
    assert!(matches!(error, PluginError::InvalidManifest(_)));

    // The traversal target must never have been written, regardless of
    // where extraction happened to stop.
    let escaped = plugins_root
        .parent()
        .unwrap()
        .parent()
        .map(|p| p.join("evil.txt"));
    if let Some(escaped) = escaped {
        assert!(!escaped.exists());
    }
}

#[tokio::test]
async fn zip_archive_with_traversal_entry_is_rejected() {
    use std::io::{Cursor, Write};

    let root = tempfile::tempdir().unwrap();
    let mut buffer = Vec::new();
    {
        // The `zip` crate does not sanitize entry NAMES on write, only on
        // READ via `enclosed_name()` — which is exactly the guard under
        // test — so we can write a genuinely malicious raw entry name here.
        let mut writer = zip::ZipWriter::new(Cursor::new(&mut buffer));
        let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
        writer.start_file("plugin.json", options).unwrap();
        writer
            .write_all(hello_manifest_json("hello-plugin").as_bytes())
            .unwrap();
        writer.start_file("../../evil.txt", options).unwrap();
        writer.write_all(b"pwned").unwrap();
        writer.finish().unwrap();
    }
    let archive_path = root.path().join("evil.zip");
    tokio::fs::write(&archive_path, &buffer).await.unwrap();

    let plugins_root = root.path().join("plugins");
    let error = stage_plugin_source(PluginSourceInput::LocalArchive(archive_path), &plugins_root)
        .await
        .expect_err("traversal entry must be rejected");
    assert!(matches!(error, PluginError::InvalidManifest(_)));

    let escaped = plugins_root
        .parent()
        .unwrap()
        .parent()
        .map(|p| p.join("evil.txt"));
    if let Some(escaped) = escaped {
        assert!(!escaped.exists());
    }
}

// ---------------------------------------------------------------------
// Symlink / hardlink escape (BLOCKER) — a tar link entry's target is
// attacker-controlled; extraction must reject any link entry outright.
// ---------------------------------------------------------------------

/// Build a `.tar.gz` with some regular entries plus one link (symlink or
/// hardlink) entry whose target the caller controls. Uses the raw
/// `Builder::append_link` — the crafted-archive equivalent of a malicious
/// bundle (real archives never legitimately ship a link into a plugin).
fn build_targz_with_link(
    regular: &[(&str, &[u8])],
    link_type: tar::EntryType,
    link_name: &str,
    link_target: &str,
) -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_bytes);
        for (name, content) in regular {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, name, *content).unwrap();
        }
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(link_type);
        header.set_size(0);
        header.set_mode(0o777);
        builder
            .append_link(&mut header, link_name, link_target)
            .unwrap();
        builder.finish().unwrap();
    }
    let mut gz_bytes = Vec::new();
    {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        let mut encoder = GzEncoder::new(&mut gz_bytes, Compression::default());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap();
    }
    gz_bytes
}

#[tokio::test]
async fn tar_symlink_workflow_entry_is_rejected_no_exfiltration() {
    // Exfiltration chain: a plugin declares workflows/evil.md but ships it as
    // a symlink to a victim secret. If extraction created the symlink,
    // register_workflows' fs::read_to_string would follow it and copy the
    // victim's real content into a plugin-visible location. Extraction must
    // reject the archive before any symlink is written.
    let root = tempfile::tempdir().unwrap();
    let secret = root.path().join("victim-secret.txt");
    tokio::fs::write(&secret, "TOP SECRET").await.unwrap();

    let archive_bytes = build_targz_with_link(
        &[(
            "plugin.json",
            hello_manifest_json("hello-plugin").as_bytes(),
        )],
        tar::EntryType::Symlink,
        "workflows/evil.md",
        secret.to_str().unwrap(),
    );
    let archive_path = root.path().join("evil-symlink.tar.gz");
    tokio::fs::write(&archive_path, &archive_bytes)
        .await
        .unwrap();

    let plugins_root = root.path().join("plugins");
    let error = stage_plugin_source(PluginSourceInput::LocalArchive(archive_path), &plugins_root)
        .await
        .expect_err("a symlink entry must be rejected");
    assert!(matches!(error, PluginError::InvalidManifest(_)));
    assert!(error.to_string().contains("symlink"));

    // Nothing committed; the secret untouched; no symlink materialized.
    assert!(!plugins_root.join("hello-plugin").exists());
    assert_eq!(
        tokio::fs::read_to_string(&secret).await.unwrap(),
        "TOP SECRET"
    );
}

#[tokio::test]
async fn tar_symlink_top_level_dir_entry_is_rejected_no_destruction() {
    // Destruction chain: a single top-level entry that is a symlink to a real
    // directory. If extraction created it, flatten_if_single_subdir (were it
    // to follow the link) would rename the victim dir's real children out and
    // then the failed remove could cascade into deletion. Extraction must
    // reject the archive first.
    let root = tempfile::tempdir().unwrap();
    let victim_dir = root.path().join("victim-dir");
    tokio::fs::create_dir_all(&victim_dir).await.unwrap();
    tokio::fs::write(victim_dir.join("keep.txt"), "precious")
        .await
        .unwrap();

    // Only a single top-level symlink entry (no plugin.json at root), pointing
    // at the victim dir.
    let archive_bytes = build_targz_with_link(
        &[],
        tar::EntryType::Symlink,
        "bundle",
        victim_dir.to_str().unwrap(),
    );
    let archive_path = root.path().join("evil-dirlink.tar.gz");
    tokio::fs::write(&archive_path, &archive_bytes)
        .await
        .unwrap();

    let plugins_root = root.path().join("plugins");
    let error = stage_plugin_source(PluginSourceInput::LocalArchive(archive_path), &plugins_root)
        .await
        .expect_err("a symlink-to-dir entry must be rejected");
    assert!(matches!(error, PluginError::InvalidManifest(_)));

    // The victim dir and its contents are fully intact.
    assert!(victim_dir.join("keep.txt").exists());
    assert_eq!(
        tokio::fs::read_to_string(victim_dir.join("keep.txt"))
            .await
            .unwrap(),
        "precious"
    );
    assert!(!plugins_root.join("hello-plugin").exists());
}

#[tokio::test]
async fn tar_hardlink_entry_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let archive_bytes = build_targz_with_link(
        &[(
            "plugin.json",
            hello_manifest_json("hello-plugin").as_bytes(),
        )],
        tar::EntryType::Link,
        "workflows/evil.md",
        "/etc/hosts",
    );
    let archive_path = root.path().join("evil-hardlink.tar.gz");
    tokio::fs::write(&archive_path, &archive_bytes)
        .await
        .unwrap();

    let plugins_root = root.path().join("plugins");
    let error = stage_plugin_source(PluginSourceInput::LocalArchive(archive_path), &plugins_root)
        .await
        .expect_err("a hardlink entry must be rejected");
    assert!(matches!(error, PluginError::InvalidManifest(_)));
    assert!(error.to_string().contains("hardlink"));
}

#[tokio::test]
async fn zip_symlink_mode_entry_lands_inert_as_a_regular_file() {
    // Zip is NOT affected by the tar link problem: extract_zip_sync writes
    // every entry as a fresh regular file via io::copy, so an entry with
    // symlink unix mode lands as a plain file (the target path as its literal
    // bytes), never a live symlink. Confirm that invariant holds — the zip
    // path deliberately does not need the tar-style link rejection.
    use std::io::{Cursor, Write};

    let root = tempfile::tempdir().unwrap();
    let mut buffer = Vec::new();
    {
        let mut writer = zip::ZipWriter::new(Cursor::new(&mut buffer));
        let options: zip::write::FileOptions<'_, ()> = zip::write::FileOptions::default();
        writer.start_file("plugin.json", options).unwrap();
        writer
            .write_all(hello_manifest_json("hello-plugin").as_bytes())
            .unwrap();
        // S_IFLNK (0o120000) | 0777 — a "symlink" mode, content = a target path.
        let link_options: zip::write::FileOptions<'_, ()> =
            zip::write::FileOptions::default().unix_permissions(0o120777);
        writer.start_file("notes.txt", link_options).unwrap();
        writer.write_all(b"/etc/passwd").unwrap();
        writer.finish().unwrap();
    }
    let archive_path = root.path().join("zip-with-symlink-mode.zip");
    tokio::fs::write(&archive_path, &buffer).await.unwrap();

    let plugins_root = root.path().join("plugins");
    let staged = stage_plugin_source(PluginSourceInput::LocalArchive(archive_path), &plugins_root)
        .await
        .expect("a zip with a symlink-mode entry stages fine (entry lands inert)");

    let landed = staged.plugin_dir.join("notes.txt");
    let meta = tokio::fs::symlink_metadata(&landed).await.unwrap();
    assert!(
        !meta.file_type().is_symlink(),
        "the zip entry must land as a regular file, never a live symlink"
    );
    assert_eq!(
        tokio::fs::read_to_string(&landed).await.unwrap(),
        "/etc/passwd",
        "it holds the target path as inert literal bytes"
    );
    staged.commit().await;
}

// ---------------------------------------------------------------------
// Url
// ---------------------------------------------------------------------

#[tokio::test]
async fn stages_bare_manifest_from_url_with_no_artifact() {
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());
    let staged = stage_plugin_source(PluginSourceInput::Url(url.clone()), &plugins_root)
        .await
        .expect("stage bare manifest url");

    assert_eq!(staged.manifest.id, "hello-plugin");
    assert_eq!(staged.source, PluginSource::Url { url, sha256: None });
    // No skill files were bundled with a bare manifest fetch.
    assert!(!staged.plugin_dir.join("skills").exists());
    staged.commit().await;
}

fn manifest_with_artifact(id: &str, platform: &str, sha256: &str, url: &str) -> String {
    serde_json::json!({
        "id": id,
        "name": "Hello",
        "version": "0.1.0",
        // Narrow the platform gate to just the one platform under test, so
        // manifest validation's artifacts/platform cross-check (every
        // *supported* platform needs an artifact when `${platform_bin}` is
        // used) doesn't demand macos/windows/linux artifacts we didn't stub.
        "platforms": [platform],
        "provides": {
            "mcp_servers": [
                {"id": "srv", "transport": {"type": "stdio", "command": "${platform_bin}"}}
            ]
        },
        "artifacts": {
            platform: {"url": url, "sha256": sha256}
        }
    })
    .to_string()
}

fn current_platform_key() -> &'static str {
    if cfg!(target_os = "macos") {
        "macos"
    } else if cfg!(target_os = "windows") {
        "windows"
    } else {
        "linux"
    }
}

#[tokio::test]
async fn fetches_verifies_and_places_platform_artifact_binary() {
    let server = wiremock::MockServer::start().await;

    let binary_name = if cfg!(target_os = "windows") {
        "hello-plugin.exe"
    } else {
        "hello-plugin"
    };
    let archive_bytes = build_targz(&[(binary_name, b"#!/bin/sh\necho hi\n")]);
    let sha256 = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&archive_bytes);
        hex::encode(hasher.finalize())
    };

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_string(manifest_with_artifact(
                "hello-plugin",
                current_platform_key(),
                &sha256,
                &format!("{}/hello-plugin.tar.gz", server.uri()),
            )),
        )
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/hello-plugin.tar.gz"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(archive_bytes))
        .mount(&server)
        .await;

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());
    let staged = stage_plugin_source(PluginSourceInput::Url(url), &plugins_root)
        .await
        .expect("stage manifest + artifact");

    let expected_bin = staged
        .plugin_dir
        .join("bin")
        .join(current_platform_key())
        .join(binary_name);
    assert!(
        expected_bin.exists(),
        "binary should be placed at {:?}",
        expected_bin
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&expected_bin)
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o111, 0o111, "binary should be executable");
    }

    match staged.source {
        PluginSource::Url {
            sha256: Some(_), ..
        } => {}
        other => panic!("expected Url source with a recorded sha256, got {other:?}"),
    }
    staged.commit().await;
}

#[tokio::test]
async fn artifact_sha256_mismatch_is_rejected_before_unpacking() {
    let server = wiremock::MockServer::start().await;

    let archive_bytes = build_targz(&[("hello-plugin", b"whatever")]);
    let wrong_sha256 = "a".repeat(64);

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_string(manifest_with_artifact(
                "hello-plugin",
                current_platform_key(),
                &wrong_sha256,
                &format!("{}/hello-plugin.tar.gz", server.uri()),
            )),
        )
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/hello-plugin.tar.gz"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(archive_bytes))
        .mount(&server)
        .await;

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());
    let error = stage_plugin_source(PluginSourceInput::Url(url), &plugins_root)
        .await
        .expect_err("sha256 mismatch must be rejected");
    assert!(matches!(error, PluginError::ArtifactVerificationFailed(_)));
    assert!(
        !plugins_root.join("hello-plugin").exists(),
        "a verification failure must never commit anything to plugins_root"
    );
}

// ---------------------------------------------------------------------
// install_plugin_from_source: commit/rollback around a real install() call
// ---------------------------------------------------------------------

/// A trivial installer whose `install()` always fails after the (real)
/// staging step, so tests can assert `install_plugin_from_source` rolls the
/// staged bundle back correctly. `uninstall`/`list` are unused by these tests.
struct AlwaysFailInstaller;

#[async_trait]
impl PluginInstaller for AlwaysFailInstaller {
    async fn install(
        &self,
        manifest: &PluginManifest,
        _plugin_dir: &Path,
        _source: PluginSource,
        _disposition: InstallDisposition,
        _installed_at: DateTime<Utc>,
    ) -> PluginResult<InstalledPlugin> {
        Err(PluginError::Registration(format!(
            "forced failure for {}",
            manifest.id
        )))
    }

    async fn uninstall(&self, _id: &str) -> PluginResult<()> {
        unimplemented!("not exercised by these tests")
    }

    async fn list(&self) -> PluginResult<Vec<InstalledPlugin>> {
        unimplemented!("not exercised by these tests")
    }
}

#[tokio::test]
async fn install_plugin_from_source_rolls_back_new_bundle_on_install_failure() {
    let root = tempfile::tempdir().unwrap();
    let source_dir = root.path().join("source");
    write_hello_plugin_dir(&source_dir, "hello-plugin").await;
    let plugins_root = root.path().join("plugins");

    let error = install_plugin_from_source(
        &AlwaysFailInstaller,
        PluginSourceInput::LocalDir(source_dir),
        &plugins_root,
        InstallDisposition::FailIfInstalled,
    )
    .await
    .expect_err("install always fails in this test");
    assert!(matches!(error, PluginError::Registration(_)));

    assert!(
        !plugins_root.join("hello-plugin").exists(),
        "a failed install must not leave a half-installed bundle behind"
    );
    // No stray staging/backup directories left over either.
    let mut leftovers = tokio::fs::read_dir(&plugins_root).await.unwrap();
    assert!(leftovers.next_entry().await.unwrap().is_none());
}

#[tokio::test]
async fn install_plugin_from_source_restores_previous_bundle_on_upgrade_failure() {
    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");

    // Simulate a pre-existing install at plugins_root/hello-plugin/ with a
    // marker file that must survive a failed "upgrade".
    let existing_dir = plugins_root.join("hello-plugin");
    write_hello_plugin_dir(&existing_dir, "hello-plugin").await;
    tokio::fs::write(existing_dir.join("MARKER"), b"original")
        .await
        .unwrap();

    let new_source_dir = root.path().join("new-source");
    write_hello_plugin_dir(&new_source_dir, "hello-plugin").await;

    let error = install_plugin_from_source(
        &AlwaysFailInstaller,
        PluginSourceInput::LocalDir(new_source_dir),
        &plugins_root,
        InstallDisposition::Upgrade,
    )
    .await
    .expect_err("install always fails in this test");
    assert!(matches!(error, PluginError::Registration(_)));

    assert!(
        existing_dir.join("MARKER").exists(),
        "a failed upgrade must restore the pre-upgrade bundle"
    );
    let mut leftovers = tokio::fs::read_dir(&plugins_root).await.unwrap();
    let mut names: Vec<String> = Vec::new();
    while let Some(entry) = leftovers.next_entry().await.unwrap() {
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    assert_eq!(
        names,
        vec!["hello-plugin".to_string()],
        "no stray staging/backup dirs should remain: {names:?}"
    );
}

#[allow(dead_code)]
fn _use_pathbuf(_p: PathBuf) {}
