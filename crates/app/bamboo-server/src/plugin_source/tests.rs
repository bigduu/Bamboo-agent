use std::path::{Path, PathBuf};

use actix_web::web;
use async_trait::async_trait;
use bamboo_config::{PluginTrustConfig, PluginTrustEnforcement, TrustedKey};
use bamboo_plugin::{
    InstallDisposition, InstalledPlugin, InstalledPlugins, PluginError, PluginInstallStatus,
    PluginInstaller, PluginManifest, PluginResult, PluginSource,
};
use bamboo_plugin_protocol::{
    FILE_CHANGED_SUBSCRIPTION_ID_V1, TOOL_EVENT_PROTOCOL_NAME, TOOL_EVENT_V1_SCHEMA_VERSION,
};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signer, SigningKey};

use super::*;
use crate::app_state::AppState;

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

async fn assert_single_rejected_staging(plugins_root: &Path, context: &str) {
    let names = plugin_root_entry_names(plugins_root).await;
    assert_eq!(names.len(), 1, "{context}: {names:?}");
    assert!(
        names[0].starts_with(".rejected-staging-"),
        "{context}: {names:?}"
    );
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
        &PluginTrustConfig::default(),
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
    let error = stage_plugin_source(
        PluginSourceInput::LocalDir(source_dir),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
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
        &PluginTrustConfig::default(),
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
        &PluginTrustConfig::default(),
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
    let error = stage_plugin_source(
        PluginSourceInput::LocalArchive(archive_path),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
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
    let error = stage_plugin_source(
        PluginSourceInput::LocalArchive(archive_path),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
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
    let error = stage_plugin_source(
        PluginSourceInput::LocalArchive(archive_path),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
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
    let error = stage_plugin_source(
        PluginSourceInput::LocalArchive(archive_path),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
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
    let error = stage_plugin_source(
        PluginSourceInput::LocalArchive(archive_path),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
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
    let staged = stage_plugin_source(
        PluginSourceInput::LocalArchive(archive_path),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
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
//
// Checksum-layer policy under test throughout THIS section (see
// `plugin_source.rs`'s module docs and `fetch_manifest_bundle`):
//   - a correct bundle sha256 -> verified, install proceeds, the VERIFIED
//     hash (not the caller's literal input) is recorded in provenance.
//   - a WRONG bundle sha256 -> `BundleVerificationFailed`, nothing staged.
//   - no sha256 and `allow_unverified: false` (the default) -> refused
//     with `ChecksumRequired`.
//   - no sha256 and `allow_unverified: true` -> proceeds (explicit opt-out).
//
// These tests isolate the CHECKSUM layer specifically, so `url_input` below
// bypasses the other two trust layers (host allowlist, signature) via
// `allow_untrusted_host: true, allow_unsigned: true` — every wiremock server
// here is plain `http://127.0.0.1:<port>` (never `https`, never in
// `trusted_hosts`) and never mounts a `.sig` route, so without the bypass
// every one of these tests would be refused by an EARLIER layer before ever
// reaching the checksum check under test. The host-allowlist and signature
// layers get their own dedicated test sections further down using
// `url_input_full` (which does NOT bypass them).

fn sha256_hex_of(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Checksum-layer-only test helper — see the section docs above for why
/// `allow_untrusted_host`/`allow_unsigned` are hardcoded `true` here.
fn url_input(url: &str, sha256: Option<&str>, allow_unverified: bool) -> PluginSourceInput {
    url_input_full(url, sha256, allow_unverified, true, true)
}

/// Full constructor for `PluginSourceInput::Url` exercising the four
/// per-layer fields — used directly by the host-allowlist/signature test
/// sections, which need per-test control over
/// `allow_untrusted_host`/`allow_unsigned`. Never sets `insecure` (that
/// aggregate gets its own dedicated section/helper further down).
fn url_input_full(
    url: &str,
    sha256: Option<&str>,
    allow_unverified: bool,
    allow_untrusted_host: bool,
    allow_unsigned: bool,
) -> PluginSourceInput {
    PluginSourceInput::Url {
        url: url.to_string(),
        sha256: sha256.map(|s| s.to_string()),
        allow_unverified,
        allow_untrusted_host,
        allow_unsigned,
        insecure: false,
    }
}

/// `--insecure` aggregate test helper: every per-layer flag left `false`,
/// only `insecure` set — proves the AGGREGATE alone (not any individual
/// `allow_*`) is what waives all three layers.
fn url_input_insecure(url: &str, sha256: Option<&str>) -> PluginSourceInput {
    PluginSourceInput::Url {
        url: url.to_string(),
        sha256: sha256.map(|s| s.to_string()),
        allow_unverified: false,
        allow_untrusted_host: false,
        allow_unsigned: false,
        insecure: true,
    }
}

#[tokio::test]
async fn stages_bare_manifest_from_url_with_correct_bundle_sha256() {
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    let bundle_sha256 = sha256_hex_of(manifest_body.as_bytes());
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());
    let staged = stage_plugin_source(
        url_input(&url, Some(&bundle_sha256), false),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect("a correct bundle sha256 must verify and stage successfully");

    assert_eq!(staged.manifest.id, "hello-plugin");
    // The VERIFIED bundle sha256 is what's recorded — this is the
    // provenance/audit trail a re-install or `plugin list` can trust,
    // distinct from any per-platform binary artifact hash.
    assert_eq!(
        staged.source,
        PluginSource::Url {
            url,
            sha256: Some(bundle_sha256),
            allow_unverified: false,
            // `url_input` bypasses the host/signature layers for this
            // checksum-focused test — see the section docs above.
            allow_untrusted_host: true,
            allow_unsigned: true,
            signed_by: None,
            insecure: false,
        }
    );
    // No skill files were bundled with a bare manifest fetch.
    assert!(!staged.plugin_dir.join("skills").exists());
    staged.commit().await;
}

#[tokio::test]
async fn url_install_with_wrong_bundle_sha256_is_rejected_before_unpacking() {
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
    let wrong_sha256 = "b".repeat(64);
    let error = stage_plugin_source(
        url_input(&url, Some(&wrong_sha256), false),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect_err("a bundle sha256 mismatch must be rejected");
    assert!(
        matches!(error, PluginError::BundleVerificationFailed(_)),
        "expected BundleVerificationFailed, got {error:?}"
    );
    assert!(error.to_string().contains(&url));

    // Nothing is committed. The rejected staging entry is retained under an
    // inert quarantine name instead of recursively deleted by path.
    assert!(!plugins_root.join("hello-plugin").exists());
    assert_single_rejected_staging(
        &plugins_root,
        "a rejected bundle checksum mismatch must retain only inert staging",
    )
    .await;
}

#[tokio::test]
async fn url_install_without_checksum_or_allow_unverified_is_refused_after_host_and_signature_layers_pass(
) {
    // This test isolates the CHECKSUM layer (see the section docs above):
    // `url_input` bypasses the host + signature layers, so the bundle fetch
    // and the (absent) `.sig` fetch both happen normally, and the install is
    // refused ONLY at the checksum gate. Unlike the pre-source-trust
    // behavior, the checksum check can no longer run before ANY network
    // access: to know whether a valid signature supersedes it (see
    // `fetch_manifest_bundle`), the bundle must already be downloaded and its
    // `.sig` already checked. The HOST-ALLOWLIST layer is the one that keeps
    // a genuine pre-fetch "zero requests" guarantee now — see
    // `untrusted_host_is_refused_before_any_fetch` below.
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    // Deliberately no `.sig` route mounted — the bundle is genuinely unsigned.

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    let error = stage_plugin_source(
        url_input(&url, None, false),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect_err("no sha256, no allow_unverified, and unsigned must be refused");
    assert!(
        matches!(error, PluginError::ChecksumRequired(_)),
        "expected ChecksumRequired, got {error:?}"
    );
    let message = error.to_string();
    assert!(message.contains("sha256"), "{message}");
    assert!(
        message.contains("allow_unverified") || message.contains("allow-unverified"),
        "{message}"
    );

    // The bundle AND its `.sig` sidecar were both fetched before the checksum
    // gate refused — exactly 2 requests, none of them extraction/install.
    let received = server.received_requests().await.unwrap_or_default();
    assert_eq!(
        received.len(),
        2,
        "expected exactly the bundle GET + the .sig GET attempt, got {received:?}"
    );

    // Nothing is committed; rejected staging is retained inertly.
    assert!(!plugins_root.join("hello-plugin").exists());
    assert_single_rejected_staging(
        &plugins_root,
        "a refused unverified install must retain only inert staging",
    )
    .await;
}

#[tokio::test]
async fn url_install_with_allow_unverified_and_no_sha256_succeeds() {
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
    let staged = stage_plugin_source(
        url_input(&url, None, true),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect("allow_unverified must let an unpinned URL install through");

    assert_eq!(staged.manifest.id, "hello-plugin");
    assert_eq!(
        staged.source,
        PluginSource::Url {
            url,
            sha256: None,
            allow_unverified: true,
            allow_untrusted_host: true,
            allow_unsigned: true,
            signed_by: None,
            insecure: false,
        },
        "an allow_unverified install has no bundle sha256 to record"
    );
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
    let artifact_sha256 = sha256_hex_of(&archive_bytes);

    let manifest_body = manifest_with_artifact(
        "hello-plugin",
        current_platform_key(),
        &artifact_sha256,
        &format!("{}/hello-plugin.tar.gz", server.uri()),
    );
    // The BUNDLE (this manifest.json response) is verified independently of
    // the binary artifact's own sha256 declared inside it — pin it here so
    // this test exercises both checks staying in force together.
    let bundle_sha256 = sha256_hex_of(manifest_body.as_bytes());

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
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
    let staged = stage_plugin_source(
        url_input(&url, Some(&bundle_sha256), false),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
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

    // Provenance records the verified BUNDLE sha256, not the binary
    // artifact's — they're deliberately different hashes here, so this
    // also confirms the two checks aren't accidentally conflated.
    assert_eq!(
        staged.source,
        PluginSource::Url {
            url,
            sha256: Some(bundle_sha256),
            allow_unverified: false,
            allow_untrusted_host: true,
            allow_unsigned: true,
            signed_by: None,
            insecure: false,
        }
    );
    staged.commit().await;
}

#[tokio::test]
async fn artifact_sha256_mismatch_is_rejected_before_unpacking() {
    let server = wiremock::MockServer::start().await;

    let archive_bytes = build_targz(&[("hello-plugin", b"whatever")]);
    let wrong_artifact_sha256 = "a".repeat(64);

    let manifest_body = manifest_with_artifact(
        "hello-plugin",
        current_platform_key(),
        &wrong_artifact_sha256,
        &format!("{}/hello-plugin.tar.gz", server.uri()),
    );
    // Pin the BUNDLE correctly so this test isolates the artifact-level
    // check (the bundle-level check is exercised separately above).
    let bundle_sha256 = sha256_hex_of(manifest_body.as_bytes());

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
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
    let error = stage_plugin_source(
        url_input(&url, Some(&bundle_sha256), false),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect_err("artifact sha256 mismatch must be rejected");
    assert!(matches!(error, PluginError::ArtifactVerificationFailed(_)));
    assert!(
        !plugins_root.join("hello-plugin").exists(),
        "a verification failure must never commit anything to plugins_root"
    );
}

// ---------------------------------------------------------------------
// Source-TRUST layer: host allowlist (layer 1 of 3 — see plugin_source.rs's
// module docs). `PluginTrustConfig::default()`'s `trusted_hosts` is
// `["github.com/bigduu/"]`, so every `wiremock` server here (always plain
// `http://127.0.0.1:<port>`) is untrusted BOTH by host and by scheme.
// ---------------------------------------------------------------------

#[tokio::test]
async fn untrusted_host_is_refused_before_any_fetch() {
    // No mock is mounted at all — if the refusal did NOT happen before the
    // fetch, this test would need a working mock to avoid an error/hang.
    let server = wiremock::MockServer::start().await;
    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    let error = stage_plugin_source(
        url_input_full(&url, None, false, false, false),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect_err("a host outside trusted_hosts must be refused");
    assert!(
        matches!(error, PluginError::UntrustedHost(_)),
        "expected UntrustedHost, got {error:?}"
    );
    let message = error.to_string();
    assert!(message.contains("trusted_hosts"), "{message}");
    assert!(message.contains("allow-untrusted-host") || message.contains("allow_untrusted_host"));

    // The core assertion (mirrors the checksum-layer "no fetch" test above):
    // confirm the server genuinely never received a request for it.
    let received = server.received_requests().await;
    assert_eq!(
        received.map(|requests| requests.len()),
        Some(0),
        "refusing an untrusted-host URL install must happen BEFORE the URL is ever fetched"
    );

    assert!(!plugins_root.join("hello-plugin").exists());
    assert_single_rejected_staging(
        &plugins_root,
        "a refused untrusted-host install must retain only inert staging",
    )
    .await;
}

#[tokio::test]
async fn allow_untrusted_host_bypasses_the_host_allowlist() {
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

    // allow_untrusted_host bypasses layer 1; allow_unverified + allow_unsigned
    // bypass layers 3/2 too so this test isolates the host-allowlist bypass
    // specifically (its own layers are covered by their dedicated sections).
    let staged = stage_plugin_source(
        url_input_full(&url, None, true, true, true),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect("allow_untrusted_host must let an untrusted-host URL install through");

    assert_eq!(staged.manifest.id, "hello-plugin");
    assert_eq!(
        staged.source,
        PluginSource::Url {
            url,
            sha256: None,
            allow_unverified: true,
            allow_untrusted_host: true,
            allow_unsigned: true,
            signed_by: None,
            insecure: false,
        }
    );
    staged.commit().await;
}

// ---------------------------------------------------------------------
// Source-TRUST layer: ed25519 publisher signature (layer 2 of 3). Test
// keypairs use `SigningKey::from_bytes` (a fixed 32-byte "secret") rather
// than a CSPRNG — deterministic and sufficient for exercising the verify
// path; nova's REAL signing key is never used or needed here.
// ---------------------------------------------------------------------

fn test_signing_key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn trusted_key_for(label: &str, signing_key: &SigningKey) -> TrustedKey {
    TrustedKey {
        label: label.to_string(),
        algorithm: "ed25519".to_string(),
        public_key: hex::encode(signing_key.verifying_key().to_bytes()),
    }
}

#[tokio::test]
async fn valid_signature_from_a_trusted_key_verifies_and_supersedes_the_checksum_requirement() {
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    let signing_key = test_signing_key(7);
    let signature = signing_key.sign(manifest_body.as_bytes());
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body.clone()))
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json.sig"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_string(hex::encode(signature.to_bytes())),
        )
        .mount(&server)
        .await;

    let trust = PluginTrustConfig {
        trusted_hosts: Vec::new(),
        trusted_keys: vec![trusted_key_for("test-key", &signing_key)],
        enforcement: PluginTrustEnforcement::Strict,
    };
    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    // No `sha256` AND `allow_unverified: false` — a signed+verified bundle
    // must NOT also require a pasted checksum (the precedence rule).
    let staged = stage_plugin_source(
        url_input_full(&url, None, false, true, false),
        &plugins_root,
        &trust,
    )
    .await
    .expect("a valid signature from a trusted key must satisfy the checksum requirement too");

    assert_eq!(staged.manifest.id, "hello-plugin");
    assert_eq!(
        staged.source,
        PluginSource::Url {
            url,
            sha256: None,
            allow_unverified: false,
            allow_untrusted_host: true,
            allow_unsigned: false,
            signed_by: Some("test-key".to_string()),
            insecure: false,
        },
        "a verified signature is recorded in provenance and needs no bundle sha256"
    );
    staged.commit().await;
}

#[tokio::test]
async fn absent_signature_is_refused_with_unsigned_or_untrusted_signature() {
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    // Deliberately no `.sig` route mounted.

    let signing_key = test_signing_key(9);
    let trust = PluginTrustConfig {
        trusted_hosts: Vec::new(),
        trusted_keys: vec![trusted_key_for("test-key", &signing_key)],
        enforcement: PluginTrustEnforcement::Strict,
    };
    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    let error = stage_plugin_source(
        url_input_full(&url, None, true, true, false),
        &plugins_root,
        &trust,
    )
    .await
    .expect_err("an absent .sig must be refused unless allow_unsigned");
    assert!(
        matches!(error, PluginError::UnsignedOrUntrustedSignature(_)),
        "expected UnsignedOrUntrustedSignature, got {error:?}"
    );
    assert!(
        error.to_string().contains("allow-unsigned")
            || error.to_string().contains("allow_unsigned")
    );
    assert!(!plugins_root.join("hello-plugin").exists());
}

#[tokio::test]
async fn signature_from_a_non_trusted_key_is_refused() {
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    let signing_key = test_signing_key(11);
    let other_key = test_signing_key(12);
    let signature = signing_key.sign(manifest_body.as_bytes());
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body.clone()))
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json.sig"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_string(hex::encode(signature.to_bytes())),
        )
        .mount(&server)
        .await;

    // `trusted_keys` only has `other_key`'s pubkey — the bundle was signed by
    // `signing_key`, a DIFFERENT (non-trusted) key.
    let trust = PluginTrustConfig {
        trusted_hosts: Vec::new(),
        trusted_keys: vec![trusted_key_for("other-key", &other_key)],
        enforcement: PluginTrustEnforcement::Strict,
    };
    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    let error = stage_plugin_source(
        url_input_full(&url, None, true, true, false),
        &plugins_root,
        &trust,
    )
    .await
    .expect_err("a signature from a non-trusted key must be refused");
    assert!(
        matches!(error, PluginError::UnsignedOrUntrustedSignature(_)),
        "expected UnsignedOrUntrustedSignature, got {error:?}"
    );
    assert!(!plugins_root.join("hello-plugin").exists());
}

#[tokio::test]
async fn allow_unsigned_bypasses_the_signature_check_but_not_the_checksum_layer() {
    // Precedence check: `allow_unsigned` only waives the SIGNATURE layer's
    // own refusal. It grants no credit toward the checksum layer the way a
    // genuinely verified signature does (see the "supersedes" test above) —
    // an unsigned install still needs its own `sha256`/`allow_unverified`.
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

    // allow_unsigned: true, but neither sha256 nor allow_unverified given.
    let error = stage_plugin_source(
        url_input_full(&url, None, false, true, true),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect_err("allow_unsigned alone must not also waive the checksum requirement");
    assert!(
        matches!(error, PluginError::ChecksumRequired(_)),
        "expected ChecksumRequired (not UnsignedOrUntrustedSignature), got {error:?}"
    );

    // Now also give allow_unverified: true — fully unsigned AND unverified,
    // both explicitly accepted, must succeed with `signed_by: None`.
    let staged = stage_plugin_source(
        url_input_full(&url, None, true, true, true),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect("allow_unsigned + allow_unverified together must let the install through");
    assert_eq!(
        staged.source,
        PluginSource::Url {
            url,
            sha256: None,
            allow_unverified: true,
            allow_untrusted_host: true,
            allow_unsigned: true,
            signed_by: None,
            insecure: false,
        }
    );
    staged.commit().await;
}

// ---------------------------------------------------------------------
// Issue #479 §4 / open question 6: a `provides.services`-declaring manifest
// is NEVER installable from a URL source unless its bundle is
// signature-verified — `allow_unsigned`/`--insecure`/
// `plugin_trust.enforcement: off` are all NOT honoured for this artifact
// kind (unlike every other capability). See `stage_into`'s services check.
// ---------------------------------------------------------------------

fn service_manifest_json(id: &str) -> String {
    serde_json::json!({
        "id": id,
        "name": "Service Plugin",
        "version": "0.1.0",
        "provides": {
            "services": [
                {"id": "svc", "command": "${platform_bin}"}
            ]
        }
    })
    .to_string()
}

#[tokio::test]
async fn services_manifest_with_allow_unsigned_is_refused_even_with_every_other_bypass() {
    let server = wiremock::MockServer::start().await;
    let manifest_body = service_manifest_json("svc-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    // Deliberately no `.sig` route mounted — this bundle is unsigned.

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    // Every per-layer bypass set AND allow_unverified — proves the refusal
    // is specific to `provides.services`, not just a re-run of the ordinary
    // signature-required check (which `allow_unsigned` alone would satisfy).
    let error = stage_plugin_source(
        url_input_full(&url, None, true, true, true),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect_err("a services-declaring manifest must refuse an unsigned URL install");
    assert!(
        matches!(error, PluginError::UnsignedOrUntrustedSignature(_)),
        "expected UnsignedOrUntrustedSignature, got {error:?}"
    );
    assert!(error.to_string().contains("provides.services"));
    assert!(!plugins_root.join("svc-plugin").exists());
}

#[tokio::test]
async fn services_manifest_with_insecure_aggregate_is_refused() {
    // Same as above but via the `--insecure` aggregate rather than the
    // individual `allow_unsigned` flag — the issue calls out BOTH must be
    // refused for a services-declaring manifest.
    let server = wiremock::MockServer::start().await;
    let manifest_body = service_manifest_json("svc-plugin-2");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    let error = stage_plugin_source(
        url_input_insecure(&url, None),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect_err("a services-declaring manifest must refuse `--insecure` too");
    assert!(matches!(
        error,
        PluginError::UnsignedOrUntrustedSignature(_)
    ));
}

#[tokio::test]
async fn services_manifest_with_a_valid_trusted_signature_is_accepted() {
    // The refusal is specifically about being UNSIGNED — a genuinely
    // signature-verified services bundle must install normally.
    let server = wiremock::MockServer::start().await;
    let manifest_body = service_manifest_json("svc-plugin-signed");
    let signing_key = test_signing_key(42);
    let signature = signing_key.sign(manifest_body.as_bytes());
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body.clone()))
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json.sig"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_string(hex::encode(signature.to_bytes())),
        )
        .mount(&server)
        .await;

    let trust = PluginTrustConfig {
        trusted_hosts: Vec::new(),
        trusted_keys: vec![trusted_key_for("test-key", &signing_key)],
        enforcement: PluginTrustEnforcement::Strict,
    };
    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    let staged = stage_plugin_source(
        url_input_full(&url, None, false, true, false),
        &plugins_root,
        &trust,
    )
    .await
    .expect("a signature-verified services manifest must install normally");
    assert_eq!(staged.manifest.id, "svc-plugin-signed");
    staged.commit().await;
}

// ---------------------------------------------------------------------
// Redirect policy (BLOCKER 1 fix): the host allowlist only vets the FIRST
// hop's `<host><path>`. Whether it's safe to transparently follow an HTTP
// redirect to a DIFFERENT host depends on whether the downloaded bytes will
// be cryptographically authenticated afterward (signature and/or checksum).
// See `http_client_following_redirects` / `http_client_no_redirects` /
// `download_bytes` in `plugin_source.rs`.
// ---------------------------------------------------------------------

#[tokio::test]
async fn host_only_trust_install_refuses_a_redirect_instead_of_following_it() {
    // allow_unsigned + allow_unverified, no sha256: NEITHER crypto layer will
    // authenticate the downloaded bytes, so the host allowlist would be the
    // SOLE control on where they actually come from — which a transparent
    // redirect defeats. `allow_untrusted_host: true` here isolates the
    // REDIRECT-POLICY behavior specifically from the host-allowlist layer's
    // own check, exactly like the checksum/signature sections above isolate
    // theirs (this wiremock server is plain http, never in `trusted_hosts`
    // anyway, so it needs the bypass just to reach the fetch at all).
    let server = wiremock::MockServer::start().await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(
            wiremock::ResponseTemplate::new(302)
                .insert_header("Location", format!("{}/elsewhere.json", server.uri())),
        )
        .mount(&server)
        .await;
    // Deliberately NOT mounting `/elsewhere.json` — if the redirect were
    // followed despite the no-redirect policy, wiremock itself would 404
    // rather than this specific refusal firing; either way the install must
    // not succeed, but asserting the exact error keeps this test honest
    // about which failure mode it's covering.

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    let error = stage_plugin_source(
        url_input_full(&url, None, true, true, true),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect_err(
        "a redirect must be refused when neither a signature nor a checksum will authenticate \
         the downloaded bytes",
    );
    // A clean, dedicated trust refusal (maps to a 403, see the HTTP error
    // status map) — NOT a generic `Registration` (500 "internal error"),
    // which would make a security-policy refusal look like a server bug. A
    // regression back to `Registration`/500 fails here.
    assert!(
        matches!(error, PluginError::RedirectRefused(_)),
        "expected a RedirectRefused trust refusal for the un-followed redirect, got {error:?}"
    );
    // The message must be actionable: it names the redirect target and tells
    // the user the three ways forward (canonical URL, signature/sha256, or
    // trusting the target host).
    let message = error.to_string();
    assert!(message.contains("/elsewhere.json"), "{message}");
    assert!(
        message.contains("--sha256") || message.contains("sha256"),
        "{message}"
    );
    assert!(message.contains("trusted_hosts"), "{message}");
    assert!(!plugins_root.join("hello-plugin").exists());
}

#[tokio::test]
async fn checksummed_install_still_follows_a_redirect() {
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    let bundle_sha256 = sha256_hex_of(manifest_body.as_bytes());

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(
            wiremock::ResponseTemplate::new(302)
                .insert_header("Location", format!("{}/actual-bundle.json", server.uri())),
        )
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/actual-bundle.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body.clone()))
        .mount(&server)
        .await;

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    // `url_input` bypasses the host/signature layers (see the checksum
    // section's docs above) so this test isolates: a sha256 pin means the
    // bytes are authenticated regardless of which host actually served them,
    // so the redirect is followed and the install still succeeds.
    let staged = stage_plugin_source(
        url_input(&url, Some(&bundle_sha256), false),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect("a checksum-pinned install must still follow a redirect to the real bytes");

    assert_eq!(staged.manifest.id, "hello-plugin");
    staged.commit().await;
}

#[tokio::test]
async fn signed_install_still_follows_a_redirect() {
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    let signing_key = test_signing_key(11);
    let signature = signing_key.sign(manifest_body.as_bytes());
    let trust = PluginTrustConfig {
        trusted_hosts: PluginTrustConfig::default().trusted_hosts,
        trusted_keys: vec![trusted_key_for("test key", &signing_key)],
        enforcement: PluginTrustEnforcement::Strict,
    };

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(
            wiremock::ResponseTemplate::new(302)
                .insert_header("Location", format!("{}/actual-bundle.json", server.uri())),
        )
        .mount(&server)
        .await;
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/actual-bundle.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body.clone()))
        .mount(&server)
        .await;
    // The `.sig` sidecar is derived from the ORIGINAL (pre-redirect) url, not
    // the redirect target — see `fetch_and_verify_signature`'s doc comment —
    // and is signed over the bytes actually downloaded (the redirected-to
    // `manifest_body`).
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json.sig"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_string(hex::encode(signature.to_bytes())),
        )
        .mount(&server)
        .await;

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let url = format!("{}/plugin.json", server.uri());

    // allow_untrusted_host: true isolates the redirect-policy behavior from
    // the host-allowlist layer (same convention as the other sections);
    // allow_unsigned: false so the signature is actually REQUIRED here.
    let staged = stage_plugin_source(
        url_input_full(&url, None, true, true, false),
        &plugins_root,
        &trust,
    )
    .await
    .expect("a signature-verified install must still follow a redirect to the real bytes");

    assert_eq!(staged.manifest.id, "hello-plugin");
    assert_eq!(
        staged.source,
        PluginSource::Url {
            url,
            sha256: None,
            allow_unverified: true,
            allow_untrusted_host: true,
            allow_unsigned: false,
            signed_by: Some("test key".to_string()),
            insecure: false,
        }
    );
    staged.commit().await;
}

// ---------------------------------------------------------------------
// `--insecure` / `plugin_trust.enforcement`: the convenience aggregate that
// waives all three trust layers at once, per-install (`PluginSourceInput::
// Url::insecure`) or persistently (`PluginTrustConfig::enforcement ==
// PluginTrustEnforcement::Off`). A supplied `sha256` is still verified in
// either case — the aggregate only turns OFF checks the caller didn't
// otherwise opt into.
// ---------------------------------------------------------------------

#[tokio::test]
async fn insecure_flag_bypasses_untrusted_host_unsigned_and_no_checksum_all_at_once() {
    // An install that would be refused on ALL THREE layers under Strict
    // (untrusted host, no `.sig` mounted, no `sha256`/`allow_unverified`)
    // must succeed once `insecure: true` is set — proving the aggregate
    // really does waive all three, not just one.
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let staged = stage_plugin_source(
        url_input_insecure(&url, None),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect("--insecure must waive the host/signature/checksum layers together");

    assert_eq!(staged.manifest.id, "hello-plugin");
    assert_eq!(
        staged.source,
        PluginSource::Url {
            url,
            sha256: None,
            allow_unverified: false,
            allow_untrusted_host: false,
            allow_unsigned: false,
            signed_by: None,
            // The AGGREGATE is what gets recorded, distinguishing this from
            // an install where the three `allow_*` flags happened to be set
            // individually (those stay `false` here — only `insecure` was
            // ever set).
            insecure: true,
        }
    );
    staged.commit().await;
}

#[tokio::test]
async fn insecure_flag_still_honors_an_explicit_wrong_sha256() {
    // Precedence check: `--insecure` never downgrades a checksum the caller
    // actually supplied — a WRONG `sha256` must still refuse the install,
    // exactly as it would without `--insecure`.
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());
    let wrong_sha256 = "c".repeat(64);

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let error = stage_plugin_source(
        url_input_insecure(&url, Some(&wrong_sha256)),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect_err("--insecure must not waive a caller-supplied, mismatched sha256");
    assert!(
        matches!(error, PluginError::BundleVerificationFailed(_)),
        "{error:?}"
    );

    // Nothing was left under plugins_root.
    assert!(!plugins_root.join("hello-plugin").exists());
}

#[tokio::test]
async fn insecure_flag_still_verifies_a_correct_explicit_sha256() {
    // The mirror image of the above: a CORRECT `sha256` alongside `--insecure`
    // is honored too (not merely tolerated) — it's still what gets recorded
    // in provenance.
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    let bundle_sha256 = sha256_hex_of(manifest_body.as_bytes());
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let staged = stage_plugin_source(
        url_input_insecure(&url, Some(&bundle_sha256)),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect("a correct sha256 alongside --insecure must still verify and stage");
    assert_eq!(
        staged.source,
        PluginSource::Url {
            url,
            sha256: Some(bundle_sha256),
            allow_unverified: false,
            allow_untrusted_host: false,
            allow_unsigned: false,
            signed_by: None,
            insecure: true,
        }
    );
    staged.commit().await;
}

#[tokio::test]
async fn enforcement_off_bypasses_all_layers_with_no_per_install_flags() {
    // The PERSISTENT, config-level form: `plugin_trust.enforcement: off`
    // must waive all three layers for a request that sets NONE of the
    // per-install flags (not even `insecure`) — proving the config alone
    // drives the aggregate, no per-call opt-in required.
    let server = wiremock::MockServer::start().await;
    let manifest_body = hello_manifest_json("hello-plugin");
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/plugin.json"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string(manifest_body))
        .mount(&server)
        .await;
    let url = format!("{}/plugin.json", server.uri());

    let trust = PluginTrustConfig {
        enforcement: PluginTrustEnforcement::Off,
        ..PluginTrustConfig::default()
    };

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    // Every trust flag left at its default (false) — no `--insecure` either.
    let input = PluginSourceInput::Url {
        url: url.clone(),
        sha256: None,
        allow_unverified: false,
        allow_untrusted_host: false,
        allow_unsigned: false,
        insecure: false,
    };
    let staged = stage_plugin_source(input, &plugins_root, &trust)
        .await
        .expect("plugin_trust.enforcement: off must waive all layers with no per-call flags");

    assert_eq!(staged.manifest.id, "hello-plugin");
    assert_eq!(
        staged.source,
        PluginSource::Url {
            url,
            sha256: None,
            allow_unverified: false,
            allow_untrusted_host: false,
            allow_unsigned: false,
            signed_by: None,
            // Recorded true even though NEITHER the request's `insecure` flag
            // NOR any individual `allow_*` was set — the config drove it.
            insecure: true,
        }
    );
    staged.commit().await;
}

#[tokio::test]
async fn enforcement_strict_is_the_default_and_still_refuses_an_untrusted_host() {
    // Regression guard: `PluginTrustConfig::default()` (what a fresh/absent
    // `plugin_trust` config section deserializes to) must remain Strict — an
    // untrusted-host URL with no flags at all is refused exactly as before
    // this feature existed.
    assert_eq!(
        PluginTrustConfig::default().enforcement,
        PluginTrustEnforcement::Strict
    );
    assert!(!PluginTrustConfig::default().enforcement_is_off());

    let server = wiremock::MockServer::start().await;
    let url = format!("{}/plugin.json", server.uri());

    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let input = PluginSourceInput::Url {
        url,
        sha256: None,
        allow_unverified: false,
        allow_untrusted_host: false,
        allow_unsigned: false,
        insecure: false,
    };
    let error = stage_plugin_source(input, &plugins_root, &PluginTrustConfig::default())
        .await
        .expect_err("Strict (the default) must still refuse an untrusted host with no flags");
    assert!(matches!(error, PluginError::UntrustedHost(_)));

    // The refusal happened before the URL was ever fetched.
    let received = server.received_requests().await;
    assert_eq!(received.map(|r| r.len()), Some(0));
}

// ---------------------------------------------------------------------
// Local sources are UNAFFECTED by the host-allowlist/signature layers — those
// layers are about remote fetch; a local path is already the user's own file.
// ---------------------------------------------------------------------

#[tokio::test]
async fn local_dir_install_ignores_a_hostile_trust_config() {
    // An intentionally empty allowlist/keyset (the most restrictive possible
    // `PluginTrustConfig`) must have ZERO effect on a local install — proves
    // `stage_into`'s LocalDir arm never consults `trust` at all.
    let hostile_trust = PluginTrustConfig {
        trusted_hosts: Vec::new(),
        trusted_keys: Vec::new(),
        enforcement: PluginTrustEnforcement::Strict,
    };
    let root = tempfile::tempdir().unwrap();
    let source_dir = root.path().join("source");
    write_hello_plugin_dir(&source_dir, "hello-plugin").await;
    let plugins_root = root.path().join("plugins");

    let staged = stage_plugin_source(
        PluginSourceInput::LocalDir(source_dir.clone()),
        &plugins_root,
        &hostile_trust,
    )
    .await
    .expect("a local install must never consult the host/signature trust config");
    assert_eq!(staged.manifest.id, "hello-plugin");
    assert_eq!(staged.source, PluginSource::LocalDir { path: source_dir });
    staged.commit().await;
}

// ---------------------------------------------------------------------
// Prepared bundle activation is a rename-only sibling transaction. A failed
// candidate publication must never merge-copy into, delete, or overwrite a
// destination that appeared after preflight.
// ---------------------------------------------------------------------

async fn prepared_upgrade_fixture(root: &Path) -> (PreparedPlugin, PathBuf, PathBuf, PathBuf) {
    let plugins_root = root.join("plugins");
    let live_dir = plugins_root.join("hello-plugin");
    write_hello_plugin_dir(&live_dir, "hello-plugin").await;
    tokio::fs::write(live_dir.join("OLD_MARKER"), b"old-bundle")
        .await
        .unwrap();

    let source_dir = root.join("new-source");
    write_hello_plugin_dir(&source_dir, "hello-plugin").await;
    tokio::fs::write(source_dir.join("NEW_MARKER"), b"new-bundle")
        .await
        .unwrap();
    let prepared = prepare_plugin_source(
        PluginSourceInput::LocalDir(source_dir.clone()),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .expect("prepare candidate");
    (prepared, plugins_root, live_dir, source_dir)
}

async fn plugin_root_entry_names(plugins_root: &Path) -> Vec<String> {
    let mut entries = tokio::fs::read_dir(plugins_root).await.unwrap();
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    names
}

fn service_plugin_manifest_json(version: &str) -> String {
    serde_json::json!({
        "id": "svc-plugin",
        "name": "Service Plugin",
        "version": version,
        "provides": {
            "services": [{"id": "svc", "command": "${platform_bin}"}]
        }
    })
    .to_string()
}

fn event_sink_service_plugin_manifest_json(version: &str, sink_id: &str) -> String {
    serde_json::json!({
        "id": "svc-plugin",
        "name": "Service Event Sink Plugin",
        "version": version,
        "provides": {
            "services": [{"id": "svc", "command": "${platform_bin}"}],
            "event_sinks": [{
                "id": sink_id,
                "service_id": "svc",
                "protocol": {
                    "name": TOOL_EVENT_PROTOCOL_NAME,
                    "version": TOOL_EVENT_V1_SCHEMA_VERSION
                },
                "subscriptions": [{"id": FILE_CHANGED_SUBSCRIPTION_ID_V1}],
                "requested_permissions": ["metadata"]
            }]
        }
    })
    .to_string()
}

async fn server_upgrade_fixture(
    root: &Path,
) -> (
    web::Data<AppState>,
    ServerPluginInstaller,
    PathBuf,
    PathBuf,
    PathBuf,
) {
    let data_dir = root.join("bamboo-home");
    let state = AppState::new(data_dir.clone())
        .await
        .expect("app state should initialize");
    state.wait_for_boot_reconcile_services().await;
    let state = web::Data::new(state);
    let installer = ServerPluginInstaller::new(state.clone());
    let plugins_root = data_dir.join("plugins");
    let live_dir = plugins_root.join("svc-plugin");
    tokio::fs::create_dir_all(&live_dir).await.unwrap();
    let old_manifest_json = service_plugin_manifest_json("1.0.0");
    tokio::fs::write(live_dir.join("plugin.json"), &old_manifest_json)
        .await
        .unwrap();
    tokio::fs::write(live_dir.join("OLD_MARKER"), b"old-bundle")
        .await
        .unwrap();
    let old_manifest = PluginManifest::parse_str(&old_manifest_json).unwrap();
    installer
        .install(
            &old_manifest,
            &live_dir,
            PluginSource::LocalDir {
                path: live_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("install old service plugin");
    assert!(state.service_manager.is_running("svc"));

    let source_dir = root.join("new-source");
    tokio::fs::create_dir_all(&source_dir).await.unwrap();
    tokio::fs::write(
        source_dir.join("plugin.json"),
        service_plugin_manifest_json("2.0.0"),
    )
    .await
    .unwrap();
    tokio::fs::write(source_dir.join("NEW_MARKER"), b"new-bundle")
        .await
        .unwrap();

    (state, installer, plugins_root, live_dir, source_dir)
}

async fn server_final_commit_upgrade_fixture(
    root: &Path,
) -> (
    web::Data<AppState>,
    ServerPluginInstaller,
    PathBuf,
    PathBuf,
    PathBuf,
    InstalledPlugin,
) {
    let data_dir = root.join("bamboo-home");
    let state = AppState::new(data_dir.clone())
        .await
        .expect("app state should initialize");
    state.wait_for_boot_reconcile_services().await;
    let state = web::Data::new(state);
    let installer = ServerPluginInstaller::new(state.clone());
    let plugins_root = data_dir.join("plugins");
    let live_dir = plugins_root.join("svc-plugin");
    tokio::fs::create_dir_all(&live_dir).await.unwrap();
    let old_manifest_json = event_sink_service_plugin_manifest_json("1.0.0", "old-sink");
    tokio::fs::write(live_dir.join("plugin.json"), &old_manifest_json)
        .await
        .unwrap();
    tokio::fs::write(live_dir.join("OLD_MARKER"), b"old-bundle")
        .await
        .unwrap();
    let old_manifest = PluginManifest::parse_str(&old_manifest_json).unwrap();
    installer
        .install(
            &old_manifest,
            &live_dir,
            PluginSource::LocalDir {
                path: live_dir.clone(),
            },
            InstallDisposition::FailIfInstalled,
            Utc::now(),
        )
        .await
        .expect("install old event-sink service plugin");
    assert!(state.service_manager.is_running("svc"));
    let previous = InstalledPlugins::load(&plugins_root.join("installed.json"))
        .await
        .unwrap()
        .get_unique("svc-plugin")
        .unwrap()
        .unwrap()
        .clone();

    let source_dir = root.join("new-source");
    tokio::fs::create_dir_all(&source_dir).await.unwrap();
    tokio::fs::write(
        source_dir.join("plugin.json"),
        event_sink_service_plugin_manifest_json("2.0.0", "new-sink"),
    )
    .await
    .unwrap();
    tokio::fs::write(source_dir.join("NEW_MARKER"), b"new-bundle")
        .await
        .unwrap();

    (
        state,
        installer,
        plugins_root,
        live_dir,
        source_dir,
        previous,
    )
}

#[tokio::test]
async fn discard_preserves_an_unknown_staging_replacement() {
    let root = tempfile::tempdir().unwrap();
    let (prepared, plugins_root, live_dir, _) = prepared_upgrade_fixture(root.path()).await;
    let prepared_dir = prepared.prepared_dir.clone();
    let displaced_candidate = plugins_root.join(format!(
        ".fault-displaced-prepared-{}",
        uuid::Uuid::new_v4()
    ));
    rename_noreplace(&prepared_dir, &displaced_candidate).unwrap();
    std::fs::create_dir(&prepared_dir).unwrap();
    std::fs::write(prepared_dir.join("UNKNOWN_MARKER"), b"unknown-owned").unwrap();

    prepared.discard().await;

    assert_eq!(
        tokio::fs::read_to_string(prepared_dir.join("UNKNOWN_MARKER"))
            .await
            .unwrap(),
        "unknown-owned",
        "discard must put an unknown replacement back without deleting it"
    );
    assert!(displaced_candidate.join("NEW_MARKER").exists());
    assert!(live_dir.join("OLD_MARKER").exists());
}

#[tokio::test]
async fn activation_cleanup_preserves_an_unknown_candidate_replacement() {
    let root = tempfile::tempdir().unwrap();
    let (prepared, plugins_root, live_dir, _) = prepared_upgrade_fixture(root.path()).await;
    let prepared_dir = prepared.prepared_dir.clone();
    let displaced_candidate = plugins_root.join(format!(
        ".fault-displaced-prepared-{}",
        uuid::Uuid::new_v4()
    ));
    rename_noreplace(&prepared_dir, &displaced_candidate).unwrap();
    std::fs::create_dir(&prepared_dir).unwrap();
    std::fs::write(prepared_dir.join("UNKNOWN_MARKER"), b"unknown-owned").unwrap();

    let error = prepared
        .activate()
        .await
        .expect_err("candidate identity replacement must fail activation");
    assert!(error
        .into_plugin_error()
        .to_string()
        .contains("manual bundle recovery is required"));
    assert_eq!(
        tokio::fs::read_to_string(prepared_dir.join("UNKNOWN_MARKER"))
            .await
            .unwrap(),
        "unknown-owned"
    );
    assert!(displaced_candidate.join("NEW_MARKER").exists());
    assert!(live_dir.join("OLD_MARKER").exists());
    let names = plugin_root_entry_names(&plugins_root).await;
    assert!(
        names
            .iter()
            .all(|name| !name.starts_with(".backup-hello-plugin-")),
        "candidate identity must be checked before moving the old live bundle: {names:?}"
    );
}

#[tokio::test]
async fn fresh_activation_rejects_a_destination_that_appears_after_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let plugins_root = root.path().join("plugins");
    let source_dir = root.path().join("source");
    write_hello_plugin_dir(&source_dir, "hello-plugin").await;
    let prepared = prepare_plugin_source(
        PluginSourceInput::LocalDir(source_dir),
        &plugins_root,
        &PluginTrustConfig::default(),
    )
    .await
    .unwrap();

    let error = prepared
        .activate_with_fault(ActivationFault::CreateDestinationDirectory)
        .await
        .expect_err("fresh activation must fail closed when its destination appears");
    assert!(error
        .into_plugin_error()
        .to_string()
        .contains("manual bundle recovery is required"));
    assert_eq!(
        tokio::fs::read_to_string(plugins_root.join("hello-plugin").join("RACE_MARKER"))
            .await
            .unwrap(),
        "race-owned"
    );
    let names = plugin_root_entry_names(&plugins_root).await;
    let candidate = names
        .iter()
        .find(|name| name.starts_with(".candidate-hello-plugin-"))
        .expect("fresh candidate must be retained rather than deleted");
    assert!(plugins_root.join(candidate).join("plugin.json").exists());
}

#[tokio::test]
async fn commit_retirement_preserves_an_unknown_backup_replacement() {
    let root = tempfile::tempdir().unwrap();
    let (prepared, plugins_root, live_dir, _) = prepared_upgrade_fixture(root.path()).await;
    let expected_live = prepared.capture_expected_live().unwrap();
    let staged = prepared
        .activate_inner(expected_live, ActivationFault::None)
        .await
        .expect("activate prepared upgrade");
    let backup_path = staged.backup.as_ref().unwrap().path.clone();
    let displaced_backup =
        plugins_root.join(format!(".fault-displaced-retired-{}", uuid::Uuid::new_v4()));
    rename_noreplace(&backup_path, &displaced_backup).unwrap();
    std::fs::create_dir(&backup_path).unwrap();
    std::fs::write(backup_path.join("UNKNOWN_MARKER"), b"unknown-owned").unwrap();

    staged.commit().await;

    assert_eq!(
        tokio::fs::read_to_string(backup_path.join("UNKNOWN_MARKER"))
            .await
            .unwrap(),
        "unknown-owned",
        "commit retirement must preserve an unknown replacement"
    );
    assert!(displaced_backup.join("OLD_MARKER").exists());
    assert!(live_dir.join("NEW_MARKER").exists());
}

#[tokio::test]
async fn activation_second_rename_failure_restores_old_bundle_without_copying_candidate() {
    let root = tempfile::tempdir().unwrap();
    let (prepared, plugins_root, live_dir, _) = prepared_upgrade_fixture(root.path()).await;

    let error = prepared
        .activate_with_fault(ActivationFault::FailCandidateRename)
        .await
        .expect_err("injected candidate rename must fail");
    assert!(
        error.recovery.is_reconciled(),
        "the previous bundle must be identity-verified at the live path"
    );
    assert_eq!(
        tokio::fs::read_to_string(live_dir.join("OLD_MARKER"))
            .await
            .unwrap(),
        "old-bundle"
    );
    assert!(
        !live_dir.join("NEW_MARKER").exists(),
        "candidate bytes must never be merge-copied into the restored bundle"
    );
    let names = plugin_root_entry_names(&plugins_root).await;
    assert!(names.iter().any(|name| name == "hello-plugin"));
    let candidate = names
        .iter()
        .find(|name| name.starts_with(".candidate-hello-plugin-"))
        .expect("failed candidate must be retained instead of recursively deleted");
    assert!(plugins_root.join(candidate).join("NEW_MARKER").exists());
}

#[tokio::test]
async fn activation_destination_race_is_preserved_and_old_bundle_stays_in_backup() {
    let root = tempfile::tempdir().unwrap();
    let (prepared, plugins_root, live_dir, _) = prepared_upgrade_fixture(root.path()).await;

    let error = prepared
        .activate_with_fault(ActivationFault::CreateDestinationDirectory)
        .await
        .expect_err("race-created destination must make no-replace publication fail");
    assert!(!error.recovery.is_reconciled());
    assert!(error
        .into_plugin_error()
        .to_string()
        .contains("manual bundle recovery is required"));
    assert_eq!(
        tokio::fs::read_to_string(live_dir.join("RACE_MARKER"))
            .await
            .unwrap(),
        "race-owned",
        "activation must not delete or overwrite the unexpected destination"
    );
    assert!(!live_dir.join("NEW_MARKER").exists());

    let names = plugin_root_entry_names(&plugins_root).await;
    assert!(
        names.iter().all(|name| !name.starts_with(".staging-")),
        "the internal UUID candidate must be cleaned: {names:?}"
    );
    let backups: Vec<_> = names
        .iter()
        .filter(|name| name.starts_with(".backup-hello-plugin-"))
        .collect();
    assert_eq!(backups.len(), 1, "old bundle must remain recoverable");
    assert_eq!(
        tokio::fs::read_to_string(plugins_root.join(backups[0]).join("OLD_MARKER"))
            .await
            .unwrap(),
        "old-bundle"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn activation_symlink_race_never_touches_external_target() {
    let root = tempfile::tempdir().unwrap();
    let (prepared, plugins_root, live_dir, _) = prepared_upgrade_fixture(root.path()).await;
    let external = root.path().join("external-target");
    tokio::fs::create_dir(&external).await.unwrap();
    tokio::fs::write(external.join("SENTINEL"), b"external-owned")
        .await
        .unwrap();

    let error = prepared
        .activate_with_fault(ActivationFault::CreateDestinationSymlink(external.clone()))
        .await
        .expect_err("symlink destination must make no-replace publication fail");
    assert!(!error.recovery.is_reconciled());
    assert!(error
        .into_plugin_error()
        .to_string()
        .contains("manual bundle recovery is required"));
    assert!(
        tokio::fs::symlink_metadata(&live_dir)
            .await
            .unwrap()
            .file_type()
            .is_symlink(),
        "the race-created symlink itself must remain untouched"
    );
    assert_eq!(
        tokio::fs::read_to_string(external.join("SENTINEL"))
            .await
            .unwrap(),
        "external-owned"
    );
    assert!(
        !external.join("plugin.json").exists() && !external.join("NEW_MARKER").exists(),
        "candidate bytes must never be written through the symlink"
    );

    let names = plugin_root_entry_names(&plugins_root).await;
    assert!(names.iter().all(|name| !name.starts_with(".staging-")));
    let backup = names
        .iter()
        .find(|name| name.starts_with(".backup-hello-plugin-"))
        .expect("old bundle backup");
    assert_eq!(
        tokio::fs::read_to_string(plugins_root.join(backup).join("OLD_MARKER"))
            .await
            .unwrap(),
        "old-bundle"
    );
}

#[tokio::test]
async fn server_source_transaction_never_restarts_after_verified_activation_restore() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer, plugins_root, live_dir, source_dir) =
        server_upgrade_fixture(root.path()).await;

    let error = install_server_plugin_from_source_with_fault(
        &installer,
        PluginSourceInput::LocalDir(source_dir),
        &plugins_root,
        &PluginTrustConfig::default(),
        InstallDisposition::Upgrade,
        Some("svc-plugin"),
        ServerSourceFault::ActivationRenameFailure,
    )
    .await
    .expect_err("injected candidate publication must fail");
    assert!(matches!(error, PluginError::Registration(_)));
    assert!(error
        .to_string()
        .contains("remain stopped pending manual recovery"));
    assert!(
        !state.service_manager.is_running("svc"),
        "a failed upgrade must never restart executable code automatically"
    );
    assert_eq!(
        tokio::fs::read_to_string(live_dir.join("OLD_MARKER"))
            .await
            .unwrap(),
        "old-bundle"
    );
    assert!(!live_dir.join("NEW_MARKER").exists());
}

#[tokio::test]
async fn final_provenance_commit_failure_aborts_registration_and_restores_exact_upgrade_state() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer, plugins_root, live_dir, source_dir, previous) =
        server_final_commit_upgrade_fixture(root.path()).await;
    let old_identity = bundle_directory_identity(&live_dir).unwrap();
    assert_eq!(previous.status, PluginInstallStatus::Installed);
    assert_eq!(previous.version, "1.0.0");
    assert_eq!(previous.plugin_dir, live_dir);
    assert_eq!(
        previous.source,
        PluginSource::LocalDir {
            path: live_dir.clone()
        }
    );
    assert_eq!(previous.registered.service_ids, vec!["svc".to_string()]);
    assert_eq!(
        previous.registered.event_sink_ids,
        vec!["old-sink".to_string()]
    );

    let error = install_server_plugin_from_source_with_fault(
        &installer,
        PluginSourceInput::LocalDir(source_dir),
        &plugins_root,
        &PluginTrustConfig::default(),
        InstallDisposition::Upgrade,
        Some("svc-plugin"),
        ServerSourceFault::FinalProvenanceCommitFailure,
    )
    .await
    .expect_err("the injected final Installed provenance commit must fail");
    let message = error.to_string();
    assert!(
        message.contains("injected final Installed provenance commit failure"),
        "{message}"
    );
    assert!(message.contains("service(s) [svc]"), "{message}");
    assert!(
        message.contains("remain stopped pending manual recovery"),
        "{message}"
    );

    assert!(
        !state.service_manager.is_running("svc"),
        "abort_install must stop the service started from the new bundle, and the old service must remain stopped"
    );
    assert_eq!(bundle_directory_identity(&live_dir).unwrap(), old_identity);
    assert_eq!(
        tokio::fs::read_to_string(live_dir.join("OLD_MARKER"))
            .await
            .unwrap(),
        "old-bundle"
    );
    assert!(
        !live_dir.join("NEW_MARKER").exists(),
        "the new bundle must not remain at the live path"
    );

    let restored = InstalledPlugins::load(&plugins_root.join("installed.json"))
        .await
        .unwrap()
        .get_unique("svc-plugin")
        .unwrap()
        .unwrap()
        .clone();
    assert_eq!(
        restored, previous,
        "abort_install must restore the previous Installed row byte-for-byte at the model level"
    );
    assert_eq!(restored.status, PluginInstallStatus::Installed);
    assert_eq!(restored.version, "1.0.0");
    assert_eq!(restored.plugin_dir, live_dir);
    assert_eq!(restored.registered.service_ids, vec!["svc".to_string()]);
    assert_eq!(
        restored.registered.event_sink_ids,
        vec!["old-sink".to_string()]
    );
}

#[tokio::test]
async fn server_source_transaction_rejects_live_replacement_after_service_stop() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer, plugins_root, live_dir, source_dir) =
        server_upgrade_fixture(root.path()).await;

    let error = install_server_plugin_from_source_with_fault(
        &installer,
        PluginSourceInput::LocalDir(source_dir),
        &plugins_root,
        &PluginTrustConfig::default(),
        InstallDisposition::Upgrade,
        Some("svc-plugin"),
        ServerSourceFault::ReplaceLiveAfterStop,
    )
    .await
    .expect_err("a post-stop replacement must not be accepted as the previous bundle");
    let message = error.to_string();
    assert!(message.contains("exact pre-stop snapshot"), "{message}");
    assert!(
        message.contains("remain stopped pending manual recovery"),
        "{message}"
    );
    assert!(!state.service_manager.is_running("svc"));
    assert_eq!(
        tokio::fs::read_to_string(live_dir.join("RACE_MARKER"))
            .await
            .unwrap(),
        "race-owned"
    );
    assert!(!live_dir.join("OLD_MARKER").exists());
    assert!(!live_dir.join("NEW_MARKER").exists());

    let names = plugin_root_entry_names(&plugins_root).await;
    let displaced_old = names
        .iter()
        .find(|name| name.starts_with(".fault-displaced-live-"))
        .expect("the pre-stop bundle must remain preserved under its displaced path");
    assert!(plugins_root.join(displaced_old).join("OLD_MARKER").exists());
    let retained_candidate = names
        .iter()
        .find(|name| name.starts_with(".candidate-svc-plugin-"))
        .expect("the prepared candidate must be retained without recursive deletion");
    assert!(plugins_root
        .join(retained_candidate)
        .join("NEW_MARKER")
        .exists());
}

#[tokio::test]
async fn server_upgrade_rejects_a_missing_live_bundle_before_stopping_services() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer, plugins_root, live_dir, source_dir) =
        server_upgrade_fixture(root.path()).await;
    let displaced_old = plugins_root.join(format!(
        ".fault-displaced-before-snapshot-{}",
        uuid::Uuid::new_v4()
    ));
    rename_noreplace(&live_dir, &displaced_old).unwrap();

    let error = install_server_plugin_from_source_with_fault(
        &installer,
        PluginSourceInput::LocalDir(source_dir),
        &plugins_root,
        &PluginTrustConfig::default(),
        InstallDisposition::Upgrade,
        Some("svc-plugin"),
        ServerSourceFault::None,
    )
    .await
    .expect_err("an upgrade requires its exact old live bundle before shutdown");
    assert!(error.to_string().contains("requires an exact live bundle"));
    assert!(
        state.service_manager.is_running("svc"),
        "the upgrade must fail before stop_services_for_upgrade"
    );
    assert!(displaced_old.join("OLD_MARKER").exists());
}

#[test]
fn failed_upgrade_with_no_stopped_services_preserves_the_underlying_error() {
    let error = PluginError::Registration("ordinary failure".to_string());
    let expected = error.to_string();
    let actual = stopped_upgrade_failure(error, &[]).to_string();
    assert_eq!(actual, expected);
    assert!(!actual.contains("restart"));
    assert!(!actual.contains("remain stopped"));
}

#[cfg(windows)]
#[test]
fn windows_bundle_identity_is_stable_across_a_sibling_rename() {
    let root = tempfile::tempdir().unwrap();
    let original = root.path().join("original");
    let moved = root.path().join("moved");
    let distinct = root.path().join("distinct");
    std::fs::create_dir(&original).unwrap();
    std::fs::create_dir(&distinct).unwrap();

    let (_, before) = capture_bundle_directory(&original).unwrap();
    rename_noreplace(&original, &moved).unwrap();
    let (_, after) = capture_bundle_directory(&moved).unwrap();
    let (_, other) = capture_bundle_directory(&distinct).unwrap();

    assert_eq!(before, after);
    assert_eq!(before.volume, after.volume);
    assert_ne!(before.file_id, other.file_id);
}

#[tokio::test]
async fn server_source_transaction_keeps_service_stopped_when_activation_restore_is_blocked() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer, plugins_root, live_dir, source_dir) =
        server_upgrade_fixture(root.path()).await;

    let error = install_server_plugin_from_source_with_fault(
        &installer,
        PluginSourceInput::LocalDir(source_dir),
        &plugins_root,
        &PluginTrustConfig::default(),
        InstallDisposition::Upgrade,
        Some("svc-plugin"),
        ServerSourceFault::ActivationDestinationDirectory,
    )
    .await
    .expect_err("race-created destination must block activation and restore");
    assert!(error
        .to_string()
        .contains("manual bundle recovery is required"));
    assert!(
        !state.service_manager.is_running("svc"),
        "the old service must stay stopped while live and backup paths are ambiguous"
    );
    assert_eq!(
        tokio::fs::read_to_string(live_dir.join("RACE_MARKER"))
            .await
            .unwrap(),
        "race-owned"
    );
    assert!(!live_dir.join("NEW_MARKER").exists());
    let names = plugin_root_entry_names(&plugins_root).await;
    let backup = names
        .iter()
        .find(|name| name.starts_with(".backup-svc-plugin-"))
        .expect("old bundle backup must remain recoverable");
    assert_eq!(
        tokio::fs::read_to_string(plugins_root.join(backup).join("OLD_MARKER"))
            .await
            .unwrap(),
        "old-bundle"
    );
}

#[tokio::test]
async fn server_source_transaction_preserves_rollback_race_and_does_not_restart() {
    let root = tempfile::tempdir().unwrap();
    let (state, installer, plugins_root, live_dir, source_dir) =
        server_upgrade_fixture(root.path()).await;

    let error = install_server_plugin_from_source_with_fault(
        &installer,
        PluginSourceInput::LocalDir(source_dir),
        &plugins_root,
        &PluginTrustConfig::default(),
        InstallDisposition::Upgrade,
        Some("svc-plugin"),
        ServerSourceFault::RollbackDestinationDirectory,
    )
    .await
    .expect_err("injected install failure must enter rollback");
    assert!(error
        .to_string()
        .contains("manual bundle recovery is required"));
    assert!(
        !state.service_manager.is_running("svc"),
        "rollback ambiguity must leave the previously-stopped service stopped"
    );
    assert_eq!(
        tokio::fs::read_to_string(live_dir.join("RACE_MARKER"))
            .await
            .unwrap(),
        "race-owned",
        "rollback must put the unexpected destination back without modifying it"
    );
    assert!(!live_dir.join("NEW_MARKER").exists());

    let names = plugin_root_entry_names(&plugins_root).await;
    let backup = names
        .iter()
        .find(|name| name.starts_with(".backup-svc-plugin-"))
        .expect("old bundle backup must stay preserved");
    assert_eq!(
        tokio::fs::read_to_string(plugins_root.join(backup).join("OLD_MARKER"))
            .await
            .unwrap(),
        "old-bundle"
    );
    let displaced = names
        .iter()
        .find(|name| name.starts_with(".fault-displaced-candidate-"))
        .expect("the known new candidate must also remain preserved");
    assert_eq!(
        tokio::fs::read_to_string(plugins_root.join(displaced).join("NEW_MARKER"))
            .await
            .unwrap(),
        "new-bundle"
    );
    assert!(names.iter().all(|name| !name.starts_with(".rollback-")));
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
        &PluginTrustConfig::default(),
        InstallDisposition::FailIfInstalled,
    )
    .await
    .expect_err("install always fails in this test");
    assert!(matches!(error, PluginError::Registration(_)));

    assert!(
        !plugins_root.join("hello-plugin").exists(),
        "a failed install must not leave a half-installed bundle behind"
    );
    let names = plugin_root_entry_names(&plugins_root).await;
    assert_eq!(names.len(), 1);
    assert!(
        names[0].starts_with(".rollback-hello-plugin-"),
        "the failed candidate is retained only at its inert quarantine path: {names:?}"
    );
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
        &PluginTrustConfig::default(),
        InstallDisposition::Upgrade,
    )
    .await
    .expect_err("install always fails in this test");
    assert!(matches!(error, PluginError::Registration(_)));

    assert!(
        existing_dir.join("MARKER").exists(),
        "a failed upgrade must restore the pre-upgrade bundle"
    );
    let names = plugin_root_entry_names(&plugins_root).await;
    assert!(names.iter().any(|name| name == "hello-plugin"));
    assert!(
        names
            .iter()
            .any(|name| name.starts_with(".rollback-hello-plugin-")),
        "the old bundle must be live and the failed candidate retained only in quarantine: {names:?}"
    );
}

#[allow(dead_code)]
fn _use_pathbuf(_p: PathBuf) {}

// ---------------------------------------------------------------------
// Decompression bomb guard: MAX_DOWNLOAD_BYTES only caps the COMPRESSED
// bytes fetched over the wire; a small, highly-compressible archive must
// still be rejected once its DECOMPRESSED output would exceed the
// (separately capped) ceiling — see `MAX_DECOMPRESSED_BYTES` / `copy_capped`.
// ---------------------------------------------------------------------

#[tokio::test]
async fn targz_exceeding_decompressed_cap_is_rejected_and_retained_inertly() {
    let root = tempfile::tempdir().unwrap();

    // A valid, small plugin.json plus one wildly compressible "skill" file
    // (16 KiB of zeros compresses to a handful of bytes) — with the cap
    // injected at 1 KiB, extraction must abort partway through that second
    // entry, well before actually writing 16 KiB to disk.
    let manifest = hello_manifest_json("hello-plugin");
    let oversized_content = vec![0u8; 16 * 1024];
    let archive_bytes = build_targz(&[
        ("plugin.json", manifest.as_bytes()),
        ("skills/hello-world/SKILL.md", &oversized_content),
    ]);
    let archive_path = root.path().join("bomb.tar.gz");
    tokio::fs::write(&archive_path, &archive_bytes)
        .await
        .unwrap();

    let plugins_root = root.path().join("plugins");
    let error = stage_plugin_source_with_decompressed_cap(
        PluginSourceInput::LocalArchive(archive_path),
        &plugins_root,
        &PluginTrustConfig::default(),
        1024,
    )
    .await
    .expect_err("an archive expanding past the injected decompressed cap must be rejected");
    assert!(matches!(error, PluginError::InvalidManifest(_)));
    assert!(error.to_string().contains("decompress"));

    // Nothing is committed under the live id. Partial extraction remains
    // inert under a rejected-staging quarantine for operator cleanup.
    assert!(!plugins_root.join("hello-plugin").exists());
    assert_single_rejected_staging(
        &plugins_root,
        "a rejected decompression bomb must retain only inert staging",
    )
    .await;
}

#[tokio::test]
async fn zip_exceeding_decompressed_cap_is_rejected_and_retained_inertly() {
    let root = tempfile::tempdir().unwrap();

    let manifest = hello_manifest_json("hello-plugin");
    let oversized_content = vec![0u8; 16 * 1024];
    let archive_bytes = build_zip(&[
        ("plugin.json", manifest.as_bytes()),
        ("skills/hello-world/SKILL.md", &oversized_content),
    ]);
    let archive_path = root.path().join("bomb.zip");
    tokio::fs::write(&archive_path, &archive_bytes)
        .await
        .unwrap();

    let plugins_root = root.path().join("plugins");
    let error = stage_plugin_source_with_decompressed_cap(
        PluginSourceInput::LocalArchive(archive_path),
        &plugins_root,
        &PluginTrustConfig::default(),
        1024,
    )
    .await
    .expect_err("an archive expanding past the injected decompressed cap must be rejected");
    assert!(matches!(error, PluginError::InvalidManifest(_)));
    assert!(error.to_string().contains("decompress"));

    assert!(!plugins_root.join("hello-plugin").exists());
    assert_single_rejected_staging(
        &plugins_root,
        "a rejected decompression bomb must retain only inert staging",
    )
    .await;
}

#[tokio::test]
async fn archive_within_decompressed_cap_still_stages_normally() {
    // Sanity check the cap doesn't false-positive on an ordinary small
    // archive comfortably under it.
    let root = tempfile::tempdir().unwrap();
    let manifest = hello_manifest_json("hello-plugin");
    let archive_bytes = build_targz(&[
        ("plugin.json", manifest.as_bytes()),
        (
            "skills/hello-world/SKILL.md",
            b"---\nname: hello-world\ndescription: demo\n---\nHi\n",
        ),
    ]);
    let archive_path = root.path().join("fine.tar.gz");
    tokio::fs::write(&archive_path, &archive_bytes)
        .await
        .unwrap();

    let plugins_root = root.path().join("plugins");
    let staged = stage_plugin_source_with_decompressed_cap(
        PluginSourceInput::LocalArchive(archive_path),
        &plugins_root,
        &PluginTrustConfig::default(),
        1024 * 1024,
    )
    .await
    .expect("an archive well under the cap must still stage normally");
    assert_eq!(staged.manifest.id, "hello-plugin");
    staged.commit().await;
}
