use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bamboo_config::{PluginTrustConfig, TrustedKey};
use bamboo_plugin::{
    InstallDisposition, InstalledPlugin, PluginError, PluginInstaller, PluginManifest,
    PluginResult, PluginSource,
};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signer, SigningKey};

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

/// Full constructor for `PluginSourceInput::Url` exercising all five fields —
/// used directly by the host-allowlist/signature test sections, which need
/// per-test control over `allow_untrusted_host`/`allow_unsigned`.
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

    // Nothing committed, and no stray staging dir left under plugins_root.
    assert!(!plugins_root.join("hello-plugin").exists());
    let mut leftovers = tokio::fs::read_dir(&plugins_root).await.unwrap();
    assert!(
        leftovers.next_entry().await.unwrap().is_none(),
        "a rejected bundle checksum mismatch must leave nothing under plugins_root"
    );
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

    // Nothing committed, and no stray staging dir left under plugins_root.
    assert!(!plugins_root.join("hello-plugin").exists());
    let mut leftovers = tokio::fs::read_dir(&plugins_root).await.unwrap();
    assert!(
        leftovers.next_entry().await.unwrap().is_none(),
        "a refused unverified install must leave nothing under plugins_root"
    );
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
    let mut leftovers = tokio::fs::read_dir(&plugins_root).await.unwrap();
    assert!(
        leftovers.next_entry().await.unwrap().is_none(),
        "a refused untrusted-host install must leave nothing under plugins_root"
    );
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
        }
    );
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
        }
    );
    staged.commit().await;
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

// ---------------------------------------------------------------------
// Decompression bomb guard: MAX_DOWNLOAD_BYTES only caps the COMPRESSED
// bytes fetched over the wire; a small, highly-compressible archive must
// still be rejected once its DECOMPRESSED output would exceed the
// (separately capped) ceiling — see `MAX_DECOMPRESSED_BYTES` / `copy_capped`.
// ---------------------------------------------------------------------

#[tokio::test]
async fn targz_exceeding_decompressed_cap_is_rejected_and_nothing_left_under_plugins_root() {
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

    // Nothing committed under plugins_root: no plugin dir, and no stray
    // staging/backup directories left behind either.
    assert!(!plugins_root.join("hello-plugin").exists());
    let mut leftovers = tokio::fs::read_dir(&plugins_root).await.unwrap();
    assert!(
        leftovers.next_entry().await.unwrap().is_none(),
        "a rejected decompression bomb must leave nothing under plugins_root"
    );
}

#[tokio::test]
async fn zip_exceeding_decompressed_cap_is_rejected_and_nothing_left_under_plugins_root() {
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
    let mut leftovers = tokio::fs::read_dir(&plugins_root).await.unwrap();
    assert!(
        leftovers.next_entry().await.unwrap().is_none(),
        "a rejected decompression bomb must leave nothing under plugins_root"
    );
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
