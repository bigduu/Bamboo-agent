//! Plugin source staging (Wave 2 § Installer-core agent, `PLUGIN_PLAN.md`
//! Deliverable B): turns whatever the caller (CLI/HTTP) pointed the
//! installer at — a local directory, a local `.zip`/`.tar.gz`/`.tgz`
//! archive, or a URL — into a validated bundle at `plugins_dir()/<id>/`,
//! ready to hand to [`bamboo_plugin::PluginInstaller::install`].
//!
//! # The three sources
//!
//! - [`PluginSourceInput::LocalDir`] — copies the directory tree.
//! - [`PluginSourceInput::LocalArchive`] — unpacks a `.zip`/`.tar.gz`/`.tgz`.
//!   If the archive wraps everything in a single top-level directory (the
//!   common `tar czf bundle.tar.gz plugin-name/` convention), that directory
//!   is flattened up so `plugin.json` ends up at the bundle root either way.
//! - [`PluginSourceInput::Url`] — fetches the manifest bundle (a bare
//!   `plugin.json`, content-only and typically an MCP server backed entirely
//!   by a downloadable binary — e.g. a nova-style plugin with no bundled
//!   skills/prompts — or an archive containing one, same flattening rule as
//!   `LocalArchive`). **Secure by default**: the downloaded bundle bytes are
//!   verified against a caller-supplied `sha256` BEFORE anything is
//!   extracted or parsed; if no `sha256` was given, the fetch is refused
//!   outright unless the caller explicitly set `allow_unverified` (see
//!   [`PluginSourceInput::Url`]'s fields and [`fetch_manifest_bundle`]). THEN,
//!   separately, for [`Platform::current`] (if the manifest declares an
//!   artifact for it), fetches the per-platform binary archive named in
//!   `manifest.artifacts`, verifies its sha256 BEFORE unpacking (mandatory —
//!   a URL plugin ships a binary that gets executed), and places the single
//!   expected executable at `bin/<platform>/<id>[.exe]` per
//!   [`bamboo_plugin::manifest::PluginArtifact`]'s archive contract. Both
//!   checks stay: the bundle-sha256 check is the new root-of-trust pin (it
//!   closes the gap where the artifact's own declared hash lives inside the
//!   very bundle that could be tampered with); the artifact-sha256 check
//!   remains as defense in depth for the binary specifically.
//!
//! All three paths run the SAME safety checks: [`PluginManifest::validate`]
//! before anything is committed to `plugins_dir()`, and path-traversal-safe
//! archive extraction (a zip entry's [`zip::read::ZipFile::enclosed_name`]
//! rejects `..`/absolute entries outright; a tar entry's path is checked for
//! `ParentDir`/root/prefix components before extraction) — a malicious
//! archive must not be able to write outside its own staging directory.
//!
//! # Swap safety (why an upgrade doesn't lose the old bundle on failure)
//!
//! `plugin_dir` is a fixed path per id (`plugins_dir()/<id>/`), so an upgrade
//! necessarily replaces whatever is already there. [`stage_plugin_source`]
//! builds the new bundle in a scratch `.staging-*` directory first (so a bad
//! source — invalid manifest, failed download, sha256 mismatch — never
//! touches the existing install), then — only once the new bundle validates
//! — moves the OLD `plugin_dir` aside to a `.backup-*` directory (not
//! deleted) before renaming the staging directory into place. The returned
//! [`StagedPlugin`] carries that backup path privately: [`StagedPlugin::commit`]
//! deletes it (call after a successful `install()`), [`StagedPlugin::rollback`]
//! removes the half-installed new bundle and restores the backup (call after
//! a failed `install()`). [`install_plugin_from_source`] wires this up
//! automatically and is the seam CLI/HTTP callers should use end to end.
//!
//! Residual gap (documented, not solved here): the plugin_dir swap itself and
//! `install()`'s own capability-registration rollback (see
//! `crate::plugin_installer`'s module docs) are two separate best-effort
//! steps, not one atomic transaction. If the process crashes between the
//! swap and `install()` returning, a retry is still safe (staging always
//! starts from a fresh scratch dir; a leftover `.backup-*`/`.staging-*` dir
//! is inert and can be swept by an operator or a future cleanup pass) but is
//! not automatic today.
//!
//! # Known follow-ups (deferred — tracked here, not fixed on this branch)
//!
//! - **URL content-bundle integrity pin: IMPLEMENTED (secure by default).**
//!   Previously only the per-platform BINARY artifact was sha256-pinned
//!   (`PluginArtifact.sha256`, verified in [`fetch_and_place_artifact`]),
//!   while the `plugin.json` / content archive fetched by
//!   [`fetch_manifest_bundle`] was trusted on HTTPS alone — a MITM or a
//!   compromised host could serve a tampered bundle, and since the binary's
//!   sha256 is DECLARED INSIDE that same untrusted manifest, tampering the
//!   bundle could rewrite the artifact hash too (the trust chain was
//!   circular). Fixed: [`PluginSourceInput::Url`] now carries a `sha256`
//!   (the expected hash of the downloaded bundle) and an `allow_unverified`
//!   opt-out. [`fetch_manifest_bundle`] verifies the bundle's actual sha256
//!   against it BEFORE any extraction/parsing on a mismatch
//!   ([`PluginError::BundleVerificationFailed`]); with neither a `sha256`
//!   nor `allow_unverified: true`, the fetch is refused up front — before
//!   the URL is ever requested — with [`PluginError::ChecksumRequired`]. A
//!   URL install can therefore no longer just download-and-trust any
//!   tar.gz. The verified bundle sha256 (not the binary artifact's) is what
//!   `PluginSource::Url.sha256` records for provenance/audit. Still
//!   deferred:
//!   - **Signature verification.** A sha256 pin only proves "this is the
//!     bytes the installer expected", not "an entity I trust produced
//!     them" — the hash itself still has to come from somewhere the caller
//!     trusts (a release page, a signed index, etc.). A real signature
//!     scheme (e.g. minisign/sigstore over the bundle) would remove that
//!     remaining hop.
//!   - **SSRF guard**, described next.
//! - **No SSRF guard on URL fetch.** [`download_bytes`] will fetch any URL,
//!   including `http://169.254.169.254/...` (cloud metadata) or private-range
//!   / loopback addresses. In a hosted/multi-tenant deployment a plugin-install
//!   URL is an SSRF vector. A private-IP / metadata-endpoint blocklist (or an
//!   allowlist of plugin registries) is a threat-model call for the deploy
//!   layer; noted here so it isn't forgotten.
//! - **`prompt-presets.json`'s `save_store` is non-atomic** (`fs::write` in
//!   place, pre-existing behaviour shared with the HTTP prompt-preset
//!   handlers): a crash mid-write can truncate `prompt-presets.json`. A
//!   write-to-temp-then-rename would make it atomic, matching what
//!   `bamboo_plugin::registry::InstalledPlugins::save` (`installed.json`) now
//!   does; deferred here as a change to a shared, pre-existing storage
//!   helper rather than this branch's new code.
//! - **The `plugin_dir` swap in [`stage_plugin_source`] runs BEFORE
//!   `PLUGIN_OP_LOCK` is acquired** (that lock is taken inside
//!   `ServerPluginInstaller::install`/`uninstall`, in `crate::plugin_installer`,
//!   which only runs after staging returns). Two concurrent installs/updates
//!   of the SAME plugin id could therefore race the backup-rename +
//!   staging-rename swap itself (not just the capability-registration steps
//!   the lock does protect). Plugin ops are rare/operator-driven, not
//!   concurrent by design, so this is accepted as a documented gap rather
//!   than moving the lock acquisition into this module; a real fix would
//!   need `stage_plugin_source` and `PluginInstaller::install` to share one
//!   lock scope (a bigger seam change than this branch's fixes).

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use bamboo_plugin::manifest::Platform;
use bamboo_plugin::{
    InstallDisposition, InstalledPlugin, PluginError, PluginInstaller, PluginManifest,
    PluginResult, PluginSource,
};

/// What the caller pointed the installer at.
#[derive(Debug, Clone)]
pub enum PluginSourceInput {
    /// A local directory containing `plugin.json` at its root.
    LocalDir(PathBuf),
    /// A local `.zip` / `.tar.gz` / `.tgz` archive containing `plugin.json`
    /// (at its root, or under a single top-level directory).
    LocalArchive(PathBuf),
    /// A URL to either a bare `plugin.json` or an archive containing one
    /// (same root-or-single-subdir rule as `LocalArchive`). Secure by
    /// default: `sha256`, when given, pins the downloaded bundle's exact
    /// bytes (verified before anything is extracted/parsed — see
    /// [`fetch_manifest_bundle`]); with no `sha256`, the fetch is refused
    /// unless `allow_unverified` is `true` (an explicit, logged opt-out of
    /// verification — see [`PluginError::ChecksumRequired`]).
    Url {
        url: String,
        sha256: Option<String>,
        allow_unverified: bool,
    },
}

/// The result of staging a [`PluginSourceInput`] into `plugins_dir()/<id>/`:
/// the validated manifest (so a caller can preview `manifest.provides` —
/// what the install WILL register — before committing to `install()`), the
/// final on-disk `plugin_dir`, and the [`PluginSource`] provenance record
/// `install()` expects. See the module docs for the backup/swap contract.
#[derive(Debug)]
pub struct StagedPlugin {
    pub manifest: PluginManifest,
    pub plugin_dir: PathBuf,
    pub source: PluginSource,
    /// The previous `plugin_dir` contents, moved aside rather than deleted
    /// when staging replaced an existing install (`None` on a fresh
    /// install). Private: only [`StagedPlugin::commit`] /
    /// [`StagedPlugin::rollback`] act on it.
    backup_dir: Option<PathBuf>,
}

impl StagedPlugin {
    /// Call after a successful `install()`: the swap is final, drop the
    /// pre-upgrade backup for good. A no-op on a fresh install.
    pub async fn commit(self) {
        if let Some(backup) = self.backup_dir {
            let _ = tokio::fs::remove_dir_all(&backup).await;
        }
    }

    /// Call after a failed `install()`: undo the swap. Removes the
    /// half-installed new bundle and, on an upgrade, restores the pre-upgrade
    /// files so the plugin is left exactly as it was.
    pub async fn rollback(self) {
        let _ = tokio::fs::remove_dir_all(&self.plugin_dir).await;
        if let Some(backup) = self.backup_dir {
            let _ = tokio::fs::rename(&backup, &self.plugin_dir).await;
        }
    }
}

/// Stage `input` into `plugins_root/<id>/`, validating the manifest before
/// anything is committed. Returns a [`StagedPlugin`] ready to hand to
/// [`bamboo_plugin::PluginInstaller::install`] — call [`StagedPlugin::commit`]
/// or [`StagedPlugin::rollback`] once you know whether `install()` succeeded
/// (or just use [`install_plugin_from_source`], which does both for you).
pub async fn stage_plugin_source(
    input: PluginSourceInput,
    plugins_root: &Path,
) -> PluginResult<StagedPlugin> {
    stage_plugin_source_inner(input, plugins_root, MAX_DECOMPRESSED_BYTES).await
}

/// Test-only seam for [`stage_plugin_source`] that lets a test inject a small
/// `max_decompressed_bytes` cap (the production cap, [`MAX_DECOMPRESSED_BYTES`],
/// is a generous 2 GiB — not practical to actually exceed in a unit test).
/// Exercises the exact same staging/swap machinery as the production path,
/// just with the archive-extraction ceiling parameterized.
#[cfg(test)]
pub(crate) async fn stage_plugin_source_with_decompressed_cap(
    input: PluginSourceInput,
    plugins_root: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<StagedPlugin> {
    stage_plugin_source_inner(input, plugins_root, max_decompressed_bytes).await
}

async fn stage_plugin_source_inner(
    input: PluginSourceInput,
    plugins_root: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<StagedPlugin> {
    tokio::fs::create_dir_all(plugins_root).await?;
    let staging_dir = plugins_root.join(format!(".staging-{}", uuid::Uuid::new_v4()));
    tokio::fs::create_dir_all(&staging_dir).await?;

    let staged = stage_into(&input, &staging_dir, max_decompressed_bytes).await;
    let (manifest, source) = match staged {
        Ok(pair) => pair,
        Err(error) => {
            let _ = tokio::fs::remove_dir_all(&staging_dir).await;
            return Err(error);
        }
    };

    if let Err(error) = manifest.validate() {
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
        return Err(error);
    }

    let plugin_dir = plugins_root.join(&manifest.id);
    let backup_dir = if tokio::fs::try_exists(&plugin_dir).await.unwrap_or(false) {
        let backup = plugins_root.join(format!(".backup-{}-{}", manifest.id, uuid::Uuid::new_v4()));
        if let Err(error) = tokio::fs::rename(&plugin_dir, &backup).await {
            let _ = tokio::fs::remove_dir_all(&staging_dir).await;
            return Err(PluginError::Io(error));
        }
        Some(backup)
    } else {
        None
    };

    if let Err(rename_error) = tokio::fs::rename(&staging_dir, &plugin_dir).await {
        // Cross-device (e.g. staging on a different mount) — fall back to a
        // recursive copy, then remove the staging dir.
        if let Err(copy_error) = copy_dir_recursive(&staging_dir, &plugin_dir).await {
            let _ = tokio::fs::remove_dir_all(&staging_dir).await;
            // Restore the backup we just moved aside, if any, since the swap
            // never actually happened.
            if let Some(backup) = &backup_dir {
                let _ = tokio::fs::rename(backup, &plugin_dir).await;
            }
            tracing::warn!(%rename_error, %copy_error, "failed to stage plugin bundle into place");
            return Err(copy_error);
        }
        let _ = tokio::fs::remove_dir_all(&staging_dir).await;
    }

    Ok(StagedPlugin {
        manifest,
        plugin_dir,
        source,
        backup_dir,
    })
}

/// Stage + `install()` + commit/rollback, in one call — the seam CLI/HTTP
/// callers should use. See the module docs.
pub async fn install_plugin_from_source(
    installer: &dyn PluginInstaller,
    input: PluginSourceInput,
    plugins_root: &Path,
    disposition: InstallDisposition,
) -> PluginResult<InstalledPlugin> {
    let staged = stage_plugin_source(input, plugins_root).await?;
    let manifest = staged.manifest.clone();
    let plugin_dir = staged.plugin_dir.clone();
    let source = staged.source.clone();

    match installer
        .install(
            &manifest,
            &plugin_dir,
            source,
            disposition,
            chrono::Utc::now(),
        )
        .await
    {
        Ok(entry) => {
            staged.commit().await;
            Ok(entry)
        }
        Err(error) => {
            staged.rollback().await;
            Err(error)
        }
    }
}

async fn stage_into(
    input: &PluginSourceInput,
    staging_dir: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<(PluginManifest, PluginSource)> {
    match input {
        PluginSourceInput::LocalDir(path) => {
            copy_dir_recursive(path, staging_dir).await?;
            let manifest = read_and_parse_manifest(staging_dir).await?;
            Ok((manifest, PluginSource::LocalDir { path: path.clone() }))
        }
        PluginSourceInput::LocalArchive(path) => {
            let bytes = tokio::fs::read(path).await?;
            let kind = detect_archive_kind(&path.to_string_lossy()).ok_or_else(|| {
                PluginError::InvalidManifest(format!(
                    "unsupported archive extension for '{}': expected .zip/.tar.gz/.tgz",
                    path.display()
                ))
            })?;
            extract_archive(
                bytes,
                kind,
                staging_dir.to_path_buf(),
                max_decompressed_bytes,
            )
            .await?;
            flatten_if_single_subdir(staging_dir).await?;
            let manifest = read_and_parse_manifest(staging_dir).await?;
            Ok((manifest, PluginSource::LocalArchive { path: path.clone() }))
        }
        PluginSourceInput::Url {
            url,
            sha256,
            allow_unverified,
        } => {
            let (manifest, verified_bundle_sha256) = fetch_manifest_bundle(
                url,
                sha256.as_deref(),
                *allow_unverified,
                staging_dir,
                max_decompressed_bytes,
            )
            .await?;
            // Binary-artifact verification stays as defense in depth (see
            // the module docs) — its own sha256, declared inside the
            // now-verified manifest, is checked in
            // `fetch_and_place_artifact`, but no longer double-duty as the
            // `PluginSource::Url` provenance hash: that's the bundle's own
            // verified sha256 now, computed above.
            fetch_and_place_artifact(&manifest, staging_dir, max_decompressed_bytes).await?;
            Ok((
                manifest,
                PluginSource::Url {
                    url: url.clone(),
                    sha256: verified_bundle_sha256,
                    allow_unverified: *allow_unverified,
                },
            ))
        }
    }
}

// ---------------------------------------------------------------------
// Manifest bundle fetch (URL source)
// ---------------------------------------------------------------------

/// Fetch `url`: either a bare `plugin.json` or an archive containing one
/// (same root-or-single-subdir rule as [`PluginSourceInput::LocalArchive`]).
/// Populates `staging_dir` with whatever the bundle contains (just
/// `plugin.json` for a bare manifest; the full skills/prompts/workflows tree
/// for an archive).
///
/// **Secure by default.** Before the URL is even requested: if `sha256` is
/// `None` and `allow_unverified` is `false`, refuses with
/// [`PluginError::ChecksumRequired`] — no network access happens for a
/// refused install. If `sha256` IS given, the downloaded bytes are hashed
/// and compared (case-insensitive) BEFORE any extraction/parsing; a
/// mismatch is [`PluginError::BundleVerificationFailed`] and nothing is
/// written beyond the (discarded) in-memory bytes. If `allow_unverified` is
/// `true` and no `sha256` was given, the fetch proceeds but logs a
/// `tracing::warn!` — an explicit, visible opt-out of verification, never a
/// silent one.
///
/// Returns the parsed manifest and, when a `sha256` was supplied and
/// confirmed, that verified hex digest (for [`PluginSource::Url`]
/// provenance) — `None` only on the `allow_unverified`-with-no-hash path.
async fn fetch_manifest_bundle(
    url: &str,
    sha256: Option<&str>,
    allow_unverified: bool,
    staging_dir: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<(PluginManifest, Option<String>)> {
    if sha256.is_none() && !allow_unverified {
        return Err(PluginError::ChecksumRequired(format!(
            "refusing to install plugin bundle from '{url}' without a checksum — pass the \
             bundle's sha256 (from the release page / a trusted source) to verify it before \
             install (CLI: `--sha256 <hex>`; HTTP: `\"sha256\": \"<hex>\"` on the url source), \
             or explicitly accept the risk of an unverified download (CLI: \
             `--allow-unverified`; HTTP: `\"allow_unverified\": true`)"
        )));
    }

    let bytes = download_bytes(url).await?;

    let verified_sha256 = match sha256 {
        Some(expected) => {
            let actual = sha256_hex(&bytes);
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(PluginError::BundleVerificationFailed(format!(
                    "sha256 mismatch for plugin bundle '{url}': expected {expected}, downloaded \
                     bytes hash to {actual} — refusing to unpack (the bundle may be tampered, \
                     corrupted, or the wrong sha256 was supplied)"
                )));
            }
            Some(actual)
        }
        None => {
            tracing::warn!(
                %url,
                "installing plugin bundle from a URL with no checksum verification \
                 (allow_unverified opt-out) — the download is trusted on HTTPS alone"
            );
            None
        }
    };

    let manifest = if let Some(kind) = detect_archive_kind(url) {
        extract_archive(
            bytes,
            kind,
            staging_dir.to_path_buf(),
            max_decompressed_bytes,
        )
        .await?;
        flatten_if_single_subdir(staging_dir).await?;
        read_and_parse_manifest(staging_dir).await?
    } else {
        let raw = String::from_utf8(bytes).map_err(|_| {
            PluginError::InvalidManifest(format!("manifest at '{url}' is not valid UTF-8"))
        })?;
        tokio::fs::create_dir_all(staging_dir).await?;
        tokio::fs::write(staging_dir.join("plugin.json"), &raw).await?;
        PluginManifest::parse_str(&raw)?
    };

    Ok((manifest, verified_sha256))
}

/// Per-platform binary artifact fetch (URL source only). Fetches the
/// artifact declared for [`Platform::current`] (a no-op if the manifest
/// declares none for this platform — not every plugin needs a binary),
/// verifies its sha256 BEFORE unpacking, and places the single expected
/// executable at `<staging_dir>/bin/<platform>/<id>[.exe]`.
///
/// This stays as defense in depth alongside [`fetch_manifest_bundle`]'s
/// bundle-sha256 check (see the module docs): the artifact hash is declared
/// INSIDE the manifest, so on its own it only proves "the binary matches
/// what this bundle's manifest says", not "this bundle itself is what the
/// caller expected" — that's what the bundle-level check now provides. The
/// verified artifact sha256 is therefore no longer surfaced to the caller
/// (it used to double as [`PluginSource::Url`]'s provenance hash before the
/// bundle-level check existed); this function's contract is purely
/// verify-then-place.
async fn fetch_and_place_artifact(
    manifest: &PluginManifest,
    staging_dir: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<()> {
    let Some(platform) = Platform::current() else {
        return Ok(());
    };
    let Some(artifact) = manifest.artifacts.get(platform.as_str()) else {
        return Ok(());
    };

    let bytes = download_bytes(&artifact.url).await?;
    let actual_sha256 = sha256_hex(&bytes);
    if !actual_sha256.eq_ignore_ascii_case(&artifact.sha256) {
        return Err(PluginError::ArtifactVerificationFailed(format!(
            "sha256 mismatch for '{}': manifest declares {}, downloaded bytes hash to {}",
            artifact.url, artifact.sha256, actual_sha256
        )));
    }

    let kind = detect_archive_kind(&artifact.url).ok_or_else(|| {
        PluginError::InvalidManifest(format!(
            "artifact url '{}' is not a .zip/.tar.gz/.tgz",
            artifact.url
        ))
    })?;

    let scratch_dir = staging_dir.join(format!(".artifact-scratch-{}", platform.as_str()));
    extract_archive(bytes, kind, scratch_dir.clone(), max_decompressed_bytes).await?;

    let expected_name = if matches!(platform, Platform::Windows) {
        format!("{}.exe", manifest.id)
    } else {
        manifest.id.clone()
    };
    let source_bin = scratch_dir.join(&expected_name);
    if !tokio::fs::try_exists(&source_bin).await.unwrap_or(false) {
        let _ = tokio::fs::remove_dir_all(&scratch_dir).await;
        return Err(PluginError::InvalidManifest(format!(
            "artifact archive for platform '{}' does not contain the expected root executable '{}'",
            platform.as_str(),
            expected_name
        )));
    }

    let dest_dir = staging_dir.join("bin").join(platform.as_str());
    tokio::fs::create_dir_all(&dest_dir).await?;
    let dest_bin = dest_dir.join(&expected_name);
    move_file(&source_bin, &dest_bin).await?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = tokio::fs::metadata(&dest_bin).await?.permissions();
        perms.set_mode(0o755);
        tokio::fs::set_permissions(&dest_bin, perms).await?;
    }

    let _ = tokio::fs::remove_dir_all(&scratch_dir).await;
    Ok(())
}

/// `rename`, falling back to copy+remove across a device boundary.
async fn move_file(source: &Path, dest: &Path) -> PluginResult<()> {
    if tokio::fs::rename(source, dest).await.is_ok() {
        return Ok(());
    }
    let data = tokio::fs::read(source).await?;
    tokio::fs::write(dest, data).await?;
    tokio::fs::remove_file(source).await?;
    Ok(())
}

// ---------------------------------------------------------------------
// HTTP fetch
// ---------------------------------------------------------------------

/// One shared `reqwest::Client` for plugin source fetches. Reuses the
/// workspace's pinned (native-tls) `reqwest` — never construct a second
/// client/connector here (see `notify_sinks::ntfy`'s identical pattern).
fn http_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(reqwest::Client::new)
}

/// Hard ceiling on any single plugin download (manifest, bundle, or binary
/// artifact archive). A malicious or misconfigured URL must not be able to
/// stream an unbounded body into memory (OOM DoS). 256 MiB is generous for a
/// plugin bundle + one platform binary while still bounding the worst case.
const MAX_DOWNLOAD_BYTES: u64 = 256 * 1024 * 1024;

/// Hard ceiling on the TOTAL decompressed bytes any single archive (zip or
/// tar.gz) may expand to across ALL of its entries combined. Complements
/// `MAX_DOWNLOAD_BYTES`, which only bounds the COMPRESSED bytes fetched over
/// the wire — a small, highly-compressible archive (a classic
/// decompression/"zip bomb") can still expand to many gigabytes on disk with
/// nothing capping the output side. Enforced incrementally DURING extraction
/// (see [`copy_capped`]) against the ACTUAL bytes read off the decompression
/// stream — never an entry's header-declared size, which a crafted archive
/// can misstate (a zip's `uncompressed_size` field in particular is pure
/// metadata the reader doesn't have to honor) — so a bomb is aborted close to
/// this ceiling rather than after it has already exhausted disk. 2 GiB is
/// generous for any legitimate plugin bundle (skills/prompts/workflows text
/// plus, at most, one platform binary) while still bounding the worst case.
const MAX_DECOMPRESSED_BYTES: u64 = 2 * 1024 * 1024 * 1024;

async fn download_bytes(url: &str) -> PluginResult<Vec<u8>> {
    use futures::StreamExt;

    let response =
        http_client().get(url).send().await.map_err(|error| {
            PluginError::Registration(format!("failed to fetch '{url}': {error}"))
        })?;
    let response = response.error_for_status().map_err(|error| {
        PluginError::Registration(format!("'{url}' returned an error status: {error}"))
    })?;

    // Reject up front if the server ADVERTISES an over-cap body (cheap, avoids
    // streaming at all)...
    if let Some(len) = response.content_length() {
        if len > MAX_DOWNLOAD_BYTES {
            return Err(PluginError::Registration(format!(
                "'{url}' advertises a {len}-byte body, over the {MAX_DOWNLOAD_BYTES}-byte plugin \
                 download cap; refusing"
            )));
        }
    }

    // ...and ALSO cap the actually-streamed bytes, since Content-Length can be
    // absent (chunked) or a lie.
    let mut stream = response.bytes_stream();
    let mut buffer: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            PluginError::Registration(format!("failed to read response body of '{url}': {error}"))
        })?;
        if buffer.len() as u64 + chunk.len() as u64 > MAX_DOWNLOAD_BYTES {
            return Err(PluginError::Registration(format!(
                "'{url}' streamed more than the {MAX_DOWNLOAD_BYTES}-byte plugin download cap; \
                 aborting"
            )));
        }
        buffer.extend_from_slice(&chunk);
    }
    Ok(buffer)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

// ---------------------------------------------------------------------
// Archive handling (path-traversal-safe)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum ArchiveKind {
    Zip,
    TarGz,
}

fn detect_archive_kind(name_or_url: &str) -> Option<ArchiveKind> {
    let lower = name_or_url.to_ascii_lowercase();
    // Strip a query string/fragment before checking the extension, in case a
    // URL looks like `.../plugin.tar.gz?token=...`.
    let lower = lower.split(['?', '#']).next().unwrap_or(&lower).to_string();
    if lower.ends_with(".zip") {
        Some(ArchiveKind::Zip)
    } else if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") {
        Some(ArchiveKind::TarGz)
    } else {
        None
    }
}

/// Extract `bytes` (a zip or tar.gz archive) into `dest_dir`, rejecting any
/// entry whose path would escape `dest_dir` (traversal / absolute paths), and
/// aborting if the TOTAL decompressed output across all entries would exceed
/// `max_decompressed_bytes` (decompression-bomb guard — see
/// [`MAX_DECOMPRESSED_BYTES`] / [`copy_capped`]). Runs the (synchronous)
/// extraction on a blocking thread.
async fn extract_archive(
    bytes: Vec<u8>,
    kind: ArchiveKind,
    dest_dir: PathBuf,
    max_decompressed_bytes: u64,
) -> PluginResult<()> {
    tokio::fs::create_dir_all(&dest_dir).await?;
    tokio::task::spawn_blocking(move || match kind {
        ArchiveKind::Zip => extract_zip_sync(&bytes, &dest_dir, max_decompressed_bytes),
        ArchiveKind::TarGz => extract_targz_sync(&bytes, &dest_dir, max_decompressed_bytes),
    })
    .await
    .map_err(|error| {
        PluginError::Registration(format!("archive extraction task panicked: {error}"))
    })?
}

/// Copy `reader` into `writer` in small, bounded chunks, tallying bytes into
/// `running_total` — which the caller carries ACROSS every entry in the
/// archive, so the cap is on the archive's total decompressed output, not
/// any one entry — and aborting the moment the cumulative count would exceed
/// `max_decompressed_bytes`. Chunked copying (rather than `std::io::copy`
/// followed by a size check afterward) keeps the amount ever actually
/// written to disk bounded near the cap even for a single maximally
/// compressible entry: the whole point of the cap is to stop a small archive
/// from exhausting disk, so only checking after a full `io::copy` completed
/// would defeat it.
fn copy_capped(
    reader: &mut impl std::io::Read,
    writer: &mut impl std::io::Write,
    running_total: &mut u64,
    max_decompressed_bytes: u64,
) -> PluginResult<()> {
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            return Ok(());
        }
        *running_total += bytes_read as u64;
        if *running_total > max_decompressed_bytes {
            return Err(PluginError::InvalidManifest(format!(
                "archive expands to more than the {max_decompressed_bytes}-byte decompressed \
                 size cap ({running_total} bytes and counting); refusing to unpack (possible \
                 decompression bomb)"
            )));
        }
        writer.write_all(&buffer[..bytes_read])?;
    }
}

fn extract_zip_sync(
    bytes: &[u8],
    dest_dir: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<()> {
    use std::io::Cursor;

    let cursor = Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|error| PluginError::InvalidManifest(format!("invalid zip archive: {error}")))?;

    let mut total_decompressed_bytes: u64 = 0;

    for index in 0..archive.len() {
        let mut file = archive.by_index(index).map_err(|error| {
            PluginError::InvalidManifest(format!("invalid zip entry at index {index}: {error}"))
        })?;
        // `enclosed_name()` is the zip crate's own traversal guard: it
        // returns `None` for any entry whose name contains `..`, is
        // absolute, or otherwise can't be safely joined under `dest_dir`.
        let Some(relative_path) = file.enclosed_name() else {
            return Err(PluginError::InvalidManifest(format!(
                "zip entry '{}' has an unsafe path (traversal/absolute) — refusing to unpack",
                file.name()
            )));
        };
        let out_path = dest_dir.join(&relative_path);
        if file.is_dir() {
            std::fs::create_dir_all(&out_path)?;
            continue;
        }
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut out_file = std::fs::File::create(&out_path)?;
        if let Err(error) = copy_capped(
            &mut file,
            &mut out_file,
            &mut total_decompressed_bytes,
            max_decompressed_bytes,
        ) {
            drop(out_file);
            // Defense in depth: remove the partial file we were just writing
            // even though the caller (`stage_plugin_source`) also wipes the
            // whole staging directory on any `Err` from this function — a
            // direct caller of this lower-level helper should never see a
            // half-written entry either.
            let _ = std::fs::remove_file(&out_path);
            return Err(error);
        }
        drop(out_file);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Some(mode) = file.unix_mode() {
                std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(mode))?;
            }
        }
    }
    Ok(())
}

fn extract_targz_sync(
    bytes: &[u8],
    dest_dir: &Path,
    max_decompressed_bytes: u64,
) -> PluginResult<()> {
    use flate2::read::GzDecoder;
    use std::path::Component;
    use tar::{Archive, EntryType};

    let decoder = GzDecoder::new(bytes);
    let mut archive = Archive::new(decoder);
    let mut total_decompressed_bytes: u64 = 0;
    for entry_result in archive.entries()? {
        let mut entry = entry_result?;

        // SECURITY (symlink/hardlink escape): reject any Symlink or HardLink
        // entry outright BEFORE unpacking. `entry.unpack()` is the raw tar API
        // — it validates the entry's OWN path (checked below) but NOT a link
        // entry's TARGET (`link_name`), which is fully attacker-controlled and
        // may be absolute or contain `..`. A malicious bundle could ship
        // e.g. `workflows/evil.md` as a symlink to `~/.ssh/id_rsa` or bamboo's
        // `config.json`; a later `fs::read_to_string` (register_workflows) would
        // follow it and copy the victim's real content into a plugin-visible
        // location = arbitrary file exfiltration, and `flatten_if_single_subdir`
        // following a symlink-to-a-real-dir could rename/destroy the victim's
        // files. A plugin bundle has no legitimate reason to ship a link (same
        // rationale as `copy_dir_recursive` skipping symlinks), so refuse the
        // whole archive. (Zip is not affected: `extract_zip_sync` writes every
        // entry as a fresh regular file via `copy_capped`, so an archived
        // "symlink" lands inert as a plain file.)
        let entry_type = entry.header().entry_type();
        if matches!(entry_type, EntryType::Symlink | EntryType::Link) {
            let link_target = entry
                .link_name()
                .ok()
                .flatten()
                .map(|path| path.display().to_string())
                .unwrap_or_default();
            return Err(PluginError::InvalidManifest(format!(
                "tar entry '{}' is a {} (target '{link_target}') — plugin bundles must not ship \
                 links; refusing to unpack",
                entry
                    .path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_default(),
                if entry_type == EntryType::Symlink {
                    "symlink"
                } else {
                    "hardlink"
                },
            )));
        }

        let relative_path = entry.path()?.into_owned();
        let is_unsafe = relative_path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        });
        if is_unsafe {
            return Err(PluginError::InvalidManifest(format!(
                "tar entry '{}' has an unsafe path (traversal/absolute) — refusing to unpack",
                relative_path.display()
            )));
        }
        let out_path = dest_dir.join(&relative_path);

        // Directories carry no content to cap — just create and move on
        // (mirrors `entry.unpack()`'s own directory handling, which this
        // function replaces for content-bearing entries below so the
        // decompressed-size cap can be enforced incrementally; see
        // `copy_capped`).
        if entry_type.is_dir() {
            std::fs::create_dir_all(&out_path)?;
            continue;
        }

        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut out_file = std::fs::File::create(&out_path)?;
        if let Err(error) = copy_capped(
            &mut entry,
            &mut out_file,
            &mut total_decompressed_bytes,
            max_decompressed_bytes,
        ) {
            drop(out_file);
            // Defense in depth: see the identical cleanup in
            // `extract_zip_sync` — the whole staging dir is also wiped by
            // the caller, but a direct caller of this helper shouldn't see
            // a half-written entry either.
            let _ = std::fs::remove_file(&out_path);
            return Err(error);
        }
        drop(out_file);

        // Preserve the entry's permission bits (matches `entry.unpack()`'s
        // own behaviour, which this manual copy replaces).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(mode) = entry.header().mode() {
                std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(mode))?;
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------

async fn read_and_parse_manifest(dir: &Path) -> PluginResult<PluginManifest> {
    let manifest_path = dir.join("plugin.json");
    let raw = tokio::fs::read_to_string(&manifest_path)
        .await
        .map_err(|_| {
            PluginError::InvalidManifest(format!(
                "no plugin.json found at '{}'",
                manifest_path.display()
            ))
        })?;
    PluginManifest::parse_str(&raw)
}

/// If `dir` has no `plugin.json` of its own but contains EXACTLY one
/// subdirectory, move that subdirectory's contents up into `dir` (the common
/// `tar czf bundle.tar.gz plugin-name/`-style archive convention). A no-op if
/// `plugin.json` is already present, or if the shape doesn't match (multiple
/// top-level entries, or a single top-level entry that isn't a directory) —
/// in either case, [`read_and_parse_manifest`] will simply fail to find
/// `plugin.json` afterwards with a clear error.
async fn flatten_if_single_subdir(dir: &Path) -> PluginResult<()> {
    if tokio::fs::try_exists(dir.join("plugin.json"))
        .await
        .unwrap_or(false)
    {
        return Ok(());
    }

    let mut entries = tokio::fs::read_dir(dir).await?;
    let mut only_entry: Option<PathBuf> = None;
    let mut count = 0usize;
    while let Some(entry) = entries.next_entry().await? {
        count += 1;
        if count > 1 {
            return Ok(());
        }
        only_entry = Some(entry.path());
    }
    let Some(candidate) = only_entry else {
        return Ok(());
    };
    // `symlink_metadata` (does NOT follow links), not `metadata`: defense in
    // depth so a symlink-to-a-real-dir can never pass `is_dir()` here and get
    // its real children renamed out. Extraction already rejects link entries
    // (see `extract_targz_sync`) and `copy_dir_recursive` skips symlinks, so
    // in practice `candidate` is always a real dir/file — but never follow a
    // link when deciding whether to descend into and move a directory's
    // contents.
    if !tokio::fs::symlink_metadata(&candidate).await?.is_dir() {
        return Ok(());
    }

    // Move `candidate`'s children up into `dir`, then remove the now-empty
    // `candidate` directory.
    let mut children = tokio::fs::read_dir(&candidate).await?;
    while let Some(child) = children.next_entry().await? {
        let dest = dir.join(child.file_name());
        tokio::fs::rename(child.path(), dest).await?;
    }
    tokio::fs::remove_dir(&candidate).await?;
    Ok(())
}

/// Recursively copy `source` into `dest` (creating `dest`).
fn copy_dir_recursive<'a>(
    source: &'a Path,
    dest: &'a Path,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = PluginResult<()>> + Send + 'a>> {
    Box::pin(async move {
        tokio::fs::create_dir_all(dest).await?;
        let mut entries = tokio::fs::read_dir(source).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            let dest_path = dest.join(entry.file_name());
            if file_type.is_dir() {
                copy_dir_recursive(&entry.path(), &dest_path).await?;
            } else if file_type.is_file() {
                tokio::fs::copy(entry.path(), &dest_path).await?;
            }
            // Symlinks are intentionally skipped — a plugin bundle has no
            // legitimate reason to ship one, and following it could escape
            // the source directory.
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests;
