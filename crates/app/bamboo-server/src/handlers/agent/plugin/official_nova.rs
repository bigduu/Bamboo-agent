//! Passive, verified package-owned Nova.app setup. No PATH lookup or desktop launch.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use actix_web::{web, HttpResponse};
use bamboo_config::PluginTrustConfig;
use bamboo_domain::mcp_config::{McpServerConfig, TransportConfig};
use bamboo_plugin::{
    InstalledPlugin, InstalledPlugins, McpTransportManifest, Platform, PluginInstallStatus,
    PluginManifest, PluginSource,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use super::errors::plugin_error_response;
use crate::app_state::AppState;
use crate::plugin_installer::ServerPluginInstaller;

const NOVA_PUBLIC_KEY: &str = "e3c429e1be50098b12c6f45737abf457189b668535875b5b3e2b4349be86ea59";
const MAX_ARCHIVE: u64 = 128 * 1024 * 1024;
const MAX_OUTPUT: u64 = 64 * 1024;
const STATUS_TIMEOUT: Duration = Duration::from_secs(10);
const INSTALL_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Clone, Deserialize)]
struct ConnectorRelease {
    version: String,
    bundle_url: String,
    bundle_sha256: String,
    manifest_sha256: String,
    executable_sha256: BTreeMap<String, String>,
    #[serde(default)]
    legacy_cli: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct AppArtifact {
    artifact_id: String,
    version: String,
    url: String,
    sha256: String,
    signing: String,
    install_mode: String,
    development: bool,
}

#[derive(Clone, Deserialize)]
struct Catalog {
    connectors: Vec<ConnectorRelease>,
    app: Option<AppArtifact>,
}

fn accepted_catalog() -> Catalog {
    // Only published, signature-verified connector and app receipts belong here.
    serde_json::from_str(include_str!("official-nova.json"))
        .expect("reviewed Nova catalog is valid JSON")
}

fn valid_artifact(artifact: &AppArtifact) -> bool {
    let parts: Vec<_> = artifact.version.split('.').collect();
    parts.len() == 3
        && parts.iter().all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
        && !artifact.artifact_id.is_empty()
        && artifact.artifact_id.len() <= 128
        && artifact.sha256.len() == 64
        && artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        && artifact.signing == "ad_hoc"
        && artifact.install_mode == "local_ad_hoc"
        && artifact.development
        && artifact.url == format!("https://github.com/bigduu/Nova/releases/download/v{0}/nova-v{0}-universal-apple-darwin-development-app.zip", artifact.version)
}

#[derive(Clone, Serialize)]
struct Host {
    os: String,
    arch: String,
    supported: bool,
    reason: Option<&'static str>,
}

impl Host {
    fn detect() -> Self {
        Self::for_platform(std::env::consts::OS, std::env::consts::ARCH)
    }

    fn for_platform(os: &str, arch: &str) -> Self {
        let reason = match (os, arch) {
            ("macos", "aarch64" | "x86_64") => None,
            ("windows", _) => Some("app_not_required"),
            _ => Some("unsupported_host"),
        };
        Self {
            os: os.into(),
            arch: arch.into(),
            supported: reason.is_none(),
            reason,
        }
    }
}

#[derive(Serialize)]
struct Connector {
    state: &'static str,
    version: Option<String>,
    reason: Option<&'static str>,
}

#[derive(Serialize)]
struct Availability {
    available: bool,
    reason: Option<&'static str>,
}

#[derive(Serialize)]
struct Capability {
    supported: bool,
    reason: Option<&'static str>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum AppStateKind {
    Missing,
    Installed,
    AlreadyInstalled,
    Unsupported,
    Error,
}

/// Nova's finite setup report, not an MCP connection or a permission probe.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AppReport {
    success: bool,
    state: AppStateKind,
    message: String,
    next_action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    signing: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    development: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connector_compatible: Option<bool>,
}

#[derive(Serialize)]
struct Setup {
    plugin_id: &'static str,
    host: Host,
    connector: Connector,
    capability: Capability,
    app: Option<AppReport>,
    install: Availability,
    artifact: Option<AppArtifact>,
    manual_guidance: String,
    desktop_server_id: Option<String>,
    desktop_readiness: &'static str,
}

struct Inspection {
    report: Setup,
    command: Option<PathBuf>,
}

fn trust_reason(trust: &PluginTrustConfig, url: &str) -> Option<&'static str> {
    if !trust.trusted_keys.iter().any(|key| {
        key.algorithm.eq_ignore_ascii_case("ed25519")
            && key.public_key.eq_ignore_ascii_case(NOVA_PUBLIC_KEY)
    }) {
        Some("publisher_not_trusted")
    } else if !trust.is_host_trusted(url) {
        Some("source_not_trusted")
    } else {
        None
    }
}

fn plain(metadata: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return false;
        }
    }
    !metadata.file_type().is_symlink()
}

async fn regular_hash(path: &Path, expected: &str, max: u64) -> Result<(), ()> {
    let metadata = tokio::fs::symlink_metadata(path).await.map_err(|_| ())?;
    if !plain(&metadata) || !metadata.is_file() || metadata.len() == 0 || metadata.len() > max {
        return Err(());
    }
    let mut reader = tokio::fs::File::open(path)
        .await
        .map_err(|_| ())?
        .take(max + 1);
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 16384];
    let mut total = 0;
    loop {
        let count = reader.read(&mut buffer).await.map_err(|_| ())?;
        if count == 0 {
            break;
        }
        total += count as u64;
        if total > max {
            return Err(());
        }
        hasher.update(&buffer[..count]);
    }
    if total != metadata.len() || !hex::encode(hasher.finalize()).eq_ignore_ascii_case(expected) {
        return Err(());
    }
    Ok(())
}

async fn validated_cli(
    state: &AppState,
    entry: &InstalledPlugin,
    release: &ConnectorRelease,
    os: &str,
) -> Result<(PathBuf, PluginManifest), ()> {
    if entry.status != PluginInstallStatus::Installed
        || entry.id != "nova"
        || entry.version != release.version
        || !matches!(&entry.source, PluginSource::Url {
            url, sha256: Some(hash), signed_by: Some(_), allow_unverified: false,
            allow_unsigned: false, allow_untrusted_host: false, insecure: false
        } if url == &release.bundle_url && hash.eq_ignore_ascii_case(&release.bundle_sha256))
    {
        return Err(());
    }
    let root = tokio::fs::canonicalize(&state.app_data_dir)
        .await
        .map_err(|_| ())?;
    let mut directory = root.clone();
    for component in ["plugins", "nova", "bin", os] {
        directory.push(component);
        let metadata = tokio::fs::symlink_metadata(&directory)
            .await
            .map_err(|_| ())?;
        if !plain(&metadata) || !metadata.is_dir() {
            return Err(());
        }
    }
    if entry.plugin_dir != state.app_data_dir.join("plugins/nova")
        || tokio::fs::canonicalize(&entry.plugin_dir)
            .await
            .map_err(|_| ())?
            != root.join("plugins/nova")
    {
        return Err(());
    }
    let manifest_path = root.join("plugins/nova/plugin.json");
    regular_hash(&manifest_path, &release.manifest_sha256, 64 * 1024).await?;
    let manifest: PluginManifest =
        serde_json::from_slice(&tokio::fs::read(&manifest_path).await.map_err(|_| ())?)
            .map_err(|_| ())?;
    manifest.validate().map_err(|_| ())?;
    if manifest.id != "nova"
        || manifest.version != entry.version
        || desktop_entry(&manifest).is_none()
    {
        return Err(());
    }
    let command = directory.join(if os == "windows" { "nova.exe" } else { "nova" });
    regular_hash(
        &command,
        release.executable_sha256.get(os).ok_or(())?,
        MAX_ARCHIVE,
    )
    .await?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if tokio::fs::metadata(&command)
            .await
            .map_err(|_| ())?
            .permissions()
            .mode()
            & 0o111
            == 0
        {
            return Err(());
        }
    }
    Ok((command, manifest))
}

fn desktop_entry(manifest: &PluginManifest) -> Option<&bamboo_plugin::McpServerManifestEntry> {
    let mut entries = manifest
        .provides
        .mcp_servers
        .iter()
        .filter(|entry| entry.id == "nova");
    let entry = entries.next()?;
    if entries.next().is_some() || !entry.enabled {
        return None;
    }
    matches!(&entry.transport, McpTransportManifest::Stdio { command, args, cwd: None, env }
        if command == "${platform_bin}" && args == &["mcp"] && env.is_empty())
    .then_some(entry)
}

fn desktop_server_id(
    entry: &InstalledPlugin,
    manifest: &PluginManifest,
    os: &str,
    servers: &[McpServerConfig],
) -> Option<String> {
    // Match the installer's literal token resolution, not canonical aliases
    // (Windows extended paths and macOS /var vs /private/var differ).
    let expected = desktop_entry(manifest)?
        .resolve(&entry.plugin_dir, &manifest.id, Platform::parse(os)?)
        .ok()?;
    let TransportConfig::Stdio(expected) = expected.transport else {
        return None;
    };
    if !entry
        .registered
        .mcp_server_ids
        .iter()
        .any(|id| id == "nova")
    {
        return None;
    }
    let mut matches = servers.iter().filter(|server| server.id == "nova");
    let server = matches.next()?;
    if matches.next().is_some() || !server.enabled {
        return None;
    }
    match &server.transport {
        TransportConfig::Stdio(config)
            if config.command == expected.command
                && config.args == expected.args
                && config.cwd == expected.cwd
                && config.env == expected.env
                && config.env_encrypted.is_empty()
                && config.env_credential_refs.is_empty() =>
        {
            Some(server.id.clone())
        }
        _ => None,
    }
}

async fn read_bounded(reader: impl AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(MAX_OUTPUT + 1).read_to_end(&mut bytes).await?;
    if bytes.len() as u64 > MAX_OUTPUT {
        return Err(std::io::Error::other("Nova CLI output exceeded the limit"));
    }
    Ok(bytes)
}

/// A separate group keeps timed-out helpers from surviving the owned CLI.
struct ProcessGroup(Option<u32>);
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        let _ = self.0;
        #[cfg(unix)]
        if let Some(group) = self.0.and_then(|id| i32::try_from(id).ok()) {
            // SAFETY: this is the dedicated process group created for our child.
            let _ = unsafe { libc::killpg(group, libc::SIGKILL) };
        }
    }
}

async fn run_cli(
    command: &Path,
    args: &[String],
    timeout: Duration,
    legacy: bool,
) -> Result<AppReport, &'static str> {
    let mut process = tokio::process::Command::new(command);
    process
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    process.process_group(0);
    let mut child = process.spawn().map_err(|_| "status_failed")?;
    let group = ProcessGroup(child.id());
    let stdout = child.stdout.take().expect("piped Nova stdout");
    let stderr = child.stderr.take().expect("piped Nova stderr");
    let result = tokio::time::timeout(timeout, async {
        tokio::try_join!(read_bounded(stdout), read_bounded(stderr), child.wait())
    })
    .await;
    // Always finish child/group cleanup before releasing the plugin operation lock.
    drop(group);
    let _ = child.kill().await;
    let _ = child.wait().await;
    let (stdout, stderr, status) = result
        .map_err(|_| "status_timeout")?
        .map_err(|_| "invalid_report")?;
    let report: AppReport = serde_json::from_slice(&stdout).map_err(|_| {
        if legacy
            && !status.success()
            && String::from_utf8_lossy(&stderr).contains("unrecognized subcommand 'app'")
        {
            "legacy_cli_unavailable"
        } else {
            "invalid_report"
        }
    })?;
    if status.success() != report.success
        || report.message.is_empty()
        || report.next_action.is_empty()
        || report.message.len() > 8192
        || report.next_action.len() > 8192
        || report
            .signing
            .as_deref()
            .is_some_and(|signing| !matches!(signing, "ad_hoc" | "developer_id" | "other"))
        || (report.success
            && !matches!(
                report.state,
                AppStateKind::Missing | AppStateKind::Installed
            ))
        || (!report.success
            && matches!(
                report.state,
                AppStateKind::Missing | AppStateKind::Installed
            ))
        || (report.state == AppStateKind::Installed
            && (report.version.is_none()
                || report.path.is_none()
                || report.signing.is_none()
                || report.development.is_none()
                || report.connector_compatible.is_none()))
    {
        return Err("invalid_report");
    }
    Ok(report)
}

fn manual_guidance(reason: Option<&str>) -> String {
    match reason {
        Some("app_not_required") => "Windows uses the packaged Nova MCP server directly; no separate Nova.app is required.",
        Some("legacy_cli_unavailable") => "This verified connector does not support Nova.app setup. Upgrade the official Nova plugin to a reviewed release with structured app setup, or follow its release guide manually.",
        Some("unsupported_host") => "Nova.app setup supports macOS Apple Silicon and Intel only.",
        Some("artifact_unavailable") => "No matching published app archive has been accepted yet. Follow the official Nova release guide; do not substitute a moving latest URL.",
        _ => "Install a verified official Nova plugin first. Setup never launches Nova.app or grants privacy permissions. After explicit local ad-hoc installation, open Nova.app and grant permissions yourself, then reconnect only its owned desktop MCP server. No keychain or certificate is created; permission continuity across upgrades is not guaranteed. If an install response is lost, refresh status before retrying.",
    }.into()
}

async fn inspect_locked(
    state: &web::Data<AppState>,
    host: Host,
    catalog: &Catalog,
) -> Result<Inspection, bamboo_plugin::PluginError> {
    let registry =
        InstalledPlugins::load(&state.app_data_dir.join("plugins/installed.json")).await?;
    let entry = registry.get_unique("nova")?;
    let mut connector = Connector {
        state: "missing",
        version: entry.map(|entry| entry.version.clone()),
        reason: Some("connector_missing"),
    };
    let mut capability = Capability {
        supported: false,
        reason: host.reason.or(Some("connector_missing")),
    };
    let mut app = None;
    let mut command = None;
    let mut desktop_id = None;
    let config = state.config.read().await;
    if let Some(entry) = entry {
        connector.state = if entry.status == PluginInstallStatus::Installing {
            "installing"
        } else {
            "unverified"
        };
        connector.reason = Some(if entry.status == PluginInstallStatus::Installing {
            "incomplete_install"
        } else {
            "unverified_install"
        });
        if let Some(release) = catalog
            .connectors
            .iter()
            .find(|release| release.version == entry.version)
        {
            connector.reason =
                trust_reason(&config.plugin_trust, &release.bundle_url).or(connector.reason);
            let eligible_abi = matches!(
                (host.os.as_str(), host.arch.as_str()),
                ("macos", "x86_64" | "aarch64") | ("windows", "x86_64")
            );
            if eligible_abi && trust_reason(&config.plugin_trust, &release.bundle_url).is_none() {
                if let Ok((binary, manifest)) = validated_cli(state, entry, release, &host.os).await
                {
                    connector.state = "verified";
                    connector.reason = None;
                    desktop_id = desktop_server_id(entry, &manifest, &host.os, &config.mcp.servers);
                    command = Some(binary);
                }
            }
        }
    }
    let trust = config.plugin_trust.clone();
    drop(config);
    if host.supported {
        capability.reason = connector.reason;
        if let (Some(binary), Some(entry)) = (&command, entry) {
            let legacy = catalog
                .connectors
                .iter()
                .find(|r| r.version == entry.version)
                .is_some_and(|r| r.legacy_cli);
            match run_cli(
                binary,
                &["app".into(), "status".into()],
                STATUS_TIMEOUT,
                legacy,
            )
            .await
            {
                Ok(report) => {
                    capability.supported = true;
                    capability.reason = None;
                    app = Some(report);
                }
                Err(reason) => capability.reason = Some(reason),
            }
        }
    }
    let artifact = catalog.app.clone();
    let unavailable = host
        .reason
        .or(connector.reason)
        .or(capability.reason)
        .or_else(|| {
            artifact
                .as_ref()
                .map_or(Some("artifact_unavailable"), |artifact| {
                    if !valid_artifact(artifact) {
                        Some("invalid_artifact_metadata")
                    } else if entry.is_none_or(|entry| entry.version != artifact.version) {
                        Some("connector_version_mismatch")
                    } else {
                        trust_reason(&trust, &artifact.url)
                    }
                })
        })
        .or_else(|| {
            (!app
                .as_ref()
                .is_some_and(|report| report.success && report.state == AppStateKind::Missing))
            .then_some("app_not_missing")
        });
    let guidance = app
        .as_ref()
        .map(|report| report.next_action.clone())
        .unwrap_or_else(|| manual_guidance(unavailable));
    Ok(Inspection {
        command,
        report: Setup {
            plugin_id: "nova",
            host,
            connector,
            capability,
            app,
            install: Availability {
                available: unavailable.is_none(),
                reason: unavailable,
            },
            artifact,
            manual_guidance: guidance,
            desktop_server_id: desktop_id,
            desktop_readiness: "not_tested",
        },
    })
}

async fn download_archive(
    artifact: &AppArtifact,
) -> Result<(tempfile::TempDir, PathBuf), &'static str> {
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::limited(5))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|_| "download_failed")?;
    let response = client
        .get(&artifact.url)
        .send()
        .await
        .map_err(|_| "download_failed")?;
    save_archive(response, &artifact.sha256, MAX_ARCHIVE).await
}

async fn save_archive(
    response: reqwest::Response,
    expected_sha256: &str,
    max: u64,
) -> Result<(tempfile::TempDir, PathBuf), &'static str> {
    if !response.status().is_success() {
        return Err("download_failed");
    }
    if response.content_length().is_some_and(|size| size > max) {
        return Err("download_too_large");
    }
    let temporary = tempfile::tempdir().map_err(|_| "download_failed")?;
    let archive = temporary.path().join("nova-app.zip");
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(&archive)
        .await
        .map_err(|_| "download_failed")?;
    let mut stream = response.bytes_stream();
    let mut total = 0u64;
    let mut hasher = Sha256::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| "download_failed")?;
        total += chunk.len() as u64;
        if total > max {
            return Err("download_too_large");
        }
        hasher.update(&chunk);
        file.write_all(&chunk)
            .await
            .map_err(|_| "download_failed")?;
    }
    file.sync_all().await.map_err(|_| "download_failed")?;
    if total == 0 || !hex::encode(hasher.finalize()).eq_ignore_ascii_case(expected_sha256) {
        return Err("archive_checksum_mismatch");
    }
    Ok((temporary, archive))
}

pub async fn nova_app(state: web::Data<AppState>) -> HttpResponse {
    let installer = ServerPluginInstaller::new(state.clone());
    let _guard = installer.begin_operation().await;
    match inspect_locked(&state, Host::detect(), &accepted_catalog()).await {
        Ok(inspection) => HttpResponse::Ok().json(inspection.report),
        Err(error) => plugin_error_response(&error),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppInstallRequest {
    artifact_id: Option<String>,
}

async fn install_owned(
    state: web::Data<AppState>,
    request: AppInstallRequest,
) -> Result<Setup, &'static str> {
    let installer = ServerPluginInstaller::new(state.clone());
    let _guard = installer.begin_operation().await;
    let catalog = accepted_catalog();
    let inspection = inspect_locked(&state, Host::detect(), &catalog)
        .await
        .map_err(|_| "invalid_registry")?;
    if !inspection.report.install.available {
        return Err(inspection
            .report
            .install
            .reason
            .unwrap_or("setup_unavailable"));
    }
    let artifact = catalog.app.as_ref().ok_or("artifact_unavailable")?;
    if request
        .artifact_id
        .as_ref()
        .is_some_and(|id| id != &artifact.artifact_id)
    {
        return Err("artifact_changed");
    }
    inspection.command.ok_or("unverified_install")?;
    let (_temporary, archive) = download_archive(artifact).await?;
    install_downloaded(&state, Host::detect(), &catalog, artifact, &archive).await
}

async fn install_downloaded(
    state: &web::Data<AppState>,
    host: Host,
    catalog: &Catalog,
    artifact: &AppArtifact,
    archive: &Path,
) -> Result<Setup, &'static str> {
    // Policy, managed bytes and app existence may have changed during download.
    let fresh = inspect_locked(state, host.clone(), catalog)
        .await
        .map_err(|_| "invalid_registry")?;
    if !fresh.report.install.available {
        return Err(fresh.report.install.reason.unwrap_or("setup_unavailable"));
    }
    let command = fresh.command.ok_or("unverified_install")?;
    let report = run_cli(
        &command,
        &[
            "app".into(),
            "install".into(),
            "--archive".into(),
            archive.to_str().ok_or("invalid_archive_path")?.into(),
            "--sha256".into(),
            artifact.sha256.clone(),
            "--local-sign".into(),
        ],
        INSTALL_TIMEOUT,
        false,
    )
    .await?;
    if !report.success || report.state != AppStateKind::Installed {
        return Err(if report.state == AppStateKind::AlreadyInstalled {
            "already_installed"
        } else {
            "install_failed"
        });
    }
    let fresh = inspect_locked(state, host, catalog)
        .await
        .map_err(|_| "invalid_registry")?
        .report;
    if !fresh.app.as_ref().is_some_and(|report| {
        report.success
            && report.state == AppStateKind::Installed
            && report.version.as_deref() == Some(artifact.version.as_str())
            && report.connector_compatible == Some(true)
            && report.signing.as_deref() == Some("ad_hoc")
            && report.development == Some(true)
    }) {
        return Err("install_unconfirmed");
    }
    Ok(fresh)
}

/// Explicit user action; the owned bounded operation survives a lost HTTP response.
pub async fn install_nova_app(
    state: web::Data<AppState>,
    body: web::Json<AppInstallRequest>,
) -> HttpResponse {
    let request = body.into_inner();
    match actix_web::rt::spawn(install_owned(state, request)).await {
        Ok(Ok(report)) => HttpResponse::Created().json(report),
        result => {
            let reason = match result {
                Ok(Err(reason)) => reason,
                _ => "install_failed",
            };
            if reason == "artifact_changed" {
                HttpResponse::BadRequest().json(serde_json::json!({"error":{"type":"official_nova_unavailable","reason":reason}}))
            } else {
                HttpResponse::Conflict().json(serde_json::json!({"error":{"type":"official_nova_unavailable","reason":reason}}))
            }
        }
    }
}

#[cfg(test)]
mod tests;
