use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zip::ZipArchive;

use bamboo_config::paths::bamboo_dir;

include!(concat!(env!("OUT_DIR"), "/frontend_package_embedded.rs"));

pub const DUPLICATE_FRONTEND_DIR_NAME: &str = "frontend";
pub const DUPLICATE_FRONTEND_MANIFEST_NAME: &str = ".frontend-manifest.json";
pub const FRONTEND_PACKAGE_ENV: &str = "BAMBOO_FRONTEND_PACKAGE";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrontendPackageManifest {
    pub schema_version: u32,
    pub frontend_name: String,
    pub frontend_version: String,
    pub bundle_hash: String,
    pub built_at: DateTime<Utc>,
    pub entry: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendPackageStatus {
    pub package_path: Option<PathBuf>,
    pub frontend_dir: PathBuf,
    pub local_manifest_path: PathBuf,
    pub bundled_manifest: FrontendPackageManifest,
    pub local_manifest: Option<FrontendPackageManifest>,
    pub refreshed: bool,
}

pub fn duplicate_frontend_dir_in(bamboo_home_dir: &Path) -> PathBuf {
    bamboo_home_dir.join(DUPLICATE_FRONTEND_DIR_NAME)
}

pub fn duplicate_frontend_dir() -> PathBuf {
    duplicate_frontend_dir_in(&bamboo_dir())
}

pub fn duplicate_frontend_manifest_path_in(bamboo_home_dir: &Path) -> PathBuf {
    duplicate_frontend_dir_in(bamboo_home_dir).join(DUPLICATE_FRONTEND_MANIFEST_NAME)
}

pub fn duplicate_frontend_manifest_path() -> PathBuf {
    duplicate_frontend_manifest_path_in(&bamboo_dir())
}

pub fn has_embedded_frontend_package() -> bool {
    DUPLICATE_FRONTEND_PACKAGE_ZIP.is_some() && DUPLICATE_FRONTEND_PACKAGE_MANIFEST.is_some()
}

fn frontend_package_candidates_under(base_dir: &Path) -> Vec<PathBuf> {
    vec![
        base_dir.join("frontend_package/lotus-frontend.zip"),
        base_dir.join(".frontend-package/lotus-frontend.zip"),
        base_dir.join("bodhi/.frontend-package/lotus-frontend.zip"),
        base_dir.join("../frontend_package/lotus-frontend.zip"),
        base_dir.join("../.frontend-package/lotus-frontend.zip"),
        base_dir.join("../bodhi/.frontend-package/lotus-frontend.zip"),
    ]
}

pub fn frontend_package_env_is_configured() -> bool {
    std::env::var_os(FRONTEND_PACKAGE_ENV).is_some()
}

fn resolve_discovered_frontend_package_path() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    if let Ok(dir) = std::env::current_dir() {
        candidates.extend(frontend_package_candidates_under(&dir));
    }

    if let Ok(current_exe) = std::env::current_exe() {
        if let Some(exe_dir) = current_exe.parent() {
            candidates.extend(frontend_package_candidates_under(exe_dir));
            candidates.push(exe_dir.join("../Resources/frontend_package/lotus-frontend.zip"));
            candidates.push(exe_dir.join("../Resources/.frontend-package/lotus-frontend.zip"));
        }
    }

    candidates.into_iter().find(|path| path.is_file())
}

fn resolve_configured_frontend_package_path(
    explicit_path: Option<&Path>,
) -> Result<Option<PathBuf>, FrontendPackageError> {
    if let Some(path) = explicit_path {
        if path.is_file() {
            return Ok(Some(path.to_path_buf()));
        }
        return Err(FrontendPackageError::ConfiguredPackageNotFound(
            path.to_path_buf(),
        ));
    }

    match std::env::var(FRONTEND_PACKAGE_ENV) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(FrontendPackageError::InvalidConfiguredPackagePath(format!(
                    "{FRONTEND_PACKAGE_ENV} is empty"
                )));
            }
            let path = PathBuf::from(trimmed);
            if path.is_file() {
                Ok(Some(path))
            } else {
                Err(FrontendPackageError::ConfiguredPackageNotFound(path))
            }
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(FrontendPackageError::InvalidConfiguredPackagePath(format!(
                "{FRONTEND_PACKAGE_ENV} must be valid UTF-8"
            )))
        }
    }
}

pub fn resolve_frontend_package_path(explicit_path: Option<&Path>) -> Option<PathBuf> {
    if let Some(path) = explicit_path {
        return path.is_file().then(|| path.to_path_buf());
    }

    if let Ok(raw) = std::env::var(FRONTEND_PACKAGE_ENV) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            let candidate = PathBuf::from(trimmed);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }

    resolve_discovered_frontend_package_path()
}

pub fn read_bundled_manifest(
    package_path: Option<&Path>,
) -> Result<FrontendPackageManifest, FrontendPackageError> {
    if let Some(package_path) = package_path {
        let embedded_manifest = read_bundled_manifest_bytes_from_zip(package_path)?;
        let sidecar_manifest = package_path.with_file_name("frontend-manifest.json");
        if sidecar_manifest.exists() {
            let sidecar_bytes =
                std::fs::read(sidecar_manifest).map_err(FrontendPackageError::Io)?;
            if sidecar_bytes != embedded_manifest {
                return Err(FrontendPackageError::ManifestMismatch);
            }
        }
        let manifest = parse_frontend_package_manifest(&embedded_manifest)?;
        validate_external_frontend_archive(package_path, &manifest)?;
        return Ok(manifest);
    }

    if let Some(bytes) = DUPLICATE_FRONTEND_PACKAGE_MANIFEST {
        return parse_frontend_package_manifest(bytes);
    }

    Err(FrontendPackageError::PackageNotFound)
}

pub fn read_bundled_manifest_from_zip(
    package_path: &Path,
) -> Result<FrontendPackageManifest, FrontendPackageError> {
    let bytes = read_bundled_manifest_bytes_from_zip(package_path)?;
    let manifest = parse_frontend_package_manifest(&bytes)?;
    validate_external_frontend_archive(package_path, &manifest)?;
    Ok(manifest)
}

fn parse_frontend_package_manifest(
    bytes: &[u8],
) -> Result<FrontendPackageManifest, FrontendPackageError> {
    let manifest: FrontendPackageManifest =
        serde_json::from_slice(bytes).map_err(FrontendPackageError::Json)?;
    if manifest.schema_version != 1 {
        return Err(FrontendPackageError::InvalidManifest(format!(
            "unsupported schema_version {}; expected 1",
            manifest.schema_version
        )));
    }
    if manifest.frontend_name.trim().is_empty() || manifest.frontend_version.trim().is_empty() {
        return Err(FrontendPackageError::InvalidManifest(
            "frontend name and version must be non-empty".to_string(),
        ));
    }
    if manifest.entry != "index.html" {
        return Err(FrontendPackageError::InvalidManifest(format!(
            "entry must be index.html, got {:?}",
            manifest.entry
        )));
    }
    let Some(hash) = manifest.bundle_hash.strip_prefix("sha256:") else {
        return Err(FrontendPackageError::InvalidManifest(
            "bundle_hash must use sha256:<hex>".to_string(),
        ));
    };
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(FrontendPackageError::InvalidManifest(
            "bundle_hash must contain exactly 64 hexadecimal digits".to_string(),
        ));
    }
    Ok(manifest)
}

fn read_bundled_manifest_bytes_from_zip(
    package_path: &Path,
) -> Result<Vec<u8>, FrontendPackageError> {
    let file = File::open(package_path).map_err(FrontendPackageError::Io)?;
    let mut archive = ZipArchive::new(file).map_err(FrontendPackageError::Zip)?;
    let mut manifest_file = archive
        .by_name("frontend-manifest.json")
        .map_err(FrontendPackageError::Zip)?;
    let mut bytes = Vec::new();
    manifest_file
        .read_to_end(&mut bytes)
        .map_err(FrontendPackageError::Io)?;
    Ok(bytes)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExternalArchiveEntryKind {
    File,
    Directory,
}

#[derive(Debug)]
struct ExternalArchiveFile {
    index: usize,
    path: String,
}

fn is_windows_reserved_name(component: &str) -> bool {
    let stem = component
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    let numbered_device = stem
        .strip_prefix("COM")
        .or_else(|| stem.strip_prefix("LPT"));

    matches!(
        stem.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || matches!(
        numbered_device,
        Some("1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³")
    )
}

fn validate_external_archive_path(
    raw_name: &str,
    kind: ExternalArchiveEntryKind,
) -> Result<String, FrontendPackageError> {
    let path = match kind {
        ExternalArchiveEntryKind::Directory => raw_name
            .strip_suffix('/')
            .ok_or_else(|| FrontendPackageError::InvalidArchivePath(raw_name.to_string()))?,
        ExternalArchiveEntryKind::File if raw_name.ends_with('/') => {
            return Err(FrontendPackageError::InvalidArchivePath(
                raw_name.to_string(),
            ));
        }
        ExternalArchiveEntryKind::File => raw_name,
    };

    if path.is_empty()
        || path.starts_with('/')
        || path.contains(['\\', ':', '\0'])
        || path.chars().any(char::is_control)
    {
        return Err(FrontendPackageError::InvalidArchivePath(
            raw_name.to_string(),
        ));
    }

    for component in path.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.ends_with(['.', ' '])
            || component
                .chars()
                .any(|character| matches!(character, '<' | '>' | '"' | '|' | '?' | '*'))
            || is_windows_reserved_name(component)
        {
            return Err(FrontendPackageError::InvalidArchivePath(
                raw_name.to_string(),
            ));
        }
    }

    Ok(path.to_string())
}

fn portable_archive_key(path: &str) -> String {
    path.to_lowercase()
}

fn register_external_archive_path(
    path: &str,
    kind: ExternalArchiveEntryKind,
    entries: &mut HashMap<String, (String, ExternalArchiveEntryKind)>,
    required_directories: &mut HashMap<String, String>,
) -> Result<(), FrontendPackageError> {
    for (index, _) in path.match_indices('/') {
        let parent = &path[..index];
        let key = portable_archive_key(parent);
        if let Some((existing, existing_kind)) = entries.get(&key) {
            if *existing_kind == ExternalArchiveEntryKind::File || existing != parent {
                return Err(FrontendPackageError::InvalidArchivePath(path.to_string()));
            }
        }
        if let Some(existing) = required_directories.get(&key) {
            if existing != parent {
                return Err(FrontendPackageError::InvalidArchivePath(path.to_string()));
            }
        } else {
            required_directories.insert(key, parent.to_string());
        }
    }

    let key = portable_archive_key(path);
    if kind == ExternalArchiveEntryKind::File && required_directories.contains_key(&key) {
        return Err(FrontendPackageError::InvalidArchivePath(path.to_string()));
    } else if kind == ExternalArchiveEntryKind::Directory {
        if let Some(required) = required_directories.get(&key) {
            if required != path {
                return Err(FrontendPackageError::InvalidArchivePath(path.to_string()));
            }
        }
    }
    if let Some((existing, existing_kind)) = entries.get(&key) {
        if existing != path || *existing_kind != kind || kind == ExternalArchiveEntryKind::File {
            return Err(FrontendPackageError::InvalidArchivePath(path.to_string()));
        }
        return Ok(());
    }

    entries.insert(key, (path.to_string(), kind));
    Ok(())
}

fn validate_external_frontend_archive(
    package_path: &Path,
    manifest: &FrontendPackageManifest,
) -> Result<(), FrontendPackageError> {
    let file = File::open(package_path).map_err(FrontendPackageError::Io)?;
    let mut archive = ZipArchive::new(file).map_err(FrontendPackageError::Zip)?;
    let mut contains_entry = false;
    let mut entries = HashMap::new();
    let mut required_directories = HashMap::new();
    let mut content_files = Vec::new();

    for index in 0..archive.len() {
        let entry = archive.by_index(index).map_err(FrontendPackageError::Zip)?;
        let kind = if entry.is_dir() {
            ExternalArchiveEntryKind::Directory
        } else {
            ExternalArchiveEntryKind::File
        };
        let raw_name = entry.name().to_string();
        let normalized = validate_external_archive_path(&raw_name, kind)?;
        let enclosed = entry
            .enclosed_name()
            .map(|path| path.to_path_buf())
            .ok_or_else(|| FrontendPackageError::InvalidArchivePath(entry.name().to_string()))?;
        let enclosed_portable = enclosed
            .iter()
            .map(|component| component.to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        if enclosed_portable != normalized {
            return Err(FrontendPackageError::InvalidArchivePath(raw_name));
        }
        register_external_archive_path(&normalized, kind, &mut entries, &mut required_directories)?;

        if kind == ExternalArchiveEntryKind::File && normalized == manifest.entry {
            contains_entry = true;
        }
        if kind == ExternalArchiveEntryKind::File && normalized != "frontend-manifest.json" {
            content_files.push(ExternalArchiveFile {
                index,
                path: normalized,
            });
        }
    }

    if !contains_entry {
        return Err(FrontendPackageError::MissingEntry(
            package_path.with_file_name(&manifest.entry),
        ));
    }

    content_files.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    for content_file in content_files {
        hasher.update(content_file.path.as_bytes());
        hasher.update([0]);
        let mut entry = archive
            .by_index(content_file.index)
            .map_err(FrontendPackageError::Zip)?;
        loop {
            let read = entry.read(&mut buffer).map_err(FrontendPackageError::Io)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        hasher.update([0]);
    }
    let actual_hash = format!("sha256:{}", hex::encode(hasher.finalize()));
    if actual_hash != manifest.bundle_hash {
        return Err(FrontendPackageError::BundleHashMismatch {
            expected: manifest.bundle_hash.clone(),
            actual: actual_hash,
        });
    }
    Ok(())
}

pub fn read_local_manifest(
    manifest_path: &Path,
) -> Result<Option<FrontendPackageManifest>, FrontendPackageError> {
    if !manifest_path.exists() {
        return Ok(None);
    }
    let file = File::open(manifest_path).map_err(FrontendPackageError::Io)?;
    let manifest = serde_json::from_reader(file).map_err(FrontendPackageError::Json)?;
    Ok(Some(manifest))
}

pub fn should_refresh_frontend(
    bundled: &FrontendPackageManifest,
    local: Option<&FrontendPackageManifest>,
    frontend_dir: &Path,
) -> bool {
    let Some(local) = local else {
        return true;
    };

    if bundled.schema_version != local.schema_version {
        return true;
    }
    if bundled.frontend_version != local.frontend_version {
        return true;
    }
    if bundled.bundle_hash != local.bundle_hash {
        return true;
    }
    if !frontend_dir.join(&bundled.entry).is_file() {
        return true;
    }

    false
}

pub fn ensure_current_frontend_dir_in(
    bamboo_home_dir: &Path,
    explicit_package_path: Option<&Path>,
) -> Result<FrontendPackageStatus, FrontendPackageError> {
    let configured_package_path = resolve_configured_frontend_package_path(explicit_package_path)?;
    let package_path = if configured_package_path.is_some() {
        configured_package_path
    } else if has_embedded_frontend_package() {
        None
    } else {
        Some(
            resolve_discovered_frontend_package_path()
                .ok_or(FrontendPackageError::PackageNotFound)?,
        )
    };

    let bundled_manifest = read_bundled_manifest(package_path.as_deref())?;
    let frontend_dir = duplicate_frontend_dir_in(bamboo_home_dir);
    let local_manifest_path = duplicate_frontend_manifest_path_in(bamboo_home_dir);
    let local_manifest = read_local_manifest(&local_manifest_path)?;

    let refresh_needed =
        should_refresh_frontend(&bundled_manifest, local_manifest.as_ref(), &frontend_dir);

    if refresh_needed {
        refresh_frontend_dir(package_path.as_deref(), &bundled_manifest, &frontend_dir)?;
    }

    Ok(FrontendPackageStatus {
        package_path,
        frontend_dir,
        local_manifest_path,
        bundled_manifest,
        local_manifest,
        refreshed: refresh_needed,
    })
}

pub fn ensure_current_frontend_dir(
    explicit_package_path: Option<&Path>,
) -> Result<FrontendPackageStatus, FrontendPackageError> {
    ensure_current_frontend_dir_in(&bamboo_dir(), explicit_package_path)
}

fn refresh_frontend_dir(
    package_path: Option<&Path>,
    bundled_manifest: &FrontendPackageManifest,
    frontend_dir: &Path,
) -> Result<(), FrontendPackageError> {
    let parent = frontend_dir
        .parent()
        .ok_or_else(|| FrontendPackageError::InvalidTarget(frontend_dir.to_path_buf()))?;
    std::fs::create_dir_all(parent).map_err(FrontendPackageError::Io)?;

    let temp_dir = parent.join(format!(
        ".{}-tmp-{}",
        DUPLICATE_FRONTEND_DIR_NAME,
        bundled_manifest.frontend_version.replace('/', "-")
    ));
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir).map_err(FrontendPackageError::Io)?;
    }
    std::fs::create_dir_all(&temp_dir).map_err(FrontendPackageError::Io)?;

    if let Some(package_path) = package_path {
        extract_frontend_zip(package_path, &temp_dir)?;
    } else if let Some(bytes) = DUPLICATE_FRONTEND_PACKAGE_ZIP {
        extract_frontend_zip_bytes(bytes, &temp_dir)?;
    } else {
        return Err(FrontendPackageError::PackageNotFound);
    }

    let extracted_entry = temp_dir.join(&bundled_manifest.entry);
    if !extracted_entry.is_file() {
        return Err(FrontendPackageError::MissingEntry(extracted_entry));
    }

    let manifest_path = temp_dir.join(DUPLICATE_FRONTEND_MANIFEST_NAME);
    let manifest_file = File::create(&manifest_path).map_err(FrontendPackageError::Io)?;
    serde_json::to_writer_pretty(manifest_file, bundled_manifest)
        .map_err(FrontendPackageError::Json)?;

    let old_dir = parent.join(format!("{}.old", DUPLICATE_FRONTEND_DIR_NAME));
    if old_dir.exists() {
        std::fs::remove_dir_all(&old_dir).map_err(FrontendPackageError::Io)?;
    }
    if frontend_dir.exists() {
        std::fs::rename(frontend_dir, &old_dir).map_err(FrontendPackageError::Io)?;
    }
    std::fs::rename(&temp_dir, frontend_dir).map_err(FrontendPackageError::Io)?;
    if old_dir.exists() {
        std::fs::remove_dir_all(&old_dir).map_err(FrontendPackageError::Io)?;
    }

    Ok(())
}

fn extract_frontend_zip(
    package_path: &Path,
    target_dir: &Path,
) -> Result<(), FrontendPackageError> {
    let file = File::open(package_path).map_err(FrontendPackageError::Io)?;
    let mut archive = ZipArchive::new(file).map_err(FrontendPackageError::Zip)?;
    extract_archive_entries(&mut archive, target_dir)
}

fn extract_frontend_zip_bytes(bytes: &[u8], target_dir: &Path) -> Result<(), FrontendPackageError> {
    let cursor = Cursor::new(bytes);
    let mut archive = ZipArchive::new(cursor).map_err(FrontendPackageError::Zip)?;
    extract_archive_entries(&mut archive, target_dir)
}

fn extract_archive_entries<R: io::Read + io::Seek>(
    archive: &mut ZipArchive<R>,
    target_dir: &Path,
) -> Result<(), FrontendPackageError> {
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(FrontendPackageError::Zip)?;
        let enclosed = entry
            .enclosed_name()
            .map(|path| path.to_path_buf())
            .ok_or_else(|| FrontendPackageError::InvalidArchivePath(entry.name().to_string()))?;
        let out_path = target_dir.join(enclosed);

        if entry.is_dir() {
            std::fs::create_dir_all(&out_path).map_err(FrontendPackageError::Io)?;
            continue;
        }

        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent).map_err(FrontendPackageError::Io)?;
        }

        let mut out_file = File::create(&out_path).map_err(FrontendPackageError::Io)?;
        io::copy(&mut entry, &mut out_file).map_err(FrontendPackageError::Io)?;
    }

    Ok(())
}

#[derive(Debug)]
pub enum FrontendPackageError {
    PackageNotFound,
    ConfiguredPackageNotFound(PathBuf),
    InvalidConfiguredPackagePath(String),
    ManifestMismatch,
    InvalidManifest(String),
    BundleHashMismatch { expected: String, actual: String },
    InvalidTarget(PathBuf),
    MissingEntry(PathBuf),
    InvalidArchivePath(String),
    Io(io::Error),
    Json(serde_json::Error),
    Zip(zip::result::ZipError),
}

impl std::fmt::Display for FrontendPackageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PackageNotFound => write!(f, "duplicate frontend package not found"),
            Self::ConfiguredPackageNotFound(path) => write!(
                f,
                "configured duplicate frontend package does not exist or is not a file: {}",
                path.display()
            ),
            Self::InvalidConfiguredPackagePath(message) => {
                write!(f, "invalid duplicate frontend package configuration: {message}")
            }
            Self::ManifestMismatch => write!(
                f,
                "frontend sidecar manifest does not match frontend-manifest.json in the archive byte-for-byte"
            ),
            Self::InvalidManifest(message) => {
                write!(f, "invalid duplicate frontend manifest: {message}")
            }
            Self::BundleHashMismatch { expected, actual } => write!(
                f,
                "frontend archive bundle hash {actual} does not match manifest value {expected}"
            ),
            Self::InvalidTarget(path) => {
                write!(
                    f,
                    "invalid duplicate frontend target path: {}",
                    path.display()
                )
            }
            Self::MissingEntry(path) => {
                write!(f, "missing extracted frontend entry: {}", path.display())
            }
            Self::InvalidArchivePath(path) => write!(f, "invalid archive path: {}", path),
            Self::Io(error) => write!(f, "i/o error: {}", error),
            Self::Json(error) => write!(f, "json error: {}", error),
            Self::Zip(error) => write!(f, "zip error: {}", error),
        }
    }
}

impl std::error::Error for FrontendPackageError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::io::Write;
    use std::sync::Mutex;
    use tempfile::tempdir;

    static FRONTEND_PACKAGE_ENV_LOCK: Mutex<()> = Mutex::new(());
    const TEST_INDEX_HTML: &[u8] = b"<html><body>ok</body></html>";

    struct ScopedFrontendPackageEnv {
        previous: Option<OsString>,
    }

    impl ScopedFrontendPackageEnv {
        fn set(path: &Path) -> Self {
            let previous = std::env::var_os(FRONTEND_PACKAGE_ENV);
            std::env::set_var(FRONTEND_PACKAGE_ENV, path);
            Self { previous }
        }
    }

    impl Drop for ScopedFrontendPackageEnv {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var(FRONTEND_PACKAGE_ENV, previous);
            } else {
                std::env::remove_var(FRONTEND_PACKAGE_ENV);
            }
        }
    }

    fn manifest_fixture() -> FrontendPackageManifest {
        let mut hasher = Sha256::new();
        hasher.update(b"index.html");
        hasher.update([0]);
        hasher.update(TEST_INDEX_HTML);
        hasher.update([0]);
        FrontendPackageManifest {
            schema_version: 1,
            frontend_name: "lotus".to_string(),
            frontend_version: "1.0.0".to_string(),
            bundle_hash: format!("sha256:{}", hex::encode(hasher.finalize())),
            built_at: Utc::now(),
            entry: "index.html".to_string(),
        }
    }

    fn write_test_zip(path: &Path, manifest: &FrontendPackageManifest) {
        let file = File::create(path).expect("zip file should be created");
        let mut writer = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);

        writer
            .start_file("index.html", options)
            .expect("index.html entry should start");
        writer
            .write_all(TEST_INDEX_HTML)
            .expect("index.html should write");

        writer
            .start_file("frontend-manifest.json", options)
            .expect("manifest entry should start");
        writer
            .write_all(serde_json::to_string_pretty(manifest).unwrap().as_bytes())
            .expect("manifest should write");

        writer.finish().expect("zip should finish");
    }

    #[test]
    fn refresh_is_required_when_local_manifest_missing() {
        let bundled = manifest_fixture();
        let temp = tempdir().unwrap();
        assert!(should_refresh_frontend(&bundled, None, temp.path()));
    }

    #[test]
    fn compiled_default_is_the_locked_clean_lotus_next_artifact() {
        if !has_embedded_frontend_package() {
            // The explicit API-only build contract intentionally omits these bytes.
            return;
        }
        let manifest = read_bundled_manifest(None).expect("embedded manifest should be valid");
        assert_eq!(manifest.frontend_name, "lotus-next");
        assert_eq!(manifest.frontend_version, "2026.9.14");

        let package_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("frontend_package/lotus-frontend.zip");
        let externally_validated = read_bundled_manifest(Some(&package_path))
            .expect("committed bytes should pass the runtime rollback verifier");
        assert_eq!(externally_validated, manifest);

        let bytes = DUPLICATE_FRONTEND_PACKAGE_ZIP.expect("embedded zip should exist");
        let mut archive = ZipArchive::new(Cursor::new(bytes)).expect("embedded zip should open");
        let universal: serde_json::Value = serde_json::from_reader(
            archive
                .by_name("lotus-next-manifest.json")
                .expect("universal manifest should be embedded"),
        )
        .expect("universal manifest should parse");
        assert_eq!(
            universal["sourceRevision"],
            "ae17b50574ccd86395cbc226b50c9fb2f0f51e0f"
        );
        assert_eq!(universal["sourceDirty"], false);
        assert_eq!(
            universal["resourcesSha256"],
            "4e5d67386d6fbef15fc8aaf84238356e86ab470fab5680c33e3d0c28860a7f2d"
        );
        assert_eq!(universal["resources"].as_array().unwrap().len(), 34);
    }

    #[test]
    fn resolve_frontend_package_path_finds_bodhi_frontend_package_layout() {
        let _environment_lock = FRONTEND_PACKAGE_ENV_LOCK.lock().unwrap();
        let temp = tempdir().unwrap();
        let bodhi_package_dir = temp.path().join("bodhi/.frontend-package");
        std::fs::create_dir_all(&bodhi_package_dir).unwrap();
        let package_path = bodhi_package_dir.join("lotus-frontend.zip");
        std::fs::write(&package_path, b"zip-bytes").unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp.path()).unwrap();

        let resolved = resolve_frontend_package_path(None);

        std::env::set_current_dir(original_dir).unwrap();

        let resolved = resolved.expect("frontend package path should resolve");
        let resolved_canonical = resolved.canonicalize().unwrap();
        let expected_canonical = package_path.canonicalize().unwrap();
        assert_eq!(resolved_canonical, expected_canonical);
    }

    #[test]
    fn refresh_not_required_when_manifest_matches_and_entry_exists() {
        let bundled = manifest_fixture();
        let temp = tempdir().unwrap();
        std::fs::write(temp.path().join("index.html"), "ok").unwrap();
        assert!(!should_refresh_frontend(
            &bundled,
            Some(&bundled),
            temp.path()
        ));
    }

    #[test]
    fn ensure_current_frontend_dir_extracts_bundle_into_bamboo_frontend() {
        let package_temp = tempdir().unwrap();
        let package_path = package_temp.path().join("lotus-frontend.zip");
        let manifest = manifest_fixture();
        write_test_zip(&package_path, &manifest);

        let data_temp = tempdir().unwrap();
        let status = ensure_current_frontend_dir_in(data_temp.path(), Some(&package_path))
            .expect("frontend extraction should succeed");

        assert!(status.refreshed);
        assert_eq!(status.package_path.as_deref(), Some(package_path.as_path()));
        assert_eq!(status.bundled_manifest, manifest);
        assert_eq!(
            std::fs::read_to_string(status.frontend_dir.join("index.html")).unwrap(),
            "<html><body>ok</body></html>"
        );
        assert!(status
            .frontend_dir
            .join(&status.bundled_manifest.entry)
            .is_file());
        assert!(status
            .frontend_dir
            .join(DUPLICATE_FRONTEND_MANIFEST_NAME)
            .is_file());
        let local_manifest = read_local_manifest(&status.local_manifest_path)
            .expect("local manifest should read")
            .expect("local manifest should exist");
        assert_eq!(
            local_manifest.frontend_version,
            status.bundled_manifest.frontend_version
        );
        assert_eq!(
            local_manifest.bundle_hash,
            status.bundled_manifest.bundle_hash
        );
    }

    #[test]
    fn configured_package_path_fails_closed_when_missing_even_with_embedded_bytes() {
        let package_temp = tempdir().unwrap();
        let missing_path = package_temp.path().join("missing-frontend.zip");
        let data_temp = tempdir().unwrap();

        let error = ensure_current_frontend_dir_in(data_temp.path(), Some(&missing_path))
            .expect_err("a missing configured package must not fall back to embedded bytes");

        assert!(matches!(
            error,
            FrontendPackageError::ConfiguredPackageNotFound(path) if path == missing_path
        ));
    }

    #[test]
    fn configured_environment_package_overrides_embedded_bytes() {
        let _environment_lock = FRONTEND_PACKAGE_ENV_LOCK.lock().unwrap();
        let package_temp = tempdir().unwrap();
        let package_path = package_temp.path().join("lotus-frontend.zip");
        let mut manifest = manifest_fixture();
        manifest.frontend_name = "environment-override".to_string();
        manifest.frontend_version = "3.0.0".to_string();
        write_test_zip(&package_path, &manifest);
        let _environment = ScopedFrontendPackageEnv::set(&package_path);
        let data_temp = tempdir().unwrap();

        let status = ensure_current_frontend_dir_in(data_temp.path(), None)
            .expect("the configured environment package should be used");

        assert_eq!(status.package_path.as_deref(), Some(package_path.as_path()));
        assert_eq!(status.bundled_manifest, manifest);
        assert_eq!(
            std::fs::read_to_string(status.frontend_dir.join("index.html")).unwrap(),
            "<html><body>ok</body></html>"
        );
    }

    #[test]
    fn configured_package_rejects_a_mismatched_adjacent_sidecar() {
        let package_temp = tempdir().unwrap();
        let package_path = package_temp.path().join("lotus-frontend.zip");
        let manifest = manifest_fixture();
        write_test_zip(&package_path, &manifest);

        let mut mismatched = manifest.clone();
        mismatched.frontend_version = "2.0.0".to_string();
        std::fs::write(
            package_temp.path().join("frontend-manifest.json"),
            serde_json::to_vec_pretty(&mismatched).unwrap(),
        )
        .unwrap();

        let error = read_bundled_manifest(Some(&package_path))
            .expect_err("a mismatched sidecar must not be trusted");
        assert!(matches!(error, FrontendPackageError::ManifestMismatch));
    }

    #[test]
    fn external_archive_paths_reject_nonportable_and_case_colliding_layouts() {
        for path in ["../escape.js", "/absolute.js", "assets\\app.js", "CON"] {
            assert!(validate_external_archive_path(path, ExternalArchiveEntryKind::File).is_err());
        }

        let mut entries = HashMap::new();
        let mut required_directories = HashMap::new();
        register_external_archive_path(
            "Assets/app.js",
            ExternalArchiveEntryKind::File,
            &mut entries,
            &mut required_directories,
        )
        .expect("the first portable path should register");
        assert!(register_external_archive_path(
            "assets/other.js",
            ExternalArchiveEntryKind::File,
            &mut entries,
            &mut required_directories,
        )
        .is_err());
    }

    #[test]
    fn configured_package_rejects_invalid_manifest_hashes_and_corrupt_payloads() {
        let invalid_temp = tempdir().unwrap();
        let invalid_path = invalid_temp.path().join("lotus-frontend.zip");
        let mut invalid_manifest = manifest_fixture();
        invalid_manifest.entry = "../index.html".to_string();
        write_test_zip(&invalid_path, &invalid_manifest);

        let error = read_bundled_manifest(Some(&invalid_path))
            .expect_err("an invalid explicit manifest must fail closed");
        assert!(matches!(error, FrontendPackageError::InvalidManifest(_)));

        let hash_mismatch_temp = tempdir().unwrap();
        let hash_mismatch_path = hash_mismatch_temp.path().join("lotus-frontend.zip");
        let mut hash_mismatch_manifest = manifest_fixture();
        hash_mismatch_manifest.bundle_hash = format!("sha256:{}", "b".repeat(64));
        write_test_zip(&hash_mismatch_path, &hash_mismatch_manifest);

        let error = read_bundled_manifest(Some(&hash_mismatch_path))
            .expect_err("a false explicit bundle hash must fail closed");
        assert!(matches!(
            error,
            FrontendPackageError::BundleHashMismatch { .. }
        ));

        let corrupt_temp = tempdir().unwrap();
        let corrupt_path = corrupt_temp.path().join("lotus-frontend.zip");
        write_test_zip(&corrupt_path, &manifest_fixture());
        let mut bytes = std::fs::read(&corrupt_path).unwrap();
        let needle = b"<html><body>ok</body></html>";
        let offset = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("stored frontend bytes should be present");
        bytes[offset] ^= 0xff;
        std::fs::write(&corrupt_path, bytes).unwrap();

        read_bundled_manifest(Some(&corrupt_path))
            .expect_err("a corrupt explicit frontend payload must fail closed");
    }
}
