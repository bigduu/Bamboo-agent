use std::fs::File;
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use zip::ZipArchive;

use bamboo_infrastructure_config::paths::bamboo_dir;

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

pub fn resolve_frontend_package_path(explicit_path: Option<&Path>) -> Option<PathBuf> {
    if let Some(path) = explicit_path {
        return path.exists().then(|| path.to_path_buf());
    }

    if let Ok(raw) = std::env::var(FRONTEND_PACKAGE_ENV) {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            let candidate = PathBuf::from(trimmed);
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

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

    candidates.into_iter().find(|path| path.exists())
}

pub fn read_bundled_manifest(
    package_path: Option<&Path>,
) -> Result<FrontendPackageManifest, FrontendPackageError> {
    if let Some(bytes) = DUPLICATE_FRONTEND_PACKAGE_MANIFEST {
        return serde_json::from_slice(bytes).map_err(FrontendPackageError::Json);
    }

    let package_path = package_path.ok_or(FrontendPackageError::PackageNotFound)?;
    let sidecar_manifest = package_path.with_file_name("frontend-manifest.json");
    if sidecar_manifest.exists() {
        let file = File::open(sidecar_manifest).map_err(FrontendPackageError::Io)?;
        return serde_json::from_reader(file).map_err(FrontendPackageError::Json);
    }

    read_bundled_manifest_from_zip(package_path)
}

pub fn read_bundled_manifest_from_zip(
    package_path: &Path,
) -> Result<FrontendPackageManifest, FrontendPackageError> {
    let file = File::open(package_path).map_err(FrontendPackageError::Io)?;
    let mut archive = ZipArchive::new(file).map_err(FrontendPackageError::Zip)?;
    let mut manifest_file = archive
        .by_name("frontend-manifest.json")
        .map_err(FrontendPackageError::Zip)?;
    let manifest: FrontendPackageManifest =
        serde_json::from_reader(&mut manifest_file).map_err(FrontendPackageError::Json)?;
    Ok(manifest)
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
    let package_path = if has_embedded_frontend_package() {
        None
    } else {
        Some(
            resolve_frontend_package_path(explicit_package_path)
                .ok_or(FrontendPackageError::PackageNotFound)?,
        )
    };

    let bundled_manifest = read_bundled_manifest(package_path.as_deref())?;
    let frontend_dir = duplicate_frontend_dir_in(bamboo_home_dir);
    let local_manifest_path = duplicate_frontend_manifest_path_in(bamboo_home_dir);
    let local_manifest = read_local_manifest(&local_manifest_path)?;

    let refresh_needed = should_refresh_frontend(
        &bundled_manifest,
        local_manifest.as_ref(),
        &frontend_dir,
    );

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

    if let Some(bytes) = DUPLICATE_FRONTEND_PACKAGE_ZIP {
        extract_frontend_zip_bytes(bytes, &temp_dir)?;
    } else {
        let package_path = package_path.ok_or(FrontendPackageError::PackageNotFound)?;
        extract_frontend_zip(package_path, &temp_dir)?;
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

fn extract_frontend_zip(package_path: &Path, target_dir: &Path) -> Result<(), FrontendPackageError> {
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
            Self::InvalidTarget(path) => {
                write!(f, "invalid duplicate frontend target path: {}", path.display())
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
    use std::io::Write;
    use tempfile::tempdir;

    fn manifest_fixture() -> FrontendPackageManifest {
        FrontendPackageManifest {
            schema_version: 1,
            frontend_name: "lotus".to_string(),
            frontend_version: "1.0.0".to_string(),
            bundle_hash: "sha256:abc".to_string(),
            built_at: Utc::now(),
            entry: "index.html".to_string(),
        }
    }

    fn write_test_zip(path: &Path, manifest: &FrontendPackageManifest) {
        let file = File::create(path).expect("zip file should be created");
        let mut writer = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();

        writer
            .start_file("index.html", options)
            .expect("index.html entry should start");
        writer
            .write_all(b"<html><body>ok</body></html>")
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
    fn resolve_frontend_package_path_finds_bodhi_frontend_package_layout() {
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
        assert!(!should_refresh_frontend(&bundled, Some(&bundled), temp.path()));
    }

    #[test]
    fn ensure_current_frontend_dir_extracts_bundle_into_bamboo_frontend() {
        let package_temp = tempdir().unwrap();
        let package_path = package_temp.path().join("lotus-frontend.zip");
        let manifest = manifest_fixture();
        write_test_zip(&package_path, &manifest);

        let data_temp = tempdir().unwrap();
        bamboo_infrastructure_config::paths::init_bamboo_dir(data_temp.path().to_path_buf());

        let status = ensure_current_frontend_dir(Some(&package_path))
            .expect("frontend extraction should succeed");

        assert!(status.refreshed);
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
        assert_eq!(local_manifest.frontend_version, status.bundled_manifest.frontend_version);
        assert_eq!(local_manifest.bundle_hash, status.bundled_manifest.bundle_hash);
    }
}
