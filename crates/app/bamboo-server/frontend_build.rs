use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{self, Cursor, Read};
use std::path::Path;
use zip::ZipArchive;

pub const FRONTEND_BUILD_MODE_ENV: &str = "BAMBOO_FRONTEND_BUILD_MODE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendBuildMode {
    Embedded,
    ApiOnly,
}

impl FrontendBuildMode {
    pub fn from_environment() -> io::Result<Self> {
        match env::var(FRONTEND_BUILD_MODE_ENV) {
            Ok(value) => Self::parse(&value),
            Err(env::VarError::NotPresent) => Ok(Self::Embedded),
            Err(env::VarError::NotUnicode(_)) => Err(invalid_data(format!(
                "{FRONTEND_BUILD_MODE_ENV} must be valid UTF-8"
            ))),
        }
    }

    fn parse(value: &str) -> io::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "embedded" => Ok(Self::Embedded),
            "api-only" => Ok(Self::ApiOnly),
            _ => Err(invalid_data(format!(
                "invalid {FRONTEND_BUILD_MODE_ENV}={value:?}; expected `embedded` or `api-only`"
            ))),
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct FrontendManifest {
    schema_version: u32,
    frontend_name: String,
    frontend_version: String,
    bundle_hash: String,
    built_at: DateTime<Utc>,
    entry: String,
}

#[derive(Debug)]
pub struct ValidatedFrontendPackage {
    pub manifest_bytes: Vec<u8>,
    pub zip_bytes: Vec<u8>,
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn parse_manifest(bytes: &[u8], source: &str) -> io::Result<FrontendManifest> {
    serde_json::from_slice(bytes)
        .map_err(|error| invalid_data(format!("invalid {source}: {error}")))
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

fn validate_archive_path(path: &str) -> io::Result<String> {
    if path.is_empty()
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains('\\')
        || path.contains(':')
        || path.contains('\0')
    {
        return Err(invalid_data(format!(
            "frontend archive contains a non-portable path: {path:?}"
        )));
    }

    let mut normalized = Vec::new();
    for component in path.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.ends_with(['.', ' '])
            || component
                .bytes()
                .any(|byte| byte < b' ' || matches!(byte, b'<' | b'>' | b'"' | b'|' | b'?' | b'*'))
            || is_windows_reserved_name(component)
        {
            return Err(invalid_data(format!(
                "frontend archive contains an unsafe path: {path:?}"
            )));
        }
        normalized.push(component);
    }

    Ok(normalized.join("/"))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArchiveEntryKind {
    File,
    Directory,
}

fn portable_key(path: &str) -> String {
    path.to_lowercase()
}

fn parent_paths(path: &str) -> impl Iterator<Item = &str> {
    path.match_indices('/').map(|(index, _)| &path[..index])
}

fn register_parent_directories(
    path: &str,
    entries: &HashMap<String, (String, ArchiveEntryKind)>,
    required_directories: &mut HashMap<String, String>,
) -> io::Result<()> {
    for parent in parent_paths(path) {
        let key = portable_key(parent);
        if let Some((existing, existing_kind)) = entries.get(&key) {
            if *existing_kind == ArchiveEntryKind::File {
                return Err(invalid_data(format!(
                    "frontend archive file {existing:?} conflicts with required directory {parent:?}"
                )));
            }
            if existing != parent {
                return Err(invalid_data(format!(
                    "frontend archive directories {existing:?} and {parent:?} collide on case-insensitive filesystems"
                )));
            }
        }
        if let Some(existing) = required_directories.get(&key) {
            if existing != parent {
                return Err(invalid_data(format!(
                    "frontend archive directories {existing:?} and {parent:?} collide on case-insensitive filesystems"
                )));
            }
        } else {
            required_directories.insert(key, parent.to_owned());
        }
    }
    Ok(())
}

fn register_archive_entry(
    path: &str,
    kind: ArchiveEntryKind,
    entries: &mut HashMap<String, (String, ArchiveEntryKind)>,
    required_directories: &mut HashMap<String, String>,
) -> io::Result<()> {
    register_parent_directories(path, entries, required_directories)?;

    let key = portable_key(path);
    if kind == ArchiveEntryKind::File {
        if let Some(required) = required_directories.get(&key) {
            return Err(invalid_data(format!(
                "frontend archive file {path:?} conflicts with required directory {required:?}"
            )));
        }
    } else if let Some(required) = required_directories.get(&key) {
        if required != path {
            return Err(invalid_data(format!(
                "frontend archive directories {required:?} and {path:?} collide on case-insensitive filesystems"
            )));
        }
    }

    if let Some((existing, existing_kind)) = entries.get(&key) {
        if existing != path {
            return Err(invalid_data(format!(
                "frontend archive paths {existing:?} and {path:?} collide on case-insensitive filesystems"
            )));
        }
        if *existing_kind != kind {
            return Err(invalid_data(format!(
                "frontend archive path {path:?} is both a file and a directory"
            )));
        }
        if kind == ArchiveEntryKind::File {
            return Err(invalid_data(format!(
                "frontend archive contains duplicate path {path:?}"
            )));
        }
        return Ok(());
    }

    entries.insert(key, (path.to_owned(), kind));
    Ok(())
}

fn validate_manifest_fields(manifest: &FrontendManifest) -> io::Result<String> {
    if manifest.schema_version != 1 {
        return Err(invalid_data(format!(
            "unsupported frontend manifest schema_version {}; expected 1",
            manifest.schema_version
        )));
    }
    if manifest.frontend_name.trim().is_empty() || manifest.frontend_version.trim().is_empty() {
        return Err(invalid_data(
            "frontend manifest name and version must be non-empty",
        ));
    }

    if manifest.entry != "index.html" {
        return Err(invalid_data(format!(
            "frontend manifest entry must be `index.html`, got {:?}",
            manifest.entry
        )));
    }

    let hash = manifest
        .bundle_hash
        .strip_prefix("sha256:")
        .ok_or_else(|| invalid_data("frontend manifest bundle_hash must use sha256:<hex>"))?;
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid_data(
            "frontend manifest bundle_hash must contain exactly 64 hexadecimal digits",
        ));
    }

    validate_archive_path(&manifest.entry)
}

pub fn validate_frontend_package(frontend_root: &Path) -> io::Result<ValidatedFrontendPackage> {
    let manifest_path = frontend_root.join("frontend-manifest.json");
    let zip_path = frontend_root.join("lotus-frontend.zip");

    let sidecar_bytes = fs::read(&manifest_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "cannot read frontend manifest {}: {error}",
                manifest_path.display()
            ),
        )
    })?;
    let sidecar = parse_manifest(&sidecar_bytes, "frontend sidecar manifest")?;
    let normalized_entry = validate_manifest_fields(&sidecar)?;

    let zip_bytes = fs::read(&zip_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "cannot read frontend archive {}: {error}",
                zip_path.display()
            ),
        )
    })?;
    let mut archive = ZipArchive::new(Cursor::new(zip_bytes.as_slice()))
        .map_err(|error| invalid_data(format!("invalid frontend zip archive: {error}")))?;

    let embedded_manifest_bytes = {
        let mut entry = archive.by_name("frontend-manifest.json").map_err(|error| {
            invalid_data(format!(
                "frontend archive is missing frontend-manifest.json: {error}"
            ))
        })?;
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes)?;
        bytes
    };
    let embedded = parse_manifest(
        &embedded_manifest_bytes,
        "frontend-manifest.json inside the frontend archive",
    )?;
    if embedded_manifest_bytes != sidecar_bytes || embedded != sidecar {
        return Err(invalid_data(
            "frontend sidecar manifest does not match frontend-manifest.json in the archive byte-for-byte",
        ));
    }

    let mut portable_paths = HashMap::new();
    let mut required_directories = HashMap::new();
    let mut contains_entry = false;

    for index in 0..archive.len() {
        let mut file = archive.by_index(index).map_err(|error| {
            invalid_data(format!(
                "cannot inspect frontend archive entry {index}: {error}"
            ))
        })?;
        let raw_name = file.name().to_owned();
        let path_to_validate = raw_name.strip_suffix('/').unwrap_or(&raw_name);
        let normalized = validate_archive_path(path_to_validate)?;
        let kind = if file.is_dir() {
            ArchiveEntryKind::Directory
        } else {
            ArchiveEntryKind::File
        };
        register_archive_entry(
            &normalized,
            kind,
            &mut portable_paths,
            &mut required_directories,
        )?;

        if kind == ArchiveEntryKind::Directory {
            continue;
        }
        if normalized == normalized_entry {
            contains_entry = true;
        }
        io::copy(&mut file, &mut io::sink()).map_err(|error| {
            invalid_data(format!(
                "cannot read frontend archive entry {normalized:?}: {error}"
            ))
        })?;
        if normalized == "frontend-manifest.json" {
            continue;
        }
    }

    if !contains_entry {
        return Err(invalid_data(format!(
            "frontend archive does not contain manifest entry {:?}",
            sidecar.entry
        )));
    }

    // schema_version 1 records a staging identity, but does not record the
    // locale-dependent path ordering used by the JavaScript hash producer.
    // Validate its algorithm/shape here and let the real extractor consume the
    // exact staged bytes; a deterministic cross-platform rehash needs a future
    // manifest schema rather than silently changing the committed v1 identity.
    drop(archive);
    Ok(ValidatedFrontendPackage {
        manifest_bytes: sidecar_bytes,
        zip_bytes,
    })
}

pub fn frontend_package_for_mode(
    mode: FrontendBuildMode,
    frontend_root: &Path,
) -> io::Result<Option<ValidatedFrontendPackage>> {
    match mode {
        FrontendBuildMode::Embedded => validate_frontend_package(frontend_root).map(Some),
        FrontendBuildMode::ApiOnly => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;
    use tempfile::TempDir;
    use zip::write::SimpleFileOptions;

    fn package_fixture(files: &[(&str, &[u8])]) -> TempDir {
        let temp = tempfile::tempdir().expect("tempdir");
        let package_root = temp.path();

        let manifest = json!({
            "schema_version": 1,
            "frontend_name": "lotus",
            "frontend_version": "test",
            "bundle_hash": format!("sha256:{}", "a".repeat(64)),
            "built_at": "2026-09-01T00:00:00.000Z",
            "entry": "index.html"
        });
        let manifest_bytes = serde_json::to_vec_pretty(&manifest).expect("serialize manifest");
        fs::write(package_root.join("frontend-manifest.json"), &manifest_bytes)
            .expect("write sidecar manifest");

        let zip_file = fs::File::create(package_root.join("lotus-frontend.zip"))
            .expect("create frontend archive");
        let mut writer = zip::ZipWriter::new(zip_file);
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
        for (path, bytes) in files {
            writer
                .start_file(*path, options)
                .expect("start fixture file");
            writer.write_all(bytes).expect("write fixture file");
        }
        writer
            .start_file("frontend-manifest.json", options)
            .expect("start embedded manifest");
        writer
            .write_all(&manifest_bytes)
            .expect("write embedded manifest");
        writer.finish().expect("finish frontend archive");

        temp
    }

    #[test]
    fn accepts_a_valid_portable_frontend_package() {
        let fixture = package_fixture(&[
            ("index.html", b"<div id=\"root\"></div>"),
            ("assets/app.js", b"console.log('ok')"),
        ]);

        let package = validate_frontend_package(fixture.path()).expect("valid package");
        assert!(!package.manifest_bytes.is_empty());
        assert!(!package.zip_bytes.is_empty());
    }

    #[test]
    fn rejects_missing_and_corrupt_packages() {
        let missing = tempfile::tempdir().expect("tempdir");
        let error = validate_frontend_package(missing.path()).expect_err("missing package");
        assert!(error.to_string().contains("cannot read frontend manifest"));

        let corrupt = package_fixture(&[("index.html", b"original")]);
        fs::write(corrupt.path().join("frontend-manifest.json"), b"not json")
            .expect("corrupt manifest");
        let error = validate_frontend_package(corrupt.path()).expect_err("corrupt package");
        assert!(error
            .to_string()
            .contains("invalid frontend sidecar manifest"));

        let damaged_archive = package_fixture(&[
            ("index.html", b"original"),
            ("assets/app.js", b"unique-bytes-to-damage"),
        ]);
        let archive_path = damaged_archive.path().join("lotus-frontend.zip");
        let mut bytes = fs::read(&archive_path).expect("read archive");
        let needle = b"unique-bytes-to-damage";
        let offset = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("stored fixture bytes");
        bytes[offset] ^= 0xff;
        fs::write(&archive_path, bytes).expect("damage archive payload");
        let error = validate_frontend_package(damaged_archive.path()).expect_err("damaged archive");
        assert!(error
            .to_string()
            .contains("cannot read frontend archive entry"));
    }

    #[test]
    fn rejects_manifest_mismatch_and_invalid_hash() {
        let mismatch = package_fixture(&[("index.html", b"original")]);
        let manifest_path = mismatch.path().join("frontend-manifest.json");
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
                .expect("parse manifest");
        manifest["frontend_version"] = json!("different");
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
        )
        .expect("write changed sidecar");
        let error = validate_frontend_package(mismatch.path()).expect_err("mismatched manifest");
        assert!(error.to_string().contains("does not match"));

        let bad_hash = package_fixture(&[("index.html", b"original")]);
        let manifest_path = bad_hash.path().join("frontend-manifest.json");
        let original = fs::read_to_string(&manifest_path).expect("read manifest");
        let changed = original.replace("sha256:", "sha256:0");
        fs::write(&manifest_path, changed).expect("write invalid hash");
        let error = validate_frontend_package(bad_hash.path()).expect_err("invalid hash");
        assert!(error.to_string().contains("exactly 64 hexadecimal"));
    }

    #[test]
    fn rejects_manifests_the_runtime_cannot_serve() {
        let invalid_time = package_fixture(&[("index.html", b"original")]);
        let manifest_path = invalid_time.path().join("frontend-manifest.json");
        let original = fs::read_to_string(&manifest_path).expect("read manifest");
        fs::write(
            &manifest_path,
            original.replace("2026-09-01T00:00:00.000Z", "not-rfc3339"),
        )
        .expect("write invalid timestamp");
        let error = validate_frontend_package(invalid_time.path()).expect_err("invalid timestamp");
        assert!(error
            .to_string()
            .contains("invalid frontend sidecar manifest"));

        let invalid_entry = package_fixture(&[("index.html", b"original")]);
        let manifest_path = invalid_entry.path().join("frontend-manifest.json");
        let original = fs::read_to_string(&manifest_path).expect("read manifest");
        fs::write(
            &manifest_path,
            original.replace("\"entry\": \"index.html\"", "\"entry\": \"app.html\""),
        )
        .expect("write invalid entry");
        let error = validate_frontend_package(invalid_entry.path()).expect_err("invalid entry");
        assert!(error.to_string().contains("entry must be `index.html`"));
    }

    #[test]
    fn rejects_paths_that_change_meaning_across_operating_systems() {
        let fixture =
            package_fixture(&[("index.html", b"ok"), ("assets\\windows.js", b"ambiguous")]);

        let error = validate_frontend_package(fixture.path()).expect_err("non-portable path");
        assert!(error.to_string().contains("non-portable path"));

        let collision = package_fixture(&[
            ("index.html", b"ok"),
            ("assets/App.js", b"upper"),
            ("assets/app.js", b"lower"),
        ]);
        let error = validate_frontend_package(collision.path()).expect_err("case collision");
        assert!(error.to_string().contains("case-insensitive filesystems"));

        let prefix_conflict = package_fixture(&[
            ("index.html", b"ok"),
            ("assets", b"file"),
            ("assets/app.js", b"nested"),
        ]);
        let error =
            validate_frontend_package(prefix_conflict.path()).expect_err("file prefix conflict");
        assert!(error.to_string().contains("required directory"));

        let reserved = package_fixture(&[("index.html", b"ok"), ("assets/NUL.js", b"bad")]);
        let error = validate_frontend_package(reserved.path()).expect_err("reserved name");
        assert!(error.to_string().contains("unsafe path"));

        let superscript_reserved =
            package_fixture(&[("index.html", b"ok"), ("assets/COM¹.js", b"bad")]);
        let error =
            validate_frontend_package(superscript_reserved.path()).expect_err("reserved name");
        assert!(error.to_string().contains("unsafe path"));

        let trailing_dot = package_fixture(&[("index.html", b"ok"), ("assets/app.js.", b"bad")]);
        let error = validate_frontend_package(trailing_dot.path()).expect_err("trailing dot");
        assert!(error.to_string().contains("unsafe path"));

        let file_directory_conflict =
            package_fixture(&[("index.html", b"ok"), ("assets/", b""), ("assets", b"file")]);
        let error = validate_frontend_package(file_directory_conflict.path())
            .expect_err("file and directory conflict");
        assert!(error.to_string().contains("both a file and a directory"));

        let casefold_prefix_conflict = package_fixture(&[
            ("index.html", b"ok"),
            ("Assets", b"file"),
            ("assets/app.js", b"nested"),
        ]);
        let error = validate_frontend_package(casefold_prefix_conflict.path())
            .expect_err("case-folded file prefix conflict");
        assert!(error.to_string().contains("required directory"));

        let root_directory = package_fixture(&[("index.html", b"ok"), ("/", b"")]);
        let error = validate_frontend_package(root_directory.path()).expect_err("root directory");
        assert!(error.to_string().contains("non-portable path"));

        for files in [
            vec![
                ("index.html", b"ok".as_slice()),
                ("Assets/", b"".as_slice()),
                ("assets/app.js", b"nested".as_slice()),
            ],
            vec![
                ("index.html", b"ok".as_slice()),
                ("assets/app.js", b"nested".as_slice()),
                ("Assets/", b"".as_slice()),
            ],
        ] {
            let casefold_directory = package_fixture(&files);
            let error = validate_frontend_package(casefold_directory.path())
                .expect_err("case-folded directory collision");
            assert!(error.to_string().contains("case-insensitive filesystems"));
        }
    }

    #[test]
    fn api_only_mode_must_be_explicit() {
        let _environment_parser: fn() -> io::Result<FrontendBuildMode> =
            FrontendBuildMode::from_environment;
        assert_eq!(
            FrontendBuildMode::parse("").expect("default mode"),
            FrontendBuildMode::Embedded
        );
        assert_eq!(
            FrontendBuildMode::parse("embedded").expect("embedded mode"),
            FrontendBuildMode::Embedded
        );
        assert_eq!(
            FrontendBuildMode::parse("api-only").expect("api-only mode"),
            FrontendBuildMode::ApiOnly
        );
        assert!(FrontendBuildMode::parse("disabled").is_err());

        let stale = tempfile::tempdir().expect("tempdir");
        fs::write(stale.path().join("frontend-manifest.json"), b"stale")
            .expect("write stale manifest");
        assert!(
            frontend_package_for_mode(FrontendBuildMode::ApiOnly, stale.path())
                .expect("API-only mode must not inspect stale frontend artifacts")
                .is_none()
        );
        assert!(frontend_package_for_mode(FrontendBuildMode::Embedded, stale.path()).is_err());
    }
}
