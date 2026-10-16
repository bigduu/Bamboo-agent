//! Fail-closed packaging for the crate-local, version-bound product manual.
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

pub const RESOURCES: &[&str] = &[
    "SKILL.md",
    "agents/bamboo.yaml",
    "references/index.md",
    "references/configuration.md",
    "references/config-keys.json",
    "references/memory.md",
    "references/hooks.md",
    "references/providers-mcp.md",
    "references/troubleshooting.md",
];

fn invalid(message: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

pub fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Read every declared UTF-8 resource; a partial or missing manual is an error.
pub fn collect(root: &Path) -> io::Result<Vec<(&'static str, Vec<u8>)>> {
    RESOURCES
        .iter()
        .map(|&relative| {
            let path = root.join(relative);
            if !fs::symlink_metadata(&path)?.file_type().is_file() {
                return Err(invalid(format!(
                    "manual resource is not a file: {relative}"
                )));
            }
            let bytes = fs::read(&path)?;
            if std::str::from_utf8(&bytes)
                .map_err(|_| invalid(format!("manual resource is not UTF-8: {relative}")))?
                .trim()
                .is_empty()
            {
                return Err(invalid(format!("manual resource is empty: {relative}")));
            }
            if relative.ends_with(".json") {
                serde_json::from_slice::<Value>(&bytes)
                    .map_err(|error| invalid(format!("invalid {relative}: {error}")))?;
            }
            Ok((relative, bytes))
        })
        .collect()
}

pub fn content_digest(resources: &[(&str, Vec<u8>)]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"bamboo.product-manual.v1\0");
    for (path, bytes) in resources {
        digest.update((path.len() as u64).to_be_bytes());
        digest.update(path.as_bytes());
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    hex::encode(digest.finalize())
}

fn git(dir: &Path, arguments: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(arguments)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn revision(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub fn source_identity(manifest_dir: &Path) -> Value {
    // Cargo's recorded source wins over any unrelated parent Git checkout into
    // which an extracted crate may have been placed.
    let vcs = manifest_dir.join(".cargo_vcs_info.json");
    println!("cargo:rerun-if-changed={}", vcs.display());
    if vcs.exists() {
        if let Ok(value) = fs::read(&vcs)
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).map_err(invalid))
        {
            if let Some(sha) = value["git"]["sha1"].as_str().filter(|sha| revision(sha)) {
                return json!({"revision": sha, "status": "source_archive",
                    "dirty": value["git"]["dirty"].as_bool(), "identity_source": "cargo_vcs_info"});
            }
        }
        return json!({"revision": null, "status": "unknown", "identity_source": "invalid_cargo_vcs_info"});
    }
    let workspace = manifest_dir.join("../../..");
    if workspace.join("builtin_skills").is_dir() {
        if let Some(sha) = git(&workspace, &["rev-parse", "HEAD"]).filter(|sha| revision(sha)) {
            for entry in ["HEAD", "index", "packed-refs"] {
                if let Some(path) = git(&workspace, &["rev-parse", "--git-path", entry]) {
                    println!("cargo:rerun-if-changed={}", workspace.join(path).display());
                }
            }
            if let Some(reference) = git(&workspace, &["symbolic-ref", "-q", "HEAD"]) {
                if let Some(path) = git(&workspace, &["rev-parse", "--git-path", &reference]) {
                    println!("cargo:rerun-if-changed={}", workspace.join(path).display());
                }
            }
            let status = git(
                &workspace,
                &["status", "--porcelain", "--untracked-files=normal"],
            );
            let state = match status.as_deref() {
                Some("") => "clean",
                Some(_) => "modified",
                None => "unknown",
            };
            return json!({"revision": sha, "status": state, "identity_source": "git"});
        }
    }
    json!({"revision": null, "status": "unknown", "identity_source": "unknown"})
}

pub fn embed(manifest_dir: &Path, out_dir: &Path, output: &mut impl Write) -> io::Result<()> {
    let root = manifest_dir.join("assets/bamboo-manual");
    println!("cargo:rerun-if-changed={}", root.display());
    let resources = collect(&root)?;
    let dependency_manifest = manifest_dir.join("Cargo.toml");
    let workspace_lock = manifest_dir.join("../../../Cargo.lock");
    let lock = if workspace_lock.is_file() {
        workspace_lock
    } else {
        manifest_dir.join("Cargo.lock")
    };
    println!("cargo:rerun-if-changed={}", dependency_manifest.display());
    println!("cargo:rerun-if-changed={}", lock.display());
    let lock_hash = match fs::read(&lock) {
        Ok(bytes) => Some(sha256(&bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let manifest = json!({
        "format_version": 1,
        "package": "bamboo-skills",
        "package_version": std::env::var("CARGO_PKG_VERSION").map_err(invalid)?,
        "source": source_identity(manifest_dir),
        "content_sha256": content_digest(&resources),
        "dependency_manifest_sha256": sha256(&fs::read(dependency_manifest)?),
        "dependency_lock_sha256": lock_hash,
        "configuration_coverage": "bounded_catalog_not_full_schema",
        "resources": resources.iter().map(|(path, bytes)| json!({
            "path": path, "bytes": bytes.len(), "sha256": sha256(bytes)
        })).collect::<Vec<_>>()
    });
    let generated = out_dir.join("bamboo-manual-manifest.json");
    fs::write(
        &generated,
        serde_json::to_vec_pretty(&manifest).map_err(invalid)?,
    )?;
    for (relative, _) in &resources {
        let path: PathBuf = root.join(relative);
        writeln!(
            output,
            "    ({:?}, include_bytes!({:?})),",
            format!("bamboo-manual/{relative}"),
            path
        )?;
    }
    writeln!(
        output,
        "    (\"bamboo-manual/manifest.json\", include_bytes!({generated:?})),"
    )?;
    Ok(())
}
