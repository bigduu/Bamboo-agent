//! Offline static export of the canonical compiled manual. No runtime configuration is read.
#[allow(dead_code)]
#[path = "../build/product_manual.rs"]
mod packaging;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub fn build(files: &[(&str, &[u8])]) -> io::Result<(String, BTreeMap<String, Vec<u8>>)> {
    let mut resources = BTreeMap::new();
    for (path, bytes) in files {
        if let Some(path) = path.strip_prefix("bamboo-manual/") {
            if resources.insert(path.to_string(), bytes.to_vec()).is_some() {
                return Err(invalid("duplicate resource"));
            }
        }
    }
    let manifest_bytes = resources
        .get("manifest.json")
        .ok_or_else(|| invalid("missing manifest"))?;
    let manifest: Value = serde_json::from_slice(manifest_bytes)?;
    let version = manifest["package_version"]
        .as_str()
        .ok_or_else(|| invalid("missing version"))?;
    if version.is_empty()
        || !version
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        || version == "."
        || version == ".."
    {
        return Err(invalid("unsafe version"));
    }
    if manifest["format_version"] != 1 || manifest["package"] != "bamboo-skills" {
        return Err(invalid("unsupported manifest"));
    }
    let entries = manifest["resources"]
        .as_array()
        .ok_or_else(|| invalid("missing resources"))?;
    if entries.len() != packaging::RESOURCES.len() || resources.len() != entries.len() + 1 {
        return Err(invalid("resource set mismatch"));
    }
    let mut ordered = Vec::new();
    let mut documents = Vec::new();
    for (entry, expected) in entries.iter().zip(packaging::RESOURCES) {
        if entry["path"] != *expected {
            return Err(invalid("resource path/order mismatch"));
        }
        let bytes = resources
            .get(*expected)
            .ok_or_else(|| invalid("missing resource"))?;
        let text = std::str::from_utf8(bytes).map_err(|_| invalid("non UTF-8 resource"))?;
        if entry["bytes"] != bytes.len() || entry["sha256"] != packaging::sha256(bytes) {
            return Err(invalid("resource integrity mismatch"));
        }
        ordered.push((*expected, bytes.clone()));
        if expected.starts_with("references/") {
            let title = text
                .lines()
                .find_map(|line| line.strip_prefix("# "))
                .unwrap_or(expected);
            documents.push(json!({"path":expected,"title":title,"text":text,"bytes":bytes.len(),"sha256":packaging::sha256(bytes)}));
        }
    }
    if manifest["content_sha256"] != packaging::content_digest(&ordered) {
        return Err(invalid("content identity mismatch"));
    }
    let bundle_id = packaging::sha256(manifest_bytes);
    let leaf = format!("{version}/{bundle_id}");
    let index = json!({"schema":1,"bundle_id":bundle_id,"package":manifest["package"],"package_version":version,"source":manifest["source"],"content_sha256":manifest["content_sha256"],"documents":documents});
    let mut output = BTreeMap::new();
    output.insert("manifest.json".into(), manifest_bytes.clone());
    for (path, bytes) in resources {
        if path != "manifest.json" {
            output.insert(format!("resources/{path}"), bytes);
        }
    }
    output.insert("index.json".into(), serde_json::to_vec_pretty(&index)?);
    for (path, bytes) in [
        ("index.html", include_bytes!("site/index.html").as_slice()),
        ("app.mjs", include_bytes!("site/app.mjs").as_slice()),
        ("verify.mjs", include_bytes!("site/verify.mjs").as_slice()),
        ("search.mjs", include_bytes!("site/search.mjs").as_slice()),
    ] {
        output.insert(path.into(), bytes.to_vec());
    }
    Ok((leaf, output))
}

/// Refuse any existing destination. Validate all bytes before creating it.
pub fn write_new(destination: &Path, files: &[(&str, &[u8])]) -> io::Result<PathBuf> {
    let (leaf, output) = build(files)?;
    fs::create_dir(destination)?;
    let root = destination.join(leaf);
    for (path, bytes) in output {
        let path = root.join(path);
        fs::create_dir_all(path.parent().ok_or_else(|| invalid("missing parent"))?)?;
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?
            .write_all(&bytes)?;
    }
    Ok(root)
}
