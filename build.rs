use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

fn collect_files(root: &Path, current: &Path, out: &mut Vec<PathBuf>) -> io::Result<()> {
    let mut entries = fs::read_dir(current)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|entry| entry.path())
        .collect::<Vec<_>>();

    entries.sort();

    for path in entries {
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("");
        if file_name.starts_with('.') {
            continue;
        }

        if path.is_dir() {
            collect_files(root, &path, out)?;
            continue;
        }

        if path.is_file() {
            let relative = path
                .strip_prefix(root)
                .expect("file should be under root")
                .to_path_buf();
            out.push(relative);
        }
    }

    Ok(())
}

fn to_unix_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn write_builtin_skills_embed(manifest_dir: &Path, out_dir: &Path) -> io::Result<()> {
    let builtin_root = manifest_dir.join("builtin_skills");
    println!("cargo:rerun-if-changed={}", builtin_root.display());

    let dest = out_dir.join("builtin_skills_embedded.rs");
    let mut file = fs::File::create(dest)?;

    let mut files = Vec::new();
    if builtin_root.exists() {
        collect_files(&builtin_root, &builtin_root, &mut files)?;
    }

    files.sort();

    writeln!(
        file,
        "pub static BUILTIN_SKILL_FILES: &[(&str, &[u8])] = &["
    )?;
    for relative in files {
        let relative_unix = to_unix_path(&relative);
        writeln!(
            file,
            "    (\"{relative}\", include_bytes!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/builtin_skills/{relative}\"))),",
            relative = relative_unix
        )?;
    }
    writeln!(file, "];" )?;

    Ok(())
}

fn ensure_frontend_package_staged(manifest_dir: &Path) {
    let zip_path = manifest_dir.join("frontend_package/lotus-frontend.zip");
    let manifest_path = manifest_dir.join("frontend_package/frontend-manifest.json");
    if zip_path.exists() && manifest_path.exists() {
        return;
    }

    println!(
        "cargo:warning=frontend_package artifacts missing; attempting to stage Lotus frontend package"
    );

    let status = Command::new("node")
        .arg("scripts/frontend-package.cjs")
        .arg("stage")
        .current_dir(manifest_dir)
        .status();

    match status {
        Ok(status) if status.success() => {
            println!("cargo:warning=staged frontend_package artifacts successfully");
        }
        Ok(status) => {
            println!(
                "cargo:warning=failed to stage frontend_package artifacts; node script exited with {}",
                status
            );
        }
        Err(error) => {
            println!(
                "cargo:warning=failed to launch frontend package staging script: {}",
                error
            );
        }
    }
}

fn write_frontend_package_embed(manifest_dir: &Path, out_dir: &Path) -> io::Result<()> {
    let frontend_root = manifest_dir.join("frontend_package");
    println!("cargo:rerun-if-changed={}", frontend_root.display());
    ensure_frontend_package_staged(manifest_dir);

    let dest = out_dir.join("frontend_package_embedded.rs");
    let mut file = fs::File::create(dest)?;

    let zip_path = frontend_root.join("lotus-frontend.zip");
    let manifest_path = frontend_root.join("frontend-manifest.json");

    if zip_path.exists() && manifest_path.exists() {
        writeln!(
            file,
            "pub static DUPLICATE_FRONTEND_PACKAGE_ZIP: Option<&[u8]> = Some(include_bytes!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/frontend_package/lotus-frontend.zip\")));"
        )?;
        writeln!(
            file,
            "pub static DUPLICATE_FRONTEND_PACKAGE_MANIFEST: Option<&[u8]> = Some(include_bytes!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/frontend_package/frontend-manifest.json\")));"
        )?;
    } else {
        writeln!(
            file,
            "pub static DUPLICATE_FRONTEND_PACKAGE_ZIP: Option<&[u8]> = None;"
        )?;
        writeln!(
            file,
            "pub static DUPLICATE_FRONTEND_PACKAGE_MANIFEST: Option<&[u8]> = None;"
        )?;
    }

    Ok(())
}

fn main() -> io::Result<()> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));

    write_builtin_skills_embed(&manifest_dir, &out_dir)?;
    write_frontend_package_embed(&manifest_dir, &out_dir)?;

    Ok(())
}
