mod frontend_build;

use frontend_build::{
    frontend_package_for_mode, FrontendBuildMode, ValidatedFrontendPackage, FRONTEND_BUILD_MODE_ENV,
};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

fn write_disabled_embed(file: &mut fs::File) -> io::Result<()> {
    writeln!(
        file,
        "pub static DUPLICATE_FRONTEND_PACKAGE_ZIP: Option<&[u8]> = None;"
    )?;
    writeln!(
        file,
        "pub static DUPLICATE_FRONTEND_PACKAGE_MANIFEST: Option<&[u8]> = None;"
    )?;
    Ok(())
}

fn write_required_embed(
    file: &mut fs::File,
    out_dir: &Path,
    package: ValidatedFrontendPackage,
) -> io::Result<()> {
    fs::write(out_dir.join("frontend_package.zip"), package.zip_bytes)?;
    fs::write(
        out_dir.join("frontend_package_manifest.json"),
        package.manifest_bytes,
    )?;
    writeln!(
        file,
        "pub static DUPLICATE_FRONTEND_PACKAGE_ZIP: Option<&[u8]> = Some(include_bytes!(concat!(env!(\"OUT_DIR\"), \"/frontend_package.zip\")));"
    )?;
    writeln!(
        file,
        "pub static DUPLICATE_FRONTEND_PACKAGE_MANIFEST: Option<&[u8]> = Some(include_bytes!(concat!(env!(\"OUT_DIR\"), \"/frontend_package_manifest.json\")));"
    )?;
    Ok(())
}

fn frontend_recovery_instruction(manifest_dir: &Path) -> &'static str {
    let workspace_stager_exists = manifest_dir
        .ancestors()
        .nth(3)
        .map(|root| root.join("scripts/frontend-package.cjs").is_file())
        .unwrap_or(false);

    if workspace_stager_exists {
        "Restore/stage it with `node scripts/frontend-package.cjs stage` from the Bamboo workspace"
    } else {
        "Re-fetch or reinstall an intact bamboo-server crate source archive"
    }
}

fn write_frontend_package_embed(manifest_dir: &Path, out_dir: &Path) -> io::Result<()> {
    let frontend_root = manifest_dir.join("frontend_package");
    let zip_path = frontend_root.join("lotus-frontend.zip");
    let manifest_path = frontend_root.join("frontend-manifest.json");

    // Staging is deliberately not a Cargo side effect. Explicit staging
    // callers own and propagate the Node command's exit status before build.rs
    // validates the committed/package-archive bytes here.
    println!("cargo:rerun-if-env-changed={FRONTEND_BUILD_MODE_ENV}");
    println!("cargo:rerun-if-changed={}", zip_path.display());
    println!("cargo:rerun-if-changed={}", manifest_path.display());

    let dest = out_dir.join("frontend_package_embedded.rs");
    let mut file = fs::File::create(dest)?;

    let mode = FrontendBuildMode::from_environment()?;
    match frontend_package_for_mode(mode, &frontend_root) {
        Ok(None) => write_disabled_embed(&mut file),
        Ok(Some(package)) => write_required_embed(&mut file, out_dir, package),
        Err(error) => {
            let recovery = frontend_recovery_instruction(manifest_dir);
            Err(io::Error::new(
                error.kind(),
                format!(
                    "required embedded frontend package is unavailable or invalid: {error}. \
                         {recovery}; or explicitly request a frontend-free build with \
                         `{FRONTEND_BUILD_MODE_ENV}=api-only` in the build environment"
                ),
            ))
        }
    }
}

fn main() -> io::Result<()> {
    // Keep the platform's default unwind tables. Disabling compact unwind to
    // hide Apple's large-__eh_frame warning aborts caught panics in the macOS
    // library-test harness instead of preserving the scheduler recovery path.
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));

    write_frontend_package_embed(&manifest_dir, &out_dir)?;

    Ok(())
}
