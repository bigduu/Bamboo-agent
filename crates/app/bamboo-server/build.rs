mod frontend_build;

use frontend_build::{
    frontend_package_for_mode, FrontendBuildMode, ValidatedFrontendPackage, FRONTEND_BUILD_MODE_ENV,
};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

fn configure_macos_test_unwinding() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }

    // The monolithic bamboo-server lib-test has more than 16 MiB of DWARF
    // unwind records. Apple's compact-unwind format cannot encode offsets that
    // large and otherwise emits an oversized-__eh_frame warning on every test
    // link. Disable compact-unwind synthesis for this library package's
    // directly linked artifacts. `rustc-link-arg-tests` does not cover a
    // library's own `#[cfg(test)]` harness, while the general form does. The
    // argument has no final-link effect while rustc creates the rlib and Cargo
    // does not propagate it to consumer links, so downstream dev/release
    // binaries do not inherit it. The linker keeps __eh_frame, preserving Rust
    // panic unwinding, backtraces, and line-table debugging.
    println!("cargo:rustc-link-arg=-Wl,-no_compact_unwind");
}

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
    configure_macos_test_unwinding();

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));

    write_frontend_package_embed(&manifest_dir, &out_dir)?;

    Ok(())
}
