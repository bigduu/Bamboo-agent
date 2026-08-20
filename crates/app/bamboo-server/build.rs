use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

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

fn write_frontend_package_embed(manifest_dir: &Path, out_dir: &Path) -> io::Result<()> {
    let frontend_root = manifest_dir.join("frontend_package");
    println!("cargo:rerun-if-changed={}", frontend_root.display());

    // Stage frontend package if needed
    let zip_path = frontend_root.join("lotus-frontend.zip");
    let manifest_path = frontend_root.join("frontend-manifest.json");
    if !zip_path.exists() || !manifest_path.exists() {
        let _ = Command::new("node")
            .arg("scripts/frontend-package.cjs")
            .arg("stage")
            .current_dir(manifest_dir)
            .status();
    }

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
    configure_macos_test_unwinding();

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));

    write_frontend_package_embed(&manifest_dir, &out_dir)?;

    Ok(())
}
