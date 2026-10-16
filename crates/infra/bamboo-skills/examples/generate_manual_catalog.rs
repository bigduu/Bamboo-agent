//! Regenerate checked-in public reference data from typed defaults; no instance I/O.
#[path = "../manual/config_catalog.rs"]
mod config_catalog;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("assets/bamboo-manual/references/config-keys.json");
    let expected = config_catalog::render();
    match std::env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [flag] if flag == "--write" => std::fs::write(&path, expected)?,
        [] => print!("{expected}"),
        [flag] if flag == "--check" => {
            if std::fs::read_to_string(&path)? != expected {
                return Err("manual catalog is stale; run with --write and review the diff".into());
            }
        }
        _ => return Err("usage: generate_manual_catalog [--check|--write]".into()),
    }
    Ok(())
}
