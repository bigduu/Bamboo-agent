use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

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

fn main() -> io::Result<()> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));

    // builtin_skills lives at the workspace root: crates/bamboo-agent-skill/../../builtin_skills
    let builtin_root = manifest_dir.join("../../builtin_skills");
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
            "    (\"{relative}\", include_bytes!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/../../builtin_skills/{relative}\"))),",
            relative = relative_unix
        )?;
    }
    writeln!(file, "];")?;

    Ok(())
}
