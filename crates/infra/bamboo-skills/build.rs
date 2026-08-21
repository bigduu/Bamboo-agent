use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const BUILTIN_SKILL_WHITELIST: &[&str] = &[
    "debug",
    "personal-assistant",
    "plan",
    "research",
    "review",
    "simplify",
    "skill-creator",
];

// Git executable bits are not represented consistently in a Windows checkout.
// Keep the embedded permission contract explicit and platform-independent.
const BUILTIN_EXECUTABLE_FILES: &[&str] = &[
    "skill-creator/scripts/aggregate_benchmark.py",
    "skill-creator/scripts/generate_report.py",
    "skill-creator/scripts/improve_description.py",
    "skill-creator/scripts/package_skill.py",
    "skill-creator/scripts/quick_validate.py",
    "skill-creator/scripts/run_eval.py",
    "skill-creator/scripts/run_loop.py",
];

fn is_builtin_skill_enabled(name: &str) -> bool {
    BUILTIN_SKILL_WHITELIST.contains(&name)
}

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

        // Filter top-level skill directories against the whitelist
        if path.is_dir() && root == current && !is_builtin_skill_enabled(file_name) {
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

    // builtin_skills lives at the workspace root. This crate sits at
    // crates/infra/bamboo-skills, so the repo root is three levels up.
    let builtin_root = manifest_dir.join("../../../builtin_skills");
    println!("cargo:rerun-if-changed={}", builtin_root.display());

    let dest = out_dir.join("builtin_skills_embedded.rs");
    let mut file = fs::File::create(dest)?;

    let mut files = Vec::new();
    if builtin_root.exists() {
        collect_files(&builtin_root, &builtin_root, &mut files)?;
    }

    files.sort();

    for executable in BUILTIN_EXECUTABLE_FILES {
        if !files.iter().any(|path| to_unix_path(path) == *executable) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("builtin executable manifest references missing file '{executable}'"),
            ));
        }
    }

    writeln!(
        file,
        "pub static BUILTIN_SKILL_FILES: &[(&str, &[u8], bool)] = &["
    )?;
    for relative in files {
        let relative_unix = to_unix_path(&relative);
        let executable = BUILTIN_EXECUTABLE_FILES.contains(&relative_unix.as_str());
        writeln!(
            file,
            "    (\"{relative}\", include_bytes!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/../../../builtin_skills/{relative}\")), {executable}),",
            relative = relative_unix,
        )?;
    }
    writeln!(file, "];")?;

    Ok(())
}
