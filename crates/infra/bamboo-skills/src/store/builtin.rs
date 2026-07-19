use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::store::parser::{parse_markdown_skill, render_skill_markdown};
use crate::types::{SkillError, SkillResult};

include!(concat!(env!("OUT_DIR"), "/builtin_skills_embedded.rs"));

pub struct BuiltinSkillBundle {
    pub skill: crate::types::SkillDefinition,
    pub files: HashMap<String, Vec<u8>>,
}

/// Archive the pre-catalog global materialization only when every file proves
/// byte-for-byte ownership by the embedded bundle. The atomic directory rename
/// is recoverable and never recursively deletes files; any user-added, removed,
/// or edited file makes the directory a user override and leaves it untouched.
pub async fn archive_exact_legacy_materialization(
    global_skills_dir: &Path,
    bundle: &BuiltinSkillBundle,
) -> SkillResult<bool> {
    let legacy_root = global_skills_dir.join(&bundle.skill.id);
    if !tokio::fs::try_exists(&legacy_root).await? {
        return Ok(false);
    }

    let mut expected = bundle.files.clone();
    expected.insert(
        "SKILL.md".to_string(),
        render_skill_markdown(&bundle.skill)?.into_bytes(),
    );
    let mut actual = HashMap::new();
    for entry in walkdir::WalkDir::new(&legacy_root).follow_links(false) {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => return Ok(false),
        };
        if entry.path() == legacy_root {
            continue;
        }
        if !entry.file_type().is_file() || entry.file_type().is_symlink() {
            if entry.file_type().is_dir() {
                continue;
            }
            return Ok(false);
        }
        let relative = match entry.path().strip_prefix(&legacy_root) {
            Ok(relative) => relative.to_string_lossy().replace('\\', "/"),
            Err(_) => return Ok(false),
        };
        actual.insert(relative, tokio::fs::read(entry.path()).await?);
    }

    if actual != expected {
        return Ok(false);
    }

    static ARCHIVE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let archive_parent = global_skills_dir
        .parent()
        .unwrap_or(global_skills_dir)
        .join("legacy-builtins-v1")
        .join(&bundle.skill.id);
    tokio::fs::create_dir_all(&archive_parent).await?;
    for _ in 0..16 {
        let sequence = ARCHIVE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let archive = archive_parent.join(format!("{}-{sequence}", std::process::id()));
        match tokio::fs::rename(&legacy_root, &archive).await {
            Ok(()) => return Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            // Another initializer won the rename, or a competing writer changed
            // the source path. Either way, do not remove or overwrite anything.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(false)
}

fn discover_skill_roots() -> Vec<String> {
    let mut roots = BUILTIN_SKILL_FILES
        .iter()
        .filter_map(|(path, _)| path.strip_suffix("/SKILL.md"))
        .map(str::to_string)
        .collect::<Vec<_>>();

    roots.sort();
    roots.dedup();
    roots.sort_by_key(|root| std::cmp::Reverse(root.len()));
    roots
}

pub fn load_builtin_skill_bundles() -> SkillResult<Vec<BuiltinSkillBundle>> {
    let mut bundles = Vec::new();

    let skills_dir = bamboo_config::paths::bamboo_dir().join("skills");
    let skills_dir_display = bamboo_config::paths::path_to_display_string(&skills_dir);

    let roots = discover_skill_roots();
    if roots.is_empty() {
        return Ok(bundles);
    }

    let mut grouped: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();
    for (path, bytes) in BUILTIN_SKILL_FILES {
        if let Some(root) = roots.iter().find(|root| {
            let prefix = format!("{}/", root);
            path.starts_with(&prefix)
        }) {
            let prefix = format!("{}/", root);
            if let Some(relative_path) = path.strip_prefix(&prefix) {
                grouped
                    .entry(root.clone())
                    .or_default()
                    .push((relative_path.to_string(), bytes.to_vec()));
            }
        }
    }

    for (skill_root, files) in grouped {
        let mut skill_markdown: Option<String> = None;
        let mut assets: HashMap<String, Vec<u8>> = HashMap::new();

        for (relative_path, bytes) in files {
            if relative_path == "SKILL.md" {
                let raw = String::from_utf8(bytes).map_err(|error| {
                    SkillError::Validation(format!(
                        "Builtin skill {} has non-UTF8 SKILL.md: {}",
                        skill_root, error
                    ))
                })?;
                skill_markdown = Some(raw.replace("<SKILLS_DIR>", &skills_dir_display));
            } else {
                assets.insert(relative_path, bytes);
            }
        }

        let markdown = skill_markdown.ok_or_else(|| {
            SkillError::Validation(format!("Builtin skill {} is missing SKILL.md", skill_root))
        })?;

        let skill =
            parse_markdown_skill(Path::new(&format!("{}/SKILL.md", skill_root)), &markdown)?;
        bundles.push(BuiltinSkillBundle {
            skill,
            files: assets,
        });
    }

    bundles.sort_by_key(|b| b.skill.id.clone());
    Ok(bundles)
}

#[cfg(test)]
mod tests {
    use super::load_builtin_skill_bundles;

    #[test]
    fn builtin_skill_creator_bundle_includes_scripts() {
        let bundles = load_builtin_skill_bundles().expect("load builtin bundles");
        let skill_creator = bundles
            .iter()
            .find(|bundle| bundle.skill.id == "skill-creator")
            .expect("skill-creator bundle");

        // Verify multiple grouped resource folders are embedded.
        assert!(skill_creator.files.contains_key("scripts/run_eval.py"));
        assert!(skill_creator.files.contains_key("agents/analyzer.md"));
        assert!(skill_creator.files.contains_key("assets/eval_review.html"));
        assert!(skill_creator
            .files
            .contains_key("eval-viewer/generate_review.py"));
    }

    #[test]
    fn builtin_personal_assistant_bundle_carries_assistant_tool_refs() {
        let bundles = load_builtin_skill_bundles().expect("load builtin bundles");
        let assistant = bundles
            .iter()
            .find(|bundle| bundle.skill.id == "personal-assistant")
            .expect("personal-assistant bundle");

        assert_eq!(assistant.skill.name, "personal-assistant");
        assert!(!assistant.skill.description.is_empty());
        for tool in ["ledger", "scheduler", "memory", "notify"] {
            assert!(
                assistant
                    .skill
                    .tool_refs
                    .iter()
                    .any(|tool_ref| tool_ref == tool),
                "personal-assistant must allow the {tool} tool, got {:?}",
                assistant.skill.tool_refs
            );
        }
    }
}
