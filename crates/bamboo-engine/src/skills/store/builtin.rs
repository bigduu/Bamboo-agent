use std::collections::HashMap;
use std::path::Path;

use crate::skills::store::parser::parse_markdown_skill;
use crate::skills::types::{SkillError, SkillResult};

include!(concat!(env!("OUT_DIR"), "/builtin_skills_embedded.rs"));

pub struct BuiltinSkillBundle {
    pub skill: crate::skills::types::SkillDefinition,
    pub files: HashMap<String, Vec<u8>>,
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

    let skills_dir = bamboo_infrastructure::paths::bamboo_dir().join("skills");
    let skills_dir_display = bamboo_infrastructure::paths::path_to_display_string(&skills_dir);

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
}
