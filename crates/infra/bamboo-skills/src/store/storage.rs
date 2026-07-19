use std::path::{Path, PathBuf};

use tokio::fs;
use tracing::{debug, info, warn};

use crate::store::parser::{parse_markdown_skill, render_skill_markdown};
use crate::types::{SkillDefinition, SkillError, SkillResult};

fn public_skill_error(error: &SkillError) -> String {
    match error {
        SkillError::Yaml(error) => match error.location() {
            Some(location) => format!(
                "SKILL.md: invalid metadata at line {}, column {}",
                location.line(),
                location.column()
            ),
            None => "SKILL.md: invalid metadata".to_string(),
        },
        SkillError::InvalidId(_) => "SKILL.md: invalid skill id".to_string(),
        SkillError::Validation(_) => "SKILL.md: invalid frontmatter or content".to_string(),
        _ => "SKILL.md: failed to load bundle".to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkillDirectorySource {
    /// Compile-time embedded bundle materialized under the global skills dir.
    Builtin,
    /// Cross-agent user skills discovered from `~/.agents/skills`.
    Agents,
    Global,
    Project,
    /// `~/.bamboo/plugins/<plugin-id>/skills` — an installed plugin's skills,
    /// discovered *in place* (no copy, no symlink). See
    /// [`discover_plugin_skill_dirs`].
    Plugin,
}

#[derive(Debug, Clone)]
pub struct SkillDiscoveryDir {
    pub dir: PathBuf,
    pub source: SkillDirectorySource,
    pub mode: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LoadedSkillRecord {
    pub skill: SkillDefinition,
    pub skill_root: PathBuf,
    pub source: SkillDirectorySource,
    pub mode: Option<String>,
    pub skill_file: PathBuf,
}

#[derive(Debug, Clone)]
pub struct FailedSkillRecord {
    pub skill_id: Option<String>,
    pub skill_root: PathBuf,
    pub skill_file: PathBuf,
    pub source: SkillDirectorySource,
    pub mode: Option<String>,
    pub error: String,
}

#[derive(Debug, Default)]
pub struct SkillLoadReport {
    pub loaded: Vec<LoadedSkillRecord>,
    pub failed: Vec<FailedSkillRecord>,
}

pub async fn ensure_skills_dir(skills_dir: &Path) -> SkillResult<()> {
    fs::create_dir_all(skills_dir).await?;
    Ok(())
}

/// Recursively find all SKILL.md files in the skills directory
async fn find_skill_files(dir: &Path) -> Vec<PathBuf> {
    let mut skill_files = Vec::new();
    let mut entries = match fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(error) => {
            warn!("Failed to read skill directory {:?}: {}", dir, error);
            return skill_files;
        }
    };

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(error) => {
                warn!("Failed while scanning skill directory {:?}: {}", dir, error);
                break;
            }
        };
        let path = entry.path();
        // Never follow directory symlinks: an external tree must not become
        // part of skill discovery merely because it is linked below a root.
        let is_real_dir = entry
            .file_type()
            .await
            .map(|kind| kind.is_dir() && !kind.is_symlink())
            .unwrap_or(false);
        if is_real_dir {
            // Check if this directory contains SKILL.md
            let skill_file = path.join("SKILL.md");
            match fs::try_exists(&skill_file).await {
                Ok(true) => {
                    let is_regular_file = fs::symlink_metadata(&skill_file)
                        .await
                        .map(|metadata| {
                            metadata.file_type().is_file() && !metadata.file_type().is_symlink()
                        })
                        .unwrap_or(false);
                    if is_regular_file {
                        skill_files.push(skill_file);
                    } else {
                        warn!(
                            "Ignoring symlinked or non-regular skill file: {:?}",
                            skill_file
                        );
                    }
                    continue; // Don't recurse into skill directories
                }
                Ok(false) => {
                    // Not a skill directory, recurse into it
                    let sub_skills = Box::pin(find_skill_files(&path)).await;
                    skill_files.extend(sub_skills);
                }
                Err(_) => {
                    debug!("Cannot check {:?}, skipping", path);
                }
            }
        }
    }

    skill_files
}

pub async fn load_skills_from_discovery_dirs(
    discovery_dirs: &[SkillDiscoveryDir],
) -> SkillResult<Vec<LoadedSkillRecord>> {
    Ok(load_skills_from_discovery_dirs_detailed(discovery_dirs)
        .await?
        .loaded)
}

/// Discover every candidate and preserve per-bundle failures for catalog LKG handling.
pub async fn load_skills_from_discovery_dirs_detailed(
    discovery_dirs: &[SkillDiscoveryDir],
) -> SkillResult<SkillLoadReport> {
    let mut report = SkillLoadReport::default();

    for discovery in discovery_dirs {
        match fs::try_exists(&discovery.dir).await {
            Ok(true) => {}
            Ok(false) => {
                debug!(
                    "Skill discovery dir not found, skipping: {:?}",
                    discovery.dir
                );
                continue;
            }
            Err(error) => {
                warn!(
                    "Failed to check skill discovery dir {:?}: {}",
                    discovery.dir, error
                );
                continue;
            }
        }

        debug!(
            "Loading skills from {:?} (source={:?}, mode={})",
            discovery.dir,
            discovery.source,
            discovery.mode.as_deref().unwrap_or("generic")
        );

        let mut skill_files = find_skill_files(&discovery.dir).await;
        skill_files.sort();
        for skill_file in skill_files {
            match fs::read_to_string(&skill_file).await {
                Ok(content) => match parse_markdown_skill(&skill_file, &content) {
                    Ok(skill) => {
                        let skill_root = skill_file
                            .parent()
                            .map(Path::to_path_buf)
                            .unwrap_or_else(|| discovery.dir.clone());
                        report.loaded.push(LoadedSkillRecord {
                            skill,
                            skill_root,
                            source: discovery.source,
                            mode: discovery.mode.clone(),
                            skill_file,
                        });
                    }
                    Err(error) => {
                        warn!("Failed to parse skill file {:?}: {}", skill_file, error);
                        let skill_root = skill_file
                            .parent()
                            .map(Path::to_path_buf)
                            .unwrap_or_else(|| discovery.dir.clone());
                        report.failed.push(FailedSkillRecord {
                            skill_id: skill_root
                                .file_name()
                                .and_then(|name| name.to_str())
                                .map(str::to_string),
                            skill_root,
                            skill_file,
                            source: discovery.source,
                            mode: discovery.mode.clone(),
                            error: public_skill_error(&error),
                        });
                    }
                },
                Err(error) => {
                    warn!("Failed to read skill file {:?}: {}", skill_file, error);
                    let skill_root = skill_file
                        .parent()
                        .map(Path::to_path_buf)
                        .unwrap_or_else(|| discovery.dir.clone());
                    report.failed.push(FailedSkillRecord {
                        skill_id: skill_root
                            .file_name()
                            .and_then(|name| name.to_str())
                            .map(str::to_string),
                        skill_root,
                        skill_file,
                        source: discovery.source,
                        mode: discovery.mode.clone(),
                        error: error.to_string(),
                    });
                }
            }
        }
    }

    info!(
        "Loaded {} skill records from discovery dirs ({} invalid)",
        report.loaded.len(),
        report.failed.len()
    );
    Ok(report)
}

/// Discover additional skill-discovery dirs contributed by installed
/// plugins: one `SkillDiscoveryDir` per `<plugins_root>/<plugin-id>/skills`
/// subdirectory.
///
/// This does NOT require a `plugin.json` to exist or be valid — it globs
/// whatever plugin directories are present under `plugins_root` and points a
/// discovery dir at each one's `skills/` subdirectory. `load_skills_from_discovery_dirs`
/// already skips a discovery dir that doesn't exist, so a plugin with no
/// `skills/` subdirectory (or none of its declared skills) is a silent no-op
/// here, not an error. Plugin skills carry no mode (plugins aren't aware of
/// `active_mode`).
pub async fn discover_plugin_skill_dirs(plugins_root: &Path) -> Vec<SkillDiscoveryDir> {
    let mut discovered = Vec::new();

    let mut entries = match fs::read_dir(plugins_root).await {
        Ok(entries) => entries,
        Err(_) => {
            // Missing (no plugins installed yet) or unreadable — both are a
            // silent no-op; `installed.json` living alongside plugin dirs is
            // not itself a directory so it's naturally skipped below anyway.
            return discovered;
        }
    };

    loop {
        let entry = match entries.next_entry().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(error) => {
                warn!("Failed to read plugins dir {:?}: {}", plugins_root, error);
                break;
            }
        };

        let is_dir = match entry.file_type().await {
            Ok(file_type) => file_type.is_dir(),
            Err(_) => false,
        };
        if !is_dir {
            continue;
        }

        discovered.push(SkillDiscoveryDir {
            dir: entry.path().join("skills"),
            source: SkillDirectorySource::Plugin,
            mode: None,
        });
    }

    // Deterministic order: `read_dir` yields entries in an unspecified order,
    // which would make the winner of a plugin-vs-plugin same-skill-id
    // collision non-deterministic (it depends on load order). Sort by plugin
    // directory path so the lowest-sorting plugin id deterministically wins,
    // and the shadowed loser is logged consistently (see the WARN in the skill
    // store's resolver).
    discovered.sort_by(|left, right| left.dir.cmp(&right.dir));
    discovered
}

pub fn skill_path(skills_dir: &Path, skill_id: &str) -> PathBuf {
    skills_dir.join(skill_id).join("SKILL.md")
}

pub async fn write_skill_file(skills_dir: &Path, skill: &SkillDefinition) -> SkillResult<()> {
    let path = skill_path(skills_dir, &skill.id);

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let content = render_skill_markdown(skill)?;
    fs::write(path, content).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_skill(name: &str) -> String {
        format!("---\nname: {name}\ndescription: test skill\n---\n\nFollow this skill.")
    }

    #[tokio::test]
    async fn agents_source_recurses_and_bad_skill_does_not_block_good_skill() {
        let root = tempfile::tempdir().expect("tempdir");
        let good = root.path().join("vendor/good");
        let bad = root.path().join("bad");
        fs::create_dir_all(&good).await.expect("good dir");
        fs::create_dir_all(&bad).await.expect("bad dir");
        fs::write(good.join("SKILL.md"), valid_skill("good"))
            .await
            .expect("good skill");
        fs::write(bad.join("SKILL.md"), "not valid frontmatter")
            .await
            .expect("bad skill");

        let records = load_skills_from_discovery_dirs(&[SkillDiscoveryDir {
            dir: root.path().to_path_buf(),
            source: SkillDirectorySource::Agents,
            mode: None,
        }])
        .await
        .expect("discovery survives invalid skill");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].skill.id, "good");
        assert_eq!(records[0].source, SkillDirectorySource::Agents);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn agents_source_does_not_follow_directory_symlinks() {
        use std::os::unix::fs::symlink;
        let root = tempfile::tempdir().expect("root");
        let outside = tempfile::tempdir().expect("outside");
        let skill = outside.path().join("escaped");
        fs::create_dir_all(&skill).await.expect("skill dir");
        fs::write(skill.join("SKILL.md"), valid_skill("escaped"))
            .await
            .expect("skill");
        symlink(outside.path(), root.path().join("linked")).expect("symlink");

        let records = load_skills_from_discovery_dirs(&[SkillDiscoveryDir {
            dir: root.path().to_path_buf(),
            source: SkillDirectorySource::Agents,
            mode: None,
        }])
        .await
        .expect("discovery");
        assert!(records.is_empty());
    }
}
