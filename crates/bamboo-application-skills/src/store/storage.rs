use std::path::{Path, PathBuf};

use tokio::fs;
use tracing::{debug, info, warn};

use crate::store::parser::{parse_markdown_skill, render_skill_markdown};
use crate::types::{SkillDefinition, SkillResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillDirectorySource {
    Global,
    Project,
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
}

pub async fn ensure_skills_dir(skills_dir: &Path) -> SkillResult<()> {
    fs::create_dir_all(skills_dir).await?;
    Ok(())
}

/// Recursively find all SKILL.md files in the skills directory
async fn find_skill_files(dir: &Path) -> SkillResult<Vec<PathBuf>> {
    let mut skill_files = Vec::new();
    let mut entries = fs::read_dir(dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();

        if path.is_dir() {
            // Check if this directory contains SKILL.md
            let skill_file = path.join("SKILL.md");
            match fs::try_exists(&skill_file).await {
                Ok(true) => {
                    skill_files.push(skill_file);
                    continue; // Don't recurse into skill directories
                }
                Ok(false) => {
                    // Not a skill directory, recurse into it
                    let sub_skills = Box::pin(find_skill_files(&path)).await?;
                    skill_files.extend(sub_skills);
                }
                Err(_) => {
                    debug!("Cannot check {:?}, skipping", path);
                }
            }
        }
    }

    Ok(skill_files)
}

pub async fn load_skills_from_discovery_dirs(
    discovery_dirs: &[SkillDiscoveryDir],
) -> SkillResult<Vec<LoadedSkillRecord>> {
    let mut loaded = Vec::new();

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

        let skill_files = find_skill_files(&discovery.dir).await?;
        for skill_file in skill_files {
            match fs::read_to_string(&skill_file).await {
                Ok(content) => match parse_markdown_skill(&skill_file, &content) {
                    Ok(skill) => {
                        let skill_root = skill_file
                            .parent()
                            .map(Path::to_path_buf)
                            .unwrap_or_else(|| discovery.dir.clone());
                        loaded.push(LoadedSkillRecord {
                            skill,
                            skill_root,
                            source: discovery.source,
                            mode: discovery.mode.clone(),
                        });
                    }
                    Err(error) => {
                        warn!("Failed to parse skill file {:?}: {}", skill_file, error);
                    }
                },
                Err(error) => {
                    warn!("Failed to read skill file {:?}: {}", skill_file, error);
                }
            }
        }
    }

    info!("Loaded {} skill records from discovery dirs", loaded.len());
    Ok(loaded)
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
