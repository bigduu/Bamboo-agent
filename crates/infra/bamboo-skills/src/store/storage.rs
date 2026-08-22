use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use bamboo_domain::bounded_dedup::{BoundedFingerprintSet, DEFAULT_BOUNDED_FINGERPRINT_CAPACITY};
use std::io::Read;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tracing::{debug, info, warn};

use crate::clone_publication::{
    clone_marker_name, parse_clone_marker_journal, std_file_identity, ClonePublicationPhase,
    MAX_CLONE_MARKER_BYTES,
};
use crate::store::parser::{parse_markdown_skill, render_skill_markdown};
use crate::types::{SkillDefinition, SkillError, SkillResult};

static STATIC_WARNINGS: LazyLock<BoundedFingerprintSet> =
    LazyLock::new(|| BoundedFingerprintSet::new(DEFAULT_BOUNDED_FINGERPRINT_CAPACITY));

fn open_file_no_follow(path: &Path) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "workflow resource is not a regular file",
            ));
        }
        Ok(file)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x00200000;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .attributes(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "workflow resource is not a regular file",
            ));
        }
        Ok(file)
    }
    #[cfg(not(any(unix, windows)))]
    {
        if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "workflow file is a symbolic link",
            ));
        }
        let file = std::fs::OpenOptions::new().read(true).open(path)?;
        if !file.metadata()?.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "workflow resource is not a regular file",
            ));
        }
        Ok(file)
    }
}

fn open_directory_no_follow(path: &Path) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(path)?;
        if !directory.metadata()?.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "workflow clone target is not a directory",
            ));
        }
        Ok(directory)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        };
        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)?;
        let metadata = directory.metadata()?;
        if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "workflow clone target is not a real directory",
            ));
        }
        Ok(directory)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let before = std::fs::symlink_metadata(path)?;
        if before.file_type().is_symlink() || !before.is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "workflow clone target is not a real directory",
            ));
        }
        std::fs::OpenOptions::new().read(true).open(path)
    }
}

fn clone_marker_allows_publication_sync(skill_file: &Path) -> bool {
    let Some(skill_root) = skill_file.parent() else {
        return false;
    };
    let Some(workflow_id) = skill_root.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(skills_root) = skill_root.parent() else {
        return false;
    };
    let marker_path = skills_root.join(clone_marker_name(workflow_id));
    let mut marker = match open_file_no_follow(&marker_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return true,
        Ok(marker) => marker,
        Err(_) => return false,
    };
    let marker_metadata = match marker.metadata() {
        Ok(metadata) if metadata.is_file() => metadata,
        Ok(_) | Err(_) => return false,
    };
    let marker_identity = match std_file_identity(&marker) {
        Some(identity) => identity,
        None => return false,
    };
    let marker_len = match usize::try_from(marker_metadata.len()) {
        Ok(len) if len <= MAX_CLONE_MARKER_BYTES => len,
        _ => return false,
    };
    let mut bytes = Vec::with_capacity(marker_len);
    if marker
        .by_ref()
        .take(MAX_CLONE_MARKER_BYTES.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .is_err()
        || bytes.len() > MAX_CLONE_MARKER_BYTES
    {
        return false;
    }
    let marker_after = match open_file_no_follow(&marker_path) {
        Ok(marker) => marker,
        Err(_) => return false,
    };
    if !marker_after
        .metadata()
        .is_ok_and(|metadata| metadata.is_file())
        || std_file_identity(&marker_after) != Some(marker_identity)
    {
        return false;
    }
    let Some(journal) = parse_clone_marker_journal(&bytes, workflow_id) else {
        return false;
    };
    if !journal.partial.is_empty() {
        return false;
    }
    let Some(marker) = journal.current() else {
        return false;
    };
    if matches!(
        marker.phase,
        ClonePublicationPhase::Aborted | ClonePublicationPhase::Retired
    ) {
        return true;
    }
    let Some(expected_target) = marker.complete_target_identity(workflow_id) else {
        return false;
    };

    let target = match open_directory_no_follow(skill_root) {
        Ok(target) => target,
        Err(_) => return false,
    };
    if !target.metadata().is_ok_and(|metadata| metadata.is_dir())
        || std_file_identity(&target) != Some(expected_target)
    {
        return false;
    }
    let target_after = match open_directory_no_follow(skill_root) {
        Ok(target) => target,
        Err(_) => return false,
    };
    target_after
        .metadata()
        .is_ok_and(|metadata| metadata.is_dir())
        && std_file_identity(&target_after) == Some(expected_target)
}

async fn clone_marker_allows_publication(skill_file: &Path) -> bool {
    let skill_file = skill_file.to_path_buf();
    tokio::task::spawn_blocking(move || clone_marker_allows_publication_sync(&skill_file))
        .await
        .unwrap_or(false)
}

pub(crate) async fn open_skill_file_no_follow(path: &Path) -> std::io::Result<tokio::fs::File> {
    let owned = path.to_path_buf();
    let file = tokio::task::spawn_blocking(move || open_file_no_follow(&owned))
        .await
        .map_err(|error| {
            std::io::Error::other(format!("workflow file open task failed: {error}"))
        })??;
    Ok(tokio::fs::File::from_std(file))
}

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
    /// Stable user-local `${BAMBOO_DATA_DIR}/projects/<id>/skills*`.
    Project,
    /// Current repo/workspace-local `<workspace>/.bamboo/skills*` overlay.
    Workspace,
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
async fn find_skill_files(dir: &Path, max_candidates: usize) -> Vec<PathBuf> {
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
                        if skill_files.len() > max_candidates {
                            break;
                        }
                    } else {
                        let key = ("non-regular-skill-file", &skill_file);
                        let error = "symlink or non-regular file";
                        if STATIC_WARNINGS.insert_if_new(&key, error) {
                            warn!(
                                "Ignoring symlinked or non-regular skill file: {:?}",
                                skill_file
                            );
                        } else {
                            debug!(
                                "Ignoring symlinked or non-regular skill file: {:?}",
                                skill_file
                            );
                        }
                    }
                    continue; // Don't recurse into skill directories
                }
                Ok(false) => {
                    // Not a skill directory, recurse into it
                    let remaining = max_candidates
                        .saturating_add(1)
                        .saturating_sub(skill_files.len());
                    let sub_skills = Box::pin(find_skill_files(&path, remaining)).await;
                    skill_files.extend(sub_skills);
                    if skill_files.len() > max_candidates {
                        break;
                    }
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
    load_skills_from_discovery_dirs_detailed_with_limits(discovery_dirs, 8 * 1024 * 1024, 1024)
        .await
}

pub async fn load_skills_from_discovery_dirs_detailed_with_limits(
    discovery_dirs: &[SkillDiscoveryDir],
    max_skill_file_bytes: usize,
    max_skill_candidates: usize,
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

        let mut skill_files = find_skill_files(&discovery.dir, max_skill_candidates).await;
        if skill_files.len() > max_skill_candidates
            || report
                .loaded
                .len()
                .saturating_add(report.failed.len())
                .saturating_add(skill_files.len())
                > max_skill_candidates
        {
            return Err(SkillError::Storage(format!(
                "workflow catalog candidate count exceeds limit ({max_skill_candidates})"
            )));
        }
        skill_files.sort();
        for skill_file in skill_files {
            if !clone_marker_allows_publication(&skill_file).await {
                let skill_root = skill_file
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_default();
                report.failed.push(FailedSkillRecord {
                    skill_id: skill_root
                        .file_name()
                        .and_then(|name| name.to_str())
                        .map(str::to_string),
                    skill_root,
                    skill_file,
                    source: discovery.source,
                    mode: discovery.mode.clone(),
                    error: "Workflow clone publication is incomplete".to_string(),
                });
                continue;
            }
            let metadata_bytes = fs::metadata(&skill_file).await?.len() as usize;
            if metadata_bytes > max_skill_file_bytes {
                let skill_root = skill_file
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_default();
                report.failed.push(FailedSkillRecord {
                    skill_id: skill_root.file_name().and_then(|name| name.to_str()).map(str::to_string),
                    skill_root,
                    skill_file,
                    source: discovery.source,
                    mode: discovery.mode.clone(),
                    error: format!("SKILL.md exceeds per-file limit ({metadata_bytes} > {max_skill_file_bytes} bytes)"),
                });
                continue;
            }
            let file = open_skill_file_no_follow(&skill_file).await?;
            let mut bytes = Vec::with_capacity(metadata_bytes.min(max_skill_file_bytes));
            file.take(max_skill_file_bytes.saturating_add(1) as u64)
                .read_to_end(&mut bytes)
                .await?;
            if bytes.len() > max_skill_file_bytes {
                let skill_root = skill_file
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_default();
                report.failed.push(FailedSkillRecord {
                    skill_id: skill_root.file_name().and_then(|name| name.to_str()).map(str::to_string),
                    skill_root,
                    skill_file,
                    source: discovery.source,
                    mode: discovery.mode.clone(),
                    error: format!("SKILL.md exceeds per-file limit after read ({} > {max_skill_file_bytes} bytes)", bytes.len()),
                });
                continue;
            }
            match String::from_utf8(bytes) {
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
                        let error_fingerprint = error.to_string();
                        let key = ("parse-skill-file", &skill_file);
                        if STATIC_WARNINGS.insert_if_new(&key, &error_fingerprint) {
                            warn!("Failed to parse skill file {:?}: {}", skill_file, error);
                        } else {
                            debug!("Failed to parse skill file {:?}: {}", skill_file, error);
                        }
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
                    let error_fingerprint = error.to_string();
                    let key = ("decode-skill-file", &skill_file);
                    if STATIC_WARNINGS.insert_if_new(&key, &error_fingerprint) {
                        warn!("Failed to read skill file {:?}: {}", skill_file, error);
                    } else {
                        debug!("Failed to read skill file {:?}: {}", skill_file, error);
                    }
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
    use crate::clone_publication::{
        ClonePublicationMarker, ClonePublicationPhase, CLONE_MARKER_SCHEMA,
    };

    fn valid_skill(name: &str) -> String {
        format!("---\nname: {name}\ndescription: test skill\n---\n\nFollow this skill.")
    }

    fn clone_marker(
        root: &Path,
        workflow_id: &str,
        phase: ClonePublicationPhase,
        target_identity: Option<crate::clone_publication::CloneNodeIdentity>,
    ) {
        let marker = ClonePublicationMarker {
            schema: CLONE_MARKER_SCHEMA,
            workflow_id: workflow_id.to_string(),
            source_revision: 7,
            source_content_digest: "a".repeat(64),
            bundle_digest: "b".repeat(64),
            staging_name: "txn-12345678-1234-1234-1234-123456789abc".to_string(),
            phase,
            stage_identity: (phase != ClonePublicationPhase::Prepared)
                .then_some(target_identity)
                .flatten(),
            target_identity: matches!(
                phase,
                ClonePublicationPhase::Complete | ClonePublicationPhase::Retired
            )
            .then_some(target_identity)
            .flatten(),
        };
        std::fs::write(
            root.join(clone_marker_name(workflow_id)),
            serde_json::to_vec(&marker).expect("marker JSON"),
        )
        .expect("clone marker");
    }

    #[tokio::test]
    async fn clone_marker_only_allows_complete_exact_target_generation() {
        let root = tempfile::tempdir().expect("root");
        let skill = root.path().join("review");
        fs::create_dir_all(&skill).await.expect("skill dir");
        fs::write(skill.join("SKILL.md"), valid_skill("review"))
            .await
            .expect("skill");
        clone_marker(root.path(), "review", ClonePublicationPhase::Prepared, None);
        let discovery = [SkillDiscoveryDir {
            dir: root.path().to_path_buf(),
            source: SkillDirectorySource::Global,
            mode: None,
        }];
        let blocked = load_skills_from_discovery_dirs_detailed(&discovery)
            .await
            .expect("blocked discovery is typed");
        assert!(blocked.loaded.is_empty());
        assert_eq!(blocked.failed.len(), 1);
        assert_eq!(
            blocked.failed[0].error,
            "Workflow clone publication is incomplete"
        );

        let target = open_directory_no_follow(&skill).expect("target handle");
        let identity = std_file_identity(&target).expect("target identity");
        clone_marker(
            root.path(),
            "review",
            ClonePublicationPhase::Complete,
            Some(identity),
        );
        let complete = load_skills_from_discovery_dirs_detailed(&discovery)
            .await
            .expect("complete discovery");
        assert_eq!(complete.loaded.len(), 1);
        assert!(complete.failed.is_empty());

        let other = root.path().join("other");
        fs::create_dir(&other).await.expect("other generation");
        let other = open_directory_no_follow(&other).expect("other target handle");
        let other_identity = std_file_identity(&other).expect("other identity");
        clone_marker(
            root.path(),
            "review",
            ClonePublicationPhase::Complete,
            Some(other_identity),
        );
        let mismatched = load_skills_from_discovery_dirs_detailed(&discovery)
            .await
            .expect("mismatched discovery is typed");
        assert!(mismatched.loaded.is_empty());
        assert_eq!(mismatched.failed.len(), 1);
    }

    #[tokio::test]
    async fn aborted_and_retired_markers_release_an_ordinary_target() {
        for phase in [
            ClonePublicationPhase::Aborted,
            ClonePublicationPhase::Retired,
        ] {
            let root = tempfile::tempdir().expect("root");
            let skill = root.path().join("review");
            fs::create_dir_all(&skill).await.expect("skill dir");
            fs::write(skill.join("SKILL.md"), valid_skill("review"))
                .await
                .expect("skill");
            let target = open_directory_no_follow(&skill).expect("target handle");
            let identity = std_file_identity(&target).expect("target identity");
            clone_marker(root.path(), "review", phase, Some(identity));
            let report = load_skills_from_discovery_dirs_detailed(&[SkillDiscoveryDir {
                dir: root.path().to_path_buf(),
                source: SkillDirectorySource::Global,
                mode: None,
            }])
            .await
            .expect("terminal marker discovery");
            assert_eq!(report.loaded.len(), 1);
            assert!(report.failed.is_empty());
        }
    }

    #[tokio::test]
    async fn malformed_or_link_like_clone_marker_fails_closed() {
        let root = tempfile::tempdir().expect("root");
        let skill = root.path().join("review");
        fs::create_dir_all(&skill).await.expect("skill dir");
        fs::write(skill.join("SKILL.md"), valid_skill("review"))
            .await
            .expect("skill");
        let marker = root.path().join(clone_marker_name("review"));
        fs::write(&marker, b"not a marker").await.expect("marker");
        let discovery = [SkillDiscoveryDir {
            dir: root.path().to_path_buf(),
            source: SkillDirectorySource::Global,
            mode: None,
        }];
        let malformed = load_skills_from_discovery_dirs_detailed(&discovery)
            .await
            .expect("malformed marker is typed");
        assert!(malformed.loaded.is_empty());
        assert_eq!(malformed.failed.len(), 1);

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            std::fs::remove_file(&marker).expect("remove malformed marker");
            let outside = tempfile::NamedTempFile::new().expect("outside marker");
            symlink(outside.path(), &marker).expect("marker symlink");
            let linked = load_skills_from_discovery_dirs_detailed(&discovery)
                .await
                .expect("link-like marker is typed");
            assert!(linked.loaded.is_empty());
            assert_eq!(linked.failed.len(), 1);
        }
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
