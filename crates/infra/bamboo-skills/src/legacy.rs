//! Non-destructive, idempotent adapters for pre-catalog workflow formats.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::store::parser::{
    is_valid_skill_id, parse_markdown_skill, render_skill_markdown, split_frontmatter,
};
use crate::store::storage::{
    open_skill_file_no_follow, FailedSkillRecord, LoadedSkillRecord, SkillDirectorySource,
    SkillDiscoveryDir, SkillLoadReport,
};
use crate::types::SkillDefinition;
use crate::types::{SkillError, SkillResult};

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(1);
pub const MAX_LEGACY_WORKFLOW_FILE_BYTES: usize = 8 * 1024 * 1024;
pub const LEGACY_WORKFLOW_SOURCE_REMOVAL_BOUNDARY: &str = "lotus-119-complete";

pub struct PreparedLegacyWorkflowMigration {
    pub files: BTreeMap<String, crate::store::builtin::BuiltinSkillFile>,
    pub manual_only: bool,
}

fn migration_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn unique_staging_file(
    directory: &Path,
    label: &str,
) -> SkillResult<(PathBuf, tokio::fs::File)> {
    loop {
        let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!(".{label}.{}.{}.tmp", std::process::id(), sequence));
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

/// Atomically replace `target` with a fully-written same-directory staging file.
pub async fn atomic_replace_file(staging: &Path, target: &Path) -> std::io::Result<()> {
    #[cfg(not(windows))]
    {
        tokio::fs::rename(staging, target).await
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };
        let source: Vec<u16> = staging.as_os_str().encode_wide().chain(Some(0)).collect();
        let destination: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
        let result = unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyImportReport {
    pub imported: Vec<String>,
    pub updated: Vec<String>,
    pub already_present: Vec<String>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyWorkflowMigrationOutcome {
    Migrated,
    AlreadyMigrated,
    Conflict,
}

fn public_legacy_error(error: &SkillError) -> String {
    match error {
        SkillError::Yaml(error) => match error.location() {
            Some(location) => format!(
                "legacy workflow: invalid metadata at line {}, column {}",
                location.line(),
                location.column()
            ),
            None => "legacy workflow: invalid metadata".to_string(),
        },
        SkillError::Validation(_) => "legacy workflow: invalid frontmatter".to_string(),
        _ => "legacy workflow: failed to load".to_string(),
    }
}

async fn read_bounded_file(path: &Path, max_bytes: usize) -> SkillResult<Vec<u8>> {
    let metadata_bytes = tokio::fs::metadata(path).await?.len() as usize;
    if metadata_bytes > max_bytes {
        return Err(SkillError::Storage(format!(
            "legacy workflow exceeds per-file limit ({metadata_bytes} > {max_bytes} bytes)"
        )));
    }
    let file = open_skill_file_no_follow(path).await?;
    let mut bytes = Vec::with_capacity(metadata_bytes.min(max_bytes));
    file.take(max_bytes.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > max_bytes {
        return Err(SkillError::Storage(format!(
            "legacy workflow exceeds per-file limit ({} > {max_bytes} bytes)",
            bytes.len()
        )));
    }
    Ok(bytes)
}

/// Read one legacy Workflow source without following symlinks and with the
/// same size bound used by catalog discovery.
pub async fn read_legacy_markdown_workflow(path: &Path) -> SkillResult<String> {
    let bytes = read_bounded_file(path, MAX_LEGACY_WORKFLOW_FILE_BYTES).await?;
    String::from_utf8(bytes)
        .map_err(|_| SkillError::Validation("legacy workflow is not valid UTF-8".to_string()))
}

fn validate_legacy_description(description: &str) -> SkillResult<()> {
    if description.contains('<') || description.contains('>') {
        return Err(SkillError::Validation(
            "Legacy workflow description cannot contain angle brackets".to_string(),
        ));
    }
    if description.len() > 1024 {
        return Err(SkillError::Validation(
            "Legacy workflow description is too long".to_string(),
        ));
    }
    Ok(())
}

fn validate_legacy_source_identity(source_identity: &str) -> SkillResult<()> {
    if source_identity.trim().is_empty()
        || source_identity.starts_with('/')
        || source_identity.contains("..")
        || source_identity.contains('\\')
    {
        return Err(SkillError::Validation(
            "legacy workflow source identity must be a safe relative path".to_string(),
        ));
    }
    Ok(())
}

fn parse_legacy_markdown_adapter(
    source: &Path,
    id: &str,
    content: &str,
) -> SkillResult<SkillDefinition> {
    if !is_valid_skill_id(id) {
        return Err(SkillError::InvalidId(id.to_string()));
    }
    let legacy_name = source
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(id);
    let (description, prompt, placeholder) = if content.lines().next() == Some("---") {
        let (frontmatter, body) = split_frontmatter(content)?;
        let metadata: serde_yaml::Value = serde_yaml::from_str(&frontmatter)?;
        let description = metadata
            .get("description")
            .and_then(serde_yaml::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        match description {
            Some(description) => (description, body.trim().to_string(), false),
            None => (
                format!(
                    "Legacy workflow '{legacy_name}' requires a description before automatic use."
                ),
                body.trim().to_string(),
                true,
            ),
        }
    } else {
        (
            format!("Legacy workflow '{legacy_name}' requires a description before automatic use."),
            content.trim().to_string(),
            true,
        )
    };
    validate_legacy_description(&description)?;
    Ok(SkillDefinition {
        id: id.to_string(),
        name: id.to_string(),
        description,
        license: None,
        compatibility: None,
        metadata: Some(serde_json::json!({
            "legacy_adapter": true,
            "legacy_manual_only": placeholder,
            "legacy_name": legacy_name,
            "original_source": source.to_string_lossy(),
            "format": "workspace_workflow_markdown"
        })),
        prompt,
        tool_refs: Vec::new(),
    })
}

/// Render one already-read legacy markdown source into its immutable canonical
/// bundle. Publication remains the caller's responsibility so HTTP migration
/// can reuse the atomic no-replace clone transaction.
pub fn prepare_legacy_markdown_workflow(
    source: &Path,
    source_identity: &str,
    id: &str,
    content: &str,
    source_catalog_identity: Option<(u64, &str)>,
    description_override: Option<&str>,
) -> SkillResult<PreparedLegacyWorkflowMigration> {
    if !is_valid_skill_id(id) {
        return Err(SkillError::InvalidId(id.to_string()));
    }
    validate_legacy_source_identity(source_identity)?;

    let mut skill = parse_legacy_markdown_adapter(source, id, content)?;
    let mut manual_only = skill
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("legacy_manual_only"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(true);
    if let Some(description) = description_override
        .map(str::trim)
        .filter(|description| !description.is_empty())
    {
        validate_legacy_description(description)?;
        skill.description = description.to_string();
        manual_only = false;
    }
    let mut migration_metadata = serde_json::json!({
        "legacy_migration": true,
        "legacy_manual_only": manual_only,
        "legacy_name": source.file_stem().and_then(|value| value.to_str()).unwrap_or(id),
        "original_source": source_identity,
        "legacy_migration_description_override": description_override
            .map(str::trim)
            .filter(|description| !description.is_empty()),
        "legacy_source_removal_boundary": LEGACY_WORKFLOW_SOURCE_REMOVAL_BOUNDARY,
        "format": "workspace_workflow_markdown"
    });
    if let Some((source_revision, source_content_digest)) = source_catalog_identity {
        let metadata = migration_metadata
            .as_object_mut()
            .expect("legacy migration metadata is an object");
        metadata.insert(
            "legacy_source_revision".to_string(),
            serde_json::Value::from(source_revision),
        );
        metadata.insert(
            "legacy_source_content_digest".to_string(),
            serde_json::Value::from(source_content_digest),
        );
    }
    skill.metadata = Some(migration_metadata);

    let mut files = BTreeMap::new();
    files.insert(
        "SKILL.md".to_string(),
        crate::store::builtin::BuiltinSkillFile {
            bytes: render_skill_markdown(&skill)?.into_bytes(),
            executable: false,
        },
    );
    if manual_only {
        files.insert(
            "agents/bamboo.yaml".to_string(),
            crate::store::builtin::BuiltinSkillFile {
                bytes: b"version: '1'\ninvocation_policy:\n  explicit: true\n  automatic: false\n"
                    .to_vec(),
                executable: false,
            },
        );
    }
    Ok(PreparedLegacyWorkflowMigration { files, manual_only })
}

/// Rebuild the exact catalog digest of already-read source bytes. The handler
/// compares this with the accepted catalog entry immediately before any target
/// publication, making a selection/read replacement a deterministic conflict.
pub fn legacy_markdown_catalog_content_digest(
    entry: &crate::catalog::WorkflowCatalogEntry,
    source: &Path,
    id: &str,
    content: &str,
) -> SkillResult<String> {
    let definition = parse_legacy_markdown_adapter(source, id, content)?;
    Ok(crate::catalog::workflow_catalog_content_digest(
        entry,
        Some(&definition),
        std::iter::empty::<(&str, &[u8])>(),
    ))
}

/// Discover legacy markdown workflows without modifying their source directory.
///
/// Each source file becomes a virtual instruction Workflow whose root identity
/// is the source file itself. Virtual legacy workflows expose no sibling
/// resources and never enter Skill activation.
pub async fn load_legacy_markdown_workflow_records(
    discovery_dirs: &[SkillDiscoveryDir],
    max_file_bytes: usize,
    max_candidates: usize,
) -> SkillResult<SkillLoadReport> {
    let mut report = SkillLoadReport::default();
    for discovery in discovery_dirs {
        let mut entries = match tokio::fs::read_dir(&discovery.dir).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                tracing::warn!(
                    directory = %discovery.dir.display(),
                    %error,
                    "Failed to inspect legacy workflow directory"
                );
                continue;
            }
        };
        let mut sources = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_type = entry.file_type().await?;
            if file_type.is_file()
                && !file_type.is_symlink()
                && path.extension().and_then(|value| value.to_str()) == Some("md")
            {
                sources.push(path);
            }
        }
        sources.sort();
        if report
            .loaded
            .len()
            .saturating_add(report.failed.len())
            .saturating_add(sources.len())
            > max_candidates
        {
            return Err(SkillError::Storage(format!(
                "legacy workflow candidate count exceeds limit ({max_candidates})"
            )));
        }
        for source in sources {
            let Some(legacy_name) = source.file_stem().and_then(|value| value.to_str()) else {
                continue;
            };
            let id = legacy_workflow_skill_id(legacy_name);
            let loaded = async {
                let bytes = read_bounded_file(&source, max_file_bytes).await?;
                let content = String::from_utf8(bytes).map_err(|_| {
                    SkillError::Validation("legacy workflow is not UTF-8".to_string())
                })?;
                parse_legacy_markdown_adapter(&source, &id, &content)
            }
            .await;
            match loaded {
                Ok(skill) => report.loaded.push(LoadedSkillRecord {
                    skill,
                    skill_root: source.clone(),
                    source: discovery.source,
                    mode: discovery.mode.clone(),
                    skill_file: source,
                }),
                Err(error) => report.failed.push(FailedSkillRecord {
                    skill_id: Some(id),
                    skill_root: source.clone(),
                    skill_file: source,
                    source: discovery.source,
                    mode: discovery.mode.clone(),
                    error: public_legacy_error(&error),
                }),
            }
        }
    }
    Ok(report)
}

/// Return in-place workflow discovery roots for installed plugins.
pub async fn discover_plugin_legacy_workflow_dirs(plugins_root: &Path) -> Vec<SkillDiscoveryDir> {
    let mut discovered = Vec::new();
    let mut entries = match tokio::fs::read_dir(plugins_root).await {
        Ok(entries) => entries,
        Err(_) => return discovered,
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry
            .file_name()
            .to_str()
            .is_none_or(|name| name.starts_with('.'))
        {
            continue;
        }
        let is_dir = entry
            .file_type()
            .await
            .map(|value| value.is_dir() && !value.is_symlink())
            .unwrap_or(false);
        if !is_dir {
            continue;
        }
        let plugin_dir = entry.path();
        let manifest = match read_bounded_file(
            &plugin_dir.join("plugin.json"),
            MAX_LEGACY_WORKFLOW_FILE_BYTES,
        )
        .await
        {
            Ok(raw) => raw,
            Err(_) => continue,
        };
        let manifest: serde_json::Value = match serde_json::from_slice(&manifest) {
            Ok(manifest) => manifest,
            Err(_) => continue,
        };
        let Some(declared) = manifest
            .get("provides")
            .and_then(|provides| provides.get("workflows"))
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };
        let declared_names: BTreeSet<&str> = declared
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect();
        if declared_names.is_empty() || declared_names.len() != declared.len() {
            continue;
        }

        let workflows = plugin_dir.join("workflows");
        let is_real_directory = tokio::fs::symlink_metadata(&workflows)
            .await
            .map(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
            .unwrap_or(false);
        if !is_real_directory {
            continue;
        }
        let mut actual_names = BTreeSet::new();
        let mut workflow_entries = match tokio::fs::read_dir(&workflows).await {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        let mut invalid_entry = false;
        loop {
            let workflow = match workflow_entries.next_entry().await {
                Ok(Some(workflow)) => workflow,
                Ok(None) => break,
                Err(_) => {
                    invalid_entry = true;
                    break;
                }
            };
            let path = workflow.path();
            if path.extension().and_then(|value| value.to_str()) != Some("md") {
                continue;
            }
            let regular = workflow
                .file_type()
                .await
                .map(|file_type| file_type.is_file() && !file_type.is_symlink())
                .unwrap_or(false);
            let Some(filename) = workflow.file_name().to_str().map(str::to_string) else {
                invalid_entry = true;
                break;
            };
            if !regular || !actual_names.insert(filename) {
                invalid_entry = true;
                break;
            }
        }
        if invalid_entry
            || actual_names.len() != declared_names.len()
            || !actual_names
                .iter()
                .all(|name| declared_names.contains(name.as_str()))
        {
            tracing::warn!(
                plugin = %entry.file_name().to_string_lossy(),
                "Skipping plugin legacy workflows because plugin.json is not authoritative for workflows/*.md"
            );
            continue;
        }
        discovered.push(SkillDiscoveryDir {
            dir: workflows,
            source: SkillDirectorySource::Plugin,
            mode: None,
        });
    }
    discovered.sort_by(|left, right| left.dir.cmp(&right.dir));
    discovered
}

fn is_owned_legacy_migration(raw: &str, target: &Path, source_identity: &str) -> bool {
    parse_markdown_skill(target, raw)
        .ok()
        .and_then(|skill| skill.metadata)
        .is_some_and(|metadata| {
            metadata
                .get("legacy_migration")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
                && metadata
                    .get("original_source")
                    .and_then(serde_json::Value::as_str)
                    == Some(source_identity)
        })
}

async fn ensure_manual_only_policy(target_dir: &Path, manual_only: bool) -> SkillResult<()> {
    if !manual_only {
        return Ok(());
    }
    let target_metadata = tokio::fs::symlink_metadata(target_dir).await?;
    if target_metadata.file_type().is_symlink() || !target_metadata.is_dir() {
        return Err(SkillError::Storage(
            "legacy migration target must be a real directory".to_string(),
        ));
    }
    let canonical_target = tokio::fs::canonicalize(target_dir).await?;
    let agents = canonical_target.join("agents");
    match tokio::fs::symlink_metadata(&agents).await {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(SkillError::Storage(
                "legacy migration agents directory must be a real directory".to_string(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir(&agents).await?;
        }
        Err(error) => return Err(error.into()),
    }
    let canonical_agents = tokio::fs::canonicalize(&agents).await?;
    if !canonical_agents.starts_with(&canonical_target) {
        return Err(SkillError::Storage(
            "legacy migration agents directory escapes the target bundle".to_string(),
        ));
    }
    let policy = canonical_agents.join("bamboo.yaml");
    match tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&policy)
        .await
    {
        Ok(mut file) => {
            file.write_all(
                b"version: '1'\ninvocation_policy:\n  explicit: true\n  automatic: false\n",
            )
            .await?;
            file.flush().await?;
            file.sync_all().await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = tokio::fs::symlink_metadata(&policy).await?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(SkillError::Storage(
                    "legacy migration policy must be a regular file".to_string(),
                ));
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

/// Clone one read-only legacy workflow into a canonical workspace Skill bundle.
///
/// The source is never changed or removed. Existing targets are never overwritten;
/// a target created by the same migration is an idempotent success.
pub async fn migrate_legacy_markdown_workflow(
    source: &Path,
    source_identity: &str,
    skills_dir: &Path,
    id: &str,
    description_override: Option<&str>,
) -> SkillResult<LegacyWorkflowMigrationOutcome> {
    let _guard = migration_lock().lock().await;
    let bytes = read_bounded_file(source, MAX_LEGACY_WORKFLOW_FILE_BYTES).await?;
    let content = String::from_utf8(bytes)
        .map_err(|_| SkillError::Validation("legacy workflow is not UTF-8".to_string()))?;
    let prepared = prepare_legacy_markdown_workflow(
        source,
        source_identity,
        id,
        &content,
        None,
        description_override,
    )?;
    let rendered = prepared
        .files
        .get("SKILL.md")
        .expect("prepared legacy migration always contains SKILL.md");

    tokio::fs::create_dir_all(skills_dir).await?;
    if tokio::fs::symlink_metadata(skills_dir)
        .await?
        .file_type()
        .is_symlink()
    {
        return Err(SkillError::Storage(
            "workspace skills directory cannot be a symbolic link".to_string(),
        ));
    }
    let target_dir = skills_dir.join(id);
    let target = target_dir.join("SKILL.md");
    match tokio::fs::symlink_metadata(&target_dir).await {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Ok(LegacyWorkflowMigrationOutcome::Conflict);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    match read_bounded_file(&target, MAX_LEGACY_WORKFLOW_FILE_BYTES).await {
        Ok(existing) => {
            let existing = String::from_utf8(existing).map_err(|_| {
                SkillError::Validation("existing Skill bundle is not UTF-8".to_string())
            })?;
            if is_owned_legacy_migration(&existing, &target, source_identity) {
                ensure_manual_only_policy(&target_dir, prepared.manual_only).await?;
                return Ok(LegacyWorkflowMigrationOutcome::AlreadyMigrated);
            }
            return Ok(LegacyWorkflowMigrationOutcome::Conflict);
        }
        Err(SkillError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    match tokio::fs::create_dir(&target_dir).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Ok(LegacyWorkflowMigrationOutcome::Conflict)
        }
        Err(error) => return Err(error.into()),
    }
    let result = async {
        let (temporary, mut file) = unique_staging_file(&target_dir, "SKILL.md").await?;
        let write_result = async {
            file.write_all(&rendered.bytes).await?;
            file.flush().await?;
            file.sync_all().await?;
            drop(file);
            tokio::fs::hard_link(&temporary, &target).await
        }
        .await;
        let _ = tokio::fs::remove_file(&temporary).await;
        write_result?;
        ensure_manual_only_policy(&target_dir, prepared.manual_only).await
    }
    .await;
    if let Err(error) = result {
        let _ = tokio::fs::remove_file(&target).await;
        let agents = target_dir.join("agents");
        let _ = tokio::fs::remove_file(agents.join("bamboo.yaml")).await;
        let _ = tokio::fs::remove_dir(&agents).await;
        let _ = tokio::fs::remove_dir(&target_dir).await;
        return Err(error);
    }
    Ok(LegacyWorkflowMigrationOutcome::Migrated)
}

/// Map a legacy filename to a stable Agent Skills-compatible bundle id.
///
/// Existing kebab-case names retain their id. Names accepted by the legacy API but rejected by
/// the Agent Skills grammar (spaces, underscores, Unicode, uppercase, and dots) receive a readable
/// ASCII prefix plus a deterministic hash, avoiding lossy normalization collisions.
pub fn legacy_workflow_skill_id(name: &str) -> String {
    if is_valid_skill_id(name) {
        return name.to_string();
    }

    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let hash = name.as_bytes().iter().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    });
    let mut slug = String::new();
    let mut pending_separator = false;
    for character in name.chars() {
        if character.is_ascii_alphanumeric() {
            if pending_separator && !slug.is_empty() {
                slug.push('-');
            }
            pending_separator = false;
            slug.push(character.to_ascii_lowercase());
            if slug.len() >= 40 {
                break;
            }
        } else {
            pending_separator = true;
        }
    }
    let slug = slug.trim_matches('-');
    if slug.is_empty() {
        format!("legacy-{hash:016x}")
    } else {
        format!("{slug}-legacy-{hash:016x}")
    }
}

/// Import `${BAMBOO_DATA_DIR}/workflows/*.md` as Skill-compatible bundles.
/// The source is never deleted and an existing target is never overwritten.
pub async fn import_legacy_markdown_workflows(
    workflows_dir: &Path,
    skills_dir: &Path,
) -> SkillResult<LegacyImportReport> {
    let mut report = LegacyImportReport {
        imported: Vec::new(),
        updated: Vec::new(),
        already_present: Vec::new(),
        diagnostics: Vec::new(),
    };
    let mut entries = match tokio::fs::read_dir(workflows_dir).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(report),
        Err(error) => return Err(error.into()),
    };
    let mut sources = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let file_type = entry.file_type().await?;
        if file_type.is_file()
            && !file_type.is_symlink()
            && path.extension().and_then(|value| value.to_str()) == Some("md")
        {
            sources.push(path);
        }
    }
    sources.sort();

    for source in sources {
        let Some(legacy_name) = source.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        let id = legacy_workflow_skill_id(legacy_name);
        let body = tokio::fs::read_to_string(&source).await?;
        match sync_legacy_markdown_bundle(&source, skills_dir, &id, &body).await? {
            LegacySyncOutcome::Imported => report.imported.push(id.clone()),
            LegacySyncOutcome::Updated => report.updated.push(id.clone()),
            LegacySyncOutcome::Unchanged => report.already_present.push(id.clone()),
            LegacySyncOutcome::Conflict => report.diagnostics.push(format!(
                "{} was not imported because {} is not owned by this legacy source",
                source.display(),
                skills_dir.join(&id).join("SKILL.md").display()
            )),
        }
    }
    Ok(report)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacySyncOutcome {
    Imported,
    Updated,
    Unchanged,
    Conflict,
}

fn is_owned_legacy_skill(raw: &str, target: &Path, source: &Path) -> bool {
    crate::store::parser::parse_markdown_skill(target, raw)
        .ok()
        .and_then(|skill| skill.metadata)
        .is_some_and(|metadata| {
            metadata.get("legacy_import").and_then(|v| v.as_bool()) == Some(true)
                && metadata.get("original_source").and_then(|v| v.as_str())
                    == Some(source.to_string_lossy().as_ref())
        })
}

/// Synchronize one legacy source into its adapter bundle. Existing non-legacy bundles are never
/// overwritten; an existing bundle owned by the same source is updated atomically.
pub async fn sync_legacy_markdown_bundle(
    source: &Path,
    skills_dir: &Path,
    id: &str,
    body: &str,
) -> SkillResult<LegacySyncOutcome> {
    if !is_valid_skill_id(id) {
        return Err(SkillError::InvalidId(id.to_string()));
    }
    let target_dir = skills_dir.join(id);
    let target = target_dir.join("SKILL.md");
    let legacy_name = source
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or(id);
    let frontmatter = serde_yaml::to_string(&serde_json::json!({
        "name": id,
        "description": format!("Imported legacy workflow '{legacy_name}'."),
        "metadata": {
            "legacy_import": true,
            "legacy_name": legacy_name,
            "original_source": source.to_string_lossy(),
            "format": "workflow_markdown"
        }
    }))?;
    let content = format!("---\n{frontmatter}---\n\n{}\n", body.trim_end());
    let existing = match tokio::fs::read_to_string(&target).await {
        Ok(existing) => Some(existing),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if let Some(existing) = existing {
        if !is_owned_legacy_skill(&existing, &target, source) {
            return Ok(LegacySyncOutcome::Conflict);
        }
        if existing == content {
            return Ok(LegacySyncOutcome::Unchanged);
        }
        let (temporary, mut file) = unique_staging_file(&target_dir, "SKILL.md").await?;
        if let Err(error) = async {
            file.write_all(content.as_bytes()).await?;
            file.flush().await?;
            file.sync_all().await?;
            drop(file);
            atomic_replace_file(&temporary, &target).await
        }
        .await
        {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(error.into());
        }
        return Ok(LegacySyncOutcome::Updated);
    }
    tokio::fs::create_dir_all(&target_dir).await?;
    let (temporary, mut file) = unique_staging_file(&target_dir, "SKILL.md").await?;
    if let Err(error) = async {
        file.write_all(content.as_bytes()).await?;
        file.flush().await?;
        file.sync_all().await?;
        tokio::fs::hard_link(&temporary, &target).await
    }
    .await
    {
        let _ = tokio::fs::remove_file(&temporary).await;
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            return Ok(LegacySyncOutcome::Conflict);
        }
        return Err(error.into());
    }
    let _ = tokio::fs::remove_file(&temporary).await;
    Ok(LegacySyncOutcome::Imported)
}

pub async fn legacy_bundle_preflight(
    source: &Path,
    skills_dir: &Path,
    id: &str,
) -> SkillResult<bool> {
    if !is_valid_skill_id(id) {
        return Err(SkillError::InvalidId(id.to_string()));
    }
    let target = skills_dir.join(id).join("SKILL.md");
    match tokio::fs::read_to_string(&target).await {
        Ok(raw) => Ok(is_owned_legacy_skill(&raw, &target, source)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error.into()),
    }
}

/// Remove only an adapter bundle owned by this legacy source.
pub async fn remove_legacy_markdown_bundle(
    source: &Path,
    skills_dir: &Path,
    id: &str,
) -> SkillResult<bool> {
    let target = skills_dir.join(id).join("SKILL.md");
    let raw = match tokio::fs::read_to_string(&target).await {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if !is_owned_legacy_skill(&raw, &target, source) {
        return Ok(false);
    }
    tokio::fs::remove_file(&target).await?;
    let dir = target.parent().expect("SKILL.md has parent");
    if tokio::fs::read_dir(dir)
        .await?
        .next_entry()
        .await?
        .is_none()
    {
        tokio::fs::remove_dir(dir).await?;
    }
    Ok(true)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct YamlMigrationDiagnostic {
    pub source: PathBuf,
    pub workflow_id: Option<String>,
    pub can_map_to_bundle: bool,
    pub message: String,
}

const MAPPABLE_YAML_DIAGNOSTIC: &str =
    "Legacy orchestration can be copied verbatim without changing execution semantics";
const UNMAPPABLE_YAML_DIAGNOSTIC: &str =
    "Legacy orchestration is invalid or cannot be mapped without changing execution semantics";

/// Inspect old WorkflowDefinition YAML without writing or changing execution semantics.
pub async fn diagnose_legacy_yaml_workflows(dir: &Path) -> Vec<YamlMigrationDiagnostic> {
    let mut diagnostics = Vec::new();
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(entries) => entries,
        Err(_) => return diagnostics,
    };
    let mut paths = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        if path
            .extension()
            .and_then(|value| value.to_str())
            .is_some_and(|value| matches!(value, "yaml" | "yml"))
        {
            paths.push(path);
        }
    }
    paths.sort();
    for path in paths {
        let result = async {
            let raw = read_bounded_file(&path, MAX_LEGACY_WORKFLOW_FILE_BYTES).await?;
            let definition: bamboo_domain::WorkflowDefinition =
                serde_yaml::from_slice(&raw).map_err(SkillError::from)?;
            definition.validate().map_err(SkillError::Validation)?;
            Ok::<_, SkillError>(definition.id)
        }
        .await;
        match result {
            Ok(id) => diagnostics.push(YamlMigrationDiagnostic {
                source: path,
                workflow_id: Some(id),
                can_map_to_bundle: true,
                message: MAPPABLE_YAML_DIAGNOSTIC.to_string(),
            }),
            Err(_) => diagnostics.push(YamlMigrationDiagnostic {
                source: path,
                workflow_id: None,
                can_map_to_bundle: false,
                message: UNMAPPABLE_YAML_DIAGNOSTIC.to_string(),
            }),
        }
    }
    diagnostics
}

/// Copy safely mappable legacy WorkflowDefinition files into existing Skill bundles. The source
/// YAML is read-only, an existing workflow.yaml is never overwritten, and definitions without a
/// matching instruction bundle remain diagnostics instead of acquiring invented instructions.
pub async fn migrate_legacy_yaml_workflows(
    dir: &Path,
    skills_dir: &Path,
) -> Vec<YamlMigrationDiagnostic> {
    let mut diagnostics = diagnose_legacy_yaml_workflows(dir).await;
    for diagnostic in &mut diagnostics {
        if !diagnostic.can_map_to_bundle {
            continue;
        }
        let Some(id) = diagnostic.workflow_id.as_deref() else {
            diagnostic.can_map_to_bundle = false;
            diagnostic.message = "Missing workflow id".to_string();
            continue;
        };
        if !is_valid_skill_id(id) {
            diagnostic.can_map_to_bundle = false;
            diagnostic.message = "Workflow id is not a valid skill bundle id".to_string();
            continue;
        }
        let bundle = skills_dir.join(id);
        if !tokio::fs::try_exists(&bundle.join("SKILL.md"))
            .await
            .unwrap_or(false)
        {
            diagnostic.can_map_to_bundle = false;
            diagnostic.message =
                "No matching Skill bundle; refusing to invent instruction semantics".to_string();
            continue;
        }
        let target = bundle.join("workflow.yaml");
        if tokio::fs::try_exists(&target).await.unwrap_or(false) {
            let valid = match tokio::fs::read_to_string(&target).await {
                Ok(raw) => serde_yaml::from_str::<bamboo_domain::WorkflowDefinition>(&raw)
                    .ok()
                    .is_some_and(|definition| definition.validate().is_ok()),
                Err(_) => false,
            };
            diagnostic.message = if valid {
                "workflow.yaml already present; left unchanged".to_string()
            } else {
                "invalid workflow.yaml already present; left unchanged for user repair".to_string()
            };
            continue;
        }
        match tokio::fs::read(&diagnostic.source).await {
            Ok(bytes) => match unique_staging_file(&bundle, "workflow.yaml").await {
                Ok((staging, mut file)) => {
                    let result = async {
                        file.write_all(&bytes).await?;
                        file.flush().await?;
                        file.sync_all().await?;
                        tokio::fs::hard_link(&staging, &target).await
                    }
                    .await;
                    let _ = tokio::fs::remove_file(&staging).await;
                    match result {
                        Ok(()) => {
                            diagnostic.message =
                                "Copied to workflow.yaml; source left unchanged".to_string()
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                            diagnostic.message =
                                "workflow.yaml already present; left unchanged".to_string()
                        }
                        Err(error) => {
                            diagnostic.can_map_to_bundle = false;
                            tracing::warn!(%error, "failed to publish legacy workflow.yaml");
                            diagnostic.message = UNMAPPABLE_YAML_DIAGNOSTIC.to_string();
                        }
                    }
                }
                Err(error) => {
                    diagnostic.can_map_to_bundle = false;
                    tracing::warn!(%error, "failed to stage legacy workflow.yaml");
                    diagnostic.message = UNMAPPABLE_YAML_DIAGNOSTIC.to_string();
                }
            },
            Err(error) => {
                diagnostic.can_map_to_bundle = false;
                tracing::warn!(%error, "failed to read legacy workflow.yaml");
                diagnostic.message = UNMAPPABLE_YAML_DIAGNOSTIC.to_string();
            }
        }
    }
    diagnostics
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn markdown_import_is_lossless_idempotent_and_keeps_source() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workflows = temp.path().join("workflows");
        let skills = temp.path().join("skills");
        tokio::fs::create_dir_all(&workflows)
            .await
            .expect("workflows");
        let source = workflows.join("release.md");
        tokio::fs::write(&source, "Release $ARGUMENTS\n")
            .await
            .expect("legacy workflow");

        let first = import_legacy_markdown_workflows(&workflows, &skills)
            .await
            .expect("first import");
        let second = import_legacy_markdown_workflows(&workflows, &skills)
            .await
            .expect("second import");
        assert_eq!(first.imported, vec!["release"]);
        assert_eq!(second.already_present, vec!["release"]);
        assert_eq!(
            tokio::fs::read_to_string(&source)
                .await
                .expect("source kept"),
            "Release $ARGUMENTS\n"
        );
        let imported = tokio::fs::read_to_string(skills.join("release/SKILL.md"))
            .await
            .expect("imported skill");
        assert!(imported.contains("Release $ARGUMENTS"));
        assert!(imported.contains("legacy_import: true"));
        assert!(imported.contains(source.to_string_lossy().as_ref()));

        tokio::fs::write(&source, "Release safely\n")
            .await
            .expect("external legacy update");
        let updated = import_legacy_markdown_workflows(&workflows, &skills)
            .await
            .expect("sync update");
        assert_eq!(updated.updated, vec!["release"]);
        assert!(tokio::fs::read_to_string(skills.join("release/SKILL.md"))
            .await
            .expect("updated skill")
            .contains("Release safely"));
    }

    #[tokio::test]
    async fn markdown_import_preserves_legacy_names_outside_skill_id_grammar() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workflows = temp.path().join("workflows");
        let skills = temp.path().join("skills");
        tokio::fs::create_dir_all(&workflows)
            .await
            .expect("workflows");
        let source = workflows.join("发布 Workflow_v2.md");
        tokio::fs::write(&source, "Preserve this body exactly.\n")
            .await
            .expect("legacy workflow");

        let expected_id = legacy_workflow_skill_id("发布 Workflow_v2");
        assert!(is_valid_skill_id(&expected_id));
        let first = import_legacy_markdown_workflows(&workflows, &skills)
            .await
            .expect("first import");
        let second = import_legacy_markdown_workflows(&workflows, &skills)
            .await
            .expect("second import");
        assert_eq!(first.imported, vec![expected_id.clone()]);
        assert_eq!(second.already_present, vec![expected_id.clone()]);

        let imported = tokio::fs::read_to_string(skills.join(expected_id).join("SKILL.md"))
            .await
            .expect("imported bundle");
        assert!(imported.contains("legacy_name: 发布 Workflow_v2"));
        assert!(imported.contains("Preserve this body exactly."));
        assert!(source.exists(), "legacy source remains untouched");
    }

    #[tokio::test]
    async fn yaml_diagnostics_refuse_to_infer_missing_composition() {
        let temp = tempfile::tempdir().expect("tempdir");
        tokio::fs::write(
            temp.path().join("safe.yaml"),
            "id: safe\nname: Safe\ndescription: Safe migration\nversion: '1'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
        )
            .await
            .expect("safe yaml");
        let unsafe_source = temp.path().join("unsafe.yaml");
        let unsafe_bytes = b"id: unsafe\ndescription: PRIVATE-DIAGNOSTIC-SENTINEL\nsteps: []\n";
        tokio::fs::write(&unsafe_source, unsafe_bytes)
            .await
            .expect("unsafe yaml");
        let diagnostics = diagnose_legacy_yaml_workflows(temp.path()).await;
        assert_eq!(diagnostics.len(), 2);
        assert!(diagnostics.iter().any(|item| item.can_map_to_bundle));
        let invalid = diagnostics
            .iter()
            .find(|item| !item.can_map_to_bundle)
            .expect("invalid diagnostic");
        assert_eq!(invalid.message, UNMAPPABLE_YAML_DIAGNOSTIC);
        assert!(!invalid.message.contains("PRIVATE-DIAGNOSTIC-SENTINEL"));
        assert!(!invalid.message.contains("unsafe.yaml"));
        assert_eq!(
            tokio::fs::read(&unsafe_source)
                .await
                .expect("source remains"),
            unsafe_bytes
        );
    }

    #[test]
    fn legacy_catalog_digest_binds_the_selected_source_bytes() {
        let source = Path::new("workflows/review.md");
        let selected = "---\ndescription: Review the selected change.\n---\nReview A.\n";
        let replacement = "---\ndescription: Review the selected change.\n---\nReview B.\n";
        let definition =
            parse_legacy_markdown_adapter(source, "review", selected).expect("selected definition");
        let mut entry = crate::catalog::entry_from_skill(
            &definition,
            SkillDirectorySource::Global,
            7,
            crate::catalog::BundleMetadata::default(),
        );
        entry.content_digest = crate::catalog::workflow_catalog_content_digest(
            &entry,
            Some(&definition),
            std::iter::empty::<(&str, &[u8])>(),
        );

        assert_eq!(
            legacy_markdown_catalog_content_digest(&entry, source, "review", selected)
                .expect("selected digest"),
            entry.content_digest
        );
        assert_ne!(
            legacy_markdown_catalog_content_digest(&entry, source, "review", replacement)
                .expect("replacement digest"),
            entry.content_digest
        );
    }

    #[tokio::test]
    async fn yaml_migration_copies_only_into_existing_skill_bundle() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workflows = temp.path().join("workflows");
        let skills = temp.path().join("skills");
        tokio::fs::create_dir_all(&workflows)
            .await
            .expect("workflows");
        tokio::fs::create_dir_all(skills.join("safe"))
            .await
            .expect("bundle");
        tokio::fs::write(
            skills.join("safe/SKILL.md"),
            "---\nname: safe\ndescription: Safe workflow\n---\nInstructions\n",
        )
        .await
        .expect("skill");
        let source = workflows.join("safe.yaml");
        tokio::fs::write(
            &source,
            "id: safe\nname: Safe\ndescription: Safe migration\nversion: '1'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
        )
        .await
        .expect("yaml");
        tokio::fs::write(skills.join("safe/.workflow.yaml.stale.tmp"), "partial: [")
            .await
            .expect("simulate stale staging file");
        let diagnostics = migrate_legacy_yaml_workflows(&workflows, &skills).await;
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].can_map_to_bundle);
        assert!(skills.join("safe/workflow.yaml").exists());
        let recovered = tokio::fs::read_to_string(skills.join("safe/workflow.yaml"))
            .await
            .expect("recovered workflow");
        let definition: bamboo_domain::WorkflowDefinition =
            serde_yaml::from_str(&recovered).expect("complete definition");
        assert_eq!(definition.id, "safe");
        assert!(source.exists(), "source remains read-only");
    }

    #[tokio::test]
    async fn yaml_migration_never_replaces_invalid_existing_target() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workflows = temp.path().join("workflows");
        let bundle = temp.path().join("skills/safe");
        tokio::fs::create_dir_all(&workflows)
            .await
            .expect("workflows");
        tokio::fs::create_dir_all(&bundle).await.expect("bundle");
        tokio::fs::write(
            bundle.join("SKILL.md"),
            "---\nname: safe\ndescription: Safe workflow\n---\nInstructions\n",
        )
        .await
        .expect("skill");
        tokio::fs::write(
            workflows.join("safe.yaml"),
            "id: safe\nname: Safe\ndescription: Safe migration\nversion: '1'\ncomposition:\n  type: call\n  tool: read_file\n  args: {}\n",
        )
        .await
        .expect("source");
        let invalid = b"partial: [";
        tokio::fs::write(bundle.join("workflow.yaml"), invalid)
            .await
            .expect("invalid target");
        let diagnostics =
            migrate_legacy_yaml_workflows(&workflows, &temp.path().join("skills")).await;
        assert!(diagnostics[0].message.contains("left unchanged"));
        assert_eq!(
            tokio::fs::read(bundle.join("workflow.yaml"))
                .await
                .expect("target preserved"),
            invalid
        );
    }

    #[tokio::test]
    async fn workspace_legacy_discovery_is_read_only_and_manual_without_description() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workflows = temp.path().join("workspace/.bamboo/workflows");
        tokio::fs::create_dir_all(&workflows)
            .await
            .expect("workflows");
        let source = workflows.join("review.md");
        tokio::fs::write(&source, "Review the current diff.\n")
            .await
            .expect("source");
        let report = load_legacy_markdown_workflow_records(
            &[SkillDiscoveryDir {
                dir: workflows.clone(),
                source: SkillDirectorySource::Workspace,
                mode: None,
            }],
            1024,
            10,
        )
        .await
        .expect("discovery");

        assert!(report.failed.is_empty());
        assert_eq!(report.loaded.len(), 1);
        let record = &report.loaded[0];
        assert_eq!(record.skill.id, "review");
        assert_eq!(record.skill.prompt, "Review the current diff.");
        assert_eq!(record.skill_root, source);
        assert_eq!(record.source, SkillDirectorySource::Workspace);
        let metadata = record.skill.metadata.as_ref().expect("legacy metadata");
        assert_eq!(metadata["legacy_adapter"], true);
        assert_eq!(metadata["legacy_manual_only"], true);
        assert!(record.skill.description.contains("requires a description"));
        assert!(!temp
            .path()
            .join("workspace/.bamboo/skills/review/SKILL.md")
            .exists());
    }

    #[tokio::test]
    async fn described_legacy_frontmatter_is_eligible_for_automatic_matching() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workflows = temp.path().join("workflows");
        tokio::fs::create_dir_all(&workflows)
            .await
            .expect("workflows");
        tokio::fs::write(
            workflows.join("release.md"),
            "---\ndescription: Use when publishing a verified release.\n---\nPublish safely.\n",
        )
        .await
        .expect("source");
        let report = load_legacy_markdown_workflow_records(
            &[SkillDiscoveryDir {
                dir: workflows,
                source: SkillDirectorySource::Plugin,
                mode: None,
            }],
            1024,
            10,
        )
        .await
        .expect("discovery");
        let skill = &report.loaded[0].skill;
        assert_eq!(skill.description, "Use when publishing a verified release.");
        assert_eq!(skill.prompt, "Publish safely.");
        assert_eq!(
            skill.metadata.as_ref().expect("metadata")["legacy_manual_only"],
            false
        );
    }

    #[tokio::test]
    async fn migration_is_idempotent_non_destructive_and_never_overwrites() {
        let temp = tempfile::tempdir().expect("tempdir");
        let workflows = temp.path().join("workspace/.bamboo/workflows");
        let skills = temp.path().join("workspace/.bamboo/skills");
        tokio::fs::create_dir_all(&workflows)
            .await
            .expect("workflows");
        let source = workflows.join("review.md");
        tokio::fs::write(&source, "Review exactly this way.\n")
            .await
            .expect("source");

        let first = migrate_legacy_markdown_workflow(
            &source,
            ".bamboo/workflows/review.md",
            &skills,
            "review",
            None,
        )
        .await
        .expect("first migration");
        let second = migrate_legacy_markdown_workflow(
            &source,
            ".bamboo/workflows/review.md",
            &skills,
            "review",
            None,
        )
        .await
        .expect("second migration");
        assert_eq!(first, LegacyWorkflowMigrationOutcome::Migrated);
        assert_eq!(second, LegacyWorkflowMigrationOutcome::AlreadyMigrated);
        assert_eq!(
            tokio::fs::read_to_string(&source)
                .await
                .expect("source kept"),
            "Review exactly this way.\n"
        );
        let target = skills.join("review/SKILL.md");
        let migrated = tokio::fs::read_to_string(&target)
            .await
            .expect("migrated Skill");
        assert!(migrated.contains("legacy_migration: true"));
        assert!(migrated.contains("original_source: .bamboo/workflows/review.md"));
        assert!(migrated.contains(&format!(
            "legacy_source_removal_boundary: {LEGACY_WORKFLOW_SOURCE_REMOVAL_BOUNDARY}"
        )));
        assert!(migrated.contains("Review exactly this way."));
        assert!(skills.join("review/agents/bamboo.yaml").exists());

        tokio::fs::write(&target, migrated.replace("Review exactly", "User edited"))
            .await
            .expect("user edit");
        let third = migrate_legacy_markdown_workflow(
            &source,
            ".bamboo/workflows/review.md",
            &skills,
            "review",
            Some("A replacement description"),
        )
        .await
        .expect("idempotent after edit");
        assert_eq!(third, LegacyWorkflowMigrationOutcome::AlreadyMigrated);
        assert!(tokio::fs::read_to_string(&target)
            .await
            .expect("edited target")
            .contains("User edited"));

        let conflict_source = workflows.join("conflict.md");
        tokio::fs::write(&conflict_source, "Legacy body")
            .await
            .expect("conflict source");
        let conflict_target = skills.join("conflict");
        tokio::fs::create_dir_all(&conflict_target)
            .await
            .expect("target");
        tokio::fs::write(
            conflict_target.join("SKILL.md"),
            "---\nname: conflict\ndescription: User owned\n---\nKeep me.\n",
        )
        .await
        .expect("user target");
        let conflict = migrate_legacy_markdown_workflow(
            &conflict_source,
            ".bamboo/workflows/conflict.md",
            &skills,
            "conflict",
            None,
        )
        .await
        .expect("conflict result");
        assert_eq!(conflict, LegacyWorkflowMigrationOutcome::Conflict);
        assert!(tokio::fs::read_to_string(conflict_target.join("SKILL.md"))
            .await
            .expect("user target kept")
            .contains("Keep me."));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn migration_rejects_symlinked_target_and_policy_directories() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir");
        let workflows = temp.path().join("workspace/.bamboo/workflows");
        let skills = temp.path().join("workspace/.bamboo/skills");
        let outside = temp.path().join("outside");
        tokio::fs::create_dir_all(&workflows)
            .await
            .expect("workflows");
        tokio::fs::create_dir_all(&skills).await.expect("skills");
        tokio::fs::create_dir_all(&outside).await.expect("outside");
        let source = workflows.join("review.md");
        tokio::fs::write(&source, "Review safely.\n")
            .await
            .expect("source");

        symlink(&outside, skills.join("review")).expect("target symlink");
        let target_conflict = migrate_legacy_markdown_workflow(
            &source,
            ".bamboo/workflows/review.md",
            &skills,
            "review",
            None,
        )
        .await
        .expect("symlink target is a conflict");
        assert_eq!(target_conflict, LegacyWorkflowMigrationOutcome::Conflict);
        assert!(!outside.join("SKILL.md").exists());
        tokio::fs::remove_file(skills.join("review"))
            .await
            .expect("remove target symlink");

        migrate_legacy_markdown_workflow(
            &source,
            ".bamboo/workflows/review.md",
            &skills,
            "review",
            Some("Use when reviewing a code change."),
        )
        .await
        .expect("initial migration");
        symlink(&outside, skills.join("review/agents")).expect("agents symlink");
        let error = migrate_legacy_markdown_workflow(
            &source,
            ".bamboo/workflows/review.md",
            &skills,
            "review",
            None,
        )
        .await
        .expect_err("symlinked agents directory must be rejected");
        assert!(error.to_string().contains("agents directory"));
        assert!(!outside.join("bamboo.yaml").exists());
    }
}
