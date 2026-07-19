//! Non-destructive, idempotent adapters for pre-catalog workflow formats.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::store::parser::is_valid_skill_id;
use crate::types::{SkillError, SkillResult};

static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(1);

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
            let raw = tokio::fs::read_to_string(&path)
                .await
                .map_err(|e| e.to_string())?;
            let definition: bamboo_domain::WorkflowDefinition =
                serde_yaml::from_str(&raw).map_err(|e| e.to_string())?;
            definition.validate()?;
            Ok::<_, String>(definition.id)
        }
        .await;
        match result {
            Ok(id) => diagnostics.push(YamlMigrationDiagnostic {
                source: path,
                workflow_id: Some(id),
                can_map_to_bundle: true,
                message: "Can be copied verbatim to workflow.yaml; execution remains owned by the orchestration runtime".to_string(),
            }),
            Err(error) => diagnostics.push(YamlMigrationDiagnostic {
                source: path,
                workflow_id: None,
                can_map_to_bundle: false,
                message: error,
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
                            diagnostic.message = error.to_string();
                        }
                    }
                }
                Err(error) => {
                    diagnostic.can_map_to_bundle = false;
                    diagnostic.message = error.to_string();
                }
            },
            Err(error) => {
                diagnostic.can_map_to_bundle = false;
                diagnostic.message = error.to_string();
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
        tokio::fs::write(temp.path().join("unsafe.yaml"), "id: unsafe\nsteps: []\n")
            .await
            .expect("unsafe yaml");
        let diagnostics = diagnose_legacy_yaml_workflows(temp.path()).await;
        assert_eq!(diagnostics.len(), 2);
        assert!(diagnostics.iter().any(|item| item.can_map_to_bundle));
        assert!(diagnostics.iter().any(|item| !item.can_map_to_bundle));
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
}
