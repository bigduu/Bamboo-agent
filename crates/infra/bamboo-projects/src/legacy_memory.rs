//! Crash-resumable migration from path-hash Project memory scopes.
//!
//! The old scope is never modified. Files are copied into a transaction stage,
//! hash-verified, then installed into the first-class Project memory root with
//! no-clobber semantics. A committed journal is the durable commit marker.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use bamboo_domain::{
    LegacyMemoryFileDisposition, LegacyMemoryMigrationFile, LegacyMemoryMigrationPhase,
    LegacyMemoryMigrationReport, LegacyMemoryReadAlias, ProjectId,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    assert_plain_directory, ensure_confined_directory, lock_exclusive, replace_path,
    sync_directory, validate_existing_confined_directory, validate_legacy_project_key,
    write_bytes_atomic, write_json_atomic, ProjectStore, ProjectStoreError, ProjectStoreResult,
    TempCleanup,
};

const JOURNAL_SCHEMA_VERSION: u32 = 1;
const LEGACY_MEMORY_STATE_DIR: &str = "legacy-memory-migration";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyMemoryReadRoot {
    pub legacy_project_key: String,
    pub root: PathBuf,
    pub read_only: bool,
}

/// Ordered memory roots for runtime read compatibility.
///
/// `primary` always wins. `legacy_aliases` are read-only fallback roots and
/// must never be used for new writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMemoryReadRoots {
    pub primary: PathBuf,
    pub legacy_aliases: Vec<LegacyMemoryReadRoot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyMemoryMigrationJournal {
    schema_version: u32,
    report: LegacyMemoryMigrationReport,
    #[serde(default)]
    resource_revision_bumped: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MigrationFault {
    None,
    AfterJournal,
    AfterFirstStaged,
    AfterFirstCommitted,
}

impl ProjectStore {
    /// Physical legacy path-hash scope. This is always read-only.
    pub fn legacy_memory_source_root(
        &self,
        legacy_project_key: &str,
    ) -> ProjectStoreResult<PathBuf> {
        validate_legacy_project_key(legacy_project_key)?;
        Ok(self
            .paths()
            .data_dir()
            .join("memory")
            .join("v1")
            .join("scopes")
            .join("projects")
            .join(legacy_project_key))
    }

    /// Copy a declared legacy scope into
    /// `${BAMBOO_DATA_DIR}/projects/<id>/memory/v1`.
    pub fn migrate_legacy_memory(
        &self,
        project_id: &ProjectId,
        legacy_project_key: &str,
    ) -> ProjectStoreResult<LegacyMemoryMigrationReport> {
        self.migrate_legacy_memory_inner(project_id, legacy_project_key, MigrationFault::None)
    }

    /// Return a durable in-progress/committed journal, if one exists.
    pub fn legacy_memory_migration_status(
        &self,
        project_id: &ProjectId,
        legacy_project_key: &str,
    ) -> ProjectStoreResult<Option<LegacyMemoryMigrationReport>> {
        validate_legacy_project_key(legacy_project_key)?;
        self.get(project_id)?;
        let project_home = self.paths().project_home(project_id);
        validate_existing_confined_directory(self.paths().data_dir(), &project_home)?;
        let state_root = self.legacy_memory_state_root(project_id);
        if std::fs::symlink_metadata(&state_root)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return Ok(None);
        }
        validate_existing_confined_directory(&project_home, &state_root)?;
        let _lock = lock_exclusive(state_root.join(".lock"))?;
        Ok(self
            .load_migration_journal(project_id, legacy_project_key)?
            .map(|journal| journal.report))
    }

    /// Alias metadata for every manifest-declared legacy key.
    ///
    /// Aliases are active during and after migration. Runtime readers must
    /// search Project-home first and use available legacy roots only as
    /// read-only fallbacks.
    pub fn legacy_memory_aliases(
        &self,
        project_id: &ProjectId,
    ) -> ProjectStoreResult<Vec<LegacyMemoryReadAlias>> {
        let manifest = self.get(project_id)?;
        let mut aliases = Vec::with_capacity(manifest.legacy_project_keys.len());
        for key in manifest.legacy_project_keys {
            let source = self.legacy_memory_source_root(&key)?;
            let source_available =
                validate_existing_confined_directory(self.paths().data_dir(), &source).is_ok();
            let migration_committed = self
                .legacy_memory_migration_status(project_id, &key)?
                .is_some_and(|report| report.phase == LegacyMemoryMigrationPhase::Committed);
            aliases.push(LegacyMemoryReadAlias {
                legacy_project_key: key,
                read_only: true,
                project_home_precedence: true,
                source_available,
                migration_committed,
            });
        }
        Ok(aliases)
    }

    pub fn project_memory_read_roots(
        &self,
        project_id: &ProjectId,
    ) -> ProjectStoreResult<ProjectMemoryReadRoots> {
        let aliases = self
            .legacy_memory_aliases(project_id)?
            .into_iter()
            .filter(|alias| alias.source_available)
            .map(|alias| {
                Ok(LegacyMemoryReadRoot {
                    root: self.legacy_memory_source_root(&alias.legacy_project_key)?,
                    legacy_project_key: alias.legacy_project_key,
                    read_only: true,
                })
            })
            .collect::<ProjectStoreResult<Vec<_>>>()?;
        Ok(ProjectMemoryReadRoots {
            primary: self.paths().memory_v1_dir(project_id),
            legacy_aliases: aliases,
        })
    }

    fn migrate_legacy_memory_inner(
        &self,
        project_id: &ProjectId,
        legacy_project_key: &str,
        fault: MigrationFault,
    ) -> ProjectStoreResult<LegacyMemoryMigrationReport> {
        validate_legacy_project_key(legacy_project_key)?;
        let manifest = self.get(project_id)?;
        if !manifest
            .legacy_project_keys
            .iter()
            .any(|key| key == legacy_project_key)
        {
            return Err(ProjectStoreError::Validation(format!(
                "legacy memory key is not declared by project {}",
                project_id
            )));
        }
        let project_home = self.paths().project_home(project_id);
        validate_existing_confined_directory(self.paths().data_dir(), &project_home)?;
        let state_root = self.legacy_memory_state_root(project_id);
        ensure_confined_directory(&project_home, &state_root)?;
        let _lock = lock_exclusive(state_root.join(".lock"))?;

        // Refresh after taking the migration lock; migration operations for
        // this Project are serialized even when the manifest changed while we
        // were waiting.
        let manifest = self.get(project_id)?;
        if !manifest
            .legacy_project_keys
            .iter()
            .any(|key| key == legacy_project_key)
        {
            return Err(ProjectStoreError::Validation(format!(
                "legacy memory key is not declared by project {}",
                project_id
            )));
        }

        let source_root = self.legacy_memory_source_root(legacy_project_key)?;
        if validate_existing_confined_directory(self.paths().data_dir(), &source_root).is_err() {
            return Err(ProjectStoreError::Validation(format!(
                "legacy memory source is missing or not a plain directory: {}",
                source_root.display()
            )));
        }
        let target_root = self.paths().memory_v1_dir(project_id);
        ensure_confined_directory(&project_home, &target_root)?;
        let source_files = enumerate_source_files(&source_root)?;

        let existing = self.load_migration_journal(project_id, legacy_project_key)?;
        if let Some(mut journal) = existing.as_ref().cloned() {
            if journal.report.phase == LegacyMemoryMigrationPhase::Committed {
                self.ensure_resource_revision_bumped(&mut journal)?;
                return Ok(journal.report.clone());
            }
        }

        let mut journal = match existing {
            Some(journal) if source_snapshot_matches(&journal.report.files, &source_files) => {
                journal
            }
            _ => self.plan_migration(project_id, legacy_project_key, &target_root, source_files)?,
        };
        self.persist_migration_journal(&journal)?;
        if fault == MigrationFault::AfterJournal {
            return Err(injected_interruption("after journal"));
        }

        let stage_root = self
            .legacy_memory_state_root(project_id)
            .join("staging")
            .join(&journal.report.transaction_id);
        ensure_confined_directory(&project_home, &stage_root)?;

        let mut staged = 0usize;
        for index in 0..journal.report.files.len() {
            let disposition = journal.report.files[index].disposition;
            if !matches!(
                disposition,
                LegacyMemoryFileDisposition::Pending | LegacyMemoryFileDisposition::Staged
            ) {
                continue;
            }
            let file = journal.report.files[index].clone();
            let relative = validated_relative_path(&file.relative_path)?;
            let source = source_root.join(&relative);
            let target = safe_target_path(&target_root, &relative)?;
            match target_state(&target, &file.sha256, file.size)? {
                TargetState::Identical => {
                    journal.report.files[index].disposition =
                        LegacyMemoryFileDisposition::ExistingIdentical;
                }
                TargetState::Missing | TargetState::Conflict => {
                    // Stage and verify even when a conflicting target exists.
                    // Commit remains no-clobber, but if that target disappears
                    // before commit the verified snapshot can be installed.
                    let staged_path = safe_target_path(&stage_root, &relative)?;
                    if !file_matches(&staged_path, &file.sha256, file.size)? {
                        copy_file_verified_atomic(&source, &staged_path, &file.sha256, file.size)?;
                    }
                    if let Some(diagnostic) =
                        canonical_topic_diagnostic(&file.relative_path, &staged_path)?
                    {
                        journal.report.files[index].disposition =
                            LegacyMemoryFileDisposition::SkippedInvalid;
                        journal.report.files[index].diagnostic = Some(diagnostic);
                        let _ = std::fs::remove_file(&staged_path);
                        touch_journal(&mut journal);
                        self.persist_migration_journal(&journal)?;
                        continue;
                    }
                    journal.report.files[index].disposition = LegacyMemoryFileDisposition::Staged;
                    staged += 1;
                }
            }
            touch_journal(&mut journal);
            self.persist_migration_journal(&journal)?;
            if fault == MigrationFault::AfterFirstStaged && staged == 1 {
                return Err(injected_interruption("after first staged file"));
            }
        }

        for index in 0..journal.report.files.len() {
            if journal.report.files[index].disposition == LegacyMemoryFileDisposition::Staged {
                let relative_path = journal.report.files[index].relative_path.clone();
                let relative = validated_relative_path(&relative_path)?;
                let staged_path = safe_target_path(&stage_root, &relative)?;
                if !file_matches(
                    &staged_path,
                    &journal.report.files[index].sha256,
                    journal.report.files[index].size,
                )? {
                    return Err(ProjectStoreError::Validation(format!(
                        "staged legacy memory file failed verification: {}",
                        relative_path
                    )));
                }
                if let Some(diagnostic) = canonical_topic_diagnostic(&relative_path, &staged_path)?
                {
                    journal.report.files[index].disposition =
                        LegacyMemoryFileDisposition::SkippedInvalid;
                    journal.report.files[index].diagnostic = Some(diagnostic);
                    let _ = std::fs::remove_file(&staged_path);
                }
            }
        }
        journal.report.phase = LegacyMemoryMigrationPhase::Verified;
        touch_journal(&mut journal);
        self.persist_migration_journal(&journal)?;

        let mut committed = 0usize;
        for index in 0..journal.report.files.len() {
            let file = journal.report.files[index].clone();
            let relative = validated_relative_path(&file.relative_path)?;
            let target = safe_target_path(&target_root, &relative)?;
            let disposition = match file.disposition {
                LegacyMemoryFileDisposition::Staged => {
                    let staged_path = safe_target_path(&stage_root, &relative)?;
                    install_verified_no_clobber(&staged_path, &target, &file.sha256, file.size)?
                }
                LegacyMemoryFileDisposition::Copied => {
                    match target_state(&target, &file.sha256, file.size)? {
                        TargetState::Identical => LegacyMemoryFileDisposition::Copied,
                        TargetState::Conflict => LegacyMemoryFileDisposition::TargetConflict,
                        TargetState::Missing => {
                            let staged_path = safe_target_path(&stage_root, &relative)?;
                            install_verified_no_clobber(
                                &staged_path,
                                &target,
                                &file.sha256,
                                file.size,
                            )?
                        }
                    }
                }
                LegacyMemoryFileDisposition::ExistingIdentical => {
                    match target_state(&target, &file.sha256, file.size)? {
                        TargetState::Identical => LegacyMemoryFileDisposition::ExistingIdentical,
                        TargetState::Missing | TargetState::Conflict => {
                            LegacyMemoryFileDisposition::TargetConflict
                        }
                    }
                }
                other => other,
            };
            journal.report.files[index].disposition = disposition;
            if disposition == LegacyMemoryFileDisposition::Copied {
                committed += 1;
            }
            touch_journal(&mut journal);
            self.persist_migration_journal(&journal)?;
            if fault == MigrationFault::AfterFirstCommitted && committed == 1 {
                return Err(injected_interruption("after first committed file"));
            }
        }

        journal.report.phase = LegacyMemoryMigrationPhase::Committed;
        let now = Utc::now();
        journal.report.updated_at = now;
        journal.report.committed_at = Some(now);
        self.persist_migration_journal(&journal)?;
        self.ensure_resource_revision_bumped(&mut journal)?;
        Ok(journal.report)
    }

    fn plan_migration(
        &self,
        project_id: &ProjectId,
        legacy_project_key: &str,
        target_root: &Path,
        source_files: Vec<SourceFile>,
    ) -> ProjectStoreResult<LegacyMemoryMigrationJournal> {
        let files = source_files
            .into_iter()
            .map(|source| {
                let relative = validated_relative_path(&source.relative_path)?;
                let target = safe_target_path(target_root, &relative)?;
                let disposition = match source.diagnostic.as_ref() {
                    Some(_) => LegacyMemoryFileDisposition::SkippedInvalid,
                    None => match target_state(&target, &source.sha256, source.size)? {
                        TargetState::Missing => LegacyMemoryFileDisposition::Pending,
                        TargetState::Identical => LegacyMemoryFileDisposition::ExistingIdentical,
                        TargetState::Conflict => LegacyMemoryFileDisposition::Pending,
                    },
                };
                Ok(LegacyMemoryMigrationFile {
                    relative_path: source.relative_path,
                    size: source.size,
                    sha256: source.sha256,
                    disposition,
                    diagnostic: source.diagnostic,
                })
            })
            .collect::<ProjectStoreResult<Vec<_>>>()?;
        let now = Utc::now();
        Ok(LegacyMemoryMigrationJournal {
            schema_version: JOURNAL_SCHEMA_VERSION,
            resource_revision_bumped: false,
            report: LegacyMemoryMigrationReport {
                project_id: project_id.clone(),
                legacy_project_key: legacy_project_key.to_string(),
                transaction_id: Uuid::new_v4().to_string(),
                phase: LegacyMemoryMigrationPhase::Copying,
                files,
                started_at: now,
                updated_at: now,
                committed_at: None,
            },
        })
    }

    fn legacy_memory_state_root(&self, project_id: &ProjectId) -> PathBuf {
        self.paths()
            .state_dir(project_id)
            .join(LEGACY_MEMORY_STATE_DIR)
    }

    fn migration_journal_path(&self, project_id: &ProjectId, legacy_project_key: &str) -> PathBuf {
        let digest = hex::encode(Sha256::digest(legacy_project_key.as_bytes()));
        self.legacy_memory_state_root(project_id)
            .join("journals")
            .join(format!("{digest}.json"))
    }

    fn load_migration_journal(
        &self,
        project_id: &ProjectId,
        legacy_project_key: &str,
    ) -> ProjectStoreResult<Option<LegacyMemoryMigrationJournal>> {
        let path = self.migration_journal_path(project_id, legacy_project_key);
        let journal_dir = path.parent().ok_or_else(|| {
            ProjectStoreError::Validation(
                "legacy memory migration journal has no parent".to_string(),
            )
        })?;
        if std::fs::symlink_metadata(journal_dir)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return Ok(None);
        }
        validate_existing_confined_directory(&self.paths().project_home(project_id), journal_dir)?;
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let journal = match serde_json::from_slice::<LegacyMemoryMigrationJournal>(&bytes) {
            Ok(journal) => journal,
            Err(error) => {
                let quarantine = path.with_file_name(format!(
                    "{}.corrupt.{}",
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("journal.json"),
                    Uuid::new_v4()
                ));
                write_bytes_atomic(&quarantine, &bytes)?;
                tracing::warn!(
                    %error,
                    quarantine = %quarantine.display(),
                    "legacy memory migration journal was corrupt; replanning"
                );
                return Ok(None);
            }
        };
        validate_journal(&journal, project_id, legacy_project_key)?;
        Ok(Some(journal))
    }

    fn persist_migration_journal(
        &self,
        journal: &LegacyMemoryMigrationJournal,
    ) -> ProjectStoreResult<()> {
        validate_journal(
            journal,
            &journal.report.project_id,
            &journal.report.legacy_project_key,
        )?;
        let path = self.migration_journal_path(
            &journal.report.project_id,
            &journal.report.legacy_project_key,
        );
        let journal_dir = path.parent().ok_or_else(|| {
            ProjectStoreError::Validation(
                "legacy memory migration journal has no parent".to_string(),
            )
        })?;
        ensure_confined_directory(
            &self.paths().project_home(&journal.report.project_id),
            journal_dir,
        )?;
        write_json_atomic(&path, journal)
    }

    fn ensure_resource_revision_bumped(
        &self,
        journal: &mut LegacyMemoryMigrationJournal,
    ) -> ProjectStoreResult<()> {
        if journal.resource_revision_bumped {
            return Ok(());
        }
        loop {
            let project = self.get(&journal.report.project_id)?;
            match self.bump_resource_revision(&project.id, project.revision) {
                Ok(_) => break,
                Err(ProjectStoreError::Conflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        journal.resource_revision_bumped = true;
        touch_journal(journal);
        self.persist_migration_journal(journal)
    }

    #[cfg(test)]
    fn migrate_legacy_memory_with_fault(
        &self,
        project_id: &ProjectId,
        legacy_project_key: &str,
        fault: MigrationFault,
    ) -> ProjectStoreResult<LegacyMemoryMigrationReport> {
        self.migrate_legacy_memory_inner(project_id, legacy_project_key, fault)
    }
}

fn validate_journal(
    journal: &LegacyMemoryMigrationJournal,
    project_id: &ProjectId,
    legacy_project_key: &str,
) -> ProjectStoreResult<()> {
    if journal.schema_version != JOURNAL_SCHEMA_VERSION
        || &journal.report.project_id != project_id
        || journal.report.legacy_project_key != legacy_project_key
    {
        return Err(ProjectStoreError::Validation(
            "legacy memory migration journal identity mismatch".to_string(),
        ));
    }
    if Uuid::parse_str(&journal.report.transaction_id).is_err() {
        return Err(ProjectStoreError::Validation(
            "legacy memory migration transaction id is invalid".to_string(),
        ));
    }
    let mut paths = BTreeMap::new();
    for file in &journal.report.files {
        validated_relative_path(&file.relative_path)?;
        if file.sha256.len() != 64
            || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            || paths.insert(&file.relative_path, ()).is_some()
        {
            return Err(ProjectStoreError::Validation(
                "legacy memory migration file manifest is invalid".to_string(),
            ));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceFile {
    relative_path: String,
    size: u64,
    sha256: String,
    diagnostic: Option<String>,
}

fn enumerate_source_files(root: &Path) -> ProjectStoreResult<Vec<SourceFile>> {
    assert_plain_directory(root)?;
    let mut files = Vec::new();
    enumerate_source_directory(root, root, &mut files)?;
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(files)
}

fn enumerate_source_directory(
    root: &Path,
    directory: &Path,
    files: &mut Vec<SourceFile>,
) -> ProjectStoreResult<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let metadata = std::fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            return Err(ProjectStoreError::Validation(format!(
                "legacy memory migration refuses symlink: {}",
                entry.path().display()
            )));
        }
        if metadata.is_dir() {
            enumerate_source_directory(root, &entry.path(), files)?;
        } else if metadata.is_file() {
            let entry_path = entry.path();
            let relative = entry_path.strip_prefix(root).map_err(|_| {
                ProjectStoreError::Validation(
                    "legacy memory entry escaped its source root".to_string(),
                )
            })?;
            let relative_path = relative_path_string(relative)?;
            let (size, sha256) = hash_regular_file(&entry.path())?;
            let diagnostic = canonical_topic_diagnostic(&relative_path, &entry.path())?;
            files.push(SourceFile {
                relative_path,
                size,
                sha256,
                diagnostic,
            });
        } else {
            return Err(ProjectStoreError::Validation(format!(
                "legacy memory migration refuses special file: {}",
                entry.path().display()
            )));
        }
    }
    Ok(())
}

fn source_snapshot_matches(planned: &[LegacyMemoryMigrationFile], source: &[SourceFile]) -> bool {
    if planned.len() != source.len() {
        return false;
    }
    planned.iter().zip(source).all(|(planned, source)| {
        planned.relative_path == source.relative_path
            && planned.size == source.size
            && planned.sha256 == source.sha256
            && (planned.disposition == LegacyMemoryFileDisposition::SkippedInvalid)
                == source.diagnostic.is_some()
    })
}

fn canonical_topic_diagnostic(
    relative_path: &str,
    file_path: &Path,
) -> ProjectStoreResult<Option<String>> {
    if !is_canonical_topic_path(relative_path) {
        return Ok(None);
    }
    let bytes = std::fs::read(file_path)?;
    let content = match std::str::from_utf8(&bytes) {
        Ok(content) => content,
        Err(_) => {
            return Ok(Some(
                "canonical memory topic is not valid UTF-8".to_string(),
            ));
        }
    };
    match parse_canonical_memory_topic(content) {
        Ok(_) => Ok(None),
        Err(error) => {
            tracing::warn!(
                path = %file_path.display(),
                %error,
                "isolating invalid legacy Project memory topic"
            );
            Ok(Some(
                "canonical memory topic frontmatter is invalid".to_string(),
            ))
        }
    }
}

fn parse_canonical_memory_topic(content: &str) -> Result<(), String> {
    let trimmed = content.trim_start_matches('\u{feff}');
    let rest = trimmed
        .strip_prefix("---\n")
        .ok_or_else(|| "missing frontmatter start marker".to_string())?;
    let end_index = rest
        .find("\n---\n")
        .ok_or_else(|| "missing frontmatter end marker".to_string())?;
    serde_yaml::from_str::<CanonicalMemoryTopicFrontmatter>(&rest[..end_index])
        .map(|_| ())
        .map_err(|error| format!("failed to parse memory frontmatter: {error}"))
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct CanonicalMemoryTopicFrontmatter {
    id: String,
    title: String,
    #[serde(rename = "type")]
    memory_type: CanonicalMemoryType,
    scope: CanonicalMemoryScope,
    #[serde(default)]
    project_key: Option<String>,
    #[serde(default)]
    granularity: Option<CanonicalTemporalGranularity>,
    status: CanonicalMemoryStatus,
    #[serde(default)]
    freshness: Option<String>,
    #[serde(default)]
    confidence: Option<String>,
    created_at: String,
    updated_at: String,
    created_by: CanonicalMemoryActor,
    updated_by: CanonicalMemoryActor,
    #[serde(default)]
    sources: Vec<CanonicalMemorySource>,
    #[serde(default)]
    relations: CanonicalMemoryRelations,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    retrieval: CanonicalMemoryRetrieval,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CanonicalMemoryType {
    User,
    Feedback,
    Project,
    Reference,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CanonicalMemoryScope {
    Session,
    Project,
    Global,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CanonicalTemporalGranularity {
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CanonicalMemoryStatus {
    Active,
    Stale,
    Superseded,
    Contradicted,
    Archived,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct CanonicalMemoryActor {
    kind: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    actor: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct CanonicalMemorySource {
    kind: String,
    id: String,
    #[serde(default)]
    message_range: Vec<String>,
}

#[allow(dead_code)]
#[derive(Default, Deserialize)]
struct CanonicalMemoryRelations {
    #[serde(default)]
    supersedes: Vec<String>,
    #[serde(default)]
    contradicted_by: Vec<String>,
    #[serde(default)]
    related: Vec<String>,
}

#[allow(dead_code)]
#[derive(Default, Deserialize)]
struct CanonicalMemoryRetrieval {
    #[serde(default)]
    keywords: Vec<String>,
    #[serde(default)]
    entities: Vec<String>,
    #[serde(default)]
    embedding_ready: bool,
    #[serde(default)]
    last_accessed_at: Option<String>,
}

fn is_canonical_topic_path(relative_path: &str) -> bool {
    let mut components = Path::new(relative_path).components();
    matches!(
        (
            components.next(),
            components.next(),
            components.next(),
        ),
        (
            Some(Component::Normal(directory)),
            Some(Component::Normal(file)),
            None
        ) if directory == "topics" && Path::new(file).extension().is_some_and(|ext| ext == "md")
    )
}

fn relative_path_string(path: &Path) -> ProjectStoreResult<String> {
    let mut components = Vec::new();
    for component in path.components() {
        let Component::Normal(component) = component else {
            return Err(ProjectStoreError::Validation(
                "legacy memory relative path is invalid".to_string(),
            ));
        };
        let component = component.to_str().ok_or_else(|| {
            ProjectStoreError::Validation("legacy memory paths must be valid UTF-8".to_string())
        })?;
        if component.contains('\\') || component.is_empty() {
            return Err(ProjectStoreError::Validation(
                "legacy memory path component is not portable".to_string(),
            ));
        }
        components.push(component);
    }
    if components.is_empty() {
        return Err(ProjectStoreError::Validation(
            "legacy memory relative path is empty".to_string(),
        ));
    }
    Ok(components.join("/"))
}

fn validated_relative_path(path: &str) -> ProjectStoreResult<PathBuf> {
    if path.is_empty() || path.contains('\\') {
        return Err(ProjectStoreError::Validation(
            "legacy memory relative path is invalid".to_string(),
        ));
    }
    let path = Path::new(path);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ProjectStoreError::Validation(
            "legacy memory relative path is invalid".to_string(),
        ));
    }
    Ok(path.to_path_buf())
}

fn safe_target_path(root: &Path, relative: &Path) -> ProjectStoreResult<PathBuf> {
    let target = confined_path(root, relative)?;
    let parent = target.parent().map(Path::to_path_buf).ok_or_else(|| {
        ProjectStoreError::Validation("legacy memory target has no parent".to_string())
    })?;
    let canonical_root = ensure_confined_directory(root, &parent)?;
    let root = std::fs::canonicalize(root)?;
    if !canonical_root.starts_with(&root) {
        return Err(ProjectStoreError::Validation(
            "legacy memory target parent escapes destination root".to_string(),
        ));
    }
    Ok(target)
}

fn confined_path(root: &Path, relative: &Path) -> ProjectStoreResult<PathBuf> {
    let relative = validated_relative_path(relative.to_str().ok_or_else(|| {
        ProjectStoreError::Validation("legacy memory target path must be valid UTF-8".to_string())
    })?)?;
    assert_plain_directory(root)?;
    Ok(std::fs::canonicalize(root)?.join(relative))
}

fn hash_regular_file(path: &Path) -> ProjectStoreResult<(u64, String)> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(ProjectStoreError::Validation(format!(
            "expected a regular file: {}",
            path.display()
        )));
    }
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut size = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| ProjectStoreError::Validation("file size overflow".to_string()))?;
    }
    Ok((size, hex::encode(digest.finalize())))
}

fn file_matches(
    path: &Path,
    expected_sha256: &str,
    expected_size: u64,
) -> ProjectStoreResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            let (size, sha256) = hash_regular_file(path)?;
            Ok(size == expected_size && sha256 == expected_sha256)
        }
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

enum TargetState {
    Missing,
    Identical,
    Conflict,
}

fn target_state(
    path: &Path,
    expected_sha256: &str,
    expected_size: u64,
) -> ProjectStoreResult<TargetState> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
            if file_matches(path, expected_sha256, expected_size)? {
                Ok(TargetState::Identical)
            } else {
                Ok(TargetState::Conflict)
            }
        }
        Ok(_) => Ok(TargetState::Conflict),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(TargetState::Missing),
        Err(error) => Err(error.into()),
    }
}

fn copy_file_verified_atomic(
    source: &Path,
    target: &Path,
    expected_sha256: &str,
    expected_size: u64,
) -> ProjectStoreResult<()> {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    assert_plain_directory(parent)?;
    let temp = parent.join(format!(".legacy-memory.tmp.{}", Uuid::new_v4()));
    let mut cleanup = TempCleanup(Some(temp.clone()));
    let mut input = File::open(source)?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    let mut digest = Sha256::new();
    let mut size = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        output.write_all(&buffer[..read])?;
        digest.update(&buffer[..read]);
        size = size
            .checked_add(read as u64)
            .ok_or_else(|| ProjectStoreError::Validation("file size overflow".to_string()))?;
    }
    output.sync_all()?;
    drop(output);
    let actual = hex::encode(digest.finalize());
    if size != expected_size || actual != expected_sha256 {
        return Err(ProjectStoreError::Validation(format!(
            "legacy memory source changed while copying: {}",
            source.display()
        )));
    }
    sync_directory(parent)?;
    replace_path(&temp, target)?;
    cleanup.0 = None;
    sync_directory(parent)?;
    Ok(())
}

fn install_verified_no_clobber(
    staged: &Path,
    target: &Path,
    expected_sha256: &str,
    expected_size: u64,
) -> ProjectStoreResult<LegacyMemoryFileDisposition> {
    match target_state(target, expected_sha256, expected_size)? {
        TargetState::Identical => return Ok(LegacyMemoryFileDisposition::ExistingIdentical),
        TargetState::Conflict => return Ok(LegacyMemoryFileDisposition::TargetConflict),
        TargetState::Missing => {}
    }
    if !file_matches(staged, expected_sha256, expected_size)? {
        return Err(ProjectStoreError::Validation(format!(
            "staged legacy memory file failed verification: {}",
            staged.display()
        )));
    }
    let parent = target.parent().unwrap_or_else(|| Path::new("."));
    assert_plain_directory(parent)?;
    let temp = parent.join(format!(".legacy-memory.commit.{}", Uuid::new_v4()));
    let mut cleanup = TempCleanup(Some(temp.clone()));
    copy_file_verified_atomic(staged, &temp, expected_sha256, expected_size)?;
    match std::fs::hard_link(&temp, target) {
        Ok(()) => {
            sync_directory(parent)?;
            std::fs::remove_file(&temp)?;
            cleanup.0 = None;
            sync_directory(parent)?;
            if !file_matches(target, expected_sha256, expected_size)? {
                return Err(ProjectStoreError::Validation(format!(
                    "committed legacy memory file failed verification: {}",
                    target.display()
                )));
            }
            Ok(LegacyMemoryFileDisposition::Copied)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            match target_state(target, expected_sha256, expected_size)? {
                TargetState::Identical => Ok(LegacyMemoryFileDisposition::ExistingIdentical),
                TargetState::Missing | TargetState::Conflict => {
                    Ok(LegacyMemoryFileDisposition::TargetConflict)
                }
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn touch_journal(journal: &mut LegacyMemoryMigrationJournal) {
    journal.report.updated_at = Utc::now();
}

fn injected_interruption(point: &str) -> ProjectStoreError {
    ProjectStoreError::Io(std::io::Error::other(format!(
        "injected legacy memory migration interruption {point}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{LegacyMemoryFileDisposition, ProjectManifest};
    use bamboo_memory::memory_store::{MemoryQueryOptions, MemoryScope, MemoryStore};
    use tempfile::TempDir;

    fn valid_topic(id: &str, title: &str, body: &str) -> String {
        format!(
            "---\n\
id: {id}\n\
title: {title}\n\
type: project\n\
scope: project\n\
project_key: zenith-deadbeef\n\
status: active\n\
created_at: 2026-01-01T00:00:00Z\n\
updated_at: 2026-01-01T00:00:00Z\n\
created_by:\n  kind: session\n\
updated_by:\n  kind: memory_write\n\
---\n\
{body}\n"
        )
    }

    fn setup() -> (TempDir, ProjectStore, ProjectManifest, String, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let store = ProjectStore::open(temp.path()).unwrap();
        let created = store.create("Memory", None).unwrap();
        let key = "zenith-deadbeef".to_string();
        let project = store
            .update(&created.id, created.revision, |manifest| {
                manifest.legacy_project_keys.push(key.clone());
                Ok(())
            })
            .unwrap();
        let source = store.legacy_memory_source_root(&key).unwrap();
        std::fs::create_dir_all(source.join("topics")).unwrap();
        std::fs::create_dir_all(source.join("views")).unwrap();
        std::fs::write(
            source.join("topics/one.md"),
            valid_topic(
                "one",
                "Project identity",
                "The Project identity remains stable.",
            ),
        )
        .unwrap();
        std::fs::write(source.join("views/dream.md"), b"dream").unwrap();
        (temp, store, project, key, source)
    }

    #[test]
    fn migration_copies_to_project_home_and_never_deletes_source() {
        let (_temp, store, project, key, source) = setup();
        let report = store.migrate_legacy_memory(&project.id, &key).unwrap();
        assert_eq!(report.phase, LegacyMemoryMigrationPhase::Committed);
        assert!(report
            .files
            .iter()
            .all(|file| file.disposition == LegacyMemoryFileDisposition::Copied));
        let target = store.paths().memory_v1_dir(&project.id);
        assert_eq!(
            std::fs::read_to_string(target.join("topics/one.md")).unwrap(),
            valid_topic(
                "one",
                "Project identity",
                "The Project identity remains stable."
            )
        );
        assert_eq!(
            std::fs::read(target.join("views/dream.md")).unwrap(),
            b"dream"
        );
        assert!(source.join("topics/one.md").exists());
        assert!(source.join("views/dream.md").exists());
        assert!(
            target.starts_with(store.paths().project_home(&project.id)),
            "assigned memory must physically live under Project home"
        );
    }

    #[test]
    fn target_content_wins_and_legacy_alias_remains_read_only() {
        let (_temp, store, project, key, source) = setup();
        let target = store.paths().memory_v1_dir(&project.id);
        std::fs::create_dir_all(target.join("topics")).unwrap();
        std::fs::write(target.join("topics/one.md"), b"new-data").unwrap();

        let report = store.migrate_legacy_memory(&project.id, &key).unwrap();
        let conflict = report
            .files
            .iter()
            .find(|file| file.relative_path == "topics/one.md")
            .unwrap();
        assert_eq!(
            conflict.disposition,
            LegacyMemoryFileDisposition::TargetConflict
        );
        assert_eq!(
            std::fs::read(target.join("topics/one.md")).unwrap(),
            b"new-data"
        );
        assert_eq!(
            std::fs::read_to_string(source.join("topics/one.md")).unwrap(),
            valid_topic(
                "one",
                "Project identity",
                "The Project identity remains stable."
            )
        );

        let roots = store.project_memory_read_roots(&project.id).unwrap();
        assert_eq!(roots.primary, target);
        assert_eq!(roots.legacy_aliases.len(), 1);
        assert!(roots.legacy_aliases[0].read_only);
        let aliases = store.legacy_memory_aliases(&project.id).unwrap();
        assert!(aliases[0].project_home_precedence);
        assert!(aliases[0].migration_committed);
    }

    #[test]
    fn interrupted_staging_resumes_idempotently() {
        let (_temp, store, project, key, source) = setup();
        assert!(store
            .migrate_legacy_memory_with_fault(&project.id, &key, MigrationFault::AfterFirstStaged,)
            .is_err());
        let status = store
            .legacy_memory_migration_status(&project.id, &key)
            .unwrap()
            .unwrap();
        assert_eq!(status.phase, LegacyMemoryMigrationPhase::Copying);

        let committed = store.migrate_legacy_memory(&project.id, &key).unwrap();
        assert_eq!(committed.phase, LegacyMemoryMigrationPhase::Committed);
        let resource_revision = store.get(&project.id).unwrap().resource_revision;
        let again = store.migrate_legacy_memory(&project.id, &key).unwrap();
        assert_eq!(again.transaction_id, committed.transaction_id);
        assert_eq!(again, committed);
        assert_eq!(
            store.get(&project.id).unwrap().resource_revision,
            resource_revision,
            "idempotent replay must not bump resource revision twice"
        );
        assert!(source.join("topics/one.md").exists());
    }

    #[test]
    fn interrupted_commit_recovers_without_overwrite_or_duplicate() {
        let (_temp, store, project, key, _source) = setup();
        assert!(store
            .migrate_legacy_memory_with_fault(
                &project.id,
                &key,
                MigrationFault::AfterFirstCommitted,
            )
            .is_err());
        let committed = store.migrate_legacy_memory(&project.id, &key).unwrap();
        assert_eq!(committed.phase, LegacyMemoryMigrationPhase::Committed);
        assert_eq!(
            committed
                .files
                .iter()
                .filter(|file| matches!(
                    file.disposition,
                    LegacyMemoryFileDisposition::Copied
                        | LegacyMemoryFileDisposition::ExistingIdentical
                ))
                .count(),
            2
        );
    }

    #[test]
    fn changed_source_replans_uncommitted_transaction() {
        let (_temp, store, project, key, source) = setup();
        assert!(store
            .migrate_legacy_memory_with_fault(&project.id, &key, MigrationFault::AfterJournal,)
            .is_err());
        let old_transaction = store
            .legacy_memory_migration_status(&project.id, &key)
            .unwrap()
            .unwrap()
            .transaction_id;
        let updated = valid_topic(
            "one",
            "Project identity updated",
            "The Project identity remains stable after migration.",
        );
        std::fs::write(source.join("topics/one.md"), &updated).unwrap();
        let committed = store.migrate_legacy_memory(&project.id, &key).unwrap();
        assert_ne!(committed.transaction_id, old_transaction);
        assert_eq!(
            std::fs::read_to_string(
                store
                    .paths()
                    .memory_v1_dir(&project.id)
                    .join("topics/one.md")
            )
            .unwrap(),
            updated
        );
    }

    #[tokio::test]
    async fn invalid_topic_is_isolated_while_valid_topic_remains_recallable() {
        let (temp, store, project, key, source) = setup();
        std::fs::write(
            source.join("topics/corrupt.md"),
            "not a canonical memory document",
        )
        .unwrap();

        let report = store.migrate_legacy_memory(&project.id, &key).unwrap();
        let corrupt = report
            .files
            .iter()
            .find(|file| file.relative_path == "topics/corrupt.md")
            .unwrap();
        assert_eq!(
            corrupt.disposition,
            LegacyMemoryFileDisposition::SkippedInvalid
        );
        assert_eq!(
            corrupt.diagnostic.as_deref(),
            Some("canonical memory topic frontmatter is invalid")
        );
        assert!(source.join("topics/corrupt.md").exists());

        let primary = store.paths().memory_v1_dir(&project.id);
        assert!(!primary.join("topics/corrupt.md").exists());
        assert!(primary.join("topics/one.md").exists());

        let memory = MemoryStore::new(temp.path()).for_project(&project.id);
        let recalled = memory
            .query_scope(
                MemoryScope::Project,
                Some(project.id.as_str()),
                Some("project identity stable"),
                None,
                None,
                None,
                &MemoryQueryOptions {
                    limit: Some(5),
                    max_chars: Some(3_000),
                    cursor: None,
                    include_related: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(recalled.matched_count, 1);
        assert_eq!(recalled.items[0].id, "one");
    }

    #[test]
    fn missing_project_queries_do_not_create_orphan_project_homes() {
        let temp = tempfile::tempdir().unwrap();
        let store = ProjectStore::open(temp.path()).unwrap();
        let missing: ProjectId = "01JMISSINGPROJECT00000000000".parse().unwrap();
        assert!(store.migrate_legacy_memory(&missing, "legacy-key").is_err());
        assert!(store
            .legacy_memory_migration_status(&missing, "legacy-key")
            .is_err());
        assert!(!store.paths().project_home(&missing).exists());
    }

    #[cfg(unix)]
    #[test]
    fn source_symlinks_are_rejected_without_following() {
        use std::os::unix::fs::symlink;

        let (temp, store, project, key, source) = setup();
        let outside = temp.path().join("outside-secret");
        std::fs::write(&outside, b"secret").unwrap();
        symlink(&outside, source.join("topics/link.md")).unwrap();
        assert!(store.migrate_legacy_memory(&project.id, &key).is_err());
        assert!(!store
            .paths()
            .memory_v1_dir(&project.id)
            .join("topics/link.md")
            .exists());
    }

    #[cfg(unix)]
    #[test]
    fn source_ancestor_symlink_is_rejected_without_read_or_target_write() {
        use std::os::unix::fs::symlink;

        let (temp, store, project, key, _source) = setup();
        let memory_root = temp.path().join("memory");
        let outside_memory = temp.path().join("outside-memory");
        std::fs::rename(&memory_root, &outside_memory).unwrap();
        symlink(&outside_memory, &memory_root).unwrap();
        let legacy_file = outside_memory
            .join("v1/scopes/projects")
            .join(&key)
            .join("topics/one.md");
        let before = std::fs::read(&legacy_file).unwrap();

        assert!(store.migrate_legacy_memory(&project.id, &key).is_err());
        assert_eq!(std::fs::read(&legacy_file).unwrap(), before);
        assert!(!store.paths().memory_v1_dir(&project.id).exists());
    }

    #[cfg(unix)]
    #[test]
    fn target_memory_symlink_has_zero_external_side_effects() {
        use std::os::unix::fs::symlink;

        let (temp, store, project, key, _source) = setup();
        let outside = temp.path().join("outside-target");
        std::fs::create_dir(&outside).unwrap();
        let project_home = store.paths().project_home(&project.id);
        symlink(&outside, project_home.join("memory")).unwrap();

        assert!(store.migrate_legacy_memory(&project.id, &key).is_err());
        assert_eq!(
            std::fs::read_dir(&outside).unwrap().count(),
            0,
            "migration must not create v1 or files through a target symlink"
        );
    }

    #[cfg(unix)]
    #[test]
    fn staging_symlink_has_zero_external_side_effects() {
        use std::os::unix::fs::symlink;

        let (temp, store, project, key, _source) = setup();
        let outside = temp.path().join("outside-stage");
        std::fs::create_dir(&outside).unwrap();
        let state = store.paths().state_dir(&project.id);
        std::fs::create_dir_all(&state).unwrap();
        symlink(&outside, state.join(LEGACY_MEMORY_STATE_DIR)).unwrap();

        assert!(store.migrate_legacy_memory(&project.id, &key).is_err());
        assert_eq!(
            std::fs::read_dir(&outside).unwrap().count(),
            0,
            "migration must not create lock, journal, or staging files through a symlink"
        );
    }
}
