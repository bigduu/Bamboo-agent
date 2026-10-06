//! A title commit advances existing observations, never Actor authority.
//! All reads/preflight and publication use the caller's existing Root tree
//! writer scope. No descendant Session lock, ensure_actor, marker or repair.

use super::*;
use bamboo_domain::{ActorDirectoryEntry, ActorSession, SessionAuthorityConflict};

fn conflict(message: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        SessionAuthorityConflict(message.into()),
    )
}

fn normalized(session: &Session) -> Session {
    let mut session = session.clone();
    if session.kind == SessionKind::Root && session.root_session_id.is_empty() {
        session.root_session_id = session.id.clone();
    }
    session
}

fn value(session: &Session) -> io::Result<serde_json::Value> {
    serde_json::to_value(normalized(session)).map_err(|e| other_io_error(e.to_string()))
}

/// A complete canonical comparison, not a metadata-version permission grant.
fn title_only(previous: &Session, incoming: &Session) -> io::Result<bool> {
    if (previous.title_version != 0 || incoming.title_version != 0)
        && (incoming.title_version < previous.title_version
            || incoming.metadata_version < previous.metadata_version)
    {
        return Err(conflict("authoritative title metadata cannot regress"));
    }
    if previous.title == incoming.title
        && previous.title_generated == incoming.title_generated
        && previous.title_version == incoming.title_version
    {
        return Ok(false);
    }
    // Only an authoritative title-version advance enters this repair. Legacy
    // direct saves keep their old behavior and cannot refresh observations.
    if previous.title_version == incoming.title_version {
        if previous.title_version != 0 {
            return Err(conflict(
                "changed authoritative title must advance its version",
            ));
        }
        return Ok(false);
    }
    let mut comparison = incoming.clone();
    comparison.title = previous.title.clone();
    comparison.title_generated = previous.title_generated;
    comparison.title_version = previous.title_version;
    comparison.metadata_version = previous.metadata_version;
    comparison.updated_at = previous.updated_at;
    if value(previous)? != value(&comparison)? {
        return Err(conflict(
            "canonical non-title Session fields changed before title commit",
        ));
    }
    if previous.title_version.checked_add(1) != Some(incoming.title_version)
        || previous.metadata_version.checked_add(1) != Some(incoming.metadata_version)
    {
        return Err(conflict(
            "title observation update requires exactly one metadata revision",
        ));
    }
    Ok(true)
}

async fn regular_bytes(path: &Path) -> io::Result<Option<Vec<u8>>> {
    match fs::symlink_metadata(path).await {
        Ok(meta) if meta.file_type().is_file() => fs::read(path).await.map(Some),
        Ok(_) => Err(conflict("title observation source is not a regular file")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

impl SessionStoreV2 {
    async fn title_canonical_session(&self, session: &Session) -> io::Result<Session> {
        let rel = Self::default_writer_rel_path(session)?;
        let root = if session.kind == SessionKind::Root {
            &session.id
        } else {
            &session.root_session_id
        };
        self.load_session_from_dir_strict(
            &self.abs_path_from_rel(&rel),
            &session.id,
            session.kind,
            root,
        )
        .await?
        .ok_or_else(|| conflict("canonical title Session is missing"))
    }

    // Offline Supervisor import does not participate in ordinary Host writer
    // locks. Keep that separate persistence path outside this repair.
    async fn ordinary_title_tree(&self, current: &Session) -> io::Result<bool> {
        if current.kind == SessionKind::Root {
            return Ok(current.authority_identity.is_ordinary());
        }
        let root = &current.root_session_id;
        Ok(self
            .load_session_from_dir_strict(
                &self.sessions_dir.join(root),
                root,
                SessionKind::Root,
                root,
            )
            .await?
            .is_some_and(|root| root.authority_identity.is_ordinary()))
    }

    /// Enumerate canonical placements rather than a rebuildable index. The
    /// shared lifecycle/Task and Root tree guards exclude supported Host
    /// creation/deletion and all ordinary Directory and Session publishers.
    async fn title_observation_rows(
        &self,
        root: &str,
    ) -> io::Result<Vec<(PathBuf, ActorDirectoryEntry)>> {
        validate_session_id(root)?;
        let root_dir = self.sessions_dir.join(root);
        let mut directories = vec![root_dir.clone()];
        let children = root_dir.join("children");
        match fs::symlink_metadata(&children).await {
            Ok(meta) if meta.file_type().is_dir() => {
                let mut entries = fs::read_dir(&children).await?;
                while let Some(entry) = entries.next_entry().await? {
                    if !entry.file_type().await?.is_dir() {
                        return Err(conflict(
                            "title observation Child placement is not a directory",
                        ));
                    }
                    directories.push(entry.path());
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
            Ok(_) => {
                return Err(conflict(
                    "title observation children placement is not a directory",
                ))
            }
        }
        directories.sort();
        let mut rows = Vec::new();
        for directory in directories {
            let path = directory.join("actor-authority.json");
            let record = regular_bytes(&path).await?;
            let marker = regular_bytes(&directory.join("actor-authority.initialized.json")).await?;
            let (record, marker) = match (record, marker) {
                (None, None) => continue,
                (Some(record), Some(marker)) => (record, marker),
                _ => {
                    return Err(conflict(
                        "title observation has an incomplete initialized Actor",
                    ))
                }
            };
            let id = directory
                .file_name()
                .and_then(|id| id.to_str())
                .ok_or_else(|| conflict("invalid title observation Actor path"))?;
            // The Directory resolver checks regular placement and rejects
            // duplicate physical Sessions with the same id across Roots.
            let (rel, canonical_path) = self
                .actor_authority_location(id)
                .await
                .map_err(|e| conflict(&e.to_string()))?;
            if canonical_path != path {
                return Err(conflict(
                    "title observation is not at its canonical placement",
                ));
            }
            let (kind, expected_root) = Self::copy_source_identity_from_rel(id, &rel)?;
            if expected_root != root {
                return Err(conflict("title observation Root mismatch"));
            }
            let current = self
                .load_session_from_dir_strict(&directory, id, kind, root)
                .await?
                .ok_or_else(|| conflict("title observation Session is missing"))?;
            let mut expected =
                ActorSession::from_session(&current).map_err(|e| conflict(&e.to_string()))?;
            expected.ancestor_observations = self
                .validate_actor_lineage(&expected)
                .await
                .map_err(|e| conflict(&e.to_string()))?;
            let project = expected
                .project_id
                .as_deref()
                .map(str::parse::<ProjectId>)
                .transpose()
                .map_err(|_| conflict("invalid title observation Project"))?;
            actor_directory::validate_census_witnesses(
                &record,
                &marker,
                &current,
                project.as_ref(),
            )?;
            let row: ActorDirectoryEntry = serde_json::from_slice(&record)
                .map_err(|_| conflict("invalid title observation Actor row"))?;
            if row.actor.project_id != current.project_id_meta()
                || row.actor.observed_metadata_version != current.metadata_version
                || row.actor.ancestor_observations != expected.ancestor_observations
            {
                return Err(conflict("title observation is stale"));
            }
            rows.push((path, row));
        }
        Ok(rows)
    }

    pub(super) async fn prepare_title_observations(
        &self,
        incoming: &Session,
    ) -> io::Result<Vec<(PathBuf, Vec<u8>)>> {
        let previous = self.title_canonical_session(incoming).await?;
        if !self.ordinary_title_tree(&previous).await? || !title_only(&previous, incoming)? {
            return Ok(Vec::new());
        }
        let mut writes = Vec::new();
        // Validate the complete initialized set before serializing any write.
        for (path, mut row) in self
            .title_observation_rows(&previous.root_session_id)
            .await?
        {
            let mut changed = false;
            if row.actor.actor_id == previous.id {
                row.actor.observed_metadata_version = incoming.metadata_version;
                changed = true;
            }
            for ancestor in &mut row.actor.ancestor_observations {
                if ancestor.actor_id == previous.id {
                    // Full prior birth/lineage/version equality was required above.
                    ancestor.metadata_version = incoming.metadata_version;
                    changed = true;
                }
            }
            if changed {
                row.revision = row
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| conflict("title observation row revision overflow"))?;
                row.validate().map_err(|e| conflict(&e.to_string()))?;
                let bytes =
                    serde_json::to_vec_pretty(&row).map_err(|e| other_io_error(e.to_string()))?;
                writes.push((path, bytes));
            }
        }
        Ok(writes)
    }

    pub(super) async fn validate_unchanged_title(&self, expected: &Session) -> io::Result<()> {
        let lifecycle = self.lock_session_lifecycle_shared().await?;
        let task = self.lock_runtime_task_sidecar_shared().await?;
        let session = self.acquire_session_maintenance_lock(&expected.id).await?;
        let guards = DefaultWriterGuards::shared(lifecycle, task, session);
        let expected = normalized(expected);
        let tree = self
            .acquire_actor_tree_write_guard(&expected.root_session_id)
            .await?;
        guards.hold_tree(tree);
        let current = self.title_canonical_session(&expected).await?;
        if value(&current)? != value(&expected)? {
            return Err(conflict(
                "canonical Session changed before title no-op validation",
            ));
        }
        if self.ordinary_title_tree(&current).await? {
            self.title_observation_rows(&current.root_session_id)
                .await?;
        }
        Ok(())
    }
}
