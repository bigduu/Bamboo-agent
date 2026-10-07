//! Final write guard for a Child's stable Project and creation identity.
//!
//! Child Sessions inherit their Project at creation. Once published, a full
//! or runtime-only save may update ordinary runtime fields but cannot rebind
//! that Child to another Project. The canonical runtime sidecar, not an index
//! snapshot, is the small authority checked under the per-Session write lock.

use super::*;
use bamboo_domain::SessionAuthorityConflict;

fn conflict(message: impl Into<String>) -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        SessionAuthorityConflict(format!(
            "Child Project or identity changed or unavailable: {}",
            message.into()
        )),
    )
}

async fn regular_file(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path).await {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => Err(conflict("canonical Child file is not a regular file")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(conflict(error.to_string())),
    }
}

async fn empty_creation_layout(directory: &Path) -> io::Result<bool> {
    let mut entries = fs::read_dir(directory).await?;
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_name().to_str() != Some("attachments")
            || !entry.file_type().await?.is_dir()
            || fs::read_dir(entry.path())
                .await?
                .next_entry()
                .await?
                .is_some()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

impl SessionStoreV2 {
    /// The global index may be stale in another Store. A new Child path must
    /// therefore rule out an earlier physical Session with the same public id
    /// before publishing its runtime file and redirecting that index.
    async fn reject_existing_child_id_elsewhere(&self, incoming: &Session) -> io::Result<()> {
        match fs::symlink_metadata(self.sessions_dir.join(&incoming.id)).await {
            Ok(_) => return Err(conflict("Child id is already used by a Root path")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(conflict(error.to_string())),
        }
        let mut roots = match fs::read_dir(&self.sessions_dir).await {
            Ok(roots) => roots,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(conflict(error.to_string())),
        };
        while let Some(root) = roots
            .next_entry()
            .await
            .map_err(|error| conflict(error.to_string()))?
        {
            if !root
                .file_type()
                .await
                .map_err(|error| conflict(error.to_string()))?
                .is_dir()
                || root.file_name().to_str() == Some(incoming.root_session_id.as_str())
            {
                continue;
            }
            let candidate = root.path().join("children").join(&incoming.id);
            match fs::symlink_metadata(candidate).await {
                Ok(_) => return Err(conflict("Child id is already used by another Root path")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(conflict(error.to_string())),
            }
        }
        Ok(())
    }

    /// The caller holds the ordinary cross-process per-Session write lock.
    /// `runtime_rel` is supplied on the runtime path because its cached index
    /// hint must not silently redirect a write to a different physical Child.
    pub(super) async fn validate_child_project_for_write(
        &self,
        incoming: &Session,
        full: bool,
        runtime_rel: Option<&str>,
    ) -> io::Result<()> {
        if incoming.kind != SessionKind::Child {
            return Ok(());
        }
        validate_session_id(&incoming.id)?;
        validate_session_id(&incoming.root_session_id)?;
        let parent_id = incoming
            .parent_session_id
            .as_deref()
            .ok_or_else(|| conflict("missing parent identity"))?;
        validate_session_id(parent_id)?;
        let rel = Self::child_rel_path(&incoming.root_session_id, &incoming.id);
        if runtime_rel.is_some_and(|actual| actual != rel) {
            return Err(conflict("runtime index points to a different Child path"));
        }
        let directory = self.abs_path_from_rel(&rel);
        match fs::symlink_metadata(&directory).await {
            Err(error) if error.kind() == io::ErrorKind::NotFound && full => {
                return self.reject_existing_child_id_elsewhere(incoming).await;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(conflict("canonical Child directory is missing"));
            }
            Err(error) => return Err(conflict(error.to_string())),
            Ok(metadata) if !metadata.file_type().is_dir() => {
                return Err(conflict(
                    "canonical Child directory is not a real directory",
                ));
            }
            Ok(_) => {}
        }
        let main = regular_file(&directory.join("session.json")).await?;
        let runtime = directory.join(RUNTIME_SIDECAR_FILE);
        if !regular_file(&runtime).await? {
            if full && !main && empty_creation_layout(&directory).await? {
                return self.reject_existing_child_id_elsewhere(incoming).await;
            }
            return Err(conflict("canonical Child runtime sidecar is missing"));
        }
        let bytes = fs::read(&runtime)
            .await
            .map_err(|error| conflict(error.to_string()))?;
        let current: Session = serde_json::from_slice(&bytes)
            .map_err(|error| conflict(format!("invalid canonical Child runtime: {error}")))?;
        if current.id != incoming.id
            || current.kind != SessionKind::Child
            || current.root_session_id != incoming.root_session_id
            || current.parent_session_id != incoming.parent_session_id
            || current.spawn_depth != incoming.spawn_depth
            || current.created_at != incoming.created_at
        {
            return Err(conflict(
                "writer does not match durable Child creation identity",
            ));
        }
        if current.project_id_meta() != incoming.project_id_meta() {
            return Err(conflict("Child Project is immutable after creation"));
        }
        if !main && !full {
            return Err(conflict("canonical Child main file is missing"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bamboo_domain::{ActorActivationClaim, ActorDirectoryPort, Session, Storage};
    use chrono::{Duration, Utc};

    use super::*;

    fn assigned_child() -> (Session, Session) {
        let mut root = Session::new("project-root", "model");
        root.set_project_id_meta("project-a");
        let mut child = Session::new_child_of("project-child", &root, "model", "Child");
        child.set_project_id_meta("project-a");
        (root, child)
    }

    #[tokio::test]
    async fn child_project_rebind_is_rejected_by_full_and_runtime_saves_across_stores(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let writer = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let (root, child) = assigned_child();
        writer.save_session(&root).await?;
        writer.save_session(&child).await?;
        let stale = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let now = Utc::now();
        let activation = writer
            .claim_activation(&ActorActivationClaim {
                actor_id: child.id.clone(),
                run_id: "run".into(),
                lease_owner: "owner".into(),
                lease_expires_at: now + Duration::minutes(5),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await?;

        let mut attempted = child.clone();
        attempted.set_project_id_meta("project-b");
        attempted.metadata_version += 1;
        let runtime_error = stale.save_runtime_state(&attempted).await.unwrap_err();
        assert_eq!(runtime_error.kind(), io::ErrorKind::WouldBlock);
        assert!(runtime_error
            .to_string()
            .contains("Child Project is immutable"));
        let full_error = writer.save_session(&attempted).await.unwrap_err();
        assert_eq!(full_error.kind(), io::ErrorKind::WouldBlock);
        assert!(full_error
            .to_string()
            .contains("Child Project is immutable"));

        let reloaded = SessionStoreV2::new(home.path().to_path_buf()).await?;
        assert_eq!(
            reloaded
                .load_session(&child.id)
                .await?
                .unwrap()
                .project_id_meta()
                .as_deref(),
            Some("project-a")
        );
        reloaded.validate_fence(&activation.fence(), now).await?;
        Ok(())
    }

    #[tokio::test]
    async fn same_child_id_cannot_move_to_another_root_and_project_across_stores(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let first = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let second = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let (root_a, child_a) = assigned_child();
        let mut root_b = Session::new("other-root", "model");
        root_b.set_project_id_meta("project-b");
        first.save_session(&root_a).await?;
        first.save_session(&child_a).await?;
        first.save_session(&root_b).await?;
        let now = Utc::now();
        let activation = first
            .claim_activation(&ActorActivationClaim {
                actor_id: child_a.id.clone(),
                run_id: "run".into(),
                lease_owner: "owner".into(),
                lease_expires_at: now + Duration::minutes(5),
                inbox_generation: 0,
                placement_ref: None,
                now,
            })
            .await?;

        let mut moved = Session::new_child_of(&child_a.id, &root_b, "model", "Moved child");
        moved.set_project_id_meta("project-b");
        let error = second.save_session(&moved).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(error.to_string().contains("another Root path"));
        assert!(!home
            .path()
            .join("sessions/other-root/children/project-child")
            .exists());
        let restarted = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let loaded = restarted.load_session(&child_a.id).await?.unwrap();
        assert_eq!(loaded.root_session_id, root_a.id);
        assert_eq!(loaded.project_id_meta().as_deref(), Some("project-a"));
        restarted.validate_fence(&activation.fence(), now).await?;
        Ok(())
    }

    #[tokio::test]
    async fn unchanged_child_project_accepts_normal_metadata_updates(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let (root, mut child) = assigned_child();
        store.save_session(&root).await?;
        store.save_session(&child).await?;
        child.title = "Updated title".into();
        child.metadata_version += 1;
        store.save_runtime_state(&child).await?;
        child.pinned = true;
        child.metadata_version += 1;
        store.save_session(&child).await?;
        let reloaded = store.load_session(&child.id).await?.unwrap();
        assert_eq!(reloaded.title, "Updated title");
        assert!(reloaded.pinned);
        assert_eq!(reloaded.project_id_meta(), child.project_id_meta());
        Ok(())
    }

    #[tokio::test]
    async fn missing_child_runtime_is_not_used_to_reset_project_authority(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let home = tempfile::tempdir()?;
        let store = SessionStoreV2::new(home.path().to_path_buf()).await?;
        let (root, child) = assigned_child();
        store.save_session(&root).await?;
        store.save_session(&child).await?;
        let path = home
            .path()
            .join("sessions/project-root/children/project-child/runtime.json");
        fs::remove_file(&path).await?;
        let mut attempted = child.clone();
        attempted.set_project_id_meta("project-b");
        assert_eq!(
            store.save_session(&attempted).await.unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(!path.exists());
        Ok(())
    }
}
