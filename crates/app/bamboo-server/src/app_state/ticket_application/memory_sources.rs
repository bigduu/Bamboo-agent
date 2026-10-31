//! Exact adopted Project source through the configured native Jiandu root.
use super::{Error, Principal, Result, TicketApplication};
use bamboo_memory::memory_store::{MemoryStore, ProjectMemoryRead};
use bamboo_tickets::{Health, InspectOptions, TicketKind};
use std::collections::BTreeSet;

impl crate::app_state::AppState {
    /// Host Rust seam, with no model/HTTP retrieval grant or source adoption.
    pub async fn read_work_memory_source(
        &self,
        work_id: &str,
        memory_id: &str,
    ) -> Result<ProjectMemoryRead> {
        self.tickets
            .read_adopted_memory_source(&self.memory_store, work_id, memory_id)
            .await
    }
}

impl TicketApplication {
    async fn read_adopted_memory_source(
        &self,
        memory: &MemoryStore,
        work_id: &str,
        memory_id: &str,
    ) -> Result<ProjectMemoryRead> {
        if memory_id.is_empty() || memory_id.len() > 256 {
            return Err(Error::ScopeDenied("invalid memory source selector".into()));
        }
        let (service, authority) = self.authority(Principal::Runtime).await?;
        if service.health() != Health::Writable {
            return Err(Error::AuthorityUnavailable(
                "current Work source authority unavailable".into(),
            ));
        }
        let (commit, snapshot) = service.published()?;
        service.work_inspect_sections(
            &authority,
            &[work_id.into()],
            &InspectOptions {
                sections: BTreeSet::new(),
                depth: 0,
                budget_bytes: 65_536,
                fixed_commit: Some(commit.clone()),
            },
        )?;
        let work = snapshot
            .tickets
            .get(work_id)
            .filter(|work| work.kind == TicketKind::Work)
            .ok_or_else(|| Error::ScopeDenied("Work source scope denied".into()))?;
        let project = snapshot
            .project
            .links
            .get(work_id)
            .ok_or_else(|| Error::ScopeDenied("Work has no authorized Project".into()))?;
        let context = snapshot
            .project
            .work_contexts
            .get(work_id)
            .and_then(|hash| snapshot.project.contexts.get(hash))
            .filter(|context| {
                &context.project_id == project
                    && context.contract_revision == work.contract_revision
            })
            .ok_or_else(|| {
                Error::ScopeDenied("Work needs a current adopted Project context".into())
            })?;
        self.validate_project_context(project, Some(context), false)?;
        let key = format!("jiandu:project:{project}:memory:{memory_id}");
        let version = context.sources.get(&key).ok_or_else(|| {
            Error::ResourceBlocked("Jiandu source is not explicitly adopted by this Work".into())
        })?;
        let read = memory
            .read_project_source_read_only(project, memory_id, version)
            .await
            .map_err(|error| {
                Error::AuthorityUnavailable(format!("Jiandu source read unavailable: {error}"))
            })?;
        let (current, _) = self.authority(Principal::Runtime).await?;
        if current.health() != Health::Writable || current.published()?.0 != commit {
            return Err(Error::RevisionConflict);
        }
        self.validate_project_context(project, Some(context), false)?;
        Ok(read)
    }
}
