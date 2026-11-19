use bamboo_agent_core::tools::ToolCtx;
use bamboo_domain::{ProjectId, ProjectStatus, Session, SessionKind};
use bamboo_engine::project_context::ProjectContextResolver;
use bamboo_memory::memory_store::SectionId;

use super::MemoryTool;

impl MemoryTool {
    /// Resolve only the current server-owned assignment of a live Root. A
    /// Child shares it when each persisted parent link proves the same Project,
    /// Root, and parent lifetime.
    pub(super) async fn section_for_context(
        &self,
        ctx: &ToolCtx,
        project_id: &ProjectId,
    ) -> Option<SectionId> {
        let projects = self.project_store.as_ref()?;
        let storage = self.session_repo.storage();
        let session_id = ctx.session_id()?;
        let in_project = |session: &Session| {
            ProjectContextResolver::memory_read_identity_for_session(session).as_ref()
                == Some(project_id)
        };
        let mut current = storage
            .load_runtime_control_plane(session_id)
            .await
            .ok()??;
        if current.id != session_id
            || !in_project(&current)
            || ctx
                .executing_caller_for(session_id)
                .is_some_and(|caller| !caller.matches_session(&current))
        {
            return None;
        }

        let root_id = logical_root(&current).to_owned();
        while current.kind == SessionKind::Child {
            let parent_id = current.parent_session_id.as_deref()?;
            let parent = storage.load_runtime_control_plane(parent_id).await.ok()??;
            if parent.id != parent_id
                || Some(parent.created_at) != current.parent_created_at
                || parent.spawn_depth.checked_add(1) != Some(current.spawn_depth)
                || logical_root(&parent) != root_id
                || !in_project(&parent)
            {
                return None;
            }
            current = parent;
        }
        if current.id != root_id
            || current.kind != SessionKind::Root
            || current.parent_session_id.is_some()
            || current.spawn_depth != 0
        {
            return None;
        }

        let projects = projects.clone();
        let project_id = project_id.clone();
        let project = tokio::task::spawn_blocking(move || projects.get(&project_id))
            .await
            .ok()?
            .ok()?;
        if project.status != ProjectStatus::Active {
            return None;
        }
        let sidebar = project.sidebar.as_ref()?;
        let section_id = sidebar.session_sections.get(&root_id)?.as_deref()?;
        // The legacy sidebar bucket has no stable Section record.
        let section = sidebar
            .sections
            .iter()
            .find(|section| section.id == section_id)?;
        let birth_token = current.root_mode_birth_token();
        let authorized = match project.sidebar_section_bindings.get(&root_id) {
            Some(binding) => {
                binding.section_id == section.id && binding.root_birth_token == birth_token
            }
            None => storage
                .first_root_lifetime_matches(&root_id, &birth_token)
                .await
                .ok()?,
        };
        if !authorized {
            return None;
        }
        SectionId::parse(section.id.as_str()).ok()
    }
}

fn logical_root(session: &Session) -> &str {
    if session.root_session_id.is_empty() && session.kind == SessionKind::Root {
        &session.id
    } else {
        &session.root_session_id
    }
}
