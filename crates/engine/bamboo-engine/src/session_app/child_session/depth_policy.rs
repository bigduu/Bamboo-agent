//! Shared creation policy. Callers supply a parent observation, never a depth.

use bamboo_domain::{ActorSession, Session};

use super::{ChildSessionError, ChildSessionPort};

/// Reload the durable parent and validate its chain before child persistence,
/// resident mutation or scheduling. The returned parent owns child derivation.
pub async fn validate_spawn_parent(
    port: &dyn ChildSessionPort,
    observed_parent: &Session,
) -> Result<Session, ChildSessionError> {
    let invalid = || ChildSessionError::InvalidArguments("invalid child spawn lineage".into());
    let parent = port.load_parent_session(&observed_parent.id).await?;
    let parent_actor = ActorSession::from_session(&parent).map_err(|_| invalid())?;
    let observed_actor = ActorSession::from_session(observed_parent).map_err(|_| invalid())?;
    if !parent_actor.matches_session(observed_parent)
        || parent_actor.project_id != observed_actor.project_id
    {
        return Err(invalid());
    }

    let max_depth = port.max_spawn_depth().await;
    if parent.spawn_depth >= max_depth {
        return Err(ChildSessionError::InvalidArguments(format!(
            "spawn depth limit ({max_depth}) reached: this agent is at depth {} and cannot create more sub-agents",
            parent.spawn_depth,
        )));
    }

    let mut current = parent.clone();
    let mut actor = parent_actor.clone();
    while let Some(parent_id) = actor.parent_actor_id.as_deref() {
        let ancestor = port.load_parent_session(parent_id).await?;
        let ancestor_actor = ActorSession::from_session(&ancestor).map_err(|_| invalid())?;
        if ancestor.id != parent_id
            || ancestor_actor.root_actor_id != parent_actor.root_actor_id
            || ancestor_actor.project_id != parent_actor.project_id
            || ancestor.spawn_depth.checked_add(1) != Some(current.spawn_depth)
            || ancestor.created_at > current.created_at
            || current
                .parent_created_at
                .is_some_and(|birth| birth != ancestor.created_at)
        {
            return Err(invalid());
        }
        current = ancestor;
        actor = ancestor_actor;
    }
    if current.id != parent_actor.root_actor_id || current.spawn_depth != 0 {
        return Err(invalid());
    }
    Ok(parent)
}
