//! Read-only, caller-scoped tree inspection from canonical durable Sessions.
//! The index supplies candidate IDs only; every displayed field comes from a
//! verified Session. The same reader serves the Root tool and Host worker RPC.

use std::collections::{HashMap, HashSet, VecDeque};

use async_trait::async_trait;
use bamboo_agent_core::tools::ToolResult;
use bamboo_domain::{ActorSession, Session, SessionKind, Storage};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{assemble_session_tree, ChildSessionEntry, SessionTreeNode, MAX_CHILD_RESULT_BYTES};

pub const MAX_TREE_CURSOR_BYTES: usize = 128;
const PAGE_NODES: usize = 32;
const MAX_DEPTH: u32 = 4;
const INDEX_NODE_CAP: usize = 5000;
pub const INDEX_SNAPSHOT_CAP: usize = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedTreeError {
    InvalidLineage,
    InvalidCursor,
    ResultTooLarge,
}

#[async_trait]
pub trait OwnedTreePort: Send + Sync {
    async fn load(&self, id: &str) -> Result<Session, OwnedTreeError>;
    async fn child_ids(&self, parent_id: &str) -> Result<Vec<String>, OwnedTreeError>;
    /// Production stores return one bounded candidate index for the whole
    /// inspection. Embeddings without this operation retain per-parent reads.
    async fn child_index(&self) -> Result<Option<Vec<(String, String)>>, OwnedTreeError> {
        Ok(None)
    }
}

/// The Host's rebuildable index is only a source of candidate IDs. `load`
/// always reads the canonical V2 Session before granting visibility.
#[async_trait]
impl OwnedTreePort for bamboo_storage::SessionStoreV2 {
    async fn load(&self, id: &str) -> Result<Session, OwnedTreeError> {
        Storage::load_session(self, id)
            .await
            .map_err(|_| OwnedTreeError::InvalidLineage)?
            .ok_or(OwnedTreeError::InvalidLineage)
    }

    async fn child_ids(&self, parent_id: &str) -> Result<Vec<String>, OwnedTreeError> {
        Ok(self
            .list_index_entries()
            .await
            .into_iter()
            .filter(|entry| {
                entry.kind == SessionKind::Child
                    && entry.parent_session_id.as_deref() == Some(parent_id)
            })
            .map(|entry| entry.id)
            .collect())
    }

    async fn child_index(&self) -> Result<Option<Vec<(String, String)>>, OwnedTreeError> {
        let entries = self.list_index_entries().await;
        if entries.len() > INDEX_SNAPSHOT_CAP {
            return Err(OwnedTreeError::InvalidLineage);
        }
        Ok(Some(
            entries
                .into_iter()
                .filter(|entry| entry.kind == SessionKind::Child)
                .filter_map(|entry| entry.parent_session_id.map(|parent| (parent, entry.id)))
                .collect(),
        ))
    }
}

fn actor(session: &Session) -> Result<ActorSession, OwnedTreeError> {
    // Both Project planes are still read by production paths. A conflicting
    // mirror cannot select whichever one happens to be consulted first.
    if let (Some(typed), Some(legacy)) = (
        session
            .runtime_metadata
            .as_ref()
            .and_then(|metadata| metadata.project_id.as_ref()),
        session.metadata.get("project_id"),
    ) {
        if typed != legacy {
            return Err(OwnedTreeError::InvalidLineage);
        }
    }
    ActorSession::from_session(session).map_err(|_| OwnedTreeError::InvalidLineage)
}

fn edge(
    parent: &Session,
    child: &Session,
    root_id: &str,
    project_id: Option<&str>,
) -> Result<(), OwnedTreeError> {
    let parent_actor = actor(parent)?;
    let child_actor = actor(child)?;
    if child.kind != SessionKind::Child
        || child.id == parent.id
        || child_actor.parent_actor_id.as_deref() != Some(parent.id.as_str())
        || parent_actor.root_actor_id != root_id
        || child_actor.root_actor_id != root_id
        || parent_actor.project_id.as_deref() != project_id
        || child_actor.project_id.as_deref() != project_id
        || child_actor.spawn_depth != parent_actor.spawn_depth.saturating_add(1)
        || child.created_at < parent.created_at
    {
        return Err(OwnedTreeError::InvalidLineage);
    }
    Ok(())
}

async fn lineage(
    port: &dyn OwnedTreePort,
    caller_id: &str,
) -> Result<Vec<Session>, OwnedTreeError> {
    let caller = port.load(caller_id).await?;
    if caller.id != caller_id || caller.spawn_depth > MAX_DEPTH {
        return Err(OwnedTreeError::InvalidLineage);
    }
    let caller_actor = actor(&caller)?;
    let mut lineage = vec![caller];
    let mut seen = HashSet::from([caller_id.to_owned()]);
    while lineage
        .last()
        .is_some_and(|session| session.kind == SessionKind::Child)
    {
        let child = lineage.last().ok_or(OwnedTreeError::InvalidLineage)?;
        let parent_id = child
            .parent_session_id
            .as_deref()
            .ok_or(OwnedTreeError::InvalidLineage)?;
        if !seen.insert(parent_id.to_owned()) || lineage.len() > MAX_DEPTH as usize {
            return Err(OwnedTreeError::InvalidLineage);
        }
        let parent = port.load(parent_id).await?;
        if parent.id != parent_id {
            return Err(OwnedTreeError::InvalidLineage);
        }
        edge(
            &parent,
            child,
            &caller_actor.root_actor_id,
            caller_actor.project_id.as_deref(),
        )?;
        lineage.push(parent);
    }
    let root = lineage.last().ok_or(OwnedTreeError::InvalidLineage)?;
    let root_actor = actor(root)?;
    if root.kind != SessionKind::Root
        || root.id != caller_actor.root_actor_id
        || root_actor.project_id != caller_actor.project_id
        || lineage.len() != caller_actor.spawn_depth as usize + 1
    {
        return Err(OwnedTreeError::InvalidLineage);
    }
    lineage.reverse();
    Ok(lineage)
}

fn proof_session(proof: &mut Sha256, session: &Session) -> Result<(), OwnedTreeError> {
    let bytes = serde_json::to_vec(&json!({
        "actor": actor(session)?,
        "updated_at": session.updated_at,
        "title": session.title,
        "last_run_status": session.last_run_status(),
    }))
    .map_err(|_| OwnedTreeError::InvalidLineage)?;
    proof.update((bytes.len() as u64).to_le_bytes());
    proof.update(bytes);
    Ok(())
}

fn lineage_proof(lineage: &[Session]) -> Result<String, OwnedTreeError> {
    let mut proof = Sha256::new();
    for session in lineage {
        proof_session(&mut proof, session)?;
    }
    Ok(hex::encode(proof.finalize()))
}

async fn verified_tree(
    port: &dyn OwnedTreePort,
    lineage: &[Session],
) -> Result<(SessionTreeNode, String), OwnedTreeError> {
    let caller = lineage.last().ok_or(OwnedTreeError::InvalidLineage)?;
    let caller_actor = actor(caller)?;
    let mut proof = Sha256::new();
    for ancestor in lineage {
        proof_session(&mut proof, ancestor)?;
    }
    let mut seen: HashSet<String> = lineage.iter().map(|session| session.id.clone()).collect();
    let mut adjacency = HashMap::new();
    let indexed = port.child_index().await?.map(|edges| {
        let mut by_parent: HashMap<String, Vec<String>> = HashMap::new();
        for (parent, child) in edges {
            by_parent.entry(parent).or_default().push(child);
        }
        by_parent
    });
    let mut queue = VecDeque::from([(caller.clone(), 0_u32)]);
    while let Some((parent, depth)) = queue.pop_front() {
        if depth >= MAX_DEPTH - caller.spawn_depth {
            continue;
        }
        let mut ids = if let Some(indexed) = &indexed {
            indexed.get(&parent.id).cloned().unwrap_or_default()
        } else {
            port.child_ids(&parent.id).await?
        };
        if ids.len() > INDEX_NODE_CAP {
            return Err(OwnedTreeError::InvalidLineage);
        }
        ids.sort_unstable();
        let mut children = Vec::new();
        for id in ids {
            if !seen.insert(id.clone()) || seen.len() > INDEX_NODE_CAP {
                return Err(OwnedTreeError::InvalidLineage);
            }
            let child = port.load(&id).await?;
            if child.id != id {
                return Err(OwnedTreeError::InvalidLineage);
            }
            edge(
                &parent,
                &child,
                &caller_actor.root_actor_id,
                caller_actor.project_id.as_deref(),
            )?;
            proof_session(&mut proof, &child)?;
            children.push(ChildSessionEntry {
                child_session_id: child.id.clone(),
                title: child.title.clone(),
                pinned: child.pinned,
                message_count: child.messages.len(),
                updated_at: child.updated_at.to_rfc3339(),
                last_run_status: child.last_run_status(),
                last_run_error: child.last_run_error(),
            });
            queue.push_back((child, depth + 1));
        }
        adjacency.insert(parent.id, children);
    }
    let tree = assemble_session_tree(
        &caller.id,
        &caller.title,
        &adjacency,
        MAX_DEPTH - caller.spawn_depth,
    );
    Ok((tree, hex::encode(proof.finalize())))
}

fn observed_status(raw: Option<&str>) -> &'static str {
    match raw {
        Some("created") => "created",
        Some("pending") => "pending",
        Some("queued") => "queued",
        Some("running") => "running",
        Some("running_in_background") => "running_in_background",
        Some("already_running") => "already_running",
        Some("completed") => "completed",
        Some("error") => "error",
        Some("timeout") => "timeout",
        Some("cancelled") => "cancelled",
        Some("message_delivered_live") => "message_delivered_live",
        Some("message_queued") => "message_queued",
        Some("activation_pending") => "activation_pending",
        Some("activation_retry_required") => "activation_retry_required",
        _ => "unknown",
    }
}

fn cursor(offset: usize, digest: &str) -> String {
    format!("tp1:{offset}:{digest}")
}

fn page(
    caller: &Session,
    tree: &SessionTreeNode,
    raw_cursor: Option<&str>,
    scope_digest: &str,
) -> Result<Value, OwnedTreeError> {
    let max_depth = MAX_DEPTH - caller.spawn_depth;
    let mut pending = vec![(tree, None)];
    let mut nodes = Vec::new();
    let mut depth_limited = false;
    while let Some((node, parent)) = pending.pop() {
        let title: String = node.title.chars().take(80).collect();
        nodes.push(json!({"actor_id":node.session_id, "parent_actor_id":parent,
            "title":title, "depth":node.depth,
            "observed_status":observed_status(node.last_run_status.as_deref())}));
        depth_limited |= node.depth >= max_depth;
        let mut children = node.children.iter().collect::<Vec<_>>();
        children.sort_unstable_by(|a, b| a.session_id.cmp(&b.session_id));
        for child in children.into_iter().rev() {
            pending.push((child, Some(node.session_id.as_str())));
        }
    }
    let fingerprint = json!({"caller":caller.id, "birth":caller.created_at,
        "project":caller.project_id_meta(), "metadata_version":caller.metadata_version,
        "canonical_scope":scope_digest, "nodes":nodes});
    let digest = hex::encode(Sha256::digest(
        serde_json::to_vec(&fingerprint).map_err(|_| OwnedTreeError::InvalidLineage)?,
    ));
    let start = match raw_cursor {
        None => 0,
        Some(raw) => {
            if raw.len() > MAX_TREE_CURSOR_BYTES {
                return Err(OwnedTreeError::InvalidCursor);
            }
            let mut fields = raw.split(':');
            let (Some("tp1"), Some(offset), Some(proof), None) =
                (fields.next(), fields.next(), fields.next(), fields.next())
            else {
                return Err(OwnedTreeError::InvalidCursor);
            };
            if offset.starts_with('0') || proof != digest {
                return Err(OwnedTreeError::InvalidCursor);
            }
            let offset = offset
                .parse::<usize>()
                .map_err(|_| OwnedTreeError::InvalidCursor)?;
            if offset >= nodes.len() {
                return Err(OwnedTreeError::InvalidCursor);
            }
            offset
        }
    };
    let mut slice = Vec::new();
    for node in nodes.iter().skip(start).take(PAGE_NODES) {
        slice.push(node.clone());
        let end = start + slice.len();
        let next = (end < nodes.len()).then(|| cursor(end, &digest));
        let candidate = json!({"actor_id":caller.id, "nodes":slice,
            "truncated":depth_limited || next.is_some(), "next_cursor":next,
            "observation":"Durable index tree; run statuses are observations, not activation leases."});
        if serde_json::to_vec(&ToolResult::text(true, candidate.to_string()))
            .map_or(true, |bytes| bytes.len() > MAX_CHILD_RESULT_BYTES)
        {
            slice.pop();
            if slice.is_empty() {
                return Err(OwnedTreeError::ResultTooLarge);
            }
            break;
        }
    }
    let end = start + slice.len();
    let next = (end < nodes.len()).then(|| cursor(end, &digest));
    Ok(json!({"actor_id":caller.id, "nodes":slice,
        "truncated":depth_limited || next.is_some(), "next_cursor":next,
        "observation":"Durable index tree; run statuses are observations, not activation leases."}))
}

pub async fn inspect_owned_tree(
    port: &dyn OwnedTreePort,
    caller_id: &str,
    raw_cursor: Option<&str>,
) -> Result<Value, OwnedTreeError> {
    let chain = lineage(port, caller_id).await?;
    let caller = chain.last().ok_or(OwnedTreeError::InvalidLineage)?;
    let (tree, scope_digest) = verified_tree(port, &chain).await?;
    let page = page(caller, &tree, raw_cursor, &scope_digest)?;
    let current = lineage(port, caller_id).await?;
    if lineage_proof(&current)? != lineage_proof(&chain)? {
        return Err(OwnedTreeError::InvalidLineage);
    }
    Ok(page)
}

/// Resolve one descendant for read-only inspection. Both lineages come from
/// canonical Sessions; matching a candidate index entry or a root id alone is
/// never enough to authorize transcript access.
pub async fn load_owned_descendant(
    port: &dyn OwnedTreePort,
    caller_id: &str,
    target_id: &str,
) -> Result<Session, OwnedTreeError> {
    let caller_chain = lineage(port, caller_id).await?;
    let target_chain = lineage(port, target_id).await?;
    if target_chain.len() <= caller_chain.len() {
        return Err(OwnedTreeError::InvalidLineage);
    }
    let same_authority = |left: &Session, right: &Session| -> Result<bool, OwnedTreeError> {
        let left = actor(left)?;
        let right = actor(right)?;
        Ok(left.actor_id == right.actor_id
            && left.session_created_at == right.session_created_at
            && left.parent_actor_id == right.parent_actor_id
            && left.root_actor_id == right.root_actor_id
            && left.project_id == right.project_id
            && left.spawn_depth == right.spawn_depth)
    };
    for (caller, target_ancestor) in caller_chain.iter().zip(&target_chain) {
        if !same_authority(caller, target_ancestor)? {
            return Err(OwnedTreeError::InvalidLineage);
        }
    }
    // Recheck authority after reading the target. If an ancestor is replaced
    // or reparented during this read, do not return its transcript snapshot.
    let current_chain = lineage(port, target_id).await?;
    if current_chain.len() != target_chain.len() {
        return Err(OwnedTreeError::InvalidLineage);
    }
    for (before, after) in target_chain.iter().zip(&current_chain) {
        if !same_authority(before, after)? {
            return Err(OwnedTreeError::InvalidLineage);
        }
    }
    target_chain
        .into_iter()
        .last()
        .ok_or(OwnedTreeError::InvalidLineage)
}

#[cfg(test)]
mod descendant_tests {
    use super::*;
    use chrono::{Duration, Utc};

    struct Sessions(HashMap<String, Session>);

    #[async_trait]
    impl OwnedTreePort for Sessions {
        async fn load(&self, id: &str) -> Result<Session, OwnedTreeError> {
            self.0
                .get(id)
                .cloned()
                .ok_or(OwnedTreeError::InvalidLineage)
        }

        async fn child_ids(&self, parent_id: &str) -> Result<Vec<String>, OwnedTreeError> {
            Ok(self
                .0
                .values()
                .filter(|session| session.parent_session_id.as_deref() == Some(parent_id))
                .map(|session| session.id.clone())
                .collect())
        }
    }

    fn sessions() -> Sessions {
        let now = Utc::now();
        let mut root = Session::new("root", "model");
        root.created_at = now;
        let mut child = Session::new_child_of("child", &root, "model", "child");
        child.created_at = now + Duration::seconds(1);
        let mut grandchild = Session::new_child_of("grandchild", &child, "model", "grandchild");
        grandchild.created_at = now + Duration::seconds(2);
        let mut sibling = Session::new_child_of("sibling", &root, "model", "sibling");
        sibling.created_at = now + Duration::seconds(1);
        let mut outsider = Session::new("outsider", "model");
        outsider.created_at = now;
        Sessions(
            [root, child, grandchild, sibling, outsider]
                .into_iter()
                .map(|session| (session.id.clone(), session))
                .collect(),
        )
    }

    #[tokio::test]
    async fn root_and_ancestor_can_inspect_descendant_but_sibling_and_other_root_cannot() {
        let store = sessions();
        assert_eq!(
            load_owned_descendant(&store, "root", "grandchild")
                .await
                .unwrap()
                .id,
            "grandchild"
        );
        assert_eq!(
            load_owned_descendant(&store, "child", "grandchild")
                .await
                .unwrap()
                .id,
            "grandchild"
        );
        for caller in ["sibling", "outsider", "grandchild"] {
            assert!(matches!(
                load_owned_descendant(&store, caller, "grandchild").await,
                Err(OwnedTreeError::InvalidLineage)
            ));
        }
    }

    #[tokio::test]
    async fn inconsistent_ancestor_birth_fails_closed() {
        let mut store = sessions();
        let invalid_birth = store.0.get("root").unwrap().created_at - Duration::seconds(1);
        store.0.get_mut("grandchild").unwrap().created_at = invalid_birth;
        assert!(matches!(
            load_owned_descendant(&store, "root", "grandchild").await,
            Err(OwnedTreeError::InvalidLineage)
        ));
    }
}
