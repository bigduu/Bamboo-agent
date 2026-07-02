//! Namespacing for the SHARED `DeployedRegistry` (one map, two writers).
//!
//! `deploy_agent` keys by an agent-chosen / random id, the cluster fabric keys
//! by a config node id. Without a prefix an id collision in the shared map would
//! silently cross-evict the other surface's worker. These helpers keep the two
//! key-spaces disjoint (`agent:<id>` vs `node:<id>`).

const AGENT_PREFIX: &str = "agent:";
const NODE_PREFIX: &str = "node:";

/// Registry key for a `deploy_agent`-deployed worker.
pub fn agent_key(id: &str) -> String {
    format!("{AGENT_PREFIX}{id}")
}

/// Registry key for a cluster-fabric node's worker.
pub fn node_key(node_id: &str) -> String {
    format!("{NODE_PREFIX}{node_id}")
}

/// Split a registry key back into `(source, bare_id)` for display.
/// `source` is `"agent"`, `"node"`, or `"unknown"` for an unprefixed key.
pub fn split(key: &str) -> (&'static str, &str) {
    if let Some(id) = key.strip_prefix(AGENT_PREFIX) {
        ("agent", id)
    } else if let Some(id) = key.strip_prefix(NODE_PREFIX) {
        ("node", id)
    } else {
        ("unknown", key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_disjoint_and_splittable() {
        // Same bare id, different namespace → different keys (no collision).
        assert_ne!(agent_key("x"), node_key("x"));
        assert_eq!(split(&agent_key("w1")), ("agent", "w1"));
        assert_eq!(split(&node_key("n1")), ("node", "n1"));
        assert_eq!(split("legacy"), ("unknown", "legacy"));
    }
}
