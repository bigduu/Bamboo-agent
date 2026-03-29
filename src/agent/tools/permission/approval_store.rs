//! Approval caching store.
//!
//! Inspired by Codex's `ApprovalStore`, this module provides a generic cache
//! for tool approval decisions. When a user approves an operation (e.g.
//! "allow Bash to run `cargo test`"), the decision is stored so identical
//! requests don't prompt again within the same session.
//!
//! This is orthogonal to `PermissionConfig`'s whitelist/session-grant system:
//! - `PermissionConfig` handles **policy** (what's always allowed, always denied)
//! - `ApprovalStore` handles **per-invocation caching** (what was asked and answered)

use std::collections::HashMap;
use std::sync::RwLock;

use serde::Serialize;

/// Cached approval decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalDecision {
    /// User approved the operation.
    Approved,
    /// User approved for the rest of the session.
    ApprovedForSession,
    /// User denied the operation.
    Denied,
}

impl ApprovalDecision {
    /// Returns `true` if this decision means "proceed".
    pub fn is_approved(self) -> bool {
        matches!(self, Self::Approved | Self::ApprovedForSession)
    }
}

/// A thread-safe cache for tool approval decisions.
///
/// Keys are serialized approval contexts (tool name + resource + operation).
/// This means two calls with identical parameters will share the same cached
/// decision, avoiding redundant user prompts.
///
/// # Example
/// ```ignore
/// use crate::agent::tools::permission::ApprovalStore;
///
/// let store = ApprovalStore::new();
///
/// // First time: no cache, need to conclusion with options
/// assert!(store.get(&("Bash", "cargo test")).is_none());
///
/// // After user approves:
/// store.put(&("Bash", "cargo test"), ApprovalDecision::ApprovedForSession);
///
/// // Second time: cached, skip prompt
/// assert!(store.get(&("Bash", "cargo test")).unwrap().is_approved());
/// ```
#[derive(Debug, Default)]
pub struct ApprovalStore {
    /// Serialized key → decision mapping.
    cache: RwLock<HashMap<String, ApprovalDecision>>,
}

impl ApprovalStore {
    /// Create a new empty approval store.
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Look up a cached approval decision.
    ///
    /// The key is serialized to JSON for comparison. Returns `None` if no
    /// cached decision exists for this key.
    pub fn get<K: Serialize>(&self, key: &K) -> Option<ApprovalDecision> {
        let serialized = serde_json::to_string(key).ok()?;
        let cache = self.cache.read().ok()?;
        cache.get(&serialized).copied()
    }

    /// Store an approval decision.
    ///
    /// The key is serialized to JSON. If a decision already exists for this
    /// key, it is overwritten.
    pub fn put<K: Serialize>(&self, key: K, decision: ApprovalDecision) {
        if let Ok(serialized) = serde_json::to_string(&key) {
            if let Ok(mut cache) = self.cache.write() {
                cache.insert(serialized, decision);
            }
        }
    }

    /// Check multiple keys and return a decision only if ALL keys are cached
    /// and approved.
    ///
    /// This is useful for tools like `apply_patch` that modify multiple files
    /// at once — we only skip the prompt if every file has been individually
    /// approved.
    pub fn check_all<K: Serialize>(&self, keys: &[K]) -> Option<ApprovalDecision> {
        let cache = self.cache.read().ok()?;
        let mut all_approved = true;
        let mut any_session = false;

        for key in keys {
            let serialized = serde_json::to_string(key).ok()?;
            match cache.get(&serialized) {
                Some(ApprovalDecision::ApprovedForSession) => {
                    any_session = true;
                }
                Some(ApprovalDecision::Approved) => {}
                _ => {
                    all_approved = false;
                    break;
                }
            }
        }

        if all_approved {
            if any_session {
                Some(ApprovalDecision::ApprovedForSession)
            } else {
                Some(ApprovalDecision::Approved)
            }
        } else {
            None
        }
    }

    /// Store a decision for multiple keys at once.
    ///
    /// This is used after a bulk approval (e.g. user approves `apply_patch`
    /// affecting 3 files → each file path gets the same decision cached).
    pub fn put_all<K: Serialize>(&self, keys: &[K], decision: ApprovalDecision) {
        if let Ok(mut cache) = self.cache.write() {
            for key in keys {
                if let Ok(serialized) = serde_json::to_string(key) {
                    cache.insert(serialized, decision);
                }
            }
        }
    }

    /// Clear all cached decisions (e.g. on session reset).
    pub fn clear(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
    }

    /// Get the number of cached decisions.
    pub fn len(&self) -> usize {
        self.cache.read().map(|c| c.len()).unwrap_or(0)
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Convenience function implementing the "check cache → ask → store" pattern.
///
/// This mirrors Codex's `with_cached_approval`:
/// 1. If all keys are already approved, return the cached decision immediately.
/// 2. Otherwise, call `fetch` to get a fresh decision from the user.
/// 3. Store the decision for each key individually so future requests
///    touching any subset can also skip prompting.
pub async fn with_cached_approval<K, F, Fut>(
    store: &ApprovalStore,
    keys: Vec<K>,
    fetch: F,
) -> ApprovalDecision
where
    K: Serialize + Clone,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ApprovalDecision>,
{
    // Check cache first
    if let Some(cached) = store.check_all(&keys) {
        tracing::trace!("ApprovalStore: cache hit for {} key(s)", keys.len());
        return cached;
    }

    // Conclusion with options
    let decision = fetch().await;

    // Store decision for future lookups
    if decision.is_approved() {
        store.put_all(&keys, decision);
    }

    decision
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_store() {
        let store = ApprovalStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
        assert!(store.get(&"anything").is_none());
    }

    #[test]
    fn test_put_and_get() {
        let store = ApprovalStore::new();
        store.put(&("Bash", "cargo test"), ApprovalDecision::Approved);
        let decision = store.get(&("Bash", "cargo test")).unwrap();
        assert_eq!(decision, ApprovalDecision::Approved);
        assert!(decision.is_approved());
    }

    #[test]
    fn test_denied_not_approved() {
        let store = ApprovalStore::new();
        store.put(&"dangerous_op", ApprovalDecision::Denied);
        let decision = store.get(&"dangerous_op").unwrap();
        assert!(!decision.is_approved());
    }

    #[test]
    fn test_different_keys_independent() {
        let store = ApprovalStore::new();
        store.put(&("Bash", "echo hi"), ApprovalDecision::Approved);
        store.put(&("Bash", "rm -rf /"), ApprovalDecision::Denied);

        assert!(store.get(&("Bash", "echo hi")).unwrap().is_approved());
        assert!(!store.get(&("Bash", "rm -rf /")).unwrap().is_approved());
        assert!(store.get(&("Bash", "other")).is_none());
    }

    #[test]
    fn test_check_all_all_approved() {
        let store = ApprovalStore::new();
        store.put(&"/tmp/a.rs", ApprovalDecision::Approved);
        store.put(&"/tmp/b.rs", ApprovalDecision::ApprovedForSession);

        let keys = vec!["/tmp/a.rs", "/tmp/b.rs"];
        let decision = store.check_all(&keys).unwrap();
        // At least one session-level → result is session-level
        assert_eq!(decision, ApprovalDecision::ApprovedForSession);
    }

    #[test]
    fn test_check_all_missing_key() {
        let store = ApprovalStore::new();
        store.put(&"/tmp/a.rs", ApprovalDecision::Approved);
        // /tmp/c.rs is not in the store

        let keys = vec!["/tmp/a.rs", "/tmp/c.rs"];
        assert!(store.check_all(&keys).is_none());
    }

    #[test]
    fn test_check_all_denied_key() {
        let store = ApprovalStore::new();
        store.put(&"/tmp/a.rs", ApprovalDecision::Approved);
        store.put(&"/tmp/b.rs", ApprovalDecision::Denied);

        let keys = vec!["/tmp/a.rs", "/tmp/b.rs"];
        assert!(store.check_all(&keys).is_none());
    }

    #[test]
    fn test_put_all() {
        let store = ApprovalStore::new();
        let keys = vec!["/tmp/a.rs", "/tmp/b.rs", "/tmp/c.rs"];
        store.put_all(&keys, ApprovalDecision::ApprovedForSession);

        assert_eq!(store.len(), 3);
        for key in &keys {
            let d = store.get(key).unwrap();
            assert_eq!(d, ApprovalDecision::ApprovedForSession);
        }
    }

    #[test]
    fn test_clear() {
        let store = ApprovalStore::new();
        store.put(&"a", ApprovalDecision::Approved);
        store.put(&"b", ApprovalDecision::Denied);
        assert_eq!(store.len(), 2);

        store.clear();
        assert!(store.is_empty());
        assert!(store.get(&"a").is_none());
    }

    #[test]
    fn test_overwrite() {
        let store = ApprovalStore::new();
        store.put(&"cmd", ApprovalDecision::Denied);
        assert!(!store.get(&"cmd").unwrap().is_approved());

        store.put(&"cmd", ApprovalDecision::ApprovedForSession);
        assert!(store.get(&"cmd").unwrap().is_approved());
    }

    #[tokio::test]
    async fn test_with_cached_approval_cache_hit() {
        let store = ApprovalStore::new();
        store.put(&"key1", ApprovalDecision::Approved);

        let decision = with_cached_approval(&store, vec!["key1"], || async {
            panic!("should not be called — cache hit");
        })
        .await;

        assert_eq!(decision, ApprovalDecision::Approved);
    }

    #[tokio::test]
    async fn test_with_cached_approval_cache_miss() {
        let store = ApprovalStore::new();

        let decision = with_cached_approval(&store, vec!["key2"], || async {
            ApprovalDecision::ApprovedForSession
        })
        .await;

        assert_eq!(decision, ApprovalDecision::ApprovedForSession);
        // Should be cached now
        assert!(store.get(&"key2").unwrap().is_approved());
    }

    #[tokio::test]
    async fn test_with_cached_approval_denied_not_cached() {
        let store = ApprovalStore::new();

        let decision =
            with_cached_approval(&store, vec!["key3"], || async { ApprovalDecision::Denied }).await;

        assert_eq!(decision, ApprovalDecision::Denied);
        // Denied decisions are NOT cached
        assert!(store.get(&"key3").is_none());
    }

    #[tokio::test]
    async fn test_with_cached_approval_multi_key() {
        let store = ApprovalStore::new();
        store.put(&"file_a", ApprovalDecision::Approved);
        // file_b is missing

        let decision = with_cached_approval(&store, vec!["file_a", "file_b"], || async {
            ApprovalDecision::ApprovedForSession
        })
        .await;

        assert_eq!(decision, ApprovalDecision::ApprovedForSession);
        // Both should be cached now
        assert!(store.get(&"file_a").unwrap().is_approved());
        assert!(store.get(&"file_b").unwrap().is_approved());
    }
}
