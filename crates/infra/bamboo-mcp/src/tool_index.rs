use crate::error::ToolRegistrationError;
use crate::manager::generation::GenerationAuthority;
#[cfg(test)]
use crate::manager::generation::McpRuntimeGeneration;
use crate::types::{McpTool, ToolAlias};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// OpenAI and Anthropic function names are bounded to 64 provider-safe bytes.
pub const MAX_MCP_TOOL_ALIAS_BYTES: usize = 64;

/// Maximum number of process-lifetime alias-to-owner relationships retained to
/// prevent a removed identity from later rebinding. A normal tool consumes one
/// canonical and one legacy relationship, so this bounds hostile catalog churn
/// while leaving ample room for ordinary long-running managers.
pub const MAX_MCP_OWNERSHIP_LEDGER_RELATIONSHIPS: usize = 65_536;

// The suffix after `mcp__` deliberately contains no second `__`. Every legacy
// alias has a `server__tool` separator, so the canonical and legacy namespaces
// are syntactically disjoint even when remote labels are attacker-controlled.
const CANONICAL_ALIAS_PREFIX: &str = "mcp__v1_";
const ALIAS_TAG_BASE32_CHARS: usize = 26;
const SERVER_ALIAS_HASH_DOMAIN: &[u8] = b"bamboo-mcp-server-alias-v1\0";
const OWNER_ALIAS_HASH_DOMAIN: &[u8] = b"bamboo-mcp-tool-owner-alias-v1\0";
const CANONICAL_LEDGER_ALIAS_HASH_DOMAIN: &[u8] = b"bamboo-mcp-canonical-ledger-alias-v1\0";
const LEGACY_LEDGER_ALIAS_HASH_DOMAIN: &[u8] = b"bamboo-mcp-legacy-ledger-alias-v1\0";
const LEDGER_OWNER_HASH_DOMAIN: &[u8] = b"bamboo-mcp-ledger-owner-v1\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CanonicalAliasFingerprint([u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct LegacyAliasFingerprint([u8; 32]);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct OwnerFingerprint([u8; 32]);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ToolOwner {
    server_id: String,
    original_name: String,
}

impl ToolOwner {
    fn as_alias(&self, alias: String) -> ToolAlias {
        ToolAlias {
            alias,
            server_id: self.server_id.clone(),
            original_name: self.original_name.clone(),
        }
    }

    fn fingerprint(&self) -> OwnerFingerprint {
        OwnerFingerprint(fingerprint(
            LEDGER_OWNER_HASH_DOMAIN,
            &[self.server_id.as_bytes(), self.original_name.as_bytes()],
        ))
    }
}

#[derive(Debug, Clone)]
struct CatalogEntry {
    canonical_alias: String,
    legacy_alias: Option<String>,
    owner: ToolOwner,
}

/// Fully validated, mutation-free publication plan for one server catalog.
#[derive(Debug, Clone)]
pub(crate) struct ServerToolCatalog {
    server_id: String,
    entries: Vec<CatalogEntry>,
}

impl ServerToolCatalog {
    pub(crate) fn server_id(&self) -> &str {
        &self.server_id
    }

    pub(crate) fn aliases(&self) -> Vec<ToolAlias> {
        self.entries
            .iter()
            .map(|entry| entry.owner.as_alias(entry.canonical_alias.clone()))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn replace_first_canonical_alias_for_test(&mut self, alias: String) {
        self.entries
            .first_mut()
            .expect("test catalog must contain one tool")
            .canonical_alias = alias;
    }

    #[cfg(test)]
    pub(crate) fn replace_first_original_name_for_test(&mut self, original_name: String) {
        self.entries
            .first_mut()
            .expect("test catalog must contain one tool")
            .owner
            .original_name = original_name;
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct IndexState {
    /// Monotonic publication generation. Every committed replacement advances
    /// this value exactly once while holding the state write lock.
    revision: u64,
    /// Canonical provider-visible alias -> exact owner.
    aliases: BTreeMap<String, ToolOwner>,
    /// Process-lifetime ownership ledger. Only fixed-size, domain-separated
    /// fingerprints survive removal, so attacker-controlled labels are neither
    /// retained nor allowed to grow one allocation without bound.
    canonical_owners: BTreeMap<CanonicalAliasFingerprint, OwnerFingerprint>,
    /// Legacy sanitized alias -> every candidate owner. Lookup is permitted
    /// only when both the live and process-lifetime owner sets contain exactly
    /// the same one owner.
    legacy_candidates: BTreeMap<String, BTreeSet<ToolOwner>>,
    /// Process-lifetime legacy ownership ledger. Once a lossy historical alias
    /// has represented multiple owner fingerprints, removal cannot make an old
    /// transcript safe to retarget to whichever owner happens to remain.
    legacy_owners: BTreeMap<LegacyAliasFingerprint, BTreeSet<OwnerFingerprint>>,
    /// Stored publication authority used for replacement and owner-aware
    /// removal. Removal never regenerates an alias from caller input.
    server_catalogs: BTreeMap<String, ServerToolCatalog>,
}

impl IndexState {
    pub(crate) fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn plan_catalog_update(
        &self,
        replacements: &[ServerToolCatalog],
        removals: &[String],
        ledger_relationship_limit: usize,
    ) -> Result<Self, ToolRegistrationError> {
        let mut next = self.clone();
        next.apply_catalog_update(replacements, removals, ledger_relationship_limit)?;
        next.revision = self
            .revision
            .checked_add(1)
            .ok_or(ToolRegistrationError::PublicationRevisionExhausted)?;
        Ok(next)
    }

    fn apply_catalog_update(
        &mut self,
        replacements: &[ServerToolCatalog],
        removals: &[String],
        ledger_relationship_limit: usize,
    ) -> Result<(), ToolRegistrationError> {
        let mut replacement_ids = BTreeSet::new();
        for replacement in replacements {
            if !replacement_ids.insert(replacement.server_id.clone()) {
                return Err(ToolRegistrationError::DuplicateServerPlan);
            }
        }
        let removal_ids: BTreeSet<&str> = removals.iter().map(String::as_str).collect();
        if replacement_ids
            .iter()
            .any(|server_id| removal_ids.contains(server_id.as_str()))
        {
            return Err(ToolRegistrationError::ConflictingServerChange);
        }

        for server_id in removals {
            self.server_catalogs.remove(server_id);
        }
        for replacement in replacements {
            self.server_catalogs
                .insert(replacement.server_id.clone(), replacement.clone());
        }
        self.rebuild_alias_maps(ledger_relationship_limit)
    }

    fn rebuild_alias_maps(
        &mut self,
        ledger_relationship_limit: usize,
    ) -> Result<(), ToolRegistrationError> {
        let mut aliases = BTreeMap::new();
        let mut canonical_owners = self.canonical_owners.clone();
        let mut legacy_candidates: BTreeMap<String, BTreeSet<ToolOwner>> = BTreeMap::new();
        let mut legacy_owners = self.legacy_owners.clone();
        let mut ledger_relationships = self.ledger_relationship_count();
        if ledger_relationships > ledger_relationship_limit {
            return Err(ToolRegistrationError::OwnershipLedgerCapacityExceeded {
                limit: ledger_relationship_limit,
                attempted: ledger_relationships,
            });
        }

        for catalog in self.server_catalogs.values() {
            for entry in &catalog.entries {
                let canonical_fingerprint = canonical_alias_fingerprint(&entry.canonical_alias);
                let owner_fingerprint = entry.owner.fingerprint();
                if let Some(existing) = canonical_owners.get(&canonical_fingerprint) {
                    if existing != &owner_fingerprint {
                        return Err(ToolRegistrationError::AliasCollision {
                            alias: entry.canonical_alias.clone(),
                        });
                    }
                } else {
                    reserve_ledger_relationship(
                        &mut ledger_relationships,
                        ledger_relationship_limit,
                    )?;
                    canonical_owners.insert(canonical_fingerprint, owner_fingerprint);
                }
                aliases.insert(entry.canonical_alias.clone(), entry.owner.clone());

                if let Some(legacy_alias) = &entry.legacy_alias {
                    legacy_candidates
                        .entry(legacy_alias.clone())
                        .or_default()
                        .insert(entry.owner.clone());
                    let historical_owners = legacy_owners
                        .entry(legacy_alias_fingerprint(legacy_alias))
                        .or_default();
                    if !historical_owners.contains(&owner_fingerprint) {
                        reserve_ledger_relationship(
                            &mut ledger_relationships,
                            ledger_relationship_limit,
                        )?;
                        historical_owners.insert(owner_fingerprint);
                    }
                }
            }
        }

        self.aliases = aliases;
        self.canonical_owners = canonical_owners;
        self.legacy_candidates = legacy_candidates;
        self.legacy_owners = legacy_owners;
        Ok(())
    }

    fn ledger_relationship_count(&self) -> usize {
        self.legacy_owners
            .values()
            .fold(self.canonical_owners.len(), |total, owners| {
                total.saturating_add(owners.len())
            })
    }

    pub(crate) fn lookup(&self, alias: &str) -> Option<ToolAlias> {
        let resolved = self.resolve(alias)?;
        Some(ToolAlias {
            alias: alias.to_string(),
            server_id: resolved.server_id,
            original_name: resolved.original_name,
        })
    }

    pub(crate) fn resolve(&self, alias: &str) -> Option<ResolvedIndexAlias> {
        if let Some(owner) = self.aliases.get(alias) {
            return Some(ResolvedIndexAlias {
                canonical_alias: alias.to_string(),
                server_id: owner.server_id.clone(),
                original_name: owner.original_name.clone(),
            });
        }
        let active_owners = self.legacy_candidates.get(alias)?;
        if active_owners.len() != 1 {
            return None;
        }
        let active_owner = active_owners.iter().next()?;
        let historical_owners = self.legacy_owners.get(&legacy_alias_fingerprint(alias))?;
        if historical_owners.len() != 1 || !historical_owners.contains(&active_owner.fingerprint())
        {
            return None;
        }
        let canonical_alias = self.aliases.iter().find_map(|(canonical_alias, owner)| {
            (owner == active_owner).then(|| canonical_alias.clone())
        })?;
        Some(ResolvedIndexAlias {
            canonical_alias,
            server_id: active_owner.server_id.clone(),
            original_name: active_owner.original_name.clone(),
        })
    }

    pub(crate) fn all_aliases(&self) -> Vec<ToolAlias> {
        self.aliases
            .iter()
            .map(|(alias, owner)| owner.as_alias(alias.clone()))
            .collect()
    }

    pub(crate) fn get_server_tools(&self, server_id: &str) -> Option<Vec<String>> {
        self.server_catalogs.get(server_id).map(|catalog| {
            catalog
                .entries
                .iter()
                .map(|entry| entry.owner.original_name.clone())
                .collect()
        })
    }

    pub(crate) fn contains_exact(&self, alias: &str) -> bool {
        self.aliases.contains_key(alias)
    }
}

pub(crate) struct ResolvedIndexAlias {
    pub(crate) canonical_alias: String,
    pub(crate) server_id: String,
    pub(crate) original_name: String,
}

/// A whole-index update that has already validated every replacement/removal.
/// The manager holds its reconciliation lock between preflight and commit.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct ToolIndexTransaction {
    base_generation: Arc<McpRuntimeGeneration>,
    base_revision: u64,
    next: IndexState,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
enum ToolIndexCommitError {
    #[error(
        "stale MCP tool-index transaction: base revision {base_revision}, current revision {current_revision}"
    )]
    StaleTransaction {
        base_revision: u64,
        current_revision: u64,
    },
}

/// Atomic owner index for MCP tool identities.
///
/// All canonical aliases, compatibility aliases, and per-server ownership are
/// published behind one lock so readers never observe a partially installed
/// catalog.
pub struct ToolIndex {
    authority: Arc<GenerationAuthority>,
}

impl ToolIndex {
    pub fn new() -> Self {
        Self {
            authority: GenerationAuthority::new(MAX_MCP_OWNERSHIP_LEDGER_RELATIONSHIPS),
        }
    }

    pub(crate) fn from_authority(authority: Arc<GenerationAuthority>) -> Self {
        Self { authority }
    }

    pub(crate) fn authority(&self) -> &Arc<GenerationAuthority> {
        &self.authority
    }

    pub fn same_authority(&self, other: &Self) -> bool {
        GenerationAuthority::same_authority(&self.authority, &other.authority)
    }

    #[cfg(test)]
    fn with_ledger_relationship_limit_for_test(ledger_relationship_limit: usize) -> Self {
        Self {
            authority: GenerationAuthority::new(ledger_relationship_limit),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_revision_for_test(&self, revision: u64) {
        let base = self.authority.generation();
        let mut index = (*base.index).clone();
        index.revision = revision;
        let next = McpRuntimeGeneration::with_index(&base, &[], &[], index, false)
            .expect("test revision must preserve the generation shape");
        self.authority.replace_prevalidated(&base, next);
    }

    fn read_state(&self) -> Arc<IndexState> {
        self.authority.generation().index.clone()
    }

    /// Generate the stable provider-visible identity for one exact owner tuple.
    ///
    /// The server and complete owner tuple use independent, domain-separated,
    /// length-framed SHA-256 hashes. Each is truncated to 26 lowercase base32
    /// characters (130 bits), keeping the complete alias within 64 bytes while
    /// allowing secret-safe metrics to group tools by a stable server pseudonym.
    /// The process-lifetime owner ledger rejects any realized truncation
    /// collision instead of silently rebinding the alias.
    pub fn generate_alias(&self, server_id: &str, tool_name: &str) -> String {
        let server_tag = hash_tag(SERVER_ALIAS_HASH_DOMAIN, &[server_id.as_bytes()]);
        let owner_tag = hash_tag(
            OWNER_ALIAS_HASH_DOMAIN,
            &[server_id.as_bytes(), tool_name.as_bytes()],
        );
        let alias = format!("{CANONICAL_ALIAS_PREFIX}{server_tag}_{owner_tag}");
        debug_assert!(is_provider_safe_alias(&alias));
        debug_assert!(alias.len() <= MAX_MCP_TOOL_ALIAS_BYTES);
        alias
    }

    /// Validate the complete allowed/non-denied catalog without mutating live
    /// publication. Duplicate original names are rejected explicitly.
    pub(crate) fn plan_server_tools(
        &self,
        server_id: &str,
        tools: &[McpTool],
        allowed_tools: &[String],
        denied_tools: &[String],
    ) -> Result<ServerToolCatalog, ToolRegistrationError> {
        if server_id.is_empty() {
            return Err(ToolRegistrationError::EmptyServerIdentity);
        }

        let mut first_positions = BTreeMap::<String, usize>::new();
        let mut entries = Vec::new();
        for (position, tool) in tools.iter().enumerate() {
            if !allowed_tools.is_empty() && !allowed_tools.contains(&tool.name) {
                continue;
            }
            if denied_tools.contains(&tool.name) {
                continue;
            }
            if tool.name.is_empty() {
                return Err(ToolRegistrationError::EmptyToolIdentity { position });
            }
            if let Some(first_position) = first_positions.insert(tool.name.clone(), position) {
                return Err(ToolRegistrationError::DuplicateToolIdentity {
                    first_position,
                    duplicate_position: position,
                });
            }

            let canonical_alias = self.generate_alias(server_id, &tool.name);
            let legacy_alias =
                legacy_alias(server_id, &tool.name).filter(|legacy| legacy != &canonical_alias);
            entries.push(CatalogEntry {
                canonical_alias,
                legacy_alias,
                owner: ToolOwner {
                    server_id: server_id.to_string(),
                    original_name: tool.name.clone(),
                },
            });
        }

        // Event/schema/listing order is stable even when an MCP server returns
        // its otherwise identical catalog in a different order.
        entries.sort_by(|left, right| left.canonical_alias.cmp(&right.canonical_alias));
        Ok(ServerToolCatalog {
            server_id: server_id.to_string(),
            entries,
        })
    }

    /// Preflight a complete set of catalog changes against one cloned snapshot.
    /// No live map is touched if any plan is invalid or collides.
    #[cfg(test)]
    pub(crate) fn preflight_catalog_update(
        &self,
        replacements: &[ServerToolCatalog],
        removals: &[String],
    ) -> Result<ToolIndexTransaction, ToolRegistrationError> {
        let base_generation = self.authority.generation();
        let base_revision = base_generation.index.revision;
        let next = base_generation.index.plan_catalog_update(
            replacements,
            removals,
            self.authority.ledger_relationship_limit,
        )?;
        Ok(ToolIndexTransaction {
            base_generation,
            base_revision,
            next,
        })
    }

    /// Commit a transaction preflighted while the manager reconciliation lock is
    /// held. A revision mismatch is an internal writer-invariant violation, not
    /// a recoverable post-durable CAS outcome: production has exactly one writer
    /// behind that lock. Fail-stop before swapping in every build so a stale
    /// snapshot can never erase a newer catalog or ownership history.
    #[cfg(test)]
    pub(crate) fn commit_catalog_update(&self, transaction: ToolIndexTransaction) {
        self.try_commit_catalog_update(transaction)
            .unwrap_or_else(|error| panic!("MCP tool-index writer invariant violated: {error}"));
    }

    #[cfg(test)]
    fn try_commit_catalog_update(
        &self,
        transaction: ToolIndexTransaction,
    ) -> Result<(), ToolIndexCommitError> {
        let current = self.authority.generation();
        let current_revision = current.index.revision;
        if !Arc::ptr_eq(&current, &transaction.base_generation) {
            return Err(ToolIndexCommitError::StaleTransaction {
                base_revision: transaction.base_revision,
                current_revision,
            });
        }
        let next = McpRuntimeGeneration::with_index(
            &transaction.base_generation,
            &[],
            &[],
            transaction.next,
            false,
        )
        .expect("test-only catalog state must construct a detached generation");
        if self
            .authority
            .try_replace(&transaction.base_generation, next)
        {
            Ok(())
        } else {
            Err(ToolIndexCommitError::StaleTransaction {
                base_revision: transaction.base_revision,
                current_revision: self.authority.generation().revision,
            })
        }
    }

    /// Test-only direct registration seam. Production writes are owned by
    /// `McpServerManager` and serialized by its reconciliation lock.
    #[cfg(test)]
    pub(crate) fn register_server_tools(
        &self,
        server_id: &str,
        tools: &[McpTool],
        allowed_tools: &[String],
        denied_tools: &[String],
    ) -> Result<Vec<ToolAlias>, ToolRegistrationError> {
        let catalog = self.plan_server_tools(server_id, tools, allowed_tools, denied_tools)?;
        let aliases = catalog.aliases();
        let transaction = self.preflight_catalog_update(std::slice::from_ref(&catalog), &[])?;
        self.try_commit_catalog_update(transaction)
            .expect("test-only MCP registration must commit its current snapshot");
        Ok(aliases)
    }

    /// Test-only direct removal seam. Production removal is one manager-owned
    /// preflight/commit transaction under the reconciliation lock.
    #[cfg(test)]
    pub(crate) fn remove_server_tools(&self, server_id: &str) {
        let transaction = self
            .preflight_catalog_update(&[], &[server_id.to_string()])
            .expect("removing one stored MCP catalog cannot introduce a collision");
        self.try_commit_catalog_update(transaction)
            .expect("test-only MCP removal must commit its current snapshot");
    }

    /// Lookup either an exact canonical alias or an unambiguous legacy alias.
    /// Canonical identities always win. Ambiguous legacy identities fail closed.
    pub fn lookup(&self, alias: &str) -> Option<ToolAlias> {
        self.read_state().lookup(alias)
    }

    /// Get every canonical provider-visible alias in stable lexical order.
    /// Legacy compatibility names are deliberately not advertised.
    pub fn all_aliases(&self) -> Vec<ToolAlias> {
        self.read_state().all_aliases()
    }

    /// Get exact original tool names for one server in canonical alias order.
    pub fn get_server_tools(&self, server_id: &str) -> Option<Vec<String>> {
        self.read_state().get_server_tools(server_id)
    }

    /// Test-only clear seam. Process-lifetime owner ledgers remain so removed
    /// aliases cannot later bind or retarget to another tuple.
    #[cfg(test)]
    pub(crate) fn clear(&self) {
        let removals = self
            .read_state()
            .server_catalogs
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let transaction = self
            .preflight_catalog_update(&[], &removals)
            .expect("clearing stored MCP catalogs cannot introduce a collision");
        self.try_commit_catalog_update(transaction)
            .expect("test-only MCP clear must commit its current snapshot");
    }

    /// Check exact canonical ownership without accepting compatibility names.
    ///
    /// Execution surfaces that promise exact tool membership must use this
    /// method rather than the legacy-aware [`Self::contains`] seam.
    pub fn contains_exact_alias(&self, alias: &str) -> bool {
        self.read_state().contains_exact(alias)
    }

    /// Check whether a canonical or unambiguous compatibility alias resolves.
    /// This is intentionally broader than exact provider-visible membership.
    pub fn contains(&self, alias: &str) -> bool {
        self.lookup(alias).is_some()
    }
}

impl Default for ToolIndex {
    fn default() -> Self {
        Self::new()
    }
}

fn update_len_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn fingerprint(domain: &[u8], components: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for component in components {
        update_len_prefixed(&mut hasher, component);
    }
    hasher.finalize().into()
}

fn hash_tag(domain: &[u8], components: &[&[u8]]) -> String {
    let digest = fingerprint(domain, components);
    let encoded = base32_lower_no_pad(&digest);
    encoded[..ALIAS_TAG_BASE32_CHARS].to_string()
}

fn canonical_alias_fingerprint(alias: &str) -> CanonicalAliasFingerprint {
    CanonicalAliasFingerprint(fingerprint(
        CANONICAL_LEDGER_ALIAS_HASH_DOMAIN,
        &[alias.as_bytes()],
    ))
}

fn legacy_alias_fingerprint(alias: &str) -> LegacyAliasFingerprint {
    LegacyAliasFingerprint(fingerprint(
        LEGACY_LEDGER_ALIAS_HASH_DOMAIN,
        &[alias.as_bytes()],
    ))
}

fn reserve_ledger_relationship(
    current: &mut usize,
    limit: usize,
) -> Result<(), ToolRegistrationError> {
    let attempted = current.saturating_add(1);
    if attempted > limit {
        return Err(ToolRegistrationError::OwnershipLedgerCapacityExceeded { limit, attempted });
    }
    *current = attempted;
    Ok(())
}

fn base32_lower_no_pad(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut output = String::with_capacity(bytes.len().div_ceil(5) * 8);
    let mut accumulator = 0u16;
    let mut bits = 0u8;
    for byte in bytes {
        accumulator = (accumulator << 8) | u16::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let index = ((accumulator >> bits) & 0x1f) as usize;
            output.push(ALPHABET[index] as char);
        }
        accumulator &= (1u16 << bits).wrapping_sub(1);
    }
    if bits > 0 {
        let index = ((accumulator << (5 - bits)) & 0x1f) as usize;
        output.push(ALPHABET[index] as char);
    }
    output
}

fn is_provider_safe_alias(alias: &str) -> bool {
    !alias.is_empty()
        && alias.len() <= MAX_MCP_TOOL_ALIAS_BYTES
        && alias
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn legacy_alias(server_id: &str, tool_name: &str) -> Option<String> {
    let sanitized_server = server_id.replace("::", "__").replace(':', "_");
    let sanitized_tool = tool_name.replace("::", "__").replace(':', "_");
    let alias = format!("mcp__{sanitized_server}__{sanitized_tool}");
    (!sanitized_server.is_empty() && !sanitized_tool.is_empty()).then_some(alias)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::{canonical_tool_name, CapabilityLoadingClass, ClassifiedToolIdentity};

    fn tool(name: &str) -> McpTool {
        McpTool {
            name: name.to_string(),
            description: format!("{name} description"),
            parameters: serde_json::json!({"type": "object"}),
            output_schema: None,
        }
    }

    #[test]
    fn canonical_codec_is_stable_bounded_and_provider_safe() {
        let server = "服务器::".repeat(100);
        let name = "tool:name with punctuation/秘密".repeat(100);
        let first = ToolIndex::new().generate_alias(&server, &name);
        let second = ToolIndex::new().generate_alias(&server, &name);

        assert_eq!(first, second);
        assert!(first.starts_with(CANONICAL_ALIAS_PREFIX));
        assert_eq!(first.len(), 61);
        assert!(first.len() <= MAX_MCP_TOOL_ALIAS_BYTES);
        assert!(is_provider_safe_alias(&first));
    }

    #[test]
    fn former_sanitize_collisions_have_distinct_canonical_aliases() {
        let index = ToolIndex::new();
        for ((left_server, left_tool), (right_server, right_tool)) in [
            (("a::b", "c"), ("a__b", "c")),
            (("a:b", "c"), ("a_b", "c")),
            (("a", "b__c"), ("a__b", "c")),
        ] {
            assert_ne!(
                index.generate_alias(left_server, left_tool),
                index.generate_alias(right_server, right_tool)
            );
            assert_eq!(
                legacy_alias(left_server, left_tool),
                legacy_alias(right_server, right_tool)
            );
        }
    }

    #[test]
    fn canonical_namespace_cannot_be_constructed_by_the_legacy_codec() {
        let index = ToolIndex::new();
        let canonical = index.generate_alias("target", "tool");
        let hash = canonical
            .strip_prefix(CANONICAL_ALIAS_PREFIX)
            .expect("canonical prefix");
        let attacker_controlled_legacy = legacy_alias("v1", hash).unwrap();

        assert_eq!(attacker_controlled_legacy, format!("mcp__v1__{hash}"));
        assert_ne!(canonical, attacker_controlled_legacy);
        assert!(!canonical["mcp__".len()..].contains("__"));
    }

    #[test]
    fn canonical_codec_exposes_stable_distinct_server_and_owner_pseudonyms() {
        fn tags(alias: &str) -> (&str, &str) {
            alias
                .strip_prefix(CANONICAL_ALIAS_PREFIX)
                .and_then(|value| value.split_once('_'))
                .expect("canonical server and owner tags")
        }

        let index = ToolIndex::new();
        let first = index.generate_alias("server-a", "tool-one");
        let sibling = index.generate_alias("server-a", "tool-two");
        let other_server = index.generate_alias("server-b", "tool-one");
        let (first_server, first_owner) = tags(&first);
        let (sibling_server, sibling_owner) = tags(&sibling);
        let (other_server_tag, other_owner) = tags(&other_server);

        assert_eq!(first_server.len(), ALIAS_TAG_BASE32_CHARS);
        assert_eq!(first_owner.len(), ALIAS_TAG_BASE32_CHARS);
        assert_eq!(first_server, sibling_server);
        assert_ne!(first_owner, sibling_owner);
        assert_ne!(first_server, other_server_tag);
        assert_ne!(first_owner, other_owner);
    }

    #[test]
    fn generated_alias_is_an_exact_deferred_capability_identity() {
        let index = ToolIndex::new();
        let alias = index.generate_alias("filesystem", "read_file");
        let identity = ClassifiedToolIdentity::from_schema_name(&alias)
            .expect("generated MCP alias must be a valid registration identity");

        assert_eq!(canonical_tool_name(&alias), alias);
        assert_eq!(identity.execution_name(), alias);
        assert_eq!(identity.alias_fallback_name(), alias);
        assert_eq!(identity.loading_class(), CapabilityLoadingClass::Deferred);
    }

    #[test]
    fn registration_filtering_and_lookup_preserve_exact_owner() {
        let index = ToolIndex::new();
        let aliases = index
            .register_server_tools(
                "fs",
                &[tool("read_file"), tool("delete_file")],
                &["read_file".to_string()],
                &["delete_file".to_string()],
            )
            .unwrap();
        assert_eq!(aliases.len(), 1);
        let alias = &aliases[0].alias;
        let lookup = index.lookup(alias).unwrap();
        assert_eq!(lookup.alias, *alias);
        assert_eq!(lookup.server_id, "fs");
        assert_eq!(lookup.original_name, "read_file");
        assert_eq!(index.get_server_tools("fs").unwrap(), ["read_file"]);
    }

    #[test]
    fn duplicate_catalog_fails_before_replacing_prior_publication() {
        let index = ToolIndex::new();
        let old = index
            .register_server_tools("fs", &[tool("old")], &[], &[])
            .unwrap();
        let error = index
            .register_server_tools(
                "fs",
                &[tool("new"), tool("duplicate"), tool("duplicate")],
                &[],
                &[],
            )
            .unwrap_err();
        assert_eq!(
            error,
            ToolRegistrationError::DuplicateToolIdentity {
                first_position: 1,
                duplicate_position: 2
            }
        );
        assert_eq!(index.all_aliases(), old);
        assert!(index.lookup(&old[0].alias).is_some());
        assert!(index.lookup(&index.generate_alias("fs", "new")).is_none());
    }

    #[test]
    fn registration_diagnostics_do_not_echo_remote_identities() {
        let index = ToolIndex::new();
        let secret_server = "https://user:password@example.invalid/private";
        let secret_tool = "Bearer highly-sensitive-remote-label";
        let error = index
            .plan_server_tools(
                secret_server,
                &[tool(secret_tool), tool(secret_tool)],
                &[],
                &[],
            )
            .unwrap_err();
        let diagnostic = error.to_string();

        assert!(!diagnostic.contains(secret_server));
        assert!(!diagnostic.contains(secret_tool));
        assert!(diagnostic.contains("positions 0 and 1"));
    }

    #[test]
    fn removed_catalog_history_retains_only_fixed_size_fingerprints() {
        let index = ToolIndex::new();
        let secret_server = "https://user:password@example.invalid/private";
        let secret_tool = "Bearer highly-sensitive-remote-label";
        let legacy = legacy_alias(secret_server, secret_tool).unwrap();
        index
            .register_server_tools(secret_server, &[tool(secret_tool)], &[], &[])
            .unwrap();
        index.remove_server_tools(secret_server);

        let state = index.read_state();
        assert!(state.aliases.is_empty());
        assert!(state.legacy_candidates.is_empty());
        assert!(state.server_catalogs.is_empty());
        assert_eq!(state.ledger_relationship_count(), 2);
        assert!(state
            .canonical_owners
            .keys()
            .all(|fingerprint| std::mem::size_of_val(fingerprint) == 32));
        assert!(state
            .canonical_owners
            .values()
            .all(|fingerprint| std::mem::size_of_val(fingerprint) == 32));
        assert!(state
            .legacy_owners
            .keys()
            .all(|fingerprint| std::mem::size_of_val(fingerprint) == 32));
        let historical_debug = format!("{:?}{:?}", state.canonical_owners, state.legacy_owners);
        assert!(!historical_debug.contains(secret_server));
        assert!(!historical_debug.contains(secret_tool));
        assert!(!historical_debug.contains(&legacy));
    }

    #[test]
    fn ledger_fingerprints_are_domain_separated_and_length_framed() {
        let owner_left = ToolOwner {
            server_id: "a".to_string(),
            original_name: "bc".to_string(),
        };
        let owner_right = ToolOwner {
            server_id: "ab".to_string(),
            original_name: "c".to_string(),
        };
        assert_ne!(owner_left.fingerprint(), owner_right.fingerprint());

        let same_input = "mcp__same__bytes";
        assert_ne!(
            canonical_alias_fingerprint(same_input).0,
            legacy_alias_fingerprint(same_input).0,
            "canonical and legacy ledgers must not share a hash domain"
        );
    }

    #[test]
    fn ledger_capacity_failure_rolls_back_and_same_owner_does_not_consume_capacity() {
        // One owner normally consumes one canonical and one legacy relationship.
        // The limit of three lets the colliding candidate reserve its canonical
        // relationship in the cloned snapshot, then fail on its fourth total
        // relationship without poisoning live history.
        let index = ToolIndex::with_ledger_relationship_limit_for_test(3);
        let old = index
            .register_server_tools("a::b", &[tool("c")], &[], &[])
            .unwrap();
        assert_eq!(index.read_state().ledger_relationship_count(), 2);

        for _ in 0..3 {
            assert_eq!(
                index
                    .register_server_tools("a::b", &[tool("c")], &[], &[])
                    .unwrap(),
                old
            );
            assert_eq!(index.read_state().ledger_relationship_count(), 2);
        }

        let error = index
            .register_server_tools("a__b", &[tool("c")], &[], &[])
            .unwrap_err();
        assert_eq!(
            error,
            ToolRegistrationError::OwnershipLedgerCapacityExceeded {
                limit: 3,
                attempted: 4,
            }
        );
        assert_eq!(index.read_state().ledger_relationship_count(), 2);
        assert_eq!(index.all_aliases(), old);
        assert!(index.lookup(&old[0].alias).is_some());
        assert!(index.lookup("mcp__a__b__c").is_some());
        assert!(index.lookup(&index.generate_alias("a__b", "c")).is_none());
    }

    #[test]
    fn whole_catalog_preflight_detects_bounded_alias_collision_without_mutation() {
        let index = ToolIndex::new();
        let old = index
            .register_server_tools("stable", &[tool("old")], &[], &[])
            .unwrap();
        let first = index
            .plan_server_tools("first", &[tool("one")], &[], &[])
            .unwrap();
        let mut second = index
            .plan_server_tools("second", &[tool("two")], &[], &[])
            .unwrap();
        second.entries[0].canonical_alias = first.entries[0].canonical_alias.clone();

        let error = index
            .preflight_catalog_update(&[first.clone(), second], &["stable".to_string()])
            .unwrap_err();
        assert_eq!(
            error,
            ToolRegistrationError::AliasCollision {
                alias: first.entries[0].canonical_alias.clone()
            }
        );
        assert_eq!(index.all_aliases(), old);
        assert!(index.lookup(&old[0].alias).is_some());
        assert!(index.get_server_tools("stable").is_some());
    }

    #[test]
    fn stale_transaction_is_rejected_before_swap_and_preserves_newer_history() {
        let index = ToolIndex::new();
        let first = index
            .register_server_tools("a::b", &[tool("c")], &[], &[])
            .unwrap();
        let legacy = legacy_alias("a::b", "c").unwrap();
        assert_eq!(index.lookup(&legacy).unwrap().server_id, "a::b");

        // Preflight A from revision 1. Its cloned snapshot contains only the
        // first owner and would therefore make the lossy legacy alias resolve
        // to that owner if it could overwrite a later publication.
        let planned = index
            .plan_server_tools("planned", &[tool("unrelated")], &[], &[])
            .unwrap();
        let stale = index
            .preflight_catalog_update(std::slice::from_ref(&planned), &[])
            .unwrap();
        let stale_fail_stop = index
            .preflight_catalog_update(std::slice::from_ref(&planned), &[])
            .unwrap();
        assert_eq!(stale.base_revision, 1);

        // Test-only direct mutation B publishes the second owner of the same
        // legacy alias and permanently records the resulting ambiguity.
        let second = index
            .register_server_tools("a__b", &[tool("c")], &[], &[])
            .unwrap();
        assert!(index.lookup(&legacy).is_none());
        assert_eq!(index.read_state().revision, 2);

        let error = index.try_commit_catalog_update(stale).unwrap_err();
        assert_eq!(
            error,
            ToolIndexCommitError::StaleTransaction {
                base_revision: 1,
                current_revision: 2,
            }
        );
        let fail_stop = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            index.commit_catalog_update(stale_fail_stop);
        }));
        assert!(
            fail_stop.is_err(),
            "the production commit seam must fail-stop on an impossible stale writer"
        );

        // The stale snapshot is rejected before the state swap: B's canonical
        // publication and fixed-size historical owner fingerprint both remain,
        // while A's unrelated planned catalog never appears.
        assert_eq!(index.read_state().revision, 2);
        assert!(index.lookup(&first[0].alias).is_some());
        assert!(index.lookup(&second[0].alias).is_some());
        assert!(index
            .lookup(&index.generate_alias("planned", "unrelated"))
            .is_none());
        assert!(index.lookup(&legacy).is_none());
        let state = index.read_state();
        let historical = state
            .legacy_owners
            .get(&legacy_alias_fingerprint(&legacy))
            .expect("legacy history remains published");
        assert_eq!(historical.len(), 2);
        assert!(historical.contains(
            &ToolOwner {
                server_id: "a__b".to_string(),
                original_name: "c".to_string(),
            }
            .fingerprint()
        ));
    }

    #[test]
    fn removed_canonical_alias_cannot_rebind_to_another_owner() {
        let index = ToolIndex::new();
        let first = index
            .plan_server_tools("first", &[tool("one")], &[], &[])
            .unwrap();
        let canonical_alias = first.entries[0].canonical_alias.clone();
        let transaction = index
            .preflight_catalog_update(std::slice::from_ref(&first), &[])
            .unwrap();
        index.commit_catalog_update(transaction);
        index.remove_server_tools("first");
        assert!(index.all_aliases().is_empty());

        let mut second = index
            .plan_server_tools("second", &[tool("two")], &[], &[])
            .unwrap();
        second.replace_first_canonical_alias_for_test(canonical_alias.clone());
        let error = index.preflight_catalog_update(&[second], &[]).unwrap_err();

        assert_eq!(
            error,
            ToolRegistrationError::AliasCollision {
                alias: canonical_alias
            }
        );
        assert!(index.all_aliases().is_empty());
    }

    #[test]
    fn ambiguous_legacy_alias_fails_closed_in_both_registration_orders() {
        for order in [
            [("a::b", "c"), ("a__b", "c")],
            [("a__b", "c"), ("a::b", "c")],
        ] {
            let index = ToolIndex::new();
            let first = index
                .register_server_tools(order[0].0, &[tool(order[0].1)], &[], &[])
                .unwrap();
            let second = index
                .register_server_tools(order[1].0, &[tool(order[1].1)], &[], &[])
                .unwrap();
            let legacy = legacy_alias("a::b", "c").unwrap();
            assert!(index.lookup(&legacy).is_none());
            assert_eq!(first[0].alias, index.generate_alias(order[0].0, order[0].1));
            assert_eq!(
                second[0].alias,
                index.generate_alias(order[1].0, order[1].1)
            );
            assert_eq!(index.lookup(&first[0].alias).unwrap().server_id, order[0].0);
            assert_eq!(
                index.lookup(&second[0].alias).unwrap().server_id,
                order[1].0
            );
            assert_eq!(index.all_aliases().len(), 2);
        }
    }

    #[test]
    fn owner_aware_removal_preserves_other_owner_and_keeps_legacy_fail_closed() {
        for registration_order in [
            [("a::b", "c"), ("a__b", "c")],
            [("a__b", "c"), ("a::b", "c")],
        ] {
            for removed_server in ["a::b", "a__b"] {
                let index = ToolIndex::new();
                for (server_id, tool_name) in registration_order {
                    index
                        .register_server_tools(server_id, &[tool(tool_name)], &[], &[])
                        .unwrap();
                }
                let legacy = legacy_alias("a::b", "c").unwrap();
                assert!(index.lookup(&legacy).is_none());
                assert_eq!(index.read_state().ledger_relationship_count(), 4);

                let survivor_server = if removed_server == "a::b" {
                    "a__b"
                } else {
                    "a::b"
                };
                let removed_alias = index.generate_alias(removed_server, "c");
                let survivor_alias = index.generate_alias(survivor_server, "c");
                index.remove_server_tools(removed_server);

                assert!(index.lookup(&removed_alias).is_none());
                assert_eq!(
                    index.lookup(&survivor_alias).unwrap().server_id,
                    survivor_server
                );
                assert!(
                    index.lookup(&legacy).is_none(),
                    "a historically ambiguous alias must not retarget after removal"
                );
                assert_eq!(
                    index.read_state().ledger_relationship_count(),
                    4,
                    "removal must retain fixed-size historical ambiguity"
                );
            }
        }
    }

    #[test]
    fn publication_and_listing_order_are_independent_of_catalog_order() {
        let index = ToolIndex::new();
        let first = index
            .register_server_tools("stable", &[tool("zeta"), tool("alpha")], &[], &[])
            .unwrap();
        let first_listing = index.all_aliases();
        let second = index
            .register_server_tools("stable", &[tool("alpha"), tool("zeta")], &[], &[])
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(first_listing, index.all_aliases());
    }

    #[test]
    fn unambiguous_provider_safe_legacy_alias_remains_lookup_only() {
        let index = ToolIndex::new();
        let canonical = index
            .register_server_tools("filesystem", &[tool("read_file")], &[], &[])
            .unwrap();
        let legacy = "mcp__filesystem__read_file";
        let lookup = index.lookup(legacy).unwrap();
        assert_eq!(lookup.alias, legacy);
        assert_eq!(lookup.server_id, "filesystem");
        assert_eq!(lookup.original_name, "read_file");
        assert_eq!(index.all_aliases(), canonical);
        assert_ne!(canonical[0].alias, legacy);
        assert!(index.contains_exact_alias(&canonical[0].alias));
        assert!(!index.contains_exact_alias(legacy));
        assert!(index.contains(legacy));
    }

    #[test]
    fn unambiguous_legacy_history_lookup_does_not_require_provider_safe_text() {
        let index = ToolIndex::new();
        let canonical = index
            .register_server_tools("server/with path", &[tool("tool/with path")], &[], &[])
            .unwrap();
        let legacy = "mcp__server/with path__tool/with path";

        let lookup = index.lookup(legacy).unwrap();
        assert_eq!(lookup.server_id, "server/with path");
        assert_eq!(lookup.original_name, "tool/with path");
        assert!(!index.contains_exact_alias(legacy));
        assert_eq!(index.all_aliases(), canonical);
        assert!(is_provider_safe_alias(&canonical[0].alias));
    }

    #[test]
    fn clear_removes_catalog_and_every_lookup_form_atomically() {
        let index = ToolIndex::new();
        let canonical = index
            .register_server_tools("filesystem", &[tool("read_file")], &[], &[])
            .unwrap();
        let legacy = "mcp__filesystem__read_file";
        assert!(index.contains_exact_alias(&canonical[0].alias));
        assert!(index.contains(legacy));

        index.clear();

        assert!(index.all_aliases().is_empty());
        assert!(index.get_server_tools("filesystem").is_none());
        assert!(!index.contains_exact_alias(&canonical[0].alias));
        assert!(!index.contains(legacy));
    }
}
