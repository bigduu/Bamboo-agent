//! Durable, provider-native transcript items used by progressive tool discovery.
//!
//! This lane is deliberately separate from [`Session::messages`]. The latter is
//! the provider-neutral, user-visible transcript; this module preserves the
//! small set of provider-owned items whose exact position carries tool-loading
//! state. Raw JSON is admitted only through the validators below, so persisted
//! data cannot become an unrestricted request-injection channel.

use std::collections::{HashMap, HashSet};
use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::Session;

pub const PROVIDER_TRANSCRIPT_SCHEMA_VERSION: u32 = 1;
const ITEM_HASH_DOMAIN: &[u8] = b"bamboo/provider-transcript-item/v1\0";
const GROUP_HASH_DOMAIN: &[u8] = b"bamboo/provider-transcript-group/v1\0";
const ANCHOR_HASH_DOMAIN: &[u8] = b"bamboo/provider-transcript-anchor/v1\0";
const TRANSCRIPT_HASH_DOMAIN: &[u8] = b"bamboo/provider-transcript-state/v1\0";

/// Provider identity boundary for native transcript replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFamily {
    OpenAi,
    Anthropic,
    Copilot,
}

impl ProviderFamily {
    /// Resolve Bamboo's underlying provider type into a native replay family.
    /// Unknown/compatibility providers intentionally return `None`.
    pub fn from_provider_type(provider_type: Option<&str>) -> Option<Self> {
        match provider_type.map(str::trim) {
            Some("openai") => Some(Self::OpenAi),
            Some("anthropic") => Some(Self::Anthropic),
            Some("copilot") => Some(Self::Copilot),
            _ => None,
        }
    }
}

/// Concrete wire protocol whose items may be replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderProtocol {
    OpenAiResponsesV1,
    AnthropicMessages2023_06_01,
}

impl ProviderProtocol {
    pub fn supports_family(self, family: ProviderFamily) -> bool {
        match self {
            Self::OpenAiResponsesV1 => {
                matches!(family, ProviderFamily::OpenAi | ProviderFamily::Copilot)
            }
            Self::AnthropicMessages2023_06_01 => family == ProviderFamily::Anthropic,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTranscriptOrigin {
    Provider,
    HostToolSearch,
    DeveloperContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTranscriptAuthor {
    Model,
    Host,
    ToolResult,
}

/// The bounded provider item variants Bamboo is willing to replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTranscriptItemKind {
    OpenAiMessage,
    OpenAiReasoning,
    OpenAiFunctionCall,
    OpenAiToolSearchCall,
    OpenAiToolSearchOutput,
    OpenAiAdditionalTools,
    AnthropicText,
    AnthropicThinking,
    AnthropicRedactedThinking,
    AnthropicServerToolUse,
    AnthropicToolSearchToolResult,
    AnthropicToolUse,
    AnthropicToolResult,
}

impl ProviderTranscriptItemKind {
    pub fn is_discovery(self) -> bool {
        matches!(
            self,
            Self::OpenAiToolSearchCall
                | Self::OpenAiToolSearchOutput
                | Self::OpenAiAdditionalTools
                | Self::AnthropicServerToolUse
                | Self::AnthropicToolSearchToolResult
                | Self::AnthropicToolResult
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderTranscriptResetReason {
    ProviderSwitch,
    Compression,
    Rollback,
    HardTruncation,
    CacheScopeChanged,
    RetentionLimit,
    ExplicitHistoryRewrite,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProviderTranscriptError {
    #[error("provider transcript payload must be a JSON object")]
    PayloadNotObject,
    #[error("unsupported provider transcript item type")]
    UnsupportedItemType,
    #[error("invalid provider transcript item: {0}")]
    InvalidItem(&'static str),
    #[error("provider family and protocol do not match")]
    FamilyProtocolMismatch,
    #[error("provider transcript group must contain at least one item")]
    EmptyGroup,
    #[error("provider transcript group has no discovery item")]
    MissingDiscoveryItem,
    #[error("provider transcript group mixes provider families or protocols")]
    MixedProviderGroup,
    #[error("provider transcript group anchor is empty or missing")]
    InvalidAnchor,
    #[error("provider transcript group order is invalid")]
    InvalidGroupOrder,
    #[error("provider transcript state contains duplicate group ids")]
    DuplicateGroupId,
    #[error("provider transcript state contains an invalid current-epoch sequence")]
    InvalidStateSequence,
    #[error("provider transcript state contains a group from a future epoch")]
    FutureGroupEpoch,
    #[error("provider transcript state current epoch does not match its active family")]
    CurrentEpochFamilyMismatch,
    #[error("provider transcript item belongs to a non-active provider family")]
    InactiveProviderFamily,
}

/// One exact provider-owned item. `payload` stays private so callers cannot
/// bypass validation after construction.
#[derive(Clone, PartialEq, Serialize)]
pub struct ProviderTranscriptItem {
    family: ProviderFamily,
    protocol: ProviderProtocol,
    origin: ProviderTranscriptOrigin,
    author: ProviderTranscriptAuthor,
    kind: ProviderTranscriptItemKind,
    id: String,
    payload: Value,
}

#[derive(Deserialize)]
struct ProviderTranscriptItemWire {
    family: ProviderFamily,
    protocol: ProviderProtocol,
    origin: ProviderTranscriptOrigin,
    author: ProviderTranscriptAuthor,
    kind: ProviderTranscriptItemKind,
    id: String,
    payload: Value,
}

impl<'de> Deserialize<'de> for ProviderTranscriptItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderTranscriptItemWire::deserialize(deserializer)?;
        let item = Self::try_from_payload(
            wire.family,
            wire.protocol,
            wire.origin,
            wire.author,
            wire.payload,
        )
        .map_err(D::Error::custom)?;
        if item.kind != wire.kind || item.id != wire.id {
            return Err(D::Error::custom(
                "provider transcript kind/id does not match its validated payload",
            ));
        }
        Ok(item)
    }
}

impl ProviderTranscriptItem {
    pub fn try_from_payload(
        family: ProviderFamily,
        protocol: ProviderProtocol,
        origin: ProviderTranscriptOrigin,
        author: ProviderTranscriptAuthor,
        payload: Value,
    ) -> Result<Self, ProviderTranscriptError> {
        if !protocol.supports_family(family) {
            return Err(ProviderTranscriptError::FamilyProtocolMismatch);
        }
        let kind = infer_and_validate_item(protocol, origin, author, &payload)?;
        let id = stable_item_id(family, protocol, origin, author, kind, &payload)?;
        Ok(Self {
            family,
            protocol,
            origin,
            author,
            kind,
            id,
            payload,
        })
    }

    pub fn family(&self) -> ProviderFamily {
        self.family
    }

    pub fn protocol(&self) -> ProviderProtocol {
        self.protocol
    }

    pub fn origin(&self) -> ProviderTranscriptOrigin {
        self.origin
    }

    pub fn author(&self) -> ProviderTranscriptAuthor {
        self.author
    }

    pub fn kind(&self) -> ProviderTranscriptItemKind {
        self.kind
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn payload(&self) -> &Value {
        &self.payload
    }

    pub fn payload_sha256(&self) -> String {
        hash_json(ITEM_HASH_DOMAIN, &self.payload)
    }
}

/// Debug output is deliberately payload-free. Provider items may include tool
/// schemas, arguments, paths, or opaque provider fields.
impl fmt::Debug for ProviderTranscriptItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderTranscriptItem")
            .field("family", &self.family)
            .field("protocol", &self.protocol)
            .field("origin", &self.origin)
            .field("author", &self.author)
            .field("kind", &self.kind)
            .field("id", &self.id)
            .field("payload_sha256", &self.payload_sha256())
            .finish()
    }
}

/// Items that must survive or be removed together, anchored at one ordinary
/// transcript message. The anchor gives later provider adapters an exact
/// chronological insertion/replacement point without putting raw items inside
/// [`super::Message`].
#[derive(Clone, PartialEq, Serialize)]
pub struct ProviderTranscriptGroup {
    id: String,
    epoch: u64,
    sequence: u64,
    anchor_message_id: String,
    family: ProviderFamily,
    protocol: ProviderProtocol,
    items: Vec<ProviderTranscriptItem>,
}

#[derive(Deserialize)]
struct ProviderTranscriptGroupWire {
    id: String,
    epoch: u64,
    sequence: u64,
    anchor_message_id: String,
    family: ProviderFamily,
    protocol: ProviderProtocol,
    items: Vec<ProviderTranscriptItem>,
}

impl<'de> Deserialize<'de> for ProviderTranscriptGroup {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderTranscriptGroupWire::deserialize(deserializer)?;
        let group = Self::new(
            wire.epoch,
            wire.sequence,
            wire.anchor_message_id,
            Some(wire.id.as_str()),
            wire.items,
        )
        .map_err(D::Error::custom)?;
        if group.id != wire.id || group.family != wire.family || group.protocol != wire.protocol {
            return Err(D::Error::custom(
                "provider transcript group identity does not match its validated items",
            ));
        }
        Ok(group)
    }
}

impl ProviderTranscriptGroup {
    fn new(
        epoch: u64,
        sequence: u64,
        anchor_message_id: String,
        id_hint: Option<&str>,
        items: Vec<ProviderTranscriptItem>,
    ) -> Result<Self, ProviderTranscriptError> {
        if anchor_message_id.trim().is_empty() {
            return Err(ProviderTranscriptError::InvalidAnchor);
        }
        let Some(first) = items.first() else {
            return Err(ProviderTranscriptError::EmptyGroup);
        };
        let family = first.family;
        let protocol = first.protocol;
        if items
            .iter()
            .any(|item| item.family != family || item.protocol != protocol)
        {
            return Err(ProviderTranscriptError::MixedProviderGroup);
        }
        if !items.iter().any(|item| item.kind.is_discovery()) {
            return Err(ProviderTranscriptError::MissingDiscoveryItem);
        }
        validate_group_order(protocol, &items)?;

        let id = stable_group_id(epoch, family, protocol, &anchor_message_id, id_hint, &items);
        Ok(Self {
            id,
            epoch,
            sequence,
            anchor_message_id,
            family,
            protocol,
            items,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn anchor_message_id(&self) -> &str {
        &self.anchor_message_id
    }

    pub fn family(&self) -> ProviderFamily {
        self.family
    }

    pub fn protocol(&self) -> ProviderProtocol {
        self.protocol
    }

    pub fn items(&self) -> &[ProviderTranscriptItem] {
        &self.items
    }
}

impl fmt::Debug for ProviderTranscriptGroup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderTranscriptGroup")
            .field("id", &self.id)
            .field("epoch", &self.epoch)
            .field("sequence", &self.sequence)
            .field(
                "anchor_message_sha256",
                &hash_bytes(ANCHOR_HASH_DOMAIN, self.anchor_message_id.as_bytes()),
            )
            .field("family", &self.family)
            .field("protocol", &self.protocol)
            .field(
                "item_kinds",
                &self.items.iter().map(|item| item.kind).collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProviderTranscriptDiagnostics {
    pub group_count: usize,
    pub item_count: usize,
    pub serialized_bytes: usize,
    pub sha256: String,
}

/// Durable native transcript state. Only groups in the active family and
/// current epoch are replayable; earlier epochs remain persisted for audit and
/// normalized fallback but can never cross a provider switch.
#[derive(Clone, PartialEq, Serialize)]
pub struct ProviderTranscriptState {
    schema_version: u32,
    state_revision: u64,
    epoch: u64,
    next_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    active_family: Option<ProviderFamily>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    groups: Vec<ProviderTranscriptGroup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_reset_reason: Option<ProviderTranscriptResetReason>,
}

#[derive(Deserialize)]
struct ProviderTranscriptStateWire {
    #[serde(default)]
    schema_version: Option<u32>,
    #[serde(default)]
    state_revision: u64,
    #[serde(default)]
    epoch: u64,
    #[serde(default)]
    next_sequence: u64,
    #[serde(default)]
    active_family: Option<ProviderFamily>,
    #[serde(default)]
    groups: Vec<ProviderTranscriptGroup>,
    #[serde(default)]
    last_reset_reason: Option<ProviderTranscriptResetReason>,
}

impl<'de> Deserialize<'de> for ProviderTranscriptState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderTranscriptStateWire::deserialize(deserializer)?;
        let wire_is_empty = wire.state_revision == 0
            && wire.epoch == 0
            && wire.next_sequence == 0
            && wire.active_family.is_none()
            && wire.groups.is_empty()
            && wire.last_reset_reason.is_none();
        let schema_version = match wire.schema_version {
            Some(PROVIDER_TRANSCRIPT_SCHEMA_VERSION) => PROVIDER_TRANSCRIPT_SCHEMA_VERSION,
            Some(_) => {
                return Err(D::Error::custom(
                    "unsupported provider transcript schema version",
                ))
            }
            None if wire_is_empty => PROVIDER_TRANSCRIPT_SCHEMA_VERSION,
            None => {
                return Err(D::Error::custom(
                    "missing provider transcript schema version",
                ))
            }
        };
        let mut ids = HashSet::new();
        if wire
            .groups
            .iter()
            .any(|group| !ids.insert(group.id.clone()))
        {
            return Err(D::Error::custom(ProviderTranscriptError::DuplicateGroupId));
        }
        if wire.groups.iter().any(|group| group.epoch > wire.epoch) {
            return Err(D::Error::custom(ProviderTranscriptError::FutureGroupEpoch));
        }
        let mut current_sequences = HashSet::new();
        let mut current_max_sequence = None;
        for group in wire.groups.iter().filter(|group| group.epoch == wire.epoch) {
            if wire.active_family != Some(group.family) {
                return Err(D::Error::custom(
                    ProviderTranscriptError::CurrentEpochFamilyMismatch,
                ));
            }
            if !current_sequences.insert(group.sequence) {
                return Err(D::Error::custom(
                    ProviderTranscriptError::InvalidStateSequence,
                ));
            }
            current_max_sequence = Some(
                current_max_sequence
                    .map_or(group.sequence, |current: u64| current.max(group.sequence)),
            );
        }
        if current_max_sequence.is_some_and(|sequence| sequence >= wire.next_sequence) {
            return Err(D::Error::custom(
                ProviderTranscriptError::InvalidStateSequence,
            ));
        }
        Ok(Self {
            schema_version,
            state_revision: wire.state_revision,
            epoch: wire.epoch,
            next_sequence: wire.next_sequence,
            active_family: wire.active_family,
            groups: wire.groups,
            last_reset_reason: wire.last_reset_reason,
        })
    }
}

impl Default for ProviderTranscriptState {
    fn default() -> Self {
        Self {
            schema_version: PROVIDER_TRANSCRIPT_SCHEMA_VERSION,
            state_revision: 0,
            epoch: 0,
            next_sequence: 0,
            active_family: None,
            groups: Vec::new(),
            last_reset_reason: None,
        }
    }
}

impl fmt::Debug for ProviderTranscriptState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderTranscriptState")
            .field("schema_version", &self.schema_version)
            .field("state_revision", &self.state_revision)
            .field("epoch", &self.epoch)
            .field("next_sequence", &self.next_sequence)
            .field("active_family", &self.active_family)
            .field("groups", &self.groups)
            .field("last_reset_reason", &self.last_reset_reason)
            .finish()
    }
}

impl ProviderTranscriptState {
    pub fn is_empty(&self) -> bool {
        self.state_revision == 0
            && self.epoch == 0
            && self.next_sequence == 0
            && self.active_family.is_none()
            && self.groups.is_empty()
            && self.last_reset_reason.is_none()
    }

    pub fn state_revision(&self) -> u64 {
        self.state_revision
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn active_family(&self) -> Option<ProviderFamily> {
        self.active_family
    }

    pub fn last_reset_reason(&self) -> Option<ProviderTranscriptResetReason> {
        self.last_reset_reason
    }

    pub fn groups(&self) -> &[ProviderTranscriptGroup] {
        &self.groups
    }

    /// Select the provider family for the next request. A switch starts a new
    /// replay epoch, making all foreign/older raw items unreachable.
    pub fn activate_family(&mut self, family: ProviderFamily) -> bool {
        if self.active_family == Some(family) {
            return false;
        }
        let is_switch = self.active_family.is_some() || !self.groups.is_empty();
        if is_switch {
            self.epoch = self.epoch.saturating_add(1);
            self.next_sequence = 0;
            self.last_reset_reason = Some(ProviderTranscriptResetReason::ProviderSwitch);
        }
        self.active_family = Some(family);
        self.state_revision = self.state_revision.saturating_add(1);
        true
    }

    /// Leave native replay when a session moves to a provider family Bamboo
    /// cannot identify. Older items remain auditable but the new epoch has no
    /// family capable of replaying them.
    pub fn deactivate_family(&mut self) -> bool {
        if self.active_family.is_none() {
            return false;
        }
        self.epoch = self.epoch.saturating_add(1);
        self.next_sequence = 0;
        self.active_family = None;
        self.last_reset_reason = Some(ProviderTranscriptResetReason::ProviderSwitch);
        self.state_revision = self.state_revision.saturating_add(1);
        true
    }

    pub fn invalidate(&mut self, reason: ProviderTranscriptResetReason) {
        if self.is_empty() {
            return;
        }
        self.epoch = self.epoch.saturating_add(1);
        self.next_sequence = 0;
        self.last_reset_reason = Some(reason);
        self.state_revision = self.state_revision.saturating_add(1);
    }

    pub fn append_group(
        &mut self,
        anchor_message_id: impl Into<String>,
        id_hint: Option<&str>,
        items: Vec<ProviderTranscriptItem>,
    ) -> Result<String, ProviderTranscriptError> {
        let family = items
            .first()
            .ok_or(ProviderTranscriptError::EmptyGroup)?
            .family;
        // Validate the complete atomic group before mutating provider/epoch
        // state. A malformed provider frame must fail closed without leaving a
        // half-activated transcript lane behind.
        let group = ProviderTranscriptGroup::new(
            self.epoch,
            self.next_sequence,
            anchor_message_id.into(),
            id_hint,
            items,
        )?;
        if let Some(existing) = self.groups.iter().find(|current| current.id == group.id) {
            if existing.epoch == group.epoch
                && existing.anchor_message_id == group.anchor_message_id
                && existing.family == group.family
                && existing.protocol == group.protocol
                && existing.items == group.items
            {
                return Ok(group.id);
            }
            return Err(ProviderTranscriptError::DuplicateGroupId);
        }
        match self.active_family {
            Some(active) if active != family => {
                return Err(ProviderTranscriptError::InactiveProviderFamily)
            }
            None => {
                self.active_family = Some(family);
                self.state_revision = self.state_revision.saturating_add(1);
            }
            Some(_) => {}
        }
        let id = group.id.clone();
        self.groups.push(group);
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.state_revision = self.state_revision.saturating_add(1);
        Ok(id)
    }

    /// Drop whole groups whose ordinary-message anchor no longer exists. No
    /// item within a group is ever retained independently.
    pub fn prune_dangling_groups(&mut self, live_message_ids: &HashSet<String>) -> usize {
        let before = self.groups.len();
        self.groups
            .retain(|group| live_message_ids.contains(group.anchor_message_id()));
        let removed = before.saturating_sub(self.groups.len());
        if removed > 0 {
            self.state_revision = self.state_revision.saturating_add(1);
            self.last_reset_reason = Some(ProviderTranscriptResetReason::Rollback);
        }
        removed
    }

    pub fn replayable_groups(
        &self,
        family: ProviderFamily,
        protocol: ProviderProtocol,
    ) -> Vec<&ProviderTranscriptGroup> {
        if self.active_family != Some(family) || !protocol.supports_family(family) {
            return Vec::new();
        }
        let mut groups = self
            .groups
            .iter()
            .filter(|group| {
                group.epoch == self.epoch && group.family == family && group.protocol == protocol
            })
            .collect::<Vec<_>>();
        groups.sort_by_key(|group| group.sequence);
        groups
    }

    /// Append-safe merge used when a stale runner is reconciled with a durable
    /// prefix. Epoch boundaries outrank append revisions; within one family and
    /// epoch the higher revision wins. Missing compatible groups are retained,
    /// while a same-epoch foreign group falls back to its ordinary message.
    pub fn merge_durable_prefix(
        &mut self,
        durable: &ProviderTranscriptState,
        ordered_message_ids: &[String],
    ) -> usize {
        let live = self.clone();
        let durable_is_newer = durable.epoch > live.epoch
            || (durable.epoch == live.epoch && durable.state_revision >= live.state_revision);
        let mut merged = if durable_is_newer {
            durable.clone()
        } else {
            live.clone()
        };
        let live_message_ids = ordered_message_ids.iter().cloned().collect::<HashSet<_>>();
        let message_order = ordered_message_ids
            .iter()
            .enumerate()
            .map(|(index, id)| (id.as_str(), index))
            .collect::<HashMap<_, _>>();
        let mut seen = merged
            .groups
            .iter()
            .map(|group| group.id.clone())
            .collect::<HashSet<_>>();
        let mut added = 0usize;
        for group in durable.groups.iter().chain(live.groups.iter()) {
            if live_message_ids.contains(group.anchor_message_id()) && seen.insert(group.id.clone())
            {
                let group = group.clone();
                if group.epoch == merged.epoch {
                    if merged.active_family != Some(group.family) {
                        // Concurrent provider branches at the same numeric epoch
                        // cannot safely share raw state. Keep the durable ordinary
                        // message, but use its normalized fallback instead of
                        // importing a foreign native group into the winning epoch.
                        continue;
                    }
                } else if group.epoch > merged.epoch {
                    // The winning boundary is authoritative. A future group from
                    // a divergent snapshot cannot be rebased without changing
                    // the provider transcript's meaning.
                    continue;
                }
                merged.groups.push(group);
                added = added.saturating_add(1);
            }
        }
        merged.prune_dangling_groups(&live_message_ids);

        // Durable messages are the authoritative prefix and live-only messages
        // are appended after it. Rebuild the active epoch's sequence from that
        // exact message chronology instead of inheriting whichever concurrent
        // snapshot happened to win the revision tie.
        let before_normalization = merged.groups.clone();
        let mut historical = Vec::new();
        let mut current = Vec::new();
        for group in std::mem::take(&mut merged.groups) {
            if group.epoch == merged.epoch {
                current.push(group);
            } else {
                historical.push(group);
            }
        }
        current.sort_by(|left, right| {
            message_order
                .get(left.anchor_message_id())
                .copied()
                .unwrap_or(usize::MAX)
                .cmp(
                    &message_order
                        .get(right.anchor_message_id())
                        .copied()
                        .unwrap_or(usize::MAX),
                )
                .then_with(|| left.sequence.cmp(&right.sequence))
                .then_with(|| left.id.cmp(&right.id))
        });
        for (sequence, group) in current.iter_mut().enumerate() {
            group.sequence = sequence as u64;
        }
        merged.next_sequence = current.len() as u64;
        historical.extend(current);
        merged.groups = historical;
        let normalized = merged.groups != before_normalization;
        if added > 0 || normalized {
            merged.state_revision = merged.state_revision.saturating_add(1);
        }
        *self = merged;
        added
    }

    pub fn diagnostics(
        &self,
        family: ProviderFamily,
        protocol: ProviderProtocol,
    ) -> ProviderTranscriptDiagnostics {
        let groups = self.replayable_groups(family, protocol);
        let item_count = groups.iter().map(|group| group.items.len()).sum();
        let bytes = serde_json::to_vec(&groups).unwrap_or_default();
        ProviderTranscriptDiagnostics {
            group_count: groups.len(),
            item_count,
            serialized_bytes: bytes.len(),
            sha256: hash_bytes(TRANSCRIPT_HASH_DOMAIN, &bytes),
        }
    }
}

impl Session {
    pub fn activate_provider_transcript_family(&mut self, family: ProviderFamily) -> bool {
        let changed = self.provider_transcript.activate_family(family);
        if changed {
            self.updated_at = chrono::Utc::now();
        }
        changed
    }

    pub fn deactivate_provider_transcript_family(&mut self) -> bool {
        let changed = self.provider_transcript.deactivate_family();
        if changed {
            self.updated_at = chrono::Utc::now();
        }
        changed
    }

    pub fn append_provider_transcript_group(
        &mut self,
        anchor_message_id: impl Into<String>,
        id_hint: Option<&str>,
        items: Vec<ProviderTranscriptItem>,
    ) -> Result<String, ProviderTranscriptError> {
        let anchor_message_id = anchor_message_id.into();
        if !self
            .messages
            .iter()
            .any(|message| message.id == anchor_message_id)
        {
            return Err(ProviderTranscriptError::InvalidAnchor);
        }
        let id = self
            .provider_transcript
            .append_group(anchor_message_id, id_hint, items)?;
        self.updated_at = chrono::Utc::now();
        Ok(id)
    }

    pub fn prune_provider_transcript(&mut self) -> usize {
        let live_message_ids = self
            .messages
            .iter()
            .map(|message| message.id.clone())
            .collect::<HashSet<_>>();
        self.provider_transcript
            .prune_dangling_groups(&live_message_ids)
    }

    pub fn invalidate_provider_transcript(&mut self, reason: ProviderTranscriptResetReason) {
        self.provider_transcript.invalidate(reason);
        self.updated_at = chrono::Utc::now();
    }

    pub fn merge_provider_transcript_from_durable(&mut self, durable: &Session) -> usize {
        let ordered_message_ids = self
            .messages
            .iter()
            .map(|message| message.id.clone())
            .collect::<Vec<_>>();
        self.provider_transcript
            .merge_durable_prefix(&durable.provider_transcript, &ordered_message_ids)
    }
}

fn infer_and_validate_item(
    protocol: ProviderProtocol,
    origin: ProviderTranscriptOrigin,
    author: ProviderTranscriptAuthor,
    payload: &Value,
) -> Result<ProviderTranscriptItemKind, ProviderTranscriptError> {
    let object = payload
        .as_object()
        .ok_or(ProviderTranscriptError::PayloadNotObject)?;
    let item_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or(ProviderTranscriptError::InvalidItem("missing item type"))?;

    match protocol {
        ProviderProtocol::OpenAiResponsesV1 => {
            validate_openai_item(item_type, origin, author, payload)
        }
        ProviderProtocol::AnthropicMessages2023_06_01 => {
            validate_anthropic_item(item_type, origin, author, payload)
        }
    }
}

fn reject_unknown_fields(
    object: &serde_json::Map<String, Value>,
    allowed: &[&str],
    label: &'static str,
) -> Result<(), ProviderTranscriptError> {
    if object
        .keys()
        .any(|field| !allowed.contains(&field.as_str()))
    {
        return Err(ProviderTranscriptError::InvalidItem(label));
    }
    Ok(())
}

fn validate_allowed_callers(value: Option<&Value>) -> bool {
    value.is_none_or(|value| {
        value.as_array().is_some_and(|callers| {
            callers
                .iter()
                .all(|caller| matches!(caller.as_str(), Some("direct" | "programmatic")))
        })
    })
}

fn validate_openai_function_definition(
    object: &serde_json::Map<String, Value>,
) -> Result<(), ProviderTranscriptError> {
    reject_unknown_fields(
        object,
        &[
            "type",
            "name",
            "parameters",
            "strict",
            "allowed_callers",
            "defer_loading",
            "description",
            "output_schema",
        ],
        "function definition fields",
    )?;
    if object.get("type").and_then(Value::as_str) != Some("function")
        || !object
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| !name.trim().is_empty())
        || object
            .get("parameters")
            .is_some_and(|parameters| !parameters.is_object())
        || object
            .get("strict")
            .is_some_and(|strict| !strict.is_boolean())
        || object
            .get("defer_loading")
            .is_some_and(|deferred| !deferred.is_boolean())
        || object
            .get("description")
            .is_some_and(|description| !description.is_string())
        || object
            .get("output_schema")
            .is_some_and(|schema| !schema.is_object())
        || !validate_allowed_callers(object.get("allowed_callers"))
    {
        return Err(ProviderTranscriptError::InvalidItem(
            "function definition shape",
        ));
    }
    Ok(())
}

fn validate_openai_tool_definition(value: &Value) -> Result<(), ProviderTranscriptError> {
    let object = value
        .as_object()
        .ok_or(ProviderTranscriptError::InvalidItem("tool definition"))?;
    match object.get("type").and_then(Value::as_str) {
        Some("function") => validate_openai_function_definition(object),
        Some("namespace") => {
            reject_unknown_fields(
                object,
                &["type", "name", "description", "tools"],
                "namespace definition fields",
            )?;
            let tools = object
                .get("tools")
                .and_then(Value::as_array)
                .ok_or(ProviderTranscriptError::InvalidItem("namespace tools"))?;
            if !object
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| !name.trim().is_empty())
                || !object
                    .get("description")
                    .and_then(Value::as_str)
                    .is_some_and(|description| !description.trim().is_empty())
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "namespace definition shape",
                ));
            }
            for tool in tools {
                let nested = tool
                    .as_object()
                    .ok_or(ProviderTranscriptError::InvalidItem(
                        "namespace tool definition",
                    ))?;
                // Bamboo currently lowers canonical callable capabilities as
                // functions. Other provider tool variants must be explicitly
                // implemented before they can enter durable replay state.
                validate_openai_function_definition(nested)?;
            }
            Ok(())
        }
        _ => Err(ProviderTranscriptError::InvalidItem(
            "unsupported loaded tool definition",
        )),
    }
}

fn validate_openai_tool_definitions(value: Option<&Value>) -> Result<(), ProviderTranscriptError> {
    let tools = value
        .and_then(Value::as_array)
        .ok_or(ProviderTranscriptError::InvalidItem("loaded tools"))?;
    for tool in tools {
        validate_openai_tool_definition(tool)?;
    }
    Ok(())
}

fn is_nonempty_string(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
}

fn is_openai_item_status(value: Option<&Value>) -> bool {
    matches!(
        value.and_then(Value::as_str),
        Some("in_progress" | "completed" | "incomplete")
    )
}

fn validate_openai_agent(value: Option<&Value>) -> Result<(), ProviderTranscriptError> {
    let Some(value) = value else {
        return Ok(());
    };
    let agent = value
        .as_object()
        .ok_or(ProviderTranscriptError::InvalidItem("agent shape"))?;
    reject_unknown_fields(agent, &["agent_name"], "agent fields")?;
    if !is_nonempty_string(agent.get("agent_name")) {
        return Err(ProviderTranscriptError::InvalidItem("agent name"));
    }
    Ok(())
}

fn validate_openai_annotation(value: &Value) -> Result<(), ProviderTranscriptError> {
    let annotation = value
        .as_object()
        .ok_or(ProviderTranscriptError::InvalidItem("output annotation"))?;
    let string = |field| annotation.get(field).is_some_and(Value::is_string);
    let index = |field| annotation.get(field).and_then(Value::as_u64).is_some();
    match annotation.get("type").and_then(Value::as_str) {
        Some("file_citation") => {
            reject_unknown_fields(
                annotation,
                &["type", "file_id", "filename", "index"],
                "file citation fields",
            )?;
            if !string("file_id") || !string("filename") || !index("index") {
                return Err(ProviderTranscriptError::InvalidItem("file citation shape"));
            }
        }
        Some("url_citation") => {
            reject_unknown_fields(
                annotation,
                &["type", "start_index", "end_index", "title", "url"],
                "url citation fields",
            )?;
            if !index("start_index") || !index("end_index") || !string("title") || !string("url") {
                return Err(ProviderTranscriptError::InvalidItem("url citation shape"));
            }
        }
        Some("container_file_citation") => {
            reject_unknown_fields(
                annotation,
                &[
                    "type",
                    "container_id",
                    "start_index",
                    "end_index",
                    "file_id",
                    "filename",
                ],
                "container citation fields",
            )?;
            if !string("container_id")
                || !index("start_index")
                || !index("end_index")
                || !string("file_id")
                || !string("filename")
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "container citation shape",
                ));
            }
        }
        Some("file_path") => {
            reject_unknown_fields(
                annotation,
                &["type", "file_id", "index"],
                "file path fields",
            )?;
            if !string("file_id") || !index("index") {
                return Err(ProviderTranscriptError::InvalidItem("file path shape"));
            }
        }
        _ => {
            return Err(ProviderTranscriptError::InvalidItem(
                "unsupported output annotation",
            ))
        }
    }
    Ok(())
}

fn validate_openai_logprob_bytes(value: Option<&Value>) -> bool {
    value.and_then(Value::as_array).is_some_and(|bytes| {
        bytes
            .iter()
            .all(|byte| byte.as_u64().is_some_and(|byte| byte <= u8::MAX as u64))
    })
}

fn validate_openai_logprobs(value: Option<&Value>) -> Result<(), ProviderTranscriptError> {
    let Some(value) = value else {
        return Ok(());
    };
    let logprobs = value
        .as_array()
        .ok_or(ProviderTranscriptError::InvalidItem("output logprobs"))?;
    for logprob in logprobs {
        let logprob = logprob
            .as_object()
            .ok_or(ProviderTranscriptError::InvalidItem("output logprob"))?;
        reject_unknown_fields(
            logprob,
            &["token", "bytes", "logprob", "top_logprobs"],
            "output logprob fields",
        )?;
        let top_logprobs = logprob
            .get("top_logprobs")
            .and_then(Value::as_array)
            .ok_or(ProviderTranscriptError::InvalidItem("top logprobs"))?;
        if !logprob.get("token").is_some_and(Value::is_string)
            || !validate_openai_logprob_bytes(logprob.get("bytes"))
            || !logprob.get("logprob").is_some_and(Value::is_number)
        {
            return Err(ProviderTranscriptError::InvalidItem("output logprob shape"));
        }
        for top in top_logprobs {
            let top = top
                .as_object()
                .ok_or(ProviderTranscriptError::InvalidItem("top logprob"))?;
            reject_unknown_fields(top, &["token", "bytes", "logprob"], "top logprob fields")?;
            if !top.get("token").is_some_and(Value::is_string)
                || !validate_openai_logprob_bytes(top.get("bytes"))
                || !top.get("logprob").is_some_and(Value::is_number)
            {
                return Err(ProviderTranscriptError::InvalidItem("top logprob shape"));
            }
        }
    }
    Ok(())
}

fn validate_openai_message_content(value: &Value) -> Result<(), ProviderTranscriptError> {
    let content = value
        .as_array()
        .ok_or(ProviderTranscriptError::InvalidItem("message content"))?;
    for part in content {
        let part = part
            .as_object()
            .ok_or(ProviderTranscriptError::InvalidItem("message content part"))?;
        match part.get("type").and_then(Value::as_str) {
            Some("output_text") => {
                reject_unknown_fields(
                    part,
                    &["type", "text", "annotations", "logprobs"],
                    "output text fields",
                )?;
                let annotations = part
                    .get("annotations")
                    .and_then(Value::as_array)
                    .ok_or(ProviderTranscriptError::InvalidItem("output annotations"))?;
                if !part.get("text").is_some_and(Value::is_string) {
                    return Err(ProviderTranscriptError::InvalidItem("output text content"));
                }
                for annotation in annotations {
                    validate_openai_annotation(annotation)?;
                }
                validate_openai_logprobs(part.get("logprobs"))?;
            }
            Some("refusal") => {
                reject_unknown_fields(part, &["type", "refusal"], "refusal fields")?;
                if !part.get("refusal").is_some_and(Value::is_string) {
                    return Err(ProviderTranscriptError::InvalidItem("refusal content"));
                }
            }
            _ => {
                return Err(ProviderTranscriptError::InvalidItem(
                    "unsupported message content",
                ))
            }
        }
    }
    Ok(())
}

fn validate_openai_reasoning_parts(
    value: Option<&Value>,
    kind: &'static str,
    required: bool,
) -> Result<(), ProviderTranscriptError> {
    let Some(value) = value else {
        return if required {
            Err(ProviderTranscriptError::InvalidItem("reasoning parts"))
        } else {
            Ok(())
        };
    };
    let parts = value
        .as_array()
        .ok_or(ProviderTranscriptError::InvalidItem("reasoning parts"))?;
    for part in parts {
        let part = part
            .as_object()
            .ok_or(ProviderTranscriptError::InvalidItem("reasoning part"))?;
        reject_unknown_fields(part, &["type", "text"], "reasoning part fields")?;
        if part.get("type").and_then(Value::as_str) != Some(kind)
            || !part.get("text").is_some_and(Value::is_string)
        {
            return Err(ProviderTranscriptError::InvalidItem("reasoning part shape"));
        }
    }
    Ok(())
}

fn validate_openai_caller(value: Option<&Value>) -> Result<(), ProviderTranscriptError> {
    let Some(value) = value else {
        return Ok(());
    };
    let caller = value
        .as_object()
        .ok_or(ProviderTranscriptError::InvalidItem("function caller"))?;
    match caller.get("type").and_then(Value::as_str) {
        Some("direct") => reject_unknown_fields(caller, &["type"], "direct caller fields"),
        Some("program") => {
            reject_unknown_fields(caller, &["type", "caller_id"], "program caller fields")?;
            if !is_nonempty_string(caller.get("caller_id")) {
                return Err(ProviderTranscriptError::InvalidItem("program caller id"));
            }
            Ok(())
        }
        _ => Err(ProviderTranscriptError::InvalidItem(
            "unsupported function caller",
        )),
    }
}

fn validate_openai_item(
    item_type: &str,
    origin: ProviderTranscriptOrigin,
    author: ProviderTranscriptAuthor,
    payload: &Value,
) -> Result<ProviderTranscriptItemKind, ProviderTranscriptError> {
    let object = payload.as_object().expect("validated object");
    let require_nonempty_string = |field: &'static str| {
        object
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or(ProviderTranscriptError::InvalidItem(field))
    };
    let execution = || {
        object
            .get("execution")
            .and_then(Value::as_str)
            .filter(|value| matches!(*value, "server" | "client"))
            .ok_or(ProviderTranscriptError::InvalidItem("execution"))
    };

    match item_type {
        "message" if origin == ProviderTranscriptOrigin::Provider => {
            reject_unknown_fields(
                object,
                &["type", "id", "content", "role", "status", "agent", "phase"],
                "provider message fields",
            )?;
            if author != ProviderTranscriptAuthor::Model
                || !is_nonempty_string(object.get("id"))
                || object.get("role").and_then(Value::as_str) != Some("assistant")
                || !is_openai_item_status(object.get("status"))
                || object.get("phase").is_some_and(|phase| {
                    !matches!(phase.as_str(), Some("commentary" | "final_answer"))
                })
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "provider message shape",
                ));
            }
            validate_openai_agent(object.get("agent"))?;
            validate_openai_message_content(object.get("content").ok_or(
                ProviderTranscriptError::InvalidItem("provider message content"),
            )?)?;
            Ok(ProviderTranscriptItemKind::OpenAiMessage)
        }
        "reasoning" if origin == ProviderTranscriptOrigin::Provider => {
            reject_unknown_fields(
                object,
                &[
                    "type",
                    "id",
                    "summary",
                    "agent",
                    "content",
                    "encrypted_content",
                    "status",
                ],
                "reasoning fields",
            )?;
            if author != ProviderTranscriptAuthor::Model
                || !is_nonempty_string(object.get("id"))
                || object
                    .get("encrypted_content")
                    .is_some_and(|content| !content.is_string())
                || object
                    .get("status")
                    .is_some_and(|_| !is_openai_item_status(object.get("status")))
            {
                return Err(ProviderTranscriptError::InvalidItem("reasoning shape"));
            }
            validate_openai_agent(object.get("agent"))?;
            validate_openai_reasoning_parts(object.get("summary"), "summary_text", true)?;
            validate_openai_reasoning_parts(object.get("content"), "reasoning_text", false)?;
            Ok(ProviderTranscriptItemKind::OpenAiReasoning)
        }
        "function_call" if origin == ProviderTranscriptOrigin::Provider => {
            reject_unknown_fields(
                object,
                &[
                    "type",
                    "id",
                    "arguments",
                    "call_id",
                    "name",
                    "agent",
                    "caller",
                    "namespace",
                    "status",
                ],
                "function call fields",
            )?;
            require_nonempty_string("name")?;
            require_nonempty_string("call_id")?;
            let arguments = object.get("arguments").and_then(Value::as_str);
            if author != ProviderTranscriptAuthor::Model
                || object
                    .get("id")
                    .is_some_and(|_| !is_nonempty_string(object.get("id")))
                || object
                    .get("namespace")
                    .is_some_and(|_| !is_nonempty_string(object.get("namespace")))
                || object
                    .get("status")
                    .is_some_and(|_| !is_openai_item_status(object.get("status")))
                || arguments.is_none()
                || !arguments.is_some_and(|arguments| {
                    serde_json::from_str::<Value>(arguments)
                        .ok()
                        .is_some_and(|value| value.is_object())
                })
            {
                return Err(ProviderTranscriptError::InvalidItem("function call shape"));
            }
            validate_openai_agent(object.get("agent"))?;
            validate_openai_caller(object.get("caller"))?;
            Ok(ProviderTranscriptItemKind::OpenAiFunctionCall)
        }
        "tool_search_call" if origin == ProviderTranscriptOrigin::Provider => {
            reject_unknown_fields(
                object,
                &[
                    "type",
                    "id",
                    "arguments",
                    "call_id",
                    "execution",
                    "status",
                    "agent",
                    "created_by",
                ],
                "tool search call fields",
            )?;
            execution()?;
            require_nonempty_string("id")?;
            require_nonempty_string("call_id")?;
            if author != ProviderTranscriptAuthor::Model
                || object.get("status").and_then(Value::as_str) != Some("completed")
                || !object.get("arguments").is_some_and(Value::is_object)
                || object
                    .get("created_by")
                    .is_some_and(|_| !is_nonempty_string(object.get("created_by")))
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "tool search call shape",
                ));
            }
            validate_openai_agent(object.get("agent"))?;
            Ok(ProviderTranscriptItemKind::OpenAiToolSearchCall)
        }
        "tool_search_output" => {
            let execution = execution()?;
            if author != ProviderTranscriptAuthor::ToolResult
                || object.get("status").and_then(Value::as_str) != Some("completed")
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "tool search output shape",
                ));
            }
            validate_openai_tool_definitions(object.get("tools"))?;
            match (execution, origin) {
                ("server", ProviderTranscriptOrigin::Provider) => {
                    reject_unknown_fields(
                        object,
                        &[
                            "type",
                            "id",
                            "call_id",
                            "execution",
                            "status",
                            "tools",
                            "agent",
                            "created_by",
                        ],
                        "server tool search output fields",
                    )?;
                    require_nonempty_string("id")?;
                    require_nonempty_string("call_id")?;
                    if object
                        .get("created_by")
                        .is_some_and(|_| !is_nonempty_string(object.get("created_by")))
                    {
                        return Err(ProviderTranscriptError::InvalidItem(
                            "tool search output created_by",
                        ));
                    }
                    validate_openai_agent(object.get("agent"))?;
                }
                ("client", ProviderTranscriptOrigin::HostToolSearch) => {
                    reject_unknown_fields(
                        object,
                        &["type", "call_id", "execution", "status", "tools"],
                        "client tool search output fields",
                    )?;
                    require_nonempty_string("call_id")?;
                }
                _ => {
                    return Err(ProviderTranscriptError::InvalidItem(
                        "tool search output origin",
                    ))
                }
            }
            Ok(ProviderTranscriptItemKind::OpenAiToolSearchOutput)
        }
        "additional_tools" if origin == ProviderTranscriptOrigin::DeveloperContext => {
            reject_unknown_fields(
                object,
                &["type", "role", "tools"],
                "additional tools fields",
            )?;
            if author != ProviderTranscriptAuthor::Host
                || object.get("role").and_then(Value::as_str) != Some("developer")
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "additional tools shape",
                ));
            }
            validate_openai_tool_definitions(object.get("tools"))?;
            Ok(ProviderTranscriptItemKind::OpenAiAdditionalTools)
        }
        _ => Err(ProviderTranscriptError::UnsupportedItemType),
    }
}

fn validate_anthropic_caller(value: Option<&Value>) -> Result<(), ProviderTranscriptError> {
    let Some(value) = value else {
        return Ok(());
    };
    let caller = value
        .as_object()
        .ok_or(ProviderTranscriptError::InvalidItem("Anthropic caller"))?;
    match caller.get("type").and_then(Value::as_str) {
        Some("direct") => reject_unknown_fields(caller, &["type"], "direct caller fields"),
        Some("code_execution_20250825" | "code_execution_20260120" | "code_execution_20260521") => {
            reject_unknown_fields(caller, &["type", "tool_id"], "server caller fields")?;
            if !is_nonempty_string(caller.get("tool_id")) {
                return Err(ProviderTranscriptError::InvalidItem(
                    "server caller tool id",
                ));
            }
            Ok(())
        }
        _ => Err(ProviderTranscriptError::InvalidItem(
            "unsupported Anthropic caller",
        )),
    }
}

fn is_string_or_null(value: Option<&Value>) -> bool {
    value.is_some_and(|value| value.is_string() || value.is_null())
}

fn validate_anthropic_text_citation(value: &Value) -> Result<(), ProviderTranscriptError> {
    let citation = value
        .as_object()
        .ok_or(ProviderTranscriptError::InvalidItem("text citation"))?;
    let string = |field| citation.get(field).is_some_and(Value::is_string);
    let index = |field| citation.get(field).and_then(Value::as_u64).is_some();
    match citation.get("type").and_then(Value::as_str) {
        Some("char_location") => {
            reject_unknown_fields(
                citation,
                &[
                    "type",
                    "cited_text",
                    "document_index",
                    "document_title",
                    "start_char_index",
                    "end_char_index",
                    "file_id",
                ],
                "char citation fields",
            )?;
            if !string("cited_text")
                || !index("document_index")
                || !is_string_or_null(citation.get("document_title"))
                || !index("start_char_index")
                || !index("end_char_index")
                || !is_string_or_null(citation.get("file_id"))
            {
                return Err(ProviderTranscriptError::InvalidItem("char citation shape"));
            }
        }
        Some("page_location") => {
            reject_unknown_fields(
                citation,
                &[
                    "type",
                    "cited_text",
                    "document_index",
                    "document_title",
                    "start_page_number",
                    "end_page_number",
                    "file_id",
                ],
                "page citation fields",
            )?;
            if !string("cited_text")
                || !index("document_index")
                || !is_string_or_null(citation.get("document_title"))
                || !index("start_page_number")
                || !index("end_page_number")
                || !is_string_or_null(citation.get("file_id"))
            {
                return Err(ProviderTranscriptError::InvalidItem("page citation shape"));
            }
        }
        Some("content_block_location") => {
            reject_unknown_fields(
                citation,
                &[
                    "type",
                    "cited_text",
                    "document_index",
                    "document_title",
                    "start_block_index",
                    "end_block_index",
                    "file_id",
                ],
                "content citation fields",
            )?;
            if !string("cited_text")
                || !index("document_index")
                || !is_string_or_null(citation.get("document_title"))
                || !index("start_block_index")
                || !index("end_block_index")
                || !is_string_or_null(citation.get("file_id"))
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "content citation shape",
                ));
            }
        }
        Some("web_search_result_location") => {
            reject_unknown_fields(
                citation,
                &["type", "cited_text", "encrypted_index", "title", "url"],
                "web citation fields",
            )?;
            if !string("cited_text")
                || !string("encrypted_index")
                || !is_string_or_null(citation.get("title"))
                || !string("url")
            {
                return Err(ProviderTranscriptError::InvalidItem("web citation shape"));
            }
        }
        Some("search_result_location") => {
            reject_unknown_fields(
                citation,
                &[
                    "type",
                    "cited_text",
                    "start_block_index",
                    "end_block_index",
                    "search_result_index",
                    "source",
                    "title",
                ],
                "search citation fields",
            )?;
            if !string("cited_text")
                || !index("start_block_index")
                || !index("end_block_index")
                || !index("search_result_index")
                || !string("source")
                || !is_string_or_null(citation.get("title"))
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "search citation shape",
                ));
            }
        }
        _ => {
            return Err(ProviderTranscriptError::InvalidItem(
                "unsupported text citation",
            ))
        }
    }
    Ok(())
}

fn validate_anthropic_text_citations(value: Option<&Value>) -> Result<(), ProviderTranscriptError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.is_null() {
        return Ok(());
    }
    let citations = value
        .as_array()
        .ok_or(ProviderTranscriptError::InvalidItem("text citations"))?;
    for citation in citations {
        validate_anthropic_text_citation(citation)?;
    }
    Ok(())
}

fn validate_anthropic_item(
    item_type: &str,
    origin: ProviderTranscriptOrigin,
    author: ProviderTranscriptAuthor,
    payload: &Value,
) -> Result<ProviderTranscriptItemKind, ProviderTranscriptError> {
    let object = payload.as_object().expect("validated object");
    let nonempty = |field: &'static str| {
        object
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or(ProviderTranscriptError::InvalidItem(field))
    };
    match item_type {
        "text" if origin == ProviderTranscriptOrigin::Provider => {
            reject_unknown_fields(object, &["type", "text", "citations"], "text block fields")?;
            if author != ProviderTranscriptAuthor::Model
                || !object.get("text").is_some_and(Value::is_string)
            {
                return Err(ProviderTranscriptError::InvalidItem("text block"));
            }
            validate_anthropic_text_citations(object.get("citations"))?;
            Ok(ProviderTranscriptItemKind::AnthropicText)
        }
        "thinking" if origin == ProviderTranscriptOrigin::Provider => {
            reject_unknown_fields(
                object,
                &["type", "thinking", "signature"],
                "thinking block fields",
            )?;
            if author != ProviderTranscriptAuthor::Model
                || !object.get("thinking").is_some_and(Value::is_string)
                || !object
                    .get("signature")
                    .and_then(Value::as_str)
                    .is_some_and(|signature| !signature.trim().is_empty())
            {
                return Err(ProviderTranscriptError::InvalidItem("thinking block"));
            }
            Ok(ProviderTranscriptItemKind::AnthropicThinking)
        }
        "redacted_thinking" if origin == ProviderTranscriptOrigin::Provider => {
            reject_unknown_fields(object, &["type", "data"], "redacted thinking fields")?;
            if author != ProviderTranscriptAuthor::Model
                || !object.get("data").is_some_and(Value::is_string)
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "redacted thinking block",
                ));
            }
            Ok(ProviderTranscriptItemKind::AnthropicRedactedThinking)
        }
        "server_tool_use" if origin == ProviderTranscriptOrigin::Provider => {
            reject_unknown_fields(
                object,
                &["type", "id", "name", "input", "caller"],
                "server tool use fields",
            )?;
            nonempty("id")?;
            let name = nonempty("name")?;
            if author != ProviderTranscriptAuthor::Model
                || !matches!(name, "tool_search_tool_regex" | "tool_search_tool_bm25")
                || !object.get("input").is_some_and(Value::is_object)
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "tool-search server_tool_use block",
                ));
            }
            validate_anthropic_caller(object.get("caller"))?;
            Ok(ProviderTranscriptItemKind::AnthropicServerToolUse)
        }
        "tool_search_tool_result" if origin == ProviderTranscriptOrigin::Provider => {
            reject_unknown_fields(
                object,
                &["type", "tool_use_id", "content"],
                "tool search result fields",
            )?;
            nonempty("tool_use_id")?;
            if author != ProviderTranscriptAuthor::ToolResult {
                return Err(ProviderTranscriptError::InvalidItem(
                    "tool search result author",
                ));
            }
            validate_anthropic_search_result_content(object.get("content"))?;
            Ok(ProviderTranscriptItemKind::AnthropicToolSearchToolResult)
        }
        "tool_use" if origin == ProviderTranscriptOrigin::Provider => {
            reject_unknown_fields(
                object,
                &["type", "id", "name", "input", "caller", "toolset_name"],
                "tool use fields",
            )?;
            nonempty("id")?;
            nonempty("name")?;
            if author != ProviderTranscriptAuthor::Model
                || !object.get("input").is_some_and(Value::is_object)
                || object.get("toolset_name").is_some_and(|toolset| {
                    !(toolset.is_null()
                        || toolset.as_str().is_some_and(|name| !name.trim().is_empty()))
                })
            {
                return Err(ProviderTranscriptError::InvalidItem("tool_use block"));
            }
            validate_anthropic_caller(object.get("caller"))?;
            Ok(ProviderTranscriptItemKind::AnthropicToolUse)
        }
        "tool_result" if origin == ProviderTranscriptOrigin::HostToolSearch => {
            reject_unknown_fields(
                object,
                &["type", "tool_use_id", "content", "is_error"],
                "custom tool result fields",
            )?;
            nonempty("tool_use_id")?;
            let Some(content) = object.get("content").and_then(Value::as_array) else {
                return Err(ProviderTranscriptError::InvalidItem(
                    "custom tool search result content",
                ));
            };
            if author != ProviderTranscriptAuthor::ToolResult
                || object
                    .get("is_error")
                    .is_some_and(|is_error| is_error.as_bool().is_none_or(|is_error| is_error))
                || content.iter().any(|reference| {
                    let Some(reference) = reference.as_object() else {
                        return true;
                    };
                    reference.len() != 2
                        || reference.get("type").and_then(Value::as_str) != Some("tool_reference")
                        || !reference
                            .get("tool_name")
                            .and_then(Value::as_str)
                            .is_some_and(|name| !name.trim().is_empty())
                })
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "custom tool references",
                ));
            }
            Ok(ProviderTranscriptItemKind::AnthropicToolResult)
        }
        _ => Err(ProviderTranscriptError::UnsupportedItemType),
    }
}

fn validate_anthropic_search_result_content(
    content: Option<&Value>,
) -> Result<(), ProviderTranscriptError> {
    let Some(content) = content.and_then(Value::as_object) else {
        return Err(ProviderTranscriptError::InvalidItem(
            "tool search result content",
        ));
    };
    match content.get("type").and_then(Value::as_str) {
        Some("tool_search_tool_search_result") => {
            reject_unknown_fields(
                content,
                &["type", "tool_references"],
                "tool reference result fields",
            )?;
            let Some(references) = content.get("tool_references").and_then(Value::as_array) else {
                return Err(ProviderTranscriptError::InvalidItem("tool references"));
            };
            if references.iter().any(|reference| {
                let Some(reference) = reference.as_object() else {
                    return true;
                };
                reference.len() != 2
                    || reference.get("type").and_then(Value::as_str) != Some("tool_reference")
                    || !reference
                        .get("tool_name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| !name.trim().is_empty())
            }) {
                return Err(ProviderTranscriptError::InvalidItem("tool reference"));
            }
            Ok(())
        }
        Some("tool_search_tool_result_error") => {
            reject_unknown_fields(
                content,
                &["type", "error_code", "error_message"],
                "tool search error fields",
            )?;
            if !matches!(
                content.get("error_code").and_then(Value::as_str),
                Some(
                    "invalid_tool_input"
                        | "unavailable"
                        | "too_many_requests"
                        | "execution_time_exceeded"
                )
            ) || !is_string_or_null(content.get("error_message"))
            {
                return Err(ProviderTranscriptError::InvalidItem(
                    "tool search error shape",
                ));
            }
            Ok(())
        }
        _ => Err(ProviderTranscriptError::InvalidItem(
            "tool search result content type",
        )),
    }
}

fn validate_group_order(
    protocol: ProviderProtocol,
    items: &[ProviderTranscriptItem],
) -> Result<(), ProviderTranscriptError> {
    match protocol {
        ProviderProtocol::OpenAiResponsesV1 => {
            let mut seen_item_ids = HashSet::<&str>::new();
            let mut seen_search_call_ids = HashSet::<&str>::new();
            let mut seen_search_outputs = HashSet::<(&str, &str)>::new();
            let mut pending_provider_calls = HashSet::<&str>::new();
            let mut pending_client_calls = HashSet::<&str>::new();
            let mut loaded_at = HashMap::<String, usize>::new();
            let mut function_calls = Vec::<(usize, &str)>::new();
            for (index, item) in items.iter().enumerate() {
                if let Some(id) = item.payload.get("id").and_then(Value::as_str) {
                    if !seen_item_ids.insert(id) {
                        return Err(ProviderTranscriptError::InvalidGroupOrder);
                    }
                }
                match item.kind {
                    ProviderTranscriptItemKind::OpenAiToolSearchCall => {
                        let execution = item
                            .payload
                            .get("execution")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let call_id = item
                            .payload
                            .get("call_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        if !seen_search_call_ids.insert(call_id) {
                            return Err(ProviderTranscriptError::InvalidGroupOrder);
                        }
                        if execution == "server" {
                            pending_provider_calls.insert(call_id);
                        } else {
                            pending_client_calls.insert(call_id);
                        }
                    }
                    ProviderTranscriptItemKind::OpenAiToolSearchOutput => {
                        let execution = item
                            .payload
                            .get("execution")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let call_id = item
                            .payload
                            .get("call_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        if !seen_search_outputs.insert((execution, call_id)) {
                            return Err(ProviderTranscriptError::InvalidGroupOrder);
                        }
                        let pending = if execution == "server" {
                            &mut pending_provider_calls
                        } else {
                            &mut pending_client_calls
                        };
                        if !pending.remove(call_id)
                            && (execution == "server"
                                || item.origin != ProviderTranscriptOrigin::HostToolSearch)
                        {
                            // A client output can be appended in a later host
                            // group after the original model call was committed.
                            // Hosted/server outputs must always complete a call
                            // in this same provider-owned atomic response.
                            return Err(ProviderTranscriptError::InvalidGroupOrder);
                        }
                        for name in openai_loaded_tool_names(&item.payload) {
                            loaded_at.entry(name).or_insert(index);
                        }
                    }
                    ProviderTranscriptItemKind::OpenAiFunctionCall => {
                        let name = item
                            .payload
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        function_calls.push((index, name));
                    }
                    _ => {}
                }
            }
            if !pending_provider_calls.is_empty() {
                return Err(ProviderTranscriptError::InvalidGroupOrder);
            }
            let mut matched_loaded_call = false;
            for (index, name) in &function_calls {
                if let Some(output_index) = loaded_at.get(*name) {
                    if index <= output_index {
                        return Err(ProviderTranscriptError::InvalidGroupOrder);
                    }
                    matched_loaded_call = true;
                }
            }
            if !loaded_at.is_empty() && !function_calls.is_empty() && !matched_loaded_call {
                return Err(ProviderTranscriptError::InvalidGroupOrder);
            }
        }
        ProviderProtocol::AnthropicMessages2023_06_01 => {
            let mut server_ids = HashSet::new();
            let mut completed_server_ids = HashSet::new();
            let mut referenced_at = HashMap::<String, usize>::new();
            let mut tool_uses = Vec::<(usize, &str)>::new();
            for (index, item) in items.iter().enumerate() {
                match item.kind {
                    ProviderTranscriptItemKind::AnthropicServerToolUse => {
                        if let Some(id) = item.payload.get("id").and_then(Value::as_str) {
                            if !server_ids.insert(id.to_string()) {
                                return Err(ProviderTranscriptError::InvalidGroupOrder);
                            }
                        }
                    }
                    ProviderTranscriptItemKind::AnthropicToolSearchToolResult => {
                        let tool_use_id = item
                            .payload
                            .get("tool_use_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        if !server_ids.contains(tool_use_id)
                            || !completed_server_ids.insert(tool_use_id.to_string())
                        {
                            return Err(ProviderTranscriptError::InvalidGroupOrder);
                        }
                        if let Some(references) = item
                            .payload
                            .get("content")
                            .and_then(|content| content.get("tool_references"))
                            .and_then(Value::as_array)
                        {
                            for reference in references {
                                if let Some(name) =
                                    reference.get("tool_name").and_then(Value::as_str)
                                {
                                    referenced_at.entry(name.to_string()).or_insert(index);
                                }
                            }
                        }
                    }
                    ProviderTranscriptItemKind::AnthropicToolUse => {
                        let name = item
                            .payload
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        tool_uses.push((index, name));
                    }
                    _ => {}
                }
            }
            if completed_server_ids.len() != server_ids.len() {
                return Err(ProviderTranscriptError::InvalidGroupOrder);
            }
            let mut matched_reference_use = false;
            for (index, name) in &tool_uses {
                if let Some(result_index) = referenced_at.get(*name) {
                    if index <= result_index {
                        return Err(ProviderTranscriptError::InvalidGroupOrder);
                    }
                    matched_reference_use = true;
                }
            }
            if !referenced_at.is_empty() && !tool_uses.is_empty() && !matched_reference_use {
                return Err(ProviderTranscriptError::InvalidGroupOrder);
            }
        }
    }
    Ok(())
}

fn openai_loaded_tool_names(payload: &Value) -> Vec<String> {
    let Some(tools) = payload.get("tools").and_then(Value::as_array) else {
        return Vec::new();
    };
    let mut names = Vec::new();
    for tool in tools {
        match tool.get("type").and_then(Value::as_str) {
            Some("function") => {
                if let Some(name) = tool.get("name").and_then(Value::as_str) {
                    names.push(name.to_string());
                }
            }
            Some("namespace") => {
                let namespace = tool.get("name").and_then(Value::as_str).unwrap_or_default();
                if let Some(functions) = tool.get("tools").and_then(Value::as_array) {
                    for function in functions {
                        if let Some(name) = function.get("name").and_then(Value::as_str) {
                            names.push(name.to_string());
                            names.push(format!("{namespace}.{name}"));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    names
}

fn stable_item_id(
    family: ProviderFamily,
    protocol: ProviderProtocol,
    origin: ProviderTranscriptOrigin,
    author: ProviderTranscriptAuthor,
    kind: ProviderTranscriptItemKind,
    payload: &Value,
) -> Result<String, ProviderTranscriptError> {
    if !payload.is_object() {
        return Err(ProviderTranscriptError::PayloadNotObject);
    }
    let identity = serde_json::json!({
        "family": family,
        "protocol": protocol,
        "origin": origin,
        "author": author,
        "kind": kind,
        "payload": payload,
    });
    let digest = hash_json(ITEM_HASH_DOMAIN, &identity);
    // Provider ids can be attacker-controlled and may contain prompt or secret
    // bytes. Persist/log only the structural kind plus a one-way payload hash.
    Ok(format!("pti_{kind:?}_{digest}"))
}

fn stable_group_id(
    epoch: u64,
    family: ProviderFamily,
    protocol: ProviderProtocol,
    anchor_message_id: &str,
    _id_hint: Option<&str>,
    items: &[ProviderTranscriptItem],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(GROUP_HASH_DOMAIN);
    hasher.update(epoch.to_be_bytes());
    hasher.update([0]);
    hasher.update(serde_json::to_vec(&family).unwrap_or_default());
    hasher.update([0]);
    hasher.update(serde_json::to_vec(&protocol).unwrap_or_default());
    hasher.update([0]);
    hasher.update(anchor_message_id.as_bytes());
    hasher.update([0]);
    for item in items {
        hasher.update([0]);
        hasher.update(item.id.as_bytes());
    }
    format!("ptg_{}", hex::encode(hasher.finalize()))
}

fn hash_json(domain: &[u8], value: &Value) -> String {
    hash_bytes(
        domain,
        &serde_json::to_vec(&canonical_json(value)).unwrap_or_default(),
    )
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), canonical_json(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        _ => value.clone(),
    }
}

fn hash_bytes(domain: &[u8], bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::Message;

    fn openai_output_items() -> Vec<ProviderTranscriptItem> {
        [
            (
                ProviderTranscriptAuthor::Model,
                json!({
                    "type": "tool_search_call",
                    "id": "tsc_1",
                    "execution": "server",
                    "call_id": "search_1",
                    "status": "completed",
                    "arguments": {"paths": ["crm"]}
                }),
            ),
            (
                ProviderTranscriptAuthor::ToolResult,
                json!({
                    "type": "tool_search_output",
                    "id": "tso_1",
                    "execution": "server",
                    "call_id": "search_1",
                    "status": "completed",
                    "tools": [{"type": "function", "name": "list_open_orders"}]
                }),
            ),
            (
                ProviderTranscriptAuthor::Model,
                json!({
                    "type": "function_call",
                    "call_id": "call_abc123",
                    "name": "list_open_orders",
                    "arguments": "{\"customer_id\":\"CUST-12345\"}"
                }),
            ),
        ]
        .into_iter()
        .map(|(author, payload)| {
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                author,
                payload,
            )
            .unwrap()
        })
        .collect()
    }

    fn anthropic_items() -> Vec<ProviderTranscriptItem> {
        [
            (
                ProviderTranscriptAuthor::Model,
                json!({"type":"text","text":"I will search."}),
            ),
            (
                ProviderTranscriptAuthor::Model,
                json!({
                    "type":"server_tool_use",
                    "id":"srvtoolu_01ABC123",
                    "name":"tool_search_tool_regex",
                    "input":{"pattern":"weather"}
                }),
            ),
            (
                ProviderTranscriptAuthor::ToolResult,
                json!({
                    "type":"tool_search_tool_result",
                    "tool_use_id":"srvtoolu_01ABC123",
                    "content":{
                        "type":"tool_search_tool_search_result",
                        "tool_references":[
                            {"type":"tool_reference","tool_name":"get_weather"}
                        ]
                    }
                }),
            ),
            (
                ProviderTranscriptAuthor::Model,
                json!({
                    "type":"tool_use",
                    "id":"toolu_01XYZ789",
                    "name":"get_weather",
                    "input":{"location":"San Francisco"}
                }),
            ),
        ]
        .into_iter()
        .map(|(author, payload)| {
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::Anthropic,
                ProviderProtocol::AnthropicMessages2023_06_01,
                ProviderTranscriptOrigin::Provider,
                author,
                payload,
            )
            .unwrap()
        })
        .collect()
    }

    fn assert_payload_rejected_without_leak(
        family: ProviderFamily,
        protocol: ProviderProtocol,
        origin: ProviderTranscriptOrigin,
        author: ProviderTranscriptAuthor,
        payload: Value,
    ) {
        let error =
            ProviderTranscriptItem::try_from_payload(family, protocol, origin, author, payload)
                .expect_err("payload must fail closed");
        let diagnostic = format!("{error:?} {error}");
        assert!(!diagnostic.contains("UNKNOWN_FIELD_SENTINEL"));
    }

    fn assert_unknown_top_level_rejected(item: &ProviderTranscriptItem) {
        let mut payload = item.payload().clone();
        payload
            .as_object_mut()
            .unwrap()
            .insert("unknown".to_string(), json!("UNKNOWN_FIELD_SENTINEL"));
        assert_payload_rejected_without_leak(
            item.family(),
            item.protocol(),
            item.origin(),
            item.author(),
            payload,
        );
    }

    #[test]
    fn openai_search_items_round_trip_in_exact_order() {
        let items = openai_output_items();
        let encoded = serde_json::to_string(&items).unwrap();
        let decoded: Vec<ProviderTranscriptItem> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, items);
        assert_eq!(
            decoded
                .iter()
                .map(ProviderTranscriptItem::kind)
                .collect::<Vec<_>>(),
            vec![
                ProviderTranscriptItemKind::OpenAiToolSearchCall,
                ProviderTranscriptItemKind::OpenAiToolSearchOutput,
                ProviderTranscriptItemKind::OpenAiFunctionCall,
            ]
        );
    }

    #[test]
    fn anthropic_reference_chain_round_trips_without_schema_expansion() {
        let items = anthropic_items();
        let encoded = serde_json::to_value(&items).unwrap();
        let decoded: Vec<ProviderTranscriptItem> = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, items);
        assert_eq!(
            decoded[2].payload()["content"]["tool_references"][0],
            json!({"type":"tool_reference","tool_name":"get_weather"})
        );
    }

    #[test]
    fn client_output_and_additional_tools_are_position_safe_variants() {
        let client = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::HostToolSearch,
            ProviderTranscriptAuthor::ToolResult,
            json!({
                "type":"tool_search_output",
                "execution":"client",
                "call_id":"call_abc123",
                "status":"completed",
                "tools":[]
            }),
        )
        .unwrap();
        let additional = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::DeveloperContext,
            ProviderTranscriptAuthor::Host,
            json!({"type":"additional_tools","role":"developer","tools":[]}),
        )
        .unwrap();
        assert_eq!(
            client.kind(),
            ProviderTranscriptItemKind::OpenAiToolSearchOutput
        );
        assert_eq!(
            additional.kind(),
            ProviderTranscriptItemKind::OpenAiAdditionalTools
        );
    }

    #[test]
    fn unsupported_or_malformed_items_fail_closed() {
        let unsupported = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({"type":"arbitrary_json","secret":"do-not-replay"}),
        );
        assert!(matches!(
            unsupported,
            Err(ProviderTranscriptError::UnsupportedItemType)
        ));

        let malformed = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::ToolResult,
            json!({
                "type":"tool_search_tool_result",
                "tool_use_id":"srvtoolu_1",
                "content":{"type":"tool_search_tool_search_result","tool_references":[
                    {"type":"not_a_reference","tool_name":"danger"}
                ]}
            }),
        );
        assert!(malformed.is_err());

        let mut reversed = openai_output_items();
        reversed.swap(0, 1);
        let group = ProviderTranscriptGroup::new(0, 0, "anchor".to_string(), None, reversed);
        assert_eq!(
            group.unwrap_err(),
            ProviderTranscriptError::InvalidGroupOrder
        );
    }

    #[test]
    fn nested_capability_and_content_shapes_fail_closed_without_payload_leaks() {
        let cases = [
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::Model,
                json!({"type":"reasoning","secret":"REASONING_SENTINEL"}),
            ),
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::ToolResult,
                json!({
                    "type":"tool_search_output","id":"tso_invalid","execution":"server",
                    "call_id":"search_invalid",
                    "status":"completed","tools":[42],"secret":"TOOLS_SENTINEL"
                }),
            ),
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::DeveloperContext,
                ProviderTranscriptAuthor::Host,
                json!({
                    "type":"additional_tools","role":"developer",
                    "tools":[{"type":"future_capability","secret":"CAP_SENTINEL"}]
                }),
            ),
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::Model,
                json!({
                    "type":"message","id":"msg_invalid","role":"assistant","status":"completed",
                    "content":[{"type":"future_content","secret":"CONTENT_SENTINEL"}]
                }),
            ),
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::Anthropic,
                ProviderProtocol::AnthropicMessages2023_06_01,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::Model,
                json!({"type":"thinking","thinking":"private","secret":"THINK_SENTINEL"}),
            ),
        ];
        for result in cases {
            let error = result.expect_err("unsupported nested shapes must fail closed");
            let diagnostic = format!("{error:?} {error}");
            for sentinel in [
                "REASONING_SENTINEL",
                "TOOLS_SENTINEL",
                "CAP_SENTINEL",
                "CONTENT_SENTINEL",
                "THINK_SENTINEL",
            ] {
                assert!(!diagnostic.contains(sentinel));
            }
        }

        let reasoning = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "id":"rs_1","type":"reasoning","status":"completed",
                "summary":[{"type":"summary_text","text":"bounded summary"}],
                "encrypted_content":"opaque"
            }),
        )
        .unwrap();
        assert_eq!(
            reasoning.kind(),
            ProviderTranscriptItemKind::OpenAiReasoning
        );

        let thinking = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({"type":"thinking","thinking":"private","signature":"signed"}),
        )
        .unwrap();
        assert_eq!(
            thinking.kind(),
            ProviderTranscriptItemKind::AnthropicThinking
        );
    }

    #[test]
    fn every_supported_payload_layer_rejects_unknown_fields() {
        let mut baselines = openai_output_items();
        baselines.extend(anthropic_items());
        baselines.extend([
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::Model,
                json!({
                    "type":"message","id":"msg_1","role":"assistant","status":"completed",
                    "content":[{"type":"output_text","text":"done","annotations":[]}]
                }),
            )
            .unwrap(),
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::Model,
                json!({
                    "type":"reasoning","id":"rs_1","status":"completed",
                    "summary":[{"type":"summary_text","text":"safe"}]
                }),
            )
            .unwrap(),
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::HostToolSearch,
                ProviderTranscriptAuthor::ToolResult,
                json!({
                    "type":"tool_search_output","execution":"client","call_id":"search_client",
                    "status":"completed","tools":[]
                }),
            )
            .unwrap(),
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::DeveloperContext,
                ProviderTranscriptAuthor::Host,
                json!({"type":"additional_tools","role":"developer","tools":[]}),
            )
            .unwrap(),
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::Anthropic,
                ProviderProtocol::AnthropicMessages2023_06_01,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::Model,
                json!({"type":"thinking","thinking":"private","signature":"opaque"}),
            )
            .unwrap(),
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::Anthropic,
                ProviderProtocol::AnthropicMessages2023_06_01,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::Model,
                json!({"type":"redacted_thinking","data":"opaque"}),
            )
            .unwrap(),
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::Anthropic,
                ProviderProtocol::AnthropicMessages2023_06_01,
                ProviderTranscriptOrigin::HostToolSearch,
                ProviderTranscriptAuthor::ToolResult,
                json!({
                    "type":"tool_result","tool_use_id":"toolu_search","is_error":false,
                    "content":[{"type":"tool_reference","tool_name":"get_weather"}]
                }),
            )
            .unwrap(),
        ]);
        for baseline in &baselines {
            assert_unknown_top_level_rejected(baseline);
        }

        assert_payload_rejected_without_leak(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"message","id":"msg_nested","role":"assistant","status":"completed",
                "content":[{
                    "type":"output_text","text":"safe","annotations":[],
                    "unknown":"UNKNOWN_FIELD_SENTINEL"
                }]
            }),
        );
        assert_payload_rejected_without_leak(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"message","id":"msg_annotation","role":"assistant","status":"completed",
                "content":[{
                    "type":"output_text","text":"safe","annotations":[{
                        "type":"file_citation","file_id":"file_1","filename":"safe.txt","index":0,
                        "unknown":"UNKNOWN_FIELD_SENTINEL"
                    }]
                }]
            }),
        );
        assert_payload_rejected_without_leak(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"message","id":"msg_logprob","role":"assistant","status":"completed",
                "content":[{
                    "type":"output_text","text":"safe","annotations":[],"logprobs":[{
                        "token":"safe","bytes":[115],"logprob":-0.1,"top_logprobs":[],
                        "unknown":"UNKNOWN_FIELD_SENTINEL"
                    }]
                }]
            }),
        );
        assert_payload_rejected_without_leak(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"reasoning","id":"rs_nested","summary":[{
                    "type":"summary_text","text":"safe","unknown":"UNKNOWN_FIELD_SENTINEL"
                }]
            }),
        );
        assert_payload_rejected_without_leak(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::ToolResult,
            json!({
                "type":"tool_search_output","id":"tso_nested","execution":"server",
                "call_id":"search_nested","status":"completed","tools":[{
                    "type":"function","name":"safe_tool","unknown":"UNKNOWN_FIELD_SENTINEL"
                }]
            }),
        );
        assert_payload_rejected_without_leak(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"tool_search_call","id":"tsc_agent","execution":"client",
                "call_id":"search_agent","status":"completed","arguments":{},
                "agent":{"agent_name":"safe","unknown":"UNKNOWN_FIELD_SENTINEL"}
            }),
        );
        assert_payload_rejected_without_leak(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"text","text":"safe","citations":[{
                    "type":"web_search_result_location","cited_text":"safe",
                    "encrypted_index":"opaque","title":"safe","url":"https://example.invalid",
                    "unknown":"UNKNOWN_FIELD_SENTINEL"
                }]
            }),
        );
        assert_payload_rejected_without_leak(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"tool_use","id":"toolu_caller","name":"safe_tool","input":{},
                "caller":{"type":"direct","unknown":"UNKNOWN_FIELD_SENTINEL"}
            }),
        );
        assert_payload_rejected_without_leak(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::ToolResult,
            json!({
                "type":"tool_search_tool_result","tool_use_id":"srvtoolu_1",
                "content":{
                    "type":"tool_search_tool_search_result","tool_references":[
                        {"type":"tool_reference","tool_name":"safe_tool"}
                    ],
                    "unknown":"UNKNOWN_FIELD_SENTINEL"
                }
            }),
        );
        assert_payload_rejected_without_leak(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::ToolResult,
            json!({
                "type":"tool_search_tool_result","tool_use_id":"srvtoolu_1",
                "content":{
                    "type":"tool_search_tool_search_result","tool_references":[{
                        "type":"tool_reference","tool_name":"safe_tool",
                        "unknown":"UNKNOWN_FIELD_SENTINEL"
                    }]
                }
            }),
        );
        assert_payload_rejected_without_leak(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::ToolResult,
            json!({
                "type":"tool_search_tool_result","tool_use_id":"srvtoolu_1",
                "content":{
                    "type":"tool_search_tool_result_error","error_code":"unavailable",
                    "error_message":null,"unknown":"UNKNOWN_FIELD_SENTINEL"
                }
            }),
        );
    }

    #[test]
    fn hosted_discovery_chains_reject_dangling_reordered_and_mismatched_uses() {
        let openai = openai_output_items();
        let mut reordered = openai.clone();
        reordered.swap(1, 2);
        assert_eq!(
            ProviderTranscriptGroup::new(0, 0, "openai".to_string(), None, reordered).unwrap_err(),
            ProviderTranscriptError::InvalidGroupOrder
        );
        assert_eq!(
            ProviderTranscriptGroup::new(
                0,
                0,
                "openai".to_string(),
                None,
                vec![openai[0].clone()],
            )
            .unwrap_err(),
            ProviderTranscriptError::InvalidGroupOrder
        );
        let mismatched_call = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"function_call","call_id":"call_other",
                "name":"not_loaded","arguments":"{}"
            }),
        )
        .unwrap();
        assert_eq!(
            ProviderTranscriptGroup::new(
                0,
                0,
                "openai".to_string(),
                None,
                vec![openai[0].clone(), openai[1].clone(), mismatched_call],
            )
            .unwrap_err(),
            ProviderTranscriptError::InvalidGroupOrder
        );

        let hosted_item = |author, payload| {
            ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                author,
                payload,
            )
            .unwrap()
        };
        let call_two = hosted_item(
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"tool_search_call","id":"tsc_2","execution":"server",
                "call_id":"search_2","status":"completed","arguments":{"query":"support"}
            }),
        );
        let output_two = hosted_item(
            ProviderTranscriptAuthor::ToolResult,
            json!({
                "type":"tool_search_output","id":"tso_2","execution":"server",
                "call_id":"search_2","status":"completed",
                "tools":[{"type":"function","name":"list_support_cases"}]
            }),
        );
        let function_two = hosted_item(
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"function_call","id":"fc_2","call_id":"function_2",
                "name":"list_support_cases","arguments":"{}","status":"completed"
            }),
        );
        let parallel = vec![
            openai[0].clone(),
            call_two.clone(),
            openai[1].clone(),
            output_two.clone(),
            openai[2].clone(),
            function_two,
        ];
        assert!(
            ProviderTranscriptGroup::new(0, 0, "openai-parallel".to_string(), None, parallel,)
                .is_ok()
        );

        let mismatched_output = hosted_item(
            ProviderTranscriptAuthor::ToolResult,
            json!({
                "type":"tool_search_output","id":"tso_mismatch","execution":"server",
                "call_id":"search_missing","status":"completed","tools":[]
            }),
        );
        assert_eq!(
            ProviderTranscriptGroup::new(
                0,
                0,
                "openai-mismatch".to_string(),
                None,
                vec![openai[0].clone(), mismatched_output],
            )
            .unwrap_err(),
            ProviderTranscriptError::InvalidGroupOrder
        );

        let duplicate_id_call = hosted_item(
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"tool_search_call","id":"tsc_1","execution":"server",
                "call_id":"search_duplicate_id","status":"completed","arguments":{}
            }),
        );
        assert_eq!(
            ProviderTranscriptGroup::new(
                0,
                0,
                "openai-duplicate-id".to_string(),
                None,
                vec![openai[0].clone(), duplicate_id_call],
            )
            .unwrap_err(),
            ProviderTranscriptError::InvalidGroupOrder
        );

        let duplicate_call_id = hosted_item(
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"tool_search_call","id":"tsc_duplicate_call","execution":"server",
                "call_id":"search_1","status":"completed","arguments":{}
            }),
        );
        assert_eq!(
            ProviderTranscriptGroup::new(
                0,
                0,
                "openai-duplicate-call".to_string(),
                None,
                vec![openai[0].clone(), duplicate_call_id],
            )
            .unwrap_err(),
            ProviderTranscriptError::InvalidGroupOrder
        );
        for malformed in [
            json!({
                "type":"tool_search_call","execution":"server","call_id":"search_missing_id",
                "status":"completed","arguments":{}
            }),
            json!({
                "type":"tool_search_call","id":"tsc_missing_call","execution":"server",
                "status":"completed","arguments":{}
            }),
        ] {
            assert!(ProviderTranscriptItem::try_from_payload(
                ProviderFamily::OpenAi,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::Model,
                malformed,
            )
            .is_err());
        }

        // Client execution intentionally stops after the call. Its host output
        // is committed in a later input group and may therefore stand alone.
        let client_call = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"tool_search_call","id":"tsc_client_1","execution":"client","call_id":"search_1",
                "status":"completed","arguments":{"query":"weather"}
            }),
        )
        .unwrap();
        assert!(ProviderTranscriptGroup::new(
            0,
            0,
            "client-call".to_string(),
            None,
            vec![client_call],
        )
        .is_ok());

        let anthropic = anthropic_items();
        let mut reordered = anthropic.clone();
        reordered.swap(2, 3);
        assert_eq!(
            ProviderTranscriptGroup::new(0, 0, "anthropic".to_string(), None, reordered)
                .unwrap_err(),
            ProviderTranscriptError::InvalidGroupOrder
        );
        assert_eq!(
            ProviderTranscriptGroup::new(
                0,
                0,
                "anthropic".to_string(),
                None,
                vec![anthropic[1].clone()],
            )
            .unwrap_err(),
            ProviderTranscriptError::InvalidGroupOrder
        );
        let mismatched_use = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::Anthropic,
            ProviderProtocol::AnthropicMessages2023_06_01,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({"type":"tool_use","id":"tool_other","name":"not_loaded","input":{}}),
        )
        .unwrap();
        assert_eq!(
            ProviderTranscriptGroup::new(
                0,
                0,
                "anthropic".to_string(),
                None,
                vec![anthropic[1].clone(), anthropic[2].clone(), mismatched_use,],
            )
            .unwrap_err(),
            ProviderTranscriptError::InvalidGroupOrder
        );
    }

    #[test]
    fn stable_ids_are_canonical_and_isolated_by_provider_identity() {
        let payload_a: Value = serde_json::from_str(
            r#"{"type":"tool_search_call","id":"tsc_stable","execution":"client","call_id":"search_1","status":"completed","arguments":{"b":2,"a":1}}"#,
        )
        .unwrap();
        let payload_b: Value = serde_json::from_str(
            r#"{"arguments":{"a":1,"b":2},"status":"completed","call_id":"search_1","execution":"client","id":"tsc_stable","type":"tool_search_call"}"#,
        )
        .unwrap();
        let item = |family, payload| {
            ProviderTranscriptItem::try_from_payload(
                family,
                ProviderProtocol::OpenAiResponsesV1,
                ProviderTranscriptOrigin::Provider,
                ProviderTranscriptAuthor::Model,
                payload,
            )
            .unwrap()
        };
        let openai_a = item(ProviderFamily::OpenAi, payload_a);
        let openai_b = item(ProviderFamily::OpenAi, payload_b);
        let copilot = item(ProviderFamily::Copilot, openai_a.payload().clone());
        assert_eq!(openai_a.id(), openai_b.id());
        assert_ne!(openai_a.id(), copilot.id());

        let openai_group =
            ProviderTranscriptGroup::new(0, 0, "anchor".to_string(), None, vec![openai_a]).unwrap();
        let copilot_group =
            ProviderTranscriptGroup::new(0, 0, "anchor".to_string(), None, vec![copilot]).unwrap();
        assert_ne!(openai_group.id(), copilot_group.id());

        let mut tampered = serde_json::to_value(openai_b).unwrap();
        tampered["id"] = json!("pti_tampered");
        assert!(serde_json::from_value::<ProviderTranscriptItem>(tampered).is_err());
    }

    #[test]
    fn session_serialization_preserves_groups_and_old_sessions_default_empty() {
        let mut session = Session::new("session-native", "gpt-5.6");
        let assistant = Message::assistant("", None);
        let anchor = assistant.id.clone();
        session.add_message(assistant);
        session.activate_provider_transcript_family(ProviderFamily::OpenAi);
        session
            .append_provider_transcript_group(&anchor, Some("resp_123"), openai_output_items())
            .unwrap();

        let encoded = serde_json::to_string(&session).unwrap();
        let decoded: Session = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.provider_transcript, session.provider_transcript);
        assert_eq!(
            decoded
                .provider_transcript
                .replayable_groups(ProviderFamily::OpenAi, ProviderProtocol::OpenAiResponsesV1)
                .len(),
            1
        );
        let mut compressed = decoded;
        compressed.reset_model_context_epoch(crate::session::ModelContextResetReason::Compression);
        assert!(compressed
            .provider_transcript
            .replayable_groups(ProviderFamily::OpenAi, ProviderProtocol::OpenAiResponsesV1)
            .is_empty());
        assert_eq!(compressed.provider_transcript.groups().len(), 1);

        let mut old = serde_json::to_value(Session::new("old", "model")).unwrap();
        old.as_object_mut().unwrap().remove("provider_transcript");
        let old: Session = serde_json::from_value(old).unwrap();
        assert!(old.provider_transcript.is_empty());
    }

    #[test]
    fn persisted_state_rejects_future_epochs_and_reused_current_sequences() {
        let mut session = Session::new("session-native", "gpt-5.6");
        let assistant = Message::assistant("", None);
        let anchor = assistant.id.clone();
        session.add_message(assistant);
        session
            .append_provider_transcript_group(&anchor, None, openai_output_items())
            .unwrap();

        let mut future = serde_json::to_value(&session).unwrap();
        future["provider_transcript"]["groups"][0]["epoch"] = json!(1);
        assert!(serde_json::from_value::<Session>(future).is_err());

        let mut reused = serde_json::to_value(&session).unwrap();
        reused["provider_transcript"]["next_sequence"] = json!(0);
        assert!(serde_json::from_value::<Session>(reused).is_err());

        let mut missing_version = serde_json::to_value(&session).unwrap();
        missing_version["provider_transcript"]
            .as_object_mut()
            .unwrap()
            .remove("schema_version");
        assert!(serde_json::from_value::<Session>(missing_version).is_err());

        let mut future_version = serde_json::to_value(&session).unwrap();
        future_version["provider_transcript"]["schema_version"] =
            json!(PROVIDER_TRANSCRIPT_SCHEMA_VERSION + 1);
        assert!(serde_json::from_value::<Session>(future_version).is_err());

        let explicitly_empty: ProviderTranscriptState = serde_json::from_value(json!({})).unwrap();
        assert!(explicitly_empty.is_empty());
    }

    #[test]
    fn rollback_prunes_the_whole_atomic_group() {
        let mut session = Session::new("session-native", "claude");
        let assistant = Message::assistant("", None);
        let anchor = assistant.id.clone();
        session.add_message(assistant);
        session.activate_provider_transcript_family(ProviderFamily::Anthropic);
        session
            .append_provider_transcript_group(&anchor, None, anthropic_items())
            .unwrap();
        session.messages.clear();

        assert_eq!(session.prune_provider_transcript(), 1);
        assert!(session.provider_transcript.groups().is_empty());
        assert_eq!(
            session.provider_transcript.last_reset_reason(),
            Some(ProviderTranscriptResetReason::Rollback)
        );
    }

    #[test]
    fn explicit_history_rewrite_invalidates_groups_even_when_anchors_survive() {
        let mut session = Session::new("session-native", "gpt-5.6");
        let assistant = Message::assistant("before edit", None);
        let anchor = assistant.id.clone();
        session.add_message(assistant);
        session
            .append_provider_transcript_group(&anchor, None, openai_output_items())
            .unwrap();
        let previous_epoch = session.provider_transcript.epoch();

        session.messages[0].content = "after edit".to_string();
        session.reset_model_context_epoch(
            crate::session::ModelContextResetReason::ExplicitHistoryRewrite,
        );

        assert_eq!(session.provider_transcript.groups().len(), 1);
        assert_eq!(session.provider_transcript.epoch(), previous_epoch + 1);
        assert!(session
            .provider_transcript
            .replayable_groups(ProviderFamily::OpenAi, ProviderProtocol::OpenAiResponsesV1)
            .is_empty());
        assert_eq!(
            session.provider_transcript.last_reset_reason(),
            Some(ProviderTranscriptResetReason::ExplicitHistoryRewrite)
        );
    }

    #[test]
    fn durable_and_runner_native_groups_merge_append_safely() {
        let mut base = Session::new("session-native", "gpt-5.6");
        let first = Message::assistant("first", None);
        let first_id = first.id.clone();
        let second = Message::assistant("second", None);
        let second_id = second.id.clone();
        base.add_message(first);
        base.add_message(second);
        let mut durable = base.clone();
        durable
            .append_provider_transcript_group(&first_id, None, openai_output_items())
            .unwrap();
        let mut runner = base;
        runner
            .append_provider_transcript_group(&second_id, None, openai_output_items())
            .unwrap();

        crate::session::append_missing_runtime_messages(&mut runner, &durable);
        assert_eq!(runner.provider_transcript.groups().len(), 2);
        assert_eq!(
            runner
                .provider_transcript
                .replayable_groups(ProviderFamily::OpenAi, ProviderProtocol::OpenAiResponsesV1)
                .into_iter()
                .map(ProviderTranscriptGroup::anchor_message_id)
                .collect::<Vec<_>>(),
            vec![first_id.as_str(), second_id.as_str()]
        );
    }

    #[test]
    fn equal_revision_merge_is_deterministic_for_same_anchor_branches() {
        let assistant = Message::assistant("anchor", None);
        let anchor = assistant.id.clone();
        let mut base = Session::new("session-native", "gpt-5.6");
        base.add_message(assistant);

        let mut left = base.clone();
        left.append_provider_transcript_group(&anchor, None, openai_output_items())
            .unwrap();
        let mut right_items = openai_output_items();
        right_items[0] = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({
                "type":"tool_search_call","id":"tsc_support","execution":"server","call_id":"search_1",
                "status":"completed","arguments":{"paths":["support"]}
            }),
        )
        .unwrap();
        let mut right = base;
        right
            .append_provider_transcript_group(&anchor, None, right_items)
            .unwrap();

        let left_original = left.provider_transcript.clone();
        let right_original = right.provider_transcript.clone();
        let ordered = vec![anchor];
        left.provider_transcript
            .merge_durable_prefix(&right_original, &ordered);
        right
            .provider_transcript
            .merge_durable_prefix(&left_original, &ordered);
        assert_eq!(left.provider_transcript, right.provider_transcript);
        assert_eq!(
            left.provider_transcript
                .replayable_groups(ProviderFamily::OpenAi, ProviderProtocol::OpenAiResponsesV1)
                .len(),
            2
        );
    }

    #[test]
    fn rejected_append_is_atomic_and_reset_reason_survives_new_epoch_append() {
        let mut state = ProviderTranscriptState::default();
        state.activate_family(ProviderFamily::Anthropic);
        let before = state.clone();
        assert_eq!(
            state
                .append_group("anchor", None, openai_output_items())
                .unwrap_err(),
            ProviderTranscriptError::InactiveProviderFamily
        );
        assert_eq!(state, before, "rejected append must not mutate state");

        let first = Message::assistant("openai", None);
        let first_anchor = first.id.clone();
        let second = Message::assistant("anthropic", None);
        let second_anchor = second.id.clone();
        let mut session = Session::new("switch-audit", "model");
        session.add_message(first);
        session.add_message(second);
        session
            .append_provider_transcript_group(&first_anchor, None, openai_output_items())
            .unwrap();
        session.activate_provider_transcript_family(ProviderFamily::Anthropic);
        session
            .append_provider_transcript_group(&second_anchor, None, anthropic_items())
            .unwrap();
        let round_trip: Session =
            serde_json::from_str(&serde_json::to_string(&session).unwrap()).unwrap();
        assert_eq!(
            round_trip.provider_transcript.last_reset_reason(),
            Some(ProviderTranscriptResetReason::ProviderSwitch)
        );
        assert_eq!(
            round_trip
                .provider_transcript
                .replayable_groups(
                    ProviderFamily::Anthropic,
                    ProviderProtocol::AnthropicMessages2023_06_01,
                )
                .len(),
            1
        );
    }

    #[test]
    fn append_merge_never_overwrites_a_newer_provider_epoch_with_more_old_groups() {
        let mut base = Session::new("session-native", "model");
        let anchors = (0..3)
            .map(|index| {
                let assistant = Message::assistant(format!("assistant {index}"), None);
                let anchor = assistant.id.clone();
                base.add_message(assistant);
                anchor
            })
            .collect::<Vec<_>>();
        let mut durable = base.clone();
        durable
            .append_provider_transcript_group(&anchors[0], None, openai_output_items())
            .unwrap();
        durable
            .append_provider_transcript_group(&anchors[1], None, openai_output_items())
            .unwrap();

        let mut runner = base;
        runner
            .append_provider_transcript_group(&anchors[2], None, openai_output_items())
            .unwrap();
        runner.activate_provider_transcript_family(ProviderFamily::Anthropic);
        let switched_epoch = runner.provider_transcript.epoch();

        crate::session::append_missing_runtime_messages(&mut runner, &durable);
        assert_eq!(runner.provider_transcript.epoch(), switched_epoch);
        assert_eq!(
            runner.provider_transcript.active_family(),
            Some(ProviderFamily::Anthropic)
        );
        assert_eq!(
            runner.provider_transcript.last_reset_reason(),
            Some(ProviderTranscriptResetReason::ProviderSwitch)
        );
        assert!(runner
            .provider_transcript
            .replayable_groups(ProviderFamily::OpenAi, ProviderProtocol::OpenAiResponsesV1)
            .is_empty());
    }

    #[test]
    fn provider_switch_starts_a_new_epoch_and_filters_foreign_json() {
        let mut session = Session::new("session-native", "model");
        let assistant = Message::assistant("", None);
        let anchor = assistant.id.clone();
        session.add_message(assistant);
        session.activate_provider_transcript_family(ProviderFamily::OpenAi);
        session
            .append_provider_transcript_group(&anchor, None, openai_output_items())
            .unwrap();
        let old_epoch = session.provider_transcript.epoch();

        assert!(session.activate_provider_transcript_family(ProviderFamily::Anthropic));
        assert_eq!(session.provider_transcript.epoch(), old_epoch + 1);
        assert_eq!(
            session.provider_transcript.last_reset_reason(),
            Some(ProviderTranscriptResetReason::ProviderSwitch)
        );
        assert!(session
            .provider_transcript
            .replayable_groups(ProviderFamily::OpenAi, ProviderProtocol::OpenAiResponsesV1)
            .is_empty());
    }

    #[test]
    fn debug_and_diagnostics_never_emit_raw_payload() {
        let secret = "credential-do-not-log";
        let item = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::DeveloperContext,
            ProviderTranscriptAuthor::Host,
            json!({
                "type":"additional_tools",
                "role":"developer",
                "tools":[{"type":"function","name":"x","description":secret}]
            }),
        )
        .unwrap();
        assert!(!format!("{item:?}").contains(secret));
        let group =
            ProviderTranscriptGroup::new(0, 0, secret.to_string(), None, vec![item]).unwrap();
        assert!(!format!("{group:?}").contains(secret));

        let error = ProviderTranscriptItem::try_from_payload(
            ProviderFamily::OpenAi,
            ProviderProtocol::OpenAiResponsesV1,
            ProviderTranscriptOrigin::Provider,
            ProviderTranscriptAuthor::Model,
            json!({"type":secret}),
        )
        .unwrap_err();
        assert!(!format!("{error:?} {error}").contains(secret));
    }
}
