//! Shared permission posture resolution and bounded audit metadata.
//!
//! The session-scoped mode is the user's explicit request. The configured mode
//! is the process-wide default. Keeping both values in one resolution prevents
//! a global `Auto` default from silently reinterpreting an explicit legacy
//! `Bypass`, while still letting a default session inherit global `Auto` at
//! every execution boundary.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::SessionPermissionMode;

pub const PERMISSION_AUDIT_REVISION_KEY: &str = "permission.audit_revision";
pub const PERMISSION_POLICY_REVISION_KEY: &str = "permission.policy_revision";
pub const PERMISSION_REQUESTED_MODE_KEY: &str = "permission.requested_mode";
pub const PERMISSION_EFFECTIVE_MODE_KEY: &str = "permission.effective_mode";
pub const PERMISSION_EXECUTOR_MAPPING_KEY: &str = "permission.executor_mapping";
pub const PERMISSION_TRANSITIONED_AT_KEY: &str = "permission.transitioned_at";

/// Complete bounded audit group copied atomically with a typed permission mode.
pub const PERMISSION_AUDIT_METADATA_KEYS: &[&str] = &[
    PERMISSION_AUDIT_REVISION_KEY,
    PERMISSION_POLICY_REVISION_KEY,
    PERMISSION_REQUESTED_MODE_KEY,
    PERMISSION_EFFECTIVE_MODE_KEY,
    PERMISSION_EXECUTOR_MAPPING_KEY,
    PERMISSION_TRANSITIONED_AT_KEY,
];

/// Maximum executor-mapping label accepted at a trust boundary.
pub const MAX_PERMISSION_EXECUTOR_MAPPING_CHARS: usize = 128;
/// Exclusive upper bound for comparable audit revisions. Values at or above
/// this boundary are malformed; allocation also fails before it would emit the
/// last representable value and lose strict monotonicity on the next write.
pub const MAX_PERMISSION_AUDIT_REVISION: u64 = i64::MAX as u64;
const MAX_PERMISSION_TRANSITION_TIMESTAMP_CHARS: usize = 64;
static AUDIT_REVISION_CLOCK: AtomicU64 = AtomicU64::new(0);

/// Permission mode controlling how the system handles permission requests.
///
/// This type lives in the domain crate so the evaluator, engine, actor
/// transport and external executors share the exact same precedence resolver.
/// `bamboo_config::PermissionMode` remains a public re-export of this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PermissionMode {
    /// Default interactive mode: prompt for all dangerous operations.
    #[default]
    Default,
    /// Plan (read-only) mode: deny all mutating tool calls, allow read-only tools.
    Plan,
    /// Accept edits mode: auto-approve file writes, prompt for command execution.
    AcceptEdits,
    /// Don't ask mode: auto-deny unless pre-approved by whitelist.
    DontAsk,
    /// Skip ordinary approval checks. Forced confirmations remain possible.
    BypassPermissions,
    /// Never emit an approval prompt. Hard policy denials remain enforced.
    Auto,
}

impl PermissionMode {
    pub fn description(self) -> &'static str {
        match self {
            Self::Default => "Interactive mode: prompt for dangerous operations",
            Self::Plan => "Plan mode: read-only, no mutations allowed",
            Self::AcceptEdits => "Accept edits: auto-approve file writes",
            Self::DontAsk => "Don't ask: auto-deny unless whitelisted",
            Self::BypassPermissions => {
                "Bypass: skip ordinary checks; forced confirmations still apply"
            }
            Self::Auto => "Auto: never request approval; hard denials still apply",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Plan => "plan",
            Self::AcceptEdits => "accept_edits",
            Self::DontAsk => "dont_ask",
            Self::BypassPermissions => "bypass",
            Self::Auto => "auto",
        }
    }

    pub fn from_audit_str(value: &str) -> Option<Self> {
        match value {
            "default" => Some(Self::Default),
            "plan" => Some(Self::Plan),
            "accept_edits" => Some(Self::AcceptEdits),
            "dont_ask" => Some(Self::DontAsk),
            "bypass" => Some(Self::BypassPermissions),
            "auto" => Some(Self::Auto),
            _ => None,
        }
    }
}

/// Requested session posture plus the effective configured behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermissionModeResolution {
    pub requested: SessionPermissionMode,
    pub effective: PermissionMode,
}

impl PermissionModeResolution {
    pub fn new(requested: SessionPermissionMode, configured: PermissionMode) -> Self {
        // Plan/read-only is an authorization boundary, not an approval mode.
        // A session Auto request may remove approval prompts, but must never
        // turn a configured Plan gate into a mutating executor posture.
        let effective = match (configured, requested) {
            (PermissionMode::Plan, _) => PermissionMode::Plan,
            (_, SessionPermissionMode::Auto) => PermissionMode::Auto,
            (_, SessionPermissionMode::Bypass) => PermissionMode::BypassPermissions,
            (_, SessionPermissionMode::Default) => configured,
        };
        Self {
            requested,
            effective,
        }
    }

    pub fn bypass_permissions(self) -> bool {
        self.effective == PermissionMode::BypassPermissions
    }

    pub fn auto_approve_permissions(self) -> bool {
        self.suppress_approval_prompts()
    }

    /// Whether Bamboo approval requests must be suppressed. This is separate
    /// from the effective authorization mode: requested Auto still suppresses
    /// prompts under a Plan/read-only overlay, while Plan continues to deny
    /// mutating operations before this approval dimension is considered.
    pub fn suppress_approval_prompts(self) -> bool {
        self.requested == SessionPermissionMode::Auto || self.effective == PermissionMode::Auto
    }

    /// Validate a requested/effective pair carried over a transport boundary.
    /// Plan may hard-overlay any request; the other modes follow the shared
    /// resolver's specificity rules.
    pub fn is_consistent(self) -> bool {
        match self.effective {
            PermissionMode::Plan => true,
            PermissionMode::Auto => self.requested != SessionPermissionMode::Bypass,
            PermissionMode::BypassPermissions => self.requested != SessionPermissionMode::Auto,
            PermissionMode::Default | PermissionMode::AcceptEdits | PermissionMode::DontAsk => {
                self.requested == SessionPermissionMode::Default
            }
        }
    }
}

/// Resolve session specificity over the process-wide configured default.
pub fn resolve_permission_mode(
    requested: SessionPermissionMode,
    configured: PermissionMode,
) -> PermissionModeResolution {
    PermissionModeResolution::new(requested, configured)
}

/// Resolve permission mode while applying an executor/profile read-only gate.
///
/// Some external runners express read-only as an isolation profile rather than
/// `PermissionMode::Plan`. Treating it as the same hard overlay centrally keeps
/// Auto from widening the sandbox at those boundaries.
pub fn resolve_permission_mode_with_read_only(
    requested: SessionPermissionMode,
    configured: PermissionMode,
    read_only: bool,
) -> PermissionModeResolution {
    resolve_permission_mode(
        requested,
        if read_only {
            PermissionMode::Plan
        } else {
            configured
        },
    )
}

/// Non-secret bounded inputs used to record one permission execution posture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionAuditSeed {
    pub policy_revision: u64,
    pub resolution: PermissionModeResolution,
    executor_mapping: String,
}

impl PermissionAuditSeed {
    pub fn new(
        policy_revision: u64,
        resolution: PermissionModeResolution,
        executor_mapping: impl Into<String>,
    ) -> Self {
        let executor_mapping = executor_mapping
            .into()
            .chars()
            .take(MAX_PERMISSION_EXECUTOR_MAPPING_CHARS)
            .collect();
        Self {
            policy_revision,
            resolution,
            executor_mapping,
        }
    }

    pub fn bamboo_runtime(policy_revision: u64, resolution: PermissionModeResolution) -> Self {
        Self::new(
            policy_revision,
            resolution,
            format!("bamboo_runtime:{}", resolution.effective.as_str()),
        )
    }

    pub fn executor_mapping(&self) -> &str {
        &self.executor_mapping
    }
}

/// Parsed complete audit group. Incomplete/invalid groups are never treated as
/// a comparable revision and therefore cannot delete a newer in-memory record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionAuditSnapshot {
    pub audit_revision: u64,
    pub policy_revision: u64,
    pub resolution: PermissionModeResolution,
    pub executor_mapping: String,
    pub transitioned_at: String,
}

impl PermissionAuditSnapshot {
    pub fn from_metadata(metadata: &HashMap<String, String>) -> Option<Self> {
        let audit_revision = metadata.get(PERMISSION_AUDIT_REVISION_KEY)?.parse().ok()?;
        if audit_revision >= MAX_PERMISSION_AUDIT_REVISION {
            return None;
        }
        let policy_revision = metadata.get(PERMISSION_POLICY_REVISION_KEY)?.parse().ok()?;
        let requested = match metadata.get(PERMISSION_REQUESTED_MODE_KEY)?.as_str() {
            "default" => SessionPermissionMode::Default,
            "bypass" => SessionPermissionMode::Bypass,
            "auto" => SessionPermissionMode::Auto,
            _ => return None,
        };
        let effective =
            PermissionMode::from_audit_str(metadata.get(PERMISSION_EFFECTIVE_MODE_KEY)?.as_str())?;
        let executor_mapping = metadata.get(PERMISSION_EXECUTOR_MAPPING_KEY)?.clone();
        if executor_mapping.is_empty()
            || executor_mapping.chars().count() > MAX_PERMISSION_EXECUTOR_MAPPING_CHARS
        {
            return None;
        }
        let transitioned_at = metadata.get(PERMISSION_TRANSITIONED_AT_KEY)?.clone();
        if !valid_permission_transition_timestamp(&transitioned_at) {
            return None;
        }
        let resolution = PermissionModeResolution {
            requested,
            effective,
        };
        if !resolution.is_consistent() {
            return None;
        }
        Some(Self {
            audit_revision,
            policy_revision,
            resolution,
            executor_mapping,
            transitioned_at,
        })
    }

    pub fn write_to(&self, metadata: &mut HashMap<String, String>) {
        metadata.insert(
            PERMISSION_AUDIT_REVISION_KEY.to_string(),
            self.audit_revision.to_string(),
        );
        metadata.insert(
            PERMISSION_POLICY_REVISION_KEY.to_string(),
            self.policy_revision.to_string(),
        );
        metadata.insert(
            PERMISSION_REQUESTED_MODE_KEY.to_string(),
            self.resolution.requested.as_str().to_string(),
        );
        metadata.insert(
            PERMISSION_EFFECTIVE_MODE_KEY.to_string(),
            self.resolution.effective.as_str().to_string(),
        );
        metadata.insert(
            PERMISSION_EXECUTOR_MAPPING_KEY.to_string(),
            self.executor_mapping.clone(),
        );
        metadata.insert(
            PERMISSION_TRANSITIONED_AT_KEY.to_string(),
            self.transitioned_at.clone(),
        );
    }
}

fn wall_clock_revision_floor() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
        .unwrap_or(1)
}

fn valid_permission_transition_timestamp(value: &str) -> bool {
    !value.is_empty()
        && value.chars().count() <= MAX_PERMISSION_TRANSITION_TIMESTAMP_CHARS
        && chrono::DateTime::parse_from_rfc3339(value).is_ok()
}

/// Allocate a process-monotonic revision strictly above a persisted floor.
/// The wall-clock component keeps revisions comparable across process restarts;
/// the atomic component makes same-process concurrent writers unique.
pub fn next_permission_audit_revision_after(
    floor: u64,
) -> Result<u64, PermissionAuditRevisionExhausted> {
    let max_allocatable = MAX_PERMISSION_AUDIT_REVISION.saturating_sub(1);
    if floor >= max_allocatable {
        return Err(PermissionAuditRevisionExhausted);
    }
    let wall_floor = wall_clock_revision_floor();
    let mut observed = AUDIT_REVISION_CLOCK.load(Ordering::Relaxed);
    loop {
        if observed >= max_allocatable {
            return Err(PermissionAuditRevisionExhausted);
        }
        let candidate = observed
            .saturating_add(1)
            .max(floor.saturating_add(1))
            .max(wall_floor.min(max_allocatable))
            .min(max_allocatable);
        match AUDIT_REVISION_CLOCK.compare_exchange_weak(
            observed,
            candidate,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(candidate),
            Err(actual) => observed = actual,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PermissionAuditRevisionExhausted;

impl std::fmt::Display for PermissionAuditRevisionExhausted {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("permission audit revision space exhausted")
    }
}

impl std::error::Error for PermissionAuditRevisionExhausted {}

/// Record a complete audit group and return its new comparable revision.
///
/// `transitioned_at` is replaced only for a true posture transition. A
/// same-mode run-start policy/mapping refresh receives a new audit revision but
/// retains the original transition timestamp (initial records get one).
pub fn record_permission_audit(
    metadata: &mut HashMap<String, String>,
    seed: &PermissionAuditSeed,
    transitioned_at: Option<&str>,
) -> Result<u64, PermissionAuditRevisionExhausted> {
    let previous = PermissionAuditSnapshot::from_metadata(metadata);
    let floor = previous
        .as_ref()
        .map(|snapshot| snapshot.audit_revision)
        .unwrap_or_default();
    let audit_revision = next_permission_audit_revision_after(floor)?;
    let transitioned_at = match transitioned_at {
        Some(value) if valid_permission_transition_timestamp(value) => value.to_string(),
        Some(_) => chrono::Utc::now().to_rfc3339(),
        None => previous
            .filter(|snapshot| snapshot.resolution == seed.resolution)
            .map(|snapshot| snapshot.transitioned_at)
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
    };
    PermissionAuditSnapshot {
        audit_revision,
        policy_revision: seed.policy_revision,
        resolution: seed.resolution,
        executor_mapping: seed.executor_mapping.clone(),
        transitioned_at,
    }
    .write_to(metadata);
    Ok(audit_revision)
}

/// Rebase an already-complete incoming audit above the latest durable floor.
/// Used by authoritative activation seeding while holding the session lock.
pub fn rebase_permission_audit_revision(
    metadata: &mut HashMap<String, String>,
    floor: u64,
) -> Option<u64> {
    let mut snapshot = PermissionAuditSnapshot::from_metadata(metadata)?;
    if snapshot.audit_revision > floor {
        return Some(snapshot.audit_revision);
    }
    snapshot.audit_revision = next_permission_audit_revision_after(floor).ok()?;
    snapshot.write_to(metadata);
    Some(snapshot.audit_revision)
}

/// True when the durable typed posture/audit is authoritative over the current
/// live snapshot. A mode transition always wins. For equal modes, only a
/// complete strictly newer audit revision wins; missing/older durable audit can
/// never delete a newer run-start policy/mapping refresh.
pub fn disk_permission_posture_is_fresher(
    current_mode: SessionPermissionMode,
    current_metadata: &HashMap<String, String>,
    disk_mode: SessionPermissionMode,
    disk_metadata: &HashMap<String, String>,
) -> bool {
    if disk_mode != current_mode {
        return true;
    }
    let Some(disk) = PermissionAuditSnapshot::from_metadata(disk_metadata)
        .filter(|snapshot| snapshot.resolution.requested == disk_mode)
    else {
        return false;
    };
    PermissionAuditSnapshot::from_metadata(current_metadata)
        .is_none_or(|current| disk.audit_revision > current.audit_revision)
}

/// Build the complete audit snapshot that must accompany a fresher durable
/// typed posture.
///
/// Legacy durable sessions may have a typed mode but an incomplete audit. A
/// real mode transition still wins; this helper synthesizes a bounded,
/// internally-consistent record above the live revision rather than pairing
/// the adopted mode with stale or partial metadata. Equal modes never adopt an
/// incomplete record.
pub fn fresher_disk_permission_audit(
    current_mode: SessionPermissionMode,
    current_metadata: &HashMap<String, String>,
    disk_mode: SessionPermissionMode,
    disk_metadata: &HashMap<String, String>,
) -> Option<PermissionAuditSnapshot> {
    if !disk_permission_posture_is_fresher(current_mode, current_metadata, disk_mode, disk_metadata)
    {
        return None;
    }

    if let Some(mut disk_audit) = PermissionAuditSnapshot::from_metadata(disk_metadata)
        .filter(|snapshot| snapshot.resolution.requested == disk_mode)
    {
        if disk_mode != current_mode {
            let current_revision = PermissionAuditSnapshot::from_metadata(current_metadata)
                .map(|snapshot| snapshot.audit_revision)
                .unwrap_or_default();
            if disk_audit.audit_revision <= current_revision {
                disk_audit.audit_revision =
                    next_permission_audit_revision_after(current_revision).ok()?;
            }
        }
        return Some(disk_audit);
    }

    let configured_hint = disk_metadata
        .get(PERMISSION_EFFECTIVE_MODE_KEY)
        .and_then(|value| PermissionMode::from_audit_str(value))
        .unwrap_or_default();
    let resolution = resolve_permission_mode(disk_mode, configured_hint);
    let policy_revision = disk_metadata
        .get(PERMISSION_POLICY_REVISION_KEY)
        .and_then(|value| value.parse().ok())
        .unwrap_or_default();
    let mapping = disk_metadata
        .get(PERMISSION_EXECUTOR_MAPPING_KEY)
        .filter(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| format!("bamboo_runtime:{}", resolution.effective.as_str()));
    let transitioned_at = disk_metadata
        .get(PERMISSION_TRANSITIONED_AT_KEY)
        .filter(|value| valid_permission_transition_timestamp(value))
        .cloned()
        .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
    let mut synthesized = current_metadata.clone();
    record_permission_audit(
        &mut synthesized,
        &PermissionAuditSeed::new(policy_revision, resolution, mapping),
        Some(&transitioned_at),
    )
    .ok()?;
    PermissionAuditSnapshot::from_metadata(&synthesized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_bypass_is_not_promoted_by_global_auto() {
        let resolution =
            resolve_permission_mode(SessionPermissionMode::Bypass, PermissionMode::Auto);
        assert_eq!(resolution.requested, SessionPermissionMode::Bypass);
        assert_eq!(resolution.effective, PermissionMode::BypassPermissions);
        assert!(resolution.bypass_permissions());
        assert!(!resolution.auto_approve_permissions());
    }

    #[test]
    fn default_session_inherits_global_auto() {
        let resolution =
            resolve_permission_mode(SessionPermissionMode::Default, PermissionMode::Auto);
        assert_eq!(resolution.requested, SessionPermissionMode::Default);
        assert_eq!(resolution.effective, PermissionMode::Auto);
        assert!(resolution.auto_approve_permissions());
    }

    #[test]
    fn configured_plan_is_a_hard_overlay_for_session_auto() {
        let resolution = resolve_permission_mode(SessionPermissionMode::Auto, PermissionMode::Plan);
        assert_eq!(resolution.requested, SessionPermissionMode::Auto);
        assert_eq!(resolution.effective, PermissionMode::Plan);
        assert!(!resolution.bypass_permissions());
        assert!(resolution.suppress_approval_prompts());
    }

    #[test]
    fn read_only_profile_is_a_hard_overlay_for_session_auto() {
        let resolution = resolve_permission_mode_with_read_only(
            SessionPermissionMode::Auto,
            PermissionMode::Auto,
            true,
        );
        assert_eq!(resolution.requested, SessionPermissionMode::Auto);
        assert_eq!(resolution.effective, PermissionMode::Plan);
    }

    #[test]
    fn same_mode_requires_strictly_newer_complete_audit() {
        let mut current = HashMap::new();
        let seed = PermissionAuditSeed::bamboo_runtime(
            3,
            resolve_permission_mode(SessionPermissionMode::Default, PermissionMode::Auto),
        );
        record_permission_audit(&mut current, &seed, Some("2026-07-31T12:00:00Z")).unwrap();
        let mut disk = current.clone();
        assert!(!disk_permission_posture_is_fresher(
            SessionPermissionMode::Default,
            &current,
            SessionPermissionMode::Default,
            &disk,
        ));

        let current_revision = PermissionAuditSnapshot::from_metadata(&current)
            .unwrap()
            .audit_revision;
        disk.insert(
            PERMISSION_AUDIT_REVISION_KEY.to_string(),
            next_permission_audit_revision_after(current_revision)
                .unwrap()
                .to_string(),
        );
        assert!(disk_permission_posture_is_fresher(
            SessionPermissionMode::Default,
            &current,
            SessionPermissionMode::Default,
            &disk,
        ));

        disk.remove(PERMISSION_EXECUTOR_MAPPING_KEY);
        assert!(!disk_permission_posture_is_fresher(
            SessionPermissionMode::Default,
            &current,
            SessionPermissionMode::Default,
            &disk,
        ));
    }

    #[test]
    fn hostile_revision_and_inconsistent_mode_pairs_are_not_comparable() {
        let mut metadata = HashMap::from([
            (
                PERMISSION_AUDIT_REVISION_KEY.to_string(),
                u64::MAX.to_string(),
            ),
            (PERMISSION_POLICY_REVISION_KEY.to_string(), "1".to_string()),
            (
                PERMISSION_REQUESTED_MODE_KEY.to_string(),
                "auto".to_string(),
            ),
            (
                PERMISSION_EFFECTIVE_MODE_KEY.to_string(),
                "bypass".to_string(),
            ),
            (
                PERMISSION_EXECUTOR_MAPPING_KEY.to_string(),
                "hostile".to_string(),
            ),
            (
                PERMISSION_TRANSITIONED_AT_KEY.to_string(),
                "2026-07-31T12:00:00Z".to_string(),
            ),
        ]);
        assert!(PermissionAuditSnapshot::from_metadata(&metadata).is_none());
        assert!(next_permission_audit_revision_after(u64::MAX).is_err());
        assert!(next_permission_audit_revision_after(
            MAX_PERMISSION_AUDIT_REVISION.saturating_sub(1)
        )
        .is_err());

        metadata.insert(PERMISSION_AUDIT_REVISION_KEY.to_string(), "1".to_string());
        assert!(PermissionAuditSnapshot::from_metadata(&metadata).is_none());
    }

    #[test]
    fn corrupt_partial_timestamp_is_healed_by_host_recording() {
        let mut metadata = HashMap::from([(
            PERMISSION_TRANSITIONED_AT_KEY.to_string(),
            "not-rfc3339".repeat(50),
        )]);
        let seed = PermissionAuditSeed::bamboo_runtime(
            4,
            resolve_permission_mode(SessionPermissionMode::Default, PermissionMode::Auto),
        );
        record_permission_audit(&mut metadata, &seed, None).unwrap();
        let snapshot = PermissionAuditSnapshot::from_metadata(&metadata).unwrap();
        assert_eq!(snapshot.resolution, seed.resolution);
        assert!(chrono::DateTime::parse_from_rfc3339(&snapshot.transitioned_at).is_ok());
    }

    #[test]
    fn legacy_mode_transition_with_bad_timestamp_synthesizes_valid_audit() {
        let mut current = HashMap::new();
        record_permission_audit(
            &mut current,
            &PermissionAuditSeed::bamboo_runtime(
                1,
                resolve_permission_mode(SessionPermissionMode::Default, PermissionMode::Default),
            ),
            Some("2026-07-31T12:00:00Z"),
        )
        .unwrap();
        let disk = HashMap::from([
            (PERMISSION_POLICY_REVISION_KEY.to_string(), "2".to_string()),
            (
                PERMISSION_REQUESTED_MODE_KEY.to_string(),
                "auto".to_string(),
            ),
            (
                PERMISSION_EFFECTIVE_MODE_KEY.to_string(),
                "auto".to_string(),
            ),
            (
                PERMISSION_EXECUTOR_MAPPING_KEY.to_string(),
                "legacy-auto".to_string(),
            ),
            (
                PERMISSION_TRANSITIONED_AT_KEY.to_string(),
                "bad timestamp".to_string(),
            ),
        ]);
        let synthesized = fresher_disk_permission_audit(
            SessionPermissionMode::Default,
            &current,
            SessionPermissionMode::Auto,
            &disk,
        )
        .unwrap();
        assert_eq!(
            synthesized.resolution.requested,
            SessionPermissionMode::Auto
        );
        assert!(chrono::DateTime::parse_from_rfc3339(&synthesized.transitioned_at).is_ok());
    }
}
