//! Pure native transport classification. Never issues a Root writer or grant.

use std::io;

use bamboo_domain::{
    ActorDirectoryEntry, ProjectId, ScopedLoopControlBinding, Session, SessionKind,
    AGENT_RUNTIME_STATE_VERSION,
};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScopedNativeTransportDisposition {
    /// Includes both a possibly current pipeline and insufficient evidence.
    /// This is never an affirmative delivery/RootWriter permission.
    LiveOrUnknown,
    Inert(ScopedNativeTransportInertReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScopedNativeTransportInertReason {
    /// The actual current census activation is terminal. This does NOT claim
    /// its physical fence equals some historical fence absent from `binding`.
    CurrentActivationTerminal,
    /// The actual current live census lease has expired at this job's `now`.
    CurrentLeaseExpired,
    /// A different run currently has exclusive native Actor write authority.
    OwnerRunSuperseded,
    /// Under the same run string, canonical state names another pipeline.
    PipelineExecutionSuperseded,
}

/// Pure deny-only transport classification on a strict *current* snapshot.
///
/// Mandatory caller preconditions, kept under the existing physical guards:
/// - Read regular Main, Runtime, actor-authority and initialization-marker bytes.
/// - Validate full Main and strict Main/Runtime overlay identity.
/// - Normalize the serializer's legacy empty Root root_session_id on the local
///   decoded copies, as RootActorWriteProof does (never persist this helper).
/// - Run actor_directory::validate_census_witnesses on BOTH Main and Runtime
///   with those SAME current record/marker bytes. Do not ensure/repair/init.
/// - `canonical` is their legitimate runtime overlay, `entry` is decoded from
///   exactly those record bytes. Missing/corrupt bytes fail before this call.
/// - Re-read/reclassify within every started blocking mutation job, with fresh
///   Utc::now(); do not cache a lease deadline across renewal or a paused job.
///
/// The old scoped binding contains no physical activation id/epoch/lease owner.
/// Never construct an old ActorActivationFence from it. This result permits
/// only disposal of matching, independently proven non-admitted transports;
/// it does not prove process drain or permit Session/Loop/cursor writes.
pub(super) fn scoped_native_transport_disposition(
    binding: &ScopedLoopControlBinding,
    canonical: &Session,
    entry: &ActorDirectoryEntry,
    now: DateTime<Utc>,
) -> io::Result<ScopedNativeTransportDisposition> {
    use ScopedNativeTransportDisposition::{Inert, LiveOrUnknown};
    use ScopedNativeTransportInertReason::{
        CurrentActivationTerminal, CurrentLeaseExpired, OwnerRunSuperseded,
        PipelineExecutionSuperseded,
    };
    let invalid = |message| io::Error::new(io::ErrorKind::InvalidData, message);
    binding
        .validate()
        .map_err(|_| invalid("invalid scoped transport selector"))?;
    entry
        .validate()
        .map_err(|_| invalid("invalid current native census"))?;
    let project: Option<ProjectId> = canonical
        .project_id_meta()
        .map(ProjectId::parse)
        .transpose()
        .map_err(|_| invalid("invalid current native Project"))?;
    if canonical.id != binding.target_session_id
        || canonical.created_at != binding.target_created_at
        || canonical.kind != SessionKind::Root
        || canonical.parent_session_id.is_some()
        || canonical.spawn_depth != 0
        || !canonical.authority_identity.is_ordinary()
        || !entry.actor.matches_session(canonical)
        || entry.actor.project_id.as_deref() != project.as_ref().map(ProjectId::as_str)
    {
        return Err(invalid("native census/canonical target lifetime mismatch"));
    }
    if let Some(runtime) = canonical.agent_runtime_state.as_ref() {
        if runtime.version != AGENT_RUNTIME_STATE_VERSION
            || runtime
                .execution_id
                .as_deref()
                .is_some_and(|id| !bamboo_domain::valid_session_run_id(id))
        {
            return Err(invalid("invalid current canonical runtime identity"));
        }
    }
    let Some(current) = entry.activation.as_ref() else {
        // A valid never-claimed Cold actor is not an old-run terminal proof.
        return Ok(LiveOrUnknown);
    };
    if !current.status.is_live() {
        // Entry::validate checked the status/logical-state/finished_at shape.
        return Ok(Inert(CurrentActivationTerminal));
    }
    if current.lease_expires_at <= now {
        return Ok(Inert(CurrentLeaseExpired));
    }
    if current.run_id != binding.owner_run_id {
        return Ok(Inert(OwnerRunSuperseded));
    }
    // Root AgentRuntimeState.run_id usually contains the logical Session id,
    // not the Actor/Router run id. Do not compare it to binding.owner_run_id.
    if canonical
        .agent_runtime_state
        .as_ref()
        .and_then(|runtime| runtime.execution_id.as_deref())
        .is_some_and(|execution| execution != binding.execution_id)
    {
        return Ok(Inert(PipelineExecutionSuperseded));
    }
    // Same run/execution stays live-or-unknown even if its runtime status says
    // Failed/Completed. A missing execution id is not a terminal proof either.
    // Epoch changes alone cannot be related to the old epoch-less binding.
    Ok(LiveOrUnknown)
}
