//! Authoritative permission refresh for approved-tool replay.
//!
//! An approval only authorizes the posture that is still active when the
//! suspended tool is about to re-enter the executor. All replay surfaces use
//! this helper so a stale in-memory Auto/Bypass snapshot cannot outrun a newer
//! durable Default/Plan transition.

use bamboo_agent_core::storage::Storage;
use bamboo_agent_core::tools::{plan_mode_allows_tool, ToolExecutionSessionFlags};
use bamboo_agent_core::{AgentError, Session};
use bamboo_domain::{AgentRuntimeState, PermissionMode};

/// Result of refreshing permission posture immediately before approval replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApprovalReplayDecision {
    /// The approved call may enter the executor with these exact live flags.
    Execute(ToolExecutionSessionFlags),
    /// The approval became stale because the latest posture is Plan and the
    /// original tool mutates state. Callers consume the marker and record a
    /// failed result, but must not emit `ToolStart` or enter the executor.
    BlockedByPlan(ToolExecutionSessionFlags),
}

/// Strictly reload and adopt the authoritative permission posture for an
/// approved tool call.
///
/// Storage errors, missing sessions, and malformed typed posture fail closed.
/// This function does not touch the replay marker, so callers can retain it
/// when returning the error. The coherent typed-mode/audit pair and Plan state
/// are committed to `session` only after every fallible validation succeeds.
pub async fn refresh_approval_replay_posture(
    storage: &dyn Storage,
    session: &mut Session,
    configured_mode: PermissionMode,
    tool_name: &str,
) -> Result<ApprovalReplayDecision, AgentError> {
    let latest = storage
        .load_runtime_control_plane(&session.id)
        .await
        .map_err(|error| {
            AgentError::Tool(format!(
                "authoritative approval replay posture refresh failed closed: {error}"
            ))
        })?
        .ok_or_else(|| {
            AgentError::Tool(
                "authoritative approval replay posture refresh failed closed: session missing"
                    .to_string(),
            )
        })?;
    let latest_runtime = latest.agent_runtime_state.as_ref().ok_or_else(|| {
        AgentError::Tool(
            "authoritative approval replay posture refresh failed closed: typed mode missing"
                .to_string(),
        )
    })?;
    let disk_mode = latest_runtime.effective_permission_mode();
    let current_mode = session
        .agent_runtime_state
        .as_ref()
        .map(AgentRuntimeState::effective_permission_mode)
        .unwrap_or_default();

    let should_adopt_audit = bamboo_domain::disk_permission_posture_is_fresher(
        current_mode,
        &session.metadata,
        disk_mode,
        &latest.metadata,
    );
    let fresher_audit = bamboo_domain::fresher_disk_permission_audit(
        current_mode,
        &session.metadata,
        disk_mode,
        &latest.metadata,
    );
    if should_adopt_audit && fresher_audit.is_none() {
        return Err(AgentError::Tool(
            "authoritative approval replay posture refresh failed closed: complete audit unavailable"
                .to_string(),
        ));
    }

    if let Some(audit) = fresher_audit {
        audit.write_to(&mut session.metadata);
    }
    let runtime = session
        .agent_runtime_state
        .get_or_insert_with(|| AgentRuntimeState::new(latest_runtime.run_id.clone()));
    runtime.set_permission_mode(disk_mode);
    runtime.plan_mode = latest_runtime.plan_mode.clone();

    let flags =
        ToolExecutionSessionFlags::from_session_and_configured_mode(session, configured_mode);
    if flags.plan_read_only && !plan_mode_allows_tool(tool_name) {
        Ok(ApprovalReplayDecision::BlockedByPlan(flags))
    } else {
        Ok(ApprovalReplayDecision::Execute(flags))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bamboo_agent_core::storage::Storage;
    use bamboo_agent_core::Session;
    use bamboo_domain::{
        record_permission_audit, resolve_permission_mode, AgentRuntimeState, PermissionAuditSeed,
        PermissionMode, PlanModeState, PlanModeStatus, SessionPermissionMode,
    };
    use tokio::sync::RwLock;

    use super::{refresh_approval_replay_posture, ApprovalReplayDecision};

    struct ReplayStorage {
        session: RwLock<Option<Session>>,
        fail_load: bool,
    }

    #[async_trait::async_trait]
    impl Storage for ReplayStorage {
        async fn save_session(&self, _session: &Session) -> std::io::Result<()> {
            Ok(())
        }

        async fn load_session(&self, _session_id: &str) -> std::io::Result<Option<Session>> {
            unreachable!("approval replay must use load_runtime_control_plane")
        }

        async fn load_runtime_control_plane(
            &self,
            _session_id: &str,
        ) -> std::io::Result<Option<Session>> {
            if self.fail_load {
                return Err(std::io::Error::other("injected posture read failure"));
            }
            Ok(self.session.read().await.clone())
        }

        async fn delete_session(&self, _session_id: &str) -> std::io::Result<bool> {
            Ok(false)
        }
    }

    fn session_with_mode(id: &str, mode: SessionPermissionMode) -> Session {
        let mut session = Session::new(id, "model");
        let mut runtime = AgentRuntimeState::new("run");
        runtime.set_permission_mode(mode);
        session.agent_runtime_state = Some(runtime);
        let resolution = resolve_permission_mode(mode, PermissionMode::Default);
        record_permission_audit(
            &mut session.metadata,
            &PermissionAuditSeed::bamboo_runtime(1, resolution),
            Some("2026-07-31T00:00:00Z"),
        )
        .unwrap();
        session
    }

    #[tokio::test]
    async fn latest_plan_blocks_mutating_replay_before_executor_entry() {
        let mut latest = session_with_mode("plan", SessionPermissionMode::Auto);
        latest.agent_runtime_state.as_mut().unwrap().plan_mode = Some(PlanModeState {
            entered_at: chrono::Utc::now(),
            pre_permission_mode: "auto".to_string(),
            plan_file_path: None,
            status: PlanModeStatus::Exploring,
        });
        let storage: Arc<dyn Storage> = Arc::new(ReplayStorage {
            session: RwLock::new(Some(latest)),
            fail_load: false,
        });
        let mut owned = session_with_mode("plan", SessionPermissionMode::Bypass);

        let decision = refresh_approval_replay_posture(
            storage.as_ref(),
            &mut owned,
            PermissionMode::Default,
            "Write",
        )
        .await
        .unwrap();

        assert_eq!(
            decision,
            ApprovalReplayDecision::BlockedByPlan(
                bamboo_agent_core::tools::ToolExecutionSessionFlags {
                    bypass_permissions: false,
                    auto_approve_permissions: true,
                    plan_read_only: true,
                }
            )
        );
    }

    #[tokio::test]
    async fn latest_auto_and_bypass_map_to_distinct_executor_flags() {
        for (mode, expected_bypass, expected_auto) in [
            (SessionPermissionMode::Auto, false, true),
            (SessionPermissionMode::Bypass, true, false),
        ] {
            let latest = session_with_mode("flags", mode);
            let storage: Arc<dyn Storage> = Arc::new(ReplayStorage {
                session: RwLock::new(Some(latest)),
                fail_load: false,
            });
            let mut owned = session_with_mode("flags", SessionPermissionMode::Default);

            let decision = refresh_approval_replay_posture(
                storage.as_ref(),
                &mut owned,
                PermissionMode::Default,
                "Write",
            )
            .await
            .unwrap();
            let ApprovalReplayDecision::Execute(flags) = decision else {
                panic!("non-Plan replay must remain executable");
            };
            assert_eq!(flags.bypass_permissions, expected_bypass);
            assert_eq!(flags.auto_approve_permissions, expected_auto);
            assert!(!flags.plan_read_only);
        }
    }

    #[tokio::test]
    async fn missing_or_failed_authoritative_load_fails_closed_without_mutation() {
        for storage in [
            ReplayStorage {
                session: RwLock::new(None),
                fail_load: false,
            },
            ReplayStorage {
                session: RwLock::new(None),
                fail_load: true,
            },
        ] {
            let mut owned = session_with_mode("missing", SessionPermissionMode::Auto);
            let before = owned.clone();
            let error = refresh_approval_replay_posture(
                &storage,
                &mut owned,
                PermissionMode::Default,
                "Write",
            )
            .await
            .expect_err("unavailable durable posture must fail closed");
            assert!(error.to_string().contains("failed closed"));
            assert_eq!(owned.agent_runtime_state, before.agent_runtime_state);
            assert_eq!(owned.metadata, before.metadata);
        }
    }
}
