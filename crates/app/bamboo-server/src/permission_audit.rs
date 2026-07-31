use bamboo_agent_core::Session;
use bamboo_domain::PermissionAuditSeed;
use bamboo_tools::permission::PermissionConfig;

/// Record the bounded, non-secret permission posture used by a Bamboo runtime
/// execution. Session-scoped modes override the process-wide default; legacy
/// stored `bypass_permissions=true` is resolved by
/// `effective_permission_mode()` and remains Bypass rather than becoming Auto.
pub(crate) fn record_bamboo_runtime_permission_metadata(
    session: &mut Session,
    config: &PermissionConfig,
) -> Result<(), bamboo_domain::PermissionAuditRevisionExhausted> {
    record_bamboo_runtime_permission_metadata_at(session, config, None)
}

pub(crate) fn record_bamboo_runtime_permission_transition_metadata(
    session: &mut Session,
    config: &PermissionConfig,
    transitioned_at: &str,
) -> Result<(), bamboo_domain::PermissionAuditRevisionExhausted> {
    record_bamboo_runtime_permission_metadata_at(session, config, Some(transitioned_at))
}

fn record_bamboo_runtime_permission_metadata_at(
    session: &mut Session,
    config: &PermissionConfig,
    transitioned_at: Option<&str>,
) -> Result<(), bamboo_domain::PermissionAuditRevisionExhausted> {
    let requested = session
        .agent_runtime_state
        .as_ref()
        .map(|state| state.effective_permission_mode())
        .unwrap_or_default();
    let configured = if session
        .agent_runtime_state
        .as_ref()
        .is_some_and(|state| state.plan_mode.is_some())
    {
        bamboo_domain::PermissionMode::Plan
    } else {
        config.mode()
    };
    let resolution = bamboo_domain::resolve_permission_mode(requested, configured);
    bamboo_domain::record_permission_audit(
        &mut session.metadata,
        &PermissionAuditSeed::bamboo_runtime(config.policy_revision(), resolution),
        transitioned_at,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_config::PermissionMode;
    use bamboo_domain::SessionPermissionMode;

    #[test]
    fn session_auto_overrides_global_mode_in_audit_metadata() {
        let config = PermissionConfig::new();
        config.set_mode(PermissionMode::Default);
        let mut session = Session::new("auto-audit", "model");
        session
            .agent_runtime_state
            .get_or_insert_default()
            .set_permission_mode(SessionPermissionMode::Auto);

        record_bamboo_runtime_permission_metadata(&mut session, &config).unwrap();

        assert_eq!(
            session.metadata.get("permission.requested_mode"),
            Some(&"auto".to_string())
        );
        assert_eq!(
            session.metadata.get("permission.effective_mode"),
            Some(&"auto".to_string())
        );
        assert_eq!(
            session.metadata.get("permission.executor_mapping"),
            Some(&"bamboo_runtime:auto".to_string())
        );
    }

    #[test]
    fn global_mode_is_effective_when_session_uses_default() {
        let config = PermissionConfig::new();
        config.set_mode(PermissionMode::AcceptEdits);
        let mut session = Session::new("global-audit", "model");

        record_bamboo_runtime_permission_metadata(&mut session, &config).unwrap();

        assert_eq!(
            session.metadata.get("permission.requested_mode"),
            Some(&"default".to_string())
        );
        assert_eq!(
            session.metadata.get("permission.effective_mode"),
            Some(&"accept_edits".to_string())
        );
        assert_eq!(
            session.metadata.get("permission.executor_mapping"),
            Some(&"bamboo_runtime:accept_edits".to_string())
        );
    }

    #[test]
    fn plan_remains_effective_under_session_auto() {
        let config = PermissionConfig::new();
        config.set_mode(PermissionMode::Plan);
        let mut session = Session::new("plan-audit", "model");
        session
            .agent_runtime_state
            .get_or_insert_default()
            .set_permission_mode(SessionPermissionMode::Auto);

        record_bamboo_runtime_permission_metadata(&mut session, &config).unwrap();

        assert_eq!(
            session.metadata.get("permission.requested_mode"),
            Some(&"auto".to_string())
        );
        assert_eq!(
            session.metadata.get("permission.effective_mode"),
            Some(&"plan".to_string())
        );
    }

    #[test]
    fn same_mode_run_start_refresh_gets_a_new_audit_revision() {
        let config = PermissionConfig::new();
        let mut session = Session::new("revision-audit", "model");
        record_bamboo_runtime_permission_metadata(&mut session, &config).unwrap();
        let first = bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata)
            .unwrap()
            .audit_revision;
        record_bamboo_runtime_permission_metadata(&mut session, &config).unwrap();
        let second = bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata)
            .unwrap()
            .audit_revision;
        assert!(second > first);
    }

    #[test]
    fn global_effective_mode_transition_refreshes_transition_timestamp() {
        let config = PermissionConfig::new();
        let mut session = Session::new("effective-transition", "model");
        record_bamboo_runtime_permission_transition_metadata(
            &mut session,
            &config,
            "2026-07-31T12:00:00Z",
        )
        .unwrap();

        config.set_mode(PermissionMode::Auto);
        record_bamboo_runtime_permission_metadata(&mut session, &config).unwrap();
        let snapshot =
            bamboo_domain::PermissionAuditSnapshot::from_metadata(&session.metadata).unwrap();
        assert_eq!(
            snapshot.resolution.requested,
            SessionPermissionMode::Default
        );
        assert_eq!(snapshot.resolution.effective, PermissionMode::Auto);
        assert_ne!(snapshot.transitioned_at, "2026-07-31T12:00:00Z");
    }
}
