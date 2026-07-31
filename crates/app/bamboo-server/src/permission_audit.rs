use bamboo_agent_core::Session;
use bamboo_config::PermissionMode;
use bamboo_domain::SessionPermissionMode;
use bamboo_tools::permission::PermissionConfig;

/// Record the bounded, non-secret permission posture used by a Bamboo runtime
/// execution. Session-scoped modes override the process-wide default; legacy
/// stored `bypass_permissions=true` is resolved by
/// `effective_permission_mode()` and remains Bypass rather than becoming Auto.
pub(crate) fn record_bamboo_runtime_permission_metadata(
    session: &mut Session,
    config: &PermissionConfig,
) {
    let requested = session
        .agent_runtime_state
        .as_ref()
        .map(|state| state.effective_permission_mode())
        .unwrap_or_default();
    let effective = match requested {
        SessionPermissionMode::Default => configured_mode_name(config.mode()),
        other => other.as_str(),
    };

    session.metadata.insert(
        "permission.policy_revision".to_string(),
        config.policy_revision().to_string(),
    );
    session.metadata.insert(
        "permission.requested_mode".to_string(),
        requested.as_str().to_string(),
    );
    session.metadata.insert(
        "permission.effective_mode".to_string(),
        effective.to_string(),
    );
    session.metadata.insert(
        "permission.executor_mapping".to_string(),
        format!("bamboo_runtime:{effective}"),
    );
}

fn configured_mode_name(mode: PermissionMode) -> &'static str {
    match mode {
        PermissionMode::Default => "default",
        PermissionMode::Plan => "plan",
        PermissionMode::AcceptEdits => "accept_edits",
        PermissionMode::DontAsk => "dont_ask",
        PermissionMode::BypassPermissions => "bypass",
        PermissionMode::Auto => "auto",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_auto_overrides_global_mode_in_audit_metadata() {
        let config = PermissionConfig::new();
        config.set_mode(PermissionMode::Default);
        let mut session = Session::new("auto-audit", "model");
        session
            .agent_runtime_state
            .get_or_insert_default()
            .set_permission_mode(SessionPermissionMode::Auto);

        record_bamboo_runtime_permission_metadata(&mut session, &config);

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

        record_bamboo_runtime_permission_metadata(&mut session, &config);

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
}
