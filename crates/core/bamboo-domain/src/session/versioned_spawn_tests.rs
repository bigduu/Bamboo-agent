//! Contract boundary for independent Root spawning, exercised before wiring it.
use super::*;
use crate::{Message, SessionAuthorityIdentity};

#[test]
fn newly_created_sessions_explicitly_stamp_the_independent_root_contract() {
    let parent = Session::new("new-root", "model");
    let child = Session::new_child_of("child", &parent, "model", "child");
    assert_eq!(parent.session_version, Some(2));
    assert_eq!(child.session_version, Some(2));
    assert_eq!(CURRENT_SESSION_VERSION, 2);
}

#[test]
fn v2_is_supported_without_turning_v1_or_missing_stamp_into_a_conflict() {
    for version in [None, Some(1), Some(2)] {
        for ultra in [false, true] {
            for supervisor in [false, true] {
                let mut session = Session::new(
                    if supervisor {
                        crate::DEFAULT_SUPERVISOR_SESSION_ID
                    } else {
                        "root"
                    },
                    "model",
                );
                session.session_version = version;
                session.set_root_orchestration_only(ultra).unwrap();
                if supervisor {
                    session.authority_identity = SessionAuthorityIdentity::Supervisor {
                        incarnation_id: uuid::Uuid::new_v4(),
                    };
                }
                let before = serde_json::to_value(&session).unwrap();
                let report = session.execution_compatibility();
                assert!(
                    report.execution_allowed,
                    "version={version:?}, ultra={ultra}, supervisor={supervisor}: {report:?}"
                );
                assert!(report.history_readable);
                assert_eq!(
                    session.allows_model_tool_execution("Bash"),
                    !ultra && !supervisor
                );
                assert_eq!(serde_json::to_value(session).unwrap(), before);
            }
        }
    }
}

#[test]
fn main_and_runtime_with_different_supported_spawn_contracts_preserve_history_but_refuse_execution()
{
    for (main, retained) in [
        (Some(1), Some(2)),
        (Some(2), Some(1)),
        (None, Some(2)),
        (Some(2), None),
    ] {
        let mut session = Session::new("retained", "model");
        session.session_version = retained;
        session.add_message(Message::user("keep goal and progress references readable"));
        session.metadata.insert(
            "permission_reexecute_tool_call_id".into(),
            "pending-legacy-call".into(),
        );
        session.agent_runtime_state = Some(AgentRuntimeState::new("retained-run"));
        session.observe_main_session_version(main);
        let before = serde_json::to_value(&session).unwrap();
        let report = session.execution_compatibility();
        assert!(report.history_readable);
        assert!(
            !report.execution_allowed,
            "main={main:?}, retained={retained:?}"
        );
        assert!(report
            .conflicts
            .iter()
            .any(|c| c.code == "session_version_mismatch"));
        assert!(
            !report
                .conflicts
                .iter()
                .any(|c| c.code == "unsupported_session_version"),
            "both known contracts must be supported individually: {report:?}"
        );
        assert_eq!(serde_json::to_value(session).unwrap(), before);
    }
}

#[test]
fn live_authority_refresh_cannot_reinterpret_an_existing_session_tool_contract() {
    for (observed, durable) in [
        (Some(1), Some(2)),
        (Some(2), Some(1)),
        (None, Some(2)),
        (Some(2), None),
    ] {
        let mut session = Session::new("same-root", "model");
        session.session_version = observed;
        let mut latest = session.clone();
        latest.session_version = durable;
        let before = serde_json::to_value(&session).unwrap();
        assert!(
            session.adopt_root_tool_authority_from(&latest).is_err(),
            "observed={observed:?}, durable={durable:?}"
        );
        assert_eq!(serde_json::to_value(session).unwrap(), before);
    }
}

#[test]
fn legacy_and_explicit_v1_are_the_same_supported_tool_contract_on_refresh() {
    for (observed, durable) in [(None, Some(1)), (Some(1), None), (Some(1), Some(1))] {
        let mut session = Session::new("compatible-root", "model");
        session.session_version = observed;
        let mut latest = session.clone();
        latest.session_version = durable;
        session.adopt_root_tool_authority_from(&latest).unwrap();
        assert!(session.execution_compatibility().execution_allowed);
        assert_eq!(
            session.session_version, observed,
            "refresh does not migrate a stamp"
        );
    }
}

#[test]
fn root_spawn_reference_never_falls_back_to_child_in_v2() {
    for reference in [
        "spawn_session",
        "spawnSession",
        "default::spawn_session",
        "default::spawnSession",
    ] {
        for version in [None, Some(1)] {
            assert_eq!(
                resolve_session_tool_reference(version, reference, |name| name == "SubAgent")
                    .as_deref(),
                Some("SubAgent")
            );
        }
        assert!(
            resolve_session_tool_reference(Some(2), reference, |name| name == "SubAgent").is_none()
        );
        assert_eq!(
            resolve_session_tool_reference(Some(2), reference, |name| name == "spawn_session"
                || name == "SubAgent")
            .as_deref(),
            Some("spawn_session")
        );
    }
}

#[test]
fn only_v2_genuine_supervisors_gain_root_spawning_in_either_mode() {
    for version in [None, Some(1), Some(2)] {
        for ultra in [false, true] {
            for supervisor in [false, true] {
                let mut session = Session::new(
                    if supervisor {
                        crate::DEFAULT_SUPERVISOR_SESSION_ID
                    } else {
                        "ordinary"
                    },
                    "model",
                );
                session.session_version = version;
                session.set_root_orchestration_only(ultra).unwrap();
                if supervisor {
                    session.authority_identity = SessionAuthorityIdentity::Supervisor {
                        incarnation_id: uuid::Uuid::new_v4(),
                    };
                }
                let expected = if version == Some(2) {
                    supervisor
                } else {
                    !ultra && !supervisor
                };
                assert_eq!(
                    session.allows_model_tool_execution("spawn_session"),
                    expected,
                    "{version:?}/{ultra}/{supervisor}"
                );
                let child = Session::new_child_of("child", &session, "model", "child");
                assert!(!child.allows_model_tool_execution("spawn_session"));
                assert!(child.supervisor_incarnation_id().is_none());
            }
        }
    }
}
