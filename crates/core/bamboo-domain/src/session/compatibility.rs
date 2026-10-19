//! Read-only assessment of the behavior contract retained by one Session.
//!
//! A legacy record without a stamp uses the existing supported contract. This
//! is neither a migration nor an authority grant. New tool semantics require a
//! separately implemented version; an old tool spelling never changes meaning
//! merely because this process is newer.

use serde::{Deserialize, Serialize};
use std::fmt;

use super::{
    AgentRuntimeState, Session, AGENT_RUNTIME_STATE_VERSION, MODEL_CONTEXT_SCHEMA_VERSION,
};

pub const CURRENT_SESSION_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionCompatibilityStatus {
    Compatible,
    LegacyCompatible,
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCompatibilityConflict {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCompatibilityReport {
    pub session_version: Option<u32>,
    pub current_session_version: u32,
    pub status: SessionCompatibilityStatus,
    pub history_readable: bool,
    pub execution_allowed: bool,
    pub conflicts: Vec<SessionCompatibilityConflict>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCompatibilityError {
    pub compatibility: SessionCompatibilityReport,
}

impl fmt::Display for SessionCompatibilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("session_execution_incompatible: ")?;
        for (index, conflict) in self.compatibility.conflicts.iter().enumerate() {
            if index != 0 {
                f.write_str("; ")?;
            }
            write!(f, "{}: {}", conflict.code, conflict.message)?;
        }
        f.write_str(" Read the existing history with a compatible runtime, or start a new session with selected context and an explicit workspace. No state was migrated.")
    }
}

impl std::error::Error for SessionCompatibilityError {}

fn supported(version: Option<u32>) -> bool {
    version.is_none_or(|version| version == CURRENT_SESSION_VERSION)
}

impl Session {
    /// Called only by the storage overlay: Main owns the behavior stamp, while
    /// a conflicting runtime observation must remain visible to execution.
    /// Absence and v1 have the same existing semantics, so that pair is safe.
    pub fn observe_main_session_version(&mut self, main_version: Option<u32>) {
        self.observed_runtime_session_version = Some(self.session_version);
        self.session_version = main_version;
    }

    pub fn execution_compatibility(&self) -> SessionCompatibilityReport {
        let mut conflicts = Vec::new();
        if let Some(version) = self
            .session_version
            .filter(|_| !supported(self.session_version))
        {
            conflicts.push(SessionCompatibilityConflict {
                code: "unsupported_session_version".into(),
                message: format!("Session behavior version {version} is not supported; this runtime supports version {CURRENT_SESSION_VERSION} and compatible legacy records."),
            });
        }
        if let Some(runtime_version) = self.observed_runtime_session_version {
            if runtime_version != self.session_version
                && !(supported(runtime_version) && supported(self.session_version))
            {
                conflicts.push(SessionCompatibilityConflict {
                    code: "session_version_mismatch".into(),
                    message: format!("Canonical Session behavior {:?} and retained runtime behavior {:?} conflict.", self.session_version, runtime_version),
                });
            }
        }
        if let Some(AgentRuntimeState { version, .. }) = &self.agent_runtime_state {
            if *version != AGENT_RUNTIME_STATE_VERSION {
                conflicts.push(SessionCompatibilityConflict {
                    code: "unsupported_runtime_state_version".into(),
                    message: format!("Retained execution-state schema {version} is not supported; expected {AGENT_RUNTIME_STATE_VERSION}."),
                });
            }
        }
        if let Some(context) = &self.model_context_state {
            if context.schema_version != MODEL_CONTEXT_SCHEMA_VERSION {
                conflicts.push(SessionCompatibilityConflict {
                    code: "unsupported_model_context_version".into(),
                    message: format!("Retained model-context schema {} is not supported; expected {MODEL_CONTEXT_SCHEMA_VERSION}.", context.schema_version),
                });
            }
        }
        let execution_allowed = conflicts.is_empty();
        SessionCompatibilityReport {
            session_version: self.session_version,
            current_session_version: CURRENT_SESSION_VERSION,
            status: if !execution_allowed {
                SessionCompatibilityStatus::Conflict
            } else if self.session_version.is_none() {
                SessionCompatibilityStatus::LegacyCompatible
            } else {
                SessionCompatibilityStatus::Compatible
            },
            history_readable: true,
            execution_allowed,
            conflicts,
        }
    }

    pub fn check_execution_compatibility(&self) -> Result<(), SessionCompatibilityError> {
        let compatibility = self.execution_compatibility();
        if compatibility.execution_allowed {
            Ok(())
        } else {
            Err(SessionCompatibilityError { compatibility })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Message, SessionAuthorityIdentity};

    #[test]
    fn missing_stamp_is_assessed_and_compatible_without_mutation() {
        let mut session = Session::new("legacy", "model");
        session.session_version = None;
        session.add_message(Message::user("keep history"));
        let before = serde_json::to_value(&session).unwrap();
        let report = session.execution_compatibility();
        assert_eq!(report.status, SessionCompatibilityStatus::LegacyCompatible);
        assert!(report.execution_allowed && report.history_readable);
        session.check_execution_compatibility().unwrap();
        assert_eq!(serde_json::to_value(&session).unwrap(), before);
    }

    #[test]
    fn current_and_legacy_contracts_preserve_mode_and_identity_boundaries() {
        for version in [None, Some(CURRENT_SESSION_VERSION)] {
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
                    assert!(session.execution_compatibility().execution_allowed);
                    session.check_execution_compatibility().unwrap();
                    assert_eq!(serde_json::to_value(&session).unwrap(), before);
                    assert_eq!(
                        session.allows_model_tool_execution("Bash"),
                        !ultra && !supervisor
                    );
                    let mut child = Session::new_child_of("child", &session, "model", "child");
                    child.session_version = version;
                    assert!(child.execution_compatibility().execution_allowed);
                    assert!(child.allows_model_tool_execution("Bash"));
                }
            }
        }
    }

    #[test]
    fn unsupported_versions_are_diagnostics_not_deserialization_failures() {
        for version in [0, 2, u32::MAX] {
            let mut session = Session::new("future", "model");
            session.session_version = Some(version);
            let before = serde_json::to_value(&session).unwrap();
            let reloaded: Session = serde_json::from_value(before.clone()).unwrap();
            let report = reloaded.execution_compatibility();
            assert!(report.history_readable);
            assert!(!report.execution_allowed);
            assert_eq!(report.conflicts[0].code, "unsupported_session_version");
            assert!(reloaded
                .check_execution_compatibility()
                .unwrap_err()
                .to_string()
                .contains("session_execution_incompatible"));
            assert_eq!(serde_json::to_value(reloaded).unwrap(), before);
        }
    }

    #[test]
    fn runtime_overlay_cannot_hide_an_unsupported_main_or_side_stamp() {
        for (main, runtime) in [(Some(99), None), (Some(1), Some(99)), (None, Some(99))] {
            let mut side = Session::new("pair", "model");
            side.session_version = runtime;
            side.observe_main_session_version(main);
            assert_eq!(side.session_version, main);
            assert!(!side.execution_compatibility().execution_allowed);
        }
        for (main, runtime) in [(None, Some(1)), (Some(1), None), (None, None)] {
            let mut side = Session::new("pair", "model");
            side.session_version = runtime;
            side.observe_main_session_version(main);
            assert_eq!(side.session_version, main);
            assert!(side.execution_compatibility().execution_allowed);
        }
    }

    #[test]
    fn unsupported_retained_model_context_is_refused_without_reset() {
        let mut session = Session::new("future-context", "model");
        session.session_version = None;
        let context = super::super::ModelContextState {
            schema_version: 99,
            state_revision: 7,
            prefix_epoch: 3,
            ..Default::default()
        };
        session.model_context_state = Some(context);
        let before = serde_json::to_value(&session).unwrap();
        let report = session.execution_compatibility();
        assert!(!report.execution_allowed);
        assert_eq!(
            report.conflicts[0].code,
            "unsupported_model_context_version"
        );
        assert_eq!(serde_json::to_value(session).unwrap(), before);
    }

    #[test]
    fn unsupported_retained_runtime_state_is_refused_without_reset() {
        let mut session = Session::new("future-runtime", "model");
        session.session_version = None;
        let mut state = AgentRuntimeState::new("retained-execution");
        state.version = 99;
        session.agent_runtime_state = Some(state);
        let before = serde_json::to_value(&session).unwrap();
        let report = session.execution_compatibility();
        assert!(!report.execution_allowed);
        assert_eq!(
            report.conflicts[0].code,
            "unsupported_runtime_state_version"
        );
        assert_eq!(serde_json::to_value(session).unwrap(), before);
    }
}
