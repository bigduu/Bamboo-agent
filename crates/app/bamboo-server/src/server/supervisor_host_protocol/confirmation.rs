//! Immutable full-intent presentation and private helper result binding.
//! Nothing in this module accepts caller-provided consent or persists approval.
use std::collections::{BTreeMap, BTreeSet};

use bamboo_domain::{
    ProjectId, SupervisorManagementMutation, SupervisorManagementRequest,
    DEFAULT_SUPERVISOR_SESSION_ID, MAX_SUPERVISOR_PROJECTS,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{state::ExactOwnerConsent, wire::Outcome};

pub(super) const MAX_HELPER_BYTES: usize = 128 * 1024;
pub(super) const MAX_PREVIEW_BYTES: usize = 64 * 1024;
pub(super) const HELPER_ARG: &str = "--bamboo-internal-scope-confirmation-v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConfirmationRequest {
    pub(super) version: u8,
    pub(super) launch_id: Uuid,
    pub(super) request_id: Uuid,
    pub(super) challenge_id: Uuid,
    pub(super) data_directory: String,
    pub(super) intent: SupervisorManagementRequest,
    pub(super) previous_projects: BTreeSet<ProjectId>,
    pub(super) selected_project_names: BTreeMap<ProjectId, String>,
}

impl ConfirmationRequest {
    /// Pretty JSON deliberately escapes control characters in names and paths.
    /// The complete canonical request and before/after sets remain scrollable;
    /// exceeding the bound refuses the request instead of truncating any text.
    pub(super) fn preview(&self) -> Result<String, &'static str> {
        let SupervisorManagementMutation::ConfigureProjectScope { allowed_projects } =
            &self.intent.mutation
        else {
            return Err("native_confirmation_unavailable");
        };
        if self.version != 1
            || self.launch_id.is_nil()
            || self.request_id.is_nil()
            || self.challenge_id.is_nil()
            || self.intent.supervisor.incarnation_id.is_nil()
            || self.intent.supervisor.session_id != DEFAULT_SUPERVISOR_SESSION_ID
            || self.data_directory.is_empty()
            || self.previous_projects.len() > MAX_SUPERVISOR_PROJECTS
            || allowed_projects.len() > MAX_SUPERVISOR_PROJECTS
            || self
                .selected_project_names
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
                != *allowed_projects
        {
            return Err("native_confirmation_unavailable");
        }
        let json =
            serde_json::to_string_pretty(self).map_err(|_| "native_confirmation_unavailable")?;
        if json.len() > MAX_PREVIEW_BYTES {
            return Err("native_confirmation_unavailable");
        }
        Ok(format!("Bamboo Supervisor Project scope\n\nReview the entire request below. The complete allowed Project set is replaced, including explicit removal. Removed Projects revoke their managed links; re-adding a Project does not re-attach old Sessions. This does not grant shell access, initialize Work, or attach Sessions.\n\n{json}"))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum HelperDecision {
    Approve,
    Cancel,
    WindowClosed,
    Unavailable { code: Unavailable },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Unavailable {
    #[serde(rename = "native_confirmation_unsupported_platform")]
    UnsupportedPlatform,
    #[serde(rename = "native_confirmation_no_gui")]
    NoGui,
    #[serde(rename = "native_confirmation_thread_unavailable")]
    ThreadUnavailable,
    #[serde(rename = "native_confirmation_unavailable")]
    UiUnavailable,
}
impl Unavailable {
    pub(super) fn code(self) -> &'static str {
        match self {
            Self::UnsupportedPlatform => "native_confirmation_unsupported_platform",
            Self::NoGui => "native_confirmation_no_gui",
            Self::ThreadUnavailable => "native_confirmation_thread_unavailable",
            Self::UiUnavailable => "native_confirmation_unavailable",
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HelperResponse {
    pub(super) request: ConfirmationRequest,
    pub(super) decision: HelperDecision,
}

/// Constructed only from the Bamboo-spawned helper's owned stdout, or tests.
/// The caller transport never deserializes a HelperResponse as an approval.
pub(super) enum Decision {
    Approved(Approval),
    Refused(&'static str),
    Cancelled(&'static str),
}

pub(super) struct Approval(ConfirmationRequest);
impl ExactOwnerConsent<SupervisorManagementRequest> for Approval {
    fn verify_exact(
        &self,
        launch: u128,
        request: u128,
        intent: &SupervisorManagementRequest,
    ) -> bool {
        self.0.launch_id.as_u128() == launch
            && self.0.request_id.as_u128() == request
            && self.0.intent == *intent
    }
}

pub(super) fn owned_helper_response(expected: &ConfirmationRequest, bytes: &[u8]) -> Decision {
    if bytes.len() > MAX_HELPER_BYTES {
        return Decision::Refused("native_confirmation_unavailable");
    }
    let Ok(response) = serde_json::from_slice::<HelperResponse>(bytes) else {
        return Decision::Refused("native_confirmation_unavailable");
    };
    if response.request != *expected {
        return Decision::Refused("native_confirmation_unavailable");
    }
    match response.decision {
        HelperDecision::Approve => Decision::Approved(Approval(response.request)),
        HelperDecision::Cancel => Decision::Cancelled("native_confirmation_cancelled"),
        HelperDecision::WindowClosed => Decision::Cancelled("native_confirmation_window_closed"),
        HelperDecision::Unavailable { code } => Decision::Refused(code.code()),
    }
}

pub(super) fn cancelled(code: &'static str) -> Outcome {
    Outcome::Cancelled {
        code,
        status_code: 409,
        not_submitted: true,
    }
}

#[cfg(test)]
pub(super) fn mock_approval(request: &ConfirmationRequest) -> Decision {
    Decision::Approved(Approval(request.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_domain::SupervisorReference;

    pub(super) fn request() -> ConfirmationRequest {
        let project = ProjectId::parse("p-a").unwrap();
        ConfirmationRequest {
            version: 1,
            launch_id: Uuid::new_v4(),
            request_id: Uuid::new_v4(),
            challenge_id: Uuid::new_v4(),
            data_directory: "/isolated/fixture".into(),
            intent: SupervisorManagementRequest {
                supervisor: SupervisorReference {
                    session_id: DEFAULT_SUPERVISOR_SESSION_ID.into(),
                    incarnation_id: Uuid::new_v4(),
                },
                expected_state_revision: u64::MAX,
                mutation: SupervisorManagementMutation::ConfigureProjectScope {
                    allowed_projects: [project.clone()].into(),
                },
            },
            previous_projects: [ProjectId::parse("old-project").unwrap()].into(),
            selected_project_names: [(project, "Name\nwith control \"text\"".into())].into(),
        }
    }

    #[test]
    fn preview_preserves_full_intent_before_after_and_escapes_names() {
        let request = request();
        let text = request.preview().unwrap();
        for needle in [
            "old-project",
            "p-a",
            "18446744073709551615",
            "Name\\nwith control",
            &request.launch_id.to_string(),
            &request.intent.supervisor.incarnation_id.to_string(),
        ] {
            assert!(text.contains(needle), "{needle}");
        }
        assert!(!text.contains("Name\nwith control"));
    }

    #[test]
    fn oversized_or_incomplete_preview_is_refused_never_truncated() {
        let mut oversized = request();
        oversized
            .selected_project_names
            .values_mut()
            .next()
            .unwrap()
            .push_str(&"x".repeat(MAX_PREVIEW_BYTES));
        assert!(oversized.preview().is_err());
        let mut request = request();
        request.selected_project_names.clear();
        assert!(request.preview().is_err());
    }

    #[test]
    fn helper_result_binds_every_field_and_never_accepts_claims_on_wire() {
        let request = request();
        let response = HelperResponse {
            request: request.clone(),
            decision: HelperDecision::Approve,
        };
        let bytes = serde_json::to_vec(&response).unwrap();
        let Decision::Approved(approval) = owned_helper_response(&request, &bytes) else {
            panic!("owned match");
        };
        assert!(approval.verify_exact(
            request.launch_id.as_u128(),
            request.request_id.as_u128(),
            &request.intent
        ));
        let mut variants = vec![];
        let mut other = request.clone();
        other.launch_id = Uuid::new_v4();
        variants.push(other);
        let mut other = request.clone();
        other.request_id = Uuid::new_v4();
        variants.push(other);
        let mut other = request.clone();
        other.challenge_id = Uuid::new_v4();
        variants.push(other);
        let mut other = request.clone();
        other.intent.expected_state_revision = 0;
        variants.push(other);
        let mut other = request.clone();
        other.previous_projects.clear();
        variants.push(other);
        let mut other = request.clone();
        other.data_directory.push_str("-other");
        variants.push(other);
        let mut other = request.clone();
        other.intent.supervisor.incarnation_id = Uuid::new_v4();
        variants.push(other);
        let mut other = request.clone();
        other.intent.mutation = SupervisorManagementMutation::ConfigureProjectScope {
            allowed_projects: BTreeSet::new(),
        };
        variants.push(other);
        let mut other = request.clone();
        other
            .selected_project_names
            .values_mut()
            .next()
            .unwrap()
            .push('x');
        variants.push(other);
        for other in variants {
            assert!(matches!(
                owned_helper_response(&other, &bytes),
                Decision::Refused(_)
            ));
        }
        let mut value = serde_json::to_value(response).unwrap();
        value["confirmed"] = serde_json::json!(true);
        assert!(matches!(
            owned_helper_response(&request, &serde_json::to_vec(&value).unwrap()),
            Decision::Refused(_)
        ));
    }

    #[test]
    fn user_cancel_close_and_failures_cannot_become_approval() {
        let request = request();
        for decision in [HelperDecision::Cancel, HelperDecision::WindowClosed] {
            let bytes = serde_json::to_vec(&HelperResponse {
                request: request.clone(),
                decision,
            })
            .unwrap();
            assert!(matches!(
                owned_helper_response(&request, &bytes),
                Decision::Cancelled(_)
            ));
        }
        for code in [
            Unavailable::UnsupportedPlatform,
            Unavailable::NoGui,
            Unavailable::ThreadUnavailable,
            Unavailable::UiUnavailable,
        ] {
            let bytes = serde_json::to_vec(&HelperResponse {
                request: request.clone(),
                decision: HelperDecision::Unavailable { code },
            })
            .unwrap();
            assert!(matches!(
                owned_helper_response(&request, &bytes),
                Decision::Refused(_)
            ));
        }
        for bytes in [
            b"true".as_slice(),
            br#"{"confirmed":true}"#,
            br#"{"decision":{"kind":"approve"}}"#,
        ] {
            assert!(matches!(
                owned_helper_response(&request, bytes),
                Decision::Refused(_)
            ));
        }
    }
}
