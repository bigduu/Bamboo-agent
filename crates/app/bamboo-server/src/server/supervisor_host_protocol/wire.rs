//! Strict native-channel data decoding. A decoded frame is not authorization.

use std::collections::BTreeSet;

use bamboo_domain::{
    ProjectId, SupervisorManagementMutation, SupervisorManagementReceipt,
    SupervisorManagementRequest, SupervisorReference, DEFAULT_SUPERVISOR_SESSION_ID,
    MAX_SUPERVISOR_PROJECTS,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::state::{self, FRAME_PREFIX, MAX_FRAME_BYTES, MAX_REQUESTS_PER_LAUNCH};

pub(super) const VERSION: u8 = 1;

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ClientFrame {
    HelloAck {
        version: u8,
        launch_id: Uuid,
    },
    Apply {
        version: u8,
        launch_id: Uuid,
        request_id: Uuid,
        intent: StrictIntent,
    },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictIntent {
    supervisor: StrictSupervisor,
    expected_state_revision: u64,
    mutation: StrictMutation,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictSupervisor {
    session_id: String,
    incarnation_id: Uuid,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StrictMutation {
    ConfigureProjectScope { allowed_projects: Vec<ProjectId> },
}

#[derive(Debug)]
pub(super) enum Request {
    HelloAck {
        launch_id: Uuid,
    },
    Apply {
        launch_id: Uuid,
        request_id: Uuid,
        intent: SupervisorManagementRequest,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InvalidFrame {
    Json,
    Version,
    NilIdentity,
    Supervisor,
    ProjectSet,
    Session,
    Size,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum Response {
    Hello {
        version: u8,
        launch_id: Uuid,
        max_frame_bytes: usize,
        max_requests_per_launch: usize,
        owner_authorization_available: bool,
    },
    Ready {
        version: u8,
        launch_id: Uuid,
        transport_ready: bool,
        owner_authorization_available: bool,
    },
    Result {
        version: u8,
        launch_id: Uuid,
        request_id: Uuid,
        outcome: Outcome,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum Outcome {
    Refused {
        code: &'static str,
        status_code: u16,
    },
    Committed {
        receipt: SupervisorManagementReceipt,
    },
    OutcomeUnconfirmed {
        code: &'static str,
        status_code: u16,
    },
}

/// The currently implementable channel. Transport negotiation never becomes
/// authorization, and no method accepts an authority from a caller or frame.
/// This is a pure component, not a CLI startup mode or a production pipe loop.
pub(super) struct UnadmittedChannel {
    launch_id: Uuid,
    session: state::Session<SupervisorManagementRequest>,
}

impl UnadmittedChannel {
    pub(super) fn new(launch_id: Uuid) -> Result<Self, InvalidFrame> {
        Ok(Self {
            launch_id,
            session: state::Session::new(launch_id.as_u128())
                .map_err(|_| InvalidFrame::NilIdentity)?,
        })
    }

    pub(super) fn hello(&self) -> Response {
        Response::Hello {
            version: VERSION,
            launch_id: self.launch_id,
            max_frame_bytes: MAX_FRAME_BYTES,
            max_requests_per_launch: MAX_REQUESTS_PER_LAUNCH,
            owner_authorization_available: false,
        }
    }

    pub(super) fn process(&mut self, payload: &[u8]) -> Result<Response, InvalidFrame> {
        let request = match decode(payload) {
            Ok(request) => request,
            Err(failure) => {
                self.session.disconnect();
                return Err(failure);
            }
        };
        match request {
            Request::HelloAck { launch_id } => {
                self.session.acknowledge(launch_id.as_u128()).map_err(|_| {
                    self.session.disconnect();
                    InvalidFrame::Session
                })?;
                Ok(Response::Ready {
                    version: VERSION,
                    launch_id: self.launch_id,
                    transport_ready: self.session.transport_ready(),
                    owner_authorization_available: false,
                })
            }
            Request::Apply {
                launch_id,
                request_id,
                intent,
            } => {
                // Intentionally no verifier: there is no independently
                // verified production native admission in this slice.
                let refusal = match self.session.begin(
                    launch_id.as_u128(),
                    request_id.as_u128(),
                    intent,
                    None,
                ) {
                    Err(refusal) => refusal,
                    Ok(_) => unreachable!("None cannot authorize a native apply"),
                };
                let (status_code, code) = match refusal {
                    state::Refusal::OwnerChannelUnavailable => {
                        (503, "host_owner_authorization_unavailable")
                    }
                    state::Refusal::Replay => (409, "native_request_replay"),
                    state::Refusal::Capacity => (503, "native_channel_exhausted"),
                    _ => {
                        self.session.disconnect();
                        return Err(InvalidFrame::Session);
                    }
                };
                Ok(Response::Result {
                    version: VERSION,
                    launch_id: self.launch_id,
                    request_id,
                    outcome: Outcome::Refused { code, status_code },
                })
            }
        }
    }
}

/// Called only with a bounded UTF-8 payload returned by the byte framer.
/// HTTP credentials, caller destinations and approval booleans are unknown
/// fields, not alternative proof. Existing Project/activity and CAS checks
/// remain mandatory in the canonical use case after genuine Host admission.
pub(super) fn decode(payload: &[u8]) -> Result<Request, InvalidFrame> {
    if payload.len() > MAX_FRAME_BYTES - FRAME_PREFIX.len() - 1 {
        return Err(InvalidFrame::Size);
    }
    let parsed: ClientFrame = serde_json::from_slice(payload).map_err(|_| InvalidFrame::Json)?;
    let (version, launch_id) = match &parsed {
        ClientFrame::HelloAck { version, launch_id }
        | ClientFrame::Apply {
            version, launch_id, ..
        } => (*version, *launch_id),
    };
    if version != VERSION {
        return Err(InvalidFrame::Version);
    }
    if launch_id.is_nil() {
        return Err(InvalidFrame::NilIdentity);
    }
    match parsed {
        ClientFrame::HelloAck { launch_id, .. } => Ok(Request::HelloAck { launch_id }),
        ClientFrame::Apply {
            launch_id,
            request_id,
            intent,
            ..
        } => {
            if request_id.is_nil() || intent.supervisor.incarnation_id.is_nil() {
                return Err(InvalidFrame::NilIdentity);
            }
            if intent.supervisor.session_id != DEFAULT_SUPERVISOR_SESSION_ID {
                return Err(InvalidFrame::Supervisor);
            }
            let StrictMutation::ConfigureProjectScope { allowed_projects } = intent.mutation;
            if allowed_projects.len() > MAX_SUPERVISOR_PROJECTS {
                return Err(InvalidFrame::ProjectSet);
            }
            let count = allowed_projects.len();
            let allowed_projects: BTreeSet<_> = allowed_projects.into_iter().collect();
            if count != allowed_projects.len() {
                return Err(InvalidFrame::ProjectSet);
            }
            Ok(Request::Apply {
                launch_id,
                request_id,
                intent: SupervisorManagementRequest {
                    supervisor: SupervisorReference {
                        session_id: intent.supervisor.session_id,
                        incarnation_id: intent.supervisor.incarnation_id,
                    },
                    expected_state_revision: intent.expected_state_revision,
                    mutation: SupervisorManagementMutation::ConfigureProjectScope {
                        allowed_projects,
                    },
                },
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn apply() -> Value {
        json!({
            "version":1,"kind":"apply",
            "launch_id":"00000000-0000-4000-8000-000000000001",
            "request_id":"00000000-0000-4000-8000-000000000002",
            "intent":{
                "supervisor":{"session_id":"bamboo-default-supervisor","incarnation_id":"00000000-0000-4000-8000-000000000003"},
                "expected_state_revision":4,
                "mutation":{"kind":"configure_project_scope","allowed_projects":["project-b","project-a"]}
            }
        })
    }

    fn parse(value: Value) -> Result<Request, InvalidFrame> {
        decode(&serde_json::to_vec(&value).unwrap())
    }

    fn ack() -> Vec<u8> {
        serde_json::to_vec(&json!({"version":1,"kind":"hello_ack","launch_id":"00000000-0000-4000-8000-000000000001"})).unwrap()
    }

    #[test]
    fn unadmitted_handshake_and_exact_apply_keep_authorization_unavailable() {
        let mut channel = UnadmittedChannel::new(
            Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap(),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(channel.hello()).unwrap()["owner_authorization_available"],
            false
        );
        let ready = serde_json::to_value(channel.process(&ack()).unwrap()).unwrap();
        assert_eq!(ready["transport_ready"], true);
        assert_eq!(ready["owner_authorization_available"], false);
        let payload = serde_json::to_vec(&apply()).unwrap();
        let response = serde_json::to_value(channel.process(&payload).unwrap()).unwrap();
        assert_eq!(
            response["outcome"]["code"],
            "host_owner_authorization_unavailable"
        );
        assert_eq!(response["outcome"]["status_code"], 503);
        let replay = serde_json::to_value(channel.process(&payload).unwrap()).unwrap();
        assert_eq!(replay["outcome"]["code"], "native_request_replay");
    }

    #[test]
    fn malformed_or_wrong_launch_frame_closes_the_component() {
        let mut channel = UnadmittedChannel::new(
            Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap(),
        )
        .unwrap();
        assert_eq!(
            channel.process(br#"{"confirmed":true}"#).unwrap_err(),
            InvalidFrame::Json
        );
        assert_eq!(channel.process(&ack()).unwrap_err(), InvalidFrame::Session);
        let mut channel = UnadmittedChannel::new(
            Uuid::parse_str("00000000-0000-4000-8000-000000000009").unwrap(),
        )
        .unwrap();
        assert_eq!(channel.process(&ack()).unwrap_err(), InvalidFrame::Session);
    }

    #[test]
    fn decodes_the_complete_typed_intent_without_authorizing_it() {
        let Request::Apply {
            launch_id,
            request_id,
            intent,
        } = parse(apply()).unwrap()
        else {
            panic!("apply expected")
        };
        assert_ne!(launch_id, request_id);
        assert_eq!(intent.supervisor.session_id, DEFAULT_SUPERVISOR_SESSION_ID);
        assert_eq!(intent.expected_state_revision, 4);
        assert_eq!(
            intent.mutation,
            SupervisorManagementMutation::ConfigureProjectScope {
                allowed_projects: [
                    ProjectId::parse("project-a").unwrap(),
                    ProjectId::parse("project-b").unwrap()
                ]
                .into_iter()
                .collect()
            }
        );
    }

    #[test]
    fn rejects_unknown_fields_at_every_intent_level() {
        for level in 0..4 {
            let mut value = apply();
            let target = match level {
                0 => &mut value,
                1 => &mut value["intent"],
                2 => &mut value["intent"]["supervisor"],
                _ => &mut value["intent"]["mutation"],
            };
            target["confirmed"] = json!(true);
            assert_eq!(parse(value).unwrap_err(), InvalidFrame::Json);
        }
    }

    #[test]
    fn handshake_cannot_carry_an_authorization_claim_or_apply_payload() {
        let mut value = json!({"version":1,"kind":"hello_ack","launch_id":"00000000-0000-4000-8000-000000000001"});
        assert!(matches!(parse(value.clone()), Ok(Request::HelloAck { .. })));
        value["owner_authorization_available"] = json!(true);
        assert_eq!(parse(value.clone()).unwrap_err(), InvalidFrame::Json);
        value
            .as_object_mut()
            .unwrap()
            .remove("owner_authorization_available");
        value["intent"] = apply()["intent"].clone();
        assert_eq!(parse(value).unwrap_err(), InvalidFrame::Json);
    }

    #[test]
    fn rejects_other_mutations_destinations_and_protocol_versions() {
        let mut value = apply();
        value["intent"]["mutation"]["kind"] = json!("attach");
        assert_eq!(parse(value).unwrap_err(), InvalidFrame::Json);
        let mut value = apply();
        value["destination_url"] = json!("http://127.0.0.1:1");
        assert_eq!(parse(value).unwrap_err(), InvalidFrame::Json);
        let mut value = apply();
        value["version"] = json!(2);
        assert_eq!(parse(value).unwrap_err(), InvalidFrame::Version);
        let mut value = apply();
        value["intent"]["supervisor"]["session_id"] = json!("ordinary-root");
        assert_eq!(parse(value).unwrap_err(), InvalidFrame::Supervisor);
    }

    #[test]
    fn rejects_nil_lifetimes_bad_ids_duplicate_and_oversized_project_sets() {
        for key in ["launch_id", "request_id"] {
            let mut value = apply();
            value[key] = json!(Uuid::nil());
            assert_eq!(parse(value).unwrap_err(), InvalidFrame::NilIdentity);
        }
        let mut value = apply();
        value["intent"]["supervisor"]["incarnation_id"] = json!(Uuid::nil());
        assert_eq!(parse(value).unwrap_err(), InvalidFrame::NilIdentity);
        let mut value = apply();
        value["intent"]["mutation"]["allowed_projects"] = json!(["../other"]);
        assert_eq!(parse(value).unwrap_err(), InvalidFrame::Json);
        let mut value = apply();
        value["intent"]["mutation"]["allowed_projects"] = json!(["project-a", "project-a"]);
        assert_eq!(parse(value).unwrap_err(), InvalidFrame::ProjectSet);
        let mut value = apply();
        value["intent"]["mutation"]["allowed_projects"] =
            json!((0..65).map(|i| format!("p-{i}")).collect::<Vec<_>>());
        assert_eq!(parse(value).unwrap_err(), InvalidFrame::ProjectSet);
    }

    #[test]
    fn empty_explicit_set_is_valid_but_revisions_must_be_exact_u64() {
        let mut value = apply();
        value["intent"]["mutation"]["allowed_projects"] = json!([]);
        assert!(matches!(parse(value.clone()), Ok(Request::Apply { .. })));
        value["intent"]["expected_state_revision"] = json!(-1);
        assert_eq!(parse(value.clone()).unwrap_err(), InvalidFrame::Json);
        value["intent"]["expected_state_revision"] = json!(4.5);
        assert_eq!(parse(value).unwrap_err(), InvalidFrame::Json);
    }

    #[test]
    fn decoder_checks_its_own_bound_before_json_parsing() {
        assert_eq!(
            decode(&vec![b' '; MAX_FRAME_BYTES]).unwrap_err(),
            InvalidFrame::Size
        );
    }
}
