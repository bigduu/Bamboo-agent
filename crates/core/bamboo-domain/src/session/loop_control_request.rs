//! Strict, state-independent command contract for one finite loop.
//! Routing and execution authority remain with the owning host/runner.

use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

pub const MAX_OBJECTIVE_BYTES: usize = 16 * 1024;
pub const MAX_ITERATIONS: u32 = 100;
pub const MAX_TOTAL_ROUNDS: u32 = 1000;

/// A command for the executing session only; no target-session routing exists.
///
/// Deserialization rejects extra fields for every action, including `status`.
/// UUIDs and objectives retain their supplied spelling for owner-side operation
/// fingerprints. State-dependent checks belong to the owning runner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum LoopControlRequest {
    Status,
    Start {
        operation_id: String,
        expected_generation: Option<String>,
        expected_revision: u64,
        objective: String,
        max_iterations: u32,
        max_total_rounds: u32,
    },
    Update {
        operation_id: String,
        generation: String,
        expected_revision: u64,
        objective: Option<String>,
        max_iterations: Option<u32>,
        max_total_rounds: Option<u32>,
    },
    Stop {
        operation_id: String,
        generation: String,
        expected_revision: u64,
    },
}

// An empty struct variant makes deny_unknown_fields apply to status as well as
// the data-bearing variants. A unit variant's ignored content can otherwise
// accept extra fields in an internally tagged enum.
#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum LoopControlWireRequest {
    Status {},
    Start {
        operation_id: String,
        #[serde(default)]
        expected_generation: Option<String>,
        expected_revision: u64,
        objective: String,
        max_iterations: u32,
        max_total_rounds: u32,
    },
    Update {
        operation_id: String,
        generation: String,
        expected_revision: u64,
        #[serde(default)]
        objective: Option<String>,
        #[serde(default)]
        max_iterations: Option<u32>,
        #[serde(default)]
        max_total_rounds: Option<u32>,
    },
    Stop {
        operation_id: String,
        generation: String,
        expected_revision: u64,
    },
}

impl<'de> Deserialize<'de> for LoopControlRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match LoopControlWireRequest::deserialize(deserializer)? {
            LoopControlWireRequest::Status {} => Self::Status,
            LoopControlWireRequest::Start {
                operation_id,
                expected_generation,
                expected_revision,
                objective,
                max_iterations,
                max_total_rounds,
            } => Self::Start {
                operation_id,
                expected_generation,
                expected_revision,
                objective,
                max_iterations,
                max_total_rounds,
            },
            LoopControlWireRequest::Update {
                operation_id,
                generation,
                expected_revision,
                objective,
                max_iterations,
                max_total_rounds,
            } => Self::Update {
                operation_id,
                generation,
                expected_revision,
                objective,
                max_iterations,
                max_total_rounds,
            },
            LoopControlWireRequest::Stop {
                operation_id,
                generation,
                expected_revision,
            } => Self::Stop {
                operation_id,
                generation,
                expected_revision,
            },
        })
    }
}

impl LoopControlRequest {
    pub fn action(&self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Start { .. } => "start",
            Self::Update { .. } => "update",
            Self::Stop { .. } => "stop",
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Status => Ok(()),
            Self::Start {
                operation_id,
                expected_generation,
                objective,
                max_iterations,
                max_total_rounds,
                ..
            } => {
                validate_uuid("operation_id", operation_id)?;
                if let Some(generation) = expected_generation {
                    validate_uuid("expected_generation", generation)?;
                }
                validate_objective(objective)?;
                validate_bound("max_iterations", *max_iterations, MAX_ITERATIONS)?;
                validate_bound("max_total_rounds", *max_total_rounds, MAX_TOTAL_ROUNDS)
            }
            Self::Update {
                operation_id,
                generation,
                objective,
                max_iterations,
                max_total_rounds,
                ..
            } => {
                validate_uuid("operation_id", operation_id)?;
                validate_uuid("generation", generation)?;
                if objective.is_none() && max_iterations.is_none() && max_total_rounds.is_none() {
                    return Err("loop_control.update requires at least one patch field".into());
                }
                if let Some(objective) = objective {
                    validate_objective(objective)?;
                }
                if let Some(max_iterations) = max_iterations {
                    validate_bound("max_iterations", *max_iterations, MAX_ITERATIONS)?;
                }
                if let Some(max_total_rounds) = max_total_rounds {
                    validate_bound("max_total_rounds", *max_total_rounds, MAX_TOTAL_ROUNDS)?;
                }
                Ok(())
            }
            Self::Stop {
                operation_id,
                generation,
                ..
            } => {
                validate_uuid("operation_id", operation_id)?;
                validate_uuid("generation", generation)
            }
        }
    }
}

fn validate_uuid(field: &str, value: &str) -> Result<(), String> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| format!("loop_control.{field} must be a valid UUID"))
}

fn validate_objective(objective: &str) -> Result<(), String> {
    if objective.trim().is_empty() || objective.len() > MAX_OBJECTIVE_BYTES {
        return Err(format!(
            "loop_control.objective must contain non-whitespace text and be at most {MAX_OBJECTIVE_BYTES} UTF-8 bytes"
        ));
    }
    Ok(())
}

fn validate_bound(field: &str, value: u32, ceiling: u32) -> Result<(), String> {
    if !(1..=ceiling).contains(&value) {
        return Err(format!(
            "loop_control.{field} must be between 1 and {ceiling}"
        ));
    }
    Ok(())
}

/// Parse strict action fields and validate only state-independent constraints.
pub fn parse_loop_control_request(arguments: &str) -> Result<LoopControlRequest, String> {
    let request: LoopControlRequest = serde_json::from_str(arguments)
        .map_err(|error| format!("invalid loop_control arguments: {error}"))?;
    request.validate()?;
    Ok(request)
}
