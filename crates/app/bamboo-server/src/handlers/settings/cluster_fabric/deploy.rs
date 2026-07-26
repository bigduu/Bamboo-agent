//! HTTP-facing thin wrappers over the shared [`bamboo_server_tools::FabricDeployer`].
//!
//! All deploy/stop/test/logs orchestration (and the worker registry) lives in
//! the engine on `AppState`, shared with the `cluster` agent tool so both
//! surfaces manage the SAME workers. These wrappers only adapt the engine's
//! [`FabricError`] to the HTTP [`AppError`].

use bamboo_config::cluster_fabric::NodeState;
use bamboo_server_tools::{FabricActionResult, FabricError};

use crate::app_state::AppState;
use crate::error::AppError;

fn map_err(e: FabricError) -> AppError {
    match e {
        FabricError::NotFound(m) => AppError::NotFound(m),
        FabricError::BadRequest(m) => AppError::BadRequest(m),
        FabricError::Conflict { expected, actual } => AppError::ConfigConflict { expected, actual },
        FabricError::Committed(m) => AppError::InternalError(anyhow::anyhow!(m)),
        FabricError::Internal(m) => AppError::InternalError(anyhow::anyhow!(m)),
    }
}

pub async fn deploy_node(
    app_state: &AppState,
    node_id: &str,
    echo: bool,
    expected_revision: u64,
) -> Result<FabricActionResult<NodeState>, AppError> {
    app_state
        .fabric_deployer
        .deploy_at_revision(node_id, echo, expected_revision)
        .await
        .map_err(map_err)
}

pub async fn stop_node(
    app_state: &AppState,
    node_id: &str,
    expected_revision: u64,
) -> Result<FabricActionResult<NodeState>, AppError> {
    app_state
        .fabric_deployer
        .stop_at_revision(node_id, expected_revision)
        .await
        .map_err(map_err)
}

pub async fn test_node(
    app_state: &AppState,
    node_id: &str,
    expected_revision: u64,
) -> Result<FabricActionResult<String>, AppError> {
    app_state
        .fabric_deployer
        .test_at_revision(node_id, expected_revision)
        .await
        .map_err(map_err)
}

pub async fn read_logs(
    app_state: &AppState,
    node_id: &str,
    lines: usize,
) -> Result<String, AppError> {
    app_state
        .fabric_deployer
        .read_logs(node_id, lines)
        .await
        .map_err(map_err)
}
