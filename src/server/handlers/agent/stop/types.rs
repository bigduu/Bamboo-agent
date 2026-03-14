use serde::Serialize;

/// Response for stop request.
#[derive(Serialize)]
pub(super) struct StopResponse {
    /// Whether the stop operation succeeded
    pub(super) success: bool,
    /// Human-readable status message
    pub(super) message: String,
}
