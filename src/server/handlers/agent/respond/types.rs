use serde::Deserialize;

/// Request payload for submitting a user response.
///
/// # Fields
///
/// * `response` - The user's response text or selected option
#[derive(Debug, Deserialize)]
pub struct RespondRequest {
    /// The user's response - either one of the options or custom input
    pub response: String,
}
