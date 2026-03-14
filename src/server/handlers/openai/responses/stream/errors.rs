use crate::server::error::AppError;

pub(super) fn map_provider_error(error: impl std::fmt::Display) -> AppError {
    let err_msg = error.to_string();
    if err_msg.contains("proxy") || err_msg.contains("407") {
        AppError::ProxyAuthRequired
    } else {
        AppError::InternalError(anyhow::anyhow!("LLM error: {}", err_msg))
    }
}
