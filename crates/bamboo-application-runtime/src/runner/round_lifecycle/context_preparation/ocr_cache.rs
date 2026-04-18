use bamboo_application_agent::Session;
use crate::config::AgentLoopConfig;

#[cfg(windows)]
use super::super::super::image_fallback::ensure_session_image_ocr_cached;

pub(super) async fn maybe_cache_ocr_results(
    session: &mut Session,
    config: &AgentLoopConfig,
    session_id: &str,
) {
    #[cfg(not(windows))]
    let _ = (session, config, session_id);

    // If OCR fallback is enabled, compute + cache OCR results into the persisted session
    // (but do NOT rewrite message parts). This keeps OCR available for the UI while
    // also allowing the LLM request to be built from text-only projections.
    #[cfg(windows)]
    if matches!(
        config.image_fallback,
        Some(ref cfg) if cfg.mode == crate::config::ImageFallbackMode::Ocr
    ) {
        let changed =
            ensure_session_image_ocr_cached(session, config.attachment_reader.as_deref()).await;
        if changed {
            if let Some(ref storage) = config.storage {
                if let Err(error) = storage.save_session(session).await {
                    tracing::warn!(
                        "[{}] Failed to save session after OCR caching: {}",
                        session_id,
                        error
                    );
                }
            }
        }
    }
}
