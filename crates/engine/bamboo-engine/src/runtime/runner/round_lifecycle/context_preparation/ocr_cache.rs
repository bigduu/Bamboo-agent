use crate::runtime::config::AgentLoopConfig;
use bamboo_agent_core::Session;

#[cfg(windows)]
use super::super::super::image_fallback::ensure_session_image_ocr_cached;

/// Populate OCR-derived message state without persisting it independently.
///
/// Retrieval-window uses this on its staged Session so the cache and archive
/// boundary become durable in one fail-closed checkpoint.
pub(super) async fn cache_ocr_results_in_session(
    session: &mut Session,
    config: &AgentLoopConfig,
) -> bool {
    #[cfg(not(windows))]
    {
        let _ = (session, config);
        false
    }

    #[cfg(windows)]
    {
        if matches!(
            config.image_fallback,
            Some(ref cfg) if cfg.mode == crate::runtime::config::ImageFallbackMode::Ocr
        ) {
            ensure_session_image_ocr_cached(session, config.attachment_reader.as_deref()).await
        } else {
            false
        }
    }
}

pub(super) async fn maybe_cache_ocr_results(
    session: &mut Session,
    config: &AgentLoopConfig,
    session_id: &str,
) {
    // If OCR fallback is enabled, compute + cache OCR results into the persisted session
    // (but do NOT rewrite message parts). This keeps OCR available for the UI while
    // also allowing the LLM request to be built from text-only projections.
    let changed = cache_ocr_results_in_session(session, config).await;
    if changed {
        if let Some(ref persistence) = config.persistence {
            if let Err(error) = persistence.save_runtime_session(session).await {
                tracing::warn!(
                    "[{}] Failed to save session after OCR caching: {}",
                    session_id,
                    error
                );
            }
        }
    }
}
