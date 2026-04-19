use bamboo_agent_core::storage::AttachmentReader;
use bamboo_agent_core::MessagePart;
use bamboo_agent_core::{ImageOcrResult, Session};

mod cache;
#[cfg(windows)]
mod line_extraction;
#[cfg(windows)]
mod reader;
#[cfg(windows)]
mod rewrite;

pub(super) async fn ensure_session_image_ocr_cached(
    session: &mut Session,
    attachment_reader: Option<&dyn AttachmentReader>,
) -> bool {
    cache::ensure_session_image_ocr_cached(session, attachment_reader).await
}

#[cfg(windows)]
pub(super) async fn rewrite_parts_to_ocr_text(
    attachment_reader: Option<&dyn AttachmentReader>,
    parts: &[MessagePart],
    cached: Option<&[ImageOcrResult]>,
) -> std::result::Result<String, String> {
    rewrite::rewrite_parts_to_ocr_text(attachment_reader, parts, cached).await
}
