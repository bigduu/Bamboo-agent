use crate::agent::core::storage::AttachmentReader;
use crate::agent::core::{ImageOcrResult, Session};
use crate::agent::llm::models::ContentPart;

mod cache;
mod line_extraction;
mod reader;
mod rewrite;

pub(super) async fn ensure_session_image_ocr_cached(
    session: &mut Session,
    attachment_reader: Option<&dyn AttachmentReader>,
) -> bool {
    cache::ensure_session_image_ocr_cached(session, attachment_reader).await
}

pub(super) async fn rewrite_parts_to_ocr_text(
    attachment_reader: Option<&dyn AttachmentReader>,
    parts: &[ContentPart],
    cached: Option<&[ImageOcrResult]>,
) -> std::result::Result<String, String> {
    rewrite::rewrite_parts_to_ocr_text(attachment_reader, parts, cached).await
}
