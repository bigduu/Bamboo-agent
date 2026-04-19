use bamboo_agent_core::storage::AttachmentReader;
use bamboo_agent_core::{ImageOcrResult, Session};

pub(super) async fn ensure_session_image_ocr_cached(
    session: &mut Session,
    attachment_reader: Option<&dyn AttachmentReader>,
) -> bool {
    let Some(reader) = attachment_reader else {
        return false;
    };

    let mut changed = false;

    for msg in session.messages.iter_mut() {
        let Some(parts) = msg.content_parts.as_ref() else {
            continue;
        };

        let image_urls = super::super::persistable_image_urls(parts);
        if image_urls.is_empty() {
            continue;
        }

        let mut results = msg.image_ocr.take().unwrap_or_default();
        for url in image_urls {
            let already = results.iter().any(|r| r.image_url == url);
            if already {
                continue;
            }

            match super::reader::ocr_image_url_to_lines(Some(reader), url.as_str()).await {
                Ok(lines) => {
                    results.push(ImageOcrResult {
                        image_url: url,
                        lines,
                        error: None,
                    });
                }
                Err(err) => {
                    results.push(ImageOcrResult {
                        image_url: url,
                        lines: Vec::new(),
                        error: Some(err),
                    });
                }
            }
            changed = true;
        }

        if results.is_empty() {
            msg.image_ocr = None;
        } else {
            msg.image_ocr = Some(results);
        }
    }

    changed
}
