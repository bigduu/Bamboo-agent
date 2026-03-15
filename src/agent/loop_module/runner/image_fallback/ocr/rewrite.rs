use crate::agent::core::storage::AttachmentReader;
use crate::agent::core::ImageOcrResult;
use crate::agent::llm::models::ContentPart;

pub(super) async fn rewrite_parts_to_ocr_text(
    attachment_reader: Option<&dyn AttachmentReader>,
    parts: &[ContentPart],
    cached: Option<&[ImageOcrResult]>,
) -> std::result::Result<String, String> {
    const OCR_COORDINATE_GUIDANCE: &str = "Coordinate format: (x,y,w,h) in pixels relative to the image top-left corner. Use spatial relationships (left/right/above/below/overlap) between boxes when interpreting the content.";

    let mut out = String::new();
    let mut image_index = 0usize;

    for part in parts {
        match part {
            ContentPart::Text { text } => out.push_str(text),
            ContentPart::ImageUrl { image_url } => {
                image_index += 1;
                let summary = super::super::super::placeholder::summarize_image_url(&image_url.url);

                let cached_lines = cached.and_then(|items| {
                    items
                        .iter()
                        .find(|r| r.image_url == image_url.url)
                        .map(|r| (r.lines.as_slice(), r.error.as_deref()))
                });

                let ocr_result = if let Some((lines, err)) = cached_lines {
                    if let Some(err) = err {
                        Err(err.to_string())
                    } else {
                        Ok(lines.to_vec())
                    }
                } else {
                    super::reader::ocr_image_url_to_lines(attachment_reader, &image_url.url).await
                };

                match ocr_result {
                    Ok(lines) if !lines.is_empty() => {
                        out.push_str("\n\n[OCR extracted from image ");
                        out.push_str(&image_index.to_string());
                        out.push_str(": ");
                        out.push_str(&summary);
                        out.push_str("]\n");
                        out.push_str(OCR_COORDINATE_GUIDANCE);
                        out.push('\n');
                        for l in lines {
                            out.push_str(&format!(
                                "({},{},{},{}) {}\n",
                                l.left, l.top, l.width, l.height, l.text
                            ));
                        }
                    }
                    Ok(_) => {
                        out.push_str("\n\n[OCR extracted from image ");
                        out.push_str(&image_index.to_string());
                        out.push_str(": ");
                        out.push_str(&summary);
                        out.push_str("]\n(no text detected)\n");
                    }
                    Err(err) => {
                        log::warn!(
                            "OCR failed for image {} ({}): {}",
                            image_index,
                            summary,
                            err
                        );
                        out.push_str("\n[Image omitted: ");
                        out.push_str(&summary);
                        out.push_str("]\n");
                    }
                }
            }
        }
    }

    Ok(out)
}
