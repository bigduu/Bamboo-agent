use base64::Engine;

use bamboo_agent_core::storage::AttachmentReader;
use bamboo_agent_core::ImageOcrLine;

pub(super) async fn ocr_image_url_to_lines(
    attachment_reader: Option<&dyn AttachmentReader>,
    url: &str,
) -> std::result::Result<Vec<ImageOcrLine>, String> {
    let (mime, bytes) = if let Some((mime, data)) = parse_data_url_base64(url) {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data.as_bytes())
            .map_err(|e| format!("invalid base64 data: {e}"))?;
        (mime, bytes)
    } else if let Some((session_id, attachment_id)) =
        super::super::attachment_urls::parse_bamboo_attachment_url(url)
    {
        let Some(reader) = attachment_reader else {
            return Err(
                "cannot resolve bamboo-attachment URL without an attachment reader".to_string(),
            );
        };
        match reader
            .read_attachment(session_id, attachment_id)
            .await
            .map_err(|e| format!("failed reading attachment: {e}"))?
        {
            Some((bytes, mime)) => (mime, bytes),
            None => return Err("attachment not found".to_string()),
        }
    } else {
        return Err("unsupported image URL (expected data: or bamboo-attachment:)".to_string());
    };

    if mime != "image/png" {
        return Err(format!(
            "unsupported mime type '{mime}' (only image/png is supported)"
        ));
    }

    // Basic validation to avoid passing junk into the decoder.
    const PNG_SIG: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
    if bytes.len() < PNG_SIG.len() || bytes[..PNG_SIG.len()] != PNG_SIG {
        return Err("decoded data is not a PNG".to_string());
    }

    // rust_ocr currently expects a PNG file path (it uses BitmapDecoder::PngDecoderId()).
    let tmp_path = std::env::temp_dir().join(format!("bamboo_ocr_{}.png", uuid::Uuid::new_v4()));
    std::fs::write(&tmp_path, &bytes).map_err(|e| format!("failed writing tmp png: {e}"))?;

    // WinRT OCR can block; keep it off the async executor.
    let tmp_path2 = tmp_path.clone();
    let coords = tokio::task::spawn_blocking(move || {
        // `rust_ocr` returns `Box<dyn Error>` which is not `Send`, so we must not
        // return it across the thread boundary. Convert to `String` inside the
        // blocking closure.
        rust_ocr::ocr_with_bounds(tmp_path2, None).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| format!("ocr task join failed: {e}"))?
    .map_err(|e| format!("ocr failed: {e}"))?;

    let _ = std::fs::remove_file(&tmp_path);

    Ok(super::line_extraction::extract_line_candidates(coords))
}

fn parse_data_url_base64(url: &str) -> Option<(String, String)> {
    // data:<mime>;base64,<data...>
    let trimmed = url.trim();
    if !trimmed.starts_with("data:") {
        return None;
    }
    let (header, data) = trimmed.split_once(',')?;
    if !header.contains(";base64") {
        return None;
    }
    let mime = header
        .strip_prefix("data:")?
        .split(';')
        .next()
        .unwrap_or("application/octet-stream")
        .trim()
        .to_string();
    Some((mime, data.trim().to_string()))
}
