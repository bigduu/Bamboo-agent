use crate::agent::llm::models::ContentPart;

pub(super) fn summarize_image_url(url: &str) -> String {
    let trimmed = url.trim();
    if trimmed.starts_with("data:") {
        // data:<mime>;base64,<data...>
        // Keep summary stable and avoid ever echoing base64 content.
        let mut mime = "unknown".to_string();
        if let Some(semi_idx) = trimmed.find(';') {
            let header = &trimmed["data:".len()..semi_idx];
            if !header.trim().is_empty() {
                mime = header.trim().to_string();
            }
        }

        let approx_bytes = trimmed
            .split_once(',')
            .map(|(_, data)| {
                let len = data.trim().len();
                // Base64 is ~4/3 expansion.
                (len.saturating_mul(3)) / 4
            })
            .unwrap_or(0);

        return format!("{mime} (~{approx_bytes} bytes)");
    }

    // For normal URLs, truncate to keep logs/responses compact.
    const MAX: usize = 120;
    if trimmed.len() <= MAX {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..MAX])
    }
}

pub(super) fn rewrite_parts_to_placeholder(parts: &[ContentPart]) -> String {
    let mut out = String::new();
    for part in parts {
        match part {
            ContentPart::Text { text } => out.push_str(text),
            ContentPart::ImageUrl { image_url } => {
                let summary = summarize_image_url(&image_url.url);
                out.push_str("\n[Image omitted: ");
                out.push_str(&summary);
                out.push_str("]\n");
            }
        }
    }
    out
}

#[cfg(any(test, windows))]
pub(super) fn persistable_image_urls(parts: &[ContentPart]) -> Vec<String> {
    parts
        .iter()
        .filter_map(|p| match p {
            ContentPart::ImageUrl { image_url } => {
                // Never persist `data:` URLs into session JSON (they can embed base64).
                let trimmed = image_url.url.trim();
                if trimmed.starts_with("data:") || trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }
            _ => None,
        })
        .collect()
}
