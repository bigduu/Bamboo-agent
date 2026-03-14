use crate::agent::core::ImageOcrLine;

pub(super) fn extract_line_candidates(coords: Vec<rust_ocr::Coordinates>) -> Vec<ImageOcrLine> {
    // `rust_ocr::ocr_with_bounds` yields word-level coordinates and then a line-level
    // coordinate for each OCR line. We pick the line-level entries by matching them
    // against the accumulated words for that line.
    let mut out = Vec::new();
    let mut current_words: Vec<String> = Vec::new();

    for c in coords {
        let text = c.text.trim().to_string();
        if text.is_empty() {
            continue;
        }

        if !current_words.is_empty() {
            let joined = current_words.join(" ");
            if normalize_ws(&joined) == normalize_ws(&text) {
                out.push(ImageOcrLine {
                    text,
                    left: c.x.round() as i32,
                    top: c.y.round() as i32,
                    width: c.width.round() as i32,
                    height: c.height.round() as i32,
                });
                current_words.clear();
                continue;
            }
        }

        current_words.push(text);
    }

    // Fallback: if we couldn't identify lines, emit a compact word list instead.
    if out.is_empty() && !current_words.is_empty() {
        out.push(ImageOcrLine {
            text: current_words.join(" "),
            left: 0,
            top: 0,
            width: 0,
            height: 0,
        });
    }

    out
}

fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}
