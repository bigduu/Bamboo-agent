use bamboo_agent_core::ImageOcrLine;

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

#[cfg(test)]
mod tests {
    use super::*;

    fn coord(x: f32, y: f32, width: f32, height: f32, text: &str) -> rust_ocr::Coordinates {
        rust_ocr::Coordinates {
            x,
            y,
            width,
            height,
            text: text.to_string(),
        }
    }

    #[test]
    fn extracts_empty_vec_for_empty_input() {
        let result = extract_line_candidates(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn extracts_empty_vec_for_whitespace_only_input() {
        let coords = vec![
            coord(0.0, 0.0, 100.0, 20.0, "  "),
            coord(0.0, 25.0, 100.0, 20.0, "\t\n"),
        ];
        let result = extract_line_candidates(coords);
        assert!(result.is_empty());
    }

    #[test]
    fn emits_fallback_for_words_without_line_coords() {
        let coords = vec![
            coord(10.0, 10.0, 50.0, 15.0, "Hello"),
            coord(65.0, 10.0, 50.0, 15.0, "World"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].text, "Hello World");
        assert_eq!(result[0].left, 0);
        assert_eq!(result[0].top, 0);
        assert_eq!(result[0].width, 0);
        assert_eq!(result[0].height, 0);
    }

    #[test]
    fn identifies_line_from_word_sequence() {
        let coords = vec![
            coord(10.0, 10.0, 50.0, 15.0, "Hello"),
            coord(65.0, 10.0, 50.0, 15.0, "World"),
            coord(10.0, 10.0, 105.0, 15.0, "Hello World"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].text, "Hello World");
        assert_eq!(result[0].left, 10);
        assert_eq!(result[0].top, 10);
        assert_eq!(result[0].width, 105);
        assert_eq!(result[0].height, 15);
    }

    #[test]
    fn handles_multiple_lines() {
        let coords = vec![
            coord(10.0, 10.0, 50.0, 15.0, "Line"),
            coord(60.0, 10.0, 30.0, 15.0, "1"),
            coord(10.0, 10.0, 80.0, 15.0, "Line 1"),
            coord(10.0, 30.0, 50.0, 15.0, "Line"),
            coord(60.0, 30.0, 30.0, 15.0, "2"),
            coord(10.0, 30.0, 80.0, 15.0, "Line 2"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].text, "Line 1");
        assert_eq!(result[1].text, "Line 2");
    }

    #[test]
    fn rounds_coordinates_to_integers() {
        let coords = vec![
            coord(10.6, 20.4, 100.7, 15.3, "Test"),
            coord(10.6, 20.4, 100.7, 15.3, "Test"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result[0].left, 11);
        assert_eq!(result[0].top, 20);
        assert_eq!(result[0].width, 101);
        assert_eq!(result[0].height, 15);
    }

    #[test]
    fn normalizes_whitespace_in_line_matching() {
        let coords = vec![
            coord(10.0, 10.0, 50.0, 15.0, "Hello"),
            coord(65.0, 10.0, 50.0, 15.0, "World"),
            coord(10.0, 10.0, 105.0, 15.0, "Hello  World"), // Double space
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].text, "Hello  World");
    }

    #[test]
    fn trims_whitespace_from_words() {
        let coords = vec![
            coord(10.0, 10.0, 50.0, 15.0, "  Hello  "),
            coord(65.0, 10.0, 50.0, 15.0, "  World  "),
            coord(10.0, 10.0, 105.0, 15.0, "Hello World"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].text, "Hello World");
    }

    #[test]
    fn filters_empty_words() {
        let coords = vec![
            coord(10.0, 10.0, 50.0, 15.0, ""),
            coord(65.0, 10.0, 50.0, 15.0, "Test"),
            coord(10.0, 10.0, 105.0, 15.0, "Test"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].text, "Test");
    }

    #[test]
    fn handles_single_word_fallback() {
        let coords = vec![coord(10.0, 10.0, 50.0, 15.0, "Single")];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].text, "Single");
        assert_eq!(result[0].left, 0);
        assert_eq!(result[0].top, 0);
    }

    #[test]
    fn normalize_ws_collapses_multiple_spaces() {
        assert_eq!(normalize_ws("hello   world"), "hello world");
        assert_eq!(normalize_ws("  hello  world  "), "hello world");
        assert_eq!(normalize_ws("hello\t\tworld"), "hello world");
        assert_eq!(normalize_ws("hello\n\nworld"), "hello world");
    }

    #[test]
    fn normalize_ws_handles_empty_string() {
        assert_eq!(normalize_ws(""), "");
        assert_eq!(normalize_ws("   "), "");
    }

    #[test]
    fn handles_mixed_line_and_fallback() {
        // First two words match a line, third word doesn't have matching line coord
        let coords = vec![
            coord(10.0, 10.0, 50.0, 15.0, "Matched"),
            coord(60.0, 10.0, 30.0, 15.0, "Line"),
            coord(10.0, 10.0, 80.0, 15.0, "Matched Line"),
            coord(10.0, 30.0, 50.0, 15.0, "Unmatched"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].text, "Matched Line");
        assert_eq!(result[0].left, 10); // Has coordinates from line coord
        assert_eq!(result[1].text, "Unmatched");
        assert_eq!(result[1].left, 0); // Fallback case
    }

    #[test]
    fn handles_zero_coordinates() {
        let coords = vec![
            coord(0.0, 0.0, 0.0, 0.0, "Test"),
            coord(0.0, 0.0, 0.0, 0.0, "Test"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].left, 0);
        assert_eq!(result[0].top, 0);
        assert_eq!(result[0].width, 0);
        assert_eq!(result[0].height, 0);
    }

    #[test]
    fn handles_large_coordinates() {
        let coords = vec![
            coord(10000.0, 20000.0, 300.0, 50.0, "Test"),
            coord(10000.0, 20000.0, 300.0, 50.0, "Test"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result[0].left, 10000);
        assert_eq!(result[0].top, 20000);
    }

    #[test]
    fn handles_unicode_text() {
        let coords = vec![
            coord(10.0, 10.0, 50.0, 15.0, "你好"),
            coord(65.0, 10.0, 50.0, 15.0, "世界"),
            coord(10.0, 10.0, 105.0, 15.0, "你好 世界"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].text, "你好 世界");
    }

    #[test]
    fn handles_special_characters() {
        let coords = vec![
            coord(10.0, 10.0, 50.0, 15.0, "function()"),
            coord(65.0, 10.0, 50.0, 15.0, "{}"),
            coord(10.0, 10.0, 105.0, 15.0, "function() {}"),
        ];
        let result = extract_line_candidates(coords);

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].text, "function() {}");
    }
}
