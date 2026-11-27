#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SourceLine<'a> {
    pub text: &'a str,
    pub start: usize,
    pub end: usize,
}

/// Iterate as `str::lines` does, retaining the byte range of each original line.
pub(super) fn source_lines(source: &str) -> impl Iterator<Item = SourceLine<'_>> {
    source.split_inclusive('\n').scan(0, |offset, raw| {
        let start = *offset;
        *offset += raw.len();
        let text = match raw.strip_suffix('\n') {
            Some(line) => line.strip_suffix('\r').unwrap_or(line),
            None => raw,
        };
        Some(SourceLine {
            text,
            start,
            end: *offset,
        })
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FenceKind<'a> {
    Code { language: Option<&'a str> },
    Mermaid { language: &'a str },
}

impl<'a> FenceKind<'a> {
    pub(super) fn language(self) -> Option<&'a str> {
        match self {
            Self::Code { language } => language,
            Self::Mermaid { language } => Some(language),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FencedBlock<'a> {
    pub kind: FenceKind<'a>,
    pub source: &'a str,
    pub closed: bool,
}

/// Read one fenced block from consecutive lines of `source`. An unfinished
/// fence keeps all body bytes seen so far, including their original line endings.
pub(super) fn take_fence<'a>(
    source: &'a str,
    opening: SourceLine<'a>,
    input: &mut impl Iterator<Item = SourceLine<'a>>,
) -> Option<FencedBlock<'a>> {
    let (marker, length, info) = fence_prefix(opening.text)?;
    if marker == b'`' && info.contains('`') {
        return None;
    }
    let language = info.split_whitespace().next();
    let kind = match language {
        Some(language) if language.eq_ignore_ascii_case("mermaid") => {
            FenceKind::Mermaid { language }
        }
        language => FenceKind::Code { language },
    };

    let mut end = opening.end;
    let mut closed = false;
    for line in input {
        if fence_prefix(line.text).is_some_and(|(closing_marker, closing_length, rest)| {
            closing_marker == marker && closing_length >= length && rest.trim().is_empty()
        }) {
            end = line.start;
            closed = true;
            break;
        }
        end = line.end;
    }
    Some(FencedBlock {
        kind,
        source: &source[opening.end..end],
        closed,
    })
}

fn fence_prefix(line: &str) -> Option<(u8, usize, &str)> {
    let line = line.trim_start();
    let marker = line.bytes().next()?;
    if !matches!(marker, b'`' | b'~') {
        return None;
    }
    let length = line.bytes().take_while(|byte| *byte == marker).count();
    (length >= 3).then_some((marker, length, &line[length..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_lines_match_standard_lines_and_keep_original_byte_ranges() {
        for source in ["", "\n", "\r\n", "a\r", "a\n", "界\r\n👩‍💻\n\nlast\r"] {
            let mut consumed = 0;
            for (line, text) in source_lines(source).zip(source.lines()) {
                assert_eq!(line.text, text);
                assert_eq!(line.start, consumed);
                assert!(source.is_char_boundary(line.end));
                consumed = line.end;
            }
            assert_eq!(source_lines(source).count(), source.lines().count());
            assert_eq!(consumed, source.len());
        }
        let source = "界\r\n👩‍💻\n";
        let lines: Vec<_> = source_lines(source).collect();
        assert_eq!(&source[lines[0].start..lines[0].end], "界\r\n");
        assert_eq!(&source[lines[1].start..lines[1].end], "👩‍💻\n");
    }

    #[test]
    fn closing_fence_requires_same_marker_length_and_only_whitespace() {
        let source = concat!(
            "````rust options\r\n",
            "  let value = \"界👩‍💻\";\r\n",
            "```\r\n",
            "~~~~\r\n",
            "```` trailing\r\n",
            "\r\n",
            "  ````` \t\r\n",
            "visible prose",
        );
        let mut input = source_lines(source);
        let opening = input.next().unwrap();
        let fenced = take_fence(source, opening, &mut input).unwrap();
        assert_eq!(
            fenced,
            FencedBlock {
                kind: FenceKind::Code {
                    language: Some("rust"),
                },
                source: "  let value = \"界👩‍💻\";\r\n```\r\n~~~~\r\n```` trailing\r\n\r\n",
                closed: true,
            }
        );
        assert_eq!(input.next().unwrap().text, "visible prose");
    }

    #[test]
    fn multiple_fences_keep_distinct_mermaid_and_code_sources() {
        let source = "  ~~~ Mermaid example\nflowchart LR\n  A --> B\n~~~~\n```\n```\nlast";
        let mut input = source_lines(source);
        let opening = input.next().unwrap();
        assert_eq!(
            take_fence(source, opening, &mut input),
            Some(FencedBlock {
                kind: FenceKind::Mermaid {
                    language: "Mermaid",
                },
                source: "flowchart LR\n  A --> B\n",
                closed: true,
            })
        );
        let opening = input.next().unwrap();
        assert_eq!(
            take_fence(source, opening, &mut input),
            Some(FencedBlock {
                kind: FenceKind::Code { language: None },
                source: "",
                closed: true,
            })
        );
        assert_eq!(input.next().unwrap().text, "last");
    }

    #[test]
    fn unfinished_fence_ends_at_the_last_available_source_byte() {
        let source = "~~~sh\r\n  echo 界\r\n👩‍💻";
        let mut input = source_lines(source);
        let opening = input.next().unwrap();
        let fenced = take_fence(source, opening, &mut input).unwrap();
        assert_eq!(fenced.source, "  echo 界\r\n👩‍💻");
        assert!(!fenced.closed);
        assert!(input.next().is_none());

        let mut limited = source_lines(source).take(2);
        let opening = limited.next().unwrap();
        let fenced = take_fence(source, opening, &mut limited).unwrap();
        assert_eq!(fenced.source, "  echo 界\r\n");
        assert!(!fenced.closed);
    }

    #[test]
    fn a_nonopening_line_does_not_advance_the_body_iterator() {
        for source in [
            "``short\nnext",
            "text with ```\nnext",
            "```rust `invalid\nnext",
        ] {
            let mut input = source_lines(source);
            let opening = input.next().unwrap();
            assert_eq!(take_fence(source, opening, &mut input), None);
            assert_eq!(input.next().unwrap().text, "next");
        }
    }
}
