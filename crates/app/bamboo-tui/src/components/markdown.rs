use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

use crate::text::{clip_cells, display_width, truncate_cells};
use crate::theme::colors;

mod fence;
mod table;

use fence::FencedBlock;

const MAX_PREVIEW_BYTES: usize = 128 * 1024;
const MAX_PREVIEW_SOURCE_LINES: usize = 2_048;
const MAX_FENCE_PREVIEW_ROWS: usize = 64;
const MAX_FENCE_LABEL_CELLS: usize = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CopyableFence<'a> {
    pub language: Option<&'a str>,
    pub source: &'a str,
}

/// Return the bounded preview source and the exact bodies of its closed fences.
pub(crate) fn copyable_fences(text: &str) -> (&str, Vec<CopyableFence<'_>>) {
    let source = preview_source(text);
    let mut input = fence::source_lines(source);
    let mut fences = Vec::new();
    while let Some(line) = input.next() {
        if let Some(block) =
            fence::take_fence(source, line, &mut input).filter(|block| block.closed)
        {
            fences.push(CopyableFence {
                language: block.kind.language(),
                source: block.source,
            });
        }
    }
    (source, fences)
}

/// Render markdown text into a list of ratatui Lines with styling.
/// Handles: backtick and tilde fences, inline `code`, **bold**, _italic_,
/// links, image descriptions, ATX and single-line Setext headings, nested
/// bullet and ordered lists, task lists, thematic breaks, quotes and tables.
pub fn render_markdown(text: &str, width: u16) -> Vec<Line<'static>> {
    render_markdown_preview(text, width, false)
}

pub(crate) fn render_streaming_markdown(text: &str, width: u16) -> Vec<Line<'static>> {
    render_markdown_preview(text, width, true)
}

fn render_markdown_preview(text: &str, width: u16, streaming: bool) -> Vec<Line<'static>> {
    let width = width as usize;
    if width == 0 {
        return Vec::new();
    }
    let source = preview_source(text);
    let mut lines: Vec<Line> = Vec::new();

    let mut input = fence::source_lines(source).peekable();
    let mut after_prose = false;
    let mut container = ParagraphContainer::None;
    while let Some(line) = input.next() {
        let follows_prose = std::mem::take(&mut after_prose);
        let previous_container = std::mem::take(&mut container);
        let raw_line = line.text;
        let body = raw_line.trim_start();
        let raw_indent = &raw_line[..raw_line.len() - body.len()];
        let indentation = column_after(0, raw_indent);
        if let Some(block) = fence::take_fence(source, line, &mut input) {
            render_code_block(&mut lines, block, width);
            container = previous_container.after_indented_block(indentation);
            continue;
        }

        if let Some(rows) = table::render_if_table(raw_line, &mut input, width) {
            lines.extend(rows);
            container = previous_container.after_indented_block(indentation);
            continue;
        }

        if let Some(markers) = rule_markers(raw_line) {
            container = previous_container.after_indented_block(indentation);
            let possible_setext = follows_prose && markers.contiguous_dashes;
            if markers.forms_break() && !possible_setext {
                lines.push(Line::styled(
                    "─".repeat(width),
                    Style::default().fg(colors::inactive()),
                ));
            } else {
                lines.push(Line::raw(raw_line.to_owned()));
                after_prose = !possible_setext;
                if matches!(raw_line.trim(), "-" | "*") {
                    if let Some(anchor) = list_anchor(raw_line) {
                        container = previous_container.after_list(indentation, &anchor);
                    }
                }
            }
            continue;
        }

        let indent = raw_indent.replace('\t', "  ");

        // Headings: 1–6 leading '#'.
        if let Some((level, content)) = heading(body) {
            let line = if content.contains('`') || content.contains('*') {
                Line::from(parse_inline(content)).style(heading_style(level))
            } else {
                Line::from(Span::styled(
                    render_heading_images(content),
                    heading_style(level),
                ))
            };
            lines.push(line);
            container = previous_container.after_indented_block(indentation);
            continue;
        }

        // Blockquote.
        let quote = body.strip_prefix('>');
        let quote_can_continue = quote.is_some_and(paragraph_can_be_lazy);
        if let Some(content) = body
            .strip_prefix("> ")
            .or_else(|| body.eq("> ").then_some(""))
        {
            let mut spans = vec![Span::styled(
                format!("{indent}│ "),
                Style::default().fg(colors::thinking()),
            )];
            spans.extend(parse_inline(content));
            lines.push(Line::from(spans));
            container = previous_container.after_quote(indentation, quote_can_continue);
            continue;
        }

        if let Some(anchor) = list_anchor(raw_line) {
            container = previous_container.after_list(indentation, &anchor);
            if let Some(item) = render_list_item(&anchor, &indent) {
                lines.push(item);
                continue;
            }
        } else if quote.is_some() {
            container = previous_container.after_quote(indentation, quote_can_continue);
        } else {
            container = previous_container.after_plain_line(body, indentation);
        }

        if !follows_prose && container == ParagraphContainer::None {
            if let Some(content) = setext_title(raw_line) {
                let level = input
                    .peek()
                    .filter(|underline| !streaming || source.as_bytes()[underline.end - 1] == b'\n')
                    .and_then(|underline| setext_level(underline.text));
                if let Some(level) = level {
                    input.next();
                    lines.push(Line::from(parse_inline(content)).style(heading_style(level)));
                    continue;
                }
            }
        }

        if raw_line.is_empty() {
            lines.push(Line::raw(""));
        } else {
            let mut spans = Vec::new();
            if !indent.is_empty() {
                spans.push(Span::raw(indent));
            }
            spans.extend(parse_inline(body));
            lines.push(Line::from(spans));
            after_prose = !body.is_empty() && (quote.is_none() || quote_can_continue);
        }
    }

    if source.len() < text.len() {
        lines.push(Line::styled(
            clip_cells("… Markdown preview clipped", width),
            Style::default().fg(colors::subtle()),
        ));
    }

    lines
}

/// Keep ambiguous lazy continuations and indented list lines out of the
/// single-line Setext path.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum ParagraphContainer {
    #[default]
    None,
    Quote,
    List {
        content_column: usize,
        lazy: bool,
    },
}

impl ParagraphContainer {
    fn enclosing_list(self, indentation: usize) -> Option<usize> {
        match self {
            Self::List { content_column, .. } if indentation >= content_column => {
                Some(content_column)
            }
            _ => None,
        }
    }

    fn after_indented_block(self, indentation: usize) -> Self {
        self.enclosing_list(indentation)
            .map_or(Self::None, |content_column| Self::List {
                content_column,
                lazy: false,
            })
    }

    fn after_quote(self, indentation: usize, lazy: bool) -> Self {
        let outer = if lazy { Self::Quote } else { Self::None };
        self.enclosing_list(indentation)
            .map_or(outer, |content_column| Self::List {
                content_column,
                lazy,
            })
    }

    fn after_list(self, indentation: usize, anchor: &ListAnchor<'_>) -> Self {
        let content_column = self
            .enclosing_list(indentation)
            .unwrap_or_else(|| anchor.continuation_column());
        Self::List {
            content_column,
            lazy: paragraph_can_be_lazy(anchor.rest),
        }
    }

    fn after_plain_line(self, body: &str, indentation: usize) -> Self {
        match self {
            Self::List { content_column, .. } if body.is_empty() => Self::List {
                content_column,
                lazy: false,
            },
            Self::List {
                content_column,
                lazy,
            } if lazy || indentation >= content_column => Self::List {
                content_column,
                lazy: true,
            },
            Self::Quote if !body.is_empty() => Self::Quote,
            _ => Self::None,
        }
    }
}

fn top_level_body(line: &str) -> Option<&str> {
    let body = line.trim_start_matches(' ');
    (line.len() - body.len() <= 3 && !body.starts_with('\t')).then_some(body)
}

fn setext_title(line: &str) -> Option<&str> {
    let title = top_level_body(line)?.trim_end_matches([' ', '\t']);
    let reference_definition = title
        .strip_prefix('[')
        .and_then(|text| text.split_once("]:"))
        .is_some_and(|(label, _)| !label.trim().is_empty() && !label.contains(['[', ']']));
    (!title.is_empty()
        && !title.starts_with('>')
        && !is_atx_heading_anchor(title)
        && !reference_definition
        && !obvious_html_block_anchor(title))
    .then_some(title)
}

fn is_atx_heading_anchor(body: &str) -> bool {
    let after_hashes = body.trim_start_matches('#');
    let hashes = body.len() - after_hashes.len();
    (1..=6).contains(&hashes) && (after_hashes.is_empty() || after_hashes.starts_with([' ', '\t']))
}

fn paragraph_can_be_lazy(content: &str) -> bool {
    let body = content.trim_start_matches([' ', '\t']);
    !body.is_empty() && !is_atx_heading_anchor(body)
}

/// Leave obvious HTML block anchors on the existing literal path.
fn obvious_html_block_anchor(line: &str) -> bool {
    let Some(markup) = line.strip_prefix('<') else {
        return false;
    };
    if markup.starts_with(['!', '?']) {
        return true;
    }
    let tag = markup.strip_prefix('/').unwrap_or(markup);
    if !tag.as_bytes().first().is_some_and(u8::is_ascii_alphabetic) {
        return false;
    }
    let rest = tag.trim_start_matches(|ch: char| ch.is_ascii_alphanumeric() || ch == '-');
    if !(rest.is_empty() || rest.starts_with([' ', '\t', '>']) || rest.starts_with("/>")) {
        return false;
    }
    let name = &tag[..tag.len() - rest.len()];
    const BLOCK_TAGS: &[&str] = &[
        "address",
        "article",
        "aside",
        "base",
        "basefont",
        "blockquote",
        "body",
        "caption",
        "center",
        "col",
        "colgroup",
        "dd",
        "details",
        "dialog",
        "dir",
        "div",
        "dl",
        "dt",
        "fieldset",
        "figcaption",
        "figure",
        "footer",
        "form",
        "frame",
        "frameset",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "head",
        "header",
        "hr",
        "html",
        "iframe",
        "legend",
        "li",
        "link",
        "main",
        "menu",
        "menuitem",
        "nav",
        "noframes",
        "ol",
        "optgroup",
        "option",
        "p",
        "param",
        "pre",
        "script",
        "search",
        "section",
        "style",
        "summary",
        "table",
        "tbody",
        "td",
        "textarea",
        "tfoot",
        "th",
        "thead",
        "title",
        "tr",
        "track",
        "ul",
    ];
    BLOCK_TAGS.iter().any(|tag| tag.eq_ignore_ascii_case(name))
        || (tag.ends_with('>') && !tag.contains('<'))
}

fn setext_level(line: &str) -> Option<usize> {
    let markers = top_level_body(line)?.trim_end_matches([' ', '\t']);
    let marker = markers.bytes().next()?;
    let level = match marker {
        b'=' => 1,
        b'-' => 2,
        _ => return None,
    };
    markers.bytes().all(|byte| byte == marker).then_some(level)
}

fn preview_source(text: &str) -> &str {
    let bytes = text.as_bytes();
    // End on a line boundary when cutting a byte-limited preview. A partial
    // line at the budget boundary could otherwise look like a closing fence.
    let byte_end = if bytes.len() <= MAX_PREVIEW_BYTES {
        bytes.len()
    } else {
        bytes[..MAX_PREVIEW_BYTES]
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |position| position + 1)
    };
    let line_end = bytes[..byte_end]
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'\n')
        .nth(MAX_PREVIEW_SOURCE_LINES - 1)
        .map_or(byte_end, |(position, _)| position + 1);
    &text[..line_end]
}

struct RuleMarkers {
    count: usize,
    contiguous_dashes: bool,
}

impl RuleMarkers {
    fn forms_break(&self) -> bool {
        self.count >= 3
    }
}

/// Classify top-level marker-only lines, including unfinished streaming input.
fn rule_markers(line: &str) -> Option<RuleMarkers> {
    let body = top_level_body(line)?;
    let markers = body.trim_end_matches([' ', '\t']);
    let marker = markers.bytes().next()?;
    if !matches!(marker, b'-' | b'*' | b'_') {
        return None;
    }
    let mut count = 0;
    for byte in markers.bytes() {
        if byte == marker {
            count += 1;
        } else if !matches!(byte, b' ' | b'\t') {
            return None;
        }
    }
    Some(RuleMarkers {
        count,
        contiguous_dashes: marker == b'-' && count == markers.len(),
    })
}

/// If `body` is an ATX heading, return `(level, content)`.
fn heading(body: &str) -> Option<(usize, &str)> {
    let hashes = body.chars().take_while(|&c| c == '#').count();
    if (1..=6).contains(&hashes) {
        let rest = &body[hashes..];
        if let Some(content) = rest.strip_prefix(' ') {
            return Some((hashes, content));
        }
    }
    None
}

fn heading_style(level: usize) -> Style {
    let bold = Style::default().add_modifier(Modifier::BOLD);
    match level {
        1 => bold.fg(colors::brand()),
        2 => bold,
        _ => bold.fg(colors::inactive()),
    }
}

struct ListAnchor<'a> {
    indent: &'a str,
    marker: &'a str,
    rest: &'a str,
}

impl<'a> ListAnchor<'a> {
    fn task(&self) -> Option<(char, &'a str)> {
        let content = self.rest.trim_start_matches([' ', '\t']);
        let task = task_item(content)?;
        // Apply the 1–4 column marker padding within this line. Container
        // nesting is handled by the existing line-oriented renderer.
        let (_, padding) = self.marker_and_padding_columns(content);
        (padding <= 4).then_some(task)
    }

    fn marker_and_padding_columns(&self, content: &str) -> (usize, usize) {
        let padding = &self.rest[..self.rest.len() - content.len()];
        let marker_end = column_after(0, self.indent) + self.marker.len();
        (marker_end, column_after(marker_end, padding) - marker_end)
    }

    fn continuation_column(&self) -> usize {
        let content = self.rest.trim_start_matches([' ', '\t']);
        let (marker_end, padding) = self.marker_and_padding_columns(content);
        let padding = if content.is_empty() || !(1..=4).contains(&padding) {
            1
        } else {
            padding
        };
        marker_end + padding
    }
}

fn column_after(start: usize, text: &str) -> usize {
    text.chars().fold(start, |column, character| {
        if character == '\t' {
            column + (4 - column % 4)
        } else {
            column + 1
        }
    })
}

fn list_anchor(line: &str) -> Option<ListAnchor<'_>> {
    let body = line.trim_start();
    let marker_len = if body.starts_with(['-', '*', '+']) {
        1
    } else {
        let digits = body.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 || !matches!(body.as_bytes().get(digits), Some(b'.' | b')')) {
            return None;
        }
        digits + 1
    };
    let (marker, rest) = body.split_at(marker_len);
    (rest.is_empty() || rest.starts_with([' ', '\t'])).then_some(ListAnchor {
        indent: &line[..line.len() - body.len()],
        marker,
        rest,
    })
}

/// The ordinary ordered-list syntax already supported by the renderer.
fn ordered_item(line: &str) -> Option<(&str, &str)> {
    let anchor = list_anchor(line)?;
    Some((
        anchor.marker.strip_suffix('.')?,
        anchor.rest.strip_prefix(' ')?,
    ))
}

fn render_list_item(anchor: &ListAnchor<'_>, indent: &str) -> Option<Line<'static>> {
    let (prefix, content) = match (anchor.marker, anchor.task()) {
        (marker, Some((box_mark, text))) if marker.len() > 1 => {
            (format!("{indent}  {marker} {box_mark} "), text)
        }
        (_, Some((box_mark, text))) => (format!("{indent}  {box_mark} "), text),
        (marker, None) if marker.ends_with('.') => (
            format!("{indent}  {marker} "),
            anchor.rest.strip_prefix(' ')?,
        ),
        ("-" | "*", None) => (format!("{indent}  • "), anchor.rest.strip_prefix(' ')?),
        _ => return None,
    };
    let mut spans = vec![Span::styled(prefix, Style::default().fg(colors::subtle()))];
    spans.extend(parse_inline(content));
    Some(Line::from(spans))
}

fn task_item(content: &str) -> Option<(char, &str)> {
    let box_mark = match content.as_bytes() {
        [b'[', b' ', b']', ..] => '☐',
        [b'[', b'x' | b'X', b']', ..] => '☑',
        _ => return None,
    };
    let rest = &content[3..];
    (rest.is_empty() || rest.starts_with(char::is_whitespace))
        .then_some((box_mark, rest.trim_start()))
}

fn render_code_block(lines: &mut Vec<Line>, block: FencedBlock<'_>, width: usize) {
    let framed = width >= 4;
    let inner_width = if framed { width - 4 } else { width };
    let border_style = Style::default().fg(colors::code_border());
    if framed {
        let language = block.kind.language().unwrap_or_default();
        let label = clip_cells(language, inner_width.min(MAX_FENCE_LABEL_CELLS));
        let label = if label.is_empty() {
            label
        } else {
            format!(" {label} ")
        };
        let rule = "─".repeat(width - 2 - display_width(&label));
        lines.push(Line::styled(format!("┌{label}{rule}┐"), border_style));
    }

    let row = |code: &str, style| {
        let code = code.replace('\t', "    ");
        let cells = if framed {
            format!("│ {} │", truncate_cells(&code, inner_width))
        } else {
            clip_cells(&code, inner_width)
        };
        Line::styled(cells, style)
    };
    let mut code = block.source.lines();
    let code_style = Style::default().fg(colors::inactive());
    lines.extend(
        code.by_ref()
            .take(MAX_FENCE_PREVIEW_ROWS)
            .map(|line| row(line, code_style)),
    );
    if code.next().is_some() {
        lines.push(row("… more code", Style::default().fg(colors::subtle())));
    }

    if framed {
        let rule = "─".repeat(width - 2);
        lines.push(Line::styled(format!("└{rule}┘"), border_style));
    }
}

/// Parse inline formatting: `code`, **bold**, _italic_ / *italic*,
/// links, image descriptions, and plain text into Spans.
fn parse_inline(text: &str) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    let mut chars = text.chars().peekable();
    let mut current = String::new();

    macro_rules! flush {
        () => {
            if !current.is_empty() {
                spans.push(Span::raw(std::mem::take(&mut current)));
            }
        };
    }

    let mut previous_escape = false;
    while let Some(ch) = chars.next() {
        let escaped = previous_escape;
        previous_escape = ch == '\\' && !escaped;
        // Inline code.
        if ch == '`' {
            flush!();
            let code = take_until(&mut chars, |c| c == '`');
            spans.push(Span::styled(
                code,
                Style::default()
                    .fg(colors::inactive())
                    .add_modifier(Modifier::REVERSED),
            ));
            continue;
        }

        if ch == '!' && !escaped && chars.next_if_eq(&'[').is_some() {
            current.push_str(&take_inline_image(&mut chars));
            continue;
        }

        // Link: [text](url) — show the text (underlined) then the url dimmed.
        if ch == '[' {
            if let Some((label, url)) = parse_link(&mut chars) {
                flush!();
                spans.push(Span::styled(
                    label,
                    Style::default()
                        .fg(colors::brand())
                        .add_modifier(Modifier::UNDERLINED),
                ));
                if !url.is_empty() {
                    spans.push(Span::styled(
                        format!(" ({url})"),
                        Style::default().fg(colors::subtle()),
                    ));
                }
                continue;
            }
            // Not a link — treat '[' as literal (the rest re-parses normally).
            current.push('[');
            continue;
        }

        // Bold (**).
        if ch == '*' && chars.peek() == Some(&'*') {
            chars.next();
            flush!();
            let bold = take_until_pair(&mut chars, '*');
            spans.push(Span::styled(
                bold,
                Style::default().add_modifier(Modifier::BOLD),
            ));
            continue;
        }

        // Italic (single * or _).
        if ch == '*' || ch == '_' {
            flush!();
            let italic = take_until(&mut chars, |c| c == ch);
            spans.push(Span::styled(
                italic,
                Style::default().add_modifier(Modifier::ITALIC),
            ));
            continue;
        }

        current.push(ch);
    }

    if !current.is_empty() {
        spans.push(Span::raw(current));
    }

    spans
}

/// Plain headings keep their existing literal text around images.
fn render_heading_images(text: &str) -> String {
    if !text.contains("![") {
        return text.to_owned();
    }
    let mut chars = text.chars().peekable();
    let mut rendered = String::with_capacity(text.len());
    let mut previous_escape = false;
    while let Some(ch) = chars.next() {
        let escaped = previous_escape;
        previous_escape = ch == '\\' && !escaped;
        if ch == '!' && !escaped && chars.next_if_eq(&'[').is_some() {
            rendered.push_str(&take_inline_image(&mut chars));
        } else {
            rendered.push(ch);
        }
    }
    rendered
}

type InlineChars<'a> = std::iter::Peekable<std::str::Chars<'a>>;

/// Consume a direct image after `![`. Keep the consumed bytes literal until
/// the destination, optional title, and outer parenthesis are all complete.
fn take_inline_image(chars: &mut InlineChars<'_>) -> String {
    let mut literal = String::from("![");
    let label = take_image_label(chars, &mut literal);
    if chars.next_if_eq(&']').is_none() {
        // Without the outer label closer, the first complete link can
        // belong to the surrounding prose.
        if let Some((start, length)) = label.first_link {
            *chars = start;
            literal.truncate(length);
        }
        return literal;
    }
    literal.push(']');
    if chars.next_if_eq(&'(').is_none() {
        return literal;
    }
    literal.push('(');
    if !take_image_target(chars, &mut literal) || chars.next_if_eq(&')').is_none() {
        return literal;
    }

    if label.text.trim().is_empty() {
        "[Image]".into()
    } else {
        label.text
    }
}

struct ImageLabel<'a> {
    text: String,
    first_link: Option<(InlineChars<'a>, usize)>,
}

fn take_image_label<'a>(chars: &mut InlineChars<'a>, literal: &mut String) -> ImageLabel<'a> {
    let mut text = String::new();
    let mut first_link = None;
    let mut depth = 0;
    let mut nested_start = None;
    while let Some(&ch) = chars.peek() {
        if ch == ']' && depth == 0 {
            break;
        }
        if ch == '[' && depth == 0 {
            nested_start = Some((chars.clone(), literal.len(), text.len()));
        }
        chars.next();
        literal.push(ch);
        match ch {
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 && chars.next_if_eq(&'(').is_some() {
                    let target_start = literal.len();
                    literal.push('(');
                    if take_image_target(chars, literal) && chars.next_if_eq(&')').is_some() {
                        literal.push(')');
                        if let Some((start, length, text_start)) = nested_start.take() {
                            // Only the inner label belongs in the projected alt.
                            text.remove(text_start);
                            first_link.get_or_insert((start, length));
                        }
                    } else {
                        text.push(']');
                        text.push_str(&literal[target_start..]);
                    }
                    continue;
                }
            }
            '\\' => {
                if let Some(escaped) = chars.next_if(char::is_ascii_punctuation) {
                    literal.push(escaped);
                    text.push(escaped);
                    continue;
                }
            }
            _ => {}
        }
        text.push(ch);
    }
    ImageLabel { text, first_link }
}

fn take_image_target(chars: &mut InlineChars<'_>, literal: &mut String) -> bool {
    take_image_spaces(chars, literal);
    let complete = if chars.next_if_eq(&'<').is_some() {
        literal.push('<');
        take_image_delimited(chars, literal, '<', '>')
    } else {
        take_image_bare_destination(chars, literal)
    };
    if !complete {
        return false;
    }

    if !take_image_spaces(chars, literal) {
        return true;
    }
    // A title is optional. Leave other bytes for the surrounding inline scan.
    let Some(open) = chars.next_if(|ch| matches!(ch, '"' | '\'' | '(')) else {
        return true;
    };
    literal.push(open);
    let close = if open == '(' { ')' } else { open };
    if !take_image_delimited(chars, literal, open, close) {
        return false;
    }
    take_image_spaces(chars, literal);
    true
}

fn take_image_spaces(chars: &mut InlineChars<'_>, literal: &mut String) -> bool {
    let start = literal.len();
    literal.extend(std::iter::from_fn(|| {
        chars.next_if(|ch| matches!(ch, ' ' | '\t'))
    }));
    literal.len() > start
}

fn take_image_bare_destination(chars: &mut InlineChars<'_>, literal: &mut String) -> bool {
    let mut depth = 0;
    while let Some(ch) = chars.next_if(|ch| !matches!(ch, ' ' | '\t') && (*ch != ')' || depth > 0))
    {
        literal.push(ch);
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            '\\' => literal.extend(chars.next_if(char::is_ascii_punctuation)),
            _ => {}
        }
    }
    depth == 0
}

fn take_image_delimited(
    chars: &mut InlineChars<'_>,
    literal: &mut String,
    open: char,
    close: char,
) -> bool {
    while let Some(ch) = chars.next() {
        literal.push(ch);
        if ch == close {
            return true;
        }
        if ch == open {
            return false;
        }
        if ch == '\\' {
            literal.extend(chars.next_if(char::is_ascii_punctuation));
        }
    }
    false
}

/// Consume chars until `stop` returns true (consuming the terminator).
fn take_until(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    stop: impl Fn(char) -> bool,
) -> String {
    let mut out = String::new();
    while let Some(&c) = chars.peek() {
        chars.next();
        if stop(c) {
            break;
        }
        out.push(c);
    }
    out
}

/// Consume until a doubled `marker` (`**`).
fn take_until_pair(chars: &mut std::iter::Peekable<std::str::Chars>, marker: char) -> String {
    let mut out = String::new();
    while let Some(c) = chars.next() {
        if c == marker && chars.peek() == Some(&marker) {
            chars.next();
            break;
        }
        out.push(c);
    }
    out
}

/// Parse the remainder of a link after the opening `[`: `text](url)`. Probes on
/// a clone and only advances `chars` on a full match, so a bare `[` is left
/// intact for literal rendering.
fn parse_link(chars: &mut std::iter::Peekable<std::str::Chars>) -> Option<(String, String)> {
    let mut probe = chars.clone();
    let mut label = String::new();
    let mut closed = false;
    for c in probe.by_ref() {
        if c == ']' {
            closed = true;
            break;
        }
        label.push(c);
    }
    if !closed || probe.peek() != Some(&'(') {
        return None;
    }
    probe.next(); // consume '('
    let mut url = String::new();
    let mut url_closed = false;
    for c in probe.by_ref() {
        if c == ')' {
            url_closed = true;
            break;
        }
        url.push(c);
    }
    if !url_closed {
        return None;
    }
    *chars = probe; // commit only on success
    Some((label, url))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_of(line: &Line) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    fn render(md: &str) -> Vec<String> {
        render_markdown(md, 80).iter().map(text_of).collect()
    }

    #[test]
    fn headings_all_levels() {
        let out = render("# H1\n## H2\n### H3\n###### H6");
        assert_eq!(out, vec!["H1", "H2", "H3", "H6"]);
        // 7 hashes is not a heading.
        assert_eq!(render("####### nope"), vec!["####### nope"]);
    }

    #[test]
    fn setext_headings_apply_level_styles_around_existing_inline_rendering() {
        let rows = render_markdown(
            "  摘要 **重点** _斜体_ [docs](manual) ![图 🧭](plot.png) `code`  \n   =\t\nSection\n---",
            80,
        );
        assert_eq!(
            rows.iter().map(text_of).collect::<Vec<_>>(),
            ["摘要 重点 斜体 docs (manual) 图 🧭 code", "Section"]
        );
        assert_eq!(rows[0].style, heading_style(1));
        assert_eq!(rows[1].style, heading_style(2));
        for (text, modifier) in [
            ("重点", Modifier::BOLD),
            ("斜体", Modifier::ITALIC),
            ("docs", Modifier::UNDERLINED),
            ("code", Modifier::REVERSED),
        ] {
            assert!(rows[0].spans.iter().any(|span| {
                span.content == text && span.style.add_modifier.contains(modifier)
            }));
        }
    }

    #[test]
    fn streaming_setext_waits_for_a_physical_underline_newline() {
        for (partial, ending, level) in [
            ("Title 界\n=", "\n", 1),
            ("Title 界\n==\t", "\n", 1),
            ("Title 界\n---", "\n", 2),
            ("Title 界\r\n   -\t", "\r\n", 2),
        ] {
            let streaming = render_streaming_markdown(partial, 80);
            assert_eq!(
                streaming.iter().map(text_of).collect::<Vec<_>>(),
                ["Title 界", partial.rsplit('\n').next().unwrap()]
            );
            let completed = render_markdown(partial, 80);
            assert_eq!(completed.len(), 1);
            assert_eq!(text_of(&completed[0]), "Title 界");
            assert_eq!(completed[0].style, heading_style(level));
            let terminated = format!("{partial}{ending}");
            assert_eq!(render_streaming_markdown(&terminated, 80), completed);
        }
    }

    #[test]
    fn setext_leaves_multiline_and_possible_container_paragraphs_on_their_old_paths() {
        for (source, expected) in [
            ("Intro\nSecond\n---", vec!["Intro", "Second", "---"]),
            (
                "> quoted\ncontinued\n---",
                vec!["│ quoted", "continued", "---"],
            ),
            (
                "- item\n  continued\n---",
                vec!["  • item", "  continued", "---"],
            ),
            (
                "- item\n\n  nested\n  ===",
                vec!["  • item", "", "  nested", "  ==="],
            ),
        ] {
            let rows = render_markdown(source, 80);
            assert_eq!(rows.iter().map(text_of).collect::<Vec<_>>(), expected);
            assert!(rows
                .iter()
                .all(|row| !row.style.add_modifier.contains(Modifier::BOLD)));
        }
        assert_eq!(
            render("- item\n\nHeading\n==="),
            ["  • item", "", "Heading"]
        );
        assert_eq!(
            render("- item\n\n Heading\n==="),
            ["  • item", "", "Heading"]
        );
        assert_eq!(
            render("10. item\n\n   Heading\n==="),
            ["  10. item", "", "Heading"]
        );
        assert_eq!(
            render("> quoted\n\n  Heading\n==="),
            ["│ quoted", "", "Heading"]
        );
        assert_eq!(
            render("> quoted\n>\nFresh\n==="),
            ["│ quoted", ">", "Fresh"]
        );
        assert_eq!(
            render("> # Quoted heading\nFresh\n==="),
            ["│ # Quoted heading", "Fresh"]
        );
        assert_eq!(
            render("- # List heading\nFresh\n==="),
            ["  • # List heading", "Fresh"]
        );
    }

    #[test]
    fn setext_does_not_promote_block_openers_or_invalid_indentation() {
        for title in [
            "#",
            "##\tATX",
            ">quote",
            "+ item",
            "1) item",
            "1.",
            "<div>",
            "<DiV>Text</DiV>",
            "<!-- comment -->",
            "[ref]: manual.md",
            "[ref]:",
            "    title",
            "\ttitle",
        ] {
            let rows = render_markdown(&format!("{title}\n---"), 80);
            assert_eq!(rows.len(), 2, "{title:?}");
            assert!(
                !rows[0].style.add_modifier.contains(Modifier::BOLD),
                "{title:?}"
            );
        }
        for underline in ["    ===", "\t---", "= =", "=-", "===suffix"] {
            assert_eq!(
                render(&format!("Title\n{underline}")).len(),
                2,
                "{underline:?}"
            );
        }
        assert_eq!(render("==="), ["==="]);
        assert_eq!(render(">\n==="), [">", "==="]);
        assert_eq!(render("[docs](manual.md)\n="), ["docs (manual.md)"]);
        assert_eq!(render("![描述](plot.png)\n---"), ["描述"]);
        assert_eq!(
            render("<em>Inline text</em>\n---"),
            ["<em>Inline text</em>"]
        );
    }

    #[test]
    fn setext_leaves_fences_and_tables_on_their_existing_paths() {
        let fenced = render("~~~text\nInside\n===\n~~~\nOutside\n===");
        assert!(fenced[1].contains("Inside"));
        assert!(fenced[2].contains("==="));
        assert_eq!(&fenced[4..], ["Outside"]);

        let table = render("| Key | Value |\n| --- | --- |\n| A | 12 |\n\nAfter\n---");
        assert_eq!(
            table[0].split_whitespace().collect::<Vec<_>>(),
            ["Key", "Value"]
        );
        assert_eq!(table[2].split_whitespace().collect::<Vec<_>>(), ["A", "12"]);
        assert_eq!(&table[3..], ["", "After"]);

        for source in [
            "-\n\n  Nested\n  ===",
            "- item\n\n  ~~~text\n  sample\n  ~~~\n  Nested\n  ===",
            "- item\n\n  | Key |\n  | --- |\n  | sample |\n\n  Nested\n  ===",
            "- item\n\n  # Inner\n  Nested\n  ===",
            "- item\n\n  ***\n  Nested\n  ===",
            "- item\n\n  > quote\n\n  Nested\n  ===",
            "- item\n  10. subitem\n\n  Nested\n  ===",
        ] {
            let rows = render(source);
            assert_eq!(&rows[rows.len() - 2..], ["  Nested", "  ==="]);
        }
    }

    #[test]
    fn ordered_and_nested_lists() {
        let out = render("1. first\n2. second\n  - nested");
        assert_eq!(out[0], "  1. first");
        assert_eq!(out[1], "  2. second");
        assert!(out[2].contains("• nested"));
        assert!(out[2].starts_with("  "), "nested indent preserved");
    }

    #[test]
    fn thematic_breaks_use_one_full_terminal_width_row() {
        for width in [1, 60, 80, 120] {
            for raw in ["---", "***", "___", " - - -", "  *\t* *\t", "   _ _ _ _  "] {
                let rows = render_markdown(raw, width);
                assert_eq!(rows.len(), 1, "{raw:?}");
                assert_eq!(text_of(&rows[0]), "─".repeat(usize::from(width)), "{raw:?}");
                assert_eq!(rows[0].width(), usize::from(width));
            }
        }
        assert!(render_markdown("---", 0).is_empty());
    }

    #[test]
    fn thematic_breaks_only_accept_homogeneous_marker_lines() {
        for raw in [
            "- * -",
            "--_",
            "---suffix",
            "***bold***",
            "word ---",
            "+++",
            "===",
            "    ---",
            "\t***",
            "---\u{a0}",
        ] {
            assert!(rule_markers(raw).is_none(), "{raw:?}");
            assert!(render(raw).iter().all(|row| !row.contains('─')), "{raw:?}");
        }
    }

    #[test]
    fn unfinished_thematic_breaks_remain_literal_during_streaming() {
        for (partial, complete) in [
            ("-", "---"),
            ("--", "---"),
            ("**", "***"),
            ("__", "___"),
            ("- -", "- - -"),
            ("* *", "* * *"),
            ("_ _", "_ _ _"),
        ] {
            assert_eq!(render(partial), [partial]);
            assert_eq!(render(complete), ["─".repeat(80)]);
        }
        assert_eq!(render("---suffix"), ["---suffix"]);
    }

    #[test]
    fn thematic_breaks_precede_list_and_inline_rendering() {
        let rows = render("- - -\n* * *\n- regular item\n- [x] done\n**strong**\n_italic_");
        assert_eq!(&rows[..2], ["─".repeat(80), "─".repeat(80)]);
        assert_eq!(
            &rows[2..],
            ["  • regular item", "  ☑ done", "strong", "italic"]
        );
    }

    #[test]
    fn contiguous_setext_dashes_take_precedence_over_adjacent_thematic_breaks() {
        let rows = render("Title 界\n   ---\t\n---\nProse\n- - -\n# Heading\n---");
        let rule = "─".repeat(80);
        assert_eq!(rows, ["Title 界", &rule, "Prose", &rule, "Heading", &rule]);
        assert_eq!(render("Title\n-\n---"), ["Title", &rule]);
        assert_eq!(render("Prose\n\n---"), ["Prose", "", &rule]);
    }

    #[test]
    fn thematic_breaks_leave_fenced_marker_lines_literal() {
        let rows = render("~~~text\n---\n***\n_ _ _\n~~~\n---");
        assert!(rows[1].contains("---"));
        assert!(rows[2].contains("***"));
        assert!(rows[3].contains("_ _ _"));
        assert_eq!(rows[5], "─".repeat(80));
    }

    #[test]
    fn thematic_breaks_end_pipe_tables_without_consuming_the_delimiter() {
        for rule in ["---", "***", "___", "*\t* *", "- - -"] {
            let rows = render(&format!(
                "| Key | Value |\n| :--- | ---: |\n| A | 12 |\n{rule}\nAfter"
            ));
            assert_eq!(rows.len(), 5, "{rule:?}");
            assert_eq!(
                rows[0].split_whitespace().collect::<Vec<_>>(),
                ["Key", "Value"]
            );
            assert_eq!(rows[2].split_whitespace().collect::<Vec<_>>(), ["A", "12"]);
            assert_eq!(rows[3], "─".repeat(80));
            assert_eq!(rows[4], "After");
        }
        let rows = render("| Key |\n| --- |\n| A |\n    ---\nAfter");
        assert_eq!(&rows[3..], ["    ---", "After"]);
    }

    #[test]
    fn unpiped_partial_rule_markers_stay_literal_in_the_table_until_complete() {
        let table = "| Name | Status |\n| --- | --- |\n| Alpha | Ready |\n| ** | Explicit |\n";
        let preceding = render(table);
        assert_eq!(preceding.len(), 4);
        assert_eq!(
            preceding[2].split_whitespace().collect::<Vec<_>>(),
            ["Alpha", "Ready"]
        );
        assert_eq!(preceding[3].trim(), "Explicit");
        for (partial, delta, displayed) in [
            ("*", " * *", "*"),
            ("**", "*", "**"),
            ("_", "__", "_"),
            ("__", "_", "__"),
            ("*\t*", " *", "*  *"),
        ] {
            let mut source = format!("{table}{partial}");
            let rows = render(&source);
            assert_eq!(rows.len(), preceding.len() + 1, "{partial:?}");
            assert_eq!(
                &rows[..preceding.len()],
                preceding.as_slice(),
                "{partial:?}"
            );
            let row = rows.last().unwrap();
            assert_eq!(row.trim_end(), displayed, "{partial:?}");
            assert_eq!(row.len(), preceding[0].len(), "{partial:?}");

            source.push_str(delta);
            let rows = render(&source);
            assert_eq!(rows.len(), preceding.len() + 1, "{source:?}");
            assert_eq!(&rows[..preceding.len()], preceding.as_slice(), "{source:?}");
            assert_eq!(rows.last().unwrap(), &"─".repeat(80));
        }
    }

    #[test]
    fn task_lists_preserve_nesting_numbering_and_inline_styles() {
        let rows = render_markdown(
            "- [ ] **准备** 🧭\n  * [x] `done`\n\t7. [X] _完成_\n+ [ ]\n1. ordinary\n+ ordinary",
            80,
        );
        assert_eq!(
            rows.iter().map(text_of).collect::<Vec<_>>(),
            [
                "  ☐ 准备 🧭",
                "    ☑ done",
                "    7. ☑ 完成",
                "  ☐ ",
                "  1. ordinary",
                "+ ordinary",
            ]
        );
        for (row, text, modifier) in [
            (&rows[0], "准备", Modifier::BOLD),
            (&rows[1], "done", Modifier::REVERSED),
            (&rows[2], "完成", Modifier::ITALIC),
        ] {
            assert!(row
                .spans
                .iter()
                .any(|span| span.content == text && span.style.add_modifier.contains(modifier)));
        }
    }

    #[test]
    fn task_markers_require_an_exact_checkbox_and_a_separator() {
        assert_eq!(
            render(concat!(
                "- [\n- [x\n- [] empty\n- [o] wrong\n- [x]suffix\n",
                "- [x](https://example.test)\n- [X]\tTabbed\n[x] Plain prose",
            )),
            [
                "  • [",
                "  • [x",
                "  • [] empty",
                "  • [o] wrong",
                "  • [x]suffix",
                "  • x (https://example.test)",
                "  ☑ Tabbed",
                "[x] Plain prose",
            ]
        );
    }

    #[test]
    fn checklist_anchors_accept_spaces_tabs_and_both_ordered_delimiters() {
        assert_eq!(
            render(concat!(
                "-   [ ] spacing\n*\t[x] tab\n+\t[X] plus\n09.\t[x] period\n",
                "7)  [X] parenthesis\n   7)\t[X] nested\n-\t [x] four columns",
            )),
            [
                "  ☐ spacing",
                "  ☑ tab",
                "  ☑ plus",
                "  09. ☑ period",
                "  7) ☑ parenthesis",
                "     7) ☑ nested",
                "  ☑ four columns",
            ]
        );
    }

    #[test]
    fn checklist_anchors_keep_missing_or_excessive_padding_literal() {
        assert_eq!(
            render(concat!(
                "-[x] no separator\n7)[X] no separator\n1.[ ] no separator\n",
                "-     [x] five spaces\n-\t  [x] five columns\n7)\t[x\n",
                "3.  ordinary\n7) ordinary\n*\tordinary",
            )),
            [
                "-[x] no separator",
                "7)[X] no separator",
                "1.[ ] no separator",
                "  •     [x] five spaces",
                "-\t  [x] five columns",
                "7)\t[x",
                "  3.  ordinary",
                "7) ordinary",
                "\tordinary",
            ]
        );
    }

    #[test]
    fn checklist_anchors_end_a_preceding_table() {
        for (item, expected) in [
            ("-\t[X] tab", "  ☑ tab"),
            ("7)  [ ] ordered", "  7) ☐ ordered"),
            ("   +\t[x] nested", "     ☑ nested"),
        ] {
            let rows = render(&format!("| Item |\n| --- |\n| table |\n{item}"));
            assert_eq!(rows.len(), 4);
            assert_eq!(rows[2].trim_end(), "table");
            assert_eq!(rows[3], expected);
        }
    }

    #[test]
    fn task_marker_text_stays_literal_in_code_and_table_cells() {
        let rows = render("~~~md\n- [x] code\n~~~\n| Item |\n| --- |\n| [ ] cell |");
        assert!(rows[1].contains("- [x] code"));
        assert_eq!(rows[5].trim_end(), "[ ] cell");
        assert!(rows.iter().all(|row| !row.contains(['☐', '☑'])));
    }

    #[test]
    fn links_render_label_and_url() {
        let out = render("see [docs](https://x.io) now");
        assert_eq!(out[0], "see docs (https://x.io) now");
        // A bare bracket is left literal.
        assert_eq!(render("[not a link"), vec!["[not a link"]);
    }

    #[test]
    fn inline_images_render_plain_alt_without_destinations() {
        let out = render(concat!(
            "see ![build_route *完成* 🧭](assets/chart(1).png) and ",
            "![escaped](assets/chart\\(2\\).png) [docs](https://example.test) ",
            "![angled](<assets/chart (wide 2).png>) ![](empty.png) ![ \t ](blank.png)",
        ));
        assert_eq!(
            out,
            [
                "see build_route *完成* 🧭 and escaped docs (https://example.test) angled [Image] [Image]"
            ]
        );
    }

    #[test]
    fn inline_image_prefixes_remain_literal_until_the_outer_close() {
        for image in [
            r#"![build_route *完成* 🧭](assets/chart_(1)\).png)"#,
            r#"![build_route *完成* 🧭](<assets/chart (draft 1).png> "title with \"quotes\", ) and *")"#,
            "![build_route *完成* 🧭](chart.png 'single quoted ) _title')",
            r#"![build_route *完成* 🧭](chart.png (parenthesized \) _title))"#,
        ] {
            for (end, _) in image.char_indices().skip(1) {
                let partial = &image[..end];
                assert_eq!(render(partial), [partial], "{partial:?}");
            }
            assert_eq!(render(image), ["build_route *完成* 🧭"]);
        }
    }

    #[test]
    fn bracketed_image_labels_stay_literal_until_the_outer_close() {
        for (image, alt) in [
            ("![API [v2]](chart.png)", "API [v2]"),
            (
                "![API [v2 [LTS]] [guide]](chart.png)",
                "API [v2 [LTS]] [guide]",
            ),
            (r"![API \] release](chart.png)", "API ] release"),
            (r"![API \[draft\] 🧪](chart.png)", "API [draft] 🧪"),
        ] {
            for (end, _) in image.char_indices().skip(1) {
                let partial = &image[..end];
                assert_eq!(render(partial), [partial], "{partial:?}");
            }
            assert_eq!(render(image), [alt]);
        }
    }

    #[test]
    fn unfinished_images_leave_neighboring_ordinary_links_visible() {
        assert_eq!(
            render("![draft [docs](https://example.test)"),
            ["![draft docs (https://example.test)"]
        );
        assert_eq!(
            render("![draft](assets/pending_chart.png [docs](https://example.test)"),
            ["![draft](assets/pending_chart.png docs (https://example.test)"]
        );
        assert_eq!(
            render("![draft [first](one.md) and [second](two.md)"),
            ["![draft first (one.md) and second (two.md)"]
        );
    }

    #[test]
    fn direct_links_inside_complete_image_labels_contribute_only_text() {
        assert_eq!(
            render("![See [guide](https://example.test)](chart.png)"),
            ["See guide"]
        );
        assert_eq!(
            render(
                r#"![See [guide \]](<assets/guide (new).md> "Reference )") and [notes](notes.md)](chart.png)"#
            ),
            ["See guide ] and notes"]
        );
    }

    #[test]
    fn closed_image_label_with_an_inner_link_stays_literal_until_its_target_closes() {
        let label = "![See [guide](https://example.test)]";
        for target in ["", "(", "(assets/chart_(new).png"] {
            let image = format!("{label}{target}");
            assert_eq!(render(&image), [image.as_str()], "{image:?}");
        }
    }

    #[test]
    fn images_in_headings_render_with_heading_style() {
        let lines = render_markdown(
            "# release_notes ![说明 🧭](assets/chart.png)\n# release_notes",
            80,
        );
        assert_eq!(text_of(&lines[0]), "release_notes 说明 🧭");
        assert_eq!(text_of(&lines[1]), "release_notes");
        for row in lines {
            let style = row.spans[0].style;
            assert_eq!(style.fg, Some(colors::brand()));
            assert!(style.add_modifier.contains(Modifier::BOLD));
        }
    }

    #[test]
    fn escaped_image_openers_keep_the_existing_link_and_heading_paths() {
        assert_eq!(
            render(r"\![literal](chart.png) \\![image](chart.png)"),
            [r"\!literal (chart.png) \\image"]
        );
        assert_eq!(
            render(r"# release_notes \![literal](chart.png)"),
            [r"release_notes \![literal](chart.png)"]
        );
    }

    #[test]
    fn invalid_nested_destination_and_title_delimiters_stay_literal() {
        for image in ["![alt](<a<b.png>)", "![alt](chart.png (title (nested)))"] {
            assert_eq!(render(image), [image]);
        }
    }

    #[test]
    fn inline_and_fenced_code_keep_literal_image_syntax() {
        let example = "![a_b*](file(1).png)";
        let out = render(&format!("`{example}`\n~~~md\n{example}\n~~~"));
        assert_eq!(out[0], example);
        assert!(out[2].contains(example));
    }

    #[test]
    fn blockquote_and_inline() {
        let out = render("> quoted **bold**");
        assert!(out[0].contains("│ "));
        assert!(out[0].contains("quoted"));
        assert!(out[0].contains("bold"));
    }

    #[test]
    fn italic_and_bold_distinct() {
        let out = render("_em_ and **strong** and `code`");
        assert_eq!(out[0], "em and strong and code");
    }

    #[test]
    fn table_aligns_columns_and_preserves_inline_styles() {
        let rows = render_markdown(
            "Check | Runs | Score\n:---- | :--: | ----:\n**Unit** | x | `2`\nFormatting | full | 100",
            80,
        );
        assert_eq!(text_of(&rows[0]), "Check        Runs   Score");
        assert_eq!(text_of(&rows[1]), "──────────   ────   ─────");
        assert_eq!(text_of(&rows[2]), "Unit          x         2");
        assert!(rows[0].style.add_modifier.contains(Modifier::BOLD));
        assert!(rows[2].spans.iter().any(|span| {
            span.content == "Unit" && span.style.add_modifier.contains(Modifier::BOLD)
        }));
        assert!(rows[2].spans.iter().any(|span| {
            span.content == "2" && span.style.add_modifier.contains(Modifier::REVERSED)
        }));
    }

    #[test]
    fn table_keeps_escaped_pipes_and_handles_short_and_long_rows() {
        let rows = render(
            "| Option | Value |\n| --- | --- |\n| `a\\|b` | enabled |\nsolo\n| more | yes | ignored |\n> outside",
        );
        assert!(rows[2].contains("a|b"));
        assert_eq!(rows[3].trim(), "solo");
        assert_eq!(
            rows[4].split_whitespace().collect::<Vec<_>>(),
            ["more", "yes"]
        );
        assert_eq!(rows[5], "│ outside");
    }

    #[test]
    fn table_appears_once_the_stream_has_a_matching_delimiter() {
        let header = "| Check | Result |";
        assert_eq!(render(header), [header]);
        assert_eq!(render("| Check | Result |\n| --- |"), [header, "| --- |"]);
        assert!(render("| Check | Result |\n| --- | --- |")[1].contains('─'));
        assert!(
            render("| Check | Result |\n| --- | --- |\n| Format | Pass")[2]
                .contains("Format   Pass")
        );
        let fenced = render("```md\n| Check | Result |\n| --- | --- |\n```");
        assert!(fenced[2].contains("| --- | --- |"));
    }

    #[test]
    fn table_wraps_wide_cells_with_styles_intact() {
        let rows = render_markdown(
            "| 項目 | Value |\n| --- | --- |\n| **四海八方** | abcdefgh |",
            14,
        );
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|row| row.width() <= 14));
        assert!(rows[2].spans.iter().any(|span| {
            span.content == "四海八" && span.style.add_modifier.contains(Modifier::BOLD)
        }));
        assert!(rows[3].spans.iter().any(|span| {
            span.content == "方" && span.style.add_modifier.contains(Modifier::BOLD)
        }));
        assert!(text_of(&rows[3]).contains("fgh"));
    }

    #[test]
    fn table_stacks_labeled_cells_when_the_columns_cannot_fit() {
        let rows = render_markdown("A | B | C\n--- | --- | ---\none | two | three", 8);
        assert_eq!(
            rows.iter().map(text_of).collect::<Vec<_>>(),
            ["A: one", "B: two", "C: three"]
        );
    }

    #[test]
    fn unicode_code_block_borders_share_one_cell_width() {
        let rendered = render_markdown("```\n界e\u{301}👨‍👩‍👧‍👦\n```", 18);
        let widths = rendered
            .iter()
            .map(|line| crate::text::display_width(&text_of(line)))
            .collect::<Vec<_>>();
        assert!(widths.iter().all(|width| *width == widths[0]), "{widths:?}");
        assert_eq!(widths[0], 18);
    }

    #[test]
    fn long_fence_emits_a_fixed_preview_and_reports_both_limits() {
        let source = format!(
            "```rust\n{}```\nAfter the preview.",
            "println!(\"still streaming\");\n".repeat(MAX_PREVIEW_SOURCE_LINES),
        );

        let rows = render(&source);

        assert_eq!(rows.len(), MAX_FENCE_PREVIEW_ROWS + 4);
        assert!(rows[0].contains(" rust "));
        assert!(rows[1..=MAX_FENCE_PREVIEW_ROWS]
            .iter()
            .all(|row| row.contains("still streaming")));
        assert!(rows[MAX_FENCE_PREVIEW_ROWS + 1].contains("… more code"));
        assert!(rows[MAX_FENCE_PREVIEW_ROWS + 2].starts_with('└'));
        assert_eq!(rows.last().unwrap(), "… Markdown preview clipped");
    }

    #[test]
    fn byte_budget_ends_before_a_partial_unicode_source_line() {
        let source = format!(
            "~~~rust\r\nretained\r\n{}\r\n~~~\r\nAfter the preview.",
            "界".repeat(MAX_PREVIEW_BYTES / 3),
        );

        assert_eq!(preview_source(&source), "~~~rust\r\nretained\r\n");
        let rows = render(&source);
        assert!(rows[1].contains("retained"));
        assert!(rows[2].starts_with('└'));
        assert_eq!(rows[3], "… Markdown preview clipped");
    }

    #[test]
    fn copyable_fences_keep_original_bodies_in_source_order() {
        let source = concat!(
            "Leading prose.\r\n",
            "````rust options\r\n",
            "\tfirst 界👩‍💻\r\n",
            "```\r\n",
            "~~~~\r\n",
            "```` trailing\r\n",
            "\r\n",
            "````` \t\r\n",
            "Between fences.\n",
            "~~~ Mermaid\n",
            "\tA --> B\n",
            "~~~\n",
            "```\n",
            "still open",
        );
        let (prefix, fences) = copyable_fences(source);
        assert_eq!(prefix, source);
        assert_eq!(
            fences,
            [
                CopyableFence {
                    language: Some("rust"),
                    source: "\tfirst 界👩‍💻\r\n```\r\n~~~~\r\n```` trailing\r\n\r\n",
                },
                CopyableFence {
                    language: Some("Mermaid"),
                    source: "\tA --> B\n",
                },
            ]
        );

        let (_, at_eof) = copyable_fences("```\nunlabelled\n```");
        assert_eq!(
            at_eof,
            [CopyableFence {
                language: None,
                source: "unlabelled\n",
            }]
        );
    }

    #[test]
    fn copyable_fences_keep_rows_beyond_the_rendered_preview() {
        let body = "\tfirst\r\n".repeat(MAX_FENCE_PREVIEW_ROWS) + "\tlast 👩‍💻\r\n";
        let source = format!("~~~sh\r\n{body}~~~\r\n");
        let (_, fences) = copyable_fences(&source);
        assert_eq!(fences.len(), 1);
        assert_eq!(fences[0].source, body);
        let rows = render(&source);
        assert!(rows.iter().any(|row| row.contains("… more code")));
        assert!(rows.iter().all(|row| !row.contains("last")));
    }

    #[test]
    fn copyable_fences_do_not_treat_a_truncated_physical_line_as_a_closer() {
        let early = "```rust\r\n\tfirst\r\n```\r\n~~~sh\n";
        let fill = "x".repeat(MAX_PREVIEW_BYTES - early.len() - 4);
        let source = format!("{early}{fill}\n~~~ still code\n~~~\n");
        assert_eq!(&source[MAX_PREVIEW_BYTES - 3..MAX_PREVIEW_BYTES], "~~~");
        let (prefix, fences) = copyable_fences(&source);
        assert_eq!(prefix.len(), MAX_PREVIEW_BYTES - 3);
        assert_eq!(prefix, preview_source(&source));
        assert_eq!(
            fences,
            [CopyableFence {
                language: Some("rust"),
                source: "\tfirst\r\n",
            }]
        );
    }

    #[test]
    fn copyable_fences_require_closure_within_the_source_line_budget() {
        let early = "```rust\nfirst\n```\n~~~sh\n";
        let source = format!("{early}{}~~~\n", "more\n".repeat(MAX_PREVIEW_SOURCE_LINES));
        let (prefix, fences) = copyable_fences(&source);
        assert_eq!(prefix.lines().count(), MAX_PREVIEW_SOURCE_LINES);
        assert_eq!(prefix, preview_source(&source));
        assert_eq!(
            fences,
            [CopyableFence {
                language: Some("rust"),
                source: "first\n",
            }]
        );
    }

    #[test]
    fn code_preview_fits_tiny_areas_and_clips_wide_graphemes() {
        let source = "```language\n界e\u{301}👨‍👩‍👧‍👦more text\n```";
        assert!(render_markdown(source, 0).is_empty());
        for width in 1..=12 {
            let rows = render_markdown(source, width);
            assert!(!rows.is_empty());
            assert!(rows.iter().all(|row| row.width() <= usize::from(width)));
        }
        let rows = render_markdown(source, 9);
        assert!(text_of(&rows[1]).contains("界e\u{301}…"));
        assert!(!text_of(&rows[1]).contains("👨"));
    }
}
