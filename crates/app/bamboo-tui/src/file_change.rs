//! Pure parsing and terminal presentation for Bamboo's bounded file-change
//! result contract. This module deliberately operates only on supplied JSON;
//! rendering a tool result must never read a path or invoke source control.

use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use serde_json::Value;

use crate::text;
use crate::theme::colors;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileChangeState {
    Proposed,
    Applied,
    Failed,
}

impl FileChangeState {
    pub(crate) fn from_phase(phase: &str) -> Self {
        if phase.eq_ignore_ascii_case("complete") {
            Self::Applied
        } else if phase.eq_ignore_ascii_case("error") || phase.eq_ignore_ascii_case("failed") {
            Self::Failed
        } else {
            Self::Proposed
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Proposed => "PROPOSED",
            Self::Applied => "APPLIED",
            Self::Failed => "FAILED",
        }
    }

    fn style(self) -> Style {
        let color = match self {
            Self::Proposed => colors::warning(),
            Self::Applied => colors::tool_done(),
            Self::Failed => colors::tool_error(),
        };
        Style::default().fg(color).add_modifier(Modifier::BOLD)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiffLineKind {
    OldFile,
    NewFile,
    Hunk,
    Context,
    Added,
    Removed,
    Marker,
    Meta,
}

impl DiffLineKind {
    fn style(self) -> Style {
        match self {
            Self::Added => Style::default().fg(colors::success()),
            Self::Removed => Style::default().fg(colors::error()),
            Self::Hunk | Self::Marker => Style::default().fg(colors::warning()),
            Self::OldFile | Self::NewFile | Self::Meta => Style::default().fg(colors::subtle()),
            Self::Context => Style::default().fg(colors::inactive()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedDiffLine {
    kind: DiffLineKind,
    old_line: Option<usize>,
    new_line: Option<usize>,
    content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RenderedDiffRow {
    pub(crate) text: String,
    pub(crate) kind: DiffLineKind,
    pub(crate) starts_hunk: bool,
}

impl RenderedDiffRow {
    pub(crate) fn styled_line(&self, indent: &str) -> Line<'static> {
        Line::from(Span::styled(
            format!("{indent}{}", self.text),
            self.kind.style(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CheckpointStatus {
    Saved,
    NotCreated(Option<String>),
}

impl CheckpointStatus {
    fn label(&self) -> String {
        match self {
            Self::Saved => "checkpoint saved".to_string(),
            Self::NotCreated(Some(reason)) => {
                format!("checkpoint none ({})", visible_text(reason))
            }
            Self::NotCreated(None) => "checkpoint none".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiagnosticStatus {
    format: String,
    valid: Option<bool>,
}

impl DiagnosticStatus {
    fn label(&self) -> String {
        let format = visible_text(&self.format);
        match self.valid {
            Some(true) => format!("diagnostics {format} valid"),
            Some(false) => format!("diagnostics {format} INVALID"),
            None => format!("diagnostics {format} reported"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileChangeView {
    operation: String,
    path: String,
    unified: String,
    old_line_count: usize,
    new_line_count: usize,
    added_lines: usize,
    removed_lines: usize,
    old_trailing_newline: bool,
    new_trailing_newline: bool,
    truncated: bool,
    checkpoint: CheckpointStatus,
    diagnostic: Option<DiagnosticStatus>,
    parsed_lines: Vec<ParsedDiffLine>,
    hunk_count: usize,
    file_count: usize,
    crlf_payload: bool,
}

impl FileChangeView {
    /// Parse only a result emitted by the canonical Write/Edit tool family.
    /// A payload from any other tool, unsupported diff format, or malformed
    /// unified text returns `None` so the caller can retain the generic view.
    pub(crate) fn from_tool_result(tool_name: &str, payload: &str) -> Option<Self> {
        let expected_operation = expected_operation(tool_name)?;
        let value: Value = serde_json::from_str(payload).ok()?;
        Self::from_value(expected_operation, &value)
    }

    /// Permission responses may optionally carry a separate, server-computed
    /// canonical file-change object. Raw tool arguments never enter this path:
    /// they are model input, not authoritative proposed-diff metadata.
    pub(crate) fn from_proposed_value(tool_name: &str, value: &Value) -> Option<Self> {
        let expected_operation = expected_operation(tool_name)?;
        Self::from_value(expected_operation, value)
    }

    fn from_value(expected_operation: &'static str, value: &Value) -> Option<Self> {
        let object = value.as_object()?;
        let operation = object.get("operation")?.as_str()?;
        if operation != expected_operation {
            return None;
        }
        let path = object.get("file_path")?.as_str()?.trim().to_string();
        if path.is_empty() || path.contains('\0') {
            return None;
        }
        object.get("message")?.as_str()?;
        if object.get("workspace")?.as_str()?.trim().is_empty() {
            return None;
        }

        let diff = object.get("diff")?.as_object()?;
        if diff.get("format")?.as_str()? != "unified" {
            return None;
        }
        match diff.get("binary") {
            None | Some(Value::Bool(false)) => {}
            Some(_) => return None,
        }
        let unified = diff.get("unified")?.as_str()?.to_string();
        if unified.contains('\0') {
            return None;
        }
        let old_line_count = value_as_usize(diff.get("old_line_count")?)?;
        let new_line_count = value_as_usize(diff.get("new_line_count")?)?;
        let added_lines = value_as_usize(diff.get("added_lines")?)?;
        let removed_lines = value_as_usize(diff.get("removed_lines")?)?;
        let old_trailing_newline = diff.get("old_trailing_newline")?.as_bool()?;
        let new_trailing_newline = diff.get("new_trailing_newline")?.as_bool()?;
        let truncated = diff.get("truncated")?.as_bool()?;
        let parsed = parse_unified(&unified, truncated)?;

        let checkpoint = match object.get("checkpoint")?.as_object()? {
            checkpoint if checkpoint.get("created")?.as_bool()? => {
                checkpoint.get("id")?.as_str()?;
                checkpoint.get("path")?.as_str()?;
                checkpoint.get("size_bytes")?.as_u64()?;
                CheckpointStatus::Saved
            }
            checkpoint => {
                CheckpointStatus::NotCreated(Some(checkpoint.get("reason")?.as_str()?.to_string()))
            }
        };
        let diagnostic = match object.get("diagnostics") {
            Some(value) => {
                let diagnostics = value.as_object()?;
                Some(DiagnosticStatus {
                    format: diagnostics.get("format")?.as_str()?.to_string(),
                    valid: Some(diagnostics.get("valid")?.as_bool()?),
                })
            }
            None => None,
        };

        Some(Self {
            operation: operation.to_string(),
            path,
            unified,
            old_line_count,
            new_line_count,
            added_lines,
            removed_lines,
            old_trailing_newline,
            new_trailing_newline,
            truncated,
            checkpoint,
            diagnostic,
            hunk_count: parsed.hunk_count,
            file_count: parsed.file_count,
            parsed_lines: parsed.lines,
            crlf_payload: parsed.crlf_payload,
        })
    }

    pub(crate) fn unified(&self) -> &str {
        &self.unified
    }

    pub(crate) fn summary_lines(
        &self,
        state: FileChangeState,
        indent: &str,
        width: usize,
    ) -> Vec<Line<'static>> {
        let content_width = width.saturating_sub(text::display_width(indent)).max(1);
        let primary = text::clip_cells(
            &format!(
                "{} · {} · {}",
                state.label(),
                self.operation,
                visible_text(&self.path)
            ),
            content_width,
        );
        let scope = if self.file_count == 1 {
            "1 file".to_string()
        } else {
            format!("{} files", self.file_count)
        };
        let hunks = if self.hunk_count == 1 {
            "1 hunk".to_string()
        } else {
            format!("{} hunks", self.hunk_count)
        };
        let stats = text::clip_cells(
            &format!(
                "DIFF · +{} added · -{} removed · {}→{} lines · {scope} · {hunks}",
                self.added_lines, self.removed_lines, self.old_line_count, self.new_line_count
            ),
            content_width,
        );
        let diagnostic = self
            .diagnostic
            .as_ref()
            .map(DiagnosticStatus::label)
            .unwrap_or_else(|| "diagnostics none".to_string());
        let truncation = if self.truncated { "TRUNCATED · " } else { "" };
        let line_endings = if self.old_trailing_newline == self.new_trailing_newline {
            format!("trailing newline {}", yes_no(self.new_trailing_newline))
        } else {
            format!(
                "trailing newline {}→{}",
                yes_no(self.old_trailing_newline),
                yes_no(self.new_trailing_newline)
            )
        };
        let transport = if self.crlf_payload {
            " · CRLF payload"
        } else {
            ""
        };
        let meta = text::clip_cells(
            &format!(
                "META · {truncation}{} · {diagnostic} · {line_endings}{transport}",
                self.checkpoint.label()
            ),
            content_width,
        );

        vec![
            Line::from(Span::styled(format!("{indent}{primary}"), state.style())),
            Line::from(Span::styled(
                format!("{indent}{stats}"),
                Style::default().fg(colors::subtle()),
            )),
            Line::from(Span::styled(
                format!("{indent}{meta}"),
                Style::default().fg(if self.truncated {
                    colors::warning()
                } else {
                    colors::subtle()
                }),
            )),
        ]
    }

    pub(crate) fn rendered_rows(&self, width: usize, wrap: bool) -> Vec<RenderedDiffRow> {
        let width = width.max(1);
        let number_width = decimal_width(self.old_line_count.max(self.new_line_count)).min(8);
        let gutter_width = number_width.saturating_mul(2).saturating_add(7);
        let body_width = width.saturating_sub(gutter_width).max(1);
        let mut rows = Vec::new();

        for parsed in &self.parsed_lines {
            let (old, new, marker) = match parsed.kind {
                DiffLineKind::Context => (
                    line_number(parsed.old_line, number_width),
                    line_number(parsed.new_line, number_width),
                    "  ",
                ),
                DiffLineKind::Added => (
                    line_number(None, number_width),
                    line_number(parsed.new_line, number_width),
                    "+ ",
                ),
                DiffLineKind::Removed => (
                    line_number(parsed.old_line, number_width),
                    line_number(None, number_width),
                    "- ",
                ),
                DiffLineKind::Hunk => ("@".repeat(number_width), "@".repeat(number_width), "  "),
                DiffLineKind::OldFile => ("-".repeat(number_width), "·".repeat(number_width), "  "),
                DiffLineKind::NewFile => ("·".repeat(number_width), "+".repeat(number_width), "  "),
                DiffLineKind::Marker => ("!".repeat(number_width), "!".repeat(number_width), "  "),
                DiffLineKind::Meta => ("·".repeat(number_width), "·".repeat(number_width), "  "),
            };
            let prefix = format!("{old} {new} │ {marker}");
            let visible_content = visible_text(&parsed.content);
            let chunks = if wrap {
                text::hard_wrap(&visible_content, body_width)
            } else {
                vec![text::clip_cells(&visible_content, body_width)]
            };
            for (index, chunk) in chunks.into_iter().enumerate() {
                let row_prefix = if index == 0 {
                    prefix.clone()
                } else {
                    format!("{} │ ↳ ", " ".repeat(number_width * 2 + 1))
                };
                rows.push(RenderedDiffRow {
                    text: format!("{row_prefix}{chunk}"),
                    kind: parsed.kind,
                    starts_hunk: index == 0 && parsed.kind == DiffLineKind::Hunk,
                });
            }
        }
        rows
    }

    pub(crate) fn hunk_offsets(&self, width: usize, wrap: bool) -> Vec<usize> {
        self.rendered_rows(width, wrap)
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.starts_hunk.then_some(index))
            .collect()
    }
}

fn expected_operation(tool_name: &str) -> Option<&'static str> {
    let normalized = tool_name
        .chars()
        .filter(|character| !matches!(character, '_' | '-'))
        .flat_map(char::to_lowercase)
        .collect::<String>();
    match normalized.as_str() {
        "write" | "writefile" => Some("Write"),
        "edit" | "editfile" | "applypatch" => Some("Edit"),
        _ => None,
    }
}

fn value_as_usize(value: &Value) -> Option<usize> {
    usize::try_from(value.as_u64()?).ok()
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn decimal_width(value: usize) -> usize {
    value.max(1).to_string().len()
}

fn line_number(value: Option<usize>, width: usize) -> String {
    match value {
        Some(value) => format!("{value:>width$}"),
        None => format!("{:>width$}", "·"),
    }
}

fn visible_text(value: &str) -> String {
    let mut visible = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\t' => visible.push('\t'),
            '\r' => visible.push('␍'),
            '\u{1b}' => visible.push('␛'),
            character if character.is_control() => visible.push('�'),
            character => visible.push(character),
        }
    }
    visible
}

struct ParsedUnified {
    lines: Vec<ParsedDiffLine>,
    hunk_count: usize,
    file_count: usize,
    crlf_payload: bool,
}

fn parse_unified(unified: &str, truncated: bool) -> Option<ParsedUnified> {
    let crlf_payload = unified.contains("\r\n");
    let mut logical = unified.split('\n').collect::<Vec<_>>();
    if unified.ends_with('\n') {
        logical.pop();
    }
    if logical.is_empty() {
        return None;
    }
    let has_truncation_marker = logical.last().is_some_and(|line| {
        line.trim_end_matches('\r')
            .starts_with("... diff truncated")
    });
    if truncated != has_truncation_marker {
        return None;
    }

    let mut lines = Vec::with_capacity(logical.len());
    let mut old_cursor = None;
    let mut new_cursor = None;
    let mut old_remaining = 0usize;
    let mut new_remaining = 0usize;
    let mut awaiting_new_header = false;
    let mut have_file_header = false;
    let mut old_headers = 0usize;
    let mut new_headers = 0usize;
    let mut hunk_count = 0usize;

    let logical_len = logical.len();
    for (index, raw) in logical.into_iter().enumerate() {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        let counts_exhausted = old_remaining == 0 && new_remaining == 0;
        let (kind, old_line, new_line, content) = if let Some(header) = line.strip_prefix("@@") {
            if awaiting_new_header || !have_file_header || !counts_exhausted {
                return None;
            }
            let (old_start, old_count, new_start, new_count) = parse_hunk_header(line)?;
            old_cursor = Some(old_start);
            new_cursor = Some(new_start);
            old_remaining = old_count;
            new_remaining = new_count;
            hunk_count += 1;
            (DiffLineKind::Hunk, None, None, format!("@@{header}"))
        } else if counts_exhausted && line.starts_with("--- ") {
            let content = line.strip_prefix("--- ").expect("prefix was checked above");
            if awaiting_new_header {
                return None;
            }
            old_headers += 1;
            awaiting_new_header = true;
            have_file_header = false;
            old_cursor = None;
            new_cursor = None;
            (DiffLineKind::OldFile, None, None, format!("--- {content}"))
        } else if awaiting_new_header && line.starts_with("+++ ") {
            let content = line.strip_prefix("+++ ").expect("prefix was checked above");
            new_headers += 1;
            awaiting_new_header = false;
            have_file_header = true;
            (DiffLineKind::NewFile, None, None, format!("+++ {content}"))
        } else if let Some(content) = line.strip_prefix(' ') {
            if old_remaining == 0 || new_remaining == 0 {
                return None;
            }
            let old = old_cursor?;
            let new = new_cursor?;
            old_cursor = Some(old.saturating_add(1));
            new_cursor = Some(new.saturating_add(1));
            old_remaining -= 1;
            new_remaining -= 1;
            (
                DiffLineKind::Context,
                Some(old),
                Some(new),
                content.to_string(),
            )
        } else if let Some(content) = line.strip_prefix('-') {
            if old_remaining == 0 {
                if !is_trailing_newline_annotation(content) || new_remaining != 0 {
                    return None;
                }
            } else {
                old_remaining -= 1;
            }
            let old = old_cursor?;
            old_cursor = Some(old.saturating_add(1));
            (DiffLineKind::Removed, Some(old), None, content.to_string())
        } else if let Some(content) = line.strip_prefix('+') {
            if new_remaining == 0 {
                if !is_trailing_newline_annotation(content) || old_remaining != 0 {
                    return None;
                }
            } else {
                new_remaining -= 1;
            }
            let new = new_cursor?;
            new_cursor = Some(new.saturating_add(1));
            (DiffLineKind::Added, None, Some(new), content.to_string())
        } else if let Some(content) = line.strip_prefix("\\ ") {
            (DiffLineKind::Marker, None, None, format!("! {content}"))
        } else if line.starts_with("... diff truncated") {
            (DiffLineKind::Meta, None, None, line.to_string())
        } else if truncated && line.is_empty() && index + 2 == logical_len {
            (
                DiffLineKind::Meta,
                None,
                None,
                "... diff truncated at line boundary".to_string(),
            )
        } else {
            return None;
        };
        lines.push(ParsedDiffLine {
            kind,
            old_line,
            new_line,
            content,
        });
    }

    if old_headers == 0
        || old_headers != new_headers
        || awaiting_new_header
        || hunk_count == 0
        || (!truncated && (old_remaining != 0 || new_remaining != 0))
    {
        return None;
    }
    Some(ParsedUnified {
        lines,
        hunk_count,
        file_count: old_headers,
        crlf_payload,
    })
}

fn is_trailing_newline_annotation(content: &str) -> bool {
    matches!(
        content,
        "[old had trailing newline]"
            | "[new missing trailing newline]"
            | "[old missing trailing newline]"
            | "[new has trailing newline]"
    )
}

fn parse_hunk_header(line: &str) -> Option<(usize, usize, usize, usize)> {
    let mut parts = line.split_whitespace();
    if parts.next()? != "@@" {
        return None;
    }
    let (old_start, old_count) = parse_range(parts.next()?, '-')?;
    let (new_start, new_count) = parse_range(parts.next()?, '+')?;
    if parts.next()? != "@@" {
        return None;
    }
    Some((old_start, old_count, new_start, new_count))
}

fn parse_range(value: &str, prefix: char) -> Option<(usize, usize)> {
    let value = value.strip_prefix(prefix)?;
    let (start, count) = value.split_once(',').map_or((value, "1"), |parts| parts);
    Some((start.parse().ok()?, count.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn payload(
        operation: &str,
        unified: &str,
        old: usize,
        new: usize,
        added: usize,
        removed: usize,
    ) -> String {
        let checkpoint = if old > 0 {
            json!({
                "created": true,
                "id": "checkpoint-1",
                "path": "/checkpoints/demo.txt",
                "size_bytes": old
            })
        } else {
            json!({"created": false, "reason": "file_did_not_exist"})
        };
        json!({
            "operation": operation,
            "message": "changed",
            "file_path": "/workspace/demo.txt",
            "workspace": "/workspace",
            "checkpoint": checkpoint,
            "diagnostics": {"format": "json", "valid": true},
            "diff": {
                "format": "unified",
                "unified": unified,
                "old_line_count": old,
                "new_line_count": new,
                "added_lines": added,
                "removed_lines": removed,
                "old_trailing_newline": old > 0,
                "new_trailing_newline": new > 0,
                "truncated": false
            }
        })
        .to_string()
    }

    #[test]
    fn parses_create_edit_delete_and_empty_file_changes() {
        let create = payload(
            "Write",
            "--- a/demo.txt\n+++ b/demo.txt\n@@ -1,0 +1,2 @@\n+alpha\n+beta",
            0,
            2,
            2,
            0,
        );
        let create = FileChangeView::from_tool_result("Write", &create).unwrap();
        assert_eq!(create.added_lines, 2);
        assert!(create
            .rendered_rows(60, true)
            .iter()
            .any(|row| row.text.contains("+ alpha")));

        let edit = payload(
            "Edit",
            "--- a/demo.txt\n+++ b/demo.txt\n@@ -1,2 +1,2 @@\n keep\n-old\n+new\n@@ -8,1 +8,2 @@\n tail\n+extra",
            8,
            9,
            2,
            1,
        );
        let edit = FileChangeView::from_tool_result("apply_patch", &edit).unwrap();
        assert_eq!(edit.hunk_offsets(60, true).len(), 2);

        let delete = payload(
            "Edit",
            "--- a/demo.txt\n+++ b/demo.txt\n@@ -1,2 +1,0 @@\n-alpha\n-beta",
            2,
            0,
            0,
            2,
        );
        assert_eq!(
            FileChangeView::from_tool_result("Edit", &delete)
                .unwrap()
                .removed_lines,
            2
        );

        let empty = payload(
            "Write",
            "--- a/demo.txt\n+++ b/demo.txt\n@@ -1,0 +1,0 @@",
            0,
            0,
            0,
            0,
        );
        assert!(FileChangeView::from_tool_result("Write", &empty).is_some());
    }

    #[test]
    fn preserves_crlf_unicode_tabs_and_exact_copy_while_wrapping() {
        let unified = "--- a/demo.txt\r\n+++ b/demo.txt\r\n@@ -1,1 +1,1 @@\r\n-\told界\u{1b}\r\n+\tnew👨‍👩‍👧‍👦\r\n-[old missing trailing newline]\r\n+[new has trailing newline]\r\n";
        let mut value: Value =
            serde_json::from_str(&payload("Edit", unified, 1, 1, 1, 1)).expect("test payload");
        value["diff"]["old_trailing_newline"] = json!(false);
        value["diff"]["new_trailing_newline"] = json!(true);
        let view = FileChangeView::from_tool_result("Edit", &value.to_string()).unwrap();
        assert_eq!(view.unified(), unified);
        assert!(view.crlf_payload);
        assert!(view
            .rendered_rows(20, true)
            .iter()
            .any(|row| row.text.contains('\t')));
        assert!(view
            .rendered_rows(20, true)
            .iter()
            .any(|row| row.text.contains('界') || row.text.contains('👨')));
        assert!(view
            .rendered_rows(20, true)
            .iter()
            .all(|row| !row.text.contains('\u{1b}')));
        let summary = view.summary_lines(FileChangeState::Applied, "", 120);
        let summary = summary
            .iter()
            .flat_map(|line| line.spans.iter())
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert!(summary.contains("trailing newline no→yes"));
    }

    #[test]
    fn header_like_content_stays_inside_its_hunk_and_bad_counts_fall_back() {
        let unified =
            "--- a/demo.txt\n+++ b/demo.txt\n@@ -1,2 +1,2 @@\n--- removed heading\n+++ added heading\n tail";
        let value = payload("Edit", unified, 2, 2, 1, 1);
        let view = FileChangeView::from_tool_result("Edit", &value).unwrap();
        assert_eq!(view.file_count, 1);
        let rows = view.rendered_rows(80, true);
        assert!(rows
            .iter()
            .any(|row| row.kind == DiffLineKind::Removed
                && row.text.contains("- -- removed heading")));
        assert!(rows
            .iter()
            .any(|row| row.kind == DiffLineKind::Added && row.text.contains("+ ++ added heading")));

        let malformed = payload(
            "Edit",
            "--- a/demo.txt\n+++ b/demo.txt\n@@ -1,2 +1,2 @@\n-old\n+new",
            2,
            2,
            1,
            1,
        );
        assert!(FileChangeView::from_tool_result("Edit", &malformed).is_none());
    }

    #[test]
    fn reports_truncation_and_navigates_multiple_files_at_supported_widths() {
        let mut value: Value = serde_json::from_str(&payload(
            "Edit",
            "--- a/one\n+++ b/one\n@@ -1,1 +1,1 @@\n-a\n+b\n--- a/two\n+++ b/two\n@@ -4,1 +4,1 @@\n-c\n+d\n... diff truncated (9 more lines)",
            4,
            4,
            2,
            2,
        ))
        .unwrap();
        value["diff"]["truncated"] = json!(true);
        let view = FileChangeView::from_tool_result("Edit", &value.to_string()).unwrap();
        assert_eq!(view.file_count, 2);
        for width in [60, 80, 120] {
            let rows = view.rendered_rows(width, true);
            assert_eq!(rows.iter().filter(|row| row.starts_hunk).count(), 2);
            assert_eq!(view.hunk_offsets(width, true).len(), 2);
            assert!(rows
                .iter()
                .all(|row| text::display_width(&row.text) <= width));
        }
        let summary = view.summary_lines(FileChangeState::Applied, "", 120);
        let summary_text = summary
            .iter()
            .flat_map(|line| line.spans.iter())
            .map(|span| span.content.as_ref())
            .collect::<String>();
        assert!(summary_text.contains("TRUNCATED"));

        value["diff"]["unified"] = json!(
            "--- a/one\n+++ b/one\n@@ -1,1 +1,1 @@\n-a\n+b\n\n... diff truncated (content too long)"
        );
        assert!(
            FileChangeView::from_tool_result("Edit", &value.to_string()).is_some(),
            "a character cap landing exactly after a newline stays canonical"
        );
    }

    #[test]
    fn malformed_unknown_and_binary_payloads_fall_back() {
        assert!(FileChangeView::from_tool_result("Write", "not json").is_none());
        let valid = payload(
            "Write",
            "--- a/demo\n+++ b/demo\n@@ -1,0 +1,1 @@\n+x",
            0,
            1,
            1,
            0,
        );
        assert!(FileChangeView::from_tool_result("Bash", &valid).is_none());
        assert!(FileChangeView::from_tool_result("Edit", &valid).is_none());

        let mut binary: Value = serde_json::from_str(&valid).unwrap();
        binary["diff"]["binary"] = json!(true);
        assert!(FileChangeView::from_tool_result("Write", &binary.to_string()).is_none());
        binary["diff"]["binary"] = json!("false");
        assert!(FileChangeView::from_tool_result("Write", &binary.to_string()).is_none());

        let mut malformed: Value = serde_json::from_str(&valid).unwrap();
        malformed["diff"]["unified"] = json!("--- a/demo\n+++ b/demo\nnot a hunk");
        assert!(FileChangeView::from_tool_result("Write", &malformed.to_string()).is_none());
    }

    #[test]
    fn proposed_permission_diff_requires_a_separate_canonical_value() {
        let canonical: Value = serde_json::from_str(&payload(
            "Edit",
            "--- a/demo\n+++ b/demo\n@@ -1,1 +1,1 @@\n-old\n+new",
            1,
            1,
            1,
            1,
        ))
        .unwrap();
        let raw_arguments = json!({"file_path": "/workspace/demo", "patch": "raw model text"});
        assert!(FileChangeView::from_proposed_value("Edit", &raw_arguments).is_none());
        assert!(FileChangeView::from_proposed_value("Edit", &canonical).is_some());
    }
}
