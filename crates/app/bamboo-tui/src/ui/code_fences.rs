use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::app::{App, FenceCopyEntry, FenceSourcePreview};
use crate::keymap::{ActionContext, ActionId};
use crate::text::{clip_cells, code_source_line, display_width, slice_cells};
use crate::theme::colors;
use crate::ui::sessions::visible_window;

pub(super) fn render(frame: &mut Frame, app: &App) {
    let Some(picker) = &app.fence_copy_picker else {
        return;
    };
    if let (Some(preview), Some(entry)) = (&picker.preview, picker.entries.get(picker.selected)) {
        render_source_preview(frame, app, entry, preview);
        return;
    }

    let screen = frame.area();
    let width = screen.width.saturating_sub(2).min(72);
    let height = usize::from(screen.height.saturating_sub(2))
        .min(picker.entries.len().saturating_add(4)) as u16;
    let area = centered_area(screen, width, height);
    let text_width = usize::from(width.saturating_sub(2));
    let capacity = usize::from(height.saturating_sub(4));
    let selected = picker.selected.min(picker.entries.len().saturating_sub(1));
    let visible = visible_window(picker.entries.len(), selected, capacity);
    picker.rows_per_page.set(visible.len().max(1));
    let subtle = Style::default().fg(colors::subtle());
    let mut lines: Vec<Line> = visible
        .map(|index| {
            let entry = &picker.entries[index];
            let is_selected = index == selected;
            let marker = if is_selected { '›' } else { ' ' };
            let language = clip_preview(entry.language.as_deref().unwrap_or("Code"), 16);
            let prefix = format!("{marker} {language} · ");
            let preview_width = text_width.saturating_sub(display_width(&prefix));
            let first_line = entry
                .source
                .lines()
                .map(str::trim)
                .find(|line| !line.is_empty())
                .unwrap_or("(empty fence)");
            let preview = clip_preview(first_line, preview_width);
            let row = if is_selected {
                Style::default()
                    .fg(colors::brand())
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(colors::inactive())
            };
            Line::styled(format!("{prefix}{preview}"), row)
        })
        .collect();
    let key = |action| app.primary_key_hint(ActionContext::FenceCopyPicker, action);
    lines.extend(
        [
            format!(
                "  {}/{} move · {}/{} page",
                key(ActionId::NavigateUp),
                key(ActionId::NavigateDown),
                key(ActionId::PageUp),
                key(ActionId::PageDown),
            ),
            format!(
                "  {} source · {} copy · {} close",
                key(ActionId::Activate),
                key(ActionId::CopyValue),
                key(ActionId::Cancel)
            ),
        ]
        .into_iter()
        .map(|hint| Line::styled(clip_cells(&hint, text_width), subtle)),
    );

    render_dialog(frame, area, " Copy a code fence ", lines);
}

fn render_source_preview(
    frame: &mut Frame,
    app: &App,
    entry: &FenceCopyEntry,
    preview: &FenceSourcePreview,
) {
    let screen = frame.area();
    let width = screen.width.saturating_sub(2);
    let height = screen.height.saturating_sub(2);
    let area = centered_area(screen, width, height);
    let columns = usize::from(width.saturating_sub(2));
    // Two border rows and three fixed footer rows stay visible while scrolling.
    let rows = usize::from(height.saturating_sub(5));
    preview.set_viewport(rows, columns);

    let first_line = preview.first_line();
    let first_column = preview.first_column();
    let subtle = Style::default().fg(colors::subtle());
    let mut lines: Vec<Line> = entry
        .source
        .lines()
        .skip(first_line)
        .take(rows)
        .map(|line| Line::from(slice_cells(&code_source_line(line), first_column, columns)))
        .collect();
    let position = if preview.line_count == 0 {
        if rows > 0 {
            lines.push(Line::styled("(empty fence)", subtle));
        }
        String::new()
    } else {
        let last_line = first_line.saturating_add(lines.len());
        format!(
            "  Lines {}–{} of {} · Column {}",
            first_line + 1,
            last_line,
            preview.line_count,
            first_column + 1
        )
    };
    lines.resize_with(rows, Line::default);
    let key = |action| app.primary_key_hint(ActionContext::FenceCopyPicker, action);
    lines.extend(
        [
            position,
            format!(
                "  {}/{} lines · {}/{} page",
                key(ActionId::NavigateUp),
                key(ActionId::NavigateDown),
                key(ActionId::PageUp),
                key(ActionId::PageDown)
            ),
            format!(
                "  {}/{} pan · {} copy · {} back",
                key(ActionId::PanLeft),
                key(ActionId::PanRight),
                key(ActionId::CopyValue),
                key(ActionId::Cancel)
            ),
        ]
        .into_iter()
        .map(|hint| Line::styled(clip_cells(&hint, columns), subtle)),
    );
    let title = match &entry.language {
        Some(language) => {
            let language = clip_preview(language, columns.saturating_sub(17));
            format!(" Code source · {language} ")
        }
        None => " Code source ".into(),
    };
    render_dialog(frame, area, &title, lines);
}

fn centered_area(screen: Rect, width: u16, height: u16) -> Rect {
    Rect::new(
        screen.x + (screen.width - width) / 2,
        screen.y + (screen.height - height) / 2,
        width,
        height,
    )
}

fn render_dialog(frame: &mut Frame, area: Rect, title: &str, lines: Vec<Line<'_>>) {
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(colors::brand()))
                .title(title),
        ),
        area,
    );
}

fn clip_preview(text: &str, width: usize) -> String {
    let text: String = text
        .chars()
        .map(|character| {
            if character.is_whitespace() {
                ' '
            } else {
                character
            }
        })
        .filter(|character| !character.is_control())
        .collect();
    clip_cells(&text, width)
}

#[cfg(test)]
mod tests {
    use crate::api::BambooClient;
    use crate::app::{App, FenceCopyEntry, FenceCopyPicker, FenceSourcePreview};
    use crate::keymap::Keymap;
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    #[test]
    fn picker_keeps_selected_source_cue_and_resolved_keys_visible_at_minimum_size() {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let selected_source = format!(
            "\r\n\t \r\n\tlet 参数 = '🧭';{}\r\nLater source row\r\n",
            "x".repeat(90)
        );
        let mut entries: Vec<_> = (0..20)
            .map(|index| FenceCopyEntry {
                language: Some("text".into()),
                source: format!("source cue {index}"),
            })
            .collect();
        entries[15] = FenceCopyEntry {
            language: None,
            source: String::new(),
        };
        entries[16] = FenceCopyEntry {
            language: Some("mermaid".into()),
            source: "\n\t  graph TD\n  A --> B\n".into(),
        };
        entries[17] = FenceCopyEntry {
            language: Some("rust".into()),
            source: selected_source.clone(),
        };
        let mut picker =
            FenceCopyPicker::new(1, "fixture:assistant".into(), 0, String::new(), entries);
        picker.selected = 17;
        app.fence_copy_picker = Some(picker);

        let mut terminal = Terminal::new(TestBackend::new(
            crate::ui::MIN_TERMINAL_WIDTH,
            crate::ui::MIN_TERMINAL_HEIGHT,
        ))
        .unwrap();
        for (open, copy, close) in [("Enter", "y", "Esc"), ("F7", "F8", "F9")] {
            if open == "F7" {
                app.set_keymap(
                    Keymap::from_json(
                        r#"{"bindings":[
                    {"context":"fence-copy-picker","action":"activate","keys":["F7"]},
                    {"context":"fence-copy-picker","action":"copy-value","keys":["F8"]},
                    {"context":"fence-copy-picker","action":"cancel","keys":["F9"]}
                ]}"#,
                    )
                    .unwrap(),
                    None,
                );
            }
            let text = render_terminal(&mut terminal, &app);
            if copy == "y" && std::env::var_os("BAMBOO_TUI_CAPTURE_FENCE_PICKER").is_some() {
                eprintln!("FENCE PICKER PREVIEW\n{text}\nEND FENCE PICKER PREVIEW");
            }
            for expected in [
                "Copy a code fence",
                "Code · (empty fence)",
                "mermaid · graph TD",
                "› rust · let",
                "Up/Down move",
                "PageUp/PageDown page",
                &format!("{open} source · {copy} copy · {close} close"),
            ] {
                assert!(text.contains(expected), "missing {expected:?}\n{text}");
            }
            let selection = text.lines().find(|line| line.contains("› rust")).unwrap();
            assert!(selection.replace(' ', "").contains("参数='🧭';"), "{text}");
            assert!(selection.contains('…'), "{text}");
            assert!(
                !text.contains("Later source row") && !text.contains("source cue 0"),
                "{text}"
            );
        }
        let picker = app.fence_copy_picker.as_ref().unwrap();
        assert_eq!(picker.rows_per_page.get(), 14);
        assert_eq!(picker.entries[17].source, selected_source);
    }

    #[test]
    fn source_preview_keeps_chrome_visible_as_code_scrolls_in_cells() {
        for width in [60, 80, 120] {
            let columns = usize::from(width - 4);
            let source = format!(
                "界e\u{301}🧭\tSOURCE\r\n{}終RIGHT\r\n{}\r\n{} SUFFIX\r\n",
                "-".repeat(columns - 1),
                (3..40)
                    .map(|line| format!("    // source line {line:02}"))
                    .collect::<Vec<_>>()
                    .join("\r\n"),
                "z".repeat(132),
            );
            let mut app = app_with_source_preview(&source);
            let mut terminal = Terminal::new(TestBackend::new(width, 20)).unwrap();
            let first = render_terminal(&mut terminal, &app);

            if width == 60 && std::env::var_os("BAMBOO_TUI_CAPTURE_FENCE_SOURCE").is_some() {
                eprintln!("FENCE SOURCE PREVIEW\n{first}\nEND FENCE SOURCE PREVIEW");
            }
            for expected in [
                "Code source · rust",
                "Lines 1–13 of 40 · Column 1",
                "Up/Down lines · PageUp/PageDown page",
                "Left/Right pan · y copy · Esc back",
            ] {
                assert!(first.contains(expected), "missing {expected:?}\n{first}");
            }
            assert!(!first.contains("SUFFIX"), "{first}");
            assert!(!first.contains('終'), "{first}");
            let buffer = terminal.backend().buffer();
            assert_eq!(buffer[(2, 2)].symbol(), "界", "{first}");
            assert_eq!(buffer[(4, 2)].symbol(), "e\u{301}", "{first}");
            assert_eq!(buffer[(5, 2)].symbol(), "🧭", "{first}");
            assert_eq!(buffer[(11, 2)].symbol(), "S", "{first}");
            assert_eq!(buffer[(width - 4, 3)].symbol(), "-", "{first}");
            assert_eq!(buffer[(width - 3, 3)].symbol(), " ", "{first}");

            let preview = app
                .fence_copy_picker
                .as_ref()
                .unwrap()
                .preview
                .as_ref()
                .unwrap();
            preview.move_by(0, 1);
            let panned = render_terminal(&mut terminal, &app);
            assert!(panned.contains("Column 2"), "{panned}");
            let buffer = terminal.backend().buffer();
            assert_eq!(buffer[(2, 2)].symbol(), " ", "{panned}");
            assert_eq!(buffer[(3, 2)].symbol(), "e\u{301}", "{panned}");
            assert_eq!(buffer[(4, 2)].symbol(), "🧭", "{panned}");

            preview.move_by(isize::MAX, isize::MAX);
            app.set_keymap(
                Keymap::from_json(
                    r#"{"bindings":[
                        {"context":"fence-copy-picker","action":"pan-left","keys":["F4"]},
                        {"context":"fence-copy-picker","action":"pan-right","keys":["F5"]},
                        {"context":"fence-copy-picker","action":"copy-value","keys":["F8"]},
                        {"context":"fence-copy-picker","action":"cancel","keys":["F9"]}
                    ]}"#,
                )
                .unwrap(),
                None,
            );
            let bottom = render_terminal(&mut terminal, &app);
            for expected in [
                " SUFFIX",
                "Lines 28–40 of 40",
                "F4/F5 pan · F8 copy · F9 back",
            ] {
                assert!(bottom.contains(expected), "missing {expected:?}\n{bottom}");
            }
            assert!(!bottom.contains("SOURCE"), "{bottom}");
        }
    }

    #[test]
    fn source_preview_clamps_scroll_when_terminal_grows() {
        let source = format!("{}{}", "short\n".repeat(19), "z".repeat(100));
        let app = app_with_source_preview(&source);
        let preview = app
            .fence_copy_picker
            .as_ref()
            .unwrap()
            .preview
            .as_ref()
            .unwrap();
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        render_terminal(&mut terminal, &app);
        preview.move_by(isize::MAX, isize::MAX);
        let scrolled = render_terminal(&mut terminal, &app);
        assert!(
            scrolled.contains("Lines 8–20 of 20 · Column 45"),
            "{scrolled}"
        );

        terminal.backend_mut().resize(120, 30);
        let expanded = render_terminal(&mut terminal, &app);
        assert!(
            expanded.contains("Lines 1–20 of 20 · Column 1"),
            "{expanded}"
        );
        assert!(expanded.contains(&"z".repeat(100)), "{expanded}");
        assert_eq!((preview.first_line(), preview.first_column()), (0, 0));
    }

    #[test]
    fn source_preview_explains_empty_fence_and_keeps_copy_and_back_available() {
        let app = app_with_source_preview("");
        let mut terminal = Terminal::new(TestBackend::new(60, 20)).unwrap();
        let text = render_terminal(&mut terminal, &app);
        assert!(text.contains("(empty fence)"), "{text}");
        assert!(text.contains("y copy · Esc back"), "{text}");
    }

    fn app_with_source_preview(source: &str) -> App {
        let mut app = App::new(BambooClient::new("http://127.0.0.1:0"));
        let entries = vec![FenceCopyEntry {
            language: Some("rust".into()),
            source: source.into(),
        }];
        let mut picker =
            FenceCopyPicker::new(1, "fixture:assistant".into(), 0, String::new(), entries);
        picker.preview = Some(FenceSourcePreview::new(source));
        app.fence_copy_picker = Some(picker);
        app
    }

    fn render_terminal(terminal: &mut Terminal<TestBackend>, app: &App) -> String {
        terminal
            .draw(|frame| crate::ui::render(frame, app))
            .unwrap();
        let buffer = terminal.backend().buffer();
        buffer
            .content()
            .chunks(usize::from(buffer.area().width))
            .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
            .collect::<Vec<_>>()
            .join("\n")
    }
}
