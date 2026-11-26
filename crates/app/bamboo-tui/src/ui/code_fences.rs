use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::Frame;

use crate::app::App;
use crate::keymap::{ActionContext, ActionId};
use crate::text::{clip_cells, display_width};
use crate::theme::colors;
use crate::ui::sessions::visible_window;

pub(super) fn render(frame: &mut Frame, app: &App) {
    let Some(picker) = &app.fence_copy_picker else {
        return;
    };
    let screen = frame.area();
    let width = screen.width.saturating_sub(2).min(72);
    let height = usize::from(screen.height.saturating_sub(2))
        .min(picker.entries.len().saturating_add(4)) as u16;
    let area = Rect::new(
        screen.x + (screen.width - width) / 2,
        screen.y + (screen.height - height) / 2,
        width,
        height,
    );
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
                "  {} copy · {} close",
                key(ActionId::CopyValue),
                key(ActionId::Cancel)
            ),
        ]
        .into_iter()
        .map(|hint| Line::styled(clip_cells(&hint, text_width), subtle)),
    );

    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(lines).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(colors::brand()))
                .title(" Copy a code fence "),
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
    use crate::app::{App, FenceCopyEntry, FenceCopyPicker};
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
        for (copy, close) in [("y", "Esc"), ("F8", "F9")] {
            if copy == "F8" {
                app.set_keymap(
                    Keymap::from_json(
                        r#"{"bindings":[
                    {"context":"fence-copy-picker","action":"copy-value","keys":["F8"]},
                    {"context":"fence-copy-picker","action":"cancel","keys":["F9"]}
                ]}"#,
                    )
                    .unwrap(),
                    None,
                );
            }
            terminal
                .draw(|frame| crate::ui::render(frame, &app))
                .unwrap();
            let buffer = terminal.backend().buffer();
            let text = buffer
                .content()
                .chunks(usize::from(buffer.area().width))
                .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
                .collect::<Vec<_>>()
                .join("\n");
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
                &format!("{copy} copy · {close} close"),
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
}
