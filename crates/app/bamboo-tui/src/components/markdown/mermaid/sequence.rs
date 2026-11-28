use ratatui::style::Style;
use ratatui::text::{Line, Span};

use crate::text::grapheme_cell_width;

use super::{DiagramGlyphs, Unavailable};

const MAX_PARTICIPANTS: usize = 6;
const MAX_MESSAGES: usize = 12;
const LANE_GAP: usize = 2;

// These Mermaid tokens can take over the lexer at a signal endpoint.
const RESERVED_IDS: &[&str] = &[
    "box",
    "participant",
    "actor",
    "create",
    "destroy",
    "loop",
    "rect",
    "opt",
    "alt",
    "else",
    "par",
    "par_over",
    "and",
    "critical",
    "option",
    "break",
    "end",
    "over",
    "note",
    "activate",
    "deactivate",
    "sequenceDiagram",
    "autonumber",
    "off",
    "link",
    "links",
    "properties",
    "details",
    "title",
    "accTitle",
    "accDescr",
];

pub(super) fn render<'a>(
    statements: impl Iterator<Item = &'a str>,
    width: usize,
    border: Style,
    glyphs: &DiagramGlyphs,
) -> Result<Vec<Line<'static>>, Unavailable> {
    let diagram = Diagram::parse(statements).ok_or(Unavailable::Unsupported)?;
    let lanes = diagram.lanes(width).ok_or(Unavailable::TooWide)?;
    let mut rows = draw_participants(&lanes, border, glyphs);
    let occupied_width = lanes.last().expect("a message has two lanes").end;
    let mut lifelines = vec![' '; occupied_width];
    for lane in &lanes {
        lifelines[lane.center] = glyphs.side;
    }
    for message in diagram.messages {
        let (left_lane, right_lane) = message.span();
        let left = lanes[left_lane].center;
        let right = lanes[right_lane].center;
        let start = left + (right - left - message.label.cells) / 2;
        let end = start + message.label.cells;
        rows.push(Line::from(vec![
            Span::styled(lifelines[..start].iter().collect::<String>(), border),
            Span::raw(message.label.text.to_owned()),
            Span::styled(lifelines[end..].iter().collect::<String>(), border),
        ]));

        let mut connector = lifelines.clone();
        connector[left + 1..right].fill(if message.dotted {
            glyphs.dotted_connector
        } else {
            glyphs.solid_connector
        });
        connector[lanes[message.to].center] = if message.from < message.to {
            glyphs.right_arrow
        } else {
            glyphs.left_arrow
        };
        rows.push(Line::styled(
            connector.into_iter().collect::<String>(),
            border,
        ));
    }
    Ok(rows)
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Label<'a> {
    text: &'a str,
    cells: usize,
}

impl<'a> Label<'a> {
    fn parse(text: &'a str) -> Option<Self> {
        let text = text.trim();
        let directive = text.strip_prefix(':').unwrap_or(text);
        if text.is_empty()
            || text.chars().any(char::is_control)
            || text.contains(['<', '>', ';', '#', '{', '}', '`'])
            || text.contains("%%")
            || directive.starts_with("wrap:")
            || directive.starts_with("nowrap:")
        {
            return None;
        }
        let cells = grapheme_cell_width(text);
        (cells > 0).then_some(Self { text, cells })
    }
}

struct Participant<'a> {
    id: &'a str,
    alias: Option<Label<'a>>,
}

impl<'a> Participant<'a> {
    fn label(&self) -> Label<'a> {
        self.alias.unwrap_or_else(|| Label {
            text: self.id,
            cells: grapheme_cell_width(self.id),
        })
    }
}

struct Message<'a> {
    from: usize,
    to: usize,
    label: Label<'a>,
    dotted: bool,
}

impl Message<'_> {
    fn span(&self) -> (usize, usize) {
        (self.from.min(self.to), self.from.max(self.to))
    }
}

struct Diagram<'a> {
    participants: Vec<Participant<'a>>,
    messages: Vec<Message<'a>>,
}

impl<'a> Diagram<'a> {
    fn parse(statements: impl Iterator<Item = &'a str>) -> Option<Self> {
        let mut diagram = Self {
            participants: Vec::new(),
            messages: Vec::new(),
        };
        for statement in statements {
            let (keyword, rest) = first_word(statement);
            if keyword.eq_ignore_ascii_case("participant") {
                let (id, alias) = first_word(rest);
                let alias = if alias.is_empty() {
                    None
                } else {
                    let (as_keyword, label) = first_word(alias);
                    if !as_keyword.eq_ignore_ascii_case("as") {
                        return None;
                    }
                    Some(Label::parse(label)?)
                };
                diagram.participant(id, alias)?;
                continue;
            }

            let (endpoints, label) = statement.split_once(':')?;
            let label = Label::parse(label)?;
            let (from, to, dotted) = match endpoints.split_once("-->>") {
                Some((from, to)) => (from, to, true),
                None => {
                    let (from, to) = endpoints.split_once("->>")?;
                    (from, to, false)
                }
            };
            let from = diagram.participant(from.trim_ascii(), None)?;
            let to = diagram.participant(to.trim_ascii(), None)?;
            if from == to || diagram.messages.len() == MAX_MESSAGES {
                return None;
            }
            diagram.messages.push(Message {
                from,
                to,
                label,
                dotted,
            });
        }
        (!diagram.messages.is_empty()).then_some(diagram)
    }

    fn participant(&mut self, id: &'a str, alias: Option<Label<'a>>) -> Option<usize> {
        if !is_plain_id(id) {
            return None;
        }
        if let Some(index) = self.participants.iter().position(|actor| actor.id == id) {
            let existing = &mut self.participants[index];
            if alias.is_some() && existing.alias.is_some() && alias != existing.alias {
                return None;
            }
            existing.alias = existing.alias.or(alias);
            return Some(index);
        }
        let index = self.participants.len();
        if index == MAX_PARTICIPANTS {
            return None;
        }
        self.participants.push(Participant { id, alias });
        Some(index)
    }

    /// Place participants from left to right. Widen only the distance needed
    /// to fit labels on messages whose right endpoint is at the current lane.
    fn lanes(&self, width: usize) -> Option<Vec<Lane<'a>>> {
        let mut lanes: Vec<Lane<'_>> = Vec::with_capacity(self.participants.len());
        for (index, participant) in self.participants.iter().enumerate() {
            let label = participant.label();
            let box_width = label.cells + 4;
            let box_center = (box_width - 1) / 2;
            let minimum_start = lanes.last().map_or(0, |lane| lane.end + LANE_GAP);
            let mut center = minimum_start + box_center;
            for message in &self.messages {
                let (left, right) = message.span();
                if right == index {
                    center = center.max(lanes[left].center + message.label.cells + 2);
                }
            }
            let start = center - box_center;
            let end = start + box_width;
            if end > width {
                return None;
            }
            lanes.push(Lane {
                label,
                start,
                center,
                end,
            });
        }
        Some(lanes)
    }
}

fn is_plain_id(id: &str) -> bool {
    let mut segments = id.split('-');
    let first = segments.next().unwrap_or_default();
    id.chars()
        .next()
        .is_some_and(|character| character.is_alphabetic() || character == '_')
        && id.chars().all(|character| character.is_alphanumeric() || matches!(character, '_' | '-'))
        && !RESERVED_IDS.iter().any(|word| first.eq_ignore_ascii_case(word))
        // Mermaid can read -x as a cross arrow.
        && segments.all(|segment| !segment.is_empty() && !segment.starts_with(['x', 'X']))
}

fn first_word(text: &str) -> (&str, &str) {
    let end = text
        .find(|character: char| character.is_ascii_whitespace())
        .unwrap_or(text.len());
    (&text[..end], text[end..].trim_ascii_start())
}

struct Lane<'a> {
    label: Label<'a>,
    start: usize,
    center: usize,
    end: usize,
}

fn draw_participants(
    lanes: &[Lane<'_>],
    border: Style,
    glyphs: &DiagramGlyphs,
) -> Vec<Line<'static>> {
    let mut top = String::new();
    let mut titles = Vec::new();
    let mut bottom = String::new();
    let [top_left, top_right, bottom_left, bottom_right] = glyphs.rectangle;
    let mut previous_end = 0;
    for lane in lanes {
        let gap = " ".repeat(lane.start - previous_end);
        top.push_str(&gap);
        top.push(top_left);
        top.push_str(&glyphs.rule.repeat(lane.end - lane.start - 2));
        top.push(top_right);

        let side = glyphs.side;
        titles.extend([
            Span::styled(format!("{gap}{side} "), border),
            Span::raw(lane.label.text.to_owned()),
            Span::styled(format!(" {side}"), border),
        ]);
        bottom.push_str(&gap);
        bottom.push(bottom_left);
        bottom.push_str(&glyphs.rule.repeat(lane.center - lane.start - 1));
        bottom.push_str(glyphs.bottom_junction);
        bottom.push_str(&glyphs.rule.repeat(lane.end - lane.center - 2));
        bottom.push(bottom_right);
        previous_end = lane.end;
    }
    vec![
        Line::styled(top, border),
        Line::from(titles),
        Line::styled(bottom, border),
    ]
}

#[cfg(test)]
mod tests {
    use super::super::{render, MermaidGlyphs, MAX_SOURCE_BYTES};
    use super::{grapheme_cell_width, Unavailable, MAX_MESSAGES, MAX_PARTICIPANTS};

    #[test]
    fn mermaid_sequence_aliases_and_opposite_arrows_fit_at_the_same_cell_width() {
        let source = concat!(
            "sequenceDiagram\r\n",
            "participant R as 审 👩‍💻\r\n",
            "participant C as API\r\n",
            "C ->> R: Go\r\n",
            "R -->> C: 回: OK\r\n",
        );
        let fixtures = [
            (
                MermaidGlyphs::Unicode,
                [
                    "┌───────┐  ┌─────┐",
                    "│ 审 👩‍💻 │  │ API │",
                    "└───┬───┘  └──┬──┘",
                    "    │   Go    │",
                    "    ←─────────│",
                    "    │ 回: OK  │",
                    "    │┄┄┄┄┄┄┄┄┄→",
                ],
            ),
            (
                MermaidGlyphs::Ascii,
                [
                    "+-------+  +-----+",
                    "| 审 👩‍💻 |  | API |",
                    "+---+---+  +--+--+",
                    "    |   Go    |",
                    "    <---------|",
                    "    | 回: OK  |",
                    "    |.........>",
                ],
            ),
        ];
        for (glyphs, expected) in fixtures {
            let rows = render(source, 18, glyphs).unwrap();
            let rows: Vec<_> = rows.iter().map(ToString::to_string).collect();
            assert!(rows.iter().all(|line| grapheme_cell_width(line) == 18));
            assert_eq!(
                rows.iter().map(|line| line.trim_end()).collect::<Vec<_>>(),
                expected
            );
            assert_eq!(render(source, 17, glyphs), Err(Unavailable::TooWide));
        }
    }

    #[test]
    fn mermaid_sequence_implicit_participants_follow_first_mentions_and_widen_only_the_needed_gap()
    {
        let source = concat!(
            "sequenceDiagram\n",
            "A->>B: Start\n",
            "B->>C: 审核完成 👩‍💻\n",
            "C-->>A: Done\n",
        );
        let rows = render(source, 25, MermaidGlyphs::Unicode).unwrap();
        assert_eq!(rows[1].to_string(), "│ A │  │ B │        │ C │");
        assert_eq!(rows[6].to_string().trim_end(), "  │      │────────────→");
        assert!(rows[5].to_string().contains("审核完成 👩‍💻"));
        assert_eq!(
            render(source, 24, MermaidGlyphs::Unicode),
            Err(Unavailable::TooWide)
        );
    }

    #[test]
    fn mermaid_sequence_ordinary_unicode_and_hyphenated_ids_have_cell_aligned_lanes() {
        let source = concat!(
            "sequenceDiagram\n",
            "PARTICIPANT edge-api\n",
            "Participant _db AS 数据库\n",
            "参加者->>edge-api: 请求\n",
            "edge-api->>_db: 查询\n",
            "_db-->>参加者: 完成\n",
        );
        let rows = render(source, 36, MermaidGlyphs::Unicode).unwrap();
        assert_eq!(rows[1].to_string(), "│ edge-api │  │ 数据库 │  │ 参加者 │");
        assert!(rows
            .iter()
            .all(|line| grapheme_cell_width(&line.to_string()) == 36));
        assert_eq!(
            render(source, 35, MermaidGlyphs::Unicode),
            Err(Unavailable::TooWide)
        );
    }

    #[test]
    fn mermaid_sequence_unsupported_constructs_keep_the_whole_diagram_out_of_the_preview() {
        for suffix in [
            "Note over A,B: Context",
            "par First\nA->>B: Continue\nend",
            "loop While open\nA->>B: Continue\nend",
            "%%{init: {}}%%",
            "autonumber",
            "actor C",
            "actor->>B: Query",
            "A->>END: Query",
            "end-user->>B: Query",
            "A->>edge-xray: Query",
            "A->>edge--api: Query",
            "A->>edge-: Query",
            "activate B",
            "A->>+B: Activate",
            "A->>A: Self",
            "A--xB: Cancel",
            "A->>B: <br/>",
            "A->>B: wrap: Query",
            "A->>B: :nowrap: Query",
            "participant C as :wrap: Description",
            "A->>B: First; B->>A: Second",
            "A->>B:",
            "A->>B->>C: Ambiguous",
            "participant C as",
            "participant A as First\nparticipant A as Second",
        ] {
            let source = format!("sequenceDiagram\nA->>B: Start\n{suffix}");
            for glyphs in [MermaidGlyphs::Unicode, MermaidGlyphs::Ascii] {
                assert_eq!(
                    render(&source, 120, glyphs),
                    Err(Unavailable::Unsupported),
                    "{suffix}"
                );
            }
        }
        assert_eq!(
            render(
                "sequenceDiagram\nparticipant A",
                120,
                MermaidGlyphs::Unicode
            ),
            Err(Unavailable::Unsupported)
        );
    }

    #[test]
    fn mermaid_sequence_enforces_its_participant_message_and_source_budgets() {
        let mut source = String::from("sequenceDiagram\n");
        for index in 0..MAX_PARTICIPANTS {
            source.push_str(&format!("participant N{index}\n"));
        }
        source.push_str(&"N0->>N1: Go\n".repeat(MAX_MESSAGES));
        let rows = render(&source, 120, MermaidGlyphs::Unicode).unwrap();
        assert_eq!(rows.len(), 3 + MAX_MESSAGES * 2);
        for overflow in [
            "participant Extra\n",
            "N0->>N1: Again\n",
            "N0-->>Extra: Bye\n",
        ] {
            assert_eq!(
                render(&format!("{source}{overflow}"), 120, MermaidGlyphs::Unicode),
                Err(Unavailable::Unsupported)
            );
        }
        source.push_str(&" ".repeat(MAX_SOURCE_BYTES - source.len()));
        assert_eq!(
            render(&source, 120, MermaidGlyphs::Ascii).unwrap().len(),
            rows.len()
        );
        source.push(' ');
        assert_eq!(
            render(&source, 120, MermaidGlyphs::Ascii),
            Err(Unavailable::Unsupported)
        );
    }
}
