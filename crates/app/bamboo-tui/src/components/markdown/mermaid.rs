use ratatui::style::Style;
use ratatui::text::{Line, Span};

use crate::text::grapheme_cell_width;
use crate::theme::colors;

use super::MermaidGlyphs;

mod sequence;
mod state;

const MAX_SOURCE_BYTES: usize = 4 * 1024;
const MAX_NODES: usize = 8;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Unavailable {
    Unsupported,
    TooWide,
}

impl Unavailable {
    pub(super) fn status(&self) -> &'static str {
        match self {
            Self::Unsupported => "Mermaid source · unsupported diagram",
            Self::TooWide => "Mermaid source · diagram too wide",
        }
    }
}

/// Render a bounded Mermaid preview. The source picker retains the original
/// fence.
pub(super) fn render(
    source: &str,
    width: usize,
    glyphs: MermaidGlyphs,
) -> Result<Vec<Line<'static>>, Unavailable> {
    if source.len() > MAX_SOURCE_BYTES {
        return Err(Unavailable::Unsupported);
    }
    let border = Style::default().fg(colors::code_border());
    let glyphs = DiagramGlyphs::for_mode(glyphs);
    let mut statements = source
        .lines()
        .map(str::trim_ascii)
        .filter(|line| !line.is_empty());
    match statements.next() {
        Some("sequenceDiagram") => return sequence::render(statements, width, border, &glyphs),
        Some("stateDiagram-v2") => {
            let path = state::parse(statements).ok_or(Unavailable::Unsupported)?;
            return render_vertical(path, width, border, &glyphs);
        }
        _ => {}
    }

    let (direction, nodes) = parse_path(source).ok_or(Unavailable::Unsupported)?;
    match direction {
        Direction::TopDown => render_vertical(nodes, width, border, &glyphs),
        Direction::LeftToRight => render_horizontal(nodes, width, border, &glyphs),
    }
}

struct DiagramGlyphs {
    rule: &'static str,
    side: char,
    top_junction: &'static str,
    bottom_junction: &'static str,
    left_junction: char,
    right_junction: char,
    down_arrow: char,
    right_arrow: char,
    left_arrow: char,
    solid_connector: char,
    dotted_connector: char,
    initial_state: char,
    final_state: char,
    rectangle: [char; 4],
    rounded: [char; 4],
    decision: [char; 4],
}

impl DiagramGlyphs {
    fn for_mode(mode: MermaidGlyphs) -> Self {
        match mode {
            MermaidGlyphs::Unicode => Self {
                rule: "─",
                side: '│',
                top_junction: "┴",
                bottom_junction: "┬",
                left_junction: '┤',
                right_junction: '├',
                down_arrow: '↓',
                right_arrow: '→',
                left_arrow: '←',
                solid_connector: '─',
                dotted_connector: '┄',
                initial_state: '●',
                final_state: '◎',
                rectangle: ['┌', '┐', '└', '┘'],
                rounded: ['╭', '╮', '╰', '╯'],
                decision: ['╱', '╲', '╲', '╱'],
            },
            MermaidGlyphs::Ascii => Self {
                rule: "-",
                side: '|',
                top_junction: "+",
                bottom_junction: "+",
                left_junction: '+',
                right_junction: '+',
                down_arrow: 'v',
                right_arrow: '>',
                left_arrow: '<',
                solid_connector: '-',
                dotted_connector: '.',
                initial_state: '*',
                final_state: '@',
                rectangle: ['+'; 4],
                rounded: ['.', '.', '\'', '\''],
                decision: ['/', '\\', '\\', '/'],
            },
        }
    }

    fn corners(&self, shape: NodeShape) -> [char; 4] {
        match shape {
            NodeShape::Rectangle => self.rectangle,
            NodeShape::Rounded => self.rounded,
            NodeShape::Decision => self.decision,
        }
    }
}

fn render_vertical(
    nodes: Vec<PathStep<'_>>,
    width: usize,
    border: Style,
    glyphs: &DiagramGlyphs,
) -> Result<Vec<Line<'static>>, Unavailable> {
    let label_width = nodes.iter().map(|step| step.cells).max().unwrap_or(0);
    let center = (label_width + 3) / 2;
    let edge_width = nodes
        .iter()
        .filter_map(|step| step.outgoing_label)
        .map(|label| center + 2 + label.cells)
        .max()
        .unwrap_or(0);
    if (label_width + 4).max(edge_width) > width {
        return Err(Unavailable::TooWide);
    }

    let rule_left = glyphs.rule.repeat(center - 1);
    let rule_right = glyphs.rule.repeat(label_width + 2 - center);
    let side = glyphs.side;
    let arrow = format!("{}{}", " ".repeat(center), glyphs.down_arrow);
    let mut rows = Vec::with_capacity(nodes.len() * 4 - 1);
    let last_index = nodes.len() - 1;
    for (index, step) in nodes.into_iter().enumerate() {
        let marker = match step.key {
            NodeKey::Initial => Some(glyphs.initial_state),
            NodeKey::Final => Some(glyphs.final_state),
            NodeKey::Named(_) => None,
        };
        if let Some(marker) = marker {
            rows.push(Line::styled(
                format!("{}{marker}", " ".repeat(center)),
                border,
            ));
        } else {
            let node = step.display;
            let [top_left, top_right, bottom_left, bottom_right] = glyphs.corners(node.shape);
            let top_center = if index == 0 {
                glyphs.rule
            } else {
                glyphs.top_junction
            };
            let bottom_center = if index == last_index {
                glyphs.rule
            } else {
                glyphs.bottom_junction
            };
            rows.push(Line::styled(
                format!("{top_left}{rule_left}{top_center}{rule_right}{top_right}"),
                border,
            ));
            rows.push(Line::from(vec![
                Span::styled(format!("{side} "), border),
                Span::raw(node.label.to_owned()),
                Span::styled(
                    format!("{} {side}", " ".repeat(label_width - step.cells)),
                    border,
                ),
            ]));
            rows.push(Line::styled(
                format!("{bottom_left}{rule_left}{bottom_center}{rule_right}{bottom_right}"),
                border,
            ));
        }
        if index != last_index {
            let row = match step.outgoing_label {
                Some(label) => Line::from(vec![
                    Span::styled(arrow.clone(), border),
                    Span::raw(format!(" {}", label.text)),
                ]),
                None => Line::styled(arrow.clone(), border),
            };
            rows.push(row);
        }
    }
    Ok(rows)
}

fn render_horizontal(
    nodes: Vec<PathStep<'_>>,
    width: usize,
    border: Style,
    glyphs: &DiagramGlyphs,
) -> Result<Vec<Line<'static>>, Unavailable> {
    let last_index = nodes.len() - 1;
    let box_widths: usize = nodes.iter().map(|step| step.cells + 4).sum();
    let gap_widths: usize = nodes[..last_index]
        .iter()
        .map(PathStep::horizontal_gap)
        .sum();
    if box_widths + gap_widths > width {
        return Err(Unavailable::TooWide);
    }

    let mut top = Vec::new();
    let mut middle = Vec::new();
    let mut bottom = String::new();
    for (index, step) in nodes.into_iter().enumerate() {
        let node = step.display;
        let [top_left, top_right, bottom_left, bottom_right] = glyphs.corners(node.shape);
        let rule = glyphs.rule.repeat(step.cells + 2);
        top.push(Span::styled(format!("{top_left}{rule}{top_right}"), border));
        bottom.push(bottom_left);
        bottom.push_str(&rule);
        bottom.push(bottom_right);

        let left_side = if index == 0 {
            glyphs.side
        } else {
            glyphs.left_junction
        };
        let right_side = if index == last_index {
            glyphs.side
        } else {
            glyphs.right_junction
        };
        middle.push(Span::styled(format!("{left_side} "), border));
        middle.push(Span::raw(node.label.to_owned()));
        middle.push(Span::styled(format!(" {right_side}"), border));
        if index != last_index {
            let gap_width = step.horizontal_gap();
            if let Some(label) = step.outgoing_label {
                top.push(Span::raw(format!(" {} ", label.text)));
            } else {
                top.push(Span::styled("   ", border));
            }
            middle.push(Span::styled(
                format!(
                    "{}{}",
                    glyphs.rule.repeat(gap_width - 1),
                    glyphs.right_arrow
                ),
                border,
            ));
            bottom.push_str(&" ".repeat(gap_width));
        }
    }
    Ok(vec![
        Line::from(top),
        Line::from(middle),
        Line::styled(bottom, border),
    ])
}

#[derive(Debug, PartialEq, Eq)]
enum Direction {
    TopDown,
    LeftToRight,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeShape {
    Rectangle,
    Rounded,
    Decision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NodeDisplay<'a> {
    label: &'a str,
    shape: NodeShape,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NodeKey<'a> {
    Named(&'a str),
    Initial,
    Final,
}

#[derive(Clone, Copy)]
struct NodeRef<'a> {
    key: NodeKey<'a>,
    display: Option<NodeDisplay<'a>>,
}

#[derive(Clone, Copy)]
struct EdgeLabel<'a> {
    text: &'a str,
    cells: usize,
}

#[derive(Clone, Copy)]
struct Edge<'a> {
    target: usize,
    label: Option<EdgeLabel<'a>>,
}

struct Node<'a> {
    reference: NodeRef<'a>,
    incoming: bool,
    next: Option<Edge<'a>>,
}

struct PathStep<'a> {
    key: NodeKey<'a>,
    display: NodeDisplay<'a>,
    cells: usize,
    outgoing_label: Option<EdgeLabel<'a>>,
}

impl PathStep<'_> {
    fn horizontal_gap(&self) -> usize {
        self.outgoing_label.map_or(3, |label| label.cells + 2)
    }
}

fn parse_path(source: &str) -> Option<(Direction, Vec<PathStep<'_>>)> {
    let mut statements = source
        .lines()
        .map(str::trim_ascii)
        .filter(|line| !line.is_empty())
        .map(|line| line.strip_suffix(';').unwrap_or(line).trim_ascii_end());
    let mut header = statements.next()?.split_ascii_whitespace();
    if !matches!(header.next(), Some("flowchart" | "graph")) {
        return None;
    }
    let direction = match header.next()? {
        "TD" => Direction::TopDown,
        "LR" => Direction::LeftToRight,
        _ => return None,
    };
    if header.next().is_some() {
        return None;
    }

    let mut nodes = Vec::new();
    for statement in statements {
        let (first, mut rest) = parse_node(statement)?;
        let mut from = register(&mut nodes, first)?;
        while !rest.is_empty() {
            let (label, after_link) = parse_link(rest)?;
            let (target, after_node) = parse_node(after_link)?;
            let to = register(&mut nodes, target)?;
            link(&mut nodes, from, to, label)?;
            from = to;
            rest = after_node;
        }
    }

    linked_path(&nodes, NodeShape::Rectangle).map(|path| (direction, path))
}

fn linked_path<'a>(nodes: &[Node<'a>], default_shape: NodeShape) -> Option<Vec<PathStep<'a>>> {
    if nodes.len() < 2 {
        return None;
    }
    let mut roots = nodes.iter().enumerate().filter(|(_, node)| !node.incoming);
    let (mut index, _) = roots.next()?;
    if roots.next().is_some() {
        return None;
    }
    let mut path = Vec::with_capacity(nodes.len());
    for _ in 0..nodes.len() {
        let node = &nodes[index];
        let key = node.reference.key;
        let display = node.reference.display.unwrap_or(NodeDisplay {
            label: match key {
                NodeKey::Named(id) => id,
                NodeKey::Initial | NodeKey::Final => "",
            },
            shape: default_shape,
        });
        path.push(PathStep {
            key,
            display,
            cells: grapheme_cell_width(display.label),
            outgoing_label: node.next.and_then(|edge| edge.label),
        });
        match node.next {
            Some(edge) => index = edge.target,
            None => return (path.len() == nodes.len()).then_some(path),
        }
    }
    None
}

fn parse_link(input: &str) -> Option<(Option<EdgeLabel<'_>>, &str)> {
    let rest = input.strip_prefix("-->")?.trim_ascii_start();
    let Some(body) = rest.strip_prefix('|') else {
        return Some((None, rest));
    };
    let (label, rest) = body.split_once('|')?;
    let text = label.trim_matches([' ', '\t']);
    if text.trim().is_empty()
        || text.chars().any(char::is_control)
        || text.contains(['[', ']', '(', ')', '{', '}', '"', '<', '>', '`', '\\', '#'])
    {
        return None;
    }
    let cells = grapheme_cell_width(text);
    (cells != 0).then_some((Some(EdgeLabel { text, cells }), rest.trim_ascii_start()))
}

fn register<'a>(nodes: &mut Vec<Node<'a>>, reference: NodeRef<'a>) -> Option<usize> {
    if let Some(index) = nodes
        .iter()
        .position(|node| node.reference.key == reference.key)
    {
        if let Some(display) = reference.display {
            let saved = &mut nodes[index].reference.display;
            if saved.is_some_and(|saved| saved != display) {
                return None;
            }
            *saved = Some(display);
        }
        return Some(index);
    }
    if nodes.len() == MAX_NODES {
        return None;
    }
    nodes.push(Node {
        reference,
        incoming: false,
        next: None,
    });
    Some(nodes.len() - 1)
}

fn link<'a>(
    nodes: &mut [Node<'a>],
    from: usize,
    to: usize,
    label: Option<EdgeLabel<'a>>,
) -> Option<()> {
    if from == to || nodes[from].next.is_some() || nodes[to].incoming {
        return None;
    }
    nodes[from].next = Some(Edge { target: to, label });
    nodes[to].incoming = true;
    Some(())
}

fn first_word(text: &str) -> (&str, &str) {
    let end = text
        .find(|character: char| character.is_ascii_whitespace())
        .unwrap_or(text.len());
    (&text[..end], text[end..].trim_ascii_start())
}

fn parse_node(input: &str) -> Option<(NodeRef<'_>, &str)> {
    let id_end = input
        .bytes()
        .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        .count();
    let (id, rest) = input.split_at(id_end);
    if matches!(
        id,
        "" | "graph"
            | "flowchart"
            | "end"
            | "subgraph"
            | "direction"
            | "class"
            | "classDef"
            | "click"
            | "style"
            | "linkStyle"
            | "interpolate"
            | "call"
            | "href"
            | "_self"
            | "_blank"
            | "_parent"
            | "_top"
    ) {
        return None;
    }
    let (shape, closer, body) = match rest.as_bytes().first() {
        Some(b'[') => (NodeShape::Rectangle, ']', &rest[1..]),
        // Mermaid reserves "(-" for ellipses.
        Some(b'(') if !rest.starts_with("(-") => (NodeShape::Rounded, ')', &rest[1..]),
        // Double braces denote a different shape.
        Some(b'{') if !rest.starts_with("{{") => (NodeShape::Decision, '}', &rest[1..]),
        _ => {
            return Some((
                NodeRef {
                    key: NodeKey::Named(id),
                    display: None,
                },
                rest.trim_ascii_start(),
            ));
        }
    };

    let (label, rest) = if let Some(quoted) = body.strip_prefix('"') {
        let (label, rest) = quoted.split_once('"')?;
        (label, rest.strip_prefix(closer)?)
    } else {
        let (label, rest) = body.split_once(closer)?;
        let unsupported_shape_text = match shape {
            NodeShape::Rectangle => label.trim_start().starts_with(['/', '|']),
            NodeShape::Rounded | NodeShape::Decision => label.contains('|'),
        };
        if unsupported_shape_text || label.contains(['(', ')', '{', '}']) {
            return None;
        }
        (label, rest)
    };
    let label = label.trim();
    if label.is_empty()
        || label.chars().any(char::is_control)
        || label.contains(['[', ']', '"', '<', '>', '`', '\\', '#'])
        || grapheme_cell_width(label) == 0
    {
        return None;
    }
    Some((
        NodeRef {
            key: NodeKey::Named(id),
            display: Some(NodeDisplay { label, shape }),
        },
        rest.trim_ascii_start(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use MermaidGlyphs::{Ascii, Unicode};

    #[test]
    fn orders_rectangles_by_edges_with_unicode_and_plain_quoted_labels() {
        let source = concat!(
            "graph  TD;\r\n",
            "  Done[\"Deploy (done) & verify\"];\r\n",
            "  Validate --> Done\r\n",
            "  Plan[计划 🧭]-->Validate\r\n",
            "  Validate[Verify]\r\n",
        );
        let (direction, nodes) = parse_path(source).unwrap();
        assert_eq!(direction, Direction::TopDown);
        assert!(nodes
            .iter()
            .all(|step| step.display.shape == NodeShape::Rectangle));
        let labels: Vec<_> = nodes.iter().map(|step| step.display.label).collect();
        assert_eq!(labels, ["计划 🧭", "Verify", "Deploy (done) & verify"]);
    }

    #[test]
    fn refuses_shapes_branches_and_ambiguous_or_disconnected_graphs() {
        for body in [
            "A[A label] --> B\nA --> C",
            "A --> C\nB --> C",
            "A --> B\nB --> A",
            "A --> B\nC --> D\nD --> C",
            "A --> B\nA --> B",
            "A --> B\nC",
            "A --> A",
            "A --> B\nA[different]\nA[conflicting]",
            "A --> B\nA(Same)\nA[Same]",
            "A --> B\nA[Same]\nA(Same)",
            "A --> B\nA(different)\nA(conflicting)",
            "A --> B --> A",
            "A --> B --> B",
            "A --> B --> C\nB --> D",
            "A --> B --> C\nD --> C",
            "A --> B --> C\nA --> B",
            "A --> B --> C\nD --> E",
            "A --> B[Original] --> C\nB[Conflicting]",
            "A --> B -->",
            "A --> B -->\nC",
            "A --> B --> C;;",
            "A --> B; B --> C",
            "A --> B --> C & D",
            "A[(Disk)] --> B",
            "A((Circle)) --> B",
            "A([Stadium]) --> B",
            "A(-Ellipse-) --> B",
            "A(One | Two) --> B",
            "A(\"Missing quote) --> B",
            "A( ) --> B",
            "A[/Data/] --> B",
            "A[|width:wide|Start] --> B",
            "A[\"Missing quote] --> B",
            "A[<b>Styled</b>] --> B",
            "A[ ] --> B",
            "A --> end",
            "graph --> B",
            "A --> flowchart",
            "interpolate --> B",
            "call --> B",
            "A --> href",
            "A --> _blank",
            "A",
        ] {
            for direction in ["TD", "LR"] {
                assert_eq!(
                    render(&format!("flowchart {direction}\n{body}"), 120, Unicode),
                    Err(Unavailable::Unsupported),
                    "{body}",
                );
            }
        }
    }

    #[test]
    fn refuses_malformed_labels_and_other_edge_syntax() {
        for body in [
            "A -->|| B",
            "A -->| \t | B",
            "A -->|\u{a0}| B",
            "A -->|\u{301}| B",
            "A -->|Unclosed B",
            "A -->|Done|",
            "A -->|Yes|No| B",
            "A -->|Label\ncontinues| B",
            "A -->|\"Quoted\"| B",
            "A -->|\"`Markdown`\"| B",
            "A -->|<b>HTML</b>| B",
            "A -->|Nested (text)| B",
            "A -->|[Link]| B",
            "A -->|#alp; entity| B",
            "A -->|line\tbreak| B",
            r"A -->|Yes\|No| B",
            "A -- Label --> B",
            "A ---|Open| B",
            "A -.->|Dotted| B",
            "A ==>|Thick| B",
            "A --->|Longer| B",
            "A -->|Different| B\nA -->|Branch| C",
            "A -->|First| B\nB -->|Back| A",
            "A -->|First| B\nA -->|Duplicate| B",
            "A --> B -->|Unclosed C",
            "A --> B -- Label --> C",
        ] {
            assert_eq!(
                render(&format!("flowchart LR\n{body}"), 120, Unicode),
                Err(Unavailable::Unsupported),
                "{body:?}",
            );
        }
    }

    const MIXED_EDGES: &str = concat!(
        "C --> D[Done];\r\n",
        "B[Check] -->\t| 审核 👩‍💻 | C(Review)\r\n",
        "A(Plan) -->|Go|B",
    );

    #[test]
    fn same_line_chains_match_separate_edges_in_both_directions() {
        let compact = "B[Check];\r\nA(Plan)-->|Go|B[Check] -->\t| 审核 👩‍💻 |C(Review)-->D[Done];";
        for (direction, width) in [("TD", 13), ("LR", 51)] {
            let preview =
                |body, width| render(&format!("flowchart {direction}\r\n{body}"), width, Unicode);
            assert_eq!(
                preview("A[Plan] --> B --> C", width).unwrap(),
                preview("A[Plan] --> B\r\nB --> C", width).unwrap()
            );
            assert_eq!(
                preview(compact, width).unwrap(),
                preview(MIXED_EDGES, width).unwrap()
            );
            assert_eq!(preview(compact, width - 1), Err(Unavailable::TooWide));
        }
    }

    #[test]
    fn vertical_labels_follow_their_edges_and_fit_by_cells() {
        let source = format!("flowchart TD\r\n{MIXED_EDGES}");
        let rows = render(&source, 13, Unicode).unwrap();
        assert_eq!(rows.len(), 15);
        assert_eq!(rows[0].to_string(), "╭────────╮");
        assert_eq!(rows[3].to_string(), "    ↓ Go");
        assert_eq!(rows[4].to_string(), "┌───┴────┐");
        assert_eq!(rows[7].to_string(), "    ↓ 审核 👩‍💻");
        assert_eq!(rows[11].to_string(), "    ↓");
        assert_eq!(grapheme_cell_width(&rows[7].to_string()), 13);
        assert_eq!(render(&source, 12, Unicode), Err(Unavailable::TooWide));
    }

    #[test]
    fn horizontal_labels_widen_only_their_own_gaps_and_fit_by_cells() {
        let source = format!("graph LR\r\n{MIXED_EDGES}");
        let rows = render(&source, 51, Unicode).unwrap();
        let content: Vec<_> = rows.iter().map(ToString::to_string).collect();
        assert_eq!(
            content,
            [
                "╭──────╮ Go ┌───────┐ 审核 👩‍💻 ╭────────╮   ┌──────┐",
                "│ Plan ├───→┤ Check ├────────→┤ Review ├──→┤ Done │",
                "╰──────╯    └───────┘         ╰────────╯   └──────┘",
            ]
        );
        assert!(content.iter().all(|row| grapheme_cell_width(row) == 51));
        assert_eq!(render(&source, 50, Unicode), Err(Unavailable::TooWide));
    }

    #[test]
    fn source_and_node_budgets_have_fixed_boundaries() {
        let chain = |nodes| {
            let mut source = String::from("flowchart TD\n");
            for node in 1..nodes {
                source.push_str(&format!("N{} --> N{node}\n", node - 1));
            }
            source
        };
        let compact = |nodes| {
            let path = (0..nodes)
                .map(|node| format!("N{node}"))
                .collect::<Vec<_>>()
                .join("-->");
            format!("flowchart TD\n{path}")
        };
        for glyphs in [Unicode, Ascii] {
            assert_eq!(
                render(&chain(MAX_NODES), 120, glyphs).unwrap().len(),
                MAX_NODES * 4 - 1
            );
            assert_eq!(
                render(&chain(MAX_NODES + 1), 120, glyphs),
                Err(Unavailable::Unsupported)
            );
            assert_eq!(
                render(&compact(MAX_NODES), 120, glyphs).unwrap().len(),
                MAX_NODES * 4 - 1
            );
            assert_eq!(
                render(&compact(MAX_NODES + 1), 120, glyphs),
                Err(Unavailable::Unsupported)
            );

            let mut source = compact(3);
            source.push_str(&" ".repeat(MAX_SOURCE_BYTES - source.len()));
            assert_eq!(render(&source, 120, glyphs).unwrap().len(), 11);
            source.push(' ');
            assert_eq!(render(&source, 120, glyphs), Err(Unavailable::Unsupported));
        }
    }

    #[test]
    fn unicode_connectors_align_at_the_exact_cell_width() {
        let source = "flowchart TD\nA[检查 👩‍💻] --> B[结果]";
        let rows = render(source, 11, Unicode).unwrap();
        let content: Vec<_> = rows.iter().map(ToString::to_string).collect();
        assert_eq!(
            content,
            [
                "┌─────────┐",
                "│ 检查 👩‍💻 │",
                "└────┬────┘",
                "     ↓",
                "┌────┴────┐",
                "│ 结果    │",
                "└─────────┘"
            ]
        );
        assert!(rows.iter().all(|row| row.width() <= 11));
        assert_eq!(render(source, 10, Unicode), Err(Unavailable::TooWide));
    }

    #[test]
    fn horizontal_boxes_fit_by_cells_and_preserve_graph_order() {
        let source = concat!(
            "graph LR;\r\n",
            "  B[Done];\r\n",
            "  A --> B\r\n",
            "  A[检查 👩‍💻]\r\n",
        );
        let rows = render(source, 22, Unicode).unwrap();
        assert_eq!(
            rows.iter().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "┌─────────┐   ┌──────┐",
                "│ 检查 👩‍💻 ├──→┤ Done │",
                "└─────────┘   └──────┘"
            ]
        );
        assert!(rows.iter().all(|row| row.width() == 22));
        assert_eq!(render(source, 21, Unicode), Err(Unavailable::TooWide));
    }

    #[test]
    fn vertical_round_nodes_preserve_mixed_shape_and_bare_reference_layout() {
        let source = concat!(
            "graph TD\n",
            "B[Review]\n",
            "A(检查 👩‍💻) --> B\n",
            "B --> C\n",
            "A(\"检查 👩‍💻\")\n",
        );
        let rows = render(source, 11, Unicode).unwrap();
        assert_eq!(
            rows.iter().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "╭─────────╮",
                "│ 检查 👩‍💻 │",
                "╰────┬────╯",
                "     ↓",
                "┌────┴────┐",
                "│ Review  │",
                "└────┬────┘",
                "     ↓",
                "┌────┴────┐",
                "│ C       │",
                "└─────────┘",
            ]
        );
        assert_eq!(render(source, 10, Unicode), Err(Unavailable::TooWide));
    }

    #[test]
    fn horizontal_round_node_accepts_quoted_parentheses_and_fits_by_cells() {
        let source = "flowchart LR\nB[Done]\nA --> B\nA(\"Plan (ok)\")";
        let rows = render(source, 24, Unicode).unwrap();
        assert_eq!(
            rows.iter().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "╭───────────╮   ┌──────┐",
                "│ Plan (ok) ├──→┤ Done │",
                "╰───────────╯   └──────┘",
            ]
        );
        assert!(rows.iter().all(|row| row.width() == 24));
        assert_eq!(render(source, 23, Unicode), Err(Unavailable::TooWide));
        assert!(render("graph LR\nA(\"- note -\") --> B(Task-)", 60, Unicode).is_ok());
        assert!(render("graph LR\nA(\"One | Two\") --> B(/review)", 60, Unicode).is_ok());
    }

    #[test]
    fn ascii_glyphs_preserve_shapes_and_label_cells_at_the_same_boundaries() {
        let fixtures: [(&str, usize, &[&str]); 2] = [
            (
                "flowchart TD",
                11,
                &[
                    ".---------.",
                    "| 检查 👩‍💻 |",
                    "'----+----'",
                    "     v 审核",
                    "+----+----+",
                    "| 结果    |",
                    "+---------+",
                ],
            ),
            (
                "graph LR",
                25,
                &[
                    ".---------. 审核 +------+",
                    "| 检查 👩‍💻 +----->+ 结果 |",
                    "'---------'      +------+",
                ],
            ),
        ];
        for (header, width, expected) in fixtures {
            let source = format!("{header}\nA(检查 👩‍💻) -->|审核| B[结果]");
            let ascii = render(&source, width, Ascii).unwrap();
            let unicode = render(&source, width, Unicode).unwrap();
            let content: Vec<_> = ascii.iter().map(ToString::to_string).collect();
            assert_eq!(content, expected);
            assert_eq!(ascii.len(), unicode.len());
            for (ascii_row, unicode_row) in content.iter().zip(&unicode) {
                assert_eq!(
                    grapheme_cell_width(ascii_row),
                    grapheme_cell_width(&unicode_row.to_string())
                );
            }
            for glyphs in [Ascii, Unicode] {
                assert_eq!(
                    render(&source, width - 1, glyphs),
                    Err(Unavailable::TooWide)
                );
            }
        }
    }

    #[test]
    fn decision_nodes_keep_mixed_shapes_at_exact_td_and_lr_widths() {
        let fixtures: [(&str, MermaidGlyphs, usize, &[&str]); 4] = [
            (
                "flowchart TD",
                Unicode,
                11,
                &[
                    "┌─────────┐",
                    "│ Start   │",
                    "└────┬────┘",
                    "     ↓",
                    "╱────┴────╲",
                    "│ 检查 👩‍💻 │",
                    "╲────┬────╱",
                    "     ↓ 审",
                    "╭────┴────╮",
                    "│ Done    │",
                    "╰─────────╯",
                ],
            ),
            (
                "flowchart TD",
                Ascii,
                11,
                &[
                    "+---------+",
                    "| Start   |",
                    "+----+----+",
                    "     v",
                    "/----+----\\",
                    "| 检查 👩‍💻 |",
                    "\\----+----/",
                    "     v 审",
                    ".----+----.",
                    "| Done    |",
                    "'---------'",
                ],
            ),
            (
                "graph LR",
                Unicode,
                35,
                &[
                    "┌───────┐   ╱─────────╲ 审 ╭──────╮",
                    "│ Start ├──→┤ 检查 👩‍💻 ├───→┤ Done │",
                    "└───────┘   ╲─────────╱    ╰──────╯",
                ],
            ),
            (
                "graph LR",
                Ascii,
                35,
                &[
                    "+-------+   /---------\\ 审 .------.",
                    "| Start +-->+ 检查 👩‍💻 +--->+ Done |",
                    "+-------+   \\---------/    '------'",
                ],
            ),
        ];
        for (header, glyphs, width, expected) in fixtures {
            let source = format!(
                "{header}\r\nB -->|审| C(Done)\r\nB{{\"检查 👩‍💻\"}}\r\nA[Start] --> B\r\nB{{检查 👩‍💻}}"
            );
            let rows = render(&source, width, glyphs).unwrap();
            let content: Vec<_> = rows.iter().map(ToString::to_string).collect();
            assert_eq!(content, expected);
            assert_eq!(
                content.iter().map(|row| grapheme_cell_width(row)).max(),
                Some(width)
            );
            assert_eq!(
                render(&source, width - 1, glyphs),
                Err(Unavailable::TooWide)
            );
        }
    }

    #[test]
    fn refuses_malformed_and_branched_decisions_in_both_modes() {
        for body in [
            "A{{Hexagon}} --> B",
            "A{{\"Quoted hexagon\"}} --> B",
            "A{Extra close}} --> B",
            "A{\"Extra close\"}} --> B",
            "A{Unclosed --> B",
            "A{\"Unclosed quote} --> B",
            "A{ } --> B",
            "A{First | Second} --> B",
            "A{Check} --> B\nA --> C",
            "A{Check} --> B\nA[Check]",
        ] {
            let source = format!("flowchart TD\n{body}");
            for glyphs in [Unicode, Ascii] {
                assert_eq!(render(&source, 60, glyphs), Err(Unavailable::Unsupported));
            }
        }
    }
}
