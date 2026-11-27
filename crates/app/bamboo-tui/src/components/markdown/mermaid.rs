use ratatui::style::Style;
use ratatui::text::{Line, Span};

use crate::text::grapheme_cell_width;
use crate::theme::colors;

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

/// Preview one path in its declared direction. The source picker can still
/// access the original fence.
pub(super) fn render(source: &str, width: usize) -> Result<Vec<Line<'static>>, Unavailable> {
    let (direction, nodes) = parse_path(source).ok_or(Unavailable::Unsupported)?;
    let border = Style::default().fg(colors::code_border());
    match direction {
        Direction::TopDown => render_vertical(nodes, width, border),
        Direction::LeftToRight => render_horizontal(nodes, width, border),
    }
}

fn render_vertical(
    nodes: Vec<PathStep<'_>>,
    width: usize,
    border: Style,
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

    let rule_left = "─".repeat(center - 1);
    let rule_right = "─".repeat(label_width + 2 - center);
    let arrow = format!("{}↓", " ".repeat(center));
    let mut rows = Vec::with_capacity(nodes.len() * 4 - 1);
    let last_index = nodes.len() - 1;
    for (index, step) in nodes.into_iter().enumerate() {
        let node = step.display;
        let [top_left, top_right, bottom_left, bottom_right] = node.shape.corners();
        let top_center = if index == 0 { '─' } else { '┴' };
        let bottom_center = if index == last_index { '─' } else { '┬' };
        rows.push(Line::styled(
            format!("{top_left}{rule_left}{top_center}{rule_right}{top_right}"),
            border,
        ));
        rows.push(Line::from(vec![
            Span::styled("│ ", border),
            Span::raw(node.label.to_owned()),
            Span::styled(
                format!("{} │", " ".repeat(label_width - step.cells)),
                border,
            ),
        ]));
        rows.push(Line::styled(
            format!("{bottom_left}{rule_left}{bottom_center}{rule_right}{bottom_right}"),
            border,
        ));
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
        let [top_left, top_right, bottom_left, bottom_right] = node.shape.corners();
        let rule = "─".repeat(step.cells + 2);
        top.push(Span::styled(format!("{top_left}{rule}{top_right}"), border));
        bottom.push(bottom_left);
        bottom.push_str(&rule);
        bottom.push(bottom_right);

        let left_side = if index == 0 { "│ " } else { "┤ " };
        let right_side = if index == last_index { " │" } else { " ├" };
        middle.push(Span::styled(left_side, border));
        middle.push(Span::raw(node.label.to_owned()));
        middle.push(Span::styled(right_side, border));
        if index != last_index {
            let gap_width = step.horizontal_gap();
            if let Some(label) = step.outgoing_label {
                top.push(Span::raw(format!(" {} ", label.text)));
            } else {
                top.push(Span::styled("   ", border));
            }
            middle.push(Span::styled(
                format!("{}→", "─".repeat(gap_width - 1)),
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
}

impl NodeShape {
    fn corners(self) -> [char; 4] {
        match self {
            Self::Rectangle => ['┌', '┐', '└', '┘'],
            Self::Rounded => ['╭', '╮', '╰', '╯'],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct NodeDisplay<'a> {
    label: &'a str,
    shape: NodeShape,
}

#[derive(Clone, Copy)]
struct NodeRef<'a> {
    id: &'a str,
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
    if source.len() > MAX_SOURCE_BYTES {
        return None;
    }
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
        let (from, rest) = parse_node(statement)?;
        let from = register(&mut nodes, from)?;
        if rest.is_empty() {
            continue;
        }
        let (label, rest) = parse_link(rest)?;
        let (to, rest) = parse_node(rest)?;
        let to = register(&mut nodes, to)?;
        if !rest.is_empty() || from == to || nodes[from].next.is_some() || nodes[to].incoming {
            return None;
        }
        nodes[from].next = Some(Edge { target: to, label });
        nodes[to].incoming = true;
    }

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
        let display = node.reference.display.unwrap_or(NodeDisplay {
            label: node.reference.id,
            shape: NodeShape::Rectangle,
        });
        path.push(PathStep {
            display,
            cells: grapheme_cell_width(display.label),
            outgoing_label: node.next.and_then(|edge| edge.label),
        });
        match node.next {
            Some(edge) => index = edge.target,
            None => return (path.len() == nodes.len()).then_some((direction, path)),
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
        .position(|node| node.reference.id == reference.id)
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
        _ => return Some((NodeRef { id, display: None }, rest.trim_ascii_start())),
    };

    let (label, rest) = if let Some(quoted) = body.strip_prefix('"') {
        let (label, rest) = quoted.split_once('"')?;
        (label, rest.strip_prefix(closer)?)
    } else {
        let (label, rest) = body.split_once(closer)?;
        let unsupported_shape_text = match shape {
            NodeShape::Rectangle => label.trim_start().starts_with(['/', '|']),
            NodeShape::Rounded => label.contains('|'),
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
            id,
            display: Some(NodeDisplay { label, shape }),
        },
        rest.trim_ascii_start(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
            "A[Plan] --> B --> C",
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
                    render(&format!("flowchart {direction}\n{body}"), 120),
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
            "A -->|Chained| B --> C",
        ] {
            assert_eq!(
                render(&format!("flowchart LR\n{body}"), 120),
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
    fn vertical_labels_follow_their_edges_and_fit_by_cells() {
        let source = format!("flowchart TD\r\n{MIXED_EDGES}");
        let rows = render(&source, 13).unwrap();
        assert_eq!(rows.len(), 15);
        assert_eq!(rows[0].to_string(), "╭────────╮");
        assert_eq!(rows[3].to_string(), "    ↓ Go");
        assert_eq!(rows[4].to_string(), "┌───┴────┐");
        assert_eq!(rows[7].to_string(), "    ↓ 审核 👩‍💻");
        assert_eq!(rows[11].to_string(), "    ↓");
        assert_eq!(grapheme_cell_width(&rows[7].to_string()), 13);
        assert_eq!(render(&source, 12), Err(Unavailable::TooWide));
    }

    #[test]
    fn horizontal_labels_widen_only_their_own_gaps_and_fit_by_cells() {
        let source = format!("graph LR\r\n{MIXED_EDGES}");
        let rows = render(&source, 51).unwrap();
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
        assert_eq!(render(&source, 50), Err(Unavailable::TooWide));
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
        assert_eq!(
            render(&chain(MAX_NODES), 120).unwrap().len(),
            MAX_NODES * 4 - 1
        );
        assert_eq!(
            render(&chain(MAX_NODES + 1), 120),
            Err(Unavailable::Unsupported)
        );

        let mut source = chain(2);
        source.push_str(&" ".repeat(MAX_SOURCE_BYTES - source.len()));
        assert_eq!(render(&source, 120).unwrap().len(), 7);
        source.push(' ');
        assert_eq!(render(&source, 120), Err(Unavailable::Unsupported));
    }

    #[test]
    fn unicode_connectors_align_at_the_exact_cell_width() {
        let source = "flowchart TD\nA[检查 👩‍💻] --> B[结果]";
        let rows = render(source, 11).unwrap();
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
        assert_eq!(render(source, 10), Err(Unavailable::TooWide));
    }

    #[test]
    fn horizontal_boxes_fit_by_cells_and_preserve_graph_order() {
        let source = concat!(
            "graph LR;\r\n",
            "  B[Done];\r\n",
            "  A --> B\r\n",
            "  A[检查 👩‍💻]\r\n",
        );
        let rows = render(source, 22).unwrap();
        assert_eq!(
            rows.iter().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "┌─────────┐   ┌──────┐",
                "│ 检查 👩‍💻 ├──→┤ Done │",
                "└─────────┘   └──────┘"
            ]
        );
        assert!(rows.iter().all(|row| row.width() == 22));
        assert_eq!(render(source, 21), Err(Unavailable::TooWide));
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
        let rows = render(source, 11).unwrap();
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
        assert_eq!(render(source, 10), Err(Unavailable::TooWide));
    }

    #[test]
    fn horizontal_round_node_accepts_quoted_parentheses_and_fits_by_cells() {
        let source = "flowchart LR\nB[Done]\nA --> B\nA(\"Plan (ok)\")";
        let rows = render(source, 24).unwrap();
        assert_eq!(
            rows.iter().map(ToString::to_string).collect::<Vec<_>>(),
            [
                "╭───────────╮   ┌──────┐",
                "│ Plan (ok) ├──→┤ Done │",
                "╰───────────╯   └──────┘",
            ]
        );
        assert!(rows.iter().all(|row| row.width() == 24));
        assert_eq!(render(source, 23), Err(Unavailable::TooWide));
        assert!(render("graph LR\nA(\"- note -\") --> B(Task-)", 60).is_ok());
        assert!(render("graph LR\nA(\"One | Two\") --> B(/review)", 60).is_ok());
    }
}
