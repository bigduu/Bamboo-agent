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

/// Preview one vertical path. The original fence remains available to the
/// source picker regardless of which view is shown.
pub(super) fn render(source: &str, width: usize) -> Result<Vec<Line<'static>>, Unavailable> {
    let labels = parse_path(source).ok_or(Unavailable::Unsupported)?;
    let labels: Vec<_> = labels
        .into_iter()
        .map(|label| (label, grapheme_cell_width(label)))
        .collect();
    let label_width = labels.iter().map(|(_, cells)| *cells).max().unwrap_or(0);
    if label_width + 4 > width {
        return Err(Unavailable::TooWide);
    }

    let center = (label_width + 3) / 2;
    let rule_left = "─".repeat(center - 1);
    let rule_right = "─".repeat(label_width + 2 - center);
    let arrow = format!("{}↓", " ".repeat(center));
    let border = Style::default().fg(colors::code_border());
    let mut rows = Vec::with_capacity(labels.len() * 4 - 1);
    let last_index = labels.len() - 1;
    for (index, (label, cells)) in labels.into_iter().enumerate() {
        let top_center = if index == 0 { '─' } else { '┴' };
        let bottom_center = if index == last_index { '─' } else { '┬' };
        rows.push(Line::styled(
            format!("┌{rule_left}{top_center}{rule_right}┐"),
            border,
        ));
        rows.push(Line::from(vec![
            Span::styled("│ ", border),
            Span::raw(label.to_owned()),
            Span::styled(format!("{} │", " ".repeat(label_width - cells)), border),
        ]));
        rows.push(Line::styled(
            format!("└{rule_left}{bottom_center}{rule_right}┘"),
            border,
        ));
        if index != last_index {
            rows.push(Line::styled(arrow.clone(), border));
        }
    }
    Ok(rows)
}

#[derive(Clone, Copy)]
struct NodeRef<'a> {
    id: &'a str,
    label: Option<&'a str>,
}

struct Node<'a> {
    reference: NodeRef<'a>,
    incoming: bool,
    next: Option<usize>,
}

fn parse_path(source: &str) -> Option<Vec<&str>> {
    if source.len() > MAX_SOURCE_BYTES {
        return None;
    }
    let mut statements = source
        .lines()
        .map(str::trim_ascii)
        .filter(|line| !line.is_empty())
        .map(|line| line.strip_suffix(';').unwrap_or(line).trim_ascii_end());
    let mut header = statements.next()?.split_ascii_whitespace();
    if !matches!(header.next(), Some("flowchart" | "graph"))
        || header.next() != Some("TD")
        || header.next().is_some()
    {
        return None;
    }

    let mut nodes = Vec::new();
    for statement in statements {
        let (from, rest) = parse_node(statement)?;
        let from = register(&mut nodes, from)?;
        if rest.is_empty() {
            continue;
        }
        let (to, rest) = parse_node(rest.strip_prefix("-->")?.trim_ascii_start())?;
        let to = register(&mut nodes, to)?;
        if !rest.is_empty() || from == to || nodes[from].next.is_some() || nodes[to].incoming {
            return None;
        }
        nodes[from].next = Some(to);
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
    let mut labels = Vec::with_capacity(nodes.len());
    for _ in 0..nodes.len() {
        let node = &nodes[index];
        labels.push(node.reference.label.unwrap_or(node.reference.id));
        match node.next {
            Some(next) => index = next,
            None => return (labels.len() == nodes.len()).then_some(labels),
        }
    }
    None
}

fn register<'a>(nodes: &mut Vec<Node<'a>>, reference: NodeRef<'a>) -> Option<usize> {
    if let Some(index) = nodes
        .iter()
        .position(|node| node.reference.id == reference.id)
    {
        if let Some(label) = reference.label {
            let saved = &mut nodes[index].reference.label;
            if saved.is_some_and(|saved| saved != label) {
                return None;
            }
            *saved = Some(label);
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
    let Some(body) = rest.strip_prefix('[') else {
        return Some((NodeRef { id, label: None }, rest.trim_ascii_start()));
    };

    let (label, rest) = body.split_once(']')?;
    let label = if let Some(quoted) = label.strip_prefix('"') {
        quoted.strip_suffix('"')?
    } else {
        if label.trim().starts_with(['/', '|']) || label.contains(['(', ')', '{', '}']) {
            return None;
        }
        label
    };
    let label = label.trim();
    if label.is_empty()
        || label.chars().any(char::is_control)
        || label.contains(['[', '"', '<', '>', '`', '\\', '#'])
        || grapheme_cell_width(label) == 0
    {
        return None;
    }
    Some((
        NodeRef {
            id,
            label: Some(label),
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
        assert_eq!(
            parse_path(source).unwrap(),
            ["计划 🧭", "Verify", "Deploy (done) & verify"]
        );
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
            "A[Plan] -->|Yes| B",
            "A[Plan] --> B --> C",
            "A[(Disk)] --> B",
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
            assert_eq!(
                render(&format!("flowchart TD\n{body}"), 120),
                Err(Unavailable::Unsupported),
                "{body}",
            );
        }
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
}
