use crate::text::grapheme_cell_width;

use super::{
    first_word, link, linked_path, register, EdgeLabel, Node, NodeDisplay, NodeKey, NodeRef,
    NodeShape, PathStep,
};

const RESERVED_IDS: &[&str] = &[
    "state",
    "stateDiagram",
    "default",
    "click",
    "href",
    "direction",
    "scale",
    "note",
    "classDef",
    "class",
    "style",
    "accTitle",
    "accDescr",
    "hide",
];

pub(super) fn parse<'a>(statements: impl Iterator<Item = &'a str>) -> Option<Vec<PathStep<'a>>> {
    let mut nodes = Vec::new();
    for statement in statements {
        if contains_direction_directive(statement) {
            return None;
        }
        let (first, rest) = first_word(statement);
        if first.eq_ignore_ascii_case("state") {
            declare_state(&mut nodes, parse_alias(rest)?)?;
            continue;
        }

        let (definition, description) = match statement.split_once(':') {
            Some((definition, label)) => (definition.trim_ascii_end(), Some(plain_label(label)?)),
            None => (statement, None),
        };
        if let Some((from, to)) = definition.split_once("-->") {
            let from = register(&mut nodes, endpoint(from.trim_ascii(), NodeKey::Initial)?)?;
            let to = register(&mut nodes, endpoint(to.trim_ascii(), NodeKey::Final)?)?;
            let label = description.map(|text| EdgeLabel {
                text,
                cells: grapheme_cell_width(text),
            });
            link(&mut nodes, from, to, label)?;
        } else {
            declare_state(&mut nodes, named_state(definition, description)?)?;
        }
    }
    if !nodes
        .iter()
        .any(|node| matches!(node.reference.key, NodeKey::Named(_)))
    {
        return None;
    }
    // Mermaid assigns these IDs to the top-level endpoint markers. Keep the
    // source visible when a named state and its matching marker would collide.
    for (marker, reserved_id) in [
        (NodeKey::Initial, "root_start"),
        (NodeKey::Final, "root_end"),
    ] {
        if nodes.iter().any(|node| node.reference.key == marker)
            && nodes
                .iter()
                .any(|node| node.reference.key == NodeKey::Named(reserved_id))
        {
            return None;
        }
    }
    linked_path(&nodes, NodeShape::Rounded)
}

fn declare_state<'a>(nodes: &mut Vec<Node<'a>>, reference: NodeRef<'a>) -> Option<usize> {
    // Repeated descriptions form a title and body in Mermaid, even when the
    // text is identical. The plain preview supports a single label.
    if reference.display.is_some()
        && nodes
            .iter()
            .any(|node| node.reference.key == reference.key && node.reference.display.is_some())
    {
        return None;
    }
    register(nodes, reference)
}

fn parse_alias(value: &str) -> Option<NodeRef<'_>> {
    let (label, rest) = value.strip_prefix('"')?.split_once('"')?;
    let lowercase_label = label.to_ascii_lowercase();
    if ["[[fork]]", "[[join]]", "[[choice]]"]
        .into_iter()
        .any(|marker| lowercase_label.contains(marker))
    {
        return None;
    }
    // The declaration separates the closing quote, as, and the state ID.
    let rest = rest.strip_prefix(|character: char| character.is_ascii_whitespace())?;
    let (keyword, id) = first_word(rest.trim_ascii_start());
    if !keyword.eq_ignore_ascii_case("as") {
        return None;
    }
    named_state(id, Some(plain_label(label)?))
}

fn contains_direction_directive(statement: &str) -> bool {
    // The state lexer can consume a direction clause anywhere in a line.
    let directive_space = |character: char| character.is_whitespace() || character == '\u{feff}';
    let lowercase = statement.to_ascii_lowercase();
    lowercase.match_indices("direction").any(|(start, word)| {
        let rest = &lowercase[start + word.len()..];
        rest.strip_prefix(directive_space).is_some_and(|rest| {
            matches!(
                rest.trim_start_matches(directive_space).as_bytes().get(..2),
                Some(b"tb" | b"bt" | b"rl" | b"lr")
            )
        })
    })
}

fn endpoint<'a>(id: &'a str, marker: NodeKey<'a>) -> Option<NodeRef<'a>> {
    if id == "[*]" {
        return Some(NodeRef {
            key: marker,
            display: None,
        });
    }
    named_state(id, None)
}

fn named_state<'a>(id: &'a str, label: Option<&'a str>) -> Option<NodeRef<'a>> {
    if !id
        .chars()
        .next()
        .is_some_and(|character| character.is_alphabetic() || character == '_')
        || !id
            .chars()
            .all(|character| character.is_alphanumeric() || character == '_')
        || RESERVED_IDS
            .iter()
            .any(|word| id.eq_ignore_ascii_case(word))
    {
        return None;
    }
    Some(NodeRef {
        key: NodeKey::Named(id),
        display: label.map(|label| NodeDisplay {
            label,
            shape: NodeShape::Rounded,
        }),
    })
}

fn plain_label(text: &str) -> Option<&str> {
    let text = text.trim();
    if text.starts_with(':')
        || text.ends_with(':')
        || text.contains("::")
        || text.contains("%%")
        || text.contains(['<', '>', ';', '#', '{', '}', '`'])
        || text.chars().any(char::is_control)
        || grapheme_cell_width(text) == 0
    {
        return None;
    }
    Some(text)
}

#[cfg(test)]
mod tests {
    use super::super::{render, MermaidGlyphs, Unavailable, MAX_NODES, MAX_SOURCE_BYTES};
    use super::grapheme_cell_width;

    #[test]
    fn mermaid_state_orders_transitions_and_distinguishes_initial_and_final_markers() {
        let source = concat!(
            "stateDiagram-v2\r\n",
            "Processing --> [*]: 完成\r\n",
            "State \"运行中 👩‍💻\" AS Processing\r\n",
            "[*] --> Idle\r\n",
            "Idle --> Processing: 请求\r\n",
            "Idle: 待命\r\n",
        );
        let fixtures = [
            (
                MermaidGlyphs::Unicode,
                [
                    "      ●",
                    "      ↓",
                    "╭─────┴─────╮",
                    "│ 待命      │",
                    "╰─────┬─────╯",
                    "      ↓ 请求",
                    "╭─────┴─────╮",
                    "│ 运行中 👩‍💻 │",
                    "╰─────┬─────╯",
                    "      ↓ 完成",
                    "      ◎",
                ],
            ),
            (
                MermaidGlyphs::Ascii,
                [
                    "      *",
                    "      v",
                    ".-----+-----.",
                    "| 待命      |",
                    "'-----+-----'",
                    "      v 请求",
                    ".-----+-----.",
                    "| 运行中 👩‍💻 |",
                    "'-----+-----'",
                    "      v 完成",
                    "      @",
                ],
            ),
        ];
        for (glyphs, expected) in fixtures {
            let rows = render(source, 13, glyphs).unwrap();
            let text: Vec<_> = rows.iter().map(ToString::to_string).collect();
            assert_eq!(text, expected);
            assert!(text.iter().all(|row| grapheme_cell_width(row) <= 13));
            assert_eq!(render(source, 12, glyphs), Err(Unavailable::TooWide));
        }
    }

    #[test]
    fn mermaid_state_width_covers_transition_labels_and_ordinary_unicode_ids() {
        let source = "stateDiagram-v2\n准备\n准备 --> Running: 验收任务 👩‍💻\nRunning: 执行";
        let rows = render(source, 16, MermaidGlyphs::Unicode).unwrap();
        assert_eq!(rows[1].to_string(), "│ 准备 │");
        assert_eq!(rows[3].to_string(), "   ↓ 验收任务 👩‍💻");
        assert_eq!(grapheme_cell_width(&rows[3].to_string()), 16);
        assert_eq!(rows[5].to_string(), "│ 执行 │");
        assert_eq!(
            render(source, 15, MermaidGlyphs::Unicode),
            Err(Unavailable::TooWide)
        );
    }

    #[test]
    fn mermaid_state_preserves_plain_text_outside_directive_syntax() {
        let source = "stateDiagram-v2\nA: Choose [[fork]]\nA --> B: Check direction to exit";
        let rows = render(source, 60, MermaidGlyphs::Unicode).unwrap();
        assert!(rows[1].to_string().contains("Choose [[fork]]"));
        assert!(rows[3].to_string().contains("Check direction to exit"));
    }

    #[test]
    fn mermaid_state_unsupported_statements_reject_the_whole_preview() {
        for body in [
            "A --> B\nA --> C",
            "A --> C\nB --> C",
            "A --> B\nB --> A",
            "A --> B\nA --> B",
            "A --> A",
            "A --> B\nC --> D",
            "A --> B\nC",
            "A --> B\n[*] --> A\nA --> [*]",
            "A --> B\nstate B {\n  [*] --> Other\n}",
            "A --> B\nstate Fork <<fork>>",
            "A --> B\n--",
            "A --> B\nnote right of B: Context",
            "A --> B\n%%{init: {}}%%",
            "A --> B\ndirection LR",
            "A --> B: choose direction LR\nB --> C",
            "A --> B: choose direction\u{feff}LR\nB --> C",
            "A --> B: choose direction \u{feff}\tLR\nB --> C",
            "A --> B: redirection lr",
            "A --> B: choose DIRECTION\tTB",
            "A --> B\nstate \"choose direction LR\" as B",
            "A --> B\nstate \"Choose [[fork]] now\" as B",
            "A --> B\nstate \"Choose [[JOIN]] now\" as B",
            "A --> B\nstate \"Choose [[choice]] now\" as B",
            "A --> B:::style",
            "A --> B:",
            "A --> B:\nNew line",
            "A --> B --> C",
            "A -->",
            "A --> click",
            "[*] --> [*]",
            "A --> B\nstate \"First\" as A\nA: Conflicting label",
            "A --> B\nA: Ready\nA: Ready",
            "A --> B\nstate \"Ready\" as A\nA: Ready",
            "A --> B\nstate \"Ready\" as A\nstate \"Ready\" as A",
            "A --> B\nstate \"Missing close as B",
            "A --> B\nstate \"Unseparated\"as B",
            "A --> B\n[*]: Start",
        ] {
            let source = format!("stateDiagram-v2\n{body}");
            assert_eq!(
                render(&source, 120, MermaidGlyphs::Unicode),
                Err(Unavailable::Unsupported),
                "{body}"
            );
        }
    }

    #[test]
    fn mermaid_state_endpoint_markers_keep_generated_names_unambiguous() {
        for body in [
            "[*] --> root_start\nroot_start --> Ready",
            "Ready --> root_end\nroot_end --> [*]",
            "[*] --> root_start\nroot_start --> root_end\nroot_end --> [*]",
        ] {
            let source = format!("stateDiagram-v2\n{body}");
            assert_eq!(
                render(&source, 60, MermaidGlyphs::Unicode),
                Err(Unavailable::Unsupported)
            );
        }
        for body in [
            "root_start --> root_end",
            "[*] --> ROOT_START\nROOT_START --> ROOT_END\nROOT_END --> [*]",
            "[*] --> root_end\nroot_end --> Ready",
            "root_start --> Ready\nReady --> [*]",
        ] {
            let source = format!("stateDiagram-v2\n{body}");
            assert!(render(&source, 60, MermaidGlyphs::Ascii).is_ok(), "{body}");
        }
    }

    #[test]
    fn mermaid_state_marker_nodes_count_towards_the_shared_budgets() {
        let path = |count| {
            let mut chain = String::from("stateDiagram-v2\n[*] --> S1\n");
            for index in 1..count {
                chain.push_str(&format!("S{index} --> S{}\n", index + 1));
            }
            chain.push_str(&format!("S{count} --> [*]\n"));
            chain
        };
        let mut source = path(MAX_NODES - 2);
        assert!(render(&source, 60, MermaidGlyphs::Ascii).is_ok());
        assert_eq!(
            render(&path(MAX_NODES - 1), 60, MermaidGlyphs::Ascii),
            Err(Unavailable::Unsupported)
        );
        source.push_str(&" ".repeat(MAX_SOURCE_BYTES - source.len()));
        assert!(render(&source, 60, MermaidGlyphs::Unicode).is_ok());
        source.push(' ');
        assert_eq!(
            render(&source, 60, MermaidGlyphs::Unicode),
            Err(Unavailable::Unsupported)
        );
    }
}
