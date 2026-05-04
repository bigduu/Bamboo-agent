//! Unit tests for title_gen helpers (no LLM/AppState dependencies).

use super::*;

#[test]
fn test_is_untitled_handles_empty_and_default() {
    assert!(is_untitled(""));
    assert!(is_untitled("   "));
    assert!(is_untitled("New Session"));
    assert!(is_untitled("  New Session  "));
    assert!(!is_untitled("My great chat"));
    assert!(!is_untitled("new session")); // case-sensitive on purpose
}

#[test]
fn test_heuristic_title_strips_code_fences_and_caps_length() {
    let input = "```rust\nfn main() {}\n```\nHelp me debug this Rust program please";
    let t = heuristic_title(input);
    assert!(!t.is_empty());
    assert!(!t.contains("```"));
    assert!(!t.contains("fn main"));
    assert!(t.starts_with("Help me debug this Rust program"));

    let long = "a ".repeat(200);
    let t2 = heuristic_title(&long);
    assert!(t2.chars().count() <= 60);
}

#[test]
fn test_heuristic_title_first_sentence_only() {
    let input = "Refactor the auth module. Then add tests. Finally ship it!";
    assert_eq!(heuristic_title(input), "Refactor the auth module");
}

#[test]
fn test_heuristic_title_trims_punctuation_tail() {
    let input = "Plan a trip to Tokyo,";
    let t = heuristic_title(input);
    assert_eq!(t, "Plan a trip to Tokyo");
}

#[test]
fn test_sanitize_strips_quotes_and_caps() {
    assert_eq!(sanitize("\"Hello world.\""), "Hello world");
    assert_eq!(sanitize("  'A quoted title'  "), "A quoted title");
    assert_eq!(sanitize("First line\nSecond line"), "First line");

    let long = "x".repeat(120);
    let s = sanitize(&long);
    assert!(s.chars().count() <= MAX_TITLE_LEN);
}

#[test]
fn test_sanitize_empty_input() {
    assert_eq!(sanitize(""), "");
    assert_eq!(sanitize("   \n  "), "");
}

#[test]
fn test_build_title_messages_truncates_to_2000_chars() {
    let huge: String = "z".repeat(5000);
    let msgs = build_title_messages(&huge);
    assert_eq!(msgs.len(), 2);
    // First message is system, second is user.
    let user_msg = &msgs[1];
    assert_eq!(user_msg.role, bamboo_agent_core::Role::User);
    assert_eq!(user_msg.content.chars().count(), 2000);
}

#[test]
fn test_build_title_messages_short_input_passthrough() {
    let msgs = build_title_messages("Hi there");
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[1].content, "Hi there");
    assert_eq!(msgs[0].role, bamboo_agent_core::Role::System);
}
