use super::path::{should_skip_entry, to_display_name};
use std::path::Path;

#[test]
fn should_skip_entry_respects_hidden_flag() {
    assert!(should_skip_entry(".env", false, false));
    assert!(!should_skip_entry(".env", false, true));
}

#[test]
fn should_skip_entry_filters_known_build_dirs() {
    assert!(should_skip_entry("node_modules", true, true));
    assert!(should_skip_entry(".git", true, true));
    assert!(!should_skip_entry("src", true, true));
}

#[test]
fn to_display_name_returns_relative_path_when_possible() {
    let root = Path::new("/tmp/project");
    let nested = Path::new("/tmp/project/src/main.rs");
    assert_eq!(to_display_name(root, nested), "src/main.rs");
}
