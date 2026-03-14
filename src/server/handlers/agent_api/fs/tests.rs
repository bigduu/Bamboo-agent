use std::fs;

use tempfile::tempdir;

use super::{claude_home_file, parse_json, read_text_file, serialize_json_pretty, write_file};

#[test]
fn json_helpers_roundtrip_pretty_json() {
    let value = serde_json::json!({
        "model": "claude",
        "enabled": true
    });

    let serialized = serialize_json_pretty(&value, "settings").expect("serialize");
    let parsed = parse_json(&serialized, "settings").expect("parse");

    assert_eq!(parsed, value);
}

#[test]
fn read_and_write_file_helpers_roundtrip_text() {
    let dir = tempdir().expect("temp dir");
    let file_path = dir.path().join("example.txt");

    write_file(&file_path, "hello world", "example").expect("write");
    let content = read_text_file(&file_path, "example").expect("read");

    assert_eq!(content, "hello world");
}

#[test]
fn parse_json_returns_error_for_invalid_content() {
    let result = parse_json("{invalid", "settings");
    assert!(result.is_err());
}

#[test]
fn read_text_file_returns_error_for_missing_file() {
    let dir = tempdir().expect("temp dir");
    let missing = dir.path().join("missing.txt");
    let result = read_text_file(&missing, "missing");
    assert!(result.is_err());
}

#[test]
fn write_file_overwrites_existing_file() {
    let dir = tempdir().expect("temp dir");
    let file_path = dir.path().join("overwrite.txt");
    fs::write(&file_path, "before").expect("seed file");

    write_file(&file_path, "after", "overwrite").expect("write");
    let content = fs::read_to_string(&file_path).expect("read back");
    assert_eq!(content, "after");
}

#[test]
fn claude_home_file_appends_requested_filename() {
    let path = claude_home_file("settings.json").expect("home path");
    assert_eq!(
        path.file_name().and_then(|name| name.to_str()),
        Some("settings.json")
    );
}
