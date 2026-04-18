use super::validation::is_safe_workflow_name;

#[test]
fn safe_workflow_name_accepts_normal_names() {
    assert!(is_safe_workflow_name("my-workflow_01"));
    assert!(is_safe_workflow_name("workflow.v2"));
    assert!(is_safe_workflow_name("Workflow Name"));
}

#[test]
fn safe_workflow_name_rejects_path_traversal_and_control_chars() {
    assert!(!is_safe_workflow_name("../secret"));
    assert!(!is_safe_workflow_name("folder/name"));
    assert!(!is_safe_workflow_name("line\nbreak"));
    assert!(!is_safe_workflow_name(" null\0byte"));
}

#[test]
fn safe_workflow_name_rejects_reserved_windows_names() {
    assert!(!is_safe_workflow_name("CON"));
    assert!(!is_safe_workflow_name("nul.txt"));
    assert!(!is_safe_workflow_name("LPT1"));
}

#[test]
fn safe_workflow_name_rejects_empty_string() {
    assert!(!is_safe_workflow_name(""));
}

#[test]
fn safe_workflow_name_rejects_whitespace_only() {
    assert!(!is_safe_workflow_name("   "));
    assert!(!is_safe_workflow_name("\t"));
    assert!(!is_safe_workflow_name("\n"));
}

#[test]
fn safe_workflow_name_rejects_leading_trailing_whitespace() {
    assert!(!is_safe_workflow_name(" workflow"));
    assert!(!is_safe_workflow_name("workflow "));
    assert!(!is_safe_workflow_name(" workflow "));
}

#[test]
fn safe_workflow_name_rejects_path_separators() {
    assert!(!is_safe_workflow_name("path/to/workflow"));
    assert!(!is_safe_workflow_name("path\\to\\workflow"));
    assert!(!is_safe_workflow_name("a/b"));
    assert!(!is_safe_workflow_name("a\\b"));
}

#[test]
fn safe_workflow_name_rejects_double_dots() {
    assert!(!is_safe_workflow_name(".."));
    assert!(!is_safe_workflow_name("a..b"));
    assert!(!is_safe_workflow_name("test..workflow"));
}

#[test]
fn safe_workflow_name_rejects_control_characters() {
    assert!(!is_safe_workflow_name("\x01"));
    assert!(!is_safe_workflow_name("work\x02flow"));
    assert!(!is_safe_workflow_name("test\x1F"));
    assert!(!is_safe_workflow_name("\x7F"));
}

#[test]
fn safe_workflow_name_rejects_very_long_names() {
    let long_name = "a".repeat(256);
    assert!(!is_safe_workflow_name(&long_name));

    let exactly_255 = "a".repeat(255);
    assert!(is_safe_workflow_name(&exactly_255));
}

#[test]
fn safe_workflow_name_accepts_various_characters() {
    assert!(is_safe_workflow_name("workflow-1"));
    assert!(is_safe_workflow_name("workflow_2"));
    assert!(is_safe_workflow_name("workflow.3"));
    assert!(is_safe_workflow_name("workflow 4"));
    assert!(is_safe_workflow_name("Workflow5"));
    assert!(is_safe_workflow_name("123"));
}

#[test]
fn safe_workflow_name_rejects_all_reserved_windows_names() {
    let reserved = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];

    for name in reserved.iter() {
        assert!(!is_safe_workflow_name(name), "Should reject {}", name);
        assert!(
            !is_safe_workflow_name(&format!("{}.txt", name)),
            "Should reject {}.txt",
            name
        );
    }
}

#[test]
fn safe_workflow_name_rejects_special_characters() {
    assert!(!is_safe_workflow_name("workflow@home"));
    assert!(!is_safe_workflow_name("workflow#1"));
    assert!(!is_safe_workflow_name("workflow!"));
    assert!(!is_safe_workflow_name("workflow$"));
    assert!(!is_safe_workflow_name("workflow%"));
    assert!(!is_safe_workflow_name("workflow&"));
    assert!(!is_safe_workflow_name("workflow*"));
    assert!(!is_safe_workflow_name("workflow+"));
    assert!(!is_safe_workflow_name("workflow="));
    assert!(!is_safe_workflow_name("workflow|"));
    assert!(!is_safe_workflow_name("workflow?"));
    assert!(!is_safe_workflow_name("workflow["));
    assert!(!is_safe_workflow_name("workflow]"));
    assert!(!is_safe_workflow_name("workflow{"));
    assert!(!is_safe_workflow_name("workflow}"));
    assert!(!is_safe_workflow_name("workflow("));
    assert!(!is_safe_workflow_name("workflow)"));
    assert!(!is_safe_workflow_name("workflow<"));
    assert!(!is_safe_workflow_name("workflow>"));
    assert!(!is_safe_workflow_name("workflow,"));
    assert!(!is_safe_workflow_name("workflow:"));
    assert!(!is_safe_workflow_name("workflow;"));
}

#[test]
fn safe_workflow_name_accepts_edge_cases() {
    // Single character
    assert!(is_safe_workflow_name("a"));
    assert!(is_safe_workflow_name("Z"));
    assert!(is_safe_workflow_name("1"));

    // Numbers only
    assert!(is_safe_workflow_name("12345"));

    // Mixed case
    assert!(is_safe_workflow_name("MyWorkflow"));

    // With all allowed special chars
    assert!(is_safe_workflow_name("my-workflow_v2.3 test"));
}

#[test]
fn safe_workflow_name_accepts_unicode_alphanumeric() {
    // Unicode letters and numbers are considered alphanumeric by Rust
    assert!(is_safe_workflow_name("你好"));
    assert!(is_safe_workflow_name("工作流"));
    assert!(is_safe_workflow_name("ワークフロー"));
    assert!(is_safe_workflow_name("αβγ"));
    assert!(is_safe_workflow_name("τρόπος"));
}

#[test]
fn safe_workflow_name_rejects_unicode_special_chars() {
    // Special unicode symbols are not alphanumeric
    assert!(!is_safe_workflow_name("workflow©"));
    assert!(!is_safe_workflow_name("workflow®"));
    assert!(!is_safe_workflow_name("test™"));
    assert!(!is_safe_workflow_name("workflow€"));
}
