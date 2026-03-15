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
