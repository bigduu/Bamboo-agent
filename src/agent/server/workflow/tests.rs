use super::{WorkflowDefinition, WorkflowLoadError, WorkflowLoader};
use agent_core::composition::ToolExpr;
use std::fs;
use std::path::{Path, PathBuf};

fn temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("workflow-tests-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&dir).expect("should create temp dir");
    dir
}

fn write_workflow(path: &Path, yaml: &str) {
    fs::write(path, yaml).expect("should write workflow yaml");
}

fn workflow_yaml_with_args(name: &str) -> String {
    format!(
        r#"id: code-review
name: {name}
description: Automatically analyze code and provide review suggestions
version: "1.0.0"
type: composition
composition:
  type: sequence
  fail_fast: false
  steps:
    - type: call
      tool: read_file
      args:
        path: "${{file_path}}"
    - type: call
      tool: generate_report
      args: {{}}
"#
    )
}

fn workflow_yaml_missing_call_args() -> String {
    r#"id: code-review
name: Intelligent Code Review
description: Automatically analyze code and provide review suggestions
version: "1.0.0"
type: composition
composition:
  type: choice
  condition:
    type: contains
    path: "result"
    value: "error"
  then_branch:
    type: call
    tool: generate_fix
  else_branch:
    type: call
    tool: generate_report
"#
    .to_string()
}

#[test]
fn parses_yaml_definition_with_version_field() {
    let yaml = workflow_yaml_with_args("Intelligent Code Review");

    let workflow: WorkflowDefinition = serde_yaml::from_str(&yaml).expect("yaml should parse");

    assert_eq!(workflow.id, "code-review");
    assert_eq!(workflow.name, "Intelligent Code Review");
    assert_eq!(workflow.description, "Automatically analyze code and provide review suggestions");
    let json = serde_json::to_value(&workflow).expect("workflow should serialize");
    assert_eq!(json["version"], serde_json::json!("1.0.0"));

    match workflow.composition {
        ToolExpr::Sequence { steps, fail_fast } => {
            assert!(!fail_fast);
            assert_eq!(steps.len(), 2);
        }
        _ => panic!("expected sequence expression"),
    }
}

#[test]
fn load_from_file_normalizes_missing_call_args() {
    let dir = temp_dir();
    let path = dir.join("code-review.yaml");
    write_workflow(&path, &workflow_yaml_missing_call_args());

    let loader = WorkflowLoader::with_dir(dir.clone());
    let workflow = loader.load_from_file(&path).expect("workflow should parse");

    match workflow.composition {
        ToolExpr::Choice {
            then_branch,
            else_branch,
            ..
        } => {
            match *then_branch {
                ToolExpr::Call { args, .. } => assert_eq!(args, serde_json::json!({})),
                _ => panic!("expected call expression"),
            }

            match else_branch {
                Some(else_expr) => match *else_expr {
                    ToolExpr::Call { args, .. } => assert_eq!(args, serde_json::json!({})),
                    _ => panic!("expected call expression"),
                },
                None => panic!("expected else branch"),
            }
        }
        _ => panic!("expected choice expression"),
    }

    fs::remove_dir_all(dir).expect("should cleanup temp dir");
}

#[test]
fn load_all_from_directory_reads_only_yaml_files() {
    let dir = temp_dir();
    write_workflow(&dir.join("a.yaml"), &workflow_yaml_with_args("A"));
    write_workflow(&dir.join("b.yml"), &workflow_yaml_with_args("B"));
    fs::write(dir.join("README.md"), "ignore").expect("should write readme");

    let loader = WorkflowLoader::with_dir(dir.clone());
    let workflows = loader
        .load_all_from_directory(&dir)
        .expect("directory should load");

    assert_eq!(workflows.len(), 2);

    fs::remove_dir_all(dir).expect("should cleanup temp dir");
}

#[test]
fn load_from_file_rejects_invalid_workflow() {
    let dir = temp_dir();
    let path = dir.join("invalid.yaml");
    write_workflow(
        &path,
        r#"id: ""
name: Invalid
description: invalid workflow
version: "1.0.0"
composition:
  type: sequence
  steps: []
"#,
    );

    let loader = WorkflowLoader::with_dir(dir.clone());
    let error = loader
        .load_from_file(&path)
        .expect_err("expected invalid workflow error");

    match error {
        WorkflowLoadError::InvalidWorkflow { message, .. } => {
            assert!(message.contains("id") || message.contains("steps"));
        }
        _ => panic!("unexpected error variant"),
    }

    fs::remove_dir_all(dir).expect("should cleanup temp dir");
}
