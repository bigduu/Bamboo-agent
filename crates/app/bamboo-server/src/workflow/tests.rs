use super::{WorkflowDefinition, WorkflowLoadError, WorkflowLoader};
use bamboo_agent_core::composition::ToolExpr;
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
    assert_eq!(
        workflow.description,
        "Automatically analyze code and provide review suggestions"
    );
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

#[test]
fn load_yaml_preserves_explicit_scalar_tags_aliases_and_null_arg_normalization() {
    let dir = temp_dir();
    let path = dir.join("typed.yaml");
    write_workflow(
        &path,
        r#"id: typed
name: !!str 123
description: Typed YAML inputs
version: !!str 1.0
composition:
  type: sequence
  steps:
    - &shared
      type: call
      tool: inspect
      args:
        literal: !!str yes
        numeric: 12
        enabled: true
    - *shared
    - type: retry
      expr:
        type: call
        tool: report
        args: null
"#,
    );

    let workflow = WorkflowLoader::with_dir(dir.clone())
        .load_from_file(&path)
        .expect("typed mapping and aliases should parse");
    assert_eq!(workflow.name, "123");
    assert_eq!(workflow.version, "1.0");
    let ToolExpr::Sequence { steps, fail_fast } = workflow.composition else {
        panic!("expected sequence");
    };
    assert!(fail_fast);
    assert_eq!(steps[0], steps[1]);
    let ToolExpr::Call { args, .. } = &steps[0] else {
        panic!("expected call");
    };
    assert_eq!(
        args,
        &serde_json::json!({"literal": "yes", "numeric": 12, "enabled": true})
    );
    let ToolExpr::Retry {
        expr,
        max_attempts,
        delay_ms,
    } = &steps[2]
    else {
        panic!("expected retry");
    };
    assert_eq!(*max_attempts, 3);
    assert_eq!(*delay_ms, 1000);
    assert_eq!(**expr, ToolExpr::call("report", serde_json::json!({})));
    fs::remove_dir_all(dir).expect("should cleanup temp dir");
}

#[test]
fn malformed_yaml_keeps_public_parse_source_path_and_location() {
    let dir = temp_dir();
    let path = dir.join("malformed.yaml");
    write_workflow(&path, "id: broken\ncomposition: [\n");
    let error = WorkflowLoader::with_dir(dir.clone())
        .load_from_file(&path)
        .expect_err("malformed YAML should fail");
    let display = error.to_string();
    assert!(display.contains(&path.display().to_string()));
    assert!(std::error::Error::source(&error).is_some());
    let WorkflowLoadError::Parse {
        path: error_path,
        source,
    } = error
    else {
        panic!("expected public parse error");
    };
    assert_eq!(error_path, path);
    let location = source
        .location()
        .expect("syntax errors retain source location");
    assert!(location.line() >= 2);
    assert!(location.column() >= 1);
    fs::remove_dir_all(dir).expect("should cleanup temp dir");
}

#[test]
fn scalar_yaml_keeps_public_parse_error() {
    let dir = temp_dir();
    let path = dir.join("scalar.yaml");
    write_workflow(&path, "single scalar\n");
    let error = WorkflowLoader::with_dir(dir.clone())
        .load_from_file(&path)
        .expect_err("workflow must be a mapping");
    assert!(
        matches!(error, WorkflowLoadError::Parse { path: error_path, .. } if error_path == path)
    );
    fs::remove_dir_all(dir).expect("should cleanup temp dir");
}
