use super::{CachedWorkflow, WorkflowDefinition, WorkflowLoadError, WorkflowLoader};
use serde_yaml::{Mapping, Value};
use std::fs;
use std::path::Path;

pub(super) fn load_from_file(
    loader: &WorkflowLoader,
    path: &Path,
) -> Result<WorkflowDefinition, WorkflowLoadError> {
    ensure_file(path)?;

    let metadata = fs::metadata(path).map_err(|source| WorkflowLoadError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let modified = metadata.modified().ok();

    let cache = loader
        .cache
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(cached) = cache.get(path) {
        if cached.modified == modified {
            return Ok(cached.definition.clone());
        }
    }
    drop(cache);

    let content = fs::read_to_string(path).map_err(|source| WorkflowLoadError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    let definition = parse_definition(path, &content)?;
    loader.validate_with_path(path, &definition)?;

    let mut cache = loader
        .cache
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.insert(
        path.to_path_buf(),
        CachedWorkflow {
            modified,
            definition: definition.clone(),
        },
    );

    Ok(definition)
}

pub(super) fn load_all_from_directory(
    loader: &WorkflowLoader,
    dir: &Path,
) -> Result<Vec<WorkflowDefinition>, WorkflowLoadError> {
    if !dir.exists() {
        return Err(WorkflowLoadError::FileNotFound(dir.to_path_buf()));
    }

    if !dir.is_dir() {
        return Err(WorkflowLoadError::NotADirectory(dir.to_path_buf()));
    }

    let mut yaml_paths = Vec::new();
    let entries = fs::read_dir(dir).map_err(|source| WorkflowLoadError::Io {
        path: dir.to_path_buf(),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| WorkflowLoadError::Io {
            path: dir.to_path_buf(),
            source,
        })?;

        let path = entry.path();
        if path.is_file() && is_yaml_file(&path) {
            yaml_paths.push(path);
        }
    }

    yaml_paths.sort();

    let mut workflows = Vec::with_capacity(yaml_paths.len());
    for path in yaml_paths {
        workflows.push(load_from_file(loader, &path)?);
    }

    Ok(workflows)
}

fn parse_definition(path: &Path, content: &str) -> Result<WorkflowDefinition, WorkflowLoadError> {
    let mut value: Value =
        serde_yaml::from_str(content).map_err(|source| WorkflowLoadError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
    normalize_definition(&mut value);

    serde_yaml::from_value(value).map_err(|source| WorkflowLoadError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

fn normalize_definition(definition: &mut Value) {
    let Some(mapping) = definition.as_mapping_mut() else {
        return;
    };

    if let Some(composition) = mapping.get_mut(yaml_key("composition")) {
        normalize_expr(composition);
    }
}

fn normalize_expr(expr: &mut Value) {
    let Some(mapping) = expr.as_mapping_mut() else {
        return;
    };

    let expression_type = mapping
        .get(yaml_key("type"))
        .and_then(Value::as_str)
        .unwrap_or_default();

    match expression_type {
        "call" => normalize_call(mapping),
        "sequence" => normalize_children(mapping, "steps"),
        "parallel" => normalize_children(mapping, "branches"),
        "choice" => {
            normalize_child(mapping, "then_branch");
            normalize_child(mapping, "else_branch");
        }
        "retry" => normalize_child(mapping, "expr"),
        "let" => {
            normalize_child(mapping, "expr");
            normalize_child(mapping, "body");
        }
        _ => {}
    }
}

fn normalize_call(mapping: &mut Mapping) {
    let args_key = yaml_key("args");
    let needs_default = match mapping.get(&args_key) {
        Some(value) => value.is_null(),
        None => true,
    };

    if needs_default {
        mapping.insert(args_key, Value::Mapping(Mapping::new()));
    }
}

fn normalize_children(mapping: &mut Mapping, field: &str) {
    if let Some(children) = mapping
        .get_mut(yaml_key(field))
        .and_then(Value::as_sequence_mut)
    {
        for child in children {
            normalize_expr(child);
        }
    }
}

fn normalize_child(mapping: &mut Mapping, field: &str) {
    if let Some(child) = mapping.get_mut(yaml_key(field)) {
        normalize_expr(child);
    }
}

fn ensure_file(path: &Path) -> Result<(), WorkflowLoadError> {
    if !path.exists() {
        return Err(WorkflowLoadError::FileNotFound(path.to_path_buf()));
    }

    if !path.is_file() {
        return Err(WorkflowLoadError::NotAFile(path.to_path_buf()));
    }

    Ok(())
}

fn is_yaml_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| {
            let ext = ext.to_ascii_lowercase();
            ext == "yaml" || ext == "yml"
        })
        .unwrap_or(false)
}

fn yaml_key(value: &str) -> Value {
    Value::String(value.to_string())
}
