use std::collections::{HashMap, HashSet};

use crate::{
    ValueRef, WorkflowPlan, WorkflowRunDefinition, WorkflowSecretHandle, WorkflowStepDefinition,
    WorkflowStepKind,
};
use thiserror::Error;

use super::schema::{validate_schema, validate_schema_shape as validate_shape};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WorkflowCompileError {
    #[error("unsupported workflow_schema {0}")]
    UnsupportedSchema(u32),
    #[error("workflow id and revision are required")]
    MissingIdentity,
    #[error("invalid workflow budget: {0}")]
    InvalidBudget(String),
    #[error("duplicate or empty step id: {0}")]
    DuplicateStep(String),
    #[error("unknown step reference: {0}")]
    UnknownStep(String),
    #[error("invalid step {step}: {message}")]
    InvalidStep { step: String, message: String },
    #[error("cyclic step dependency: {0}")]
    Cycle(String),
    #[error("invalid schema: {0}")]
    InvalidSchema(String),
}

#[derive(Debug, Clone)]
pub struct CompiledWorkflow {
    pub definition: WorkflowRunDefinition,
    pub steps: HashMap<String, WorkflowStepDefinition>,
}

impl CompiledWorkflow {
    pub fn compile(definition: WorkflowRunDefinition) -> Result<Self, WorkflowCompileError> {
        if definition.workflow_schema != 1 {
            return Err(WorkflowCompileError::UnsupportedSchema(
                definition.workflow_schema,
            ));
        }
        if definition.id.trim().is_empty() || definition.revision == 0 {
            return Err(WorkflowCompileError::MissingIdentity);
        }
        validate_budget(&definition)?;
        validate_schema_shape(&definition.input_schema)?;
        if let Some(schema) = &definition.output_schema {
            validate_schema_shape(schema)?;
        }
        let mut steps = HashMap::new();
        for step in &definition.steps {
            if step.id.trim().is_empty() || steps.insert(step.id.clone(), step.clone()).is_some() {
                return Err(WorkflowCompileError::DuplicateStep(step.id.clone()));
            }
            validate_step(step)?;
        }
        if steps.is_empty() {
            return Err(WorkflowCompileError::InvalidStep {
                step: "<definition>".to_string(),
                message: "at least one step is required".to_string(),
            });
        }
        validate_plan(&definition.plan, &steps)?;
        validate_dependencies(&definition.plan, &steps)?;
        validate_execution_bindings(&definition, &steps)?;
        Ok(Self { definition, steps })
    }

    pub fn validate_input(&self, value: &serde_json::Value) -> Result<(), String> {
        validate_schema(&self.definition.input_schema, value)
    }
}

fn validate_schema_shape(schema: &serde_json::Value) -> Result<(), WorkflowCompileError> {
    validate_shape(schema, "$").map_err(WorkflowCompileError::InvalidSchema)
}

fn validate_budget(definition: &WorkflowRunDefinition) -> Result<(), WorkflowCompileError> {
    let budget = &definition.budgets;
    if budget.max_concurrency == 0 || budget.max_concurrency > 64 {
        return Err(WorkflowCompileError::InvalidBudget(
            "max_concurrency must be 1..=64".to_string(),
        ));
    }
    if budget.max_steps == 0
        || budget.max_agents > budget.max_steps
        || budget.max_nesting_depth == 0
        || budget.wall_time_ms == 0
    {
        return Err(WorkflowCompileError::InvalidBudget(
            "step/agent/depth/wall budgets must be bounded and coherent".to_string(),
        ));
    }
    Ok(())
}

fn validate_step(step: &WorkflowStepDefinition) -> Result<(), WorkflowCompileError> {
    let required = match &step.kind {
        WorkflowStepKind::Tool { tool, .. } => tool,
        WorkflowStepKind::Agent {
            agent,
            structured_output_attempts,
            ..
        } => {
            if *structured_output_attempts == 0 || *structured_output_attempts > 8 {
                return Err(WorkflowCompileError::InvalidStep {
                    step: step.id.clone(),
                    message: "structured_output_attempts must be 1..=8".to_string(),
                });
            }
            agent
        }
        WorkflowStepKind::Workflow {
            workflow_id,
            revision,
            ..
        } => {
            if *revision == 0 {
                return Err(WorkflowCompileError::InvalidStep {
                    step: step.id.clone(),
                    message: "nested workflow revision must be fixed and non-zero".to_string(),
                });
            }
            workflow_id
        }
    };
    if required.trim().is_empty() {
        return Err(WorkflowCompileError::InvalidStep {
            step: step.id.clone(),
            message: "target cannot be empty".to_string(),
        });
    }
    if let Some(schema) = &step.output_schema {
        validate_schema_shape(schema)?;
    }
    Ok(())
}

fn validate_plan(
    plan: &WorkflowPlan,
    steps: &HashMap<String, WorkflowStepDefinition>,
) -> Result<(), WorkflowCompileError> {
    let mut seen = HashSet::new();
    validate_plan_inner(plan, steps, &mut seen)?;
    let unreferenced = steps
        .keys()
        .filter(|step| !seen.contains(*step))
        .cloned()
        .collect::<Vec<_>>();
    if !unreferenced.is_empty() {
        return Err(WorkflowCompileError::InvalidStep {
            step: "<plan>".to_string(),
            message: format!("unreferenced steps: {}", unreferenced.join(", ")),
        });
    }
    Ok(())
}

fn validate_plan_inner(
    plan: &WorkflowPlan,
    steps: &HashMap<String, WorkflowStepDefinition>,
    seen: &mut HashSet<String>,
) -> Result<(), WorkflowCompileError> {
    match plan {
        WorkflowPlan::Step { step } => {
            if !steps.contains_key(step) {
                return Err(WorkflowCompileError::UnknownStep(step.clone()));
            }
            if !seen.insert(step.clone()) {
                return Err(WorkflowCompileError::InvalidStep {
                    step: step.clone(),
                    message: "step appears more than once in the execution plan".to_string(),
                });
            }
        }
        WorkflowPlan::Sequence { nodes } | WorkflowPlan::Parallel { nodes } => {
            if nodes.is_empty() {
                return Err(WorkflowCompileError::InvalidStep {
                    step: "<plan>".to_string(),
                    message: "sequence/parallel requires nodes".to_string(),
                });
            }
            for node in nodes {
                validate_plan_inner(node, steps, seen)?;
            }
        }
        WorkflowPlan::Map { source, item, body } => {
            if item.trim().is_empty() {
                return Err(WorkflowCompileError::InvalidStep {
                    step: "<map>".to_string(),
                    message: "map item name cannot be empty".to_string(),
                });
            }
            validate_ref(source, steps)?;
            validate_plan_inner(body, steps, seen)?;
        }
        WorkflowPlan::Retry {
            node, max_attempts, ..
        } => {
            if *max_attempts == 0 {
                return Err(WorkflowCompileError::InvalidStep {
                    step: "<retry>".to_string(),
                    message: "max_attempts must be greater than zero".to_string(),
                });
            }
            validate_plan_inner(node, steps, seen)?;
        }
    }
    Ok(())
}

fn validate_execution_bindings(
    definition: &WorkflowRunDefinition,
    steps: &HashMap<String, WorkflowStepDefinition>,
) -> Result<(), WorkflowCompileError> {
    type ItemSchemas = HashMap<String, serde_json::Value>;

    fn reference_schema(
        owner: &str,
        reference: &ValueRef,
        definition: &WorkflowRunDefinition,
        steps: &HashMap<String, WorkflowStepDefinition>,
        available: &HashSet<String>,
        items: &ItemSchemas,
    ) -> Result<serde_json::Value, WorkflowCompileError> {
        let invalid = |message: String| WorkflowCompileError::InvalidStep {
            step: owner.to_string(),
            message,
        };
        match reference {
            ValueRef::Args { pointer } => schema_pointer(&definition.input_schema, pointer)
                .cloned()
                .ok_or_else(|| invalid("args reference has an invalid schema pointer".to_string())),
            ValueRef::Step { step, pointer } => {
                if !available.contains(step) {
                    return Err(invalid(format!(
                        "step reference '{step}' is not available before this step"
                    )));
                }
                let schema = steps
                    .get(step)
                    .and_then(|step| step.output_schema.as_ref())
                    .ok_or_else(|| {
                        invalid(format!(
                            "step reference '{step}' requires a declared output_schema"
                        ))
                    })?;
                schema_pointer(schema, pointer).cloned().ok_or_else(|| {
                    invalid(format!(
                        "step reference '{step}' has an invalid schema pointer"
                    ))
                })
            }
            ValueRef::Item { name, pointer } => {
                let schema = items.get(name).ok_or_else(|| {
                    invalid(format!(
                        "map item reference '{name}' is outside its map body"
                    ))
                })?;
                schema_pointer(schema, pointer).cloned().ok_or_else(|| {
                    invalid(format!(
                        "map item reference '{name}' has an invalid schema pointer"
                    ))
                })
            }
            ValueRef::Literal { value } => Ok(schema_for_literal(value)),
        }
    }

    fn walk_template(
        owner: &str,
        value: &serde_json::Value,
        definition: &WorkflowRunDefinition,
        steps: &HashMap<String, WorkflowStepDefinition>,
        available: &HashSet<String>,
        items: &ItemSchemas,
    ) -> Result<(), WorkflowCompileError> {
        match value {
            serde_json::Value::Object(object) if object.contains_key("$secret") => {
                let handle: WorkflowSecretHandle =
                    serde_json::from_value(value.clone()).map_err(|error| {
                        WorkflowCompileError::InvalidStep {
                            step: owner.to_string(),
                            message: format!("malformed secret capability handle: {error}"),
                        }
                    })?;
                if handle.capability.trim().is_empty() {
                    return Err(WorkflowCompileError::InvalidStep {
                        step: owner.to_string(),
                        message: "secret capability handle cannot be empty".to_string(),
                    });
                }
                Ok(())
            }
            serde_json::Value::Object(object) if object.contains_key("from") => {
                let reference: ValueRef =
                    serde_json::from_value(value.clone()).map_err(|error| {
                        WorkflowCompileError::InvalidStep {
                            step: owner.to_string(),
                            message: format!("malformed value reference: {error}"),
                        }
                    })?;
                reference_schema(owner, &reference, definition, steps, available, items)?;
                Ok(())
            }
            serde_json::Value::Object(object) => object.values().try_for_each(|child| {
                walk_template(owner, child, definition, steps, available, items)
            }),
            serde_json::Value::Array(array) => array.iter().try_for_each(|child| {
                walk_template(owner, child, definition, steps, available, items)
            }),
            _ => Ok(()),
        }
    }

    fn walk_plan(
        plan: &WorkflowPlan,
        definition: &WorkflowRunDefinition,
        steps: &HashMap<String, WorkflowStepDefinition>,
        available: &HashSet<String>,
        items: &ItemSchemas,
    ) -> Result<HashSet<String>, WorkflowCompileError> {
        match plan {
            WorkflowPlan::Step { step } => {
                let definition_step = &steps[step];
                let template = match &definition_step.kind {
                    WorkflowStepKind::Tool { args, .. }
                    | WorkflowStepKind::Workflow { args, .. } => args,
                    WorkflowStepKind::Agent { prompt, .. } => prompt,
                };
                walk_template(step, template, definition, steps, available, items)?;
                let mut result = available.clone();
                result.insert(step.clone());
                Ok(result)
            }
            WorkflowPlan::Sequence { nodes } => {
                nodes.iter().try_fold(available.clone(), |available, node| {
                    walk_plan(node, definition, steps, &available, items)
                })
            }
            WorkflowPlan::Parallel { nodes } => {
                let mut result = available.clone();
                for node in nodes {
                    // Every sibling receives the same pre-parallel availability;
                    // no sibling may consume another sibling's output.
                    result.extend(walk_plan(node, definition, steps, available, items)?);
                }
                Ok(result)
            }
            WorkflowPlan::Map { source, item, body } => {
                let item_schema =
                    reference_schema("<map>", source, definition, steps, available, items)?
                        .get("items")
                        .cloned()
                        .ok_or_else(|| WorkflowCompileError::InvalidStep {
                            step: "<map>".to_string(),
                            message: "map source schema must declare array items".to_string(),
                        })?;
                let mut nested_items = items.clone();
                nested_items.insert(item.clone(), item_schema);
                // Map body step outputs are materialized under per-item scoped
                // runtime ids (`step@map[index]`), not as root-level step ids.
                // Validate the body, but do not leak its availability to nodes
                // that execute after the map.
                walk_plan(body, definition, steps, available, &nested_items)?;
                Ok(available.clone())
            }
            WorkflowPlan::Retry { node, .. } => {
                walk_plan(node, definition, steps, available, items)
            }
        }
    }

    walk_plan(
        &definition.plan,
        definition,
        steps,
        &HashSet::new(),
        &HashMap::new(),
    )?;
    Ok(())
}

fn schema_for_literal(value: &serde_json::Value) -> serde_json::Value {
    let kind = match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    };
    serde_json::json!({"type": kind})
}

fn schema_pointer<'a>(
    schema: &'a serde_json::Value,
    pointer: &str,
) -> Option<&'a serde_json::Value> {
    if pointer.is_empty() {
        return Some(schema);
    }
    let mut current = schema;
    for token in pointer.strip_prefix('/')?.split('/') {
        let token = token.replace("~1", "/").replace("~0", "~");
        current = if token.parse::<usize>().is_ok() {
            current.get("items")?
        } else {
            current.get("properties")?.get(&token)?
        };
    }
    Some(current)
}

fn validate_ref(
    value_ref: &ValueRef,
    steps: &HashMap<String, WorkflowStepDefinition>,
) -> Result<(), WorkflowCompileError> {
    if let ValueRef::Step { step, .. } = value_ref {
        if !steps.contains_key(step) {
            return Err(WorkflowCompileError::UnknownStep(step.clone()));
        }
    }
    Ok(())
}

fn validate_dependencies(
    plan: &WorkflowPlan,
    steps: &HashMap<String, WorkflowStepDefinition>,
) -> Result<(), WorkflowCompileError> {
    let mut edges: HashMap<String, HashSet<String>> = HashMap::new();
    collect_plan_edges(plan, HashSet::new(), &mut edges);
    for (id, step) in steps {
        collect_value_step_refs(step, |dependency| {
            edges
                .entry(id.clone())
                .or_default()
                .insert(dependency.to_string());
        });
    }
    fn visit(
        node: &str,
        edges: &HashMap<String, HashSet<String>>,
        visiting: &mut HashSet<String>,
        visited: &mut HashSet<String>,
    ) -> Result<(), WorkflowCompileError> {
        if visited.contains(node) {
            return Ok(());
        }
        if !visiting.insert(node.to_string()) {
            return Err(WorkflowCompileError::Cycle(node.to_string()));
        }
        if let Some(dependencies) = edges.get(node) {
            for dependency in dependencies {
                visit(dependency, edges, visiting, visited)?;
            }
        }
        visiting.remove(node);
        visited.insert(node.to_string());
        Ok(())
    }
    let mut visited = HashSet::new();
    for id in steps.keys() {
        visit(id, &edges, &mut HashSet::new(), &mut visited)?;
    }
    Ok(())
}

fn collect_plan_edges(
    plan: &WorkflowPlan,
    previous: HashSet<String>,
    edges: &mut HashMap<String, HashSet<String>>,
) -> HashSet<String> {
    match plan {
        WorkflowPlan::Step { step } => {
            edges.entry(step.clone()).or_default().extend(previous);
            HashSet::from([step.clone()])
        }
        WorkflowPlan::Sequence { nodes } => nodes.iter().fold(previous, |prior, node| {
            collect_plan_edges(node, prior, edges)
        }),
        WorkflowPlan::Parallel { nodes } => {
            let mut tails = HashSet::new();
            for node in nodes {
                tails.extend(collect_plan_edges(node, previous.clone(), edges));
            }
            tails
        }
        WorkflowPlan::Map { body, .. } | WorkflowPlan::Retry { node: body, .. } => {
            collect_plan_edges(body, previous, edges)
        }
    }
}

fn collect_value_step_refs(step: &WorkflowStepDefinition, mut found: impl FnMut(&str)) {
    let value = match &step.kind {
        WorkflowStepKind::Tool { args, .. } | WorkflowStepKind::Workflow { args, .. } => args,
        WorkflowStepKind::Agent { prompt, .. } => prompt,
    };
    fn walk(value: &serde_json::Value, found: &mut impl FnMut(&str)) {
        match value {
            serde_json::Value::Object(object) => {
                if object.get("from").and_then(serde_json::Value::as_str) == Some("step") {
                    if let Some(step) = object.get("step").and_then(serde_json::Value::as_str) {
                        found(step);
                    }
                }
                for child in object.values() {
                    walk(child, found);
                }
            }
            serde_json::Value::Array(array) => {
                for child in array {
                    walk(child, found);
                }
            }
            _ => {}
        }
    }
    walk(value, &mut found);
}
