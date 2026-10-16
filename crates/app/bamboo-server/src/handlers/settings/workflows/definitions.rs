//! Explicit authoring of versioned orchestration definitions. Saving never runs a plan.
//!
//! The normal SkillStore remains the only catalog. Authoring owns only marked
//! User/Project bundles; embedded, plugin and externally edited bundles are not
//! adopted. All publication paths come from server-owned scope resolution.

use actix_web::{http::StatusCode, web, HttpResponse};
use bamboo_domain::{CompiledWorkflow, WorkflowRunDefinition};
use bamboo_skills::{WorkflowKind, WorkflowStatus};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{app_state::AppState, error::AppError};

use super::{
    definition_publication::{
        self, DefinitionPublicationError, DefinitionSnapshot, MAX_DEFINITION_BYTES,
    },
    handlers::{clone_target_scope, combined_catalog_snapshot, unique_winner, CloneTargetScope},
    types::CloneWorkflowTarget,
    validation::is_safe_workflow_name,
};

const AUTHORING_BODY: &str = "Run this versioned orchestration only after explicit user selection. Saving or editing this definition does not authorize execution.\n";
const MAX_DESCRIPTION_BYTES: usize = 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ValidateDefinitionRequest {
    pub definition: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateDefinitionRequest {
    pub target: CloneWorkflowTarget,
    #[serde(default)]
    pub session_id: Option<String>,
    pub description: String,
    pub definition: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateDefinitionRequest {
    pub target: CloneWorkflowTarget,
    #[serde(default)]
    pub session_id: Option<String>,
    pub expected_revision: u64,
    pub expected_digest: String,
    pub expected_skill_digest: String,
    pub definition: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefinitionQuery {
    pub target: CloneWorkflowTarget,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthoringSkillHeader {
    name: String,
    description: String,
    metadata: AuthoringMetadata,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuthoringMetadata {
    workflow_definition_authoring: u32,
}

fn error(status: StatusCode, message: &str) -> Box<HttpResponse> {
    Box::new(crate::error::json_error(status, message))
}

fn input_fields_preserved(input: &Value, normalized: &Value) -> bool {
    match (input, normalized) {
        (Value::Object(input), Value::Object(normalized)) => input.iter().all(|(key, value)| {
            normalized
                .get(key)
                .is_some_and(|normalized| input_fields_preserved(value, normalized))
        }),
        (Value::Array(input), Value::Array(normalized)) => {
            input.len() == normalized.len()
                && input
                    .iter()
                    .zip(normalized)
                    .all(|(input, normalized)| input_fields_preserved(input, normalized))
        }
        _ => input == normalized,
    }
}

fn validate_definition(value: &Value) -> Result<WorkflowRunDefinition, Box<HttpResponse>> {
    let encoded = serde_json::to_vec(value)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "Invalid workflow definition"))?;
    if encoded.len() > MAX_DEFINITION_BYTES {
        return Err(error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Workflow definition is too large",
        ));
    }
    // Workflow YAML also supports catalog policy fields. They are intentionally
    // not accepted in this explicit-authoring contract, even though domain
    // deserialization would otherwise ignore them. No caller can opt into
    // automatic execution by smuggling a metadata field alongside the plan.
    const FIELDS: &[&str] = &[
        "workflow_schema",
        "id",
        "revision",
        "input_schema",
        "output_schema",
        "steps",
        "plan",
        "budgets",
    ];
    if value
        .as_object()
        .is_none_or(|object| object.keys().any(|key| !FIELDS.contains(&key.as_str())))
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Unknown workflow definition field",
        ));
    }
    let definition: WorkflowRunDefinition = serde_json::from_value(value.clone())
        .map_err(|_| error(StatusCode::BAD_REQUEST, "Invalid workflow definition shape"))?;
    let mut normalized = serde_json::to_value(&definition)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "Invalid workflow definition"))?;
    // Optional typed nulls are intentionally omitted by the domain serializer.
    // Restore only the known fields before checking that nothing was discarded.
    normalized
        .as_object_mut()
        .expect("typed definition object")
        .entry("output_schema")
        .or_insert(Value::Null);
    for step in normalized["steps"]
        .as_array_mut()
        .expect("typed steps array")
    {
        let agent = step["type"] == "agent";
        let object = step.as_object_mut().expect("typed step object");
        object.entry("output_schema").or_insert(Value::Null);
        if agent {
            object.entry("model").or_insert(Value::Null);
            object.entry("effort").or_insert(Value::Null);
        }
    }
    let budgets = normalized["budgets"]
        .as_object_mut()
        .expect("typed budgets object");
    budgets.entry("max_tokens").or_insert(Value::Null);
    budgets.entry("max_cost_micros").or_insert(Value::Null);
    if !input_fields_preserved(value, &normalized) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Unknown workflow field or unsupported value",
        ));
    }
    if serde_json::to_vec_pretty(&definition)
        .map_err(|_| error(StatusCode::BAD_REQUEST, "Invalid workflow definition"))?
        .len()
        > MAX_DEFINITION_BYTES
    {
        return Err(error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "Normalized workflow definition is too large",
        ));
    }
    if !is_safe_workflow_name(&definition.id)
        || definition.id.starts_with('.')
        || definition.id.ends_with('.')
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Invalid workflow definition id",
        ));
    }
    skill_bytes(&definition.id, "Definition validation")?;
    CompiledWorkflow::compile(definition.clone()).map_err(|problem| {
        let detail: String = problem.to_string().chars().take(1024).collect();
        error(
            StatusCode::BAD_REQUEST,
            &format!("Workflow validation failed: {detail}"),
        )
    })?;
    Ok(definition)
}

fn skill_bytes(id: &str, description: &str) -> Result<Vec<u8>, Box<HttpResponse>> {
    let description = description.trim();
    if description.is_empty()
        || description.len() > MAX_DESCRIPTION_BYTES
        || description.contains('\0')
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "Workflow description must be 1..=1024 bytes",
        ));
    }
    let header = AuthoringSkillHeader {
        name: id.to_string(),
        description: description.to_string(),
        metadata: AuthoringMetadata {
            workflow_definition_authoring: 1,
        },
    };
    let yaml = serde_yaml::to_string(&header).map_err(|_| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Workflow metadata could not be encoded",
        )
    })?;
    let markdown = format!("---\n{yaml}---\n{AUTHORING_BODY}");
    bamboo_skills::store::parser::parse_markdown_skill(
        &std::path::Path::new(id).join("SKILL.md"),
        &markdown,
    )
    .map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            "Workflow id must be a canonical Skill name (lowercase kebab-case, at most 64 bytes)",
        )
    })?;
    Ok(markdown.into_bytes())
}

fn verify_authored_skill(id: &str, bytes: &[u8]) -> Result<String, Box<HttpResponse>> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        error(
            StatusCode::CONFLICT,
            "Workflow bundle is not managed by definition authoring",
        )
    })?;
    let header = text
        .strip_prefix("---\n")
        .and_then(|text| text.split_once("\n---\n"))
        .map(|(header, _)| header)
        .ok_or_else(|| {
            error(
                StatusCode::CONFLICT,
                "Workflow bundle is not managed by definition authoring",
            )
        })?;
    let header: AuthoringSkillHeader = serde_yaml::from_str(header).map_err(|_| {
        error(
            StatusCode::CONFLICT,
            "Workflow bundle is not managed by definition authoring",
        )
    })?;
    if header.name != id
        || header.metadata.workflow_definition_authoring != 1
        || skill_bytes(id, &header.description)? != bytes
    {
        return Err(error(
            StatusCode::CONFLICT,
            "Workflow metadata was edited externally; preserve and reconcile it before updating",
        ));
    }
    Ok(header.description)
}

fn stored_definition(
    snapshot: &DefinitionSnapshot,
) -> Result<WorkflowRunDefinition, Box<HttpResponse>> {
    let value: Value = serde_yaml::from_slice(&snapshot.bytes).map_err(|_| {
        error(
            StatusCode::CONFLICT,
            "Stored definition was edited externally; reconcile it before updating",
        )
    })?;
    validate_definition(&value).map_err(|_| {
        error(
            StatusCode::CONFLICT,
            "Stored definition contains unsupported edits; reconcile it before updating",
        )
    })
}

async fn validate_nested_definitions(
    scope: &CloneTargetScope,
    definition: &WorkflowRunDefinition,
) -> Result<(), Box<HttpResponse>> {
    for step in &definition.steps {
        if let bamboo_domain::WorkflowStepKind::Workflow {
            workflow_id,
            revision,
            ..
        } = &step.kind
        {
            let bundle = scope
                .store
                .pin_workflow_definition_bundle(workflow_id, *revision)
                .await
                .map_err(|_| {
                    error(
                        StatusCode::BAD_REQUEST,
                        "Nested workflow revision is unavailable in the selected catalog",
                    )
                })?;
            if bundle
                .definitions
                .values()
                .any(|nested| nested.id == definition.id)
            {
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "Nested workflow would depend on the definition being replaced",
                ));
            }
        }
    }
    Ok(())
}

fn publication_error(problem: DefinitionPublicationError) -> Box<HttpResponse> {
    match problem {
        DefinitionPublicationError::Invalid(message) => error(StatusCode::BAD_REQUEST, message),
        DefinitionPublicationError::Conflict(message) => error(StatusCode::CONFLICT, message),
        DefinitionPublicationError::Io(problem)
            if problem.kind() == std::io::ErrorKind::NotFound =>
        {
            error(StatusCode::NOT_FOUND, "Workflow definition not found")
        }
        DefinitionPublicationError::Io(problem) => {
            tracing::error!(%problem, "workflow definition publication failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Workflow publication could not be confirmed; read the definition before retrying",
            )
        }
        DefinitionPublicationError::Internal(message)
        | DefinitionPublicationError::PublicationUncertain(message) => {
            tracing::error!(
                reason = message,
                "workflow definition publication could not be confirmed"
            );
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Workflow publication could not be confirmed; read the definition before retrying",
            )
        }
    }
}

async fn scope_for(
    state: &AppState,
    target: CloneWorkflowTarget,
    session_id: Option<&str>,
) -> Result<CloneTargetScope, Box<HttpResponse>> {
    clone_target_scope(state, target, session_id).await
}

async fn require_winner(
    scope: &CloneTargetScope,
    id: &str,
    creating: bool,
) -> Result<(), Box<HttpResponse>> {
    scope.store.reload().await.map_err(|problem| {
        tracing::error!(%problem, "workflow authoring catalog refresh failed");
        error(
            StatusCode::SERVICE_UNAVAILABLE,
            "Workflow catalog is unavailable",
        )
    })?;
    let catalog = combined_catalog_snapshot(&scope.store).await;
    let winner = unique_winner(&catalog, id)
        .map_err(|_| error(StatusCode::CONFLICT, "Workflow identity is ambiguous"))?;
    match (creating, winner) {
        (true, None) => Ok(()),
        (true, Some(_)) => Err(error(
            StatusCode::CONFLICT,
            "Workflow id already exists; choose a new id",
        )),
        (false, Some(entry))
            if entry.source == scope.published_source
                && entry.kind == WorkflowKind::Orchestration
                && entry.status == WorkflowStatus::Valid =>
        {
            let root = scope
                .store
                .get_workflow_root(id)
                .await
                .map_err(|_| error(StatusCode::CONFLICT, "Workflow source is unavailable"))?;
            let expected_root = tokio::fs::canonicalize(&scope.trusted_root)
                .await
                .map_err(|_| {
                    error(
                        StatusCode::CONFLICT,
                        "Workflow authoring root is unavailable",
                    )
                })?
                .join("skills")
                .join(id);
            if root != expected_root {
                return Err(error(
                    StatusCode::CONFLICT,
                    "Workflow is owned by a different discovery root",
                ));
            }
            Ok(())
        }
        (false, Some(_)) => Err(error(
            StatusCode::CONFLICT,
            "Only the selected User or Project authored definition can be edited",
        )),
        (false, None) => Err(error(
            StatusCode::NOT_FOUND,
            "Workflow definition not found",
        )),
    }
}

async fn response_for(
    scope: &CloneTargetScope,
    snapshot: DefinitionSnapshot,
    status: StatusCode,
    saved: bool,
) -> HttpResponse {
    let definition = match stored_definition(&snapshot) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let description = match verify_authored_skill(&definition.id, &snapshot.skill_bytes) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    // The filesystem commit already happened. A later catalog refresh problem
    // must not masquerade as an unsaved edit or encourage a blind write retry.
    let refreshed = scope.store.reload().await.is_ok();
    let catalog = combined_catalog_snapshot(&scope.store).await;
    let entry = unique_winner(&catalog, &definition.id)
        .ok()
        .flatten()
        .filter(|entry| {
            refreshed
                && entry.source == scope.published_source
                && entry.kind == WorkflowKind::Orchestration
                && entry.status == WorkflowStatus::Valid
                && entry.revision == definition.revision
        });
    // Same-revision external edits or missing nested revisions must not make
    // an unrelated publication look executable merely because its id matches.
    let pinned_matches = if entry.is_some() {
        scope
            .store
            .pin_workflow_definition_bundle(&definition.id, definition.revision)
            .await
            .is_ok_and(|bundle| bundle.root() == Some(&definition))
    } else {
        false
    };
    let entry = entry.filter(|_| pinned_matches);
    HttpResponse::build(status)
        .insert_header(("Cache-Control", "no-store"))
        .json(json!({
            "definition": definition,
            "description": description,
            "source": scope.published_source,
            "content_digest": snapshot.digest,
            "skill_digest": snapshot.skill_digest,
            "saved": saved,
            "catalog_available": entry.is_some(),
            "catalog_revision": entry.map(|_| catalog.revision),
            "catalog_content_digest": entry.map(|entry| entry.content_digest.as_str()),
            "invocation_policy": entry.map(|entry| &entry.invocation_policy)
        }))
}

pub async fn validate_workflow_definition(
    body: web::Json<ValidateDefinitionRequest>,
) -> HttpResponse {
    match validate_definition(&body.definition) {
        Ok(definition) => HttpResponse::Ok()
            .insert_header(("Cache-Control", "no-store"))
            .json(json!({"valid":true,"definition":definition,"execution_authorized":false})),
        Err(response) => *response,
    }
}

pub async fn create_workflow_definition(
    state: web::Data<AppState>,
    body: web::Json<CreateDefinitionRequest>,
) -> Result<HttpResponse, AppError> {
    let definition = match validate_definition(&body.definition) {
        Ok(value) => value,
        Err(response) => return Ok(*response),
    };
    if definition.revision != 1 {
        return Ok(*error(
            StatusCode::BAD_REQUEST,
            "New workflow definitions must start at revision 1",
        ));
    }
    let skill = match skill_bytes(&definition.id, &body.description) {
        Ok(value) => value,
        Err(response) => return Ok(*response),
    };
    let mut scope = match scope_for(&state, body.target, body.session_id.as_deref()).await {
        Ok(value) => value,
        Err(response) => return Ok(*response),
    };
    if let Err(response) = require_winner(&scope, &definition.id, true).await {
        return Ok(*response);
    }
    if let Err(response) = validate_nested_definitions(&scope, &definition).await {
        return Ok(*response);
    }
    let bytes = serde_json::to_vec_pretty(&definition)
        .map_err(|problem| AppError::InternalError(problem.into()))?;
    let root = scope.trusted_root.clone();
    let id = definition.id;
    let session_guard = scope.session_guard.take();
    let publication = tokio::task::spawn_blocking(move || {
        (
            definition_publication::create_definition(&root, &id, &skill, &bytes),
            session_guard,
        )
    })
    .await;
    match publication {
        Ok((Ok(snapshot), _session_guard)) => {
            Ok(response_for(&scope, snapshot, StatusCode::CREATED, true).await)
        }
        Ok((Err(problem), _session_guard)) => Ok(*publication_error(problem)),
        Err(problem) => {
            tracing::error!(%problem, "workflow definition task failed");
            Ok(*error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Workflow publication could not be confirmed; read before retrying",
            ))
        }
    }
}

pub async fn get_workflow_definition(
    state: web::Data<AppState>,
    id: web::Path<String>,
    query: web::Query<DefinitionQuery>,
) -> Result<HttpResponse, AppError> {
    let id = id.into_inner();
    if !is_safe_workflow_name(&id) || id.starts_with('.') || id.ends_with('.') {
        return Ok(*error(
            StatusCode::BAD_REQUEST,
            "Invalid workflow definition id",
        ));
    }
    let scope = match scope_for(&state, query.target, query.session_id.as_deref()).await {
        Ok(value) => value,
        Err(response) => return Ok(*response),
    };
    if let Err(response) = require_winner(&scope, &id, false).await {
        return Ok(*response);
    }
    let root = scope.trusted_root.clone();
    match tokio::task::spawn_blocking(move || definition_publication::read_definition(&root, &id))
        .await
    {
        Ok(Ok(snapshot)) => Ok(response_for(&scope, snapshot, StatusCode::OK, false).await),
        Ok(Err(problem)) => Ok(*publication_error(problem)),
        Err(problem) => {
            tracing::error!(%problem, "workflow definition read failed");
            Ok(*error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Workflow definition is unavailable",
            ))
        }
    }
}

pub async fn update_workflow_definition(
    state: web::Data<AppState>,
    id: web::Path<String>,
    body: web::Json<UpdateDefinitionRequest>,
) -> Result<HttpResponse, AppError> {
    let id = id.into_inner();
    let definition = match validate_definition(&body.definition) {
        Ok(value) => value,
        Err(response) => return Ok(*response),
    };
    if definition.id != id || body.expected_revision.checked_add(1) != Some(definition.revision) {
        return Ok(*error(
            StatusCode::BAD_REQUEST,
            "Update must keep the workflow id and increment its expected revision exactly once",
        ));
    }
    let mut scope = match scope_for(&state, body.target, body.session_id.as_deref()).await {
        Ok(value) => value,
        Err(response) => return Ok(*response),
    };
    if let Err(response) = require_winner(&scope, &id, false).await {
        return Ok(*response);
    }
    let root = scope.trusted_root.clone();
    let current_id = id.clone();
    let current = match tokio::task::spawn_blocking(move || {
        definition_publication::read_definition(&root, &current_id)
    })
    .await
    {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(problem)) => return Ok(*publication_error(problem)),
        Err(problem) => {
            tracing::error!(%problem, "workflow definition read failed");
            return Ok(*error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Workflow definition is unavailable",
            ));
        }
    };
    if body.expected_revision != current.revision
        || body.expected_digest != current.digest
        || body.expected_skill_digest != current.skill_digest
    {
        return Ok(*error(
            StatusCode::CONFLICT,
            "Workflow definition changed; reload before updating",
        ));
    }
    if let Err(response) = verify_authored_skill(&id, &current.skill_bytes) {
        return Ok(*response);
    }
    if let Err(response) = stored_definition(&current) {
        return Ok(*response);
    }
    if let Err(response) = validate_nested_definitions(&scope, &definition).await {
        return Ok(*response);
    }
    let bytes = serde_json::to_vec_pretty(&definition)
        .map_err(|problem| AppError::InternalError(problem.into()))?;
    let root = scope.trusted_root.clone();
    let expected_digest = current.digest;
    let expected_skill_digest = current.skill_digest;
    let revision = current.revision;
    let session_guard = scope.session_guard.take();
    match tokio::task::spawn_blocking(move || {
        let result = definition_publication::update_definition(
            &root,
            &id,
            &expected_digest,
            revision,
            &expected_skill_digest,
            &bytes,
        );
        (result, session_guard)
    })
    .await
    {
        Ok((Ok(snapshot), _session_guard)) => {
            Ok(response_for(&scope, snapshot, StatusCode::OK, true).await)
        }
        Ok((Err(problem), _session_guard)) => Ok(*publication_error(problem)),
        Err(problem) => {
            tracing::error!(%problem, "workflow definition update task failed");
            Ok(*error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Workflow publication could not be confirmed; read before retrying",
            ))
        }
    }
}

#[cfg(test)]
#[path = "definition_tests.rs"]
mod tests;
