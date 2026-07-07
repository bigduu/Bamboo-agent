use actix_web::{web, HttpResponse, Result};
use uuid::Uuid;

use crate::app_state::AppState;
use bamboo_agent_core::Session;
use bamboo_engine::model_config_helper::normalize_gold_config_json;

use super::super::super::types::{CreateSessionRequest, CreateSessionResponse, SessionSummary};

/// `POST /api/v1/sessions`
pub async fn create_session(
    state: web::Data<AppState>,
    req: web::Json<CreateSessionRequest>,
) -> Result<HttpResponse> {
    let id = Uuid::new_v4().to_string();
    let global_default_prompt =
        bamboo_engine::prompt_defaults::read_global_default_system_prompt_template();
    let config_snapshot = state.config.read().await.clone();
    let gold_config_json = match req
        .gold_config
        .as_ref()
        .map(normalize_gold_config_json)
        .transpose()
    {
        Ok(value) => value,
        Err(error) => {
            return Ok(HttpResponse::BadRequest().json(serde_json::json!({
                "error": "Invalid gold_config",
                "message": error.to_string()
            })));
        }
    };

    let session = build_new_session(
        &id,
        &req,
        gold_config_json,
        global_default_prompt.as_str(),
        &config_snapshot,
    );

    state
        .storage
        .save_session(&session)
        .await
        .map_err(|error| {
            actix_web::error::ErrorInternalServerError(format!("Failed to save session: {error}"))
        })?;

    state.sessions.insert(
        id.clone(),
        std::sync::Arc::new(parking_lot::RwLock::new(session.clone())),
    );

    // Publish onto the account change feed so other clients insert the new
    // session into their list without polling `GET /sessions`.
    state.account_sink.record(
        Some(&id),
        &bamboo_agent_core::AgentEvent::SessionCreated {
            session_id: id.clone(),
            title: session.title.clone(),
            kind: session.kind,
            created_at: session.created_at,
        },
    );

    match state.session_store.get_index_entry(&id).await {
        // 201 Created — a new resource was created. Aligns `POST /api/v1/sessions`
        // with every other create endpoint (chat, mcp-add, prompt-presets,
        // provider-instances, cluster-nodes), which already return 201. #251
        // (finding 3).
        Some(entry) => Ok(HttpResponse::Created().json(CreateSessionResponse {
            session: SessionSummary::from_entry(entry, false),
        })),
        None => Ok(HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Session created but missing from index",
            "session_id": id
        }))),
    }
}

fn build_new_session(
    id: &str,
    req: &CreateSessionRequest,
    gold_config_json: Option<String>,
    global_default_prompt: &str,
    config: &bamboo_llm::Config,
) -> Session {
    use bamboo_engine::session_app::session_create::{
        build_new_session as crate_build, CreateSessionConfig, CreateSessionInput,
    };

    let input = CreateSessionInput {
        id: id.to_string(),
        title: req.title.clone(),
        system_prompt: req.system_prompt.clone(),
        model: req.model.clone(),
        model_ref: req.model_ref.clone(),
        reasoning_effort: req.reasoning_effort,
        gold_config_json,
    };
    let create_config = CreateSessionConfig {
        default_model: config.get_model(),
        default_reasoning_effort: config.get_reasoning_effort(),
        global_default_prompt: global_default_prompt.to_string(),
        builtin_fallback_prompt: crate::app_state::DEFAULT_BASE_PROMPT,
    };

    crate_build(&input, &create_config)
}

#[cfg(test)]
mod tests {
    use actix_web::{http::StatusCode, test, web, App};
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::routes::configure_routes;
    use crate::AppState;

    async fn new_state() -> web::Data<AppState> {
        let temp_dir = tempdir().expect("tempdir");
        bamboo_config::paths::init_bamboo_dir(temp_dir.path().to_path_buf());
        web::Data::new(
            AppState::new(temp_dir.path().to_path_buf())
                .await
                .expect("app state"),
        )
    }

    #[actix_web::test]
    async fn create_session_returns_201_created() {
        let state = new_state().await;
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .configure(configure_routes),
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/api/v1/sessions")
                .set_json(serde_json::json!({ "title": "New session" }))
                .to_request(),
        )
        .await;

        // 201 Created — aligns with the other create endpoints. #251 (finding 3).
        assert_eq!(resp.status(), StatusCode::CREATED);

        let body: Value = test::read_body_json(resp).await;
        assert!(
            body["session"]["id"].as_str().is_some(),
            "response should carry the created session summary"
        );
    }
}
