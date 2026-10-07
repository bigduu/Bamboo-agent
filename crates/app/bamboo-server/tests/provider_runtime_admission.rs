//! Configuration, HTTP catalog, routing and cold reload share one model admission authority.
//! All provider traffic, credentials and memory belong to these loopback fixtures.

use std::collections::BTreeSet;

use actix_web::http::{Method, StatusCode};
use actix_web::{test, web, App};
use bamboo_domain::{Message, ProviderModelRef};
use bamboo_llm::LLMChunk;
use bamboo_server::{configure_routes, AppState};
use futures::StreamExt;
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const INSTANCES: &str = "/api/v1/bamboo/settings/provider-instances";
const CATALOG: &str = "/api/v1/bamboo/provider-catalog";
const CUSTOM: &str = "custom/relay-alias";

async fn request(
    state: &web::Data<AppState>,
    method: Method,
    uri: &str,
    payload: Option<Value>,
) -> (StatusCode, Value) {
    let app = test::init_service(
        App::new()
            .app_data(state.clone())
            .configure(configure_routes),
    )
    .await;
    let mut request = test::TestRequest::default().method(method).uri(uri);
    if let Some(payload) = payload {
        request = request.set_json(payload);
    }
    let response = test::call_service(&app, request.to_request()).await;
    let status = response.status();
    (status, test::read_body_json(response).await)
}

fn model_ids(catalog: &Value) -> BTreeSet<String> {
    catalog["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|model| model["reference"]["model"].as_str().unwrap().to_string())
        .collect()
}

async fn state_from_fixture(data_dir: &std::path::Path) -> web::Data<AppState> {
    web::Data::new(
        AppState::new_with_memory_store(
            data_dir.to_path_buf(),
            bamboo_memory::memory_store::MemoryStore::new(data_dir.join("jiandu")),
        )
        .await
        .expect("fixture app state"),
    )
}

#[actix_web::test]
async fn discovery_selection_custom_routing_and_reload_share_exact_admission() {
    let data = tempfile::tempdir().unwrap();
    bamboo_config::paths::init_bamboo_dir(data.path().to_path_buf());
    let upstream = MockServer::start().await;
    let candidates: Vec<Value> = (0..1000)
        .map(|n| json!({"id": format!("relay-{n}"), "object":"model"}))
        .collect();
    Mock::given(method("GET"))
        .and(path("/v1/models"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": candidates})))
        .mount(&upstream)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            concat!(
                "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"custom model response\"}}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n"
            ),
            "text/event-stream",
        ))
        .mount(&upstream)
        .await;
    let config: bamboo_config::Config = serde_json::from_value(json!({
        "headless_auth": true,
        "features": {"provider_model_ref": true},
        "default_provider_instance":"relay",
        "defaults":{"chat":{"provider":"relay","model":"chat"}},
        "provider_instances":{"relay":{
            "provider_type":"openai", "enabled":true, "label":"Fixture relay", "model":"chat",
            "runtime_models":["chat"], "base_url":format!("{}/v1", upstream.uri()),
            "api_key":"fixture-key-not-a-real-credential"
        }}
    }))
    .unwrap();
    let state = state_from_fixture(data.path()).await;
    state
        .update_config_with_provider_credentials(
            move |candidate| {
                candidate.provider_instances = config.provider_instances.clone();
                candidate.default_provider_instance = config.default_provider_instance.clone();
                candidate.defaults = config.defaults.clone();
                candidate.features = config.features.clone();
                Ok(())
            },
            BTreeSet::new(),
            BTreeSet::from(["relay".to_string()]),
            bamboo_server::app_state::ConfigUpdateEffects {
                reload_provider: bamboo_config::patch::ReloadMode::Strict,
                reconcile_mcp: bamboo_config::patch::ReloadMode::None,
            },
        )
        .await
        .expect("seed via the production credential transaction");

    let (status, before) = request(&state, Method::GET, CATALOG, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(model_ids(&before), BTreeSet::from(["chat".into()]));
    let (status, discovered) = request(
        &state,
        Method::POST,
        &format!("{CATALOG}/fetch-models"),
        Some(json!({"provider":"relay"})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        discovered["fetched"][0]["models"].as_array().unwrap().len(),
        1000
    );
    let (_, after_discovery) = request(&state, Method::GET, CATALOG, None).await;
    assert_eq!(model_ids(&after_discovery), model_ids(&before));

    let (status, saved) = request(
        &state,
        Method::PUT,
        &format!("{INSTANCES}/relay"),
        Some(json!({"config":{"runtime_models":["chat", "relay-7", CUSTOM]}})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{saved}");
    let expected = BTreeSet::from(["chat".into(), "relay-7".into(), CUSTOM.into()]);
    let (_, selected) = request(&state, Method::GET, CATALOG, None).await;
    assert_eq!(model_ids(&selected), expected);
    let provider = state
        .get_provider_for_model_ref(&ProviderModelRef::new("relay", CUSTOM))
        .unwrap();
    let mut stream = provider
        .chat_stream(&[Message::user("fixture")], &[], None, CUSTOM)
        .await
        .unwrap();
    let mut response = String::new();
    while let Some(chunk) = stream.next().await {
        if let LLMChunk::Token(token) = chunk.unwrap() {
            response.push_str(&token);
        }
    }
    assert_eq!(response, "custom model response");
    assert!(state
        .get_provider_for_model_ref(&ProviderModelRef::new("relay", "relay-8"))
        .is_err());
    let chat_requests = upstream
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|request| request.method.as_str() == "POST")
        .collect::<Vec<_>>();
    assert_eq!(chat_requests.len(), 1);
    assert_eq!(
        serde_json::from_slice::<Value>(&chat_requests[0].body).unwrap()["model"],
        CUSTOM
    );

    let (status, rejected) = request(
        &state,
        Method::PUT,
        &format!("{INSTANCES}/relay"),
        Some(json!({"config":{"runtime_models":[]}})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
    let (_, still_selected) = request(&state, Method::GET, CATALOG, None).await;
    assert_eq!(model_ids(&still_selected), expected);
    state.shutdown().await;
    drop(state);
    let restored = state_from_fixture(data.path()).await;
    let (_, restored_catalog) = request(&restored, Method::GET, CATALOG, None).await;
    assert_eq!(model_ids(&restored_catalog), expected);
    let (_, instances) = request(&restored, Method::GET, INSTANCES, None).await;
    assert_eq!(
        instances["instances"][0]["config"]["runtime_models"],
        json!(["chat", "relay-7", CUSTOM])
    );
    assert!(restored
        .get_provider_for_model_ref(&ProviderModelRef::new("relay", "relay-8"))
        .is_err());
    restored.shutdown().await;
}
