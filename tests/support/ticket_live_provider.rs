//! Explicit, read-only live configuration and a transparent fixture bridge.
//! The owned Host sees a dummy credential; the selected credential stays here.
#![allow(dead_code)] // The proposal and Host targets use different subsets.
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use bamboo_engine::ticket_worker_plan::tickets::content_hash;
use bamboo_llm::OpenAIProvider;
use futures::StreamExt;
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Mutex, time::Duration};

pub const HOST_CREDENTIAL: &str = "isolated-ticket-live-fixture";

pub struct LiveConfig {
    pub model: String,
    pub responses_only_models: Vec<String>,
    endpoint: String,
    secret: String,
    protected: Vec<(PathBuf, Vec<u8>)>,
    supervisor_only: bool,
}

impl LiveConfig {
    pub fn read() -> Self {
        let root = PathBuf::from(
            std::env::var("BAMBOO_TICKET_MODEL_CONFIG_ROOT")
                .expect("explicit read-only provider root is required; no fixture fallback"),
        );
        let mut protected = Vec::new();
        for name in [
            "providers.json",
            "credentials.json",
            ".bamboo_encryption_key",
        ] {
            let path = root.join(name);
            if path.is_file() {
                protected.push((path.clone(), std::fs::read(path).unwrap()));
            }
        }
        let providers: Value =
            serde_json::from_slice(&std::fs::read(root.join("providers.json")).unwrap()).unwrap();
        let defaults = &providers["data"]["defaults"]["chat"];
        let id = defaults["provider"].as_str().expect("configured provider");
        let instance = &providers["data"]["provider_instances"][id];
        assert_eq!(instance["enabled"], true);
        assert_eq!(instance["provider_type"], "openai");
        let reference = instance["credential_ref"]
            .as_str()
            .expect("existing credential reference");
        let credentials: Value =
            serde_json::from_slice(&std::fs::read(root.join("credentials.json")).unwrap()).unwrap();
        let ciphertext = credentials["data"]["entries"][reference]["ciphertext"]
            .as_str()
            .expect("existing encrypted credential");
        if std::env::var_os("BAMBOO_CONFIG_ENCRYPTION_KEY").is_none() {
            let key = std::fs::read_to_string(root.join(".bamboo_encryption_key"))
                .expect("existing encryption key; generation is forbidden");
            assert!(key.trim().len() == 64, "invalid existing encryption key");
            std::env::set_var("BAMBOO_CONFIG_ENCRYPTION_KEY", key.trim());
        }
        Self {
            model: defaults["model"].as_str().expect("configured model").into(),
            responses_only_models: instance["responses_only_models"]
                .as_array()
                .map(|v| {
                    v.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default(),
            endpoint: instance["base_url"]
                .as_str()
                .expect("configured endpoint")
                .into(),
            secret: bamboo_config::encryption::decrypt(ciphertext)
                .expect("existing credential must decrypt"),
            protected,
            supervisor_only: true,
        }
    }

    pub fn provider(&self) -> OpenAIProvider {
        OpenAIProvider::new(self.secret.clone())
            .with_base_url(&self.endpoint)
            .with_responses_only_models(self.responses_only_models.clone())
    }

    pub fn verify_unchanged(&self) {
        for (path, before) in &self.protected {
            assert!(
                std::fs::read(path).is_ok_and(|after| after == *before),
                "selected read-only provider configuration changed"
            );
        }
    }

    pub fn synthetic_transport_test(endpoint: String) -> Self {
        Self {
            model: "transport-only".into(),
            responses_only_models: vec![],
            endpoint,
            secret: "upstream-transport-fixture".into(),
            protected: vec![],
            supervisor_only: false,
        }
    }
}

struct State {
    config: LiveConfig,
    client: reqwest::Client,
    requests: Mutex<Vec<Value>>,
}

async fn forward(request: HttpRequest, body: web::Bytes, state: web::Data<State>) -> HttpResponse {
    if request
        .headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        != Some(&format!("Bearer {HOST_CREDENTIAL}"))
    {
        return HttpResponse::Unauthorized().finish();
    }
    let suffix = match (request.method().as_str(), request.path()) {
        ("GET", "/v1/models") => "/models",
        ("POST", "/v1/chat/completions") => "/chat/completions",
        ("POST", "/v1/responses") => "/responses",
        _ => return HttpResponse::NotFound().finish(),
    };
    let model = if request.method() == actix_web::http::Method::POST {
        let Ok(value) = serde_json::from_slice::<Value>(&body) else {
            return HttpResponse::BadRequest().finish();
        };
        if value["model"] != state.config.model {
            return HttpResponse::BadRequest().body("selected model mismatch");
        }
        if state.config.supervisor_only {
            let tool_names: Vec<_> = value["tools"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|tool| {
                    tool["name"]
                        .as_str()
                        .or_else(|| tool["function"]["name"].as_str())
                })
                .collect();
            if !["work_overview", "work_update"]
                .iter()
                .all(|name| tool_names.contains(name))
            {
                return HttpResponse::Forbidden()
                    .body("fixture permits Supervisor tool requests only");
            }
        }
        value["model"].clone()
    } else {
        Value::Null
    };
    let index = {
        let mut requests = state.requests.lock().unwrap();
        if requests.len() >= 256 {
            return HttpResponse::TooManyRequests().body("bounded fixture request limit");
        }
        let index = requests.len();
        requests.push(json!({"path":request.path(),"model":model,
            "request_bytes":body.len(),"request_sha256":content_hash(&body),"status":null}));
        index
    };
    let method = reqwest::Method::from_bytes(request.method().as_str().as_bytes()).unwrap();
    let mut upstream = state
        .client
        .request(
            method,
            format!("{}{suffix}", state.config.endpoint.trim_end_matches('/')),
        )
        .bearer_auth(&state.config.secret);
    if !body.is_empty() {
        upstream = upstream
            .header("content-type", "application/json")
            .body(body.to_vec());
    }
    let response = match upstream.send().await {
        Ok(response) => response,
        Err(_) => {
            state.requests.lock().unwrap()[index]["error"] = json!("upstream transport failure");
            return HttpResponse::BadGateway().body("upstream transport failure; no fallback");
        }
    };
    let status = response.status().as_u16();
    state.requests.lock().unwrap()[index]["status"] = json!(status);
    if !response.status().is_success() {
        return HttpResponse::BadGateway().body(format!("upstream HTTP {status}; no fallback"));
    }
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_owned();
    HttpResponse::build(actix_web::http::StatusCode::from_u16(status).unwrap())
        .content_type(content_type)
        .streaming(response.bytes_stream().map(|chunk| {
            chunk.map_err(|_| actix_web::error::ErrorBadGateway("upstream stream failure"))
        }))
}

pub struct LiveBridge {
    pub base: String,
    pub model: String,
    pub responses_only_models: Vec<String>,
    state: web::Data<State>,
    handle: actix_web::dev::ServerHandle,
}

impl LiveBridge {
    pub async fn start(config: LiveConfig) -> Self {
        let model = config.model.clone();
        let responses_only_models = config.responses_only_models.clone();
        let state = web::Data::new(State {
            config,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .build()
                .unwrap(),
            requests: Mutex::new(vec![]),
        });
        let app_state = state.clone();
        let server = HttpServer::new(move || {
            App::new()
                .app_data(app_state.clone())
                .app_data(web::PayloadConfig::new(2 * 1024 * 1024))
                .default_service(web::route().to(forward))
        })
        .workers(1)
        .bind(("127.0.0.1", 0))
        .unwrap();
        let base = format!("http://{}/v1", server.addrs()[0]);
        let server = server.run();
        let handle = server.handle();
        actix_web::rt::spawn(server);
        Self {
            base,
            model,
            responses_only_models,
            state,
            handle,
        }
    }

    pub fn requests(&self) -> Vec<Value> {
        self.state.requests.lock().unwrap().clone()
    }

    pub fn verify_unchanged(&self) {
        self.state.config.verify_unchanged();
    }

    pub async fn finish(self) {
        self.verify_unchanged();
        self.handle.stop(true).await;
    }
}
