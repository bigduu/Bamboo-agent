//! Explicit opt-in live provider checks. No configuration migration/save and
//! no production records. Deterministic Runtime tests do not run this target.
use bamboo_llm::{provider::LLMProvider, OpenAIProvider};
use serde_json::Value;

fn configured_provider() -> (OpenAIProvider, String) {
    let root = std::path::PathBuf::from(std::env::var("BAMBOO_TICKET_MODEL_CONFIG_ROOT").expect("explicit read-only provider root"));
    let providers: Value = serde_json::from_slice(&std::fs::read(root.join("providers.json")).unwrap()).unwrap();
    let defaults = &providers["data"]["defaults"]["chat"];
    let id = defaults["provider"].as_str().expect("configured chat provider");
    let model = defaults["model"].as_str().unwrap().to_owned();
    let instance = &providers["data"]["provider_instances"][id];
    assert_eq!(instance["enabled"], true);
    assert_eq!(instance["provider_type"], "openai");
    let reference = instance["credential_ref"].as_str().expect("existing credential reference");
    let credentials: Value = serde_json::from_slice(&std::fs::read(root.join("credentials.json")).unwrap()).unwrap();
    let encrypted = credentials["data"]["entries"][reference]["ciphertext"].as_str().expect("existing encrypted credential");
    // Set only this isolated test process's key. No global key file is created
    // or modified and no ConfigFacade migration/credential write is invoked.
    if std::env::var_os("BAMBOO_CONFIG_ENCRYPTION_KEY").is_none() {
        let key = std::fs::read_to_string(root.join(".bamboo_encryption_key")).expect("existing key; no generation permitted");
        assert_eq!(key.trim().len(), 64);
        std::env::set_var("BAMBOO_CONFIG_ENCRYPTION_KEY", key.trim());
    }
    let secret = bamboo_config::encryption::decrypt(encrypted).expect("existing credential must decrypt");
    let patterns = instance["responses_only_models"].as_array().map(|v| v.iter().filter_map(Value::as_str).map(str::to_owned).collect()).unwrap_or_default();
    (OpenAIProvider::new(secret).with_base_url(instance["base_url"].as_str().unwrap()).with_responses_only_models(patterns), model)
}

#[tokio::test]
#[ignore = "uses explicitly selected existing live provider; run manually with read-only config root"]
async fn live_ticket_model_readiness() {
    let (provider, model) = configured_provider();
    let models = tokio::time::timeout(std::time::Duration::from_secs(30), provider.list_models()).await.expect("live bridge deadline").expect("authenticated model catalog");
    assert!(models.contains(&model), "configured model absent from authenticated catalog");
    eprintln!("LIVE_TICKET_MODEL_READY model={model}; existing credential used read-only; semantic evaluation not yet run");
}
