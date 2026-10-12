use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bamboo_agent_core::tools::ToolSchema;
use bamboo_agent_core::{Message, Session};
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMStream};
use futures::stream;
use serde_json::json;

use super::AppState;

#[derive(Default)]
struct ModelLimitProbe {
    output_limits: Mutex<Vec<Option<u32>>>,
}

#[async_trait]
impl LLMProvider for ModelLimitProbe {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        _model: &str,
    ) -> Result<LLMStream, LLMError> {
        self.output_limits.lock().unwrap().push(max_output_tokens);
        Ok(Box::pin(stream::iter([
            Ok(LLMChunk::Token("finished".into())),
            Ok(LLMChunk::Done),
        ])))
    }
}

#[tokio::test]
async fn hosted_restart_loads_legacy_raw_model_limits_and_retains_them_until_repair() {
    let _key = bamboo_config::encryption::set_test_encryption_key([0x64; 32]);
    let dir = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let installed = bamboo_config::ConfigFacade::open_or_migrate(dir.path()).unwrap();
    let mut config = installed.effective_config();
    config.proxy_auth = Some(bamboo_config::ProxyAuth {
        username: "test-user".into(),
        password: "test-password".into(),
    });
    bamboo_config::persist_proxy_auth_credential_transaction_at_revision(
        dir.path(),
        &mut config,
        installed.registry().core.snapshot().revision,
    )
    .unwrap();
    drop(installed);
    let path = dir.path().join("model_limits.json");
    let write_limit = |context, output| {
        std::fs::write(
            &path,
            json!([{
                "model_pattern": "legacy-hosted-model",
                "max_context_tokens": context,
                "max_output_tokens": output
            }])
            .to_string(),
        )
        .unwrap();
    };
    write_limit(48_000, 4_000);

    // Use the real hosted constructor: it must inject the section reopened
    // from an already-installed modular layout into its assembled Agent.
    let state = AppState::new(dir.path().to_path_buf()).await.unwrap();
    let section = state
        .config_facade
        .as_ref()
        .unwrap()
        .registry()
        .model_limits
        .clone();
    assert_eq!(
        section.snapshot().status,
        bamboo_config::SectionStatus::Healthy
    );
    let probe = Arc::new(ModelLimitProbe::default());
    let project = state
        .project_store
        .create_with_project_path(
            "Legacy model limits",
            None,
            workspace.path().to_string_lossy().into_owned(),
            Vec::new(),
        )
        .unwrap();
    let mut contexts = Vec::new();

    for (run, invalid) in [None, Some("{invalid"), Some("[{}]"), None]
        .into_iter()
        .enumerate()
    {
        if let Some(invalid) = invalid {
            std::fs::write(&path, invalid).unwrap();
            section.reload();
            assert_ne!(
                section.snapshot().status,
                bamboo_config::SectionStatus::Healthy
            );
        } else if run > 0 {
            write_limit(96_000, 8_000);
            section.reload();
            assert_eq!(
                section.snapshot().status,
                bamboo_config::SectionStatus::Healthy
            );
        }
        let mut session = Session::new(format!("legacy-model-limit-{run}"), "legacy-hosted-model");
        session.set_project_id_meta(project.id.to_string());
        session.add_message(Message::user("finish this request"));
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(128);
        state
            .agent
            .execute_direct(
                &mut session,
                bamboo_engine::ExecuteRequestBuilder::new(
                    "",
                    event_tx,
                    tokio_util::sync::CancellationToken::new(),
                )
                .provider_override(probe.clone())
                .model("legacy-hosted-model")
                .build(),
            )
            .await
            .unwrap();
        contexts.push(session.effective_token_budget().unwrap().max_context_tokens);
    }

    assert_eq!(contexts, [48_000, 48_000, 48_000, 96_000]);
    assert_eq!(
        *probe.output_limits.lock().unwrap(),
        [Some(4_000), Some(4_000), Some(4_000), Some(8_000)]
    );
    state.shutdown().await;
}
