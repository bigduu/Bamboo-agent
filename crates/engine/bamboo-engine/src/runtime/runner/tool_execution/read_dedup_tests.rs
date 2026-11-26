use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bamboo_agent_core::tools::{FunctionCall, ToolCall, ToolSchema};
use bamboo_agent_core::{Message, Role, Session};
use bamboo_domain::Storage;
use bamboo_llm::{LLMChunk, LLMError, LLMProvider, LLMRequestOptions, LLMStream, PromptIR};
use bamboo_storage::{LockedSessionStore, SessionStoreV2};
use bamboo_tools::tools::read::{ReadResultIdentity, READ_RESULT_IDENTITY_KEY};
use bamboo_tools::BuiltinToolExecutorBuilder;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::runtime::config::{AgentLoopConfig, PromptMemoryFlags};

struct ReadProvider {
    path: PathBuf,
    requests: Mutex<Vec<(Vec<Message>, Value)>>,
}

#[async_trait]
impl LLMProvider for ReadProvider {
    async fn chat_stream_ir(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        max_output_tokens: Option<u32>,
        model: &str,
        _: Option<&LLMRequestOptions>,
    ) -> Result<LLMStream, LLMError> {
        let messages = ir.body_chat();
        let wire = bamboo_llm::providers::anthropic::build_anthropic_request_with_cache_blocks(
            &messages,
            &ir.system_blocks,
            tools,
            model,
            max_output_tokens.unwrap_or(1024),
            true,
            None,
            None,
            Some(&ir.cache),
            false,
        );
        let round = {
            let mut requests = self.requests.lock().unwrap();
            requests.push((messages, wire));
            requests.len()
        };
        let read = |id: &str, offset: usize| ToolCall {
            id: id.into(),
            tool_type: "function".into(),
            function: FunctionCall {
                name: "Read".into(),
                arguments: json!({"file_path": self.path, "offset": offset, "limit": 12})
                    .to_string(),
            },
        };
        let output = match round {
            1 => LLMChunk::ToolCalls(vec![read("read-earlier", 0)]),
            2 => LLMChunk::ToolCalls(vec![read("read-latest", 0), read("read-other-range", 12)]),
            3 => LLMChunk::Token("Read results received.".into()),
            _ => panic!("unexpected model request {round}"),
        };
        Ok(Box::pin(futures::stream::iter([
            Ok(output),
            Ok(LLMChunk::Done),
        ])))
    }

    async fn chat_stream(
        &self,
        _: &[Message],
        _: &[ToolSchema],
        _: Option<u32>,
        _: &str,
    ) -> Result<LLMStream, LLMError> {
        panic!("expected canonical PromptIR dispatch")
    }
}

fn read_result<'a>(messages: &'a [Message], call_id: &str) -> &'a Message {
    messages
        .iter()
        .find(|message| {
            message.role == Role::Tool && message.tool_call_id.as_deref() == Some(call_id)
        })
        .unwrap()
}

#[tokio::test]
async fn outgoing_model_request_deduplicates_real_reads_and_preserves_saved_transcript() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("observations.txt");
    let input = (1..=24)
        .map(|number| format!("Measurement {number} records a different observation.\n"))
        .collect::<String>();
    std::fs::write(&path, input).unwrap();
    let storage: Arc<dyn Storage> = Arc::new(
        SessionStoreV2::new(home.path().to_path_buf())
            .await
            .unwrap(),
    );
    let mut session = Session::new("read-prompt-projection", "model");
    storage.save_session(&session).await.unwrap();
    let config = AgentLoopConfig {
        model_name: Some("model".into()),
        system_prompt: Some("Use the file observations to finish the task.".into()),
        storage: Some(storage.clone()),
        persistence: Some(Arc::new(LockedSessionStore::new(storage))),
        prompt_memory_flags: PromptMemoryFlags {
            project_prompt_injection: false,
            relevant_recall: false,
            relevant_recall_rerank: false,
            project_first_dream: false,
            ledger_agenda: false,
        },
        ..Default::default()
    };
    let provider = Arc::new(ReadProvider {
        path,
        requests: Mutex::new(Vec::new()),
    });
    let tools = Arc::new(
        BuiltinToolExecutorBuilder::new()
            .with_filesystem_tool("Read")
            .unwrap()
            .build(),
    );
    let (events, _receiver) = mpsc::channel(256);
    crate::runtime::runner::run_agent_loop_with_config(
        &mut session,
        "Read the measurements and finish.".into(),
        events,
        provider.clone(),
        tools,
        CancellationToken::new(),
        config,
    )
    .await
    .unwrap();

    let reopened = SessionStoreV2::new(home.path().to_path_buf())
        .await
        .unwrap();
    let saved = reopened.load_session(&session.id).await.unwrap().unwrap();
    let earlier = read_result(&saved.messages, "read-earlier");
    let latest = read_result(&saved.messages, "read-latest");
    let other_range = read_result(&saved.messages, "read-other-range");
    assert_eq!(earlier.content, latest.content);
    assert_ne!(latest.content, other_range.content);
    for result in [earlier, latest, other_range] {
        let identity: ReadResultIdentity = serde_json::from_value(
            result.metadata.as_ref().unwrap()[READ_RESULT_IDENTITY_KEY].clone(),
        )
        .unwrap();
        assert!(identity.matches_full_result(&result.content));
    }

    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        read_result(&requests[1].0, "read-earlier").content,
        earlier.content
    );
    let (outgoing, wire) = &requests[2];
    let projected = read_result(outgoing, "read-earlier");
    assert_eq!(projected.id, earlier.id);
    assert!(projected.content.contains("read-latest"));
    assert!(projected.content.len() < earlier.content.len());
    assert_eq!(read_result(outgoing, "read-latest").content, latest.content);
    assert_eq!(
        read_result(outgoing, "read-other-range").content,
        other_range.content
    );

    let blocks = wire["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap())
        .collect::<Vec<_>>();
    let pair_ids = |kind: &str, field: &str| {
        blocks
            .iter()
            .filter(|block| block["type"] == kind)
            .map(|block| block[field].as_str().unwrap())
            .collect::<BTreeSet<_>>()
    };
    let expected = BTreeSet::from(["read-earlier", "read-latest", "read-other-range"]);
    assert_eq!(pair_ids("tool_use", "id"), expected);
    assert_eq!(pair_ids("tool_result", "tool_use_id"), expected);
    for block in blocks.iter().filter(|block| block["type"] == "tool_result") {
        let call_id = block["tool_use_id"].as_str().unwrap();
        assert_eq!(block["content"], read_result(outgoing, call_id).content);
    }
}
