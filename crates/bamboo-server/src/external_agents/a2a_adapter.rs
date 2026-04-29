use std::time::Duration;

use async_trait::async_trait;
use bamboo_agent_core::{AgentError, AgentEvent, Role, TokenUsage};
use bamboo_engine::runtime::execution::{ExternalChildRunner, SpawnJob};
use bamboo_infrastructure::a2a::types::{
    A2ARole, CancelTaskRequest, GetTaskRequest, Message, Part, PartContentWire,
    SendMessageConfiguration, SendMessageRequest,
};
use bamboo_infrastructure::a2a::{
    validate_agent_card_for_jsonrpc_mvp, A2AAuth, A2AClient, A2AClientConfig, A2AJsonRpcClient,
};
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::config::ExternalAgentProfile;
use super::mapping::{A2AEventMapper, A2AMappedEvents};

/// A2A external child runner that delegates child session execution to a remote
/// A2A-compliant agent via JSON-RPC + SSE.
pub struct A2AExternalChildRunner {
    client: A2AJsonRpcClient,
    profile: ExternalAgentProfile,
}

impl A2AExternalChildRunner {
    pub fn new(client: A2AJsonRpcClient, profile: ExternalAgentProfile) -> Self {
        Self { client, profile }
    }

    /// Build an [`A2AClientConfig`] from an [`ExternalAgentProfile`] and auth token.
    pub fn build_client_config(
        profile: &ExternalAgentProfile,
        auth_token: Option<String>,
    ) -> bamboo_infrastructure::a2a::A2AClientResult<A2AClientConfig> {
        let auth = match auth_token {
            Some(token) => A2AAuth::Bearer(token),
            None => A2AAuth::None,
        };

        let agent_card_url = profile.agent_card_url.clone().ok_or_else(|| {
            bamboo_infrastructure::a2a::A2AClientError::InvalidAgentCard(format!(
                "Profile {} has no agent_card_url",
                profile.agent_id
            ))
        })?;

        Ok(A2AClientConfig {
            profile_id: profile.agent_id.clone(),
            agent_card_url,
            rpc_url_override: profile.rpc_url_override.clone(),
            auth,
            tenant: profile.tenant.clone(),
            request_timeout: Duration::from_secs(120),
            protocol_version: "1.0".to_string(),
            extensions: Vec::new(),
        })
    }

    /// Build a `SendMessageRequest` from the current child session state.
    fn build_send_message_request(
        &self,
        session: &bamboo_agent_core::Session,
    ) -> SendMessageRequest {
        let mut message = build_a2a_message(session);

        // Carry over context_id if we have one from a previous task in this session.
        message.context_id = session.metadata.get("a2a.context_id").cloned();

        // Build reference_task_ids from previous attempts.
        message.reference_task_ids = session
            .metadata
            .get("a2a.reference_task_ids")
            .and_then(|v| serde_json::from_str::<Vec<String>>(v).ok())
            .unwrap_or_default();

        let configuration = Some(SendMessageConfiguration {
            accepted_output_modes: Some(vec!["text/plain".to_string()]),
            history_length: Some(0),
            blocking: Some(false),
            extra: Default::default(),
        });

        let mut metadata = serde_json::json!({
            "bamboo_session_id": session.id,
            "bamboo_attempt": session.metadata.get("a2a.attempt").unwrap_or(&"1".to_string()),
        });

        if let Some(skill) = &self.profile.skill {
            metadata["skill"] = serde_json::json!(skill);
        }

        SendMessageRequest {
            tenant: self.profile.tenant.clone(),
            message,
            configuration,
            metadata: Some(metadata),
        }
    }
}

#[async_trait]
impl ExternalChildRunner for A2AExternalChildRunner {
    async fn should_handle(&self, session: &bamboo_agent_core::Session) -> bool {
        let kind = session.metadata.get("runtime.kind");
        let protocol = session.metadata.get("external.protocol");
        let agent_id = session.metadata.get("external.agent_id");

        kind == Some(&"external".to_string())
            && protocol == Some(&"a2a_jsonrpc".to_string())
            && agent_id == Some(&self.profile.agent_id)
    }

    async fn execute_external_child(
        &self,
        session: &mut bamboo_agent_core::Session,
        _job: &SpawnJob,
        event_tx: mpsc::Sender<AgentEvent>,
        cancel_token: CancellationToken,
    ) -> bamboo_engine::runtime::runner::Result<()> {
        // Increment attempt counter.
        let attempt: u64 = session
            .metadata
            .get("a2a.attempt")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
            + 1;
        session
            .metadata
            .insert("a2a.attempt".to_string(), attempt.to_string());

        // Validate agent card before sending. If non-streaming fallback is allowed,
        // do not require streaming capability; use the card to select the execution path.
        let card_validation = match self.client.fetch_agent_card().await {
            Ok(card) => match validate_agent_card_for_jsonrpc_mvp(
                &card,
                !self.profile.allow_non_streaming_fallback,
                self.profile.skill.as_deref(),
            ) {
                Ok(validation) => validation,
                Err(e) => {
                    return Err(AgentError::LLM(format!(
                        "A2A agent card validation failed for profile {}: {}",
                        self.profile.agent_id, e
                    )));
                }
            },
            Err(e) => {
                return Err(AgentError::LLM(format!(
                    "A2A agent card fetch failed for profile {}: {}",
                    self.profile.agent_id, e
                )));
            }
        };

        let request = self.build_send_message_request(session);

        let fallback_to_non_streaming = self.profile.allow_non_streaming_fallback;
        if !card_validation.streaming_supported {
            if fallback_to_non_streaming {
                tracing::info!(
                    "A2A profile {} does not advertise streaming; using non-streaming fallback.",
                    self.profile.agent_id
                );
                return handle_non_streaming(
                    &self.client,
                    request,
                    event_tx,
                    session,
                    self.profile.tenant.clone(),
                )
                .await;
            }

            return Err(AgentError::LLM(format!(
                "A2A profile {} does not support streaming and non-streaming fallback is disabled",
                self.profile.agent_id
            )));
        }

        // Try streaming first.
        let stream_result = self.client.send_streaming_message(request.clone()).await;

        let stream = match stream_result {
            Ok(stream) => stream,
            Err(e) => {
                if fallback_to_non_streaming {
                    tracing::warn!(
                        "A2A streaming failed for profile {}: {}. Falling back to non-streaming.",
                        self.profile.agent_id,
                        e
                    );
                    return handle_non_streaming(
                        &self.client,
                        request,
                        event_tx,
                        session,
                        self.profile.tenant.clone(),
                    )
                    .await;
                }
                return Err(AgentError::LLM(format!(
                    "A2A streaming failed and fallback disabled: {}",
                    e
                )));
            }
        };

        handle_streaming(
            &self.client,
            stream,
            event_tx,
            cancel_token,
            session,
            self.profile.tenant.clone(),
        )
        .await
    }
}

/// Consume a non-streaming A2A response and map to AgentEvents.
async fn handle_non_streaming(
    client: &A2AJsonRpcClient,
    request: SendMessageRequest,
    event_tx: mpsc::Sender<AgentEvent>,
    session: &mut bamboo_agent_core::Session,
    tenant: Option<String>,
) -> bamboo_engine::runtime::runner::Result<()> {
    let response = client
        .send_message(request)
        .await
        .map_err(|e| AgentError::LLM(format!("A2A send_message failed: {}", e)))?;

    let mut mapper = A2AEventMapper::new();

    // Build a synthetic StreamResponse from the non-streaming result.
    let synthetic = bamboo_infrastructure::a2a::types::StreamResponse {
        task: response.task.clone(),
        message: response.message.clone(),
        status_update: None,
        artifact_update: None,
    };

    let mapped = mapper.map_stream_response(synthetic);
    apply_mapped_events(&event_tx, session, mapped).await?;

    // If task is present, emit its status as well.
    if let Some(task) = &response.task {
        let status_update = bamboo_infrastructure::a2a::types::StreamResponse {
            task: None,
            message: None,
            status_update: Some(bamboo_infrastructure::a2a::types::TaskStatusUpdateEvent {
                task_id: task.id.clone(),
                context_id: task.context_id.clone().unwrap_or_default(),
                status: task.status.clone(),
                metadata: None,
            }),
            artifact_update: None,
        };
        let mapped = mapper.map_stream_response(status_update);
        apply_mapped_events(&event_tx, session, mapped).await?;
    }

    if mapper.is_terminal() {
        append_reference_task_id(session, &mapper);
        add_final_assistant_message(session, &mapper);
        return Ok(());
    }

    // Non-terminal: if there's a message but no task, it's a simple text response.
    if response.message.is_some() && response.task.is_none() {
        let _ = event_tx
            .send(AgentEvent::Complete {
                usage: TokenUsage::default(),
            })
            .await;
        append_reference_task_id(session, &mapper);
        add_final_assistant_message(session, &mapper);
        return Ok(());
    }

    // Non-terminal with a task: attempt recovery via GetTask.
    if let Some(task) = &response.task {
        match recover_task_state(client, &task.id, tenant.clone()).await {
            Ok(recovered) => {
                let mapped = mapper.map_stream_response(recovered);
                apply_mapped_events(&event_tx, session, mapped).await?;
                if mapper.is_terminal() {
                    append_reference_task_id(session, &mapper);
                    add_final_assistant_message(session, &mapper);
                    return Ok(());
                }
            }
            Err(e) => {
                tracing::warn!("A2A GetTask recovery after non-streaming failed: {}", e);
            }
        }
    }

    let msg = "A2A non-streaming response did not reach terminal state".to_string();
    let _ = event_tx
        .send(AgentEvent::Error {
            message: msg.clone(),
        })
        .await;
    append_reference_task_id(session, &mapper);
    add_final_assistant_message(session, &mapper);
    Err(AgentError::LLM(msg))
}

/// Consume an SSE stream, map to AgentEvents, handle cancellation and recovery.
async fn handle_streaming(
    client: &A2AJsonRpcClient,
    mut stream: bamboo_infrastructure::a2a::A2AStream,
    event_tx: mpsc::Sender<AgentEvent>,
    cancel_token: CancellationToken,
    session: &mut bamboo_agent_core::Session,
    tenant: Option<String>,
) -> bamboo_engine::runtime::runner::Result<()> {
    let mut mapper = A2AEventMapper::new();

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                if let Some(task_id) = mapper.latest_task_id() {
                    let _ = client.cancel_task(CancelTaskRequest {
                        tenant: tenant.clone(),
                        id: task_id.to_string(),
                        metadata: Some(serde_json::json!({"cancelledBy": "bamboo"})),
                    }).await;
                }
                return Err(AgentError::Cancelled);
            }
            item = stream.next() => {
                match item {
                    None => {
                        // Stream closed.
                        break;
                    }
                    Some(Err(e)) => {
                        tracing::warn!("A2A stream error: {}", e);
                        if !mapper.is_terminal() {
                            // Try recovery via GetTask.
                            if let Some(task_id) = mapper.latest_task_id() {
                                match recover_task_state(client, task_id, tenant.clone()).await {
                                    Ok(recovered) => {
                                        let mapped = mapper.map_stream_response(recovered);
                                        let _ = apply_mapped_events(&event_tx, session, mapped).await;
                                    }
                                    Err(recovery_err) => {
                                        tracing::warn!("A2A GetTask recovery failed: {}", recovery_err);
                                    }
                                }
                            }
                        }
                        return Err(AgentError::LLM(format!("A2A stream error: {}", e)));
                    }
                    Some(Ok(response)) => {
                        let mapped = mapper.map_stream_response(response);
                        apply_mapped_events(&event_tx, session, mapped).await?;

                        if mapper.is_terminal() {
                            append_reference_task_id(session, &mapper);
                            add_final_assistant_message(session, &mapper);
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    // Stream ended without terminal state — attempt recovery.
    if !mapper.is_terminal() {
        if let Some(task_id) = mapper.latest_task_id() {
            match recover_task_state(client, task_id, tenant.clone()).await {
                Ok(recovered) => {
                    let mapped = mapper.map_stream_response(recovered);
                    apply_mapped_events(&event_tx, session, mapped).await?;
                }
                Err(e) => {
                    tracing::warn!("A2A GetTask recovery after stream close failed: {}", e);
                }
            }
        }

        if !mapper.is_terminal() {
            let msg = "A2A stream closed without terminal state".to_string();
            let _ = event_tx
                .send(AgentEvent::Error {
                    message: msg.clone(),
                })
                .await;
            append_reference_task_id(session, &mapper);
            add_final_assistant_message(session, &mapper);
            return Err(AgentError::LLM(msg));
        }
    }

    append_reference_task_id(session, &mapper);
    add_final_assistant_message(session, &mapper);

    Ok(())
}

/// Call `GetTask` to recover the final state of a task after stream disruption.
async fn recover_task_state(
    client: &A2AJsonRpcClient,
    task_id: &str,
    tenant: Option<String>,
) -> bamboo_infrastructure::a2a::A2AClientResult<bamboo_infrastructure::a2a::types::StreamResponse>
{
    let task = client
        .get_task(GetTaskRequest {
            tenant,
            id: task_id.to_string(),
            history_length: Some(0),
        })
        .await?;

    Ok(bamboo_infrastructure::a2a::types::StreamResponse {
        task: Some(task),
        message: None,
        status_update: None,
        artifact_update: None,
    })
}

/// Send mapped events through the event channel and apply metadata updates.
async fn apply_mapped_events(
    event_tx: &mpsc::Sender<AgentEvent>,
    session: &mut bamboo_agent_core::Session,
    mapped: A2AMappedEvents,
) -> bamboo_engine::runtime::runner::Result<()> {
    for (k, v) in mapped.metadata_updates {
        session.metadata.insert(k, v);
    }

    for event in mapped.events {
        event_tx
            .send(event)
            .await
            .map_err(|_| AgentError::Tool("event channel closed".to_string()))?;
    }
    Ok(())
}

/// Append latest task_id to reference_task_ids for future retries.
fn append_reference_task_id(session: &mut bamboo_agent_core::Session, mapper: &A2AEventMapper) {
    if let Some(task_id) = mapper.latest_task_id() {
        let mut refs: Vec<String> = session
            .metadata
            .get("a2a.reference_task_ids")
            .and_then(|v| serde_json::from_str(v).ok())
            .unwrap_or_default();
        if !refs.contains(&task_id.to_string()) {
            refs.push(task_id.to_string());
            session.metadata.insert(
                "a2a.reference_task_ids".to_string(),
                serde_json::to_string(&refs).unwrap_or_default(),
            );
        }
    }
}

/// Append an assistant message with the final accumulated text.
fn add_final_assistant_message(session: &mut bamboo_agent_core::Session, mapper: &A2AEventMapper) {
    let text = mapper.final_text();
    if text.is_empty() {
        return;
    }
    session.messages.push(bamboo_agent_core::Message {
        id: uuid::Uuid::new_v4().to_string(),
        role: Role::Assistant,
        content: text.to_string(),
        reasoning: None,
        content_parts: None,
        image_ocr: None,
        phase: None,
        tool_calls: None,
        tool_call_id: None,
        tool_success: None,
        compressed: false,
        compressed_by_event_id: None,
        never_compress: false,
        compression_level: 0,
        created_at: chrono::Utc::now(),
        metadata: None,
    });
}

/// Convert Bamboo session messages into a single A2A `Message`.
///
/// For MVP we send the latest user message as the primary message.  If the
/// session has no user message we synthesise one from the session title.
fn build_a2a_message(session: &bamboo_agent_core::Session) -> Message {
    let content = session
        .messages
        .iter()
        .rev()
        .find(|m| matches!(m.role, Role::User))
        .map(|m| m.content.clone())
        .unwrap_or_else(|| {
            session
                .metadata
                .get("title")
                .cloned()
                .unwrap_or_else(|| "Execute task".to_string())
        });

    Message {
        message_id: uuid::Uuid::new_v4().to_string(),
        context_id: session.metadata.get("a2a.context_id").cloned(),
        task_id: None,
        role: A2ARole::User,
        parts: vec![Part {
            content: PartContentWire::Text { text: content },
            metadata: None,
            filename: None,
            media_type: Some("text/plain".to_string()),
        }],
        metadata: None,
        extensions: Vec::new(),
        reference_task_ids: Vec::new(),
    }
}
