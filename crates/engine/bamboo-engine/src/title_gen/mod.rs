//! Asynchronous session naming from durable conversation evidence.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bamboo_agent_core::{Message, Session, TitleSource};
use serde::Serialize;
use tokio::{sync::oneshot, time::timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::app_context::AgentSessionContext;
use crate::session_app::metadata::{
    GeneratedTitleCommit, MetadataError, SessionMetadataService, TitleGenerationRevision,
};

mod fallback;
mod prompt;
#[cfg(test)]
mod tests;
mod tracker;

pub use fallback::heuristic_title;
pub use prompt::build_title_messages;
pub use tracker::TitleGenerationTracker;

use prompt::{ModelTitle, TitleContext};

const TITLE_GEN_TIMEOUT_SECS: u64 = 30;

/// Result of an explicit title request. Only an updated result writes metadata
/// and publishes `SessionTitleUpdated`.
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum TitleGenerationOutcome {
    Updated { title: String, title_version: u64 },
    InsufficientContext,
    Unavailable,
    Unchanged,
    Superseded,
    Busy,
}

enum Request {
    Automatic,
    Regenerate {
        observed: Option<TitleGenerationRevision>,
    },
}

impl Request {
    fn is_automatic(&self) -> bool {
        matches!(self, Self::Automatic)
    }
}

/// Evaluate newly durable conversation evidence for a still-pending title.
pub fn spawn_title_generation(state: Arc<dyn AgentSessionContext>, session_id: String) {
    spawn_inner(state, session_id, Request::Automatic, None);
}

/// Request a title even when a session already has a title.
pub fn spawn_title_generation_force(state: Arc<dyn AgentSessionContext>, session_id: String) {
    spawn_inner(
        state,
        session_id,
        Request::Regenerate { observed: None },
        None,
    );
}

/// Request a title against the revision the caller observed. Dropping the
/// receiver leaves the background job running.
pub fn spawn_title_regeneration(
    state: Arc<dyn AgentSessionContext>,
    observed: &Session,
) -> oneshot::Receiver<TitleGenerationOutcome> {
    let (sender, receiver) = oneshot::channel();
    spawn_inner(
        state,
        observed.id.clone(),
        Request::Regenerate {
            observed: Some(TitleGenerationRevision::capture(observed)),
        },
        Some(sender),
    );
    receiver
}

fn spawn_inner(
    state: Arc<dyn AgentSessionContext>,
    session_id: String,
    request: Request,
    result: Option<oneshot::Sender<TitleGenerationOutcome>>,
) {
    let acquired = state.title_generation_tracker().map_or_else(
        || state.title_gen_acquire(&session_id),
        |tracker| tracker.acquire(&session_id, request.is_automatic()),
    );
    if !acquired {
        if let Some(result) = result {
            let _ = result.send(TitleGenerationOutcome::Busy);
        }
        return;
    }

    let mut guard = TitleGenGuard {
        state,
        session_id,
        armed: true,
    };
    tokio::spawn(async move {
        let outcome = run_title_generation(&guard.state, &guard.session_id, request).await;
        let mut queued = guard.finish();
        if let Some(result) = result {
            let _ = result.send(outcome);
        }
        while queued {
            run_title_generation(&guard.state, &guard.session_id, Request::Automatic).await;
            queued = guard.finish();
        }
    });
}

struct TitleGenGuard {
    state: Arc<dyn AgentSessionContext>,
    session_id: String,
    armed: bool,
}

impl TitleGenGuard {
    fn finish(&mut self) -> bool {
        if let Some(tracker) = self.state.title_generation_tracker() {
            self.armed = tracker.finish(&self.session_id);
            return self.armed;
        }
        false
    }
}

impl Drop for TitleGenGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Some(tracker) = self.state.title_generation_tracker() {
                tracker.release(&self.session_id);
            } else {
                self.state.title_gen_release(&self.session_id);
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum TitleModelError {
    RouteUnavailable,
    Bootstrap,
    Request,
    Stream,
    ReasoningOnly,
    Timeout,
}

async fn run_title_generation(
    state: &Arc<dyn AgentSessionContext>,
    session_id: &str,
    request: Request,
) -> TitleGenerationOutcome {
    use TitleGenerationOutcome::{
        InsufficientContext, Superseded, Unavailable, Unchanged, Updated,
    };

    let session = match state.storage().load_session(session_id).await {
        Ok(Some(session)) => session,
        Ok(None) => return Superseded,
        Err(_) => {
            warn!(%session_id, "title-gen: session load failed");
            return Unavailable;
        }
    };
    match &request {
        Request::Automatic if session.title_generated => return Superseded,
        Request::Regenerate {
            observed: Some(revision),
        } if !revision.matches(&session) => {
            return Superseded;
        }
        _ => {}
    }

    let Some(context) = TitleContext::from_session(&session) else {
        debug!(%session_id, "title-gen: no eligible conversation evidence");
        return InsufficientContext;
    };
    let repeated_evidence = state
        .title_generation_tracker()
        .is_some_and(|tracker| !tracker.record_attempt(&session, context.fingerprint()));
    if repeated_evidence && request.is_automatic() {
        return Unchanged;
    }

    let started_at = Instant::now();
    let output = match try_llm_title(state, &session, context.messages()).await {
        Ok(output) => output,
        Err(reason) => {
            warn!(%session_id, ?reason, duration_ms = started_at.elapsed().as_millis(), "title-gen: model unavailable");
            return Unavailable;
        }
    };
    let title = match context.parse(&output) {
        ModelTitle::Title(title) => title,
        ModelTitle::InsufficientContext => {
            info!(%session_id, duration_ms = started_at.elapsed().as_millis(), "title-gen: model deferred");
            return InsufficientContext;
        }
        ModelTitle::Invalid(reason) => {
            warn!(%session_id, reason, duration_ms = started_at.elapsed().as_millis(), "title-gen: rejected model output");
            return Unavailable;
        }
    };

    match SessionMetadataService::commit_generated_title(
        state.as_ref(),
        session_id,
        &title,
        TitleSource::Auto,
        !request.is_automatic(),
        Some(&TitleGenerationRevision::capture(&session)),
    )
    .await
    {
        Ok(GeneratedTitleCommit::Updated(title, title_version)) => {
            info!(%session_id, title_version, duration_ms = started_at.elapsed().as_millis(), "title-gen: applied model title");
            Updated {
                title,
                title_version,
            }
        }
        Ok(GeneratedTitleCommit::Unchanged) => Unchanged,
        Ok(GeneratedTitleCommit::Superseded) | Err(MetadataError::NotFound(_)) => Superseded,
        Err(_) => {
            warn!(%session_id, "title-gen: title commit failed");
            Unavailable
        }
    }
}

/// Consume the fast-model response without exposing conversation excerpts or
/// upstream error content to the title diagnostic.
async fn try_llm_title(
    state: &Arc<dyn AgentSessionContext>,
    session: &Session,
    messages: Vec<Message>,
) -> Result<String, TitleModelError> {
    // Select the target and its handle while the same config is current.
    // Drop the guard before any provider request or stream await.
    let (provider_name, resolved, stream_timeout) = {
        let config = state.config().read().await;
        let provider_name = title_provider_name(&config, session);
        let resolved = crate::model_config_helper::try_resolve_fast_model(
            &config,
            &provider_name,
            state.provider_registry(),
        )
        .map_err(|_| TitleModelError::RouteUnavailable)?
        .ok_or(TitleModelError::RouteUnavailable)?;
        let actual_provider = config.defaults.as_ref().map_or(provider_name, |defaults| {
            defaults
                .fast
                .as_ref()
                .unwrap_or(&defaults.chat)
                .provider
                .clone()
        });
        (actual_provider, resolved, config.stream_timeout)
    };

    let model_name = resolved.model_name.clone();
    let provider = resolved.provider;
    debug!(provider = %provider_name, model = %model_name, "title-gen: selected model");
    let timeout_context = crate::runtime::stream::handler::StreamTimeoutContext::new(
        stream_timeout,
        Some(&provider_name),
        Some(&model_name),
    )
    .begin_request();

    let fut = async move {
        let options = bamboo_llm::provider::LLMRequestOptions {
            request_purpose: Some("title_generation".to_string()),
            ..Default::default()
        };
        let cancel_token = CancellationToken::new();
        let stream = crate::runtime::stream::handler::await_stream_bootstrap(
            provider.chat_stream_with_options(
                &messages,
                &[],
                Some(128),
                &model_name,
                Some(&options),
            ),
            &cancel_token,
            "title-gen",
            &timeout_context,
        )
        .await
        .map_err(|_| TitleModelError::Bootstrap)?
        .map_err(|_| TitleModelError::Request)?;
        let output = crate::runtime::stream::handler::consume_llm_stream_silent_with_context(
            stream,
            &cancel_token,
            "title-gen",
            &timeout_context,
        )
        .await
        .map_err(|_| TitleModelError::Stream)?;
        if output.content.trim().is_empty()
            && (!output.reasoning_content.trim().is_empty() || output.thinking_tokens > 0)
        {
            return Err(TitleModelError::ReasoningOnly);
        }
        Ok(output.content)
    };

    timeout(Duration::from_secs(TITLE_GEN_TIMEOUT_SECS), fut)
        .await
        .map_err(|_| TitleModelError::Timeout)?
}

fn title_provider_name(config: &bamboo_config::Config, session: &Session) -> String {
    if let Some(ref m) = session.model_ref {
        m.provider.clone()
    } else if let Some(p) = session.provider_name() {
        p
    } else {
        config.effective_default_provider().to_string()
    }
}
