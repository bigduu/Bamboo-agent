//! Agent loop runner implementation.
//!
//! This module provides the core agent execution loop that orchestrates LLM interactions,
//! tool execution, and event streaming for conversational AI agents.

use std::sync::Arc;

use base64::Engine;
use chrono::Utc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::core::agent::events::{TokenBudgetUsage, TokenUsage};
use crate::agent::core::budget::{
    prepare_hybrid_context, HeuristicTokenCounter, ModelLimitsRegistry, TokenBudget,
};
use crate::agent::core::storage::AttachmentReader;
use crate::agent::core::tools::{
    handle_tool_result_with_agentic_support, parse_tool_args, ToolExecutor, ToolHandlingOutcome,
    ToolSchema,
};
use crate::agent::core::{
    AgentError, AgentEvent, ExternalMemory, Message, Session, TodoItemStatus,
};
#[cfg(windows)]
use crate::agent::core::{ImageOcrLine, ImageOcrResult};
use crate::agent::llm::models::ContentPart;
use crate::agent::llm::LLMProvider;
use crate::agent::metrics::{
    MetricsCollector, RoundStatus as MetricsRoundStatus, SessionStatus as MetricsSessionStatus,
    TokenUsage as MetricsTokenUsage,
};
use crate::agent::tools::guide::{context::GuideBuildContext, EnhancedPromptBuilder};
use crate::agent::tools::CreateTodoListTool;

use crate::agent::loop_module::config::AgentLoopConfig;
use crate::agent::loop_module::stream::handler::consume_llm_stream;
use crate::agent::loop_module::todo_context::TodoLoopContext;

fn summarize_image_url(url: &str) -> String {
    let trimmed = url.trim();
    if trimmed.starts_with("data:") {
        // data:<mime>;base64,<data...>
        // Keep summary stable and avoid ever echoing base64 content.
        let mut mime = "unknown".to_string();
        if let Some(semi_idx) = trimmed.find(';') {
            let header = &trimmed["data:".len()..semi_idx];
            if !header.trim().is_empty() {
                mime = header.trim().to_string();
            }
        }

        let approx_bytes = trimmed
            .split_once(',')
            .map(|(_, data)| {
                let len = data.trim().len();
                // Base64 is ~4/3 expansion.
                (len.saturating_mul(3)) / 4
            })
            .unwrap_or(0);

        return format!("{mime} (~{approx_bytes} bytes)");
    }

    // For normal URLs, truncate to keep logs/responses compact.
    const MAX: usize = 120;
    if trimmed.len() <= MAX {
        trimmed.to_string()
    } else {
        format!("{}...", &trimmed[..MAX])
    }
}

fn rewrite_parts_to_placeholder(parts: &[ContentPart]) -> String {
    let mut out = String::new();
    for part in parts.iter() {
        match part {
            ContentPart::Text { text } => out.push_str(text),
            ContentPart::ImageUrl { image_url } => {
                let summary = summarize_image_url(&image_url.url);
                out.push_str("\n[Image omitted: ");
                out.push_str(&summary);
                out.push_str("]\n");
            }
        }
    }
    out
}

#[cfg(any(test, windows))]
fn persistable_image_urls(parts: &[ContentPart]) -> Vec<String> {
    parts
        .iter()
        .filter_map(|p| match p {
            ContentPart::ImageUrl { image_url } => {
                // Never persist `data:` URLs into session JSON (they can embed base64).
                let trimmed = image_url.url.trim();
                if trimmed.starts_with("data:") || trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }
            _ => None,
        })
        .collect()
}

async fn apply_image_fallback_to_llm_messages(
    messages: &mut [Message],
    fallback: crate::agent::loop_module::config::ImageFallbackConfig,
    attachment_reader: Option<&dyn AttachmentReader>,
) -> Result<()> {
    use crate::agent::loop_module::config::ImageFallbackMode;
    #[cfg(not(windows))]
    let _ = attachment_reader;

    let mode = fallback.mode;
    for msg in messages.iter_mut() {
        let Some(parts) = msg.content_parts.as_ref() else {
            continue;
        };

        let has_images = parts
            .iter()
            .any(|p| matches!(p, crate::agent::llm::models::ContentPart::ImageUrl { .. }));
        if !has_images {
            continue;
        }

        match mode {
            ImageFallbackMode::Error => {
                return Err(AgentError::LLM(
                    "This server does not currently support image inputs. Configure hooks.image_fallback.mode='placeholder' or 'ocr' to degrade gracefully.".to_string(),
                ));
            }
            ImageFallbackMode::Placeholder => {
                msg.content = rewrite_parts_to_placeholder(parts);
                msg.content_parts = None;
            }
            ImageFallbackMode::Ocr => {
                #[cfg(windows)]
                {
                    let rewritten = rewrite_parts_to_ocr_text(
                        attachment_reader,
                        parts,
                        msg.image_ocr.as_deref(),
                    )
                    .await
                    .map_err(AgentError::LLM)?;
                    msg.content = rewritten;
                    msg.content_parts = None;
                }
                #[cfg(not(windows))]
                {
                    log::info!(
                        "OCR image fallback requested but OCR is currently Windows-only; leaving images intact."
                    );
                }
            }
        }
    }

    Ok(())
}

#[cfg(windows)]
async fn ensure_session_image_ocr_cached(
    session: &mut Session,
    attachment_reader: Option<&dyn AttachmentReader>,
) -> bool {
    let Some(reader) = attachment_reader else {
        return false;
    };

    let mut changed = false;

    for msg in session.messages.iter_mut() {
        let Some(parts) = msg.content_parts.as_ref() else {
            continue;
        };

        let image_urls = persistable_image_urls(parts);
        if image_urls.is_empty() {
            continue;
        }

        let mut results = msg.image_ocr.take().unwrap_or_default();
        for url in image_urls {
            let already = results.iter().any(|r| r.image_url == url);
            if already {
                continue;
            }

            match ocr_image_url_to_lines(Some(reader), url.as_str()).await {
                Ok(lines) => {
                    results.push(ImageOcrResult {
                        image_url: url,
                        lines,
                        error: None,
                    });
                }
                Err(err) => {
                    results.push(ImageOcrResult {
                        image_url: url,
                        lines: Vec::new(),
                        error: Some(err),
                    });
                }
            }
            changed = true;
        }

        if results.is_empty() {
            msg.image_ocr = None;
        } else {
            msg.image_ocr = Some(results);
        }
    }

    changed
}

#[cfg(windows)]
fn parse_data_url_base64(url: &str) -> Option<(String, String)> {
    // data:<mime>;base64,<data...>
    let trimmed = url.trim();
    if !trimmed.starts_with("data:") {
        return None;
    }
    let (header, data) = trimmed.split_once(',')?;
    if !header.contains(";base64") {
        return None;
    }
    let mime = header
        .strip_prefix("data:")?
        .split(';')
        .next()
        .unwrap_or("application/octet-stream")
        .trim()
        .to_string();
    Some((mime, data.trim().to_string()))
}

#[cfg(windows)]
async fn rewrite_parts_to_ocr_text(
    attachment_reader: Option<&dyn AttachmentReader>,
    parts: &[ContentPart],
    cached: Option<&[ImageOcrResult]>,
) -> std::result::Result<String, String> {
    let mut out = String::new();
    let mut image_index = 0usize;

    for part in parts.iter() {
        match part {
            ContentPart::Text { text } => out.push_str(text),
            ContentPart::ImageUrl { image_url } => {
                image_index += 1;
                let summary = summarize_image_url(&image_url.url);

                let cached_lines = cached.and_then(|items| {
                    items
                        .iter()
                        .find(|r| r.image_url == image_url.url)
                        .map(|r| (r.lines.as_slice(), r.error.as_deref()))
                });

                let ocr_result = if let Some((lines, err)) = cached_lines {
                    if let Some(err) = err {
                        Err(err.to_string())
                    } else {
                        Ok(lines.to_vec())
                    }
                } else {
                    ocr_image_url_to_lines(attachment_reader, &image_url.url).await
                };

                match ocr_result {
                    Ok(lines) if !lines.is_empty() => {
                        out.push_str("\n\n[OCR extracted from image ");
                        out.push_str(&image_index.to_string());
                        out.push_str(": ");
                        out.push_str(&summary);
                        out.push_str("]\n");
                        for l in lines {
                            out.push_str(&format!(
                                "({},{},{},{}) {}\n",
                                l.left, l.top, l.width, l.height, l.text
                            ));
                        }
                    }
                    Ok(_) => {
                        out.push_str("\n\n[OCR extracted from image ");
                        out.push_str(&image_index.to_string());
                        out.push_str(": ");
                        out.push_str(&summary);
                        out.push_str("]\n(no text detected)\n");
                    }
                    Err(err) => {
                        log::warn!(
                            "OCR failed for image {} ({}): {}",
                            image_index,
                            summary,
                            err
                        );
                        out.push_str("\n[Image omitted: ");
                        out.push_str(&summary);
                        out.push_str("]\n");
                    }
                }
            }
        }
    }

    Ok(out)
}

#[cfg(windows)]
async fn ocr_image_url_to_lines(
    attachment_reader: Option<&dyn AttachmentReader>,
    url: &str,
) -> std::result::Result<Vec<ImageOcrLine>, String> {
    let (mime, bytes) = if let Some((mime, data)) = parse_data_url_base64(url) {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data.as_bytes())
            .map_err(|e| format!("invalid base64 data: {e}"))?;
        (mime, bytes)
    } else if let Some((session_id, attachment_id)) = parse_bamboo_attachment_url(url) {
        let Some(reader) = attachment_reader else {
            return Err(
                "cannot resolve bamboo-attachment URL without an attachment reader".to_string(),
            );
        };
        match reader
            .read_attachment(session_id, attachment_id)
            .await
            .map_err(|e| format!("failed reading attachment: {e}"))?
        {
            Some((bytes, mime)) => (mime, bytes),
            None => return Err("attachment not found".to_string()),
        }
    } else {
        return Err("unsupported image URL (expected data: or bamboo-attachment:)".to_string());
    };

    if mime != "image/png" {
        return Err(format!(
            "unsupported mime type '{mime}' (only image/png is supported)"
        ));
    }

    // Basic validation to avoid passing junk into the decoder.
    const PNG_SIG: [u8; 8] = [137, 80, 78, 71, 13, 10, 26, 10];
    if bytes.len() < PNG_SIG.len() || bytes[..PNG_SIG.len()] != PNG_SIG {
        return Err("decoded data is not a PNG".to_string());
    }

    // rust_ocr currently expects a PNG file path (it uses BitmapDecoder::PngDecoderId()).
    let tmp_path = std::env::temp_dir().join(format!("bamboo_ocr_{}.png", uuid::Uuid::new_v4()));
    std::fs::write(&tmp_path, &bytes).map_err(|e| format!("failed writing tmp png: {e}"))?;

    // WinRT OCR can block; keep it off the async executor.
    let tmp_path2 = tmp_path.clone();
    let coords = tokio::task::spawn_blocking(move || {
        // `rust_ocr` returns `Box<dyn Error>` which is not `Send`, so we must not
        // return it across the thread boundary. Convert to `String` inside the
        // blocking closure.
        rust_ocr::ocr_with_bounds(tmp_path2, None).map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| format!("ocr task join failed: {e}"))?
    .map_err(|e| format!("ocr failed: {e}"))?;

    let _ = std::fs::remove_file(&tmp_path);

    Ok(extract_line_candidates(coords))
}

#[cfg(windows)]
fn extract_line_candidates(coords: Vec<rust_ocr::Coordinates>) -> Vec<ImageOcrLine> {
    // `rust_ocr::ocr_with_bounds` yields word-level coordinates and then a line-level
    // coordinate for each OCR line. We pick the line-level entries by matching them
    // against the accumulated words for that line.
    let mut out = Vec::new();
    let mut current_words: Vec<String> = Vec::new();

    for c in coords.into_iter() {
        let text = c.text.trim().to_string();
        if text.is_empty() {
            continue;
        }

        if !current_words.is_empty() {
            let joined = current_words.join(" ");
            if normalize_ws(&joined) == normalize_ws(&text) {
                out.push(ImageOcrLine {
                    text,
                    left: c.x.round() as i32,
                    top: c.y.round() as i32,
                    width: c.width.round() as i32,
                    height: c.height.round() as i32,
                });
                current_words.clear();
                continue;
            }
        }

        current_words.push(text);
    }

    // Fallback: if we couldn't identify lines, emit a compact word list instead.
    if out.is_empty() && !current_words.is_empty() {
        out.push(ImageOcrLine {
            text: current_words.join(" "),
            left: 0,
            top: 0,
            width: 0,
            height: 0,
        });
    }

    out
}

#[cfg(windows)]
fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn parse_bamboo_attachment_url(url: &str) -> Option<(&str, &str)> {
    let trimmed = url.trim();
    let rest = trimmed.strip_prefix("bamboo-attachment://")?;
    let (session_id, attachment_id) = rest.split_once('/')?;
    if session_id.is_empty() || attachment_id.is_empty() {
        return None;
    }
    Some((session_id, attachment_id))
}

async fn resolve_bamboo_attachments_for_llm(
    messages: &mut [Message],
    reader: &dyn AttachmentReader,
) -> Result<()> {
    for msg in messages.iter_mut() {
        let Some(parts) = msg.content_parts.as_mut() else {
            continue;
        };
        for part in parts.iter_mut() {
            let ContentPart::ImageUrl { image_url } = part else {
                continue;
            };
            let Some((session_id, attachment_id)) = parse_bamboo_attachment_url(&image_url.url)
            else {
                continue;
            };
            let Some((bytes, mime)) = reader
                .read_attachment(session_id, attachment_id)
                .await
                .map_err(|e| AgentError::LLM(format!("failed to read attachment: {e}")))?
            else {
                return Err(AgentError::LLM(format!(
                    "attachment not found: {session_id}/{attachment_id}"
                )));
            };
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            image_url.url = format!("data:{mime};base64,{encoded}");
        }
    }
    Ok(())
}

/// Result type for agent loop operations.
pub type Result<T> = std::result::Result<T, AgentError>;

/// Runs the agent loop with a custom configuration.
///
/// This is the primary entry point for executing an agent conversation loop.
/// It manages LLM streaming, tool execution, todo list tracking, metrics collection,
/// and event emission throughout the conversation lifecycle.
///
/// # Arguments
///
/// * `session` - The conversation session to operate on
/// * `initial_message` - The user's initial message to process
/// * `event_tx` - Channel sender for agent events
/// * `llm` - The LLM provider to use for generation
/// * `tools` - The tool executor for handling tool calls
/// * `cancel_token` - Token for cancelling the operation
/// * `config` - Configuration controlling loop behavior
///
/// # Returns
///
/// Returns `Ok(())` on successful completion, or an error if the loop fails.
pub async fn run_agent_loop_with_config(
    session: &mut Session,
    initial_message: String,
    event_tx: mpsc::Sender<AgentEvent>,
    llm: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
    cancel_token: CancellationToken,
    config: AgentLoopConfig,
) -> Result<()> {
    let debug_logger = DebugLogger::new(log::log_enabled!(log::Level::Debug));
    let session_id = session.id.clone();
    let metrics_collector = config.metrics_collector.clone();
    let model_name = config
        .model_name
        .clone()
        .unwrap_or_else(|| "unknown".to_string());

    if let Some(metrics) = metrics_collector.as_ref() {
        metrics.session_started(session_id.clone(), model_name.clone(), session.created_at);
        metrics.session_message_count(
            session_id.clone(),
            session.messages.len() as u32,
            Utc::now(),
        );
    }

    log::debug!(
        "[{}] Starting agent loop with message: {}",
        session_id,
        initial_message
    );
    debug_logger.log_event(
        &session_id,
        "agent_loop_start",
        serde_json::json!({
            "message": initial_message,
            "max_rounds": config.max_rounds,
            "initial_message_count": session.messages.len(),
        }),
    );

    let skill_context = if let Some(skill_manager) = config.skill_manager.as_ref() {
        let context = skill_manager
            .build_skill_context(Some(session.id.as_str()))
            .await;
        if !context.is_empty() {
            log::info!(
                "[{}] Skill context loaded, length: {} chars",
                session_id,
                context.len()
            );
            log::debug!("[{}] Skill context content:\n{}", session_id, context);
        } else {
            log::info!("[{}] No skill context loaded (empty)", session_id);
        }
        context
    } else {
        log::info!("[{}] No skill manager configured", session_id);
        String::new()
    };

    // Build tool guide context for enhanced prompting
    let base_prompt_for_language = config
        .system_prompt
        .as_deref()
        .or_else(|| {
            session
                .messages
                .iter()
                .find(|message| matches!(message.role, crate::agent::core::Role::System))
                .map(|message| message.content.as_str())
        })
        .unwrap_or_default();
    let guide_context = GuideBuildContext::from_system_prompt(base_prompt_for_language);
    let tool_schemas = resolve_available_tool_schemas(&config, tools.as_ref());
    let tool_guide_context = EnhancedPromptBuilder::build(
        Some(config.tool_registry.as_ref()),
        &tool_schemas,
        &guide_context,
    );
    log::info!(
        "[{}] Tool guide context built, length: {} chars",
        session_id,
        tool_guide_context.len()
    );

    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    {
        let base_prompt = config
            .system_prompt
            .as_deref()
            .unwrap_or(&system_message.content);
        system_message.content =
            merge_system_prompt_with_contexts(base_prompt, &skill_context, &tool_guide_context);
    } else {
        let base_prompt = config.system_prompt.as_deref().unwrap_or_default();
        let merged_prompt =
            merge_system_prompt_with_contexts(base_prompt, &skill_context, &tool_guide_context);
        if !merged_prompt.is_empty() {
            session.messages.insert(0, Message::system(merged_prompt));
        }
    }

    if !config.skip_initial_user_message {
        session.add_message(Message::user(initial_message.clone()));
        if let Some(metrics) = metrics_collector.as_ref() {
            metrics.session_message_count(
                session_id.clone(),
                session.messages.len() as u32,
                Utc::now(),
            );
        }
    }

    let mut sent_complete = false;

    // Initialize TodoLoopContext from session's todo list
    let mut todo_context = TodoLoopContext::from_session(session);
    if todo_context.is_some() {
        log::debug!("[{}] TodoLoopContext initialized", session_id);
    }

    for round in 0..config.max_rounds {
        // Load/refresh persistent memory note for this round.
        inject_external_memory_into_system_message(session).await;

        // Inject todo list into system message at the start of each round
        inject_todo_list_into_system_message(session);

        // Update TodoLoopContext round and inject into prompt
        if let Some(ref mut ctx) = todo_context {
            ctx.current_round = round as u32;
            ctx.max_rounds = config.max_rounds as u32;
        }

        let round_id = format!("{}-round-{}", session_id, round + 1);
        let mut round_status = MetricsRoundStatus::Success;
        let mut round_error: Option<String> = None;

        debug_logger.log_event(
            &session_id,
            "round_start",
            serde_json::json!({
                "round": round + 1,
                "total_rounds": config.max_rounds,
                "message_count": session.messages.len(),
            }),
        );

        if cancel_token.is_cancelled() {
            if let Some(metrics) = metrics_collector.as_ref() {
                metrics.session_message_count(
                    session_id.clone(),
                    session.messages.len() as u32,
                    Utc::now(),
                );
                metrics.session_completed(
                    session_id.clone(),
                    MetricsSessionStatus::Cancelled,
                    Utc::now(),
                );
            }
            return Err(AgentError::Cancelled);
        }

        if let Some(metrics) = metrics_collector.as_ref() {
            metrics.round_started(
                round_id.clone(),
                session_id.clone(),
                model_name.clone(),
                Utc::now(),
            );
        }

        let tool_schemas = resolve_available_tool_schemas(&config, tools.as_ref());

        // If OCR fallback is enabled, compute + cache OCR results into the persisted session
        // (but do NOT rewrite message parts). This keeps OCR available for the UI while
        // also allowing the LLM request to be built from text-only projections.
        #[cfg(windows)]
        if matches!(
            config.image_fallback,
            Some(crate::agent::loop_module::config::ImageFallbackConfig {
                mode: crate::agent::loop_module::config::ImageFallbackMode::Ocr
            })
        ) {
            let changed =
                ensure_session_image_ocr_cached(session, config.attachment_reader.as_deref()).await;
            if changed {
                if let Some(ref storage) = config.storage {
                    if let Err(e) = storage.save_session(session).await {
                        log::warn!(
                            "[{}] Failed to save session after OCR caching: {}",
                            session_id,
                            e
                        );
                    }
                }
            }
        }

        // Token budget preparation
        let budget = resolve_token_budget(session, &config, &model_name);
        let counter = HeuristicTokenCounter::default();

        let mut prepared_context = match prepare_hybrid_context(session, &budget, &counter) {
            Ok(ctx) => ctx,
            Err(e) => {
                let agent_error = AgentError::Budget(e.to_string());
                round_status = MetricsRoundStatus::Error;
                round_error = Some(agent_error.to_string());
                if let Some(metrics) = metrics_collector.as_ref() {
                    metrics.round_completed(
                        round_id.clone(),
                        Utc::now(),
                        round_status,
                        MetricsTokenUsage::default(),
                        round_error.clone(),
                    );
                    metrics.session_message_count(
                        session_id.clone(),
                        session.messages.len() as u32,
                        Utc::now(),
                    );
                    metrics.session_completed(
                        session_id.clone(),
                        MetricsSessionStatus::Error,
                        Utc::now(),
                    );
                }
                return Err(agent_error);
            }
        };

        // Apply image fallback (placeholder / OCR / error) to the prepared LLM context only.
        // This must never mutate the persisted session messages (UI should still show images).
        if let Some(fallback) = config.image_fallback {
            apply_image_fallback_to_llm_messages(
                &mut prepared_context.messages,
                fallback,
                config.attachment_reader.as_deref(),
            )
            .await?;
        }

        // Resolve `bamboo-attachment://...` URLs into `data:` URLs for upstream providers.
        // This must only mutate the prepared context (never the persisted session messages).
        if let Some(reader) = config.attachment_reader.as_deref() {
            resolve_bamboo_attachments_for_llm(&mut prepared_context.messages, reader).await?;
        }

        if prepared_context.truncation_occurred {
            log::info!(
                "[{}] Context truncated: removed {} segments, using {} tokens of {} ({:.1}%)",
                session_id,
                prepared_context.segments_removed,
                prepared_context.token_usage.total_tokens,
                prepared_context.token_usage.budget_limit,
                prepared_context.token_usage.usage_percentage()
            );
        }

        let timer = Timer::new("llm_request");

        // Use model from config (provided by execute request), not from session
        let model = config.model_name.as_deref().ok_or_else(|| {
            crate::agent::core::AgentError::LLM(
                "model_name is required in AgentLoopConfig".to_string(),
            )
        })?;

        let stream = match llm
            .chat_stream(
                &prepared_context.messages,
                &tool_schemas,
                Some(budget.max_output_tokens),
                model,
            )
            .await
        {
            Ok(stream) => {
                // Send token budget update AFTER LLM call succeeds
                // This timing gives frontend time to subscribe to /events endpoint
                let usage = TokenBudgetUsage {
                    system_tokens: prepared_context.token_usage.system_tokens,
                    summary_tokens: prepared_context.token_usage.summary_tokens,
                    window_tokens: prepared_context.token_usage.window_tokens,
                    total_tokens: prepared_context.token_usage.total_tokens,
                    budget_limit: prepared_context.token_usage.budget_limit,
                    truncation_occurred: prepared_context.truncation_occurred,
                    segments_removed: prepared_context.segments_removed,
                };

                // Save to session for persistence
                session.token_usage = Some(usage.clone());

                let budget_event = AgentEvent::TokenBudgetUpdated { usage };
                if let Err(e) = event_tx.send(budget_event).await {
                    log::warn!("[{}] Failed to send token budget event: {}", session_id, e);
                }
                stream
            }
            Err(error) => {
                let agent_error = AgentError::LLM(error.to_string());
                round_status = MetricsRoundStatus::Error;
                round_error = Some(agent_error.to_string());
                if let Some(metrics) = metrics_collector.as_ref() {
                    metrics.round_completed(
                        round_id.clone(),
                        Utc::now(),
                        round_status,
                        MetricsTokenUsage::default(),
                        round_error.clone(),
                    );
                    metrics.session_message_count(
                        session_id.clone(),
                        session.messages.len() as u32,
                        Utc::now(),
                    );
                    metrics.session_completed(
                        session_id.clone(),
                        MetricsSessionStatus::Error,
                        Utc::now(),
                    );
                }
                return Err(agent_error);
            }
        };

        let stream_output =
            match consume_llm_stream(stream, &event_tx, &cancel_token, &session_id).await {
                Ok(output) => output,
                Err(error) => {
                    round_status = if matches!(error, AgentError::Cancelled) {
                        MetricsRoundStatus::Cancelled
                    } else {
                        MetricsRoundStatus::Error
                    };
                    round_error = Some(error.to_string());
                    if let Some(metrics) = metrics_collector.as_ref() {
                        metrics.round_completed(
                            round_id.clone(),
                            Utc::now(),
                            round_status,
                            MetricsTokenUsage::default(),
                            round_error.clone(),
                        );
                        let session_status = if matches!(error, AgentError::Cancelled) {
                            MetricsSessionStatus::Cancelled
                        } else {
                            MetricsSessionStatus::Error
                        };
                        metrics.session_message_count(
                            session_id.clone(),
                            session.messages.len() as u32,
                            Utc::now(),
                        );
                        metrics.session_completed(session_id.clone(), session_status, Utc::now());
                    }
                    return Err(error);
                }
            };

        let round_usage = MetricsTokenUsage {
            prompt_tokens: 0,
            completion_tokens: stream_output.token_count as u64,
            total_tokens: stream_output.token_count as u64,
        };

        let llm_duration = timer.elapsed_ms();
        timer.debug(&session_id);
        log::debug!(
            "[{}] LLM response completed in {}ms, {} tokens received",
            session_id,
            llm_duration,
            stream_output.token_count
        );

        if stream_output.tool_calls.is_empty() {
            session.add_message(Message::assistant(stream_output.content, None));

            let _ = event_tx
                .send(AgentEvent::Complete {
                    usage: TokenUsage {
                        prompt_tokens: 0,
                        completion_tokens: stream_output.token_count as u32,
                        total_tokens: stream_output.token_count as u32,
                    },
                })
                .await;

            if let Some(metrics) = metrics_collector.as_ref() {
                metrics.round_completed(
                    round_id.clone(),
                    Utc::now(),
                    MetricsRoundStatus::Success,
                    round_usage,
                    None,
                );
                metrics.session_message_count(
                    session_id.clone(),
                    session.messages.len() as u32,
                    Utc::now(),
                );
            }

            sent_complete = true;
            break;
        }

        session.add_message(Message::assistant(
            stream_output.content,
            Some(stream_output.tool_calls.clone()),
        ));

        let mut awaiting_clarification = false;

        for tool_call in &stream_output.tool_calls {
            let args = parse_tool_args(&tool_call.function.arguments)
                .unwrap_or_else(|_| serde_json::json!({}));

            send_event_with_metrics(
                &event_tx,
                metrics_collector.as_ref(),
                &session_id,
                &round_id,
                AgentEvent::ToolStart {
                    tool_call_id: tool_call.id.clone(),
                    tool_name: tool_call.function.name.clone(),
                    arguments: args,
                },
            )
            .await;

            let tool_timer = Timer::new(format!("tool_{}", tool_call.function.name));

            let tool_ctx = crate::agent::core::tools::ToolExecutionContext {
                session_id: Some(session_id.as_str()),
                tool_call_id: &tool_call.id,
                event_tx: Some(&event_tx),
            };

            match crate::agent::core::tools::executor::execute_tool_call_with_context(
                tool_call,
                tools.as_ref(),
                config.composition_executor.as_ref().map(Arc::clone),
                tool_ctx,
            )
            .await
            {
                Ok(result) => {
                    // Track tool execution in TodoLoopContext
                    if let Some(ref mut ctx) = todo_context {
                        // IMPORTANT: First auto-update status (may set active_item)
                        // Then track tool execution (so first tool is recorded)
                        ctx.auto_update_status(&tool_call.function.name, &result);

                        ctx.track_tool_execution(&tool_call.function.name, &result, round as u32);

                        // Send progress event if active item exists
                        // Note: Even if auto_update_status cleared active_item_id (completed),
                        // we still have a reference to the just-updated item
                        let progress_event = if let Some(ref active_id) = ctx.active_item_id {
                            // Active item still set (in progress or blocked)
                            ctx.items.iter().find(|i| &i.id == active_id).map(|item| {
                                AgentEvent::TodoListItemProgress {
                                    session_id: session_id.clone(),
                                    item_id: item.id.clone(),
                                    status: item.status.clone(),
                                    tool_calls_count: item.tool_calls.len(),
                                    version: ctx.version,
                                }
                            })
                        } else {
                            // Active item was just completed, find it by checking last updated item
                            // The item that was just updated will have the highest version bump
                            ctx.items
                                .iter()
                                .find(|item| item.status == TodoItemStatus::Completed)
                                .map(|item| AgentEvent::TodoListItemProgress {
                                    session_id: session_id.clone(),
                                    item_id: item.id.clone(),
                                    status: item.status.clone(),
                                    tool_calls_count: item.tool_calls.len(),
                                    version: ctx.version,
                                })
                        };

                        if let Some(event) = progress_event {
                            let _ = event_tx.send(event).await;
                        }
                    }

                    // Handle todo list tools specially
                    if tool_call.function.name == "create_todo_list" && result.success {
                        if let Ok(args) =
                            serde_json::from_str::<serde_json::Value>(&tool_call.function.arguments)
                        {
                            if let Ok(todo_list) =
                                CreateTodoListTool::todo_list_from_args(&args, &session_id)
                            {
                                session.set_todo_list(todo_list.clone());
                                log::info!(
                                    "[{}] Todo list '{}' created with {} items",
                                    session_id,
                                    todo_list.title,
                                    todo_list.items.len()
                                );

                                // Save session to persist todo list
                                if let Some(ref storage) = config.storage {
                                    if let Err(e) = storage.save_session(session).await {
                                        log::warn!("[{}] Failed to save session after todo list creation: {}", session_id, e);
                                    } else {
                                        log::debug!(
                                            "[{}] Session saved after todo list creation",
                                            session_id
                                        );
                                    }
                                }

                                // Emit event for frontend
                                let _ = event_tx
                                    .send(AgentEvent::TodoListUpdated {
                                        todo_list: todo_list.clone(),
                                    })
                                    .await;

                                // IMPORTANT: Re-initialize TodoLoopContext from session
                                // This enables automatic tracking for newly created lists
                                todo_context = TodoLoopContext::from_session(session);
                                if todo_context.is_some() {
                                    log::debug!("[{}] TodoLoopContext re-initialized after create_todo_list", session_id);
                                }
                            }
                        }
                    } else if tool_call.function.name == "update_todo_item" && result.success {
                        if let Ok(args) =
                            serde_json::from_str::<serde_json::Value>(&tool_call.function.arguments)
                        {
                            if let (Some(item_id), Some(status)) =
                                (args["item_id"].as_str(), args["status"].as_str())
                            {
                                let status_enum = match status {
                                    "pending" => Some(crate::agent::core::TodoItemStatus::Pending),
                                    "in_progress" => {
                                        Some(crate::agent::core::TodoItemStatus::InProgress)
                                    }
                                    "completed" => {
                                        Some(crate::agent::core::TodoItemStatus::Completed)
                                    }
                                    "blocked" => Some(crate::agent::core::TodoItemStatus::Blocked),
                                    _ => None,
                                };
                                if let Some(s) = status_enum {
                                    let notes = args["notes"].as_str();

                                    // IMPORTANT: Update TodoLoopContext first to keep it in sync
                                    // This prevents final sync from overwriting manual updates
                                    if let Some(ref mut ctx) = todo_context {
                                        ctx.update_item_status(item_id, s.clone());
                                    }

                                    if let Err(e) = session.update_todo_item(item_id, s, notes) {
                                        log::warn!(
                                            "[{}] Failed to update todo item: {}",
                                            session_id,
                                            e
                                        );
                                    } else {
                                        log::info!(
                                            "[{}] Updated todo item '{}' to '{}'",
                                            session_id,
                                            item_id,
                                            status
                                        );

                                        // Save session to persist todo list changes
                                        if let Some(ref storage) = config.storage {
                                            if let Err(e) = storage.save_session(session).await {
                                                log::warn!("[{}] Failed to save session after todo item update: {}", session_id, e);
                                            } else {
                                                log::debug!(
                                                    "[{}] Session saved after todo item update",
                                                    session_id
                                                );
                                            }
                                        }

                                        // Emit event for frontend
                                        if let Some(ref todo_list) = session.todo_list {
                                            let _ = event_tx
                                                .send(AgentEvent::TodoListUpdated {
                                                    todo_list: todo_list.clone(),
                                                })
                                                .await;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Handle ask_user tool specially - emit NeedClarification event
                    if tool_call.function.name == "ask_user" && result.success {
                        if let Ok(payload) =
                            serde_json::from_str::<serde_json::Value>(&result.result)
                        {
                            let question = payload["question"]
                                .as_str()
                                .unwrap_or("Please select:")
                                .to_string();
                            let options: Vec<String> = payload["options"]
                                .as_array()
                                .map(|arr| {
                                    arr.iter()
                                        .filter_map(|v| v.as_str().map(String::from))
                                        .collect()
                                })
                                .unwrap_or_default();
                            let allow_custom = payload["allow_custom"].as_bool().unwrap_or(true);

                            log::info!(
                                "[{}] ask_user tool called, awaiting user response",
                                session_id
                            );

                            // Add tool result message (required by OpenAI API)
                            // This is a placeholder indicating we're waiting for user
                            let tool_result_msg = Message::tool_result(
                                tool_call.id.clone(),
                                format!("Waiting for user response to: {}", question),
                            );
                            log::debug!("[{}] Adding tool result message for ask_user, tool_call_id: {}, message_id: {}",
                                session_id, tool_call.id, tool_result_msg.id);
                            session.add_message(tool_result_msg);

                            // Ensure the UI/tooling pipeline sees this tool call as completed.
                            // (ToolStart was already emitted above; without a ToolComplete, some
                            // clients may keep the tool in an in-progress state.)
                            send_event_with_metrics(
                                &event_tx,
                                metrics_collector.as_ref(),
                                &session_id,
                                &round_id,
                                AgentEvent::ToolComplete {
                                    tool_call_id: tool_call.id.clone(),
                                    result: result.clone(),
                                },
                            )
                            .await;

                            // Emit NeedClarification event with options
                            let _ = event_tx
                                .send(AgentEvent::NeedClarification {
                                    question: question.clone(),
                                    options: if options.is_empty() {
                                        None
                                    } else {
                                        Some(options.clone())
                                    },
                                })
                                .await;

                            // Store pending question in session for resume handling
                            session.set_pending_question(
                                tool_call.id.clone(),
                                question,
                                options,
                                allow_custom,
                            );

                            // Save session to persist the pending question
                            if let Some(ref storage) = config.storage {
                                if let Err(e) = storage.save_session(session).await {
                                    log::warn!(
                                        "[{}] Failed to save session after ask_user: {}",
                                        session_id,
                                        e
                                    );
                                }
                            }

                            awaiting_clarification = true;
                            break;
                        }
                    }

                    send_event_with_metrics(
                        &event_tx,
                        metrics_collector.as_ref(),
                        &session_id,
                        &round_id,
                        AgentEvent::ToolComplete {
                            tool_call_id: tool_call.id.clone(),
                            result: result.clone(),
                        },
                    )
                    .await;

                    if !result.success && round_error.is_none() {
                        round_status = MetricsRoundStatus::Error;
                        round_error = Some(format!(
                            "Tool \"{}\" returned an unsuccessful result",
                            tool_call.function.name
                        ));
                    }

                    debug_logger.log_event(
                        &session_id,
                        "tool_complete",
                        serde_json::json!({
                            "tool_name": tool_call.function.name,
                            "tool_call_id": tool_call.id,
                            "duration_ms": tool_timer.elapsed_ms(),
                            "success": result.success,
                        }),
                    );

                    let outcome = handle_tool_result_with_agentic_support(
                        &result,
                        tool_call,
                        &event_tx,
                        session,
                        tools.as_ref(),
                        config.composition_executor.as_ref().map(Arc::clone),
                    )
                    .await;

                    if outcome == ToolHandlingOutcome::AwaitingClarification {
                        awaiting_clarification = true;
                        break;
                    }
                }
                Err(error) => {
                    let error_message = error.to_string();
                    round_status = MetricsRoundStatus::Error;
                    round_error = Some(error_message.clone());

                    send_event_with_metrics(
                        &event_tx,
                        metrics_collector.as_ref(),
                        &session_id,
                        &round_id,
                        AgentEvent::ToolError {
                            tool_call_id: tool_call.id.clone(),
                            error: error_message.clone(),
                        },
                    )
                    .await;

                    session.add_message(Message::tool_result(
                        tool_call.id.clone(),
                        format!("Error: {error_message}"),
                    ));
                }
            }
        }

        if awaiting_clarification {
            if let Some(metrics) = metrics_collector.as_ref() {
                metrics.round_completed(
                    round_id.clone(),
                    Utc::now(),
                    round_status,
                    round_usage,
                    round_error.clone(),
                );
                metrics.session_message_count(
                    session_id.clone(),
                    session.messages.len() as u32,
                    Utc::now(),
                );
            }
            break;
        }

        debug_logger.log_event(
            &session_id,
            "round_complete",
            serde_json::json!({
                "round": round + 1,
                "message_count": session.messages.len(),
            }),
        );

        // ========== NEW: TodoList Evaluation at end of each round ==========
        // Let LLM evaluate task progress with a dedicated query
        if let Some(ref ctx) = todo_context {
            use crate::agent::loop_module::todo_evaluation::evaluate_todo_progress;

            log::debug!(
                "[{}] Evaluating todo list progress at end of round {}",
                session_id,
                round + 1
            );

            // Use model from config
            let model = config.model_name.as_deref().ok_or_else(|| {
                crate::agent::core::AgentError::LLM(
                    "model_name is required in AgentLoopConfig".to_string(),
                )
            })?;

            match evaluate_todo_progress(
                ctx,
                session,
                llm.clone(),
                &event_tx,
                &session_id,
                model, // Pass model from config
            )
            .await
            {
                Ok(evaluation_result) => {
                    if evaluation_result.needs_evaluation && !evaluation_result.updates.is_empty() {
                        log::info!(
                            "[{}] LLM evaluated {} todo item updates",
                            session_id,
                            evaluation_result.updates.len()
                        );

                        // Apply LLM's updates to TodoLoopContext
                        if let Some(ref mut ctx) = todo_context {
                            for update in evaluation_result.updates {
                                let status = update.status.clone();
                                ctx.update_item_status(&update.item_id, status);

                                // Also update session for persistence
                                if let Some(notes) = update.notes {
                                    let _ = session.update_todo_item(
                                        &update.item_id,
                                        update.status,
                                        Some(&notes),
                                    );
                                } else {
                                    let status = update.status.clone();
                                    let _ = session.update_todo_item(&update.item_id, status, None);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    log::warn!("[{}] Todo evaluation failed: {}", session_id, e);
                }
            }
        }

        if let Some(metrics) = metrics_collector.as_ref() {
            metrics.round_completed(
                round_id.clone(),
                Utc::now(),
                round_status,
                round_usage,
                round_error.clone(),
            );
            metrics.session_message_count(
                session_id.clone(),
                session.messages.len() as u32,
                Utc::now(),
            );
        }
    }

    // Check if all todo items completed
    if let Some(ref ctx) = todo_context {
        if ctx.is_all_completed() {
            log::info!("[{}] All todo items completed", session_id);

            let _ = event_tx
                .send(AgentEvent::TodoListCompleted {
                    session_id: session_id.clone(),
                    completed_at: Utc::now(),
                    total_rounds: ctx.current_round + 1, // Convert 0-indexed to 1-indexed for display
                    total_tool_calls: ctx.items.iter().map(|i| i.tool_calls.len()).sum(),
                })
                .await;
        }
    }

    // Sync TodoLoopContext back to session
    if let Some(ctx) = todo_context {
        // Save version to session metadata before consuming ctx
        let version = ctx.version;
        session
            .metadata
            .insert("todo_list_version".to_string(), version.to_string());

        session.todo_list = Some(ctx.into_todo_list());
        session.updated_at = Utc::now();

        log::debug!(
            "[{}] Synced TodoLoopContext to session, version={}",
            session_id,
            version
        );

        // Persist session with updated todo list
        if let Some(ref storage) = config.storage {
            if let Err(e) = storage.save_session(session).await {
                log::warn!(
                    "[{}] Failed to save session after agent loop: {}",
                    session_id,
                    e
                );
            } else {
                log::debug!("[{}] Session saved with updated todo list", session_id);
            }
        }
    }

    if !sent_complete {
        let _ = event_tx
            .send(AgentEvent::Complete {
                usage: TokenUsage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
            })
            .await;
    }

    if let Some(metrics) = metrics_collector.as_ref() {
        metrics.session_message_count(
            session_id.clone(),
            session.messages.len() as u32,
            Utc::now(),
        );
        if !session.has_pending_question() {
            metrics.session_completed(session_id, MetricsSessionStatus::Completed, Utc::now());
        }
    }

    Ok(())
}

async fn send_event_with_metrics(
    event_tx: &mpsc::Sender<AgentEvent>,
    metrics_collector: Option<&MetricsCollector>,
    session_id: &str,
    round_id: &str,
    event: AgentEvent,
) {
    if let Some(metrics) = metrics_collector {
        metrics.record_agent_event(session_id, round_id, &event);
    }

    let _ = event_tx.send(event).await;
}

fn resolve_token_budget(
    session: &Session,
    config: &AgentLoopConfig,
    model_name: &str,
) -> TokenBudget {
    // Priority: session override > config override > model defaults
    if let Some(ref budget) = session.token_budget {
        log::debug!("Using session-specific token budget");
        return budget.clone();
    }

    if let Some(ref budget) = config.token_budget {
        log::debug!("Using config token budget");
        return budget.clone();
    }

    // Default to model limits
    let registry = ModelLimitsRegistry::default();
    let model_limit = registry.get_or_default(model_name);

    TokenBudget::with_safety_margin(
        model_limit.max_context_tokens,
        model_limit.get_max_output_tokens(),
        crate::agent::core::budget::BudgetStrategy::default(),
        model_limit.get_safety_margin(),
    )
}

fn resolve_available_tool_schemas(
    config: &AgentLoopConfig,
    tools: &dyn ToolExecutor,
) -> Vec<ToolSchema> {
    let mut tool_schemas = config.tool_registry.list_tools();
    if tool_schemas.is_empty() {
        tool_schemas = tools.list_tools();
    }

    tool_schemas.extend(config.additional_tool_schemas.clone());
    tool_schemas.sort_by(|left, right| left.function.name.cmp(&right.function.name));
    tool_schemas.dedup_by(|left, right| left.function.name == right.function.name);
    tool_schemas
}

const SKILL_CONTEXT_MARKER: &str = "\n\n## Available Skills\n";
const TOOL_GUIDE_MARKER: &str = "## Tool Usage Guidelines\n";
const EXTERNAL_MEMORY_MARKER: &str = "<!-- BAMBOO_EXTERNAL_MEMORY_START -->\n";
const EXTERNAL_MEMORY_PROMPT_MAX_CHARS: usize = 4_000;
const EXTERNAL_MEMORY_TOOL_NAME: &str = "memory_note";

fn merge_system_prompt_with_contexts(
    base_prompt: &str,
    skill_context: &str,
    tool_guide_context: &str,
) -> String {
    let mut merged = strip_existing_tool_guide_context(&strip_existing_skill_context(base_prompt));

    let sections: Vec<&str> = [skill_context, tool_guide_context]
        .into_iter()
        .map(str::trim)
        .filter(|section| !section.is_empty())
        .collect();

    if sections.is_empty() {
        return merged;
    }

    if merged.trim().is_empty() {
        return sections.join("\n\n");
    }

    for section in sections {
        merged.push_str("\n\n");
        merged.push_str(section);
    }

    merged
}

fn strip_existing_skill_context(prompt: &str) -> String {
    strip_existing_prompt_section(prompt, SKILL_CONTEXT_MARKER)
}

fn strip_existing_tool_guide_context(prompt: &str) -> String {
    strip_existing_prompt_section(prompt, TOOL_GUIDE_MARKER)
}

fn strip_existing_prompt_section(prompt: &str, marker: &str) -> String {
    if let Some(index) = prompt.find(marker) {
        prompt[..index].trim_end().to_string()
    } else {
        prompt.to_string()
    }
}

fn strip_existing_external_memory(prompt: &str) -> String {
    strip_existing_prompt_section(prompt, EXTERNAL_MEMORY_MARKER)
}

fn truncate_chars(value: &str, max_chars: usize) -> (String, bool) {
    let mut out = String::new();
    let mut count = 0usize;
    for ch in value.chars() {
        if count >= max_chars {
            return (out, true);
        }
        out.push(ch);
        count += 1;
    }
    (out, false)
}

async fn inject_external_memory_into_system_message(session: &mut Session) {
    let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    else {
        return;
    };

    // Remove any previously injected memory (and everything after it, e.g. todo list),
    // then re-append a fresh memory section for this round.
    let base_prompt = strip_existing_external_memory(&system_message.content);

    let memory = ExternalMemory::with_defaults();
    let note = match memory.read_note(session.id.as_str()).await {
        Ok(Some(content)) => content,
        Ok(None) => String::new(),
        Err(error) => {
            log::warn!(
                "[{}] Failed to read external memory note: {}",
                session.id,
                error
            );
            String::new()
        }
    };

    let note = note.trim().to_string();
    let note_len = note.chars().count();
    let (note_snippet, truncated) = truncate_chars(&note, EXTERNAL_MEMORY_PROMPT_MAX_CHARS);
    let note_for_prompt = if note_snippet.trim().is_empty() {
        "_(empty)_".to_string()
    } else {
        note_snippet
    };

    let mut section = String::new();
    section.push_str("\n\n");
    section.push_str(EXTERNAL_MEMORY_MARKER);
    section.push_str("## External Memory (Persistent)\n\n");
    section.push_str("You have access to a persistent per-session memory note.\n");
    section.push_str("- If you learn durable information that will help later (preferences, key decisions, constraints, environment details), update the note using the tool.\n");
    section.push_str("- Do NOT store secrets/tokens.\n");
    section.push_str("- Keep the note concise and factual. If it gets too long, compress it (rewrite a shorter version) and replace it.\n\n");
    section.push_str("Tool usage:\n");
    section.push_str(&format!(
        "- Append: call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"append\",\"content\":\"...\"}}`\n"
    ));
    section.push_str(&format!(
        "- Replace (for compression): call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"replace\",\"content\":\"...\"}}`\n"
    ));
    section.push_str(&format!(
        "- Read full note (if truncated): call `{EXTERNAL_MEMORY_TOOL_NAME}` with `{{\"action\":\"read\"}}`\n\n"
    ));

    section.push_str("### Current Note (markdown)\n");
    // Use a 4-backtick fence to reduce collisions with markdown code fences inside the note itself.
    section.push_str("````md\n");
    section.push_str(&note_for_prompt);
    section.push_str("\n````\n");

    if truncated {
        section.push_str(&format!(
            "\nNote is truncated in the system prompt (showing first {} chars of {}). Use `{}` action=read to view it and then action=replace to compress it.\n",
            EXTERNAL_MEMORY_PROMPT_MAX_CHARS, note_len, EXTERNAL_MEMORY_TOOL_NAME
        ));
    }

    system_message.content = format!("{}{}", base_prompt.trim_end(), section);
}

const TODO_LIST_MARKER: &str = "\n\n## Current Task List:";

/// Inject todo list into system message if it exists
fn inject_todo_list_into_system_message(session: &mut Session) {
    let todo_context = session.format_todo_list_for_prompt();

    if let Some(system_message) = session
        .messages
        .iter_mut()
        .find(|message| matches!(message.role, crate::agent::core::Role::System))
    {
        let base_prompt = strip_existing_todo_list(&system_message.content);

        if !todo_context.is_empty() {
            system_message.content = format!("{}\n{}", base_prompt, todo_context);
            log::info!(
                "Injected todo list into system message ({} chars)",
                todo_context.len()
            );
        } else {
            system_message.content = base_prompt;
        }
    } else if !todo_context.is_empty() {
        // No system message exists but we have todo context
        session
            .messages
            .insert(0, Message::system(todo_context.clone()));
        log::info!(
            "Created system message with todo list ({} chars)",
            todo_context.len()
        );
    }
}

fn strip_existing_todo_list(prompt: &str) -> String {
    if let Some(index) = prompt.find(TODO_LIST_MARKER) {
        prompt[..index].trim_end().to_string()
    } else {
        prompt.to_string()
    }
}

#[allow(dead_code)]
pub async fn run_agent_loop(
    session: &mut Session,
    initial_message: String,
    event_tx: mpsc::Sender<AgentEvent>,
    llm: Arc<dyn LLMProvider>,
    tools: Arc<dyn ToolExecutor>,
    cancel_token: CancellationToken,
    max_rounds: usize,
) -> Result<()> {
    run_agent_loop_with_config(
        session,
        initial_message,
        event_tx,
        llm,
        tools,
        cancel_token,
        AgentLoopConfig {
            max_rounds,
            skip_initial_user_message: false,
            ..Default::default()
        },
    )
    .await
}

struct DebugLogger {
    enabled: bool,
}

impl DebugLogger {
    fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    fn log_event(&self, session_id: &str, event_type: &str, details: serde_json::Value) {
        if !self.enabled {
            return;
        }

        log::debug!("[{}] {}: {}", session_id, event_type, details);
    }
}

struct Timer {
    name: String,
    start: std::time::Instant,
}

impl Timer {
    fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            start: std::time::Instant::now(),
        }
    }

    fn elapsed_ms(&self) -> u128 {
        self.start.elapsed().as_millis()
    }

    fn debug(&self, session_id: &str) {
        log::debug!(
            "[{}] {} completed in {}ms",
            session_id,
            self.name,
            self.elapsed_ms()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_image_fallback_to_llm_messages, merge_system_prompt_with_contexts,
        persistable_image_urls, strip_existing_skill_context, strip_existing_tool_guide_context,
        AgentLoopConfig,
    };

    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::stream;
    use tokio::sync::{mpsc, Mutex};
    use tokio_util::sync::CancellationToken;

    use crate::agent::core::tools::{
        FunctionCall, Tool, ToolError, ToolExecutionContext, ToolResult,
    };
    use crate::agent::core::{Message, Session};
    use crate::agent::llm::models::{ContentPart, ImageUrl};
    use crate::agent::llm::{LLMChunk, LLMProvider, LLMStream};
    use crate::agent::loop_module::config::{ImageFallbackConfig, ImageFallbackMode};
    use crate::agent::tools::BuiltinToolExecutorBuilder;

    #[test]
    fn persistable_image_urls_filters_out_data_urls() {
        let parts = vec![
            ContentPart::Text {
                text: "hello".to_string(),
            },
            ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "data:image/png;base64,AAAA".to_string(),
                    detail: None,
                },
            },
            ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "bamboo-attachment://s1/a1".to_string(),
                    detail: None,
                },
            },
        ];

        let urls = persistable_image_urls(&parts);
        assert_eq!(urls, vec!["bamboo-attachment://s1/a1".to_string()]);
    }

    #[tokio::test]
    async fn image_fallback_placeholder_does_not_mutate_persisted_session_messages() {
        let parts = vec![
            ContentPart::Text {
                text: "这个内容有什么".to_string(),
            },
            ContentPart::ImageUrl {
                image_url: ImageUrl {
                    url: "bamboo-attachment://s1/a1".to_string(),
                    detail: None,
                },
            },
        ];

        let mut session = Session::new("s1", "m");
        session
            .messages
            .push(Message::user_with_parts("这个内容有什么", parts));

        let mut llm_messages = session.messages.clone();
        apply_image_fallback_to_llm_messages(
            &mut llm_messages,
            ImageFallbackConfig {
                mode: ImageFallbackMode::Placeholder,
            },
            None,
        )
        .await
        .unwrap();

        assert!(session.messages[0].content_parts.is_some());
        assert!(llm_messages[0].content_parts.is_none());
        assert!(llm_messages[0]
            .content
            .contains("[Image omitted: bamboo-attachment://s1/a1]"));
    }

    /// Regression test: tool calls executed inside the agent loop MUST receive a ToolExecutionContext
    /// with `session_id=Some(...)`. This is required by server-only tools like `spawn_session`.
    #[tokio::test]
    async fn agent_loop_passes_session_id_into_tool_execution_context() {
        struct QueueProvider {
            // Each `chat_stream` call pops one pre-baked stream.
            queue: Mutex<Vec<Vec<crate::agent::llm::provider::Result<LLMChunk>>>>,
        }

        #[async_trait]
        impl LLMProvider for QueueProvider {
            async fn chat_stream(
                &self,
                _messages: &[Message],
                _tools: &[crate::agent::core::tools::ToolSchema],
                _max_output_tokens: Option<u32>,
                _model: &str,
            ) -> crate::agent::llm::provider::Result<LLMStream> {
                let mut guard = self.queue.lock().await;
                if guard.is_empty() {
                    panic!("test provider queue exhausted");
                }
                let items = guard.remove(0);
                Ok(Box::pin(stream::iter(items)))
            }
        }

        struct SessionIdRequiredTool {
            seen_session_id: Arc<Mutex<Option<String>>>,
        }

        #[async_trait]
        impl Tool for SessionIdRequiredTool {
            fn name(&self) -> &str {
                // Use the exact name we rely on in production.
                "spawn_session"
            }

            fn description(&self) -> &str {
                "test tool that requires session_id in ToolExecutionContext"
            }

            fn parameters_schema(&self) -> serde_json::Value {
                serde_json::json!({
                    "type": "object",
                    "properties": {
                        "goal": { "type": "string" }
                    },
                    "required": ["goal"]
                })
            }

            async fn execute(&self, _args: serde_json::Value) -> Result<ToolResult, ToolError> {
                // This tool is expected to be executed via `execute_with_context`.
                Err(ToolError::Execution(
                    "spawn_session test tool must be executed with context".to_string(),
                ))
            }

            async fn execute_with_context(
                &self,
                _args: serde_json::Value,
                ctx: ToolExecutionContext<'_>,
            ) -> Result<ToolResult, ToolError> {
                let Some(session_id) = ctx.session_id else {
                    return Err(ToolError::Execution(
                        "missing session_id in tool context".to_string(),
                    ));
                };

                *self.seen_session_id.lock().await = Some(session_id.to_string());

                Ok(ToolResult {
                    success: true,
                    result: "ok".to_string(),
                    display_preference: None,
                })
            }
        }

        let seen_session_id = Arc::new(Mutex::new(None));
        let tools = BuiltinToolExecutorBuilder::new()
            .with_tool(SessionIdRequiredTool {
                seen_session_id: seen_session_id.clone(),
            })
            .expect("register test tool")
            .build();

        let tool_call = crate::agent::core::tools::ToolCall {
            id: "call_spawn".to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: "spawn_session".to_string(),
                arguments: r#"{"goal":"do it"}"#.to_string(),
            },
        };

        let provider = Arc::new(QueueProvider {
            queue: Mutex::new(vec![
                vec![Ok(LLMChunk::ToolCalls(vec![tool_call])), Ok(LLMChunk::Done)],
                vec![Ok(LLMChunk::Token("done".to_string())), Ok(LLMChunk::Done)],
            ]),
        });

        let mut session = Session::new("session-ctx-test", "ignored");

        let (event_tx, _event_rx) = mpsc::channel(64);
        let config = AgentLoopConfig {
            max_rounds: 3,
            system_prompt: Some("sys".to_string()),
            model_name: Some("test-model".to_string()),
            ..Default::default()
        };

        super::run_agent_loop_with_config(
            &mut session,
            "hello".to_string(),
            event_tx,
            provider,
            Arc::new(tools),
            CancellationToken::new(),
            config,
        )
        .await
        .expect("agent loop should succeed");

        assert_eq!(
            seen_session_id.lock().await.clone(),
            Some("session-ctx-test".to_string())
        );
    }

    #[test]
    fn merge_system_prompt_with_contexts_appends_both_contexts() {
        let merged = merge_system_prompt_with_contexts(
            "You are a helpful assistant.",
            "\n\n## Available Skills\n\n### Skill\nDetails",
            "## Tool Usage Guidelines\n\n### File Reading Tools\nDetails",
        );
        assert!(merged.starts_with("You are a helpful assistant."));
        assert!(merged.contains("## Available Skills"));
        assert!(merged.contains("## Tool Usage Guidelines"));
    }

    #[test]
    fn merge_system_prompt_with_contexts_handles_empty_base_prompt() {
        let merged = merge_system_prompt_with_contexts(
            "",
            "\n\n## Available Skills\n\n### Skill",
            "## Tool Usage Guidelines\n\n### File Reading Tools",
        );
        assert_eq!(
            merged,
            "## Available Skills\n\n### Skill\n\n## Tool Usage Guidelines\n\n### File Reading Tools"
        );
    }

    #[test]
    fn strip_existing_skill_context_removes_previous_section() {
        let stripped = strip_existing_skill_context(
            "Base prompt\n\n## Available Skills\n\n### One\nInstructions",
        );
        assert_eq!(stripped, "Base prompt");
    }

    #[test]
    fn strip_existing_tool_guide_context_removes_previous_section() {
        let stripped = strip_existing_tool_guide_context(
            "Base prompt\n\n## Tool Usage Guidelines\n\n### File Reading Tools\nInstructions",
        );
        assert_eq!(stripped, "Base prompt");
    }

    // ========== MODEL REQUIREMENT ARCHITECTURE TESTS ==========
    // These tests ensure the design principle:
    // "Agent loop must use config.model_name, not session.model"

    /// Test: AgentLoopConfig.model_name defaults to None
    #[test]
    fn agent_loop_config_model_name_defaults_to_none() {
        let config = AgentLoopConfig::default();
        assert!(
            config.model_name.is_none(),
            "model_name should default to None, forcing explicit setting"
        );
    }

    /// Test: AgentLoopConfig can have model_name set
    #[test]
    fn agent_loop_config_can_set_model_name() {
        let config = AgentLoopConfig {
            model_name: Some("kimi-for-coding".to_string()),
            ..Default::default()
        };
        assert_eq!(config.model_name, Some("kimi-for-coding".to_string()));
    }

    /// Test: Model must be extracted from config, not session
    /// This test documents the requirement that model comes from config.model_name
    #[test]
    fn model_must_come_from_config_not_session() {
        use crate::agent::core::Session;

        // Create a config with model
        let config = AgentLoopConfig {
            model_name: Some("config-model".to_string()),
            ..Default::default()
        };

        // Create a session with a different model (just for recording)
        let session = Session::new("test", "session-model");

        // The model used for execution should come from config, not session
        let execution_model = config.model_name.as_deref().unwrap();
        assert_eq!(
            execution_model, "config-model",
            "Model must come from config.model_name, not session.model"
        );

        // session.model is different (just for recording)
        assert_eq!(
            session.model, "session-model",
            "session.model is just for recording, not execution"
        );
    }
}
