use super::*;
use bamboo_domain::{CapabilityLoadingMode, Message, ToolSchema};
use bamboo_llm::{
    prompt_ir::PromptIR,
    provider::{
        LLMError, LLMProvider, LLMRequestOptions, LLMStream, ProviderModelInfo,
        ProviderVisibleToolFootprint,
    },
    types::LLMChunk,
};
use bamboo_tickets::{
    ExecutionBudgetPurpose, ExecutionTokenOverCapUsage, ExecutionTokenUsage, WorkTokenAdmission,
    WorkTokenAttempt, WorkTokenRequest, WorkTokenReservation,
};
use futures::StreamExt;

use crate::runtime::config::{AgentLoopConfig, AuxiliaryModelConfig};

fn denied(error: impl std::fmt::Display) -> LLMError {
    // The existing runner must retain the accounting boundary, rather than
    // retrying it as a transient transport/bootstrap failure.
    LLMError::RequestFailed(format!("Work token budget: {error}"))
}

struct BudgetedProvider {
    inner: Arc<dyn LLMProvider>,
    budget: Arc<NativeWorkTokenBudget>,
    limits: Arc<AgentLoopConfig>,
}

struct Admission {
    budget: Arc<NativeWorkTokenBudget>,
    authority: Authority,
    record: WorkTokenReservation,
    transport: bamboo_llm::retry::WorkPhysicalRequest,
}

impl Admission {
    fn over_cap(&self, report: ExecutionTokenOverCapUsage) -> bamboo_llm::provider::Result<()> {
        self.budget
            .service
            .report_work_token_over_cap(
                &self.authority,
                &self.record.request.operation_id,
                &self.record.request.request_hash,
                &self.record.receipt,
                report,
            )
            .map(|_| ())
            .map_err(denied)
    }
    fn settle(&self, usage: ExecutionTokenUsage) -> bamboo_llm::provider::Result<()> {
        self.budget
            .service
            .settle_work_tokens(
                &self.authority,
                &self.record.request.operation_id,
                &self.record.request.request_hash,
                &self.record.receipt,
                usage,
            )
            .map(|_| ())
            .map_err(denied)
    }
}

impl NativeWorkTokenBudget {
    pub(crate) fn decorate(
        self: &Arc<Self>,
        loop_config: &mut AgentLoopConfig,
        llm: Arc<dyn LLMProvider>,
    ) -> Arc<dyn LLMProvider> {
        let budget = self;
        let limits = Arc::new(AgentLoopConfig {
            app_data_dir: loop_config.app_data_dir.clone(),
            model_limits_section: loop_config.model_limits_section.clone(),
            legacy_model_limits: loop_config.legacy_model_limits.clone(),
            ..Default::default()
        });
        for provider in [
            &mut loop_config.fast_model_provider,
            &mut loop_config.background_model_provider,
            &mut loop_config.summarization_model_provider,
        ] {
            if let Some(inner) = provider.take() {
                *provider = Some(budget.wrap(inner, &limits));
            }
        }
        if let Some(resolve) = loop_config.auxiliary_model_resolver.take() {
            let budget = budget.clone();
            let limits = limits.clone();
            loop_config.auxiliary_model_resolver = Some(Arc::new(move || {
                let mut models = resolve();
                budget.wrap_auxiliary(&mut models, &limits);
                models
            }));
        }
        if let Some(resolve) = loop_config.vision_model_resolver.take() {
            let budget = budget.clone();
            let limits = limits.clone();
            loop_config.vision_model_resolver = Some(Arc::new(move || {
                let future = resolve();
                let budget = budget.clone();
                let limits = limits.clone();
                Box::pin(async move {
                    let mut selected = future.await?;
                    selected.provider = budget.wrap(selected.provider, &limits);
                    Ok(selected)
                })
            }));
        }
        budget.wrap(llm, &limits)
    }

    pub(crate) fn wrap(
        self: &Arc<Self>,
        inner: Arc<dyn LLMProvider>,
        config: &AgentLoopConfig,
    ) -> Arc<dyn LLMProvider> {
        Arc::new(BudgetedProvider {
            inner,
            budget: self.clone(),
            // Do not capture provider/config callbacks in their own wrapper.
            limits: Arc::new(AgentLoopConfig {
                app_data_dir: config.app_data_dir.clone(),
                model_limits_section: config.model_limits_section.clone(),
                legacy_model_limits: config.legacy_model_limits.clone(),
                ..Default::default()
            }),
        })
    }

    pub(crate) fn wrap_auxiliary(
        self: &Arc<Self>,
        models: &mut AuxiliaryModelConfig,
        config: &AgentLoopConfig,
    ) {
        for provider in [
            &mut models.fast_model_provider,
            &mut models.background_model_provider,
            &mut models.summarization_model_provider,
        ] {
            if let Some(inner) = provider.take() {
                *provider = Some(self.wrap(inner, config));
            }
        }
    }
}

impl BudgetedProvider {
    async fn admit(
        &self,
        model: &str,
        output: Option<u32>,
        fingerprint: &str,
    ) -> bamboo_llm::provider::Result<Option<Admission>> {
        if self
            .budget
            .service
            .published()
            .map_err(denied)?
            .1
            .execution_budget
            .is_none()
        {
            return Ok(None);
        }
        let (input_cap, model_output) =
            crate::runtime::runner::round_lifecycle::resolve_known_token_ceilings(
                &self.limits,
                model,
                self.inner.as_ref(),
            )
            .await
            .ok_or_else(|| denied("configured or provider-reported model ceilings required"))?;
        let output_cap = output.unwrap_or(model_output);
        if output_cap == 0 || output_cap > model_output {
            return Err(denied(
                "request output ceiling exceeds the selected model's known limit",
            ));
        }
        let operation_id = format!("native-llm/{}", uuid::Uuid::new_v4());
        let request_hash = bamboo_tickets::content_hash(
            &bamboo_tickets::canonical_bytes(&(model, input_cap, output_cap, fingerprint))
                .map_err(denied)?,
        );
        for _ in 0..4 {
            let authority = self.budget.authority().await.map_err(denied)?;
            let snapshot = self.budget.service.published().map_err(denied)?.1;
            let dispatch = self.budget.dispatch.as_ref().expect("authority checked");
            let assignment = snapshot
                .assignments
                .get(&dispatch.assignment_id)
                .ok_or_else(|| denied("Assignment missing"))?;
            let project_id = snapshot
                .project
                .links
                .get(&assignment.work_id)
                .ok_or_else(|| denied("attributable Work Project required"))?
                .clone();
            let request = WorkTokenRequest {
                operation_id: operation_id.clone(),
                project_id,
                work_id: assignment.work_id.clone(),
                purpose: ExecutionBudgetPurpose::Worker,
                request_hash: request_hash.clone(),
                observed_seq: snapshot.seq,
                input_cap,
                output_cap,
                attempt: Some(WorkTokenAttempt {
                    assignment_id: assignment.id.clone(),
                    generation: assignment.generation,
                    authority_epoch: assignment.authority_epoch,
                }),
            };
            let now_ms = u64::try_from(chrono::Utc::now().timestamp_millis()).map_err(denied)?;
            match self
                .budget
                .service
                .reserve_work_tokens(&authority, &request, now_ms)
            {
                Ok(WorkTokenAdmission::Reserved(record)) => {
                    return Ok(Some(Admission {
                        budget: self.budget.clone(),
                        authority,
                        record,
                        transport: Default::default(),
                    }))
                }
                Ok(WorkTokenAdmission::Blocked(reason)) => {
                    return Err(denied(format!("admission blocked: {reason:?}")))
                }
                // Only this known-not-committed CAS conflict is retried. Each
                // retry revalidates canonical dispatch and Root authority.
                Err(Error::RevisionConflict) => continue,
                Err(error) => return Err(denied(error)),
            }
        }
        Err(denied("Ticket accounting changed during bounded admission"))
    }

    fn meter(stream: LLMStream, admission: Option<Admission>) -> LLMStream {
        let Some(admission) = admission else {
            return stream;
        };
        Box::pin(futures::stream::unfold(
            (stream, Some(admission), Usage::default()),
            |(mut stream, mut admission, mut usage)| async move {
                let receipt = admission.as_ref()?;
                match receipt.transport.scope(stream.next()).await {
                    Some(Ok(chunk)) => {
                        usage.observe(&chunk);
                        if usage
                            .input
                            .is_some_and(|v| v > u64::from(receipt.record.request.input_cap))
                            || usage
                                .output
                                .is_some_and(|v| v > u64::from(receipt.record.request.output_cap))
                        {
                            let error = receipt
                                .over_cap(ExecutionTokenOverCapUsage {
                                    input_tokens: usage.input,
                                    output_tokens: usage.output,
                                })
                                .err()
                                .unwrap_or_else(|| {
                                    denied("provider exceeded its reserved ceilings")
                                });
                            admission = None;
                            return Some((Err(error), (stream, admission, usage)));
                        }
                        Some((Ok(chunk), (stream, admission, usage)))
                    }
                    Some(Err(error)) => {
                        // Transport/protocol failure retains the complete cap.
                        admission = None;
                        Some((Err(error), (stream, admission, usage)))
                    }
                    None => {
                        // Done can precede trailing usage. Only EOF completes a
                        // successful meter; dropping/cancelling retains None/full.
                        let measured = if usage.done {
                            usage.measured().unwrap_or(ExecutionTokenUsage::Unknown)
                        } else {
                            ExecutionTokenUsage::Unknown
                        };
                        match receipt.settle(measured) {
                            Ok(()) => None,
                            Err(error) => {
                                admission = None;
                                Some((Err(error), (stream, admission, usage)))
                            }
                        }
                    }
                }
            },
        ))
    }
}

#[derive(Default)]
struct Usage {
    input: Option<u64>,
    output: Option<u64>,
    done: bool,
    inconsistent: bool,
}
impl Usage {
    fn observe(&mut self, chunk: &LLMChunk) {
        match chunk {
            LLMChunk::Done => self.done = true,
            LLMChunk::ProviderUsage {
                input_tokens,
                output_tokens,
                total_tokens,
                ..
            } => {
                if let (Some(input), Some(output), Some(total)) =
                    (input_tokens, output_tokens, total_tokens)
                {
                    self.inconsistent |= input.checked_add(*output) != Some(*total);
                }
                if let Some(input) = input_tokens {
                    self.input = Some(self.input.unwrap_or(0).max(*input));
                }
                if let Some(output) = output_tokens {
                    self.output = Some(self.output.unwrap_or(0).max(*output));
                }
                // Reasoning/cache are subsets of these canonical totals.
            }
            // Legacy Anthropic chunks lose optional-field presence in the
            // parser. Until it preserves it, they keep the full reservation.
            _ => {}
        }
    }
    fn measured(&self) -> Option<ExecutionTokenUsage> {
        if self.inconsistent {
            return None;
        }
        Some(ExecutionTokenUsage::Known {
            input_tokens: self.input?,
            output_tokens: self.output?,
        })
    }
}

#[async_trait::async_trait]
impl LLMProvider for BudgetedProvider {
    fn resource_allocation_identity(&self) -> Option<usize> {
        Some(
            self.inner
                .resource_allocation_identity()
                .unwrap_or_else(|| Arc::as_ptr(&self.inner) as *const () as usize),
        )
    }
    async fn supports_vision(&self, model: &str) -> bool {
        self.inner.supports_vision(model).await
    }
    async fn vision_support_override(&self, model: &str) -> Option<bool> {
        self.inner.vision_support_override(model).await
    }
    async fn capability_loading_mode(
        &self,
        model: &str,
        required: Option<&str>,
    ) -> CapabilityLoadingMode {
        self.inner.capability_loading_mode(model, required).await
    }
    async fn provider_visible_tool_footprint(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        model: &str,
        required: Option<&str>,
    ) -> bamboo_llm::provider::Result<ProviderVisibleToolFootprint> {
        self.inner
            .provider_visible_tool_footprint(ir, tools, model, required)
            .await
    }
    async fn list_models(&self) -> bamboo_llm::provider::Result<Vec<String>> {
        self.inner.list_models().await
    }
    async fn list_model_info(&self) -> bamboo_llm::provider::Result<Vec<ProviderModelInfo>> {
        self.inner.list_model_info().await
    }
    async fn chat_stream(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        output: Option<u32>,
        model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        self.chat_stream_with_options(messages, tools, output, model, None)
            .await
    }
    async fn chat_stream_with_options(
        &self,
        messages: &[Message],
        tools: &[ToolSchema],
        output: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        let admission = self
            .admit(
                model,
                output,
                &format!("messages:{messages:?};tools:{tools:?};options:{options:?}"),
            )
            .await?;
        let bounded_output = admission
            .as_ref()
            .map(|a| a.record.request.output_cap)
            .or(output);
        let request =
            self.inner
                .chat_stream_with_options(messages, tools, bounded_output, model, options);
        let stream = if let Some(receipt) = &admission {
            receipt.transport.scope(request).await
        } else {
            request.await
        }?;
        Ok(Self::meter(stream, admission))
    }
    async fn chat_stream_ir(
        &self,
        ir: &PromptIR,
        tools: &[ToolSchema],
        output: Option<u32>,
        model: &str,
        options: Option<&LLMRequestOptions>,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        let admission = self
            .admit(
                model,
                output,
                &format!("ir:{ir:?};tools:{tools:?};options:{options:?}"),
            )
            .await?;
        let bounded_output = admission
            .as_ref()
            .map(|a| a.record.request.output_cap)
            .or(output);
        // Preserve native IR and continuation/cache policy exactly.
        let request = self
            .inner
            .chat_stream_ir(ir, tools, bounded_output, model, options);
        let stream = if let Some(receipt) = &admission {
            receipt.transport.scope(request).await
        } else {
            request.await
        }?;
        Ok(Self::meter(stream, admission))
    }
}
