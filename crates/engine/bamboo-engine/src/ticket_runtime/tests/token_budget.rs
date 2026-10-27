//! Real FileStore + canonical SessionStore + actual activation admission.
//! The scripted provider checks the production decorator, without paid calls.
//! This is not evidence that the full Native Worker has run; its Host test is separate.
use super::*;
use crate::{runtime::config::AgentLoopConfig, ticket_token_budget::NativeWorkTokenBudget};
use bamboo_llm::{provider::ProviderModelInfo, LLMChunk, LLMError, LLMProvider, LLMStream};
use bamboo_tickets::{ExecutionTokenUsage, WorkTokenLimits};
use futures::StreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};

mod terminal;

enum Script {
    Known,
    KnownText,
    DoneError,
    DoneOverCap,
    DonePending,
    RepeatedDone,
    NoDone,
    Partial,
    BootstrapError,
    StreamError,
    Pending,
    OverCap,
    PartialOverCap,
    InconsistentOverCap,
    LegacyCachedInputOverCap,
    LegacyCompositeInputOverCap,
    LegacyOutputOverCap,
    LegacyThinkingOverCap,
    LegacyOverflowOverCap,
    LegacyWithinCap,
    MixedKnown,
    CanonicalCacheOnlyOverCap,
    CanonicalReasoningOnlyOverCap,
    CanonicalTotalOnlyOverCap,
    LegacySplitInputOverCap,
    CanonicalSplitCacheOverCap,
    SplitTotalMismatch,
}
struct Provider {
    script: Script,
    calls: AtomicUsize,
}
impl Provider {
    fn new(script: Script) -> Arc<Self> {
        Arc::new(Self {
            script,
            calls: AtomicUsize::new(0),
        })
    }
}
fn usage(input: Option<u64>, output: Option<u64>) -> LLMChunk {
    LLMChunk::ProviderUsage {
        input_tokens: input,
        output_tokens: output,
        total_tokens: None,
        reasoning_tokens: Some(5),
        cache_creation_input_tokens: None,
        cache_read_input_tokens: Some(10),
        cache_write_input_tokens: None,
    }
}
#[async_trait::async_trait]
impl LLMProvider for Provider {
    async fn vision_support_override(&self, model: &str) -> Option<bool> {
        (model == "auxiliary-model").then_some(true)
    }
    async fn list_model_info(&self) -> bamboo_llm::provider::Result<Vec<ProviderModelInfo>> {
        Ok(vec![
            ProviderModelInfo {
                id: "fixture-model".into(),
                max_context_tokens: Some(100),
                max_output_tokens: Some(40),
            },
            ProviderModelInfo {
                id: "auxiliary-model".into(),
                max_context_tokens: Some(8192),
                max_output_tokens: Some(2048),
            },
        ])
    }
    async fn chat_stream(
        &self,
        _: &[bamboo_domain::Message],
        _: &[bamboo_domain::ToolSchema],
        cap: Option<u32>,
        model: &str,
    ) -> bamboo_llm::provider::Result<LLMStream> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if model == "auxiliary-model" {
            assert!(cap.is_some_and(|cap| cap > 0 && cap <= 2048));
        } else {
            assert_eq!(cap, Some(40));
        }
        let mut complete = usage(Some(20), Some(u64::from(cap.unwrap_or(40).min(10))));
        if let LLMChunk::ProviderUsage {
            reasoning_tokens,
            cache_read_input_tokens,
            ..
        } = &mut complete
        {
            *reasoning_tokens = None;
            *cache_read_input_tokens = None;
        }
        let chunks = match self.script {
            Script::KnownText => vec![
                Ok(LLMChunk::Token("Fixture result".into())),
                Ok(complete.clone()),
                Ok(LLMChunk::Done),
                Ok(LLMChunk::Token("FORBIDDEN_LATE_TEXT".into())),
                Ok(LLMChunk::ReasoningToken("FORBIDDEN_LATE_REASONING".into())),
                Ok(LLMChunk::ReasoningSignature(
                    "FORBIDDEN_LATE_SIGNATURE".into(),
                )),
                Ok(LLMChunk::ResponseId("FORBIDDEN_LATE_ID".into())),
                Ok(LLMChunk::ResponsesEvent {
                    event_type: "late".into(),
                    data: Box::new(serde_json::json!({"late":true})),
                }),
                Ok(LLMChunk::ToolCalls(vec![])),
                Ok(complete),
                Ok(LLMChunk::Done),
            ],
            Script::DoneError => vec![
                Ok(complete),
                Ok(LLMChunk::Done),
                Err(LLMError::Stream("late failure".into())),
            ],
            Script::DoneOverCap => vec![
                Ok(complete),
                Ok(LLMChunk::Done),
                Ok(usage(Some(101), Some(10))),
            ],
            Script::NoDone => vec![
                Ok(LLMChunk::Token("Incomplete response".into())),
                Ok(complete),
            ],
            Script::DonePending | Script::RepeatedDone => {
                let prefix = futures::stream::iter(vec![
                    Ok(LLMChunk::Token("Fixture result".into())),
                    Ok(complete),
                    Ok(LLMChunk::Done),
                ]);
                return if matches!(self.script, Script::DonePending) {
                    Ok(Box::pin(prefix.chain(futures::stream::pending())))
                } else {
                    Ok(Box::pin(prefix.chain(futures::stream::repeat_with(|| {
                        Ok(LLMChunk::Done)
                    }))))
                };
            }
            Script::BootstrapError => {
                return Err(LLMError::Api("controlled bootstrap error".into()))
            }
            Script::Pending => return std::future::pending().await,
            Script::StreamError => vec![
                Ok(usage(Some(20), Some(10))),
                Err(LLMError::Stream("controlled stream error".into())),
            ],
            Script::Partial => vec![Ok(LLMChunk::Done), Ok(usage(None, Some(10)))],
            Script::OverCap => vec![Ok(usage(Some(101), Some(10)))],
            Script::PartialOverCap => vec![Ok(usage(Some(101), None))],
            Script::LegacySplitInputOverCap => vec![
                Ok(LLMChunk::CacheUsage {
                    input_tokens: 80,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 10,
                }),
                Ok(LLMChunk::CacheUsage {
                    input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 40,
                }),
            ],
            Script::CanonicalSplitCacheOverCap => {
                let mut first = usage(None, None);
                let mut second = usage(None, None);
                if let LLMChunk::ProviderUsage {
                    cache_creation_input_tokens,
                    ..
                } = &mut first
                {
                    *cache_creation_input_tokens = Some(80);
                }
                if let LLMChunk::ProviderUsage {
                    cache_read_input_tokens,
                    ..
                } = &mut second
                {
                    *cache_read_input_tokens = Some(40);
                }
                vec![Ok(first), Ok(second)]
            }
            Script::SplitTotalMismatch => {
                let mut total = usage(None, None);
                if let LLMChunk::ProviderUsage { total_tokens, .. } = &mut total {
                    *total_tokens = Some(100);
                }
                vec![Ok(usage(Some(20), Some(10))), Ok(LLMChunk::Done), Ok(total)]
            }
            Script::CanonicalCacheOnlyOverCap
            | Script::CanonicalReasoningOnlyOverCap
            | Script::CanonicalTotalOnlyOverCap => {
                let mut chunk = usage(None, None);
                if let LLMChunk::ProviderUsage {
                    cache_read_input_tokens,
                    reasoning_tokens,
                    total_tokens,
                    ..
                } = &mut chunk
                {
                    match self.script {
                        Script::CanonicalCacheOnlyOverCap => *cache_read_input_tokens = Some(101),
                        Script::CanonicalReasoningOnlyOverCap => *reasoning_tokens = Some(41),
                        Script::CanonicalTotalOnlyOverCap => *total_tokens = Some(141),
                        _ => unreachable!(),
                    }
                }
                vec![Ok(chunk)]
            }
            Script::LegacyCachedInputOverCap => vec![Ok(LLMChunk::CacheUsage {
                input_tokens: 0,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 101,
            })],
            Script::LegacyCompositeInputOverCap => vec![Ok(LLMChunk::CacheUsage {
                input_tokens: 40,
                cache_creation_input_tokens: 40,
                cache_read_input_tokens: 40,
            })],
            Script::LegacyOverflowOverCap => vec![Ok(LLMChunk::CacheUsage {
                input_tokens: u64::MAX,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 1,
            })],
            Script::LegacyOutputOverCap => vec![Ok(LLMChunk::UsageSummary {
                output_tokens: 41,
                thinking_tokens: 0,
            })],
            Script::LegacyThinkingOverCap => vec![Ok(LLMChunk::UsageSummary {
                output_tokens: 0,
                thinking_tokens: 41,
            })],
            Script::LegacyWithinCap => vec![
                Ok(LLMChunk::CacheUsage {
                    input_tokens: 60,
                    cache_creation_input_tokens: 10,
                    cache_read_input_tokens: 30,
                }),
                Ok(LLMChunk::UsageSummary {
                    output_tokens: 40,
                    thinking_tokens: 5,
                }),
                Ok(LLMChunk::Done),
            ],
            Script::MixedKnown => {
                let mut canonical = usage(Some(20), Some(10));
                if let LLMChunk::ProviderUsage {
                    cache_creation_input_tokens,
                    cache_read_input_tokens,
                    ..
                } = &mut canonical
                {
                    *cache_creation_input_tokens = Some(10);
                    *cache_read_input_tokens = Some(5);
                }
                vec![
                    Ok(LLMChunk::CacheUsage {
                        input_tokens: 5,
                        cache_creation_input_tokens: 10,
                        cache_read_input_tokens: 5,
                    }),
                    Ok(LLMChunk::UsageSummary {
                        output_tokens: 10,
                        thinking_tokens: 5,
                    }),
                    Ok(canonical),
                    Ok(LLMChunk::Done),
                ]
            }
            Script::InconsistentOverCap => {
                let mut chunk = usage(Some(101), Some(10));
                if let LLMChunk::ProviderUsage { total_tokens, .. } = &mut chunk {
                    *total_tokens = Some(1);
                }
                vec![Ok(chunk)]
            }
            Script::Known => vec![
                Ok(LLMChunk::Done),
                Ok(usage(Some(20), Some(10))),
                Ok(LLMChunk::Done),
            ],
        };
        Ok(Box::pin(futures::stream::iter(chunks)))
    }
}

async fn admitted() -> (Fixture, crate::session_activation::SessionRunRegistration) {
    let mut f = Fixture::with_project(true).await;
    let registration = f
        .runtime
        .router
        .register_run(&f.child.id, "budget-host-run")
        .await
        .unwrap();
    admit_ticket_run(&f.service, &f.runtime, &mut f.child, "budget-host-run")
        .await
        .unwrap();
    (f, registration)
}
fn configure(f: &Fixture, cap: u64) {
    let user = Authority::from_verified_host(
        f.spec.binding.clone(),
        Principal::User {
            user_id: "budget-owner".into(),
        },
    );
    f.service
        .configure_work_token_budget(
            &user,
            0,
            WorkTokenLimits {
                work_tokens: std::collections::BTreeMap::from([(f.spec.work_id.clone(), cap)]),
                utc_day_tokens: cap,
                total_tokens: cap,
            },
        )
        .unwrap();
}
fn ledger(f: &Fixture) -> bamboo_tickets::WorkTokenBudget {
    f.service
        .work_token_budget(&Authority::from_verified_host(
            f.spec.binding.clone(),
            Principal::Runtime,
        ))
        .unwrap()
        .unwrap()
}
async fn provider(f: &Fixture, inner: Arc<Provider>) -> Arc<dyn LLMProvider> {
    NativeWorkTokenBudget::capture(Some(f.service.clone()), f.storage.clone(), &f.child)
        .await
        .unwrap()
        .unwrap()
        .wrap(
            inner,
            &AgentLoopConfig {
                app_data_dir: Some(f.dir.path().to_path_buf()),
                ..Default::default()
            },
        )
}

#[tokio::test]
async fn token_budget_reserves_before_provider_and_settles_only_after_trailing_usage_eof() {
    let (f, _registration) = admitted().await;
    configure(&f, 140);
    let inner = Provider::new(Script::Known);
    let decorated = provider(&f, inner.clone()).await;
    assert_eq!(
        decorated.resource_allocation_identity(),
        Some(Arc::as_ptr(&inner) as *const () as usize)
    );
    let mut stream = decorated
        .chat_stream(&[], &[], None, "fixture-model")
        .await
        .unwrap();
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger(&f).total_charged, 140);
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        LLMChunk::ProviderUsage { .. }
    ));
    assert_eq!(ledger(&f).total_charged, 140);
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        LLMChunk::Done
    ));
    // Existing auxiliary consumers stop here; their charge is already settled.
    let budget = ledger(&f);
    assert_eq!(budget.total_charged, 30);
    let final_seq = f.service.published().unwrap().1.seq;
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
    assert_eq!(f.service.published().unwrap().1.seq, final_seq);
    let record = budget.reservations.values().next().unwrap();
    assert_eq!(
        record.request.attempt.as_ref().unwrap().assignment_id,
        f.spec.assignment_id
    );
    assert_eq!(record.request.project_id.as_str(), "budget-fixture-project");
    assert_eq!(
        record.usage,
        Some(ExecutionTokenUsage::Known {
            input_tokens: 20,
            output_tokens: 10
        })
    );
    assert!(decorated
        .chat_stream(&[], &[], None, "fixture-model")
        .await
        .is_err());
    assert_eq!(
        inner.calls.load(Ordering::SeqCst),
        1,
        "exhaustion blocks actual provider entry"
    );
}

#[tokio::test]
async fn token_budget_missing_error_cancel_and_dropped_stream_keep_the_full_charge() {
    for script in [
        Script::Partial,
        Script::BootstrapError,
        Script::StreamError,
        Script::Pending,
        Script::Known,
    ] {
        let (f, _registration) = admitted().await;
        configure(&f, 140);
        let inner = Provider::new(script);
        let decorated = provider(&f, inner.clone()).await;
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            decorated.chat_stream(&[], &[], None, "fixture-model"),
        )
        .await;
        if let Ok(Ok(mut stream)) = result {
            if matches!(inner.script, Script::Known) {
                drop(stream);
            } else {
                while let Some(chunk) = stream.next().await {
                    if chunk.is_err() {
                        break;
                    }
                }
            }
        }
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(ledger(&f).total_charged, 140);
        assert!(decorated
            .chat_stream(&[], &[], None, "fixture-model")
            .await
            .is_err());
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn token_budget_over_cap_report_halts_subsequent_provider_entry() {
    for script in [
        Script::OverCap,
        Script::PartialOverCap,
        Script::InconsistentOverCap,
        Script::LegacyCachedInputOverCap,
        Script::LegacyCompositeInputOverCap,
        Script::LegacyOutputOverCap,
        Script::LegacyThinkingOverCap,
        Script::LegacyOverflowOverCap,
        Script::CanonicalCacheOnlyOverCap,
        Script::CanonicalReasoningOnlyOverCap,
        Script::CanonicalTotalOnlyOverCap,
        Script::LegacySplitInputOverCap,
        Script::CanonicalSplitCacheOverCap,
    ] {
        let (f, _registration) = admitted().await;
        configure(&f, 500);
        let inner = Provider::new(script);
        let decorated = provider(&f, inner.clone()).await;
        let mut stream = decorated
            .chat_stream(&[], &[], None, "fixture-model")
            .await
            .unwrap();
        let mut saw_error = false;
        while let Some(chunk) = stream.next().await {
            if chunk.is_err() {
                saw_error = true;
                break;
            }
        }
        assert!(
            saw_error,
            "overrun must fail while consuming its real stream"
        );
        let budget = ledger(&f);
        assert_eq!(budget.total_charged, 140);
        assert!(budget
            .reservations
            .values()
            .next()
            .unwrap()
            .over_cap_usage
            .is_some());
        assert!(decorated
            .chat_stream(&[], &[], None, "fixture-model")
            .await
            .is_err());
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn token_budget_incomplete_or_inconsistent_usage_keeps_caps_without_double_counting() {
    for (script, charged, known) in [
        (Script::LegacyWithinCap, 140, false),
        (Script::MixedKnown, 30, true),
        (Script::SplitTotalMismatch, 140, false),
    ] {
        let (f, _registration) = admitted().await;
        configure(&f, 500);
        let decorated = provider(&f, Provider::new(script)).await;
        let mut stream = decorated
            .chat_stream(&[], &[], None, "fixture-model")
            .await
            .unwrap();
        while let Some(chunk) = stream.next().await {
            chunk.unwrap();
        }
        let budget = ledger(&f);
        assert_eq!(budget.total_charged, charged);
        let receipt = budget.reservations.values().next().unwrap();
        assert!(receipt.over_cap_usage.is_none());
        assert_eq!(
            matches!(receipt.usage, Some(ExecutionTokenUsage::Known { .. })),
            known
        );
    }
}

#[tokio::test]
async fn token_budget_dynamic_auxiliary_and_vision_cannot_escape_shared_exhaustion() {
    use crate::runtime::config::AuxiliaryModelConfig;
    let (f, _registration) = admitted().await;
    configure(&f, 140);
    let inner = Provider::new(Script::Known);
    let budget =
        NativeWorkTokenBudget::capture(Some(f.service.clone()), f.storage.clone(), &f.child)
            .await
            .unwrap()
            .unwrap();
    let dynamic: Arc<dyn LLMProvider> = inner.clone();
    let vision: Arc<dyn LLMProvider> = inner.clone();
    let mut config = AgentLoopConfig {
        app_data_dir: Some(f.dir.path().to_path_buf()),
        fast_model_provider: Some(inner.clone()),
        summarization_model_provider: Some(inner.clone()),
        auxiliary_model_resolver: Some(Arc::new(move || AuxiliaryModelConfig {
            background_model_provider: Some(dynamic.clone()),
            ..Default::default()
        })),
        vision_model_resolver: Some(Arc::new(move || {
            let vision = vision.clone();
            Box::pin(async move { Ok(bamboo_llm::ResolvedModel::new(vision, "fixture-model")) })
        })),
        ..Default::default()
    };
    let main = budget.decorate(&mut config, inner.clone());
    drop(
        main.chat_stream(&[], &[], None, "fixture-model")
            .await
            .unwrap(),
    );
    let late_aux = config.auxiliary_model_resolver.as_ref().unwrap()();
    let late_vision = config.vision_model_resolver.as_ref().unwrap()()
        .await
        .unwrap();
    for provider in [
        config.fast_model_provider.unwrap(),
        config.summarization_model_provider.unwrap(),
        late_aux.background_model_provider.unwrap(),
        late_vision.provider,
    ] {
        assert!(provider
            .chat_stream(&[], &[], None, "fixture-model")
            .await
            .is_err());
    }
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    assert_eq!(ledger(&f).total_charged, 140);
}

#[tokio::test]
async fn token_budget_unknown_model_and_revoked_root_block_before_provider() {
    let (f, _registration) = admitted().await;
    configure(&f, 500);
    let inner = Provider::new(Script::Known);
    let decorated = provider(&f, inner.clone()).await;
    assert!(decorated
        .chat_stream(&[], &[], None, "unknown-budget-model")
        .await
        .is_err());
    assert!(decorated
        .chat_stream(&[], &[], Some(41), "fixture-model")
        .await
        .is_err());
    let root = f
        .storage
        .load_root_authority(&f.supervisor.id)
        .await
        .unwrap()
        .unwrap();
    assert!(f.storage.delete_session(&root.id).await.unwrap());
    let replacement = f
        .storage
        .get_or_create_default_supervisor("replacement")
        .await
        .unwrap();
    assert_ne!(
        root.supervisor_incarnation_id().unwrap(),
        replacement.incarnation_id
    );
    assert!(decorated
        .chat_stream(&[], &[], None, "fixture-model")
        .await
        .is_err());
    assert_eq!(inner.calls.load(Ordering::SeqCst), 0);
    assert_eq!(ledger(&f).total_charged, 0);
}

#[tokio::test]
async fn token_budget_native_descendant_uses_original_work_and_rejects_lost_ancestry() {
    let (f, _registration) = admitted().await;
    configure(&f, 500);
    let mut descendant = Session::new_child_of(
        "native-grandchild",
        &f.child,
        "fixture-model",
        "nested work",
    );
    f.storage.save_session(&descendant).await.unwrap();
    let budget =
        NativeWorkTokenBudget::capture(Some(f.service.clone()), f.storage.clone(), &descendant)
            .await
            .unwrap()
            .unwrap();
    let inner = Provider::new(Script::Known);
    let decorated = budget.wrap(
        inner.clone(),
        &AgentLoopConfig {
            app_data_dir: Some(f.dir.path().to_path_buf()),
            ..Default::default()
        },
    );
    let mut stream = decorated
        .chat_stream(&[], &[], None, "fixture-model")
        .await
        .unwrap();
    while let Some(chunk) = stream.next().await {
        chunk.unwrap();
    }
    assert_eq!(ledger(&f).total_charged, 30);
    descendant.parent_session_id = Some(f.supervisor.id.clone());
    // Storage already refuses reparenting. Loss of the original canonical
    // child independently closes the provider capability without a bypass.
    assert!(f.storage.save_session(&descendant).await.is_err());
    f.storage.delete_session(&descendant.id).await.unwrap();
    assert!(decorated
        .chat_stream(&[], &[], None, "fixture-model")
        .await
        .is_err());
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn token_budget_unconfigured_ordinary_child_preserves_legacy_and_later_config_fails_closed() {
    let f = Fixture::with_project(true).await;
    let mut root = f
        .storage
        .load_root_authority(&f.supervisor.id)
        .await
        .unwrap()
        .unwrap();
    root.set_root_orchestration_only(false).unwrap();
    f.storage.save_session(&root).await.unwrap();
    let child = Session::new_child_of(
        "ordinary-no-budget",
        &root,
        "fixture-model",
        "ordinary child",
    );
    f.storage.save_session(&child).await.unwrap();
    let capability =
        NativeWorkTokenBudget::capture(Some(f.service.clone()), f.storage.clone(), &child)
            .await
            .unwrap()
            .unwrap();
    let inner = Provider::new(Script::Known);
    let decorated = capability.wrap(
        inner.clone(),
        &AgentLoopConfig {
            app_data_dir: Some(f.dir.path().to_path_buf()),
            ..Default::default()
        },
    );
    let mut stream = decorated
        .chat_stream(&[], &[], Some(40), "fixture-model")
        .await
        .unwrap();
    while let Some(chunk) = stream.next().await {
        chunk.unwrap();
    }
    assert!(f.service.published().unwrap().1.execution_budget.is_none());
    assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
    configure(&f, 500);
    assert!(decorated
        .chat_stream(&[], &[], Some(40), "fixture-model")
        .await
        .is_err());
    assert_eq!(
        inner.calls.load(Ordering::SeqCst),
        1,
        "late configuration cannot escape the retained capability"
    );
    assert_eq!(ledger(&f).total_charged, 0);
}
