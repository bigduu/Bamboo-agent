//! Real canonical accounting at the terminal seen by the production consumers.
use super::*;
use std::time::Duration;

#[tokio::test]
async fn token_budget_summary_and_vision_stop_at_done_with_measured_charges() {
    use crate::llm_summarizer::{LlmSummarizer, SummaryRequestBudget};
    use crate::runtime::{
        config::{ImageFallbackConfig, ImageFallbackMode},
        runner::image_fallback::apply_image_fallback_to_llm_messages,
    };
    use bamboo_compression::Summarizer;
    use bamboo_domain::Message;
    use bamboo_llm::models::{ContentPart, ImageUrl};
    let (f, _registration) = admitted().await;
    configure(&f, 20_000);
    let inner = Provider::new(Script::KnownText);
    let decorated = provider(&f, inner.clone()).await;
    let summarizer = LlmSummarizer::new(decorated.clone(), "auxiliary-model".into(), None, None)
        .with_heuristic_fallback_on_error(false)
        .with_request_budget(SummaryRequestBudget {
            context_window_tokens: 8192,
            max_output_tokens: 40,
            safety_margin_tokens: 0,
            safe_window_percent: 90,
            target_summary_tokens: 32,
            target_ratio: 0.2,
        });
    let summary = summarizer
        .summarize(&[Message::user(
            "Keep the admitted Work goal and evidence. ".repeat(20),
        )])
        .await
        .unwrap();
    assert_eq!(summary, "Fixture result");
    assert!(inner.calls.load(Ordering::SeqCst) > 0);
    assert!(ledger(&f)
        .reservations
        .values()
        .all(|r| matches!(r.usage, Some(ExecutionTokenUsage::Known { .. }))));
    let mut images = vec![Message::user_with_parts(
        "Describe the fixture",
        vec![ContentPart::ImageUrl {
            image_url: ImageUrl {
                url: "data:image/png;base64,AAAA".into(),
                detail: None,
            },
        }
        .into()],
    )];
    apply_image_fallback_to_llm_messages(
        &mut images,
        ImageFallbackConfig {
            mode: ImageFallbackMode::Vision,
            vision_model: Some("auxiliary-model".into()),
        },
        None,
        Some(&decorated),
    )
    .await
    .unwrap();
    assert!(images[0].content.contains("Fixture result"));
    assert!(!images[0].content.contains("FORBIDDEN"));
    let budget = ledger(&f);
    assert_eq!(
        budget.reservations.len(),
        inner.calls.load(Ordering::SeqCst)
    );
    let measured: u64 = budget
        .reservations
        .values()
        .map(|r| match r.usage {
            Some(ExecutionTokenUsage::Known {
                input_tokens,
                output_tokens,
            }) => input_tokens + output_tokens,
            ref other => panic!("successful auxiliary call retained full caps: {other:?}"),
        })
        .sum();
    assert_eq!(budget.total_charged, measured);
    assert!(measured <= 30 * inner.calls.load(Ordering::SeqCst) as u64);
}

#[tokio::test]
async fn token_budget_terminal_is_unique_and_suppresses_late_semantic_frames() {
    let (f, _registration) = admitted().await;
    configure(&f, 140);
    let decorated = provider(&f, Provider::new(Script::KnownText)).await;
    let mut stream = decorated
        .chat_stream(&[], &[], None, "fixture-model")
        .await
        .unwrap();
    let mut text = String::new();
    let mut terminals = 0;
    while let Some(chunk) = stream.next().await {
        match chunk.unwrap() {
            LLMChunk::Token(value) => text.push_str(&value),
            LLMChunk::ProviderUsage { .. } => {}
            LLMChunk::Done => {
                terminals += 1;
                assert_eq!(ledger(&f).total_charged, 30);
            }
            other => panic!("late semantic metadata escaped: {other:?}"),
        }
    }
    assert_eq!(text, "Fixture result");
    assert_eq!(terminals, 1);
    let seq = f.service.published().unwrap().1.seq;
    assert!(stream.next().await.is_none());
    assert_eq!(f.service.published().unwrap().1.seq, seq);
}

#[tokio::test]
async fn token_budget_drop_after_provider_done_without_eof_keeps_full_charge() {
    let (f, _registration) = admitted().await;
    configure(&f, 140);
    let decorated = provider(&f, Provider::new(Script::Known)).await;
    let mut stream = decorated
        .chat_stream(&[], &[], None, "fixture-model")
        .await
        .unwrap();
    // Raw Done has already arrived, followed by this usage; no EOF was polled.
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        LLMChunk::ProviderUsage { .. }
    ));
    drop(stream);
    let budget = ledger(&f);
    assert_eq!(budget.total_charged, 140);
    assert!(budget.reservations.values().next().unwrap().usage.is_none());
    assert!(decorated
        .chat_stream(&[], &[], None, "fixture-model")
        .await
        .is_err());
}

#[tokio::test]
async fn token_budget_post_done_failure_and_missing_terminal_never_refund() {
    for script in [Script::DoneError, Script::DoneOverCap, Script::NoDone] {
        let (f, _registration) = admitted().await;
        configure(&f, 140);
        let inner = Provider::new(script);
        let decorated = provider(&f, inner.clone()).await;
        let mut stream = decorated
            .chat_stream(&[], &[], None, "fixture-model")
            .await
            .unwrap();
        while let Some(chunk) = stream.next().await {
            assert!(!matches!(chunk, Ok(LLMChunk::Done)));
            if chunk.is_err() {
                break;
            }
        }
        let budget = ledger(&f);
        assert_eq!(budget.total_charged, 140);
        let receipt = budget.reservations.values().next().unwrap();
        assert_eq!(
            receipt.over_cap_usage.is_some(),
            matches!(inner.script, Script::DoneOverCap)
        );
        assert!(decorated
            .chat_stream(&[], &[], None, "fixture-model")
            .await
            .is_err());
    }
}

#[tokio::test]
async fn token_budget_post_done_pending_has_a_finite_deadline() {
    for script in [Script::DonePending, Script::RepeatedDone] {
        let (f, _registration) = admitted().await;
        configure(&f, 140);
        let decorated = provider(&f, Provider::new(script)).await;
        let mut stream = decorated
            .chat_stream(&[], &[], None, "fixture-model")
            .await
            .unwrap();
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            LLMChunk::Token(_)
        ));
        assert!(matches!(
            stream.next().await.unwrap().unwrap(),
            LLMChunk::ProviderUsage { .. }
        ));
        let terminal = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert!(terminal.unwrap_err().to_string().contains("after Done"));
        assert!(stream.next().await.is_none());
        assert_eq!(ledger(&f).total_charged, 140);
    }
}

#[tokio::test]
async fn token_budget_ready_duplicate_done_yields_to_cancellation() {
    let (f, _registration) = admitted().await;
    configure(&f, 140);
    let decorated = provider(&f, Provider::new(Script::RepeatedDone)).await;
    let mut stream = decorated
        .chat_stream(&[], &[], None, "fixture-model")
        .await
        .unwrap();
    stream.next().await.unwrap().unwrap();
    stream.next().await.unwrap().unwrap();
    let started = std::time::Instant::now();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), stream.next())
            .await
            .is_err()
    );
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "ready trailers starved caller cancellation"
    );
    drop(stream);
    assert_eq!(ledger(&f).total_charged, 140);
}

#[tokio::test]
async fn token_budget_unconfigured_stream_keeps_semantic_done_without_waiting_for_eof() {
    let (f, _registration) = admitted().await;
    let decorated = provider(&f, Provider::new(Script::DonePending)).await;
    let mut stream = decorated
        .chat_stream(&[], &[], Some(40), "fixture-model")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(100), async {
        while let Some(chunk) = stream.next().await {
            if matches!(chunk.unwrap(), LLMChunk::Done) {
                return;
            }
        }
        panic!("semantic terminal missing");
    })
    .await
    .unwrap();
    assert!(f.service.published().unwrap().1.execution_budget.is_none());
}
