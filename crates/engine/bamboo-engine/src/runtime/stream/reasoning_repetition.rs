use std::collections::VecDeque;

use bamboo_llm::provider::CompletionDiagnostics;
use bamboo_llm::{LLMChunk, LLMStream};
use futures::StreamExt;

const HISTORY_BYTES: usize = 4096;
const CHECK_INTERVAL_BYTES: usize = 128;
const MIN_REPEATED_BYTES: usize = 1024;
const MIN_REPETITIONS: usize = 4;

#[derive(Default)]
struct ReasoningRepetition {
    history: VecDeque<u8>,
    prefix_lengths: Vec<usize>,
    bytes_since_check: usize,
}

impl ReasoningRepetition {
    fn observe(&mut self, mut bytes: &[u8]) -> bool {
        while !bytes.is_empty() {
            let take = bytes
                .len()
                .min(CHECK_INTERVAL_BYTES - self.bytes_since_check);
            let discard = (self.history.len() + take).saturating_sub(HISTORY_BYTES);
            self.history.drain(..discard);
            self.history.extend(&bytes[..take]);
            bytes = &bytes[take..];
            self.bytes_since_check += take;
            if self.bytes_since_check == CHECK_INTERVAL_BYTES {
                self.bytes_since_check = 0;
                if self.history.len() >= MIN_REPEATED_BYTES && self.has_repeating_suffix() {
                    return true;
                }
            }
        }
        false
    }

    /// A prefix function on the reversed history tests every trailing span in
    /// one pass. For a span of length N, N minus its longest matching prefix
    /// and suffix gives the shortest period, even at a partial cycle boundary.
    fn has_repeating_suffix(&mut self) -> bool {
        self.prefix_lengths.resize(self.history.len(), 0);
        let last = self.history.len() - 1;
        for index in 1..self.history.len() {
            let byte = self.history[last - index];
            let mut prefix = self.prefix_lengths[index - 1];
            while prefix > 0 && byte != self.history[last - prefix] {
                prefix = self.prefix_lengths[prefix - 1];
            }
            if byte == self.history[last - prefix] {
                prefix += 1;
            }
            self.prefix_lengths[index] = prefix;
            let length = index + 1;
            if length >= MIN_REPEATED_BYTES && length / (length - prefix) >= MIN_REPETITIONS {
                return true;
            }
        }
        false
    }
}

pub(crate) fn stop_repeated_reasoning(
    stream: LLMStream,
    completion: CompletionDiagnostics,
) -> LLMStream {
    Box::pin(futures::stream::unfold(
        (
            Some(stream),
            completion,
            Some(ReasoningRepetition::default()),
        ),
        |(stream, completion, mut detector)| async move {
            let mut stream = stream?;
            let item = stream.next().await?;
            if let Some(repetition) = detector.as_mut() {
                match &item {
                    Ok(LLMChunk::ReasoningToken(text)) if repetition.observe(text.as_bytes()) => {
                        if completion.try_stop_for_reasoning_repetition() {
                            tracing::info!("Stopping a stream after repeated reasoning");
                            drop(stream);
                            return completion
                                .take_pending_usage()
                                .map(|usage| (Ok(usage), (None, completion, None)));
                        }
                        detector = None;
                    }
                    // The current recovery path can discard a reasoning-only
                    // attempt. Once there is visible output or tool activity,
                    // keep reading so the ordinary response path can settle it.
                    Ok(chunk) if chunk.blocks_reasoning_cutoff() => detector = None,
                    Ok(LLMChunk::ReasoningSignature(_) | LLMChunk::ProviderTranscriptItem(_)) => {
                        *repetition = ReasoningRepetition::default();
                    }
                    _ => {}
                }
            }
            Some((item, (Some(stream), completion, detector)))
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_llm::provider::REASONING_REPETITION_FINISH_REASON;

    #[test]
    fn detects_a_period_across_fragments_in_a_long_stream() {
        let mut detector = ReasoningRepetition::default();
        let progress: String = (0..1024)
            .map(|n| format!("Step {n} made progress. "))
            .collect();
        assert!(!detector.observe(progress.as_bytes()));
        let cycle: String = (0..24)
            .map(|n| format!("Reconsidering aspect {n}. "))
            .collect();
        assert!(cycle.len() > 512);
        let reasoning = cycle.repeat(5);
        assert!(reasoning
            .as_bytes()
            .chunks(29)
            .any(|fragment| detector.observe(fragment)));
        assert!(detector.history.len() <= HISTORY_BYTES);
    }

    #[tokio::test]
    async fn stops_reading_the_reasoning_only_stream_before_its_next_item() {
        let completion = CompletionDiagnostics::for_agent_round(None);
        let stream: LLMStream = Box::pin(
            futures::stream::iter([Ok(LLMChunk::ReasoningToken(
                "Revisiting this assumption. ".repeat(90),
            ))])
            .chain(futures::stream::poll_fn(|_| {
                panic!("read after repetition")
            })),
        );
        let mut guarded = stop_repeated_reasoning(stream, completion.clone());
        assert!(guarded.next().await.is_none());
        assert_eq!(
            completion.finish_reason(),
            REASONING_REPETITION_FINISH_REASON
        );
    }

    #[tokio::test]
    async fn leaves_an_attempt_with_visible_output_to_the_normal_stream_handler() {
        let completion = CompletionDiagnostics::for_agent_round(None);
        let stream: LLMStream = Box::pin(futures::stream::iter([
            Ok(LLMChunk::Token("Visible progress.".into())),
            Ok(LLMChunk::ReasoningToken(
                "Revisiting this assumption. ".repeat(90),
            )),
            Ok(LLMChunk::Done),
        ]));
        let output: Vec<_> = stop_repeated_reasoning(stream, completion.clone())
            .collect()
            .await;
        assert_eq!(output.len(), 3);
        assert!(matches!(output[2], Ok(LLMChunk::Done)));
        assert_eq!(completion.finish_reason(), "unavailable");
    }
}
