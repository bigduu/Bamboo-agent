use bamboo_metrics::types::{ForwardTokenDetails, TokenUsage as MetricsTokenUsage};

use super::super::types::{
    ResponsesInputTokensDetails, ResponsesOutputTokensDetails, ResponsesUsage,
};
use crate::handlers::llm_compat::usage::build_estimated_usage;

/// Cumulative provider usage observed on the compatibility stream.
///
/// OpenAI terminal usage frames are snapshots, not deltas. Each present field
/// replaces the previous value while omitted fields retain what was already
/// observed. Explicit zeros therefore remain authoritative.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ResponsesUsageAccumulator {
    saw_provider_usage: bool,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    total_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
    cached_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
}

impl ResponsesUsageAccumulator {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn record(
        &mut self,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        total_tokens: Option<u64>,
        reasoning_tokens: Option<u64>,
        cached_tokens: Option<u64>,
        cache_write_tokens: Option<u64>,
    ) {
        self.saw_provider_usage = true;
        if input_tokens.is_some() {
            self.input_tokens = input_tokens;
        }
        if output_tokens.is_some() {
            self.output_tokens = output_tokens;
        }
        if total_tokens.is_some() {
            self.total_tokens = total_tokens;
        }
        if reasoning_tokens.is_some() {
            self.reasoning_tokens = reasoning_tokens;
        }
        if cached_tokens.is_some() {
            self.cached_tokens = cached_tokens;
        }
        if cache_write_tokens.is_some() {
            self.cache_write_tokens = cache_write_tokens;
        }
    }

    pub(super) fn response_usage(self) -> Option<ResponsesUsage> {
        if !self.saw_provider_usage {
            return None;
        }

        let input_tokens_details = (self.cached_tokens.is_some()
            || self.cache_write_tokens.is_some())
        .then_some(ResponsesInputTokensDetails {
            cached_tokens: self.cached_tokens,
            cache_write_tokens: self.cache_write_tokens,
        });
        let output_tokens_details =
            self.reasoning_tokens
                .is_some()
                .then_some(ResponsesOutputTokensDetails {
                    reasoning_tokens: self.reasoning_tokens,
                });
        Some(ResponsesUsage {
            input_tokens: self.input_tokens,
            input_tokens_details,
            output_tokens: self.output_tokens,
            output_tokens_details,
            total_tokens: self.total_tokens,
        })
    }

    /// Metrics use exact provider fields independently, falling back only for
    /// the field the provider omitted.
    pub(super) fn metrics_usage(
        self,
        estimated_prompt_tokens: u64,
        estimated_completion_tokens: u64,
    ) -> (MetricsTokenUsage, Option<ForwardTokenDetails>) {
        if !self.saw_provider_usage {
            return (
                build_estimated_usage(estimated_prompt_tokens, estimated_completion_tokens),
                None,
            );
        }

        let prompt_tokens = self.input_tokens.unwrap_or(estimated_prompt_tokens);
        let completion_tokens = self.output_tokens.unwrap_or(estimated_completion_tokens);
        let token_details = ForwardTokenDetails {
            cache_creation_input_tokens: None,
            cache_read_input_tokens: self.cached_tokens,
            cache_write_input_tokens: self.cache_write_tokens,
            reasoning_output_tokens: self.reasoning_tokens,
        };
        (
            MetricsTokenUsage {
                prompt_tokens,
                completion_tokens,
                total_tokens: self
                    .total_tokens
                    .unwrap_or_else(|| prompt_tokens.saturating_add(completion_tokens)),
            },
            (!token_details.is_empty()).then_some(token_details),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_openai_details_and_explicit_zeroes() {
        let mut usage = ResponsesUsageAccumulator::default();
        usage.record(Some(80), Some(20), Some(100), Some(5), Some(0), Some(64));

        let response = usage.response_usage().expect("provider usage");
        assert_eq!(response.input_tokens, Some(80));
        assert_eq!(response.output_tokens, Some(20));
        assert_eq!(response.total_tokens, Some(100));
        let input_details = response.input_tokens_details.expect("input details");
        assert_eq!(input_details.cached_tokens, Some(0));
        assert_eq!(input_details.cache_write_tokens, Some(64));
        assert_eq!(
            response
                .output_tokens_details
                .expect("output details")
                .reasoning_tokens,
            Some(5)
        );
    }

    #[test]
    fn metrics_prefer_provider_values_field_by_field() {
        let mut usage = ResponsesUsageAccumulator::default();
        usage.record(Some(8), None, Some(13), None, None, None);

        let (metrics, details) = usage.metrics_usage(99, 5);
        assert_eq!(metrics.prompt_tokens, 8);
        assert_eq!(metrics.completion_tokens, 5);
        assert_eq!(metrics.total_tokens, 13);
        assert!(details.is_none());
    }

    #[test]
    fn metrics_keep_cache_write_distinct_from_cache_creation() {
        let mut usage = ResponsesUsageAccumulator::default();
        usage.record(Some(80), Some(20), Some(100), Some(5), Some(32), Some(48));

        let (_, details) = usage.metrics_usage(999, 999);
        let details = details.expect("provider token details");
        assert_eq!(details.cache_creation_input_tokens, None);
        assert_eq!(details.cache_read_input_tokens, Some(32));
        assert_eq!(details.cache_write_input_tokens, Some(48));
        assert_eq!(details.reasoning_output_tokens, Some(5));
    }

    #[test]
    fn outward_usage_does_not_invent_an_omitted_total() {
        let mut usage = ResponsesUsageAccumulator::default();
        usage.record(Some(8), Some(5), None, None, None, None);

        let response = usage.response_usage().expect("provider usage");
        assert_eq!(response.input_tokens, Some(8));
        assert_eq!(response.output_tokens, Some(5));
        assert_eq!(response.total_tokens, None);
    }
}
