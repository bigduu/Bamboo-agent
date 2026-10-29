//! Per-execution opt-in. No persistent Session or provider mutation.

use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;

/// Retry only establishment of the initial HTTP response. Successful streams
/// and tool execution are never replayed by this policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum UpstreamRetry {
    #[default]
    Bounded,
    UntilCancelled,
}

tokio::task_local! {
    static UPSTREAM_RETRY: UpstreamRetry;
}

/// Scope this to one main-provider bootstrap. The owner must retain a
/// cancellation select around the future; dropping it cancels send/backoff.
/// Child tasks do not inherit the task-local opt-in.
pub async fn with_upstream_retry<F: Future>(policy: UpstreamRetry, future: F) -> F::Output {
    UPSTREAM_RETRY.scope(policy, future).await
}

pub(crate) fn continuous_request_retries() -> bool {
    !super::work_request_budget_active()
        && UPSTREAM_RETRY
            .try_with(|p| *p == UpstreamRetry::UntilCancelled)
            .unwrap_or(false)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitialRequestLimits {
    pub total_budget: Duration,
    pub attempt_timeout: Duration,
}

impl Default for InitialRequestLimits {
    fn default() -> Self {
        Self {
            total_budget: Duration::from_secs(120),
            attempt_timeout: Duration::from_secs(20),
        }
    }
}

impl InitialRequestLimits {
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let defaults = Self::default();
        let duration = |name, fallback: Duration, maximum| {
            lookup(name)
                .and_then(|v| v.trim().parse::<u64>().ok())
                .filter(|v| (1..=maximum).contains(v))
                .map(Duration::from_millis)
                .unwrap_or(fallback)
        };
        Self {
            total_budget: duration(
                "BAMBOO_LLM_RETRY_TOTAL_BUDGET_MS",
                defaults.total_budget,
                3_600_000,
            ),
            attempt_timeout: duration(
                "BAMBOO_LLM_RETRY_ATTEMPT_TIMEOUT_MS",
                defaults.attempt_timeout,
                60_000,
            ),
        }
    }
}

/// Operator settings are read once, like the existing retry count/backoff env.
/// Invalid or overflowing values retain finite defaults. No live config writes.
pub fn initial_request_limits() -> &'static InitialRequestLimits {
    static LIMITS: OnceLock<InitialRequestLimits> = OnceLock::new();
    LIMITS.get_or_init(|| InitialRequestLimits::from_lookup(|key| std::env::var(key).ok()))
}

#[derive(Debug, serde::Serialize)]
pub struct UpstreamRetryDefaults {
    pub supported_modes: [&'static str; 2],
    pub default_mode: &'static str,
    pub max_attempts: u32,
    pub total_budget_ms: u64,
    pub attempt_timeout_ms: u64,
}

pub fn upstream_retry_defaults() -> UpstreamRetryDefaults {
    let limits = initial_request_limits();
    UpstreamRetryDefaults {
        supported_modes: ["bounded", "until_cancelled"],
        default_mode: "bounded",
        max_attempts: super::global().max_attempts,
        total_budget_ms: limits.total_budget.as_millis() as u64,
        attempt_timeout_ms: limits.attempt_timeout.as_millis() as u64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_are_configurable_and_invalid_values_keep_finite_defaults() {
        let configured = InitialRequestLimits::from_lookup(|key| {
            Some(
                match key {
                    "BAMBOO_LLM_RETRY_TOTAL_BUDGET_MS" => "300000",
                    _ => "45000",
                }
                .into(),
            )
        });
        assert_eq!(configured.total_budget, Duration::from_secs(300));
        assert_eq!(configured.attempt_timeout, Duration::from_secs(45));
        for value in ["0", "-1", "bad", "18446744073709551615"] {
            assert_eq!(
                InitialRequestLimits::from_lookup(|_| Some(value.into())),
                InitialRequestLimits::default()
            );
        }
    }

    #[test]
    fn wire_requires_an_explicit_known_mode() {
        assert_eq!(
            serde_json::from_str::<UpstreamRetry>(r#"{"mode":"until_cancelled"}"#).unwrap(),
            UpstreamRetry::UntilCancelled
        );
        assert!(serde_json::from_str::<UpstreamRetry>(r#"{"mode":"forever"}"#).is_err());
        assert!(serde_json::from_str::<UpstreamRetry>(r#"{}"#).is_err());
        assert_eq!(UpstreamRetry::default(), UpstreamRetry::Bounded);
    }

    #[tokio::test]
    async fn opt_in_is_scoped_and_cannot_override_work_physical_reservations() {
        assert!(!continuous_request_retries());
        with_upstream_retry(UpstreamRetry::UntilCancelled, async {
            assert!(continuous_request_retries());
            assert!(!tokio::spawn(async { continuous_request_retries() })
                .await
                .unwrap());
            super::super::WorkPhysicalRequest::default()
                .scope(async {
                    assert!(!continuous_request_retries());
                })
                .await;
        })
        .await;
        assert!(!continuous_request_retries());
    }
}
