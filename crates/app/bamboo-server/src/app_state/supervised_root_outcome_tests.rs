//! Bounded supervised Root outcome and crash recovery regressions.
#![cfg(any(target_os = "linux", target_os = "macos"))]

#[tokio::test]
async fn subscription_recovers_across_owned_process_crash_windows() {
    super::observer_subscriptions::tests::subscription_recovers_across_owned_process_crash_windows(
    )
    .await;
}

#[tokio::test]
async fn consecutive_native_root_outcomes_have_distinct_observations() {
    super::observer_subscriptions::tests::consecutive_native_root_outcomes_have_distinct_observations()
        .await;
}

#[tokio::test]
async fn offline_observer_recovers_both_native_root_outcomes() {
    super::observer_subscriptions::tests::offline_observer_recovers_both_native_root_outcomes()
        .await;
}
