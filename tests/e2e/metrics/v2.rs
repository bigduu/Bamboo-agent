use super::*;
use bamboo_metrics::{
    ForwardStatus, MetricsStorage, RoundStatus, SessionStatus, SqliteMetricsStorage, TokenUsage,
};
use chrono::{Duration, NaiveDate, TimeZone, Utc};

async fn seed_chat_metrics(
    storage: &SqliteMetricsStorage,
    suffix: &str,
    date: NaiveDate,
    tokens: u64,
) {
    let started_at = Utc.from_utc_datetime(
        &date
            .and_hms_opt(12, 0, 0)
            .expect("fixture date has a valid noon"),
    );
    let session_id = format!("timeline-session-{suffix}");
    let round_id = format!("timeline-round-{suffix}");

    storage
        .upsert_session_start(&session_id, "chat-model", started_at)
        .await
        .expect("seed session start");
    storage
        .insert_round_start(&round_id, &session_id, "chat-model", started_at)
        .await
        .expect("seed round start");
    storage
        .complete_round(
            &round_id,
            started_at + Duration::seconds(1),
            RoundStatus::Success,
            TokenUsage {
                prompt_tokens: tokens,
                completion_tokens: 0,
                total_tokens: tokens,
            },
            1,
            0,
            None,
        )
        .await
        .expect("seed round completion");
    storage
        .complete_session(
            &session_id,
            SessionStatus::Completed,
            started_at + Duration::seconds(2),
        )
        .await
        .expect("seed session completion");
}

async fn seed_forward_metrics(
    storage: &SqliteMetricsStorage,
    suffix: &str,
    date: NaiveDate,
    tokens: u64,
) {
    let started_at = Utc.from_utc_datetime(
        &date
            .and_hms_opt(12, 0, 0)
            .expect("fixture date has a valid noon"),
    );
    let forward_id = format!("timeline-forward-{suffix}");

    storage
        .insert_forward_start(
            &forward_id,
            "openai.chat_completions",
            "forward-model",
            false,
            started_at,
        )
        .await
        .expect("seed forward start");
    storage
        .complete_forward(
            &forward_id,
            started_at + Duration::seconds(1),
            Some(200),
            ForwardStatus::Success,
            Some(TokenUsage {
                prompt_tokens: tokens,
                completion_tokens: 0,
                total_tokens: tokens,
            }),
            None,
            None,
        )
        .await
        .expect("seed forward completion");
}

fn timeline_totals(
    timeline: &[handlers::metrics::UnifiedTimelinePoint],
) -> (u64, u64, u64, u32, u32, u64) {
    timeline.iter().fold(
        (0, 0, 0, 0, 0, 0),
        |(
            chat_tokens,
            forward_tokens,
            total_tokens,
            chat_sessions,
            forward_requests,
            cached_outputs,
        ),
         point| {
            (
                chat_tokens + point.chat_tokens,
                forward_tokens + point.forward_tokens,
                total_tokens + point.total_tokens,
                chat_sessions + point.chat_sessions,
                forward_requests + point.forward_requests,
                cached_outputs + point.prompt_cached_tool_outputs,
            )
        },
    )
}

#[actix_web::test]
async fn test_metrics_v2_summary_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/v2/summary",
        web::get().to(handlers::metrics::v2_unified_summary),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/v2/summary")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_metrics_v2_timeline_endpoint() {
    let state = crate::e2e::common::create_test_app().await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/v2/timeline",
        web::get().to(handlers::metrics::v2_unified_timeline),
    ))
    .await;

    let req = test::TestRequest::get()
        .uri("/api/v1/metrics/v2/timeline")
        .to_request();

    let resp = test::call_service(&app, req).await;

    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_metrics_v2_timeline_honors_granularity_and_reconciles_totals() {
    let state = crate::e2e::common::create_test_app().await;
    let storage = SqliteMetricsStorage::new(state.app_data_dir.join("metrics.db"));
    storage.init().await.expect("initialize metrics storage");

    // The fixed future range is immune to the service's rolling retention
    // worker. Jan 31/Feb 1 and Feb 28/Mar 1 are sparse chat/forward pairs in
    // the same Monday-based weeks, while also crossing calendar months.
    seed_chat_metrics(
        &storage,
        "jan-31",
        NaiveDate::from_ymd_opt(2099, 1, 31).expect("valid date"),
        10,
    )
    .await;
    seed_forward_metrics(
        &storage,
        "feb-01",
        NaiveDate::from_ymd_opt(2099, 2, 1).expect("valid date"),
        20,
    )
    .await;
    seed_chat_metrics(
        &storage,
        "feb-08",
        NaiveDate::from_ymd_opt(2099, 2, 8).expect("valid date"),
        30,
    )
    .await;
    seed_forward_metrics(
        &storage,
        "feb-28",
        NaiveDate::from_ymd_opt(2099, 2, 28).expect("valid date"),
        40,
    )
    .await;
    seed_chat_metrics(
        &storage,
        "mar-01",
        NaiveDate::from_ymd_opt(2099, 3, 1).expect("valid date"),
        50,
    )
    .await;

    let app = test::init_service(App::new().app_data(state).route(
        "/api/v1/metrics/v2/timeline",
        web::get().to(handlers::metrics::v2_unified_timeline),
    ))
    .await;
    let range = "days=40&end_date=2099-03-08";

    let default: Vec<handlers::metrics::UnifiedTimelinePoint> = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri(&format!("/api/v1/metrics/v2/timeline?{range}"))
            .to_request(),
    )
    .await;
    let daily: Vec<handlers::metrics::UnifiedTimelinePoint> = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri(&format!(
                "/api/v1/metrics/v2/timeline?{range}&granularity=daily"
            ))
            .to_request(),
    )
    .await;
    let weekly: Vec<handlers::metrics::UnifiedTimelinePoint> = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri(&format!(
                "/api/v1/metrics/v2/timeline?{range}&granularity=weekly"
            ))
            .to_request(),
    )
    .await;
    let monthly: Vec<handlers::metrics::UnifiedTimelinePoint> = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri(&format!(
                "/api/v1/metrics/v2/timeline?{range}&granularity=monthly"
            ))
            .to_request(),
    )
    .await;
    let invalid: Vec<handlers::metrics::UnifiedTimelinePoint> = test::call_and_read_body_json(
        &app,
        test::TestRequest::get()
            .uri(&format!(
                "/api/v1/metrics/v2/timeline?{range}&granularity=unexpected"
            ))
            .to_request(),
    )
    .await;

    assert_eq!(default, daily, "omitted granularity defaults to daily");
    assert_eq!(invalid, daily, "unknown granularity follows /metrics/daily");
    assert_eq!(daily.len(), 5);
    assert!(daily.iter().all(|point| point.period_start.is_none()));
    assert!(daily.iter().all(|point| point.period_end.is_none()));

    assert_eq!(weekly.len(), 3);
    assert_eq!(weekly[0].date, "2099-01-26..2099-02-01");
    assert_eq!(weekly[0].period_start.as_deref(), Some("2099-01-26"));
    assert_eq!(weekly[0].period_end.as_deref(), Some("2099-02-01"));
    assert_eq!(weekly[0].chat_tokens, 10);
    assert_eq!(weekly[0].forward_tokens, 20);
    assert_eq!(weekly[2].date, "2099-02-23..2099-03-01");
    assert_eq!(weekly[2].chat_tokens, 50);
    assert_eq!(weekly[2].forward_tokens, 40);

    assert_eq!(monthly.len(), 3);
    assert_eq!(monthly[0].period_start.as_deref(), Some("2099-01-01"));
    assert_eq!(monthly[0].period_end.as_deref(), Some("2099-01-31"));
    assert_eq!(monthly[1].period_start.as_deref(), Some("2099-02-01"));
    assert_eq!(monthly[1].period_end.as_deref(), Some("2099-02-28"));
    assert_eq!(monthly[2].period_start.as_deref(), Some("2099-03-01"));
    assert_eq!(monthly[2].period_end.as_deref(), Some("2099-03-01"));

    let expected = (90, 60, 150, 3, 2, 3);
    assert_eq!(timeline_totals(&daily), expected);
    assert_eq!(timeline_totals(&weekly), expected);
    assert_eq!(timeline_totals(&monthly), expected);
}
