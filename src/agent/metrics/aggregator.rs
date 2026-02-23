use std::collections::HashMap;

use chrono::{Datelike, Duration, NaiveDate, Weekday};
use serde::{Deserialize, Serialize};

use crate::types::{DailyMetrics, TokenUsage};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeriodMetrics {
    pub label: String,
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub total_sessions: u32,
    pub total_rounds: u32,
    pub total_token_usage: TokenUsage,
    pub total_tool_calls: u32,
    pub model_breakdown: HashMap<String, TokenUsage>,
    pub tool_breakdown: HashMap<String, u32>,
}

pub fn aggregate_weekly(daily_metrics: &[DailyMetrics]) -> Vec<PeriodMetrics> {
    aggregate_by_period(daily_metrics, start_of_week)
}

pub fn aggregate_monthly(daily_metrics: &[DailyMetrics]) -> Vec<PeriodMetrics> {
    aggregate_by_period(daily_metrics, |date| {
        date.with_day(1)
            .expect("day one should always be valid for a date")
    })
}

fn aggregate_by_period<F>(
    daily_metrics: &[DailyMetrics],
    period_start_resolver: F,
) -> Vec<PeriodMetrics>
where
    F: Fn(NaiveDate) -> NaiveDate,
{
    let mut buckets: HashMap<NaiveDate, PeriodMetrics> = HashMap::new();

    for day in daily_metrics {
        let period_start = period_start_resolver(day.date);

        let entry = buckets
            .entry(period_start)
            .or_insert_with(|| PeriodMetrics {
                label: period_start.to_string(),
                period_start,
                period_end: period_start,
                total_sessions: 0,
                total_rounds: 0,
                total_token_usage: TokenUsage::default(),
                total_tool_calls: 0,
                model_breakdown: HashMap::new(),
                tool_breakdown: HashMap::new(),
            });

        if day.date > entry.period_end {
            entry.period_end = day.date;
        }

        entry.total_sessions += day.total_sessions;
        entry.total_rounds += day.total_rounds;
        entry.total_token_usage.add_assign(day.total_token_usage);
        entry.total_tool_calls += day.total_tool_calls;

        for (model, usage) in &day.model_breakdown {
            let model_entry = entry.model_breakdown.entry(model.clone()).or_default();
            model_entry.add_assign(*usage);
        }

        for (tool, count) in &day.tool_breakdown {
            *entry.tool_breakdown.entry(tool.clone()).or_insert(0) += count;
        }
    }

    let mut periods: Vec<PeriodMetrics> = buckets.into_values().collect();
    periods.sort_by_key(|period| period.period_start);

    for period in &mut periods {
        period.label = format_period_label(period.period_start, period.period_end);
    }

    periods
}

fn start_of_week(date: NaiveDate) -> NaiveDate {
    let weekday = match date.weekday() {
        Weekday::Mon => 0,
        Weekday::Tue => 1,
        Weekday::Wed => 2,
        Weekday::Thu => 3,
        Weekday::Fri => 4,
        Weekday::Sat => 5,
        Weekday::Sun => 6,
    };

    date - Duration::days(weekday)
}

fn format_period_label(start: NaiveDate, end: NaiveDate) -> String {
    if start == end {
        start.to_string()
    } else {
        format!("{}..{}", start, end)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::NaiveDate;

    use super::{aggregate_monthly, aggregate_weekly};
    use crate::types::{DailyMetrics, TokenUsage};

    #[test]
    fn aggregate_weekly_combines_days_into_a_single_week_bucket() {
        let input = vec![
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2026, 2, 9).expect("valid date"),
                total_sessions: 2,
                total_rounds: 3,
                total_token_usage: TokenUsage {
                    prompt_tokens: 10,
                    completion_tokens: 20,
                    total_tokens: 30,
                },
                total_tool_calls: 4,
                model_breakdown: HashMap::new(),
                tool_breakdown: HashMap::new(),
            },
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2026, 2, 10).expect("valid date"),
                total_sessions: 1,
                total_rounds: 2,
                total_token_usage: TokenUsage {
                    prompt_tokens: 5,
                    completion_tokens: 5,
                    total_tokens: 10,
                },
                total_tool_calls: 1,
                model_breakdown: HashMap::new(),
                tool_breakdown: HashMap::new(),
            },
        ];

        let result = aggregate_weekly(&input);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].total_sessions, 3);
        assert_eq!(result[0].total_rounds, 5);
        assert_eq!(result[0].total_token_usage.total_tokens, 40);
        assert_eq!(result[0].total_tool_calls, 5);
    }

    #[test]
    fn aggregate_monthly_groups_metrics_by_month_start() {
        let input = vec![
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2026, 1, 31).expect("valid date"),
                total_sessions: 1,
                total_rounds: 1,
                total_token_usage: TokenUsage {
                    prompt_tokens: 1,
                    completion_tokens: 1,
                    total_tokens: 2,
                },
                total_tool_calls: 1,
                model_breakdown: HashMap::new(),
                tool_breakdown: HashMap::new(),
            },
            DailyMetrics {
                date: NaiveDate::from_ymd_opt(2026, 2, 1).expect("valid date"),
                total_sessions: 2,
                total_rounds: 2,
                total_token_usage: TokenUsage {
                    prompt_tokens: 2,
                    completion_tokens: 3,
                    total_tokens: 5,
                },
                total_tool_calls: 3,
                model_breakdown: HashMap::new(),
                tool_breakdown: HashMap::new(),
            },
        ];

        let result = aggregate_monthly(&input);
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].period_start,
            NaiveDate::from_ymd_opt(2026, 1, 1).expect("valid date")
        );
        assert_eq!(
            result[1].period_start,
            NaiveDate::from_ymd_opt(2026, 2, 1).expect("valid date")
        );
    }
}
