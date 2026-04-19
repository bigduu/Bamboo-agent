use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::schedules::{
    MisFirePolicy, OverlapPolicy, ScheduleEntry, ScheduleRunConfig, ScheduleRunRecord,
    ScheduleRunStatus, ScheduleSpec, ScheduleState, ScheduleTrigger,
};

#[derive(Debug, Clone, Serialize)]
pub struct ScheduleView {
    pub id: String,
    pub name: String,
    pub enabled: bool,
    pub trigger: ScheduleTrigger,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_at: Option<DateTime<Utc>>,
    pub misfire_policy: MisFirePolicy,
    pub overlap_policy: OverlapPolicy,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: ScheduleState,
    pub run_config: ScheduleRunConfig,
}

impl ScheduleView {
    pub fn from_spec_and_state(spec: ScheduleSpec, state: ScheduleState) -> Self {
        Self {
            id: spec.id,
            name: spec.name,
            enabled: spec.enabled,
            trigger: spec.trigger,
            timezone: spec.timezone,
            start_at: spec.start_at,
            end_at: spec.end_at,
            misfire_policy: spec.misfire_policy,
            overlap_policy: spec.overlap_policy,
            created_at: spec.created_at,
            updated_at: spec.updated_at,
            state,
            run_config: spec.run_config,
        }
    }
}

impl From<ScheduleEntry> for ScheduleView {
    fn from(value: ScheduleEntry) -> Self {
        ScheduleView::from_spec_and_state(value.to_schedule_spec(), value.to_schedule_state())
    }
}

#[derive(Debug, Serialize)]
pub struct ListSchedulesResponse {
    pub schedules: Vec<ScheduleView>,
}

#[derive(Debug, Deserialize)]
pub struct CreateScheduleRequest {
    pub name: String,
    pub trigger: ScheduleTrigger,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub misfire_policy: Option<MisFirePolicy>,
    #[serde(default)]
    pub overlap_policy: Option<OverlapPolicy>,
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub run_config: ScheduleRunConfig,
}

#[derive(Debug, Deserialize)]
pub struct PatchScheduleRequest {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub trigger: Option<ScheduleTrigger>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub start_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub end_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub misfire_policy: Option<MisFirePolicy>,
    #[serde(default)]
    pub overlap_policy: Option<OverlapPolicy>,
    #[serde(default)]
    pub run_config: Option<ScheduleRunConfig>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScheduleRunRecordView {
    pub run_id: String,
    pub schedule_id: String,
    pub scheduled_for: DateTime<Utc>,
    pub claimed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    pub status: ScheduleRunStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dispatch_lag_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_duration_ms: Option<u64>,
    pub was_catch_up: bool,
}

impl From<ScheduleRunRecord> for ScheduleRunRecordView {
    fn from(value: ScheduleRunRecord) -> Self {
        Self {
            run_id: value.run_id,
            schedule_id: value.schedule_id,
            scheduled_for: value.scheduled_for,
            claimed_at: value.claimed_at,
            started_at: value.started_at,
            completed_at: value.completed_at,
            status: value.status,
            outcome_reason: value.outcome_reason,
            session_id: value.session_id,
            dispatch_lag_ms: value.dispatch_lag_ms,
            execution_duration_ms: value.execution_duration_ms,
            was_catch_up: value.was_catch_up,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ListScheduleSessionsResponse {
    pub schedule_id: String,
    pub sessions: Vec<crate::handlers::agent::sessions::SessionSummary>,
}

#[derive(Debug, Serialize)]
pub struct ListScheduleRunsResponse {
    pub schedule_id: String,
    pub runs: Vec<ScheduleRunRecordView>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedules::{ScheduleState, ScheduleTrigger};

    #[test]
    fn test_schedule_view_from_entry_serialization() {
        let entry = ScheduleEntry {
            id: "sched-1".to_string(),
            name: "daily".to_string(),
            enabled: true,
            trigger: ScheduleTrigger::Interval {
                every_seconds: 60,
                anchor_at: None,
            },
            timezone: Some("Asia/Shanghai".to_string()),
            start_at: None,
            end_at: None,
            misfire_policy: MisFirePolicy::default(),
            overlap_policy: OverlapPolicy::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            state: ScheduleState {
                running_run_count: 1,
                ..Default::default()
            },
            run_config: ScheduleRunConfig::default(),
        };
        let view = ScheduleView::from(entry);

        let json = serde_json::to_string(&view).unwrap();
        assert!(json.contains("\"id\":\"sched-1\""));
        assert!(json.contains("\"state\""));
        assert!(json.contains("\"running_run_count\":1"));
    }

    #[test]
    fn test_list_schedules_response_serialization() {
        let response = ListSchedulesResponse { schedules: vec![] };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"schedules\":[]"));
    }

    #[test]
    fn test_list_schedules_response_debug() {
        let response = ListSchedulesResponse { schedules: vec![] };

        let debug_str = format!("{:?}", response);
        assert!(debug_str.contains("ListSchedulesResponse"));
    }

    #[test]
    fn test_create_schedule_request_deserialization() {
        let json = r#"{
            "name":"daily-report",
            "trigger":{"type":"interval","every_seconds":86400},
            "enabled":true,
            "run_config":{"prompt":"Generate daily report"}
        }"#;

        let req: CreateScheduleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "daily-report");
        assert!(matches!(
            req.trigger,
            ScheduleTrigger::Interval {
                every_seconds: 86400,
                anchor_at: None
            }
        ));
        assert!(req.enabled);
    }

    #[test]
    fn test_create_schedule_request_minimal() {
        let json = r#"{"name":"test","trigger":{"type":"interval","every_seconds":3600}}"#;
        let req: CreateScheduleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "test");
        assert!(matches!(
            req.trigger,
            ScheduleTrigger::Interval {
                every_seconds: 3600,
                anchor_at: None
            }
        ));
        assert!(!req.enabled); // default false
    }

    #[test]
    fn test_create_schedule_request_debug() {
        let req = CreateScheduleRequest {
            name: "test".to_string(),
            trigger: ScheduleTrigger::Interval {
                every_seconds: 60,
                anchor_at: None,
            },
            timezone: None,
            start_at: None,
            end_at: None,
            misfire_policy: None,
            overlap_policy: None,
            enabled: false,
            run_config: ScheduleRunConfig::default(),
        };

        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("CreateScheduleRequest"));
    }

    #[test]
    fn test_patch_schedule_request_all_fields() {
        let json = r#"{
            "name":"updated",
            "enabled":true,
            "trigger":{"type":"interval","every_seconds":7200},
            "run_config":{"prompt":"Updated"}
        }"#;

        let req: PatchScheduleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, Some("updated".to_string()));
        assert_eq!(req.enabled, Some(true));
        assert!(matches!(
            req.trigger,
            Some(ScheduleTrigger::Interval {
                every_seconds: 7200,
                anchor_at: None
            })
        ));
    }

    #[test]
    fn test_create_schedule_request_with_trigger_deserialization() {
        let json = r#"{
            "name":"daily-report",
            "trigger":{"type":"daily","hour":9,"minute":30},
            "timezone":"Asia/Shanghai",
            "misfire_policy":{"type":"run_once"},
            "overlap_policy":"queue_one"
        }"#;

        let req: CreateScheduleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "daily-report");
        assert!(matches!(
            req.trigger,
            ScheduleTrigger::Daily {
                hour: 9,
                minute: 30,
                second: 0
            }
        ));
        assert_eq!(req.timezone.as_deref(), Some("Asia/Shanghai"));
    }

    #[test]
    fn test_patch_schedule_request_partial() {
        let json = r#"{"enabled":false}"#;
        let req: PatchScheduleRequest = serde_json::from_str(json).unwrap();
        assert!(req.name.is_none());
        assert_eq!(req.enabled, Some(false));
        assert!(req.trigger.is_none());
    }

    #[test]
    fn test_patch_schedule_request_empty() {
        let json = r#"{}"#;
        let req: PatchScheduleRequest = serde_json::from_str(json).unwrap();
        assert!(req.name.is_none());
        assert!(req.enabled.is_none());
        assert!(req.trigger.is_none());
    }

    #[test]
    fn test_patch_schedule_request_debug() {
        let req = PatchScheduleRequest {
            name: Some("test".to_string()),
            enabled: None,
            trigger: None,
            timezone: None,
            start_at: None,
            end_at: None,
            misfire_policy: None,
            overlap_policy: None,
            run_config: None,
        };

        let debug_str = format!("{:?}", req);
        assert!(debug_str.contains("PatchScheduleRequest"));
    }

    #[test]
    fn test_list_schedule_sessions_response_serialization() {
        let response = ListScheduleSessionsResponse {
            schedule_id: "sched-123".to_string(),
            sessions: vec![],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("sched-123"));
        assert!(json.contains("\"sessions\":[]"));
    }

    #[test]
    fn test_list_schedule_sessions_response_debug() {
        let response = ListScheduleSessionsResponse {
            schedule_id: "test".to_string(),
            sessions: vec![],
        };

        let debug_str = format!("{:?}", response);
        assert!(debug_str.contains("ListScheduleSessionsResponse"));
    }

    #[test]
    fn test_schedule_run_record_view_serialization() {
        let record = ScheduleRunRecord {
            run_id: "run-1".to_string(),
            schedule_id: "sched-1".to_string(),
            scheduled_for: Utc::now(),
            claimed_at: Utc::now(),
            started_at: None,
            completed_at: None,
            status: ScheduleRunStatus::Queued,
            outcome_reason: Some("waiting".to_string()),
            session_id: None,
            dispatch_lag_ms: Some(15),
            execution_duration_ms: None,
            was_catch_up: false,
        };
        let view = ScheduleRunRecordView::from(record);
        let json = serde_json::to_string(&view).unwrap();
        assert!(json.contains("\"run_id\":\"run-1\""));
        assert!(json.contains("\"status\":\"queued\""));
        assert!(json.contains("\"dispatch_lag_ms\":15"));
    }

    #[test]
    fn test_list_schedule_runs_response_serialization() {
        let response = ListScheduleRunsResponse {
            schedule_id: "sched-123".to_string(),
            runs: vec![],
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("sched-123"));
        assert!(json.contains("\"runs\":[]"));
    }
}
