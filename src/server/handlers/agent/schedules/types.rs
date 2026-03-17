use serde::{Deserialize, Serialize};

use crate::server::schedules::store::ScheduleRunConfig;
use crate::server::schedules::ScheduleEntry;

#[derive(Debug, Serialize)]
pub struct ListSchedulesResponse {
    pub schedules: Vec<ScheduleEntry>,
}

#[derive(Debug, Deserialize)]
pub struct CreateScheduleRequest {
    pub name: String,
    pub interval_seconds: u64,
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
    pub interval_seconds: Option<u64>,
    #[serde(default)]
    pub run_config: Option<ScheduleRunConfig>,
}

#[derive(Debug, Serialize)]
pub struct ListScheduleSessionsResponse {
    pub schedule_id: String,
    pub sessions: Vec<crate::server::handlers::agent::sessions::SessionSummary>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
            "interval_seconds":86400,
            "enabled":true,
            "run_config":{"prompt":"Generate daily report"}
        }"#;

        let req: CreateScheduleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "daily-report");
        assert_eq!(req.interval_seconds, 86400);
        assert!(req.enabled);
    }

    #[test]
    fn test_create_schedule_request_minimal() {
        let json = r#"{"name":"test","interval_seconds":3600}"#;
        let req: CreateScheduleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, "test");
        assert_eq!(req.interval_seconds, 3600);
        assert!(!req.enabled); // default false
    }

    #[test]
    fn test_create_schedule_request_debug() {
        let req = CreateScheduleRequest {
            name: "test".to_string(),
            interval_seconds: 60,
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
            "interval_seconds":7200,
            "run_config":{"prompt":"Updated"}
        }"#;

        let req: PatchScheduleRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.name, Some("updated".to_string()));
        assert_eq!(req.enabled, Some(true));
        assert_eq!(req.interval_seconds, Some(7200));
    }

    #[test]
    fn test_patch_schedule_request_partial() {
        let json = r#"{"enabled":false}"#;
        let req: PatchScheduleRequest = serde_json::from_str(json).unwrap();
        assert!(req.name.is_none());
        assert_eq!(req.enabled, Some(false));
        assert!(req.interval_seconds.is_none());
    }

    #[test]
    fn test_patch_schedule_request_empty() {
        let json = r#"{}"#;
        let req: PatchScheduleRequest = serde_json::from_str(json).unwrap();
        assert!(req.name.is_none());
        assert!(req.enabled.is_none());
        assert!(req.interval_seconds.is_none());
    }

    #[test]
    fn test_patch_schedule_request_debug() {
        let req = PatchScheduleRequest {
            name: Some("test".to_string()),
            enabled: None,
            interval_seconds: None,
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
}
