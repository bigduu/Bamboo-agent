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
