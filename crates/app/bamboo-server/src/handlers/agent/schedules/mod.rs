//! Schedule management endpoints.

mod handlers;
mod types;
mod validation;

pub use handlers::{
    cancel_schedule_run, create_schedule, delete_schedule, list_runs_for_schedule, list_schedules,
    list_sessions_for_schedule, patch_schedule, rename_schedule, run_now, set_schedule_daily_time,
    set_schedule_interval, set_schedule_monthly_time, set_schedule_once_at, set_schedule_prompt,
    set_schedule_weekly_time,
};
pub use types::{
    CreateScheduleRequest, ListScheduleRunsResponse, ListScheduleSessionsResponse,
    ListSchedulesResponse, PatchScheduleRequest, RenameScheduleRequest, ScheduleRunRecordView,
    ScheduleView, SetScheduleClockTimeRequest, SetScheduleDailyTimeRequest,
    SetScheduleIntervalRequest, SetScheduleOnceAtRequest, SetSchedulePromptRequest,
};

#[cfg(test)]
mod tests;
