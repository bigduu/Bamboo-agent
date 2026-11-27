use chrono::{DateTime, TimeDelta, Utc};

use crate::api::types::{
    CreateScheduleRequest, ScheduleDailyTime, ScheduleRunConfigReq, ScheduleTriggerReq,
    ScheduleWeekdayReq,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ScheduleTriggerKind {
    #[default]
    Cron,
    Once,
    Interval,
    Daily,
    Weekly,
    Monthly,
}

impl ScheduleTriggerKind {
    const ORDER: [Self; 6] = [
        Self::Cron,
        Self::Once,
        Self::Interval,
        Self::Daily,
        Self::Weekly,
        Self::Monthly,
    ];

    fn is_calendar(self) -> bool {
        matches!(self, Self::Daily | Self::Weekly | Self::Monthly)
    }

    fn label(self) -> &'static str {
        match self {
            Self::Cron => "Cron",
            Self::Once => "Once",
            Self::Interval => "Interval",
            Self::Daily => "Daily",
            Self::Weekly => "Weekly",
            Self::Monthly => "Monthly",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScheduleField {
    Name,
    Trigger,
    Timing,
    Days,
    Prompt,
}

/// In-progress schedule creation opened with `n` on the Schedules tab.
#[derive(Default)]
pub struct ScheduleForm {
    pub name: String,
    pub timing: String,
    pub days: String,
    pub prompt: String,
    pub trigger: ScheduleTriggerKind,
    pub field: usize,
    pub error: Option<String>,
}

impl ScheduleForm {
    pub fn fields(&self) -> &'static [ScheduleField] {
        use ScheduleField::{Days, Name, Prompt, Timing, Trigger};
        match self.trigger {
            ScheduleTriggerKind::Weekly | ScheduleTriggerKind::Monthly => {
                &[Name, Trigger, Timing, Days, Prompt]
            }
            _ => &[Name, Trigger, Timing, Prompt],
        }
    }

    pub fn field_label(&self, field: ScheduleField) -> &'static str {
        match field {
            ScheduleField::Name => "Name",
            ScheduleField::Trigger => "Trigger",
            ScheduleField::Timing => match self.trigger {
                ScheduleTriggerKind::Cron => "Cron expression",
                ScheduleTriggerKind::Once => "Run at",
                ScheduleTriggerKind::Interval => "Every (seconds)",
                _ => "Time (UTC)",
            },
            ScheduleField::Days => match self.trigger {
                ScheduleTriggerKind::Weekly => "Weekdays",
                _ => "Days of month",
            },
            ScheduleField::Prompt => "Prompt",
        }
    }

    pub fn field_value(&self, field: ScheduleField) -> &str {
        match field {
            ScheduleField::Name => &self.name,
            ScheduleField::Trigger => self.trigger.label(),
            ScheduleField::Timing => &self.timing,
            ScheduleField::Days => &self.days,
            ScheduleField::Prompt => &self.prompt,
        }
    }

    pub fn hint(&self) -> &'static str {
        match self.trigger {
            ScheduleTriggerKind::Cron => {
                "UTC cron order is second, minute, hour, day, month, weekday"
            }
            ScheduleTriggerKind::Once => "Future RFC3339 timestamp with an offset (Z or +08:00)",
            ScheduleTriggerKind::Interval => "Positive whole seconds between runs",
            ScheduleTriggerKind::Daily => "HH:MM[:SS] UTC",
            ScheduleTriggerKind::Weekly => "HH:MM[:SS] UTC · e.g. mon, wed, fri",
            ScheduleTriggerKind::Monthly => "HH:MM[:SS] UTC · days such as 1, 15, 31",
        }
    }

    pub fn current_mut(&mut self) -> Option<&mut String> {
        match self.fields().get(self.field)? {
            ScheduleField::Name => Some(&mut self.name),
            ScheduleField::Trigger => None,
            ScheduleField::Timing => Some(&mut self.timing),
            ScheduleField::Days => Some(&mut self.days),
            ScheduleField::Prompt => Some(&mut self.prompt),
        }
    }

    pub fn move_focus(&mut self, direction: isize) {
        self.field = shifted_index(self.field, direction, self.fields().len());
    }

    pub fn change_trigger(&mut self, direction: isize) {
        if self.fields().get(self.field) != Some(&ScheduleField::Trigger) {
            return;
        }
        let order = ScheduleTriggerKind::ORDER;
        let index = order.iter().position(|kind| *kind == self.trigger).unwrap();
        let next = order[shifted_index(index, direction, order.len())];
        if next == self.trigger {
            return;
        }
        if !(self.trigger.is_calendar() && next.is_calendar()) {
            self.timing.clear();
        }
        self.days.clear();
        self.trigger = next;
        self.error = None;
    }

    pub fn request(
        &self,
        project_id: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<CreateScheduleRequest, &'static str> {
        let name = self.name.trim();
        if name.is_empty() {
            return Err("Enter a schedule name");
        }
        let prompt = self.prompt.trim();
        if prompt.is_empty() {
            return Err("Enter a task prompt");
        }
        let timing = self.timing.trim();
        let trigger = match self.trigger {
            ScheduleTriggerKind::Cron => {
                if timing.is_empty() {
                    return Err("Enter a cron expression");
                }
                ScheduleTriggerReq::Cron {
                    expr: timing.to_owned(),
                }
            }
            ScheduleTriggerKind::Once => {
                let at = DateTime::parse_from_rfc3339(timing)
                    .map_err(|_| "Enter an RFC3339 timestamp with an offset (Z or +08:00)")?
                    .with_timezone(&Utc);
                if at <= now {
                    return Err("Once must be scheduled for a future time");
                }
                ScheduleTriggerReq::Once { at }
            }
            ScheduleTriggerKind::Interval => {
                let every_seconds = timing
                    .parse::<u64>()
                    .ok()
                    .filter(|seconds| *seconds > 0)
                    .ok_or("Enter a positive whole number of seconds")?;
                i64::try_from(every_seconds)
                    .ok()
                    .and_then(TimeDelta::try_seconds)
                    .and_then(|delta| now.checked_add_signed(delta))
                    .ok_or("Interval is too large to compute its next run")?;
                ScheduleTriggerReq::Interval { every_seconds }
            }
            ScheduleTriggerKind::Daily => {
                let (hour, minute, second) = parse_clock(timing)?;
                ScheduleTriggerReq::Daily {
                    hour,
                    minute,
                    second,
                }
            }
            ScheduleTriggerKind::Weekly => {
                let (hour, minute, second) = parse_clock(timing)?;
                ScheduleTriggerReq::Weekly {
                    weekdays: parse_days(
                        &self.days,
                        parse_weekday,
                        "Enter weekdays such as mon, wed, fri",
                    )?,
                    hour,
                    minute,
                    second,
                }
            }
            ScheduleTriggerKind::Monthly => {
                let (hour, minute, second) = parse_clock(timing)?;
                ScheduleTriggerReq::Monthly {
                    days: parse_days(
                        &self.days,
                        |day| day.parse::<u8>().ok().filter(|d| (1..=31).contains(d)),
                        "Enter days of the month from 1 to 31",
                    )?,
                    hour,
                    minute,
                    second,
                }
            }
        };
        Ok(CreateScheduleRequest {
            name: name.to_owned(),
            trigger,
            timezone: self.trigger.is_calendar().then_some("UTC"),
            enabled: true,
            run_config: ScheduleRunConfigReq {
                project_id,
                task_message: Some(prompt.to_owned()),
                auto_execute: true,
            },
        })
    }
}

fn shifted_index(index: usize, direction: isize, len: usize) -> usize {
    (index % len + direction.rem_euclid(len as isize) as usize) % len
}

fn parse_clock(timing: &str) -> Result<(u8, u8, u8), &'static str> {
    parse_daily_clock(timing)
        .map(|time| (time.hour, time.minute, time.second))
        .ok_or("Enter a UTC time in HH:MM or HH:MM:SS format")
}

pub(crate) fn parse_daily_clock(input: &str) -> Option<ScheduleDailyTime> {
    let component = |part: &str| {
        if part.len() != 2 || !part.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        part.parse::<u8>().ok()
    };
    let mut parts = input.trim().split(':');
    let hour = component(parts.next()?)?;
    let minute = component(parts.next()?)?;
    let second = component(parts.next().unwrap_or("00"))?;
    if parts.next().is_some() {
        return None;
    }
    ScheduleDailyTime::new(hour, minute, second)
}

fn parse_days<T>(
    input: &str,
    parse: impl Fn(&str) -> Option<T>,
    error: &'static str,
) -> Result<Vec<T>, &'static str> {
    let days = input
        .split(|character: char| character == ',' || character.is_whitespace())
        .filter(|part| !part.is_empty())
        .map(|part| parse(part).ok_or(error))
        .collect::<Result<Vec<_>, _>>()?;
    if days.is_empty() {
        return Err(error);
    }
    Ok(days)
}

fn parse_weekday(day: &str) -> Option<ScheduleWeekdayReq> {
    use ScheduleWeekdayReq::{Fri, Mon, Sat, Sun, Thu, Tue, Wed};
    match day.to_ascii_lowercase().as_str() {
        "mon" | "monday" => Some(Mon),
        "tue" | "tuesday" => Some(Tue),
        "wed" | "wednesday" => Some(Wed),
        "thu" | "thursday" => Some(Thu),
        "fri" | "friday" => Some(Fri),
        "sat" | "saturday" => Some(Sat),
        "sun" | "sunday" => Some(Sun),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn form(trigger: ScheduleTriggerKind, timing: &str, days: &str) -> ScheduleForm {
        ScheduleForm {
            name: "  Weekly report  ".into(),
            timing: timing.into(),
            days: days.into(),
            prompt: "  Summarize Project changes.  ".into(),
            trigger,
            ..ScheduleForm::default()
        }
    }

    fn now() -> DateTime<Utc> {
        "2030-02-03T00:00:00Z".parse().unwrap()
    }

    #[test]
    fn daily_clock_parses_wall_time_and_preserves_creation_validation() {
        assert_eq!(parse_daily_clock("08:15"), ScheduleDailyTime::new(8, 15, 0));
        assert_eq!(
            parse_daily_clock(" 23:59:59 "),
            ScheduleDailyTime::new(23, 59, 59)
        );
        for invalid in [
            "",
            "8:15",
            "08:5",
            "24:00",
            "08:60",
            "08:15:60",
            "08:15:00:00",
            "08:15Z",
        ] {
            assert_eq!(parse_daily_clock(invalid), None, "{invalid:?}");
        }
        assert_eq!(
            parse_clock("24:00"),
            Err("Enter a UTC time in HH:MM or HH:MM:SS format")
        );
    }

    #[test]
    fn all_six_schedule_triggers_match_the_create_request_wire() {
        use ScheduleTriggerKind::{Cron, Daily, Interval, Monthly, Once, Weekly};
        for (kind, timing, days, trigger, timezone) in [
            (
                Cron,
                "  0 0 9 * * MON  ",
                "",
                json!({"type": "cron", "expr": "0 0 9 * * MON"}),
                None,
            ),
            (
                Once,
                "2030-02-03T09:10:11+08:00",
                "",
                json!({"type": "once", "at": "2030-02-03T01:10:11Z"}),
                None,
            ),
            (
                Interval,
                "3600",
                "",
                json!({"type": "interval", "every_seconds": 3600}),
                None,
            ),
            (
                Daily,
                "09:30",
                "",
                json!({"type": "daily", "hour": 9, "minute": 30, "second": 0}),
                Some("UTC"),
            ),
            (
                Weekly,
                "09:30:15",
                "Mon, Wednesday   FRI",
                json!({"type": "weekly", "weekdays": ["mon", "wed", "fri"], "hour": 9, "minute": 30, "second": 15}),
                Some("UTC"),
            ),
            (
                Monthly,
                "09:30:15",
                "1, 15 31",
                json!({"type": "monthly", "days": [1, 15, 31], "hour": 9, "minute": 30, "second": 15}),
                Some("UTC"),
            ),
        ] {
            let form = form(kind, timing, days);
            let request = form.request(Some("project-id".into()), now()).unwrap();
            let actual = serde_json::to_value(request).unwrap();
            let mut expected = json!({
                "name": "Weekly report",
                "trigger": trigger,
                "enabled": true,
                "run_config": {
                    "project_id": "project-id",
                    "task_message": "Summarize Project changes.",
                    "auto_execute": true,
                },
            });
            if let Some(timezone) = timezone {
                expected["timezone"] = json!(timezone);
            }
            assert_eq!(actual, expected, "{kind:?}");
            let unassigned = serde_json::to_value(form.request(None, now()).unwrap()).unwrap();
            assert!(unassigned["run_config"].get("project_id").is_none());
        }
    }

    #[test]
    fn invalid_fields_do_not_make_create_requests() {
        use ScheduleTriggerKind::{Cron, Daily, Interval, Monthly, Once, Weekly};
        for (kind, timing, days, expected) in [
            (Cron, "  ", "", "Enter a cron expression"),
            (
                Once,
                "2030-02-04T01:02:03",
                "",
                "Enter an RFC3339 timestamp with an offset (Z or +08:00)",
            ),
            (
                Once,
                "2030-02-03T00:00:00Z",
                "",
                "Once must be scheduled for a future time",
            ),
            (
                Once,
                "2030-02-03T07:59:59+08:00",
                "",
                "Once must be scheduled for a future time",
            ),
            (
                Interval,
                "0",
                "",
                "Enter a positive whole number of seconds",
            ),
            (
                Interval,
                "-1",
                "",
                "Enter a positive whole number of seconds",
            ),
            (
                Interval,
                "1.5",
                "",
                "Enter a positive whole number of seconds",
            ),
            (
                Interval,
                "18446744073709551615",
                "",
                "Interval is too large to compute its next run",
            ),
            (Weekly, "09:30", "", "Enter weekdays such as mon, wed, fri"),
            (
                Weekly,
                "09:30",
                "mon,mondayish",
                "Enter weekdays such as mon, wed, fri",
            ),
            (
                Monthly,
                "09:30",
                ", ",
                "Enter days of the month from 1 to 31",
            ),
            (
                Monthly,
                "09:30",
                "0, 4",
                "Enter days of the month from 1 to 31",
            ),
            (
                Monthly,
                "09:30",
                "15,32",
                "Enter days of the month from 1 to 31",
            ),
        ] {
            let result = form(kind, timing, days).request(None, now());
            assert_eq!(result.unwrap_err(), expected, "{kind:?}, {timing}, {days}");
        }
        for timing in ["9:05", "09:5", "24:00", "13:60", "07:00:60", "19:30:00:00"] {
            let result = form(Daily, timing, "").request(None, now());
            assert_eq!(
                result.unwrap_err(),
                "Enter a UTC time in HH:MM or HH:MM:SS format",
                "{timing}"
            );
        }
        let mut form = form(Cron, "0 0 9 * * MON", "");
        form.prompt.clear();
        assert_eq!(
            form.request(None, now()).unwrap_err(),
            "Enter a task prompt"
        );
        form.name.clear();
        assert_eq!(
            form.request(None, now()).unwrap_err(),
            "Enter a schedule name"
        );
    }

    #[test]
    fn changing_trigger_updates_fields_and_keeps_compatible_drafts() {
        use ScheduleField::{Days, Prompt, Trigger};
        use ScheduleTriggerKind::{Cron, Monthly, Weekly};
        let mut form = form(Cron, "0 0 9 * * MON", "");
        assert_eq!(ScheduleForm::default().trigger, Cron);
        form.change_trigger(-1);
        assert_eq!(form.trigger, Cron);
        form.move_focus(-1);
        assert_eq!(form.fields()[form.field], Prompt);
        form.move_focus(2);
        assert_eq!(form.fields()[form.field], Trigger);
        assert!(form.current_mut().is_none());
        form.error = Some("old error".into());
        form.change_trigger(-1);
        assert_eq!(form.trigger, Monthly);
        assert!(form.timing.is_empty());
        assert!(form.error.is_none());
        assert!(form.fields().contains(&Days));

        form.timing = "09:30".into();
        form.days = "1, 15, 31".into();
        form.change_trigger(-1);
        assert_eq!(form.trigger, Weekly);
        assert_eq!(form.timing, "09:30");
        assert!(form.days.is_empty());
        form.move_focus(2);
        assert_eq!(form.fields()[form.field], Days);
        form.current_mut().unwrap().push_str("mon");
        assert_eq!(form.days, "mon");
        form.move_focus(-2);
        form.change_trigger(2);
        assert_eq!(form.trigger, Cron);
        assert!(!form.fields().contains(&Days));
        assert!(form.days.is_empty());
        assert!(form.timing.is_empty());
        assert_eq!(form.name, "  Weekly report  ");
        assert_eq!(form.prompt, "  Summarize Project changes.  ");
    }
}
