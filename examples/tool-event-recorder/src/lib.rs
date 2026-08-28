//! Minimal ToolEvent sink implemented only against Bamboo's public wire crate.
//!
//! The host owns process supervision and authorization. This crate reads one
//! projected ToolEvent JSON object per stdin line and appends validated events
//! to a caller-selected NDJSON file. It deliberately has no dependency on the
//! Bamboo server, plugin installer, or tool executor.

use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use bamboo_plugin_protocol::ProjectedToolEventV1;
use serde::Deserialize;

pub const SERVICE_CONFIG_ENV: &str = "BAMBOO_PLUGIN_SERVICE_CONFIG";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecorderConfig {
    output_path: PathBuf,
    startup_log_path: PathBuf,
    #[serde(default)]
    startup_delay_ms: u64,
    #[serde(default)]
    crash_once_marker_path: Option<PathBuf>,
}

/// Run the native recorder using the service-config path supplied by Bamboo.
pub fn run_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path = std::env::var_os(SERVICE_CONFIG_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "service config is not set"))?;
    let config: RecorderConfig = serde_json::from_slice(&fs::read(config_path)?).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "service config is not valid recorder JSON",
        )
    })?;
    run(config, io::stdin().lock())
}

fn run(
    config: RecorderConfig,
    input: impl BufRead,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ensure_parent(&config.output_path)?;
    ensure_parent(&config.startup_log_path)?;
    append_startup(&config.startup_log_path)?;

    let crash_after_first_event = match &config.crash_once_marker_path {
        Some(marker) => {
            ensure_parent(marker)?;
            match OpenOptions::new().write(true).create_new(true).open(marker) {
                Ok(mut marker_file) => {
                    marker_file.write_all(b"crash armed\n")?;
                    marker_file.flush()?;
                    true
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => false,
                Err(error) => return Err(error.into()),
            }
        }
        None => false,
    };

    if config.startup_delay_ms > 0 {
        std::thread::sleep(Duration::from_millis(config.startup_delay_ms));
    }

    let output = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&config.output_path)?;
    let mut output = BufWriter::new(output);
    for line in input.lines() {
        let line = line?;
        let event: ProjectedToolEventV1 = match serde_json::from_str(&line) {
            Ok(event) => event,
            Err(_) => {
                eprintln!("tool-event-recorder ignored malformed JSON input");
                continue;
            }
        };
        if event.validate_bounds().is_err() {
            eprintln!("tool-event-recorder ignored invalid ToolEventV1 input");
            continue;
        }
        serde_json::to_writer(&mut output, &event)?;
        output.write_all(b"\n")?;
        output.flush()?;
        if crash_after_first_event {
            // Exercise the supervisor's unexpected-exit path. The event is
            // flushed first, so the fixture can distinguish restart from loss.
            std::process::exit(23);
        }
    }
    Ok(())
}

fn ensure_parent(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn append_startup(path: &Path) -> io::Result<()> {
    let mut output = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(output, "{}", std::process::id())?;
    output.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bamboo_plugin_protocol::{
        tool_event_v1_schema, TOOL_EVENT_PATH_REDACTION_PERMISSION_NOT_GRANTED,
    };

    #[test]
    fn checked_in_schema_is_generated_from_the_public_protocol() {
        let checked_in: serde_json::Value =
            serde_json::from_str(include_str!("../schema/tool-event-v1.schema.json")).unwrap();
        assert_eq!(checked_in, tool_event_v1_schema());
    }

    #[test]
    fn metadata_only_golden_round_trips_without_observation_fields() {
        let raw = include_str!("../examples/metadata-only.file-changed.json");
        let event: ProjectedToolEventV1 = serde_json::from_str(raw).unwrap();
        event.validate_bounds().unwrap();
        assert!(event.context.tool_name.is_none());
        assert!(event.data.path.is_none());
        assert!(event.data.diff.is_none());
        assert!(event.data.content.is_none());
        assert_eq!(
            event.data.path_redaction_reason.as_deref(),
            Some(TOOL_EVENT_PATH_REDACTION_PERMISSION_NOT_GRANTED)
        );

        let wire = serde_json::to_value(event).unwrap();
        assert!(wire["context"].get("tool_name").is_none());
        assert!(wire["data"].get("path").is_none());
    }
}
