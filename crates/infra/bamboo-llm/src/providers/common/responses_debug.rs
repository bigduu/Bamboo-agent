use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::provider::Result;
use crate::types::LLMChunk;

static RESPONSES_DEBUG_LOG_PATH: OnceLock<PathBuf> = OnceLock::new();
static RESPONSES_DEBUG_ENABLED: OnceLock<bool> = OnceLock::new();

fn parse_env_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn debug_enabled() -> bool {
    *RESPONSES_DEBUG_ENABLED.get_or_init(|| {
        let enabled_by_flag = std::env::var("BAMBOO_RESPONSES_DEBUG")
            .ok()
            .is_some_and(|value| parse_env_truthy(&value));

        let enabled_by_path = std::env::var("BAMBOO_RESPONSES_DEBUG_FILE")
            .ok()
            .map(|value| value.trim().to_string())
            .is_some_and(|value| !value.is_empty());

        enabled_by_flag || enabled_by_path
    })
}

fn debug_log_path() -> &'static PathBuf {
    RESPONSES_DEBUG_LOG_PATH.get_or_init(|| {
        let from_env = std::env::var("BAMBOO_RESPONSES_DEBUG_FILE")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);

        from_env.unwrap_or_else(|| std::env::temp_dir().join("bamboo-responses-debug.log"))
    })
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn escaped(text: &str) -> String {
    text.replace('\n', "\\n").replace('\r', "\\r")
}

/// Append one raw /responses SSE parse record to the debug file.
///
/// This is intentionally best-effort and must never break the stream.
pub fn append_responses_sse_record(
    provider: &str,
    model: &str,
    event: &str,
    data: &str,
    parsed: &Result<Option<LLMChunk>>,
) {
    if !debug_enabled() {
        return;
    }

    let path = debug_log_path();
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(_) => return,
    };

    let parsed_summary = match parsed {
        Ok(Some(chunk)) => format!("ok:{chunk:?}"),
        Ok(None) => "ok:none".to_string(),
        Err(error) => format!("err:{}", escaped(error.to_string().as_str())),
    };

    let _ = writeln!(
        file,
        "ts_ms={} provider={} model={} event={} parsed={} data={}",
        now_millis(),
        escaped(provider),
        escaped(model),
        escaped(event),
        parsed_summary,
        escaped(data),
    );
}
