//! Reject oversized arguments before invoking a tool; never truncate a request.

use super::ToolError;
use serde_json::Value;

const DEFAULT_LIMIT: usize = 256 * 1024;
const WRITE_LIMIT: usize = 1024 * 1024;
const QUERY_LIMIT: usize = 64 * 1024;
// A 1 MiB file needs 1,398,104 base64 bytes. The remaining budget covers the
// bounded selector, basename, MIME type, epoch, and JSON escaping.
const BROWSER_FILE_INPUT_WIRE_LIMIT: usize = 1_405_000;

/// Limits use resolved execution identities, so a namespaced custom tool cannot
/// inherit the policy of an unrelated built-in with the same suffix.
pub fn tool_input_limit(execution_name: &str) -> usize {
    match execution_name {
        "Write" | "Edit" | "NotebookEdit" => WRITE_LIMIT,
        "Bash" | "Read" | "Grep" | "Glob" => QUERY_LIMIT,
        _ => DEFAULT_LIMIT,
    }
}

pub fn check_raw_tool_input(execution_name: &str, raw: &str) -> Result<(), ToolError> {
    if execution_name == "browser"
        && raw.len() > DEFAULT_LIMIT
        && raw.len() <= BROWSER_FILE_INPUT_WIRE_LIMIT
    {
        // Only this action gets the larger wire budget. Invalid JSON and every
        // other browser action retain the ordinary limit.
        if let Ok(args) = serde_json::from_str::<Value>(raw) {
            return check_size(
                execution_name,
                raw.len(),
                input_limit(execution_name, &args),
            );
        }
    }
    check_size(execution_name, raw.len(), tool_input_limit(execution_name))
}

/// Check synthesized/preparsed arguments too. The wire check alone does not
/// constrain values supplied by callers through ToolExecutionContext.
pub fn check_parsed_tool_input(execution_name: &str, args: &Value) -> Result<(), ToolError> {
    let size = serde_json::to_vec(args)
        .map_err(|_| ToolError::InvalidArguments("Could not measure tool arguments".into()))?
        .len();
    check_size(execution_name, size, input_limit(execution_name, args))
}

fn input_limit(execution_name: &str, args: &Value) -> usize {
    if execution_name == "browser"
        && args.get("action").and_then(Value::as_str) == Some("set_file_input")
    {
        BROWSER_FILE_INPUT_WIRE_LIMIT
    } else {
        tool_input_limit(execution_name)
    }
}

fn check_size(execution_name: &str, size: usize, limit: usize) -> Result<(), ToolError> {
    if size <= limit {
        return Ok(());
    }
    let advice = match execution_name {
        "Write" | "Edit" | "NotebookEdit" => "Split the content into smaller writes or patches.",
        "Bash" | "js_repl" => "Move large inline content into a file and shorten the command.",
        _ => "Split the request or pass a file reference instead of inline content.",
    };
    Err(ToolError::InvalidArguments(format!(
        "Tool {execution_name} arguments contain {size} bytes; the limit is {limit} bytes. {advice}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn exact_boundary_is_allowed_but_one_more_byte_is_rejected() {
        let raw = "x".repeat(tool_input_limit("Bash"));
        assert!(check_raw_tool_input("Bash", &raw).is_ok());
        let error = check_raw_tool_input("Bash", &(raw + "x")).unwrap_err();
        assert!(error.to_string().contains("65537 bytes"));
    }

    #[test]
    fn utf8_bytes_and_preparsed_values_are_both_bounded() {
        assert!(check_raw_tool_input("Read", &"字".repeat(30_000)).is_err());
        assert!(check_parsed_tool_input("Read", &json!({"path":"x".repeat(QUERY_LIMIT)})).is_err());
        assert!(check_parsed_tool_input("Write", &json!({"content":"字".repeat(30_000)})).is_ok());
    }

    #[test]
    fn custom_names_do_not_inherit_builtin_limits() {
        assert_eq!(tool_input_limit("Write"), WRITE_LIMIT);
        assert_eq!(tool_input_limit("vendor::Write"), DEFAULT_LIMIT);
        assert_eq!(tool_input_limit("BashOutput"), DEFAULT_LIMIT);
        assert_eq!(tool_input_limit("vendor::browser"), DEFAULT_LIMIT);
    }

    #[test]
    fn only_browser_file_input_gets_the_one_mebibyte_wire_budget() {
        let data_base64 = format!("{}==", "A".repeat((1024_usize * 1024).div_ceil(3) * 4 - 2));
        let args = json!({
            "action": "set_file_input",
            "selector": "#upload",
            "filename": "sample.bin",
            "mime_type": "application/octet-stream",
            "data_base64": data_base64,
            "expected_epoch": 42,
        });
        let raw = args.to_string();
        assert!(raw.len() > DEFAULT_LIMIT);
        assert!(check_raw_tool_input("browser", &raw).is_ok());
        assert!(check_parsed_tool_input("browser", &args).is_ok());

        let ordinary = json!({"action":"navigate", "url":"x".repeat(DEFAULT_LIMIT)});
        assert!(check_raw_tool_input("browser", &ordinary.to_string()).is_err());
        assert!(check_parsed_tool_input("browser", &ordinary).is_err());
        let malformed = format!(
            "{{\"action\":\"set_file_input\",\"data_base64\":\"{}",
            "A".repeat(DEFAULT_LIMIT)
        );
        assert!(check_raw_tool_input("browser", &malformed).is_err());
        assert!(check_raw_tool_input("vendor::browser", &raw).is_err());
        assert!(check_parsed_tool_input("vendor::browser", &args).is_err());

        let oversized = json!({"action":"set_file_input", "data_base64":"A".repeat(BROWSER_FILE_INPUT_WIRE_LIMIT)});
        assert!(check_raw_tool_input("browser", &oversized.to_string()).is_err());
        assert!(check_parsed_tool_input("browser", &oversized).is_err());
    }
}
