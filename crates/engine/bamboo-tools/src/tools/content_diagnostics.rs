use serde_json::{json, Value};
use std::path::Path;

pub fn attach_file_diagnostics(payload: &mut Value, path: &Path, content: &str) {
    if let Some(diagnostics) = diagnostics_for_file(path, content) {
        payload["diagnostics"] = diagnostics;
    }
}

pub fn diagnostics_for_file(path: &Path, content: &str) -> Option<Value> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())?;

    match extension.as_str() {
        "json" => Some(match serde_json::from_str::<serde_json::Value>(content) {
            Ok(_) => json!({
                "format": "json",
                "valid": true
            }),
            Err(error) => json!({
                "format": "json",
                "valid": false,
                "message": error.to_string(),
                "line": error.line(),
                "column": error.column()
            }),
        }),
        "yaml" | "yml" => Some(match serde_yaml::from_str::<serde_yaml::Value>(content) {
            Ok(_) => json!({
                "format": "yaml",
                "valid": true
            }),
            Err(error) => {
                let mut payload = json!({
                    "format": "yaml",
                    "valid": false,
                    "message": error.to_string()
                });
                if let Some(location) = error.location() {
                    payload["line"] = json!(location.line());
                    payload["column"] = json!(location.column());
                }
                payload
            }
        }),
        "toml" => Some(match toml::from_str::<toml::Value>(content) {
            Ok(_) => json!({
                "format": "toml",
                "valid": true
            }),
            Err(error) => json!({
                "format": "toml",
                "valid": false,
                "message": error.to_string()
            }),
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostics_report_json_parse_error() {
        let diagnostics = diagnostics_for_file(Path::new("/tmp/test.json"), "{")
            .expect("json diagnostics should exist");
        assert_eq!(diagnostics["format"], "json");
        assert_eq!(diagnostics["valid"], false);
        assert!(diagnostics["message"]
            .as_str()
            .unwrap_or_default()
            .contains("EOF"));
    }

    #[test]
    fn diagnostics_skip_unsupported_extensions() {
        let diagnostics = diagnostics_for_file(Path::new("/tmp/test.txt"), "plain");
        assert!(diagnostics.is_none());
    }
}
