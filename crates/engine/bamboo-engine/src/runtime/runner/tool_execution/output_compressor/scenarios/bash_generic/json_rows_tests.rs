use super::{compress, json_rows, CompressionTier, MAX_STDOUT_LINES};
use crate::auto_dream_privacy::contains_secret_like_value;
use crate::runtime::runner::tool_execution::output_compressor;
use crate::runtime::runner::tool_execution::per_call::ToolExecutionOutcome;
use bamboo_agent_core::tools::ToolResult;
use bamboo_compression::{TiktokenTokenCounter, TokenCounter};
use serde_json::{json, Value};

fn envelope(stdout: &str, stderr: &str) -> String {
    json!({
        "command": "cat records.json",
        "stdout": stdout,
        "stderr": stderr,
        "exit_code": 0,
        "timed_out": false,
        "stdout_truncated": false,
        "stderr_truncated": false,
        "extra": {"pid": 123, "complete": true},
    })
    .to_string()
}

fn sample_rows(count: usize) -> Vec<Value> {
    (0..count)
        .map(|index| {
            json!({
                "canonical_session_message_sequence_number": index,
                "completed_tool_execution_round_number": index % 7,
                "effective_project_authority_scope_identifier": "project-a",
                "expected_runtime_discovery_catalog_revision": 3,
                "host_observed_tool_output_token_count": 32,
                "provider_native_transcript_replay_epoch": 1,
                "provider_scoped_runtime_model_identifier": "model-a",
                "reconciled_workflow_activation_generation": 2,
                "session_owned_context_binding_is_current": true,
                "source_anchored_tool_call_is_complete": true,
                "tool_result_reported_optional_explanation": null,
                "user_visible_current_activity_description": format!("item-{index:03}"),
            })
        })
        .collect()
}

fn read_table(table: &str) -> Vec<Value> {
    let mut lines = table.lines();
    assert!(lines.next().unwrap().starts_with("JSON table ("));
    let columns: Vec<String> = lines
        .next()
        .unwrap()
        .split('\t')
        .map(|cell| serde_json::from_str(cell).unwrap())
        .collect();
    lines
        .map(|line| {
            let values: Vec<Value> = line
                .split('\t')
                .map(|cell| serde_json::from_str(cell).unwrap())
                .collect();
            assert_eq!(columns.len(), values.len());
            Value::Object(columns.iter().cloned().zip(values).collect())
        })
        .collect()
}

#[test]
fn json_rows_preserve_order_and_repeats_without_text_deduplication() {
    let mut rows = sample_rows(30);
    rows[1] = rows[0].clone();
    rows[2] = rows[0].clone();
    rows[3] = rows[0].clone();
    rows[4] = rows[0].clone();
    let stdout = serde_json::to_string_pretty(&rows).unwrap();
    let result = compress(&envelope(&stdout, ""), CompressionTier::Standard);
    let parsed: Value = serde_json::from_str(&result.compressed).unwrap();
    assert!(result.was_compressed);
    assert_eq!(read_table(parsed["stdout"].as_str().unwrap()), rows);
    assert!(!result.compressed.contains("identical lines collapsed"));
}

#[test]
fn json_rows_keep_exact_numeric_literals_and_escaped_strings() {
    let text = "行\t\"quoted\"|\\\nnext";
    let encoded = serde_json::to_string(text).unwrap();
    let row = format!(
        r#"{{"scalar_integer_value":18446744073709551616,"scalar_decimal_value":0.12345678901234567890123456789,"scalar_exponent_value":1e400,"scalar_boolean_value":true,"scalar_null_value":null,"scalar_string_value":{encoded}}}"#
    );
    let stdout = format!("[{}]", vec![row; 40].join(","));
    let result = compress(&envelope(&stdout, ""), CompressionTier::Standard);
    let parsed: Value = serde_json::from_str(&result.compressed).unwrap();
    let table = parsed["stdout"].as_str().unwrap();
    let mut lines = table.lines();
    assert!(lines.next().unwrap().contains("40 rows"));
    let columns: Vec<String> = lines
        .next()
        .unwrap()
        .split('\t')
        .map(|cell| serde_json::from_str(cell).unwrap())
        .collect();
    let data: Vec<&str> = lines.collect();
    assert_eq!(data.len(), 40);
    for line in data {
        let cells: std::collections::BTreeMap<_, _> = columns
            .iter()
            .map(String::as_str)
            .zip(line.split('\t'))
            .collect();
        assert_eq!(cells["scalar_integer_value"], "18446744073709551616");
        assert_eq!(
            cells["scalar_decimal_value"],
            "0.12345678901234567890123456789"
        );
        assert_eq!(cells["scalar_exponent_value"], "1e400");
        assert_eq!(cells["scalar_boolean_value"], "true");
        assert_eq!(cells["scalar_null_value"], "null");
        assert_eq!(cells["scalar_string_value"], encoded);
        assert_eq!(
            serde_json::from_str::<String>(cells["scalar_string_value"]).unwrap(),
            text
        );
    }
}

#[test]
fn json_rows_encode_unusual_headers_and_accept_different_field_order() {
    let key = "header\tline\n\"quoted\"|列";
    let row = json!({(key): "", "other_long_column_name": false});
    let rows = vec![row; 40];
    let mut stdout = serde_json::to_string_pretty(&rows).unwrap();
    // Field order is not part of the supported array's column identity.
    stdout.push(' ');
    let table = json_rows::compact(&stdout).unwrap();
    assert!(table
        .lines()
        .nth(1)
        .unwrap()
        .contains(&serde_json::to_string(key).unwrap()));
    assert_eq!(read_table(&table), rows);
    let differently_ordered =
        "[{\"long_first_column\":1,\"long_second_column\":2},{\"long_second_column\":4,\"long_first_column\":3}]";
    let padded = format!(
        "[{}]",
        vec![&differently_ordered[1..differently_ordered.len() - 1]; 30].join(",")
    );
    assert_eq!(
        read_table(&json_rows::compact(&padded).unwrap()),
        serde_json::from_str::<Vec<Value>>(&padded).unwrap()
    );
}

#[test]
fn json_rows_small_and_unsupported_shapes_retain_the_generic_path() {
    let small = envelope(r#"[{"a":1},{"a":2}]"#, "");
    let result = compress(&small, CompressionTier::Standard);
    assert!(!result.was_compressed);
    assert_eq!(result.compressed, small);

    for stdout in [
        "[]",
        "[{}]",
        r#"[{"a":1},{"b":2}]"#,
        r#"[{"a":[]},{"a":[]}]"#,
        r#"[{"a":{"v":1}},{"a":{"v":2}}]"#,
        "[1,2]",
        r#"[{"a":1,"a":2},{"a":3}]"#,
        "[",
    ] {
        assert!(json_rows::compact(stdout).is_none(), "{stdout}");
        let stderr = "a normal diagnostic line\n".repeat(100);
        let result = compress(&envelope(stdout, &stderr), CompressionTier::Standard);
        let parsed: Value = serde_json::from_str(&result.compressed).unwrap();
        assert_eq!(parsed["stdout"], format!("{stdout}\n"));
        assert!(!result.compressed.contains("JSON table ("));
    }
}

#[test]
fn json_rows_preserve_bash_metadata_and_existing_stderr_processing() {
    let stdout = serde_json::to_string_pretty(&sample_rows(30)).unwrap();
    let stderr = "\x1b[33mrepeated warning\x1b[0m\n".repeat(100);
    let original = envelope(&stdout, &stderr);
    let result = compress(&original, CompressionTier::Standard);
    let mut parsed: Value = serde_json::from_str(&result.compressed).unwrap();
    let mut expected: Value = serde_json::from_str(&original).unwrap();
    let text_result = compress(
        &envelope("plain stdout", &stderr),
        CompressionTier::Standard,
    );
    let text_parsed: Value = serde_json::from_str(&text_result.compressed).unwrap();
    assert_eq!(parsed["stderr"], text_parsed["stderr"]);
    parsed.as_object_mut().unwrap().remove("stdout");
    parsed.as_object_mut().unwrap().remove("stderr");
    expected.as_object_mut().unwrap().remove("stdout");
    expected.as_object_mut().unwrap().remove("stderr");
    assert_eq!(parsed, expected);
}

#[test]
fn json_rows_still_obey_generic_line_and_byte_caps() {
    let stdout = serde_json::to_string_pretty(&sample_rows(300)).unwrap();
    let result = compress(&envelope(&stdout, ""), CompressionTier::Aggressive);
    let parsed: Value = serde_json::from_str(&result.compressed).unwrap();
    let table = parsed["stdout"].as_str().unwrap();
    assert!(table.contains("lines omitted"));
    assert!(table.lines().count() <= MAX_STDOUT_LINES + 5);
    assert!(table.contains("item-000"));
    assert!(table.contains("item-299"));

    let rows = vec![json!({"long_scalar_column_name": "汉".repeat(20_000)}); 4];
    let stdout = serde_json::to_string(&rows).unwrap();
    let result = compress(&envelope(&stdout, ""), CompressionTier::Aggressive);
    let parsed: Value = serde_json::from_str(&result.compressed).unwrap();
    let table = parsed["stdout"].as_str().unwrap();
    assert!(table.contains("output truncated"));
    assert!(table.len() <= output_compressor::filters::DEFAULT_MAX_BYTES);
}

#[test]
fn json_rows_runtime_and_screened_tee_roundtrip() {
    let root = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("json_rows_tests::json_rows_runtime_child")
        .arg("--nocapture")
        .env("BAMBOO_DATA_DIR", root.path())
        .env("BAMBOO_JSON_ROWS_TEST", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "isolated JSON rows runtime failed: {}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
}

#[tokio::test]
async fn json_rows_runtime_child() {
    if std::env::var_os("BAMBOO_JSON_ROWS_TEST").is_none() {
        return;
    }
    let root = std::path::PathBuf::from(std::env::var_os("BAMBOO_DATA_DIR").unwrap());
    assert_eq!(bamboo_config::paths::bamboo_dir(), root);
    let counter = TiktokenTokenCounter::default();
    for (session, credential) in [("json-rows-safe", false), ("json-rows-credential", true)] {
        let mut rows = sample_rows(170);
        if credential {
            rows[0]["user_visible_current_activity_description"] =
                Value::String("Authorization: Bearer synthetic-credential-value".into());
        }
        let stdout = serde_json::to_string_pretty(&rows).unwrap();
        let canonical_raw = envelope(&stdout, "ordinary diagnostic\n");
        // The legacy Pgpass guard screens long physical lines with four colons
        // (#1694). This equivalent JSON encoding keeps that rule unchanged.
        let encoded_stdout = serde_json::to_string(&stdout).unwrap();
        let raw =
            serde_json::to_string_pretty(&serde_json::from_str::<Value>(&canonical_raw).unwrap())
                .unwrap()
                .replace(&encoded_stdout, &encoded_stdout.replace(':', "\\u003a"));
        assert_eq!(
            serde_json::from_str::<Value>(&raw).unwrap(),
            serde_json::from_str::<Value>(&canonical_raw).unwrap()
        );
        assert_eq!(contains_secret_like_value(&raw), credential);
        // Use the ordinary encoding as the baseline so escaping cannot inflate
        // the claimed token savings.
        let original_tokens = counter.count_text(&canonical_raw);
        assert!(
            original_tokens >= 10_000,
            "fixture reaches semantic compression tier"
        );
        let outcome = ToolExecutionOutcome {
            output_cap: None,
            portable_tool: None,
            permission_replay_origin: None,
            result: Ok(ToolResult::text(true, raw.clone())),
            needs_human: None,
            post_tool_hook_eligible: true,
            tool_duration: std::time::Duration::ZERO,
        };
        let output = output_compressor::maybe_compress(
            "Bash",
            r#"{"command":"cat records.json"}"#,
            session,
            outcome,
            u32::MAX,
            None,
            None,
        )
        .await;
        let result = output.result.unwrap();
        assert!(result.success);
        let (compressed, note) = result.result.split_once("\n\n").unwrap();
        let parsed: Value = serde_json::from_str(compressed).unwrap();
        assert_eq!(read_table(parsed["stdout"].as_str().unwrap()), rows);
        assert!(
            counter.count_text(&result.result) * 100 <= original_tokens * 70,
            "real runtime output, including tee hint, saves at least 30%"
        );
        let files: Vec<_> = std::fs::read_dir(root.join("tee").join(session))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1);
        let path = &files[0];
        assert!(note.contains(&bamboo_config::paths::path_to_display_string(path)));
        let stored = tokio::fs::read(path).await.unwrap();
        if credential {
            assert_eq!(
                stored,
                b"[tee output omitted: credential-like content detected]\n"
            );
            assert!(note.contains("credential-like output was omitted"));
            assert!(!note.contains("complete output"));
        } else {
            assert_eq!(stored, raw.as_bytes());
            assert!(note.contains("no credential-like content detected"));
        }
    }
}
