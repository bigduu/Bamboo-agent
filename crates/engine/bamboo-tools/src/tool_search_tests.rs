use super::*;
use bamboo_domain::FunctionSchema;
use serde_json::json;

fn schema(name: &str, description: &str, parameters: Value) -> ClassifiedToolSchema {
    ClassifiedToolSchema::new(ToolSchema {
        schema_type: "function".to_string(),
        function: FunctionSchema {
            name: name.to_string(),
            description: description.to_string(),
            parameters,
        },
    })
    .unwrap()
}

fn names(tools: Vec<String>) -> Vec<String> {
    tools
}

#[test]
fn codex_metadata_expands_names_and_walks_parameter_descriptions_only() {
    let tool = schema(
        "create_event",
        "Create entries",
        json!({
            "type":"object", "description":"Calendar input", "properties":{
                "participants": {"type":"array", "items": {
                    "type":"object", "properties": {
                        "recipient": {"anyOf":[
                            {"type":"string", "description":"Email attendee"},
                            {"type":"object", "properties":{"timezone":{"description":"Regional offset"}}}
                        ]}
                    }
                }},
                "token":{"type":"string", "default":"secretdefault", "examples":["secretexample"],
                    "enum":["secretenum"]}
            }, "x-credential":"secretcredential"
        }),
    );
    let text = default_tool_search_text(tool.schema());
    assert!(text.contains("create_event create event"));
    for expected in [
        "Calendar input",
        "participants",
        "recipient",
        "Email attendee",
        "timezone",
        "Regional offset",
    ] {
        assert!(text.contains(expected), "missing {expected}");
    }
    for secret in [
        "secretdefault",
        "secretexample",
        "secretenum",
        "secretcredential",
    ] {
        assert!(!text.contains(secret), "indexed value {secret}");
    }
    let index = ToolSearchIndex::from_resolved_catalog(&[tool]);
    for query in ["timezone", "offset!", "attendee", "create event"] {
        assert_eq!(
            names(index.search(query, Some(1)).unwrap()),
            ["create_event"]
        );
    }
    assert!(index.search("secretcredential", None).unwrap().is_empty());
}

#[test]
fn bm25_ranks_relevant_metadata_with_exact_execution_identities() {
    let precise = schema(
        "lookup",
        "Weather forecast",
        json!({
            "type":"object", "properties":{"postal_code":{"type":"string", "description":"Forecast location"}},
            "required":["postal_code"], "additionalProperties":false
        }),
    );
    let broad = schema(
        "generic",
        "Weather reports alongside calendars scheduling contacts travel photos documents",
        json!({}),
    );
    let index = ToolSearchIndex::from_resolved_catalog(&[broad, precise.clone()]);
    let results = index.search("weather forecast", Some(2)).unwrap();
    assert_eq!(results[0], precise.execution_name());
    assert_eq!(results.len(), 2);
    assert!(index.search("zzqxvplmn", None).unwrap().is_empty());
}

#[test]
fn equal_scores_have_a_stable_identity_order_before_the_limit() {
    let catalog = (0..12)
        .rev()
        .map(|i| schema(&format!("action{i:02}"), "Forecast weather", json!({})))
        .collect::<Vec<_>>();
    let expected = vec!["action00", "action01", "action02", "action03", "action04"];
    for _ in 0..12 {
        let index = ToolSearchIndex::from_resolved_catalog(&catalog);
        assert_eq!(names(index.search("weather", None).unwrap()), expected);
    }
}

#[test]
fn only_current_deferred_tools_are_searchable_with_exact_first_aliases() {
    let catalog = [
        schema("Read", "Weather forecast", json!({})),
        schema("Workspace", "Weather forecast", json!({})),
        schema("Glob", "Find paths", json!({})),
        schema("glob", "A custom exact registration", json!({})),
        schema("weather", "Current forecast", json!({})),
    ];
    let index = ToolSearchIndex::from_resolved_catalog(&catalog);
    assert_eq!(names(index.search("weather", None).unwrap()), ["weather"]);
    assert_eq!(names(index.search("tool:glob", Some(1)).unwrap()), ["glob"]);
    let alias_only = ToolSearchIndex::from_resolved_catalog(&[catalog[2].clone()]);
    assert_eq!(names(alias_only.search("glob", Some(1)).unwrap()), ["Glob"]);
    let current = ToolSearchIndex::from_resolved_catalog(&[schema(
        "weather",
        "Calendar schedule",
        json!({}),
    )]);
    assert!(current.search("forecast", None).unwrap().is_empty());
    assert_eq!(
        names(current.search("schedule", None).unwrap()),
        ["weather"]
    );
    let removed = ToolSearchIndex::from_resolved_catalog(&[]);
    assert!(removed.search("weather", None).unwrap().is_empty());
}

#[test]
fn queries_and_results_share_bounded_host_limits() {
    let index = ToolSearchIndex::from_resolved_catalog(&[schema("lookup", "Weather", json!({}))]);
    assert!(matches!(
        index.search("  ", None),
        Err(ToolSearchError::EmptyQuery)
    ));
    assert!(matches!(
        index.search("weather", Some(0)),
        Err(ToolSearchError::InvalidLimit(0))
    ));
    assert!(matches!(
        index.search("weather", Some(6)),
        Err(ToolSearchError::InvalidLimit(6))
    ));
    assert!(matches!(
        index.search(&"界".repeat(257), None),
        Err(ToolSearchError::QueryTooLong {
            actual: 257,
            maximum: 256
        })
    ));
    assert!(index.search(&"界".repeat(256), None).is_ok());
}
