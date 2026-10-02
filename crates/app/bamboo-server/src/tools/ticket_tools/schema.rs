use serde_json::{json, Value};

fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object", "additionalProperties":false, "properties":properties, "required":required})
}
fn operation(name: &str, mut properties: Value, required: &[&str]) -> Value {
    properties["op"] = json!({"type":"string", "const":name});
    let mut fields = vec!["op"];
    fields.extend_from_slice(required);
    object(properties, &fields)
}
pub fn parameters(name: &str) -> Value {
    let id = json!({"type":"string", "minLength":1});
    let ids = json!({"type":"array", "items":id, "maxItems":32});
    let nullable_id = json!({"type":["string", "null"]});
    let cursor = json!({"anyOf":[{"type":"null"},object(json!({"commit":id,"query_hash":id,"offset":{"type":"integer","minimum":0}}), &["commit","query_hash","offset"])]});
    let limit = json!({"type":"integer", "minimum":1, "maximum":100});
    match name {
        "work_overview" => object(json!({}), &[]),
        "work_search" => object(
            json!({"filter":object(json!({
            "query":{"type":"string","maxLength":512}, "kind":{"type":["string","null"],"enum":["goal","work","step",null]},
            "state":{"type":["string","null"],"enum":["draft","ready","active","submitted","accepted","blocked","cancelled",null]},
            "updated_after":{"type":["integer","null"],"minimum":0}, "updated_before":{"type":["integer","null"],"minimum":0},
            "include_archived":{"type":"boolean"}}), &["query","kind","state","updated_after","updated_before","include_archived"]),
            "limit":limit,"cursor":cursor,"fixed_commit":nullable_id}),
            &["filter", "limit", "cursor", "fixed_commit"],
        ),
        "work_inspect" => object(
            json!({"ids":ids,"depth":{"type":"integer","minimum":0,"maximum":4},
            "sections":{"type":"array","uniqueItems":true,"maxItems":3,"items":{"type":"string","enum":["assignments","requests","submissions"]}},
            "budget_bytes":{"type":"integer","minimum":128,"maximum":65536},"fixed_commit":nullable_id}),
            &["ids", "sections", "depth", "budget_bytes", "fixed_commit"],
        ),
        "work_changes" => object(
            json!({"since_seq":{"type":"integer","minimum":0},"limit":limit,"cursor":cursor}),
            &["since_seq", "limit", "cursor"],
        ),
        _ => {
            let contract = object(
                json!({"title":id,"objective":id,
                "constraints":{"type":"array","items":{"type":"string"}}, "acceptance":{"type":"array","items":id,"minItems":1},
                "user_acceptance_required":{"type":"boolean"},"allowed_tools":{"type":"array","items":{"type":"string","enum":["Task"]},"minItems":1,"maxItems":1}}),
                &[
                    "title",
                    "objective",
                    "constraints",
                    "acceptance",
                    "user_acceptance_required",
                    "allowed_tools",
                ],
            );
            let evidence = json!({"type":"array","items":id,"minItems":1});
            let update = operation(
                "update_contract",
                json!({"work_id":id,"contract":contract}),
                &["work_id", "contract"],
            );
            let reopen = operation("reopen", json!({"work_id":id}), &["work_id"]);
            let operations = if name == "work_dispatch" {
                vec![
                    operation(
                        "start",
                        json!({"work_id":id,"temp_id":id,"workspace":{"type":"null","description":"MVP native Task-only route uses its Host workspace; coding/shell permissions are not provided."}}),
                        &["work_id", "temp_id", "workspace"],
                    ),
                    update,
                    operation("cancel", json!({"work_id":id}), &["work_id"]),
                    operation(
                        "pause",
                        json!({"work_id":id,"reason":id}),
                        &["work_id", "reason"],
                    ),
                    operation("ready", json!({"work_id":id}), &["work_id"]),
                    reopen,
                ]
            } else {
                vec![
                    operation(
                        "create",
                        json!({"temp_id":id,"kind":{"type":"string","enum":["goal","work","step"]},"parent":nullable_id,"contract":contract,
                    "depends_on":{"type":"array","items":id}}),
                        &["temp_id", "kind", "parent", "contract", "depends_on"],
                    ),
                    operation("ready", json!({"work_id":id}), &["work_id"]),
                    operation(
                        "set_dependencies",
                        json!({"work_id":id,"depends_on":{"type":"array","items":id}}),
                        &["work_id", "depends_on"],
                    ),
                    update,
                    operation(
                        "ask",
                        json!({"work_id":id,"temp_id":id,"prompt":id,"action":{"type":"null"}}),
                        &["work_id", "temp_id", "prompt", "action"],
                    ),
                    operation(
                        "accept",
                        json!({"work_id":id,"submission_id":id,"evidence":evidence}),
                        &["work_id", "submission_id", "evidence"],
                    ),
                    operation(
                        "accept_goal",
                        json!({"goal_id":id,"evidence":evidence}),
                        &["goal_id", "evidence"],
                    ),
                    operation(
                        "reject",
                        json!({"work_id":id,"submission_id":id,"reason":id}),
                        &["work_id", "submission_id", "reason"],
                    ),
                    reopen,
                    operation(
                        "archive",
                        json!({"ticket_id":id,"archived":{"type":"boolean"}}),
                        &["ticket_id", "archived"],
                    ),
                ]
            };
            object(
                json!({"operation_id":{"type":"string","minLength":1,"maxLength":256,"description":"Stable ID for exact input retry, never reuse for changed operations."},
                "expected_seq":{"type":"integer","minimum":0},"expected_epoch":{"type":"integer","minimum":1},
                "operations":{"type":"array","minItems":1,"maxItems":64,"items":{"oneOf":operations}}}),
                &[
                    "operation_id",
                    "expected_seq",
                    "expected_epoch",
                    "operations",
                ],
            )
        }
    }
}
