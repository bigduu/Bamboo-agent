//! Pure strict projection of child-reported content. No reported reference is read.
use serde::{
    de::{self, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::{json, Value};
use std::fmt;

pub const MAX_CHILD_RESULT_BYTES: usize = 8192;

pub fn unavailable_child_result(view: &str, reason: &str) -> Value {
    json!({"view":view,"version":1,"available":false,"reason":reason})
}

pub(super) fn lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Report {
    version: u32,
    outcome: Outcome,
    summary: String,
    reported_evidence: Vec<Evidence>,
    reported_verification: Vec<Verification>,
    proposals: Vec<String>,
    blockers: Vec<String>,
    open_decisions: Vec<String>,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Outcome {
    Completed,
    Partial,
    Blocked,
    Failed,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Evidence {
    description: String,
    reference: Option<String>,
    sha256: Option<String>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Verification {
    check: String,
    reported_status: VerificationStatus,
    details: String,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum VerificationStatus {
    Passed,
    Failed,
    NotRun,
    Unknown,
}

fn nonblank(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max
}
fn strings(values: &[String]) -> bool {
    values.len() <= 16 && values.iter().all(|value| nonblank(value, 1024))
}

/// Duplicate detection must precede Value's map construction, including nested maps.
struct Unique(Value);
impl<'de> Deserialize<'de> for Unique {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = Unique;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("unique JSON members")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Unique, E> {
                Ok(Unique(json!(v)))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Unique, E> {
                Ok(Unique(v.into()))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Unique, E> {
                Ok(Unique(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Unique, A::Error> {
                let mut values = Vec::new();
                while let Some(Unique(value)) = a.next_element()? {
                    values.push(value);
                }
                Ok(Unique(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Unique, A::Error> {
                let mut values = serde_json::Map::new();
                while let Some(key) = a.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate member"));
                    }
                    values.insert(key, a.next_value::<Unique>()?.0);
                }
                Ok(Unique(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(UniqueVisitor)
    }
}

pub(super) fn decode_report(content: &str) -> Result<Value, &'static str> {
    if content.len() > MAX_CHILD_RESULT_BYTES {
        return Err("result_budget_exceeded");
    }
    let malformed = || "report_malformed";
    let raw = serde_json::from_str::<Unique>(content)
        .map_err(|_| malformed())?
        .0;
    let report: Report = serde_json::from_value(raw.clone()).map_err(|_| malformed())?;
    let value = serde_json::to_value(&report).map_err(|_| malformed())?;
    // Option fields deserialize absent as None; exact shape comparison requires explicit null.
    if value != raw
        || report.version != 1
        || !nonblank(&report.summary, 2048)
        || report.reported_evidence.len() > 16
        || report.reported_verification.len() > 16
        || !strings(&report.proposals)
        || !strings(&report.blockers)
        || !strings(&report.open_decisions)
        || report.reported_evidence.iter().any(|e| {
            !nonblank(&e.description, 1024)
                || e.reference.as_ref().is_some_and(|s| !nonblank(s, 1024))
                || e.sha256.as_ref().is_some_and(|s| !lowercase_sha256(s))
        })
        || report
            .reported_verification
            .iter()
            .any(|v| !nonblank(&v.check, 256) || v.details.len() > 1024)
    {
        return Err(malformed());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn report() -> Value {
        json!({"version":1,"outcome":"blocked","summary":"quoted \"claim\" \\ 🪷\n",
            "reported_evidence":[{"description":"reported only","reference":"/never/open/this","sha256":null}],
            "reported_verification":[{"check":"test","reported_status":"not_run","details":""}],
            "proposals":[],"blockers":["Need an answer"],"open_decisions":[]})
    }
    #[test]
    fn strict_child_report_requires_every_member_and_explicit_null() {
        let value = report();
        assert_eq!(decode_report(&value.to_string()).unwrap(), value);
        for key in value.as_object().unwrap().keys() {
            let mut bad = value.clone();
            bad.as_object_mut().unwrap().remove(key);
            assert!(decode_report(&bad.to_string()).is_err(), "{key}");
        }
        for key in ["description", "reference", "sha256"] {
            let mut bad = value.clone();
            bad["reported_evidence"][0]
                .as_object_mut()
                .unwrap()
                .remove(key);
            assert!(decode_report(&bad.to_string()).is_err(), "{key}");
        }
        for key in ["check", "reported_status", "details"] {
            let mut bad = value.clone();
            bad["reported_verification"][0]
                .as_object_mut()
                .unwrap()
                .remove(key);
            assert!(decode_report(&bad.to_string()).is_err(), "{key}");
        }
        for (path, bad_value) in [
            ("version", json!(2)),
            ("version", json!(1.0)),
            ("outcome", json!("success")),
            ("summary", json!(null)),
            ("summary", json!("  ")),
            ("summary", json!("x".repeat(2049))),
            ("proposals", json!([""])),
            ("proposals", json!(vec!["a"; 17])),
            ("blockers", json!([3])),
            ("open_decisions", json!(["x".repeat(1025)])),
        ] {
            let mut bad = value.clone();
            bad[path] = bad_value;
            assert!(decode_report(&bad.to_string()).is_err(), "{path}");
        }
    }
    #[test]
    fn strict_child_report_rejects_duplicates_unknowns_and_extraction() {
        let text = report().to_string();
        for bad in [
            format!("{text} {{}}"),
            format!("```json\n{text}\n```"),
            format!("prose {text}"),
            text.replacen("\"version\":1", "\"version\":1,\"version\":1", 1),
            text.replacen("\"reference\":", "\"reference\":null,\"reference\":", 1),
            text.replacen("\"check\":", "\"check\":\"x\",\"check\":", 1),
            text.replacen("\"version\":1", "\"version\":1,\"verified\":true", 1),
            text.replacen(
                "\"description\":",
                "\"host_proof\":true,\"description\":",
                1,
            ),
            text.replacen("\"check\":", "\"run_id\":1,\"check\":", 1),
        ] {
            assert_eq!(decode_report(&bad).unwrap_err(), "report_malformed");
        }
        for (field, bad) in [
            ("reference", json!("")),
            ("reference", json!(7)),
            ("sha256", json!("A".repeat(64))),
            ("description", json!("x".repeat(1025))),
        ] {
            let mut value = report();
            value["reported_evidence"][0][field] = bad;
            assert!(decode_report(&value.to_string()).is_err());
        }
        for (field, bad) in [
            ("check", json!("x".repeat(257))),
            ("reported_status", json!("verified")),
            ("details", json!("x".repeat(1025))),
        ] {
            let mut value = report();
            value["reported_verification"][0][field] = bad;
            assert!(decode_report(&value.to_string()).is_err());
        }
        let escaped = text.replace('🪷', "\\ud83e\\udeb7");
        assert_eq!(decode_report(&escaped).unwrap(), report());
        assert_eq!(
            decode_report(&format!("{}{}", " ".repeat(8192), text)).unwrap_err(),
            "result_budget_exceeded"
        );
    }
}
