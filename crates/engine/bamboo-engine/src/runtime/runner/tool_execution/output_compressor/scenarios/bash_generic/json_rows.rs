//! Compact repeated JSON field names without changing scalar values.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

// Borrow scalar bytes instead of parsing numbers through f64. Duplicate fields
// are outside the supported shape and fall back to the existing text path.
struct Row<'a>(BTreeMap<String, &'a RawValue>);

impl<'de> Deserialize<'de> for Row<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct RowVisitor;

        impl<'de> Visitor<'de> for RowVisitor {
            type Value = Row<'de>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON object with unique fields")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut values = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, &'de RawValue>()? {
                    if values.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate JSON field"));
                    }
                }
                Ok(Row(values))
            }
        }

        deserializer.deserialize_map(RowVisitor)
    }
}

/// Same-field object arrays become TSV with JSON-encoded headers and raw scalar
/// cells. Tabs, newlines and quotes stay escaped; row order and repeats stay intact.
/// The caller retains its existing line/byte caps and tee recovery behavior.
pub(super) fn compact(stdout: &str) -> Option<String> {
    let rows: Vec<Row<'_>> = serde_json::from_str(stdout).ok()?;
    let first = rows.first()?;
    if first.0.is_empty() {
        return None;
    }
    let columns: Vec<&String> = first.0.keys().collect();
    for row in &rows {
        if !row.0.keys().eq(columns.iter().copied())
            || row
                .0
                .values()
                .any(|value| matches!(value.get().as_bytes().first(), Some(b'[' | b'{')))
        {
            return None;
        }
    }

    let mut table = format!(
        "JSON table ({} rows; tab-separated JSON headers and scalar cells)\n",
        rows.len()
    );
    for (index, column) in columns.iter().enumerate() {
        if index > 0 {
            table.push('\t');
        }
        table.push_str(&serde_json::to_string(column).ok()?);
    }
    for row in &rows {
        table.push('\n');
        for (index, value) in row.0.values().enumerate() {
            if index > 0 {
                table.push('\t');
            }
            table.push_str(value.get());
        }
    }
    (table.len() < stdout.len()).then_some(table)
}
