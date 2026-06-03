//! Deserialization types for the unified `memory` tool arguments.

use std::collections::HashSet;

use serde::Deserialize;

use bamboo_memory::memory_store::{DurableMemoryStatus, DurableMemoryType};

pub(super) type FilterTypeSet = (
    Option<HashSet<DurableMemoryType>>,
    Option<HashSet<DurableMemoryStatus>>,
);

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(super) enum MemoryArgs {
    SessionRead {
        #[serde(default)]
        topic: Option<String>,
        #[serde(default)]
        options: Option<MemoryActionOptions>,
    },
    SessionAppend {
        #[serde(default)]
        topic: Option<String>,
        content: String,
    },
    SessionReplace {
        #[serde(default)]
        topic: Option<String>,
        content: String,
    },
    SessionClear {
        #[serde(default)]
        topic: Option<String>,
    },
    SessionListTopics,
    Query {
        scope: String,
        #[serde(default)]
        query: Option<String>,
        #[serde(default)]
        filters: Option<QueryFilters>,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        options: Option<MemoryActionOptions>,
    },
    Get {
        id: String,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        options: Option<MemoryActionOptions>,
    },
    Write {
        scope: String,
        #[serde(rename = "type")]
        r#type: String,
        title: String,
        content: String,
        #[serde(default)]
        tags: Vec<String>,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        options: Option<WriteOptions>,
    },
    Merge {
        id: String,
        content: String,
        #[serde(default)]
        tags: Vec<String>,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        source_memory_ids: Vec<String>,
        #[serde(default)]
        mode: Option<String>,
        #[serde(default)]
        reason: Option<String>,
    },
    Split {
        id: String,
        #[serde(default)]
        project_key: Option<String>,
        pieces: Vec<SplitPiece>,
    },
    FindDuplicates {
        scope: String,
        title: String,
        #[serde(default)]
        content: Option<String>,
        #[serde(rename = "type", default)]
        r#type: Option<String>,
        #[serde(default)]
        tags: Vec<String>,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        options: Option<MemoryActionOptions>,
    },
    ScanBlobs {
        scope: String,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        min_sections: Option<usize>,
        #[serde(default)]
        options: Option<MemoryActionOptions>,
    },
    Purge {
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        scope: Option<String>,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        project_key: Option<String>,
        #[serde(default)]
        filters: Option<QueryFilters>,
        #[serde(default)]
        mode: Option<String>,
    },
    Inspect {
        scope: String,
        #[serde(default)]
        project_key: Option<String>,
    },
    Rebuild {
        scope: String,
        #[serde(default)]
        project_key: Option<String>,
    },
}

#[derive(Debug, Deserialize, Default)]
pub(super) struct MemoryActionOptions {
    #[serde(default)]
    pub(super) limit: Option<usize>,
    #[serde(default)]
    pub(super) max_chars: Option<usize>,
    #[serde(default)]
    pub(super) cursor: Option<String>,
    #[serde(default)]
    pub(super) include_related: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
pub(super) struct QueryFilters {
    #[serde(default)]
    pub(super) r#type: Vec<String>,
    #[serde(default)]
    pub(super) status: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(super) struct WriteOptions {
    #[serde(default)]
    pub(super) allow_merge_if_similar: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SplitPiece {
    pub(super) title: String,
    #[serde(rename = "type", default)]
    pub(super) r#type: Option<String>,
    pub(super) content: String,
    #[serde(default)]
    pub(super) tags: Vec<String>,
}
