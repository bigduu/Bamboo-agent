//! Deserialization types for the session inspector tool arguments.

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(super) enum SessionInspectorArgs {
    /// Materialize a bounded, immutable observation of this root or its tree.
    ExportContext { session_id: String },

    /// List sessions from the global index, with filtering/pagination.
    List {
        #[serde(default)]
        query: Option<String>,
        #[serde(default)]
        kind: Option<String>, // "root" | "child"
        #[serde(default)]
        pinned: Option<bool>,
        #[serde(default)]
        parent_session_id: Option<String>,
        #[serde(default)]
        root_session_id: Option<String>,
        #[serde(default)]
        created_by_schedule_id: Option<String>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        offset: Option<usize>,
    },

    /// Get a single session's index metadata (fast).
    GetMeta { session_id: String },

    /// Read message slice from a session, bounded by limit/offset and content truncation.
    ReadMessages {
        session_id: String,
        /// Read from end of the conversation (default true).
        #[serde(default)]
        from_end: Option<bool>,
        /// Offset from the chosen side (0 = start/end depending on from_end).
        #[serde(default)]
        offset: Option<usize>,
        /// Number of messages to return.
        #[serde(default)]
        limit: Option<usize>,
        /// Truncate each message content to at most this many chars.
        #[serde(default)]
        truncate_chars: Option<usize>,
        /// Include System messages (default false).
        #[serde(default)]
        include_system: Option<bool>,
        /// Include Tool role messages (default true).
        #[serde(default)]
        include_tool: Option<bool>,
        /// Include Assistant tool_calls and tool_call_id fields (default false).
        #[serde(default)]
        include_tool_calls: Option<bool>,
        /// Include image URLs found in content_parts (default true).
        #[serde(default)]
        include_image_urls: Option<bool>,
    },

    /// Read compressed historical context from local SQLite index cache.
    ReadCompressedCache {
        session_id: String,
        #[serde(default)]
        offset: Option<usize>,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        truncate_chars: Option<usize>,
        #[serde(default)]
        include_summary: Option<bool>,
    },

    /// Search session titles and (optionally) tail messages.
    Search {
        query: String,
        /// Search mode.
        /// - "title" (default): search in titles only
        /// - "tail_messages": also scan the last `tail_messages` messages of each candidate
        #[serde(default)]
        mode: Option<String>,
        /// Max sessions to scan when mode != "title".
        #[serde(default)]
        max_sessions: Option<usize>,
        /// Tail message count per session when scanning content.
        #[serde(default)]
        tail_messages: Option<usize>,
        /// Case-sensitive search (default false).
        #[serde(default)]
        case_sensitive: Option<bool>,
        /// Max matches to return.
        #[serde(default)]
        max_matches: Option<usize>,
    },
}
