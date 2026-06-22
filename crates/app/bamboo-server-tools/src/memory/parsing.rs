//! Argument parsing / validation helpers for [`MemoryTool`].

use std::collections::HashSet;

use bamboo_agent_core::tools::ToolError;
use bamboo_memory::memory_store::{
    DurableMemoryStatus, DurableMemoryType, MemoryScope, TemporalGranularity,
};

use super::args::{FilterTypeSet, QueryFilters};
use super::MemoryTool;

impl MemoryTool {
    pub(super) fn parse_scope(scope: Option<&str>) -> Result<MemoryScope, ToolError> {
        match scope
            .unwrap_or("session")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "session" => Ok(MemoryScope::Session),
            "project" => Ok(MemoryScope::Project),
            "global" => Ok(MemoryScope::Global),
            other => Err(ToolError::InvalidArguments(format!(
                "invalid scope '{other}'; expected one of: session, project, global"
            ))),
        }
    }

    /// Parse an optional temporal granularity. `None`/empty stays `None` (the memory
    /// simply carries no temporal dimension), preserving back-compat. An unknown
    /// non-empty value is rejected so typos surface instead of silently dropping.
    pub(super) fn parse_granularity(
        granularity: Option<&str>,
    ) -> Result<Option<TemporalGranularity>, ToolError> {
        let Some(value) = granularity.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        TemporalGranularity::parse(value).map(Some).ok_or_else(|| {
            ToolError::InvalidArguments(format!(
                "invalid granularity '{value}'; expected one of: day, week, month, quarter, year"
            ))
        })
    }

    pub(super) fn parse_type(value: &str) -> Result<DurableMemoryType, ToolError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "user" => Ok(DurableMemoryType::User),
            "feedback" => Ok(DurableMemoryType::Feedback),
            "project" => Ok(DurableMemoryType::Project),
            "reference" => Ok(DurableMemoryType::Reference),
            other => Err(ToolError::InvalidArguments(format!(
                "invalid type '{other}'; expected one of: user, feedback, project, reference"
            ))),
        }
    }

    pub(super) fn parse_status(value: &str) -> Result<DurableMemoryStatus, ToolError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "active" => Ok(DurableMemoryStatus::Active),
            "stale" => Ok(DurableMemoryStatus::Stale),
            "superseded" => Ok(DurableMemoryStatus::Superseded),
            "contradicted" => Ok(DurableMemoryStatus::Contradicted),
            "archived" => Ok(DurableMemoryStatus::Archived),
            other => Err(ToolError::InvalidArguments(format!(
                "invalid status '{other}'; expected one of: active, stale, superseded, contradicted, archived"
            ))),
        }
    }

    pub(super) fn parse_query_filters(
        filters: Option<&QueryFilters>,
    ) -> Result<FilterTypeSet, ToolError> {
        let filter_types = filters
            .map(|value| {
                value
                    .r#type
                    .iter()
                    .map(|item| Self::parse_type(item))
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?;
        let filter_statuses = filters
            .map(|value| {
                value
                    .status
                    .iter()
                    .map(|item| Self::parse_status(item))
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?;
        Ok((filter_types, filter_statuses))
    }

    pub(super) fn parse_merge_mode(value: Option<&str>) -> Result<Option<String>, ToolError> {
        let Some(mode) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        let normalized = mode.to_ascii_lowercase();
        match normalized.as_str() {
            "semantic_merge" | "merge" | "contradict" => Ok(Some(normalized)),
            other => Err(ToolError::InvalidArguments(format!(
                "invalid merge mode '{other}'; expected one of: merge, semantic_merge, contradict"
            ))),
        }
    }
}
