//! Bounded named-agent definitions. These host-only catalogs grant no authority.
//!
//! See `docs/design/named-agent-definitions-v1.md` for the file contract and
//! platform boundary. Only the public metadata projections are serializable.

mod builtin;
mod parser;
mod reader;
mod scoped;

pub use scoped::{
    NamedAgentProfileIdentity, NamedAgentProfileMetadata, NamedAgentProfileSource,
    NamedAgentProfileStatus, ScopedNamedAgentCatalog, ScopedNamedAgentCatalogMetadata,
    ScopedNamedAgentCatalogStatus,
};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use serde::Serialize;

/// Hard ceilings; a caller may only tighten these budgets.
#[derive(Debug, Clone, Copy)]
pub struct NamedAgentLimits {
    pub max_candidates: usize,
    pub max_scan_entries: usize,
    pub max_file_bytes: usize,
    pub max_prompt_bytes: usize,
    pub max_publication_bytes: usize,
}

impl Default for NamedAgentLimits {
    fn default() -> Self {
        Self {
            max_candidates: 128,
            max_scan_entries: 1_024,
            max_file_bytes: 65_536,
            max_prompt_bytes: 49_152,
            max_publication_bytes: 1_048_576,
        }
    }
}

#[derive(Default)]
struct ScanBudget {
    candidates: usize,
    entries: usize,
    read_bytes: usize,
}

impl NamedAgentLimits {
    fn valid(self) -> bool {
        let hard = Self::default();
        self.max_candidates > 0
            && self.max_candidates <= hard.max_candidates
            && self.max_scan_entries >= self.max_candidates
            && self.max_scan_entries <= hard.max_scan_entries
            && self.max_file_bytes > 0
            && self.max_file_bytes <= hard.max_file_bytes
            && self.max_prompt_bytes > 0
            && self.max_prompt_bytes <= hard.max_prompt_bytes
            && self.max_publication_bytes > 0
            && self.max_publication_bytes <= hard.max_publication_bytes
    }
}

/// Static codes only: no filesystem errors, paths, filenames or source excerpts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NamedAgentDiagnosticCode {
    UnsupportedPlatform,
    InvalidLimits,
    PathEscape,
    UnsafePath,
    RootUnavailable,
    ReadFailed,
    NotRegularFile,
    CandidateLimitExceeded,
    ScanLimitExceeded,
    FileTooLarge,
    AggregateLimitExceeded,
    InvalidUtf8,
    MissingFrontmatter,
    MalformedFrontmatter,
    UnsupportedSchemaVersion,
    InvalidName,
    InvalidDescription,
    InvalidModelHint,
    InvalidToolDeclarations,
    EmptyPrompt,
    PromptTooLarge,
    CredentialMaterial,
    DuplicateName,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NamedAgentStatus {
    Valid,
    Invalid,
    Conflict,
}

/// An invalid source is anonymous; even its filename may contain a credential.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NamedAgentMetadata {
    pub name: Option<String>,
    pub description: Option<String>,
    pub revision: Option<String>,
    pub status: NamedAgentStatus,
    pub diagnostic_code: Option<NamedAgentDiagnosticCode>,
}

impl NamedAgentMetadata {
    fn invalid(code: NamedAgentDiagnosticCode) -> Self {
        Self {
            name: None,
            description: None,
            revision: None,
            status: NamedAgentStatus::Invalid,
            diagnostic_code: Some(code),
        }
    }
}

/// Internal display/inspection data, never a model-facing instruction or tool list.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct NamedAgentCatalogMetadata {
    pub entries: Vec<NamedAgentMetadata>,
    pub diagnostic_code: Option<NamedAgentDiagnosticCode>,
}

/// Host-private content. Deliberately has no Serialize implementation.
pub struct NamedAgentDefinition {
    name: String,
    description: String,
    revision: String,
    model_hint: Option<String>,
    tool_allow: Vec<String>,
    tool_deny: Vec<String>,
    system_prompt: String,
}

impl NamedAgentDefinition {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// A routing hint only. Resolving it requires separate host policy.
    pub fn model_hint(&self) -> Option<&str> {
        self.model_hint.as_deref()
    }

    /// Declarations only; these neither grant tools nor override host permissions.
    pub fn tool_allow(&self) -> &[String] {
        &self.tool_allow
    }

    pub fn tool_deny(&self) -> &[String] {
        &self.tool_deny
    }

    pub fn system_prompt(&self) -> &str {
        &self.system_prompt
    }

    fn metadata(&self) -> NamedAgentMetadata {
        NamedAgentMetadata {
            name: Some(self.name.clone()),
            description: Some(self.description.clone()),
            revision: Some(self.revision.clone()),
            status: NamedAgentStatus::Valid,
            diagnostic_code: None,
        }
    }

    fn content_bytes(&self) -> usize {
        self.name.len()
            + self.description.len()
            + self.revision.len()
            + self.model_hint.as_ref().map_or(0, String::len)
            + self.tool_allow.iter().map(String::len).sum::<usize>()
            + self.tool_deny.iter().map(String::len).sum::<usize>()
            + self.system_prompt.len()
    }
}

impl fmt::Debug for NamedAgentDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.metadata().fmt(formatter)
    }
}

/// One immutable scan. There is no watcher, fallback generation or disk writer.
pub struct NamedAgentCatalog {
    definitions: BTreeMap<String, NamedAgentDefinition>,
    metadata: NamedAgentCatalogMetadata,
}

impl fmt::Debug for NamedAgentCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.metadata.fmt(formatter)
    }
}

impl NamedAgentCatalog {
    /// Read the stabilized global data directory, not a Project/plugin source.
    pub fn load_configured() -> Self {
        Self::discover(
            &bamboo_config::paths::bamboo_dir(),
            NamedAgentLimits::default(),
        )
    }

    /// `data_root` must come from trusted host configuration, never a tool request.
    pub fn discover(data_root: &Path, limits: NamedAgentLimits) -> Self {
        Self::discover_with_budget(data_root, limits, &mut ScanBudget::default())
    }

    fn discover_with_budget(
        data_root: &Path,
        limits: NamedAgentLimits,
        budget: &mut ScanBudget,
    ) -> Self {
        if !limits.valid() {
            return Self::rejected(NamedAgentDiagnosticCode::InvalidLimits);
        }
        let directory = match reader::AgentDirectory::open(data_root) {
            Ok(Some(directory)) => directory,
            Ok(None) => return Self::empty(),
            Err(code) => return Self::rejected(code),
        };
        let candidates = match directory.candidates(limits, budget) {
            Ok(candidates) => candidates,
            Err(code) => return Self::rejected(code),
        };
        let mut definitions = Vec::new();
        let mut entries = Vec::new();
        for candidate in candidates {
            let bytes = match directory.read(
                &candidate,
                limits.max_file_bytes,
                limits.max_publication_bytes - budget.read_bytes,
            ) {
                Ok(bytes) => bytes,
                Err((code, partial_read_bytes)) => {
                    budget.read_bytes += partial_read_bytes;
                    if budget.read_bytes > limits.max_publication_bytes
                        || code == NamedAgentDiagnosticCode::AggregateLimitExceeded
                    {
                        return Self::rejected(NamedAgentDiagnosticCode::AggregateLimitExceeded);
                    }
                    entries.push(NamedAgentMetadata::invalid(code));
                    continue;
                }
            };
            // Count actual read lengths, including invalid sources, not stat sizes.
            budget.read_bytes += bytes.len();
            if budget.read_bytes > limits.max_publication_bytes {
                return Self::rejected(NamedAgentDiagnosticCode::AggregateLimitExceeded);
            }
            if bytes.len() > limits.max_file_bytes {
                entries.push(NamedAgentMetadata::invalid(
                    NamedAgentDiagnosticCode::FileTooLarge,
                ));
                continue;
            }
            match parser::parse(&bytes, limits) {
                Ok(definition) => definitions.push(definition),
                Err(code) => entries.push(NamedAgentMetadata::invalid(code)),
            }
        }
        let mut seen = BTreeSet::new();
        let mut duplicates = BTreeSet::new();
        for definition in &definitions {
            if !seen.insert(definition.name.clone()) {
                duplicates.insert(definition.name.clone());
            }
        }
        let mut published = BTreeMap::new();
        for definition in definitions {
            let mut metadata = definition.metadata();
            if duplicates.contains(&definition.name) {
                metadata.status = NamedAgentStatus::Conflict;
                metadata.diagnostic_code = Some(NamedAgentDiagnosticCode::DuplicateName);
            } else {
                published.insert(definition.name.clone(), definition);
            }
            entries.push(metadata);
        }
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        let metadata = NamedAgentCatalogMetadata {
            entries,
            diagnostic_code: None,
        };
        // The publication budget also covers the actual serialized metadata and
        // all retained definition strings; anonymous invalid rows have a cost.
        let publication_bytes = serde_json::to_vec(&metadata).map_or(usize::MAX, |v| v.len());
        let publication_bytes = published
            .values()
            .fold(publication_bytes, |sum, definition| {
                sum.saturating_add(definition.content_bytes())
            });
        if publication_bytes > limits.max_publication_bytes {
            return Self::rejected(NamedAgentDiagnosticCode::AggregateLimitExceeded);
        }
        Self {
            definitions: published,
            metadata,
        }
    }

    pub fn metadata(&self) -> &NamedAgentCatalogMetadata {
        &self.metadata
    }

    /// Exact validated name only; conflicting/invalid definitions are unavailable.
    pub fn get(&self, name: &str) -> Option<&NamedAgentDefinition> {
        self.definitions.get(name)
    }

    fn empty() -> Self {
        Self {
            definitions: BTreeMap::new(),
            metadata: NamedAgentCatalogMetadata::default(),
        }
    }

    fn rejected(code: NamedAgentDiagnosticCode) -> Self {
        Self {
            metadata: NamedAgentCatalogMetadata {
                diagnostic_code: Some(code),
                ..Default::default()
            },
            ..Self::empty()
        }
    }
}

#[cfg(test)]
mod tests;
