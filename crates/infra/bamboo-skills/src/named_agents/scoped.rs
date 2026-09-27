//! One immutable host observation, not a profile application or permission grant.
use super::*;
use bamboo_domain::ProjectId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NamedAgentProfileSource {
    Builtin,
    Global,
    Project,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NamedAgentProfileIdentity {
    pub name: String,
    pub source: NamedAgentProfileSource,
    pub project_id: Option<ProjectId>,
    pub revision: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NamedAgentProfileStatus {
    Selectable,
    Shadowed,
    Invalid,
    Conflict,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NamedAgentProfileMetadata {
    pub identity: Option<NamedAgentProfileIdentity>,
    pub description: Option<String>,
    pub status: NamedAgentProfileStatus,
    pub diagnostic_code: Option<NamedAgentDiagnosticCode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopedNamedAgentCatalogStatus {
    Available,
    Unavailable,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct ScopedNamedAgentCatalogMetadata {
    pub status: ScopedNamedAgentCatalogStatus,
    pub entries: Vec<NamedAgentProfileMetadata>,
    pub diagnostic_code: Option<NamedAgentDiagnosticCode>,
}

/// No Serialize or mutable definition access. Debug exposes the public projection only.
pub struct ScopedNamedAgentCatalog {
    definitions: BTreeMap<String, (NamedAgentProfileIdentity, NamedAgentDefinition)>,
    metadata: ScopedNamedAgentCatalogMetadata,
}

impl fmt::Debug for ScopedNamedAgentCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.metadata.fmt(f)
    }
}

impl ScopedNamedAgentCatalog {
    /// Roots and typed Project identity must come from host authority, never a
    /// request path. Missing agents is empty; no watcher, fallback or disk writer.
    pub fn discover(
        global_home: &Path,
        project: Option<(&ProjectId, &Path)>,
        limits: NamedAgentLimits,
    ) -> Result<Self, NamedAgentDiagnosticCode> {
        Self::discover_inner(global_home, project, limits, false)
    }

    /// Explicit default-role package: Project > Global > Builtin. Roots and
    /// Project identity remain host-authored; source/revision are exact identity.
    pub fn discover_with_builtins(
        global_home: &Path,
        project: Option<(&ProjectId, &Path)>,
        limits: NamedAgentLimits,
    ) -> Result<Self, NamedAgentDiagnosticCode> {
        Self::discover_inner(global_home, project, limits, true)
    }

    fn discover_inner(
        global_home: &Path,
        project: Option<(&ProjectId, &Path)>,
        limits: NamedAgentLimits,
        include_builtins: bool,
    ) -> Result<Self, NamedAgentDiagnosticCode> {
        if !limits.valid() {
            return Err(NamedAgentDiagnosticCode::InvalidLimits);
        }
        let mut budget = ScanBudget::default();
        let mut catalog = Self {
            definitions: BTreeMap::new(),
            metadata: ScopedNamedAgentCatalogMetadata {
                status: ScopedNamedAgentCatalogStatus::Available,
                entries: Vec::new(),
                diagnostic_code: None,
            },
        };
        if include_builtins {
            let layer = builtin::catalog(limits, &mut budget);
            if let Some(code) = layer.metadata.diagnostic_code {
                catalog.unavailable(code);
                return catalog.checked(limits);
            }
            catalog.add_layer(layer, NamedAgentProfileSource::Builtin, None);
        }
        let global = NamedAgentCatalog::discover_with_budget(global_home, limits, &mut budget);
        if let Some(code) = global.metadata.diagnostic_code {
            catalog.unavailable(code);
            return catalog.checked(limits);
        }
        let anonymous_global = global
            .metadata
            .entries
            .iter()
            .find(|entry| entry.name.is_none())
            .and_then(|entry| entry.diagnostic_code);
        catalog.add_layer(global, NamedAgentProfileSource::Global, None);
        if let Some(code) = anonymous_global.filter(|_| include_builtins) {
            catalog.unavailable(code);
            return catalog.checked(limits);
        }
        if let Some((id, home)) = project {
            let layer = NamedAgentCatalog::discover_with_budget(home, limits, &mut budget);
            if let Some(code) = layer.metadata.diagnostic_code {
                catalog.unavailable(code);
            } else {
                let anonymous = layer
                    .metadata
                    .entries
                    .iter()
                    .find(|entry| entry.name.is_none())
                    .and_then(|entry| entry.diagnostic_code);
                catalog.add_layer(layer, NamedAgentProfileSource::Project, Some(id));
                // An unknown higher-layer name cannot safely permit any fallback.
                if let Some(code) = anonymous {
                    catalog.unavailable(code);
                }
            }
        }
        catalog.checked(limits)
    }

    fn add_layer(
        &mut self,
        layer: NamedAgentCatalog,
        source: NamedAgentProfileSource,
        project_id: Option<&ProjectId>,
    ) {
        let names: BTreeSet<_> = layer
            .metadata
            .entries
            .iter()
            .filter_map(|entry| entry.name.as_ref())
            .cloned()
            .collect();
        if source != NamedAgentProfileSource::Builtin {
            for entry in &mut self.metadata.entries {
                if entry
                    .identity
                    .as_ref()
                    .is_some_and(|id| names.contains(&id.name))
                    && entry.status == NamedAgentProfileStatus::Selectable
                {
                    entry.status = NamedAgentProfileStatus::Shadowed;
                }
            }
            for name in &names {
                self.definitions.remove(name);
            }
        }
        for entry in layer.metadata.entries {
            let identity =
                entry
                    .name
                    .zip(entry.revision)
                    .map(|(name, revision)| NamedAgentProfileIdentity {
                        name,
                        source,
                        project_id: project_id.cloned(),
                        revision,
                    });
            self.metadata.entries.push(NamedAgentProfileMetadata {
                identity,
                description: entry.description,
                status: match entry.status {
                    NamedAgentStatus::Valid => NamedAgentProfileStatus::Selectable,
                    NamedAgentStatus::Invalid => NamedAgentProfileStatus::Invalid,
                    NamedAgentStatus::Conflict => NamedAgentProfileStatus::Conflict,
                },
                diagnostic_code: entry.diagnostic_code,
            });
        }
        for (name, definition) in layer.definitions {
            let identity = NamedAgentProfileIdentity {
                name: name.clone(),
                source,
                project_id: project_id.cloned(),
                revision: definition.revision.clone(),
            };
            self.definitions.insert(name, (identity, definition));
        }
    }

    fn unavailable(&mut self, code: NamedAgentDiagnosticCode) {
        self.definitions.clear();
        self.metadata.status = ScopedNamedAgentCatalogStatus::Unavailable;
        self.metadata.diagnostic_code = Some(code);
        for entry in &mut self.metadata.entries {
            if entry.status == NamedAgentProfileStatus::Selectable {
                entry.status = NamedAgentProfileStatus::Unavailable;
            }
        }
    }

    fn checked(self, limits: NamedAgentLimits) -> Result<Self, NamedAgentDiagnosticCode> {
        // Include retained identity strings as well as definitions and the
        // exact serialized public response. Tiny budgets may fit no response.
        let mut bytes = serde_json::to_vec(&self.metadata)
            .map_err(|_| NamedAgentDiagnosticCode::AggregateLimitExceeded)?
            .len();
        for (key, (identity, definition)) in &self.definitions {
            bytes = bytes.saturating_add(key.len() + definition.content_bytes());
            bytes = bytes.saturating_add(identity.name.len() + identity.revision.len());
            bytes =
                bytes.saturating_add(identity.project_id.as_ref().map_or(0, |p| p.as_str().len()));
        }
        if bytes > limits.max_publication_bytes {
            return Err(NamedAgentDiagnosticCode::AggregateLimitExceeded);
        }
        Ok(self)
    }

    pub fn metadata(&self) -> &ScopedNamedAgentCatalogMetadata {
        &self.metadata
    }

    /// Exact lookup within this retained observation. Never reopens a file or
    /// implies current authorization; a later scan may have a new revision.
    pub fn get(&self, identity: &NamedAgentProfileIdentity) -> Option<&NamedAgentDefinition> {
        self.definitions
            .get(&identity.name)
            .filter(|(current, _)| current == identity)
            .map(|(_, definition)| definition)
    }
}

#[cfg(test)]
#[path = "scoped_tests.rs"]
mod tests;
