//! Compiled private role package; declarations never grant runtime capabilities.
use super::*;

const DOCUMENTS: [&[u8]; 3] = [
    include_bytes!("builtin/explorer.md"),
    include_bytes!("builtin/implementer.md"),
    include_bytes!("builtin/reviewer.md"),
];
const MAX_BUILTIN_PROMPT_BYTES: usize = 4_096;

pub(super) fn catalog(limits: NamedAgentLimits, budget: &mut ScanBudget) -> NamedAgentCatalog {
    let mut catalog = NamedAgentCatalog::empty();
    for bytes in DOCUMENTS {
        budget.candidates = budget.candidates.saturating_add(1);
        if budget.candidates > limits.max_candidates {
            return NamedAgentCatalog::rejected(NamedAgentDiagnosticCode::CandidateLimitExceeded);
        }
        if bytes.len() > limits.max_file_bytes {
            return NamedAgentCatalog::rejected(NamedAgentDiagnosticCode::FileTooLarge);
        }
        // Static text consumed by the catalog shares the retained text budget;
        // this is not a claim of additional filesystem I/O.
        budget.read_bytes = budget.read_bytes.saturating_add(bytes.len());
        if budget.read_bytes > limits.max_publication_bytes {
            return NamedAgentCatalog::rejected(NamedAgentDiagnosticCode::AggregateLimitExceeded);
        }
        let definition = match parser::parse(
            bytes,
            NamedAgentLimits {
                max_prompt_bytes: limits.max_prompt_bytes.min(MAX_BUILTIN_PROMPT_BYTES),
                ..limits
            },
        ) {
            Ok(definition) => definition,
            Err(code) => return NamedAgentCatalog::rejected(code),
        };
        catalog.metadata.entries.push(definition.metadata());
        catalog
            .definitions
            .insert(definition.name.clone(), definition);
    }
    catalog
}

#[cfg(test)]
#[path = "builtin_tests.rs"]
mod tests;
