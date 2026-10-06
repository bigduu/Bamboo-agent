//! Name matching for protected legacy Skill history. Consumers own role policy.

use bamboo_domain::canonical_tool_name;

/// Transient choice preserving each consumer's existing matching semantics.
#[derive(Debug, Clone, Copy)]
pub(super) enum SkillNameMatch {
    Exact,
    Canonical,
}

pub(super) fn is_skill_tool_name(name: &str, matching: SkillNameMatch) -> bool {
    let matches_skill = |name: &str| matches!(name, "load_skill" | "read_skill_resource");
    match matching {
        SkillNameMatch::Exact => matches_skill(name),
        SkillNameMatch::Canonical => matches_skill(&canonical_tool_name(name)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_skill_name_matching_preserves_exact_and_domain_canonical_boundaries() {
        for (name, exact, canonical) in [
            ("load_skill", true, true),
            ("read_skill_resource", true, true),
            ("LOAD_SKILL", false, true),
            ("READ_SKILL_RESOURCE", false, true),
            ("default::LoAd_SkIlL", false, true),
            ("outer::inner::read_skill_resource", false, true),
            (" read_skill_resource ", false, true),
            ("namespace:: LOAD_SKILL ", false, true),
            ("mcp__server__load_skill", false, false),
            ("mcp__server__read_skill_resource", false, false),
            ("preload_skill", false, false),
            ("read_skill_resource_extra", false, false),
            ("load_skill()", false, false),
            ("skills_list", false, false),
            ("skills_read", false, false),
            ("namespace::load_skill::", false, false),
            ("", false, false),
            ("  ", false, false),
        ] {
            assert_eq!(
                is_skill_tool_name(name, SkillNameMatch::Exact),
                exact,
                "{name:?}"
            );
            assert_eq!(
                is_skill_tool_name(name, SkillNameMatch::Canonical),
                canonical,
                "{name:?}"
            );
        }
    }
}
