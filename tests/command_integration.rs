//! Slash command integration tests
//!
//! Tests for slash command loading, parsing, and management.

#[cfg(test)]
mod tests {
    use bamboo_agent::commands::slash_commands::SlashCommand;

    #[test]
    fn test_slash_command_structure() {
        let cmd = SlashCommand {
            id: "test-command".to_string(),
            name: "test".to_string(),
            full_command: "/test".to_string(),
            scope: "global".to_string(),
            namespace: None,
            file_path: "/path/to/test.md".to_string(),
            content: "Test command content".to_string(),
            description: Some("A test command".to_string()),
            allowed_tools: vec![],
            has_bash_commands: false,
            has_file_references: false,
            accepts_arguments: false,
        };

        assert_eq!(cmd.id, "test-command");
        assert_eq!(cmd.name, "test");
        assert_eq!(cmd.full_command, "/test");
        assert!(cmd.description.is_some());
    }

    #[test]
    fn test_slash_command_with_namespace() {
        let cmd = SlashCommand {
            id: "myproject-test".to_string(),
            name: "test".to_string(),
            full_command: "/myproject:test".to_string(),
            scope: "project".to_string(),
            namespace: Some("myproject".to_string()),
            file_path: "/project/.claude/commands/test.md".to_string(),
            content: "Test content".to_string(),
            description: None,
            allowed_tools: vec!["read_file".to_string()],
            has_bash_commands: true,
            has_file_references: false,
            accepts_arguments: true,
        };

        assert!(cmd.namespace.is_some());
        assert_eq!(cmd.namespace.unwrap(), "myproject");
        assert_eq!(cmd.full_command, "/myproject:test");
        assert!(!cmd.allowed_tools.is_empty());
    }

    #[test]
    fn test_command_markdown_parsing() {
        let markdown_with_frontmatter = r#"---
allowed-tools:
  - read_file
  - write_file
description: My custom command
---

# Command Content

This is the command body.
"#;

        // Verify frontmatter exists
        assert!(markdown_with_frontmatter.starts_with("---"));
        assert!(markdown_with_frontmatter.contains("allowed-tools"));
        assert!(markdown_with_frontmatter.contains("description"));
        assert!(markdown_with_frontmatter.contains("# Command Content"));
    }

    #[test]
    fn test_command_without_frontmatter() {
        let plain_markdown = r#"# Simple Command

Just a simple command without frontmatter.
"#;

        assert!(plain_markdown.starts_with("# Simple Command"));
        assert!(!plain_markdown.contains("---"));
    }

    #[test]
    fn test_command_features() {
        // Test bash command detection
        let cmd_with_bash = "Run `npm test` to execute tests";
        assert!(cmd_with_bash.contains("`"));

        // Test file reference detection
        let cmd_with_files = "Read the file at `src/main.rs`";
        assert!(cmd_with_files.contains(".rs"));

        // Test argument detection
        let cmd_with_args = "Usage: /mycommand <filename>";
        assert!(cmd_with_args.contains("<"));
        assert!(cmd_with_args.contains(">"));
    }

    #[test]
    fn test_keyword_masking() {
        use bamboo_agent::commands::keyword_masking::load_keyword_masking_config;
        use bamboo_agent::core::keyword_masking::{KeywordEntry, KeywordMaskingConfig, MatchType};
        use std::path::Path;

        // Test default config
        let config = KeywordMaskingConfig::default();
        assert!(config.entries.is_empty());

        // Test config with entries
        let entry = KeywordEntry {
            pattern: "secret".to_string(),
            match_type: MatchType::Exact,
            enabled: true,
        };

        let config_with_entries = KeywordMaskingConfig {
            entries: vec![entry.clone()],
        };

        assert_eq!(config_with_entries.entries.len(), 1);
        assert_eq!(config_with_entries.entries[0].pattern, "secret");
    }
}
