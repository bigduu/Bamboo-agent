//! Parsed permission rule with optional tool name and resource pattern.
//!
//! This module provides a parser for permission rules like `Bash(npm run *)` or
//! `Write(/src/**)` or `Read`. It supports case-insensitive tool name matching
//! and glob pattern matching against tool arguments.

use serde_json::Value;

use crate::permission::config::match_glob_pattern;

/// Match a pattern against a target string with support for prefix/suffix/infix wildcards.
///
/// This extends the basic glob matching with support for patterns like:
/// - `npm run *` - prefix matching
/// - `*example.com*` - infix matching
/// - `*.rs` - suffix matching (also handled by match_glob_pattern)
/// - `*` - match anything
/// - Exact string match
fn match_tool_pattern(pattern: &str, target: &str) -> bool {
    // Universal wildcard
    if pattern == "*" {
        return true;
    }

    // If pattern contains '*', handle prefix/suffix/infix matching
    if pattern.contains('*') {
        let parts: Vec<&str> = pattern.split('*').collect();

        if parts.len() == 2 {
            // Single wildcard: prefix* or *suffix or prefix*suffix
            let (prefix, suffix) = (parts[0], parts[1]);

            if prefix.is_empty() && suffix.is_empty() {
                // Pattern is just "*"
                return true;
            }

            if prefix.is_empty() {
                // Pattern is "*suffix" - check if target ends with suffix
                return target.ends_with(suffix);
            }

            if suffix.is_empty() {
                // Pattern is "prefix*" - check if target starts with prefix
                return target.starts_with(prefix);
            }

            // Pattern is "prefix*suffix" - check if target starts with prefix and ends with suffix
            return target.starts_with(prefix) && target.ends_with(suffix);
        }

        // Multiple wildcards: use a simple approach
        // Check that all non-wildcard parts appear in order
        let mut current_pos = 0;
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                continue; // Skip empty parts (consecutive wildcards)
            }

            if i == 0 && !pattern.starts_with('*') {
                // First part and pattern doesn't start with * - must match at beginning
                if !target.starts_with(part) {
                    return false;
                }
                current_pos = part.len();
            } else if i == parts.len() - 1 && !pattern.ends_with('*') {
                // Last part and pattern doesn't end with * - must match at end
                if !target[current_pos..].ends_with(part) {
                    return false;
                }
            } else {
                // Middle part - find it after current position
                if let Some(pos) = target[current_pos..].find(part) {
                    current_pos += pos + part.len();
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    // No wildcards - try exact match first, then fall back to glob matching
    if pattern == target {
        return true;
    }

    // Fall back to the existing glob matcher for path-style patterns
    match_glob_pattern(pattern, target)
}

/// A parsed permission rule with optional tool name and resource pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRule {
    /// The tool name (e.g., "Bash", "Write", "Read")
    pub tool_name: String,
    /// Optional pattern to match against tool arguments
    pub pattern: Option<String>,
}

impl ParsedRule {
    /// Parse a rule string like "Bash(npm run *)" or "Write(/src/**)" or "Read".
    ///
    /// If the string contains '(' and ends with ')', the part before '(' is the tool name
    /// and the content between '(' and ')' is the pattern. Handles nested parentheses
    /// by matching the outermost pair.
    ///
    /// # Examples
    ///
    /// ```
    /// use bamboo_tools::permission::rule_parser::ParsedRule;
    ///
    /// let rule = ParsedRule::parse("Read");
    /// assert_eq!(rule.tool_name, "Read");
    /// assert_eq!(rule.pattern, None);
    ///
    /// let rule = ParsedRule::parse("Bash(npm run *)");
    /// assert_eq!(rule.tool_name, "Bash");
    /// assert_eq!(rule.pattern, Some("npm run *".to_string()));
    /// ```
    pub fn parse(rule: &str) -> Self {
        let trimmed = rule.trim();

        // Find the outermost '(' and matching ')'
        if let Some(open_paren) = trimmed.find('(') {
            if trimmed.ends_with(')') {
                let tool_name = trimmed[..open_paren].trim().to_string();
                let pattern = trimmed[open_paren + 1..trimmed.len() - 1]
                    .trim()
                    .to_string();

                if pattern.is_empty() {
                    return Self {
                        tool_name,
                        pattern: None,
                    };
                }

                return Self {
                    tool_name,
                    pattern: Some(pattern),
                };
            }
        }

        // No parentheses - just a tool name
        Self {
            tool_name: trimmed.to_string(),
            pattern: None,
        }
    }

    /// Check if a tool call matches this rule.
    ///
    /// Matching logic:
    /// 1. Case-insensitive tool name match
    /// 2. If pattern is None → match any call to this tool
    /// 3. If pattern exists → match against relevant argument:
    ///    - For Bash tool: match against "command" argument
    ///    - For Write/Edit/Read: match against "file_path" argument
    ///    - For WebFetch: match against "url" argument
    ///    - For others: match against stringified args
    ///
    /// # Examples
    ///
    /// ```
    /// use bamboo_tools::permission::rule_parser::ParsedRule;
    /// use serde_json::json;
    ///
    /// let rule = ParsedRule::parse("Bash(npm run *)");
    /// assert!(rule.matches_tool_call("Bash", &json!({"command": "npm run test"})));
    /// assert!(!rule.matches_tool_call("Bash", &json!({"command": "cargo build"})));
    /// ```
    pub fn matches_tool_call(&self, tool_name: &str, args: &Value) -> bool {
        // Case-insensitive tool name match
        if !self.tool_name.eq_ignore_ascii_case(tool_name) {
            return false;
        }

        // If no pattern, match any call to this tool
        let Some(pattern) = &self.pattern else {
            return true;
        };

        // Normalize tool name for case-insensitive matching
        let tool_name_lower = tool_name.to_ascii_lowercase();

        // Extract the relevant argument based on tool type
        let target = match tool_name_lower.as_str() {
            "bash" => args.get("command").and_then(|v| v.as_str()),
            "write" | "edit" | "read" | "apply_patch" => {
                args.get("file_path").and_then(|v| v.as_str())
            }
            "webfetch" => args.get("url").and_then(|v| v.as_str()),
            "notebookedit" => args.get("notebook_path").and_then(|v| v.as_str()),
            "websearch" => args.get("query").and_then(|v| v.as_str()),
            "js_repl" => args.get("code").and_then(|v| v.as_str()),
            "bashoutput" | "killshell" => args.get("bash_id").and_then(|v| v.as_str()),
            "session_note" | "memory" => args.get("action").and_then(|v| v.as_str()),
            _ => None,
        };

        match target {
            Some(target_str) => match_tool_pattern(pattern, target_str),
            None => {
                // Fallback: match against stringified args
                let args_str = args.to_string();
                match_tool_pattern(pattern, &args_str)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_parse_tool_only() {
        let rule = ParsedRule::parse("Read");
        assert_eq!(rule.tool_name, "Read");
        assert_eq!(rule.pattern, None);
    }

    #[test]
    fn test_parse_tool_with_pattern() {
        let rule = ParsedRule::parse("Bash(npm run *)");
        assert_eq!(rule.tool_name, "Bash");
        assert_eq!(rule.pattern, Some("npm run *".to_string()));
    }

    #[test]
    fn test_parse_write_path() {
        let rule = ParsedRule::parse("Write(/src/**)");
        assert_eq!(rule.tool_name, "Write");
        assert_eq!(rule.pattern, Some("/src/**".to_string()));
    }

    #[test]
    fn test_parse_nested_parens() {
        // Edge case: nested parentheses in pattern
        let rule = ParsedRule::parse("Bash(echo $(foo))");
        assert_eq!(rule.tool_name, "Bash");
        assert_eq!(rule.pattern, Some("echo $(foo)".to_string()));
    }

    #[test]
    fn test_parse_empty_parens() {
        let rule = ParsedRule::parse("Bash()");
        assert_eq!(rule.tool_name, "Bash");
        assert_eq!(rule.pattern, None);
    }

    #[test]
    fn test_parse_with_whitespace() {
        let rule = ParsedRule::parse("  Bash(  npm run *  )  ");
        assert_eq!(rule.tool_name, "Bash");
        assert_eq!(rule.pattern, Some("npm run *".to_string()));
    }

    #[test]
    fn test_match_bash_command() {
        let rule = ParsedRule::parse("Bash(npm run *)");
        assert!(rule.matches_tool_call("Bash", &json!({"command": "npm run test"})));
        assert!(rule.matches_tool_call("Bash", &json!({"command": "npm run build"})));
        assert!(!rule.matches_tool_call("Bash", &json!({"command": "cargo build"})));
    }

    #[test]
    fn test_match_bash_wildcard() {
        let rule = ParsedRule::parse("Bash(*)");
        assert!(rule.matches_tool_call("Bash", &json!({"command": "anything here"})));
        assert!(rule.matches_tool_call("Bash", &json!({"command": "npm run test"})));
    }

    #[test]
    fn test_match_write_path() {
        let rule = ParsedRule::parse("Write(/src/**)");
        assert!(rule.matches_tool_call("Write", &json!({"file_path": "/src/main.rs"})));
        assert!(rule.matches_tool_call("Write", &json!({"file_path": "/src/components/button.rs"})));
        assert!(!rule.matches_tool_call("Write", &json!({"file_path": "/tmp/test.txt"})));
    }

    #[test]
    fn test_match_read_exact() {
        let rule = ParsedRule::parse("Read(./.env)");
        assert!(rule.matches_tool_call("Read", &json!({"file_path": "./.env"})));
        assert!(!rule.matches_tool_call("Read", &json!({"file_path": "./.env.local"})));
    }

    #[test]
    fn test_match_read_glob() {
        let rule = ParsedRule::parse("Read(./.env.*)");
        assert!(rule.matches_tool_call("Read", &json!({"file_path": "./.env.production"})));
        assert!(rule.matches_tool_call("Read", &json!({"file_path": "./.env.staging"})));
        assert!(!rule.matches_tool_call("Read", &json!({"file_path": "./.env"})));
    }

    #[test]
    fn test_match_web_fetch() {
        let rule = ParsedRule::parse("WebFetch(*example.com*)");
        assert!(rule.matches_tool_call("WebFetch", &json!({"url": "https://example.com/path"})));
        assert!(rule.matches_tool_call("WebFetch", &json!({"url": "http://sub.example.com/api"})));
        assert!(!rule.matches_tool_call("WebFetch", &json!({"url": "https://other.com"})));
    }

    #[test]
    fn test_match_case_insensitive_tool_name() {
        let rule = ParsedRule::parse("bash(npm run *)");
        assert!(rule.matches_tool_call("Bash", &json!({"command": "npm run test"})));

        let rule = ParsedRule::parse("BASH(npm run *)");
        assert!(rule.matches_tool_call("Bash", &json!({"command": "npm run test"})));
    }

    #[test]
    fn test_match_tool_only_matches_any_args() {
        let rule = ParsedRule::parse("Bash");
        assert!(rule.matches_tool_call("Bash", &json!({"command": "anything"})));
        assert!(rule.matches_tool_call("Bash", &json!({"command": "rm -rf /"})));
    }

    #[test]
    fn test_match_wrong_tool() {
        let rule = ParsedRule::parse("Bash(npm run *)");
        assert!(!rule.matches_tool_call("Write", &json!({"file_path": "/tmp/test"})));
    }

    #[test]
    fn test_match_edit_path() {
        let rule = ParsedRule::parse("Edit(/src/**)");
        assert!(rule.matches_tool_call("Edit", &json!({"file_path": "/src/main.rs"})));
        assert!(!rule.matches_tool_call("Edit", &json!({"file_path": "/tmp/test.txt"})));
    }

    #[test]
    fn test_match_apply_patch() {
        let rule = ParsedRule::parse("apply_patch(/src/**)");
        assert!(rule.matches_tool_call("apply_patch", &json!({"file_path": "/src/main.rs"})));
        assert!(!rule.matches_tool_call("apply_patch", &json!({"file_path": "/tmp/test.txt"})));
    }

    #[test]
    fn test_match_notebook_edit() {
        let rule = ParsedRule::parse("NotebookEdit(/notebooks/**)");
        assert!(rule.matches_tool_call(
            "NotebookEdit",
            &json!({"notebook_path": "/notebooks/test.ipynb"})
        ));
        assert!(
            !rule.matches_tool_call("NotebookEdit", &json!({"notebook_path": "/tmp/test.ipynb"}))
        );
    }

    #[test]
    fn test_match_web_search() {
        let rule = ParsedRule::parse("WebSearch(rust *)");
        assert!(rule.matches_tool_call("WebSearch", &json!({"query": "rust async"})));
        assert!(!rule.matches_tool_call("WebSearch", &json!({"query": "python async"})));
    }

    #[test]
    fn test_match_js_repl() {
        let rule = ParsedRule::parse("js_repl(console.*)");
        assert!(rule.matches_tool_call("js_repl", &json!({"code": "console.log('hi')"})));
        assert!(!rule.matches_tool_call("js_repl", &json!({"code": "1 + 1"})));
    }

    #[test]
    fn test_match_bash_output() {
        let rule = ParsedRule::parse("BashOutput(abc-*)");
        assert!(rule.matches_tool_call("BashOutput", &json!({"bash_id": "abc-123"})));
        assert!(!rule.matches_tool_call("BashOutput", &json!({"bash_id": "xyz-123"})));
    }

    #[test]
    fn test_match_kill_shell() {
        let rule = ParsedRule::parse("KillShell(abc-*)");
        assert!(rule.matches_tool_call("KillShell", &json!({"bash_id": "abc-123"})));
        assert!(!rule.matches_tool_call("KillShell", &json!({"bash_id": "xyz-123"})));
    }

    #[test]
    fn test_match_session_note() {
        let rule = ParsedRule::parse("session_note(append)");
        assert!(rule.matches_tool_call("session_note", &json!({"action": "append"})));
        assert!(!rule.matches_tool_call("session_note", &json!({"action": "read"})));
    }

    #[test]
    fn test_match_memory() {
        let rule = ParsedRule::parse("memory(session_*)");
        assert!(rule.matches_tool_call("memory", &json!({"action": "session_append"})));
        assert!(!rule.matches_tool_call("memory", &json!({"action": "write"})));
    }

    #[test]
    fn test_match_unknown_tool_fallback() {
        let rule = ParsedRule::parse("CustomTool(*hello*)");
        assert!(rule.matches_tool_call("CustomTool", &json!({"any": "hello world"})));
        assert!(!rule.matches_tool_call("CustomTool", &json!({"any": "goodbye"})));
    }

    #[test]
    fn test_match_deny_overrides_allow() {
        // Simulate deny "Bash(curl *)" overriding allow "Bash(*)"
        let deny_rule = ParsedRule::parse("Bash(curl *)");
        let allow_rule = ParsedRule::parse("Bash(*)");

        let args_curl = json!({"command": "curl https://example.com"});
        let args_ls = json!({"command": "ls -la"});

        // Deny rule matches curl
        assert!(deny_rule.matches_tool_call("Bash", &args_curl));
        // Allow rule also matches curl
        assert!(allow_rule.matches_tool_call("Bash", &args_curl));
        // Deny rule does not match ls
        assert!(!deny_rule.matches_tool_call("Bash", &args_ls));
        // Allow rule matches ls
        assert!(allow_rule.matches_tool_call("Bash", &args_ls));
    }
}
