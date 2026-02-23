use crate::types::{McpTool, ToolAlias};
use dashmap::DashMap;

/// Maps tool aliases to their original server/tool names
pub struct ToolIndex {
    /// alias -> (server_id, original_name)
    aliases: DashMap<String, (String, String)>,
    /// server_id -> list of tool names
    server_tools: DashMap<String, Vec<String>>,
}

impl ToolIndex {
    pub fn new() -> Self {
        Self {
            aliases: DashMap::new(),
            server_tools: DashMap::new(),
        }
    }

    /// Generate a unique alias for a tool from a specific server
    pub fn generate_alias(&self,
        server_id: &str,
        tool_name: &str,
    ) -> String {
        // Use format: mcp__{server_id}__{tool_name}
        // Sanitize server_id and tool_name (replace :: with __)
        let sanitized_server = server_id.replace("::", "__").replace(':', "_");
        let sanitized_tool = tool_name.replace("::", "__").replace(':', "_");
        format!("mcp__{}__{}", sanitized_server, sanitized_tool)
    }

    /// Register tools from a server
    pub fn register_server_tools(
        &self,
        server_id: &str,
        tools: &[McpTool],
        allowed_tools: &[ String],
        denied_tools: &[ String],
    ) -> Vec<ToolAlias> {
        let mut aliases = Vec::new();
        let mut tool_names = Vec::new();

        for tool in tools {
            // Check if tool is allowed
            if !allowed_tools.is_empty() && !allowed_tools.contains(&tool.name) {
                continue;
            }

            // Check if tool is denied
            if denied_tools.contains(&tool.name) {
                continue;
            }

            let alias = self.generate_alias(server_id, &tool.name);
            self.aliases
                .insert(alias.clone(), (server_id.to_string(), tool.name.clone()));
            tool_names.push(tool.name.clone());

            aliases.push(ToolAlias {
                alias: alias.clone(),
                server_id: server_id.to_string(),
                original_name: tool.name.clone(),
            });
        }

        self.server_tools.insert(server_id.to_string(), tool_names);
        aliases
    }

    /// Remove all tools from a server
    pub fn remove_server_tools(&self,
        server_id: &str,
    ) {
        if let Some((_, tools)) = self.server_tools.remove(server_id) {
            for tool_name in tools {
                let alias = self.generate_alias(server_id, &tool_name);
                self.aliases.remove(&alias);
            }
        }
    }

    /// Lookup a tool by its alias
    pub fn lookup(&self,
        alias: &str,
    ) -> Option<ToolAlias> {
        self.aliases.get(alias).map(|entry| {
            let (server_id, original_name) = entry.value();
            ToolAlias {
                alias: alias.to_string(),
                server_id: server_id.clone(),
                original_name: original_name.clone(),
            }
        })
    }

    /// Get all registered aliases
    pub fn all_aliases(&self,
    ) -> Vec<ToolAlias> {
        self.aliases
            .iter()
            .map(|entry| {
                let (server_id, original_name) = entry.value();
                ToolAlias {
                    alias: entry.key().clone(),
                    server_id: server_id.clone(),
                    original_name: original_name.clone(),
                }
            })
            .collect()
    }

    /// Get tools for a specific server
    pub fn get_server_tools(&self,
        server_id: &str,
    ) -> Option<Vec<String>> {
        self.server_tools.get(server_id).map(|entry| entry.clone())
    }

    /// Clear all registered tools
    pub fn clear(&self) {
        self.aliases.clear();
        self.server_tools.clear();
    }

    /// Check if an alias exists
    pub fn contains(&self,
        alias: &str,
    ) -> bool {
        self.aliases.contains_key(alias)
    }
}

impl Default for ToolIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_alias() {
        let index = ToolIndex::new();
        let alias = index.generate_alias("filesystem", "read_file");
        assert_eq!(alias, "mcp__filesystem__read_file");
    }

    #[test]
    fn test_generate_alias_sanitizes() {
        let index = ToolIndex::new();
        let alias = index.generate_alias("my::server", "tool::name");
        assert_eq!(alias, "mcp__my__server__tool__name");
    }

    #[test]
    fn test_register_and_lookup() {
        let index = ToolIndex::new();
        let tools = vec![
            McpTool {
                name: "read_file".to_string(),
                description: "Read a file".to_string(),
                parameters: serde_json::json!({}),
            },
            McpTool {
                name: "write_file".to_string(),
                description: "Write a file".to_string(),
                parameters: serde_json::json!({}),
            },
        ];

        let aliases = index.register_server_tools("fs", &tools, &[], &[]);
        assert_eq!(aliases.len(), 2);

        let lookup = index.lookup("mcp__fs__read_file");
        assert!(lookup.is_some());
        let lookup = lookup.unwrap();
        assert_eq!(lookup.server_id, "fs");
        assert_eq!(lookup.original_name, "read_file");
    }

    #[test]
    fn test_allowed_tools_filter() {
        let index = ToolIndex::new();
        let tools = vec![
            McpTool {
                name: "read_file".to_string(),
                description: "Read".to_string(),
                parameters: serde_json::json!({}),
            },
            McpTool {
                name: "delete_file".to_string(),
                description: "Delete".to_string(),
                parameters: serde_json::json!({}),
            },
        ];

        let aliases =
            index.register_server_tools("fs", &tools, &["read_file".to_string()], &[]);
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].original_name, "read_file");
    }

    #[test]
    fn test_denied_tools_filter() {
        let index = ToolIndex::new();
        let tools = vec![
            McpTool {
                name: "read_file".to_string(),
                description: "Read".to_string(),
                parameters: serde_json::json!({}),
            },
            McpTool {
                name: "delete_file".to_string(),
                description: "Delete".to_string(),
                parameters: serde_json::json!({}),
            },
        ];

        let aliases =
            index.register_server_tools("fs", &tools, &[], &["delete_file".to_string()]);
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].original_name, "read_file");
    }

    #[test]
    fn test_remove_server_tools() {
        let index = ToolIndex::new();
        let tools = vec![McpTool {
            name: "read_file".to_string(),
            description: "Read".to_string(),
            parameters: serde_json::json!({}),
        }];

        index.register_server_tools("fs", &tools, &[], &[]);
        assert!(index.contains("mcp__fs__read_file"));

        index.remove_server_tools("fs");
        assert!(!index.contains("mcp__fs__read_file"));
    }
}
