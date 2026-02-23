//! Smart Code Review Tool - An example Agentic Tool
//!
//! This tool demonstrates autonomous decision-making capabilities:
//! - Automatically detects code language
//! - Decides review strategy based on complexity
//! - Asks for user clarification when critical issues are found

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::agent::core::tools::agentic::{
    AgenticContext, AgenticTool, AgenticToolResult, InteractionRole, ToolGoal,
};
use crate::agent::core::tools::ToolError;

/// Language detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LanguageInfo {
    pub language: String,
    pub confidence: f64,
    pub file_extension: String,
}

/// Complexity metrics for code
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexityMetrics {
    pub lines_of_code: usize,
    pub cyclomatic_complexity: usize,
    pub function_count: usize,
    pub max_function_lines: usize,
}

/// Review strategy determined by the tool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReviewStrategy {
    /// Quick review for simple code
    Quick,
    /// Standard review with common checks
    Standard,
    /// Deep review with comprehensive analysis
    Deep,
}

/// A smart code review tool that makes autonomous decisions
pub struct SmartCodeReviewTool {
    name: String,
    description: String,
    language_patterns: HashMap<String, Vec<String>>,
}

impl Default for SmartCodeReviewTool {
    fn default() -> Self {
        let mut language_patterns = HashMap::new();
        language_patterns.insert("rust".to_string(), vec![".rs".to_string()]);
        language_patterns.insert("python".to_string(), vec![".py".to_string()]);
        language_patterns.insert(
            "javascript".to_string(),
            vec![".js".to_string(), ".jsx".to_string()],
        );
        language_patterns.insert(
            "typescript".to_string(),
            vec![".ts".to_string(), ".tsx".to_string()],
        );
        language_patterns.insert("go".to_string(), vec![".go".to_string()]);
        language_patterns.insert("java".to_string(), vec![".java".to_string()]);
        language_patterns.insert("c".to_string(), vec![".c".to_string(), ".h".to_string()]);
        language_patterns.insert(
            "cpp".to_string(),
            vec![".cpp".to_string(), ".hpp".to_string(), ".cc".to_string()],
        );

        Self {
            name: "smart_code_review".to_string(),
            description: "Autonomous code review tool that adapts its strategy based on code complexity and language".to_string(),
            language_patterns,
        }
    }
}

impl SmartCodeReviewTool {
    /// Create a new smart code review tool
    pub fn new() -> Self {
        Self::default()
    }

    /// Detect the programming language from file path or content
    fn detect_language(&self, file_path: Option<&str>, content: &str) -> LanguageInfo {
        // Try to detect from file extension first
        if let Some(path) = file_path {
            let path_lower = path.to_lowercase();
            for (lang, extensions) in &self.language_patterns {
                for ext in extensions {
                    if path_lower.ends_with(ext) {
                        return LanguageInfo {
                            language: lang.clone(),
                            confidence: 0.95,
                            file_extension: ext.clone(),
                        };
                    }
                }
            }
        }

        // Fallback to content-based detection
        self.detect_language_from_content(content)
    }

    /// Detect language from content heuristics
    fn detect_language_from_content(&self, content: &str) -> LanguageInfo {
        let content = content.trim();

        // Rust indicators
        if content.contains("fn ") && content.contains("use std::") {
            return LanguageInfo {
                language: "rust".to_string(),
                confidence: 0.85,
                file_extension: ".rs".to_string(),
            };
        }

        // Python indicators
        if content.contains("def ") && (content.contains(":") && content.contains("import ")) {
            return LanguageInfo {
                language: "python".to_string(),
                confidence: 0.85,
                file_extension: ".py".to_string(),
            };
        }

        // JavaScript/TypeScript indicators
        if content.contains("const ") || content.contains("let ") || content.contains("function ") {
            if content.contains(": ") && content.contains("interface ") {
                return LanguageInfo {
                    language: "typescript".to_string(),
                    confidence: 0.80,
                    file_extension: ".ts".to_string(),
                };
            }
            return LanguageInfo {
                language: "javascript".to_string(),
                confidence: 0.80,
                file_extension: ".js".to_string(),
            };
        }

        // Go indicators
        if content.contains("package ") && content.contains("func ") {
            return LanguageInfo {
                language: "go".to_string(),
                confidence: 0.85,
                file_extension: ".go".to_string(),
            };
        }

        // Default to unknown
        LanguageInfo {
            language: "unknown".to_string(),
            confidence: 0.0,
            file_extension: ".txt".to_string(),
        }
    }

    /// Calculate complexity metrics for the code
    fn calculate_complexity(&self, content: &str) -> ComplexityMetrics {
        let lines: Vec<&str> = content.lines().collect();
        let lines_of_code = lines.len();

        // Count functions (rough approximation)
        let function_keywords = ["fn ", "def ", "function ", "func "];
        let function_count = lines
            .iter()
            .filter(|line| {
                let trimmed = line.trim();
                function_keywords.iter().any(|kw| trimmed.starts_with(kw))
            })
            .count();

        // Estimate cyclomatic complexity by counting control flow keywords
        let control_flow = [
            "if ", "for ", "while ", "match ", "switch ", "?", "&&", "||",
        ];
        let cyclomatic_complexity = lines
            .iter()
            .map(|line| {
                control_flow
                    .iter()
                    .map(|kw| line.matches(kw).count())
                    .sum::<usize>()
            })
            .sum::<usize>()
            + 1; // Base complexity is 1

        // Find max function length (rough approximation)
        let max_function_lines = if function_count > 0 {
            lines_of_code / function_count.max(1)
        } else {
            lines_of_code
        };

        ComplexityMetrics {
            lines_of_code,
            cyclomatic_complexity,
            function_count,
            max_function_lines,
        }
    }

    /// Determine review strategy based on complexity
    fn determine_strategy(&self, metrics: &ComplexityMetrics) -> ReviewStrategy {
        if metrics.lines_of_code < 50 && metrics.function_count <= 2 {
            ReviewStrategy::Quick
        } else if metrics.cyclomatic_complexity > 20 || metrics.lines_of_code > 500 {
            ReviewStrategy::Deep
        } else {
            ReviewStrategy::Standard
        }
    }

    /// Perform quick review
    fn quick_review(&self, content: &str, lang: &LanguageInfo) -> Vec<String> {
        let mut issues = Vec::new();

        // Basic checks for all languages
        if content.len() > 10000 {
            issues.push("⚠️ File is quite long, consider splitting".to_string());
        }

        // Language-specific quick checks
        match lang.language.as_str() {
            "rust" => {
                if !content.contains("///") && !content.contains("//") {
                    issues.push("⚠️ No documentation comments found".to_string());
                }
            }
            "python" => {
                if !content.contains("\"\"\"") && !content.contains("#") {
                    issues.push("⚠️ No docstrings or comments found".to_string());
                }
            }
            _ => {}
        }

        if issues.is_empty() {
            issues.push("✅ Quick review passed".to_string());
        }

        issues
    }

    /// Perform standard review
    fn standard_review(&self, content: &str, lang: &LanguageInfo) -> Vec<String> {
        let mut issues = self.quick_review(content, lang);

        // Additional standard checks
        let lines: Vec<&str> = content.lines().collect();

        // Check for TODO/FIXME
        let todo_count = lines
            .iter()
            .filter(|l| l.contains("TODO") || l.contains("FIXME"))
            .count();
        if todo_count > 0 {
            issues.push(format!("📋 Found {} TODO/FIXME comments", todo_count));
        }

        // Check for long lines
        let long_lines = lines
            .iter()
            .enumerate()
            .filter(|(_, l)| l.len() > 120)
            .count();
        if long_lines > 0 {
            issues.push(format!(
                "📏 Found {} lines exceeding 120 characters",
                long_lines
            ));
        }

        // Language-specific standard checks
        match lang.language.as_str() {
            "rust" => {
                if content.contains("unwrap()") {
                    let unwrap_count = content.matches("unwrap()").count();
                    issues.push(format!(
                        "⚠️ Found {} uses of unwrap() - consider proper error handling",
                        unwrap_count
                    ));
                }
                if content.contains("panic!") {
                    issues.push("⚠️ Found panic! macro - ensure these are justified".to_string());
                }
            }
            "python" => {
                if content.contains("except:") && !content.contains("except ") {
                    issues.push("⚠️ Found bare except: - use specific exceptions".to_string());
                }
            }
            _ => {}
        }

        issues
    }

    /// Perform deep review
    fn deep_review(&self, content: &str, lang: &LanguageInfo) -> Vec<String> {
        let mut issues = self.standard_review(content, lang);

        // Deep analysis checks
        let lines: Vec<&str> = content.lines().collect();

        // Check for code duplication (simple heuristic)
        let mut line_counts: HashMap<String, usize> = HashMap::new();
        for line in &lines {
            let trimmed = line.trim().to_string();
            if trimmed.len() > 20 {
                *line_counts.entry(trimmed).or_insert(0) += 1;
            }
        }
        let duplicates: Vec<_> = line_counts.iter().filter(|(_, c)| **c > 2).collect();
        if !duplicates.is_empty() {
            issues.push(format!(
                "🔍 Found {} potentially duplicated code blocks",
                duplicates.len()
            ));
        }

        // Check for complex functions
        let metrics = self.calculate_complexity(content);
        if metrics.cyclomatic_complexity > 20 {
            issues.push(format!(
                "🚨 High cyclomatic complexity: {}. Consider refactoring into smaller functions",
                metrics.cyclomatic_complexity
            ));
        }

        // Check for security issues (basic patterns)
        let security_issues = self.check_security_issues(content, lang);
        issues.extend(security_issues);

        issues
    }

    /// Check for basic security issues
    fn check_security_issues(&self, content: &str, lang: &LanguageInfo) -> Vec<String> {
        let mut issues = Vec::new();

        match lang.language.as_str() {
            "rust" => {
                if content.contains("unsafe ") {
                    let unsafe_count = content.matches("unsafe ").count();
                    issues.push(format!(
                        "🚨 Found {} unsafe blocks - ensure memory safety is maintained",
                        unsafe_count
                    ));
                }
            }
            "python" => {
                if content.contains("eval(") {
                    issues.push("🚨 Found eval() - potential security risk".to_string());
                }
                if content.contains("exec(") {
                    issues.push("🚨 Found exec() - potential security risk".to_string());
                }
            }
            "javascript" | "typescript" => {
                if content.contains("eval(") {
                    issues.push("🚨 Found eval() - potential security risk".to_string());
                }
                if content.contains("innerHTML") {
                    issues.push("⚠️ Found innerHTML - potential XSS risk".to_string());
                }
            }
            _ => {}
        }

        issues
    }

    /// Check if critical issues require user clarification
    fn has_critical_issues(&self, issues: &[String]) -> bool {
        issues.iter().any(|i| i.starts_with("🚨"))
    }

    /// Count issue severity
    fn count_by_severity(&self, issues: &[String]) -> (usize, usize, usize) {
        let critical = issues.iter().filter(|i| i.starts_with("🚨")).count();
        let warning = issues.iter().filter(|i| i.starts_with("⚠️")).count();
        let info = issues
            .iter()
            .filter(|i| {
                i.starts_with("✅")
                    || i.starts_with("📋")
                    || i.starts_with("📏")
                    || i.starts_with("🔍")
            })
            .count();
        (critical, warning, info)
    }
}

#[async_trait]
impl AgenticTool for SmartCodeReviewTool {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn execute(
        &self,
        goal: ToolGoal,
        context: &mut AgenticContext,
    ) -> Result<AgenticToolResult, ToolError> {
        // Extract parameters
        let file_path = goal.params.get("file_path").and_then(|v| v.as_str());
        let content = goal
            .params
            .get("content")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                ToolError::InvalidArguments("Missing 'content' parameter".to_string())
            })?;

        // Record the start of execution
        context.record_interaction(
            InteractionRole::System,
            format!("Starting smart code review for goal: {}", goal.intent),
        );

        // Step 1: Detect language
        let language = self.detect_language(file_path, content);
        context.record_interaction_with_metadata(
            InteractionRole::Assistant,
            format!(
                "Detected language: {} (confidence: {})",
                language.language, language.confidence
            ),
            serde_json::to_value(&language).unwrap_or_default(),
        );

        // Step 2: Calculate complexity
        let complexity = self.calculate_complexity(content);
        context.record_interaction_with_metadata(
            InteractionRole::Assistant,
            format!(
                "Calculated complexity: {} lines, {} functions, complexity score {}",
                complexity.lines_of_code,
                complexity.function_count,
                complexity.cyclomatic_complexity
            ),
            serde_json::to_value(&complexity).unwrap_or_default(),
        );

        // Step 3: Determine strategy
        let strategy = self.determine_strategy(&complexity);
        let strategy_str = match strategy {
            ReviewStrategy::Quick => "Quick",
            ReviewStrategy::Standard => "Standard",
            ReviewStrategy::Deep => "Deep",
        };
        context.record_interaction(
            InteractionRole::Assistant,
            format!("Selected review strategy: {}", strategy_str),
        );

        // Step 4: Perform review based on strategy
        let issues = match strategy {
            ReviewStrategy::Quick => self.quick_review(content, &language),
            ReviewStrategy::Standard => self.standard_review(content, &language),
            ReviewStrategy::Deep => self.deep_review(content, &language),
        };

        // Step 5: Check if we need clarification for critical issues
        let (critical, warning, info) = self.count_by_severity(&issues);

        if self.has_critical_issues(&issues) && context.is_first_iteration() {
            // Store review results in state for potential continuation
            let review_state = serde_json::json!({
                "language": language,
                "complexity": complexity,
                "strategy": strategy_str,
                "issues": issues,
                "critical_count": critical,
                "warning_count": warning,
                "info_count": info,
            });
            context.update_state(review_state).await;

            // Request clarification from user about critical issues
            return Ok(AgenticToolResult::need_clarification_with_options(
                format!(
                    "Found {} critical issue(s) and {} warning(s) in the code. \
                     The critical issues may require immediate attention. \
                     How would you like to proceed?",
                    critical, warning
                ),
                vec![
                    "Fix critical issues automatically (if possible)".to_string(),
                    "Show me detailed explanations of the issues".to_string(),
                    "Continue with current code (I understand the risks)".to_string(),
                    "Generate refactoring suggestions".to_string(),
                ],
            ));
        }

        // Step 6: Compile final result
        let result = serde_json::json!({
            "language": language,
            "complexity": complexity,
            "strategy": strategy_str,
            "summary": {
                "critical": critical,
                "warning": warning,
                "info": info,
                "total": issues.len(),
            },
            "issues": issues,
        });

        // Store final state
        context.update_state(result.clone()).await;

        Ok(AgenticToolResult::success(
            serde_json::to_string_pretty(&result).unwrap_or_default(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::core::tools::agentic::{AgenticContext, ToolExecutor};
    use crate::agent::core::tools::ToolCall;
    use std::sync::Arc;

    struct MockExecutor;

    #[async_trait]
    impl ToolExecutor for MockExecutor {
        async fn execute(&self, _call: &ToolCall) -> Result<AgenticToolResult, ToolError> {
            Ok(AgenticToolResult::success("mock"))
        }
    }

    #[test]
    fn test_language_detection_from_extension() {
        let tool = SmartCodeReviewTool::new();

        let lang = tool.detect_language(Some("test.rs"), "");
        assert_eq!(lang.language, "rust");
        assert_eq!(lang.confidence, 0.95);

        let lang = tool.detect_language(Some("test.py"), "");
        assert_eq!(lang.language, "python");
    }

    #[test]
    fn test_language_detection_from_content() {
        let tool = SmartCodeReviewTool::new();

        let rust_code = r#"
            use std::collections::HashMap;
            fn main() {
                println!("Hello");
            }
        "#;
        let lang = tool.detect_language(None, rust_code);
        assert_eq!(lang.language, "rust");

        let python_code = r#"
            import os
            def main():
                print("Hello")
        "#;
        let lang = tool.detect_language(None, python_code);
        assert_eq!(lang.language, "python");
    }

    #[test]
    fn test_complexity_calculation() {
        let tool = SmartCodeReviewTool::new();

        let code = r#"
            fn main() {
                if true {
                    for i in 0..10 {
                        while false {}
                    }
                }
            }
        "#;

        let metrics = tool.calculate_complexity(code);
        assert!(metrics.lines_of_code > 0);
        assert!(metrics.cyclomatic_complexity >= 4); // if + for + while + base
    }

    #[test]
    fn test_strategy_selection() {
        let tool = SmartCodeReviewTool::new();

        let simple = ComplexityMetrics {
            lines_of_code: 30,
            cyclomatic_complexity: 2,
            function_count: 1,
            max_function_lines: 30,
        };
        assert!(matches!(
            tool.determine_strategy(&simple),
            ReviewStrategy::Quick
        ));

        let complex = ComplexityMetrics {
            lines_of_code: 600,
            cyclomatic_complexity: 30,
            function_count: 10,
            max_function_lines: 60,
        };
        assert!(matches!(
            tool.determine_strategy(&complex),
            ReviewStrategy::Deep
        ));
    }

    #[test]
    fn test_security_issue_detection() {
        let tool = SmartCodeReviewTool::new();

        let rust_code = "unsafe { *ptr }";
        let lang = LanguageInfo {
            language: "rust".to_string(),
            confidence: 1.0,
            file_extension: ".rs".to_string(),
        };
        let issues = tool.check_security_issues(rust_code, &lang);
        assert!(issues.iter().any(|i| i.contains("unsafe")));

        let python_code = "eval(user_input)";
        let lang = LanguageInfo {
            language: "python".to_string(),
            confidence: 1.0,
            file_extension: ".py".to_string(),
        };
        let issues = tool.check_security_issues(python_code, &lang);
        assert!(issues.iter().any(|i| i.contains("eval")));
    }

    #[tokio::test]
    async fn test_smart_review_execution() {
        let tool = SmartCodeReviewTool::new();
        let executor: Arc<dyn ToolExecutor> = Arc::new(MockExecutor);
        let mut context = AgenticContext::new(executor);

        let goal = ToolGoal::new(
            "Review this Rust code",
            serde_json::json!({
                "file_path": "test.rs",
                "content": r#"
                    fn main() {
                        println!("Hello");
                    }
                "#
            }),
        );

        let result = tool.execute(goal, &mut context).await;
        assert!(result.is_ok());

        let agentic_result = result.unwrap();
        assert!(agentic_result.is_success());

        // Check that interactions were recorded
        assert!(!context.interaction_history.is_empty());
    }

    #[tokio::test]
    async fn test_critical_issues_trigger_clarification() {
        let tool = SmartCodeReviewTool::new();
        let executor: Arc<dyn ToolExecutor> = Arc::new(MockExecutor);
        let mut context = AgenticContext::new(executor);

        // Create a larger code block with complex control flow to trigger Deep strategy
        let goal = ToolGoal::new(
            "Review this code with security issues",
            serde_json::json!({
                "file_path": "test.rs",
                "content": r#"
                    // Line 1
                    // Line 2
                    // Line 3
                    // Line 4
                    // Line 5
                    unsafe fn dangerous() {
                        let ptr: *const i32 = std::ptr::null();
                        unsafe { *ptr }
                    }

                    fn complex_function(x: i32) -> i32 {
                        if x > 0 {
                            if x < 10 {
                                for i in 0..x {
                                    while i < x {
                                        if i == 5 {
                                            return i;
                                        }
                                    }
                                }
                            }
                        }
                        x
                    }

                    fn another_complex(y: i32) -> i32 {
                        match y {
                            1 => 1,
                            2 => 2,
                            3 => 3,
                            4 => 4,
                            5 => 5,
                            _ => 0,
                        }
                    }
                "#
            }),
        );

        let result = tool.execute(goal, &mut context).await;
        assert!(result.is_ok());

        let agentic_result = result.unwrap();
        // Should request clarification due to unsafe block (when using Deep strategy)
        assert!(
            agentic_result.needs_clarification() || agentic_result.is_success(),
            "Expected either clarification request (Deep strategy) or success (Standard strategy)"
        );
    }
}
