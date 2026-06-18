//! Post-execution output compression pipeline.
//!
//! Compresses tool output *after* execution but *before* the result enters
//! the session message list.  This keeps the LLM context window lean while
//! the full output is optionally tee-saved for on-demand inspection.

pub(crate) mod filters;
pub(crate) mod scenarios;
pub(crate) mod tee;

use super::per_call::ToolExecutionOutcome;

// ── Context Pressure ─────────────────────────────────────────────────────────

/// Current context window pressure, used to adjust compression aggressiveness.
pub(crate) struct ContextPressure {
    /// Current token usage as percentage of compression trigger (0-100).
    pub usage_percent: u8,
    /// Remaining tokens before compression trigger.
    pub remaining_tokens: u32,
}

// ── Compression Tier ────────────────────────────────────────────────────────

/// Tier classification based on output token count.
///
/// Determines which compression strategy to apply:
/// - `Passthrough`: output is small enough to keep as-is
/// - `Light`: ANSI strip + blank collapse + duplicate line collapse
/// - `Standard`: scenario-aware semantic compression
/// - `Aggressive`: Standard + tighter line limits
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompressionTier {
    Passthrough,
    Light,
    Standard,
    Aggressive,
}

/// Token thresholds for tier classification.
const TIER_LIGHT_TOKENS: u32 = 1_000;
const TIER_STANDARD_TOKENS: u32 = 10_000;
const TIER_AGGRESSIVE_TOKENS: u32 = 25_000;

/// Classify output into a compression tier by token count.
fn classify_tier(text: &str) -> CompressionTier {
    use bamboo_compression::TokenCounter;
    let counter = bamboo_compression::TiktokenTokenCounter::default();
    let tokens = counter.count_text(text);

    if tokens < TIER_LIGHT_TOKENS {
        CompressionTier::Passthrough
    } else if tokens < TIER_STANDARD_TOKENS {
        CompressionTier::Light
    } else if tokens < TIER_AGGRESSIVE_TOKENS {
        CompressionTier::Standard
    } else {
        CompressionTier::Aggressive
    }
}

/// Adjust compression tier based on context pressure.
///
/// - Low pressure (< 50%): only compress Standard and above
/// - Normal pressure (50-80%): compress Light and above
/// - High pressure (> 80%): compress everything, promote Light → Standard
fn adjust_tier_for_pressure(
    tier: CompressionTier,
    pressure: Option<&ContextPressure>,
) -> CompressionTier {
    let Some(p) = pressure else {
        return tier; // no pressure info → use base tier
    };

    match p.usage_percent {
        0..=49 => {
            // Low pressure: skip Light tier (only compress Standard+)
            match tier {
                CompressionTier::Light => CompressionTier::Passthrough,
                other => other,
            }
        }
        50..=80 => {
            // Normal pressure: use base tier as-is
            tier
        }
        _ => {
            // High pressure: promote Light → Standard for more aggressive compression
            match tier {
                CompressionTier::Light => CompressionTier::Standard,
                other => other,
            }
        }
    }
}

/// Apply light-weight compression (ANSI strip + blank collapse + dedup).
fn apply_light_compression(raw_result: &str) -> CompressionResult {
    // For JSON Bash envelopes, apply to stdout/stderr separately
    let parsed: serde_json::Value = match serde_json::from_str(raw_result) {
        Ok(v) => v,
        Err(_) => {
            // Plain text
            let clean = filters::strip_ansi(raw_result);
            let collapsed = filters::collapse_blank_lines(&clean);
            let deduped = filters::collapse_duplicate_lines(&collapsed, 3);
            let was_compressed = deduped.len() != raw_result.len();
            return CompressionResult {
                compressed: deduped,
                was_compressed,
            };
        }
    };

    let stdout = parsed.get("stdout").and_then(|v| v.as_str()).unwrap_or("");
    let stderr = parsed.get("stderr").and_then(|v| v.as_str()).unwrap_or("");

    let clean_stdout = filters::strip_ansi(stdout);
    let clean_stderr = filters::strip_ansi(stderr);
    let collapsed_stdout = filters::collapse_blank_lines(&clean_stdout);
    let collapsed_stderr = filters::collapse_blank_lines(&clean_stderr);
    let deduped_stdout = filters::collapse_duplicate_lines(&collapsed_stdout, 3);
    let deduped_stderr = filters::collapse_duplicate_lines(&collapsed_stderr, 3);

    let was_compressed = deduped_stdout.len() != stdout.len()
        || deduped_stderr.len() != stderr.len()
        || clean_stdout.len() != stdout.len()
        || clean_stderr.len() != stderr.len();

    if !was_compressed {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    // Rebuild JSON
    let mut obj = parsed.clone();
    if let Some(map) = obj.as_object_mut() {
        map.insert(
            "stdout".to_string(),
            serde_json::Value::String(deduped_stdout),
        );
        map.insert(
            "stderr".to_string(),
            serde_json::Value::String(deduped_stderr),
        );
    }
    let compressed = serde_json::to_string(&obj).unwrap_or_else(|_| raw_result.to_string());

    CompressionResult {
        compressed,
        was_compressed: true,
    }
}

// ── Scenario Detection ──────────────────────────────────────────────────────

/// High-level classification of a tool's output so the correct compression
/// strategy can be selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OutputScenario {
    /// `cargo test`, `pytest`, `npm test`, `vitest`, `jest`, etc.
    BashTest,
    /// `tsc`, `eslint`, `cargo check`, `cargo clippy`, etc.
    BashBuild,
    /// `git status`, `git diff`, `git log`, etc.
    BashGit,
    /// `npm install`, `cargo build`, `pip install`, etc.
    BashPackage,
    /// Any other Bash command.
    BashGeneric,
    /// `Read` tool output (code files).
    ReadCode,
    /// `Grep` tool output.
    GrepResults,
    /// `WebFetch` tool output.
    WebFetchHtml,
    /// Everything else — no compression applied.
    PassThrough,
}

/// Detect the output scenario from the tool name and (optionally) the command
/// embedded in the Bash JSON arguments.
pub(crate) fn detect_scenario(tool_name: &str, args_json: &str) -> OutputScenario {
    match tool_name {
        "Bash" | "BashOutput" => detect_bash_scenario(args_json),
        "Read" | "read_file" => OutputScenario::ReadCode,
        "Grep" => OutputScenario::GrepResults,
        "WebFetch" => OutputScenario::WebFetchHtml,
        _ => OutputScenario::PassThrough,
    }
}

/// Sub-classifier for Bash / BashOutput tools.
fn detect_bash_scenario(args_json: &str) -> OutputScenario {
    let command = extract_command(args_json);
    let cmd = command.trim();

    if is_test_command(cmd) {
        OutputScenario::BashTest
    } else if is_build_command(cmd) {
        OutputScenario::BashBuild
    } else if is_git_command(cmd) {
        OutputScenario::BashGit
    } else if is_package_command(cmd) {
        OutputScenario::BashPackage
    } else {
        OutputScenario::BashGeneric
    }
}

/// Extract the `command` field from a Bash JSON arguments string.
fn extract_command(args_json: &str) -> String {
    serde_json::from_str::<serde_json::Value>(args_json)
        .ok()
        .and_then(|v| v.get("command").and_then(|c| c.as_str()).map(String::from))
        .unwrap_or_default()
}

fn is_test_command(cmd: &str) -> bool {
    let patterns = [
        "cargo test",
        "cargo nextest",
        "pytest",
        "python -m pytest",
        "npm test",
        "npm run test",
        "npx vitest",
        "npx jest",
        "yarn test",
        "pnpm test",
        "go test",
        "dotnet test",
        "mvn test",
        "gradle test",
        "./gradlew test",
        "phpunit",
        "rspec",
        "bundle exec rspec",
    ];
    patterns.iter().any(|p| cmd_contains(cmd, p))
}

fn is_build_command(cmd: &str) -> bool {
    let patterns = [
        // Rust
        "cargo check",
        "cargo clippy",
        "cargo build",
        // JavaScript / TypeScript
        "tsc",
        "eslint",
        "npm run build",
        "npm run lint",
        "npx tsc",
        // Go
        "go build",
        "go vet",
        // .NET
        "dotnet build",
        "dotnet publish",
        // Maven
        "mvn compile",
        "mvn package",
        "mvn verify",
        "mvn install",
        "mvn clean",
        "mvn deploy",
        "mvn site",
        // Gradle
        "gradle build",
        "gradle assemble",
        "gradle compileJava",
        "gradle compileKotlin",
        "./gradlew build",
        "./gradlew assemble",
        "./gradlew compileJava",
        // Docker
        "docker build",
        "docker compose build",
        "docker-compose build",
        // Terraform
        "terraform plan",
        "terraform apply",
        "terraform init",
    ];
    patterns.iter().any(|p| cmd_contains(cmd, p))
}

fn is_git_command(cmd: &str) -> bool {
    let patterns = [
        "git status",
        "git diff",
        "git log",
        "git show",
        "git branch",
        "git stash",
    ];
    patterns.iter().any(|p| cmd_contains(cmd, p))
}

fn is_package_command(cmd: &str) -> bool {
    let patterns = [
        // JavaScript
        "npm install",
        "npm ci",
        "yarn install",
        "yarn add",
        "pnpm install",
        "pnpm add",
        // Python
        "pip install",
        "pip3 install",
        "poetry install",
        // Rust
        "cargo install",
        // Go
        "go mod download",
        "go mod tidy",
        "go get ",
        // PHP
        "composer install",
        "composer update",
        "composer require",
        // Ruby
        "bundle install",
        "bundler install",
        "gem install",
        // System
        "apt install",
        "apt-get install",
        "brew install",
        // Maven / Gradle deps
        "mvn dependency:",
        "gradle dependencies",
        "./gradlew dependencies",
        // .NET
        "dotnet restore",
        "nuget install",
    ];
    patterns.iter().any(|p| cmd_contains(cmd, p))
}

/// Check whether `cmd` contains the pattern, accounting for leading pipes,
/// `&&`, `||`, `cd dir &&`, etc.
fn cmd_contains(cmd: &str, pattern: &str) -> bool {
    // Simple substring match is sufficient for Phase 1.
    // We match against the full command text which catches piped / chained
    // commands too (e.g. `cd foo && cargo test`).
    cmd.contains(pattern)
}

// ── Main Entry Point ────────────────────────────────────────────────────────

/// Outcome of the compression pipeline.
pub(crate) struct CompressionResult {
    /// The (potentially) compressed result string to replace `ToolResult.result`.
    pub compressed: String,
    /// Whether any compression was actually applied.
    pub was_compressed: bool,
}

/// Compress a `ToolExecutionOutcome` in-place.
///
/// Returns the same outcome with the `ToolResult.result` potentially replaced
/// by a compressed version.  If no compression is needed the outcome is
/// returned unchanged.
pub(super) async fn maybe_compress(
    tool_name: &str,
    args_json: &str,
    session_id: &str,
    mut outcome: ToolExecutionOutcome,
    max_tool_output_tokens: u32,
    context_pressure: Option<ContextPressure>,
    task_hint: Option<&TaskCompressionHint>,
) -> ToolExecutionOutcome {
    let result = match outcome.result {
        Ok(ref mut result) => result,
        Err(_) => return outcome, // error path — never compress
    };

    let scenario = detect_scenario(tool_name, args_json);
    if scenario != OutputScenario::PassThrough {
        let original = result.result.clone();
        let base_tier = classify_tier(&original);
        let tier = adjust_tier_for_pressure(base_tier, context_pressure.as_ref());

        let compressed = match tier {
            CompressionTier::Passthrough => CompressionResult {
                compressed: original.clone(),
                was_compressed: false,
            },
            CompressionTier::Light => apply_light_compression(&original),
            CompressionTier::Standard | CompressionTier::Aggressive => {
                compress_by_scenario(scenario, &original, args_json, tier)
            }
        };

        if compressed.was_compressed {
            // Tee-save full output when compression occurred.
            let tee_note =
                tee::tee_save_if_needed(session_id, args_json, &original, &compressed.compressed)
                    .await;

            // Replace result with compressed version (+ optional tee note).
            result.result = match tee_note {
                Some(note) => format!("{}\n\n{}", compressed.compressed, note),
                None => compressed.compressed,
            };
        }
    }

    // Enforce hard token ceiling if configured.
    if max_tool_output_tokens > 0 {
        use bamboo_compression::TokenCounter;
        let counter = bamboo_compression::TiktokenTokenCounter::default();
        let tokens = counter.count_text(&result.result);
        if tokens > max_tool_output_tokens {
            let truncated = truncate_to_token_budget(
                &result.result,
                max_tool_output_tokens,
                &counter,
                task_hint,
            );
            tracing::info!(
                "[{}] Tool output truncated: {} tokens > {} limit, {} chars → {} chars",
                session_id,
                tokens,
                max_tool_output_tokens,
                result.result.len(),
                truncated.len(),
            );
            result.result = truncated;
        }
    }

    outcome
}

/// Task-aware compression hint: significant terms from the ACTIVE task's
/// completion criteria (+ description). When a tool output must be truncated to
/// fit the token budget, lines matching any term are preferentially preserved so
/// task-relevant evidence (e.g. the criterion the agent is verifying) survives
/// the cut. Built once per round from the `TaskLoopContext`
/// (see `build_task_compression_hint`). This is the Phase-4 "task-aware" masking:
/// truncation is biased toward what the active task needs, not just head/tail.
#[derive(Debug, Clone, Default)]
pub(crate) struct TaskCompressionHint {
    keywords: Vec<String>,
}

impl TaskCompressionHint {
    /// Build from raw phrases (criteria / description): split into lowercased
    /// significant terms (≥4 chars), deduped, capped.
    pub(crate) fn from_phrases<I: IntoIterator<Item = String>>(phrases: I) -> Self {
        let mut keywords: Vec<String> = Vec::new();
        'outer: for phrase in phrases {
            for term in phrase.split(|c: char| !c.is_alphanumeric()) {
                let term = term.trim().to_lowercase();
                if term.len() >= 4 && !keywords.contains(&term) {
                    keywords.push(term);
                }
                if keywords.len() >= 32 {
                    break 'outer;
                }
            }
        }
        Self { keywords }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.keywords.is_empty()
    }

    fn matches(&self, line: &str) -> bool {
        let lower = line.to_lowercase();
        self.keywords.iter().any(|k| lower.contains(k.as_str()))
    }
}

/// Collect up to `max_tokens` worth of lines from `text` that match the task
/// hint, for preservation when the output is otherwise truncated. Returns `None`
/// when no lines match (or the hint is empty).
fn collect_task_relevant_lines(
    text: &str,
    hint: &TaskCompressionHint,
    max_tokens: u32,
    counter: &bamboo_compression::TiktokenTokenCounter,
) -> Option<String> {
    use bamboo_compression::TokenCounter;
    if hint.is_empty() || max_tokens == 0 {
        return None;
    }
    let mut kept: Vec<&str> = Vec::new();
    let mut used = 0u32;
    for line in text.lines() {
        if !hint.matches(line) {
            continue;
        }
        let cost = counter.count_text(line).saturating_add(1);
        if used.saturating_add(cost) > max_tokens {
            break;
        }
        used += cost;
        kept.push(line);
        if kept.len() >= 50 {
            break;
        }
    }
    if kept.is_empty() {
        None
    } else {
        Some(kept.join("\n"))
    }
}

/// Truncate text to fit within a token budget by keeping head + tail portions.
///
/// When a [`TaskCompressionHint`] is present AND some lines in the dropped middle
/// match it, a slice of the budget is reserved to preserve those task-relevant
/// lines (Phase-4 task-aware masking). With no hint (or no match) the behaviour
/// is the original 70/30 head/tail split, byte-for-byte.
fn truncate_to_token_budget(
    text: &str,
    max_tokens: u32,
    counter: &bamboo_compression::TiktokenTokenCounter,
    task_hint: Option<&TaskCompressionHint>,
) -> String {
    use bamboo_compression::TokenCounter;

    let preserved = task_hint
        .filter(|hint| !hint.is_empty())
        .and_then(|hint| {
            collect_task_relevant_lines(text, hint, (max_tokens as f64 * 0.15) as u32, counter)
        });

    if let Some(lines) = preserved {
        // Reserve ~15% of the budget for task-relevant lines and shrink head/tail
        // to 0.5/0.2, leaving ~15% headroom for the truncation marker + role/
        // newline boilerplate so the assembled output stays within `max_tokens`
        // (head + tail are sliced from the full text, so their sum must stay
        // comfortably under budget rather than at it).
        let head_tokens = (max_tokens as f64 * 0.5) as u32;
        let tail_tokens = (max_tokens as f64 * 0.2) as u32;
        let head = find_prefix_within_tokens(text, head_tokens, counter);
        let tail = find_suffix_within_tokens(text, tail_tokens, counter);
        return format!(
            "{}\n\n[... tool output truncated: {} tokens → {} token budget; task-relevant lines preserved ...]\n{}\n\n{}",
            head,
            counter.count_text(text),
            max_tokens,
            lines,
            tail,
        );
    }

    let head_tokens = (max_tokens as f64 * 0.7) as u32;
    let tail_tokens = max_tokens.saturating_sub(head_tokens);

    let head = find_prefix_within_tokens(text, head_tokens, counter);
    let tail = find_suffix_within_tokens(text, tail_tokens, counter);

    format!(
        "{}\n\n[... tool output truncated: {} tokens → {} token budget ...]\n\n{}",
        head,
        counter.count_text(text),
        max_tokens,
        tail,
    )
}

fn find_prefix_within_tokens(
    text: &str,
    max_tokens: u32,
    counter: &bamboo_compression::TiktokenTokenCounter,
) -> String {
    use bamboo_compression::TokenCounter;
    let mut end = text.len();
    for (i, _) in text.char_indices() {
        if counter.count_text(&text[..i]) > max_tokens {
            end = i;
            break;
        }
    }
    text[..end].to_string()
}

fn find_suffix_within_tokens(
    text: &str,
    max_tokens: u32,
    counter: &bamboo_compression::TiktokenTokenCounter,
) -> String {
    use bamboo_compression::TokenCounter;
    let mut start = 0;
    for (i, _) in text.char_indices().rev() {
        if counter.count_text(&text[i..]) > max_tokens {
            break;
        }
        start = i;
    }
    text[start..].to_string()
}

/// Dispatch to the appropriate scenario compressor.
fn compress_by_scenario(
    scenario: OutputScenario,
    raw_result: &str,
    _args_json: &str,
    tier: CompressionTier,
) -> CompressionResult {
    match scenario {
        OutputScenario::BashTest => scenarios::bash_test::compress(raw_result, tier),
        OutputScenario::BashBuild => scenarios::bash_build::compress(raw_result, tier),
        OutputScenario::BashGit => scenarios::bash_git::compress(raw_result, tier),
        OutputScenario::BashPackage => scenarios::bash_package::compress(raw_result, tier),
        OutputScenario::BashGeneric => scenarios::bash_generic::compress(raw_result, tier),
        OutputScenario::ReadCode => scenarios::read_code::compress(raw_result, tier),
        OutputScenario::GrepResults => scenarios::grep_results::compress(raw_result, tier),
        OutputScenario::WebFetchHtml => scenarios::web_fetch::compress(raw_result, tier),
        OutputScenario::PassThrough => CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        },
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_cargo_test() {
        let args = r#"{"command": "cargo test --workspace"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashTest);
    }

    #[test]
    fn detect_npm_test() {
        let args = r#"{"command": "npm test"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashTest);
    }

    #[test]
    fn detect_pytest() {
        let args = r#"{"command": "cd myproject && pytest -v"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashTest);
    }

    #[test]
    fn detect_cargo_check() {
        let args = r#"{"command": "cargo check"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashBuild);
    }

    #[test]
    fn detect_cargo_clippy() {
        let args = r#"{"command": "cargo clippy -- -D warnings"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashBuild);
    }

    #[test]
    fn detect_git_status() {
        let args = r#"{"command": "git status"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashGit);
    }

    #[test]
    fn detect_git_diff() {
        let args = r#"{"command": "git diff --stat"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashGit);
    }

    #[test]
    fn detect_npm_install() {
        let args = r#"{"command": "npm install"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashPackage);
    }

    #[test]
    fn detect_generic_bash() {
        let args = r#"{"command": "ls -la"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashGeneric);
    }

    #[test]
    fn detect_chained_cargo_test() {
        let args = r#"{"command": "cd bamboo && cargo test --lib"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashTest);
    }

    #[test]
    fn detect_read_tool() {
        assert_eq!(detect_scenario("Read", "{}"), OutputScenario::ReadCode);
        assert_eq!(detect_scenario("read_file", "{}"), OutputScenario::ReadCode);
    }

    #[test]
    fn detect_grep_tool() {
        assert_eq!(detect_scenario("Grep", "{}"), OutputScenario::GrepResults);
    }

    #[test]
    fn detect_webfetch_tool() {
        assert_eq!(
            detect_scenario("WebFetch", "{}"),
            OutputScenario::WebFetchHtml
        );
    }

    #[test]
    fn detect_passthrough_tools() {
        assert_eq!(detect_scenario("Edit", "{}"), OutputScenario::PassThrough);
        assert_eq!(detect_scenario("Write", "{}"), OutputScenario::PassThrough);
        assert_eq!(detect_scenario("Task", "{}"), OutputScenario::PassThrough);
        assert_eq!(
            detect_scenario("conclusion_with_options", "{}"),
            OutputScenario::PassThrough
        );
    }

    #[test]
    fn detect_invalid_args_json() {
        // Invalid JSON should fall through gracefully
        assert_eq!(
            detect_scenario("Bash", "not-json"),
            OutputScenario::BashGeneric
        );
    }

    // ── Maven command detection ──

    #[test]
    fn detect_mvn_compile() {
        let args = r#"{"command": "mvn compile"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashBuild);
    }

    #[test]
    fn detect_mvn_clean_install() {
        let args = r#"{"command": "mvn clean install -DskipTests"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashBuild);
    }

    #[test]
    fn detect_mvn_package() {
        let args = r#"{"command": "mvn package -Dmaven.test.skip=true"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashBuild);
    }

    #[test]
    fn detect_mvn_verify() {
        let args = r#"{"command": "mvn verify"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashBuild);
    }

    #[test]
    fn detect_mvn_test() {
        let args = r#"{"command": "mvn test"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashTest);
    }

    #[test]
    fn detect_mvn_dependency() {
        let args = r#"{"command": "mvn dependency:tree"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashPackage);
    }

    // ── Gradle / Docker / Terraform detection ──

    #[test]
    fn detect_gradle_build() {
        let args = r#"{"command": "gradle build"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashBuild);
    }

    #[test]
    fn detect_gradlew_build() {
        let args = r#"{"command": "./gradlew build"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashBuild);
    }

    #[test]
    fn detect_gradlew_test() {
        let args = r#"{"command": "./gradlew test"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashTest);
    }

    #[test]
    fn detect_docker_build() {
        let args = r#"{"command": "docker build -t myapp ."}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashBuild);
    }

    #[test]
    fn detect_terraform_plan() {
        let args = r#"{"command": "terraform plan"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashBuild);
    }

    #[test]
    fn detect_go_mod_download() {
        let args = r#"{"command": "go mod download"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashPackage);
    }

    #[test]
    fn detect_composer_install() {
        let args = r#"{"command": "composer install"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashPackage);
    }

    #[test]
    fn detect_bundle_install() {
        let args = r#"{"command": "bundle install"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashPackage);
    }

    #[test]
    fn detect_dotnet_restore() {
        let args = r#"{"command": "dotnet restore"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashPackage);
    }

    #[test]
    fn detect_poetry_install() {
        let args = r#"{"command": "poetry install"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashPackage);
    }

    // ── Context pressure tests ──

    #[test]
    fn pressure_low_skips_light_tier() {
        let pressure = ContextPressure {
            usage_percent: 30,
            remaining_tokens: 100_000,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Light, Some(&pressure)),
            CompressionTier::Passthrough
        );
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Standard, Some(&pressure)),
            CompressionTier::Standard
        );
    }

    #[test]
    fn pressure_normal_uses_base_tier() {
        let pressure = ContextPressure {
            usage_percent: 65,
            remaining_tokens: 50_000,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Light, Some(&pressure)),
            CompressionTier::Light
        );
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Standard, Some(&pressure)),
            CompressionTier::Standard
        );
    }

    #[test]
    fn pressure_high_promotes_light_to_standard() {
        let pressure = ContextPressure {
            usage_percent: 90,
            remaining_tokens: 5_000,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Light, Some(&pressure)),
            CompressionTier::Standard
        );
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Passthrough, Some(&pressure)),
            CompressionTier::Passthrough
        );
    }

    #[test]
    fn pressure_none_uses_base_tier() {
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Light, None),
            CompressionTier::Light
        );
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Passthrough, None),
            CompressionTier::Passthrough
        );
    }

    // ── Tier classification edge cases ──

    #[test]
    fn classify_tier_empty_string() {
        assert_eq!(classify_tier(""), CompressionTier::Passthrough);
    }

    #[test]
    fn classify_tier_short_string() {
        assert_eq!(classify_tier("hello world"), CompressionTier::Passthrough);
    }

    #[test]
    fn classify_tier_aggressive_is_reachable() {
        // Use real words for more accurate token count; each word ≈ 1 token
        let words: Vec<String> = (0..30_000).map(|i| format!("word{}", i)).collect();
        let big = words.join(" ");
        let tier = classify_tier(&big);
        assert_eq!(tier, CompressionTier::Aggressive);
    }

    // ── Pressure boundary edge cases ──

    #[test]
    fn pressure_boundary_50_percent() {
        // Exactly 50% → normal range (50-80)
        let pressure = ContextPressure {
            usage_percent: 50,
            remaining_tokens: 50_000,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Light, Some(&pressure)),
            CompressionTier::Light
        );
    }

    #[test]
    fn pressure_boundary_80_percent() {
        // Exactly 80% → normal range (50-80)
        let pressure = ContextPressure {
            usage_percent: 80,
            remaining_tokens: 20_000,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Light, Some(&pressure)),
            CompressionTier::Light
        );
    }

    #[test]
    fn pressure_boundary_81_percent() {
        // 81% → high pressure range (>80)
        let pressure = ContextPressure {
            usage_percent: 81,
            remaining_tokens: 19_000,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Light, Some(&pressure)),
            CompressionTier::Standard
        );
    }

    #[test]
    fn pressure_boundary_49_percent() {
        // 49% → low pressure range (<50)
        let pressure = ContextPressure {
            usage_percent: 49,
            remaining_tokens: 51_000,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Light, Some(&pressure)),
            CompressionTier::Passthrough
        );
    }

    #[test]
    fn pressure_does_not_change_aggressive() {
        let low = ContextPressure {
            usage_percent: 10,
            remaining_tokens: 90_000,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Aggressive, Some(&low)),
            CompressionTier::Aggressive
        );
    }

    #[test]
    fn pressure_does_not_change_standard_at_low() {
        let low = ContextPressure {
            usage_percent: 10,
            remaining_tokens: 90_000,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Standard, Some(&low)),
            CompressionTier::Standard
        );
    }

    #[test]
    fn pressure_zero_percent() {
        let pressure = ContextPressure {
            usage_percent: 0,
            remaining_tokens: 200_000,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Light, Some(&pressure)),
            CompressionTier::Passthrough
        );
    }

    #[test]
    fn pressure_100_percent() {
        let pressure = ContextPressure {
            usage_percent: 100,
            remaining_tokens: 0,
        };
        assert_eq!(
            adjust_tier_for_pressure(CompressionTier::Light, Some(&pressure)),
            CompressionTier::Standard
        );
    }

    // ── Light compression edge cases ──

    #[test]
    fn light_compression_plain_text_no_changes() {
        let input = "hello\nworld\n";
        let result = apply_light_compression(input);
        assert!(!result.was_compressed);
    }

    #[test]
    fn light_compression_strips_ansi_plain_text() {
        let input = "\x1b[32mgreen text\x1b[0m\n";
        let result = apply_light_compression(input);
        assert!(result.was_compressed);
        assert!(!result.compressed.contains("\x1b["));
    }

    #[test]
    fn light_compression_json_envelope() {
        let json = serde_json::json!({
            "command": "echo",
            "stdout": "\x1b[32mhello\x1b[0m",
            "stderr": "",
            "exit_code": 0,
        })
        .to_string();
        let result = apply_light_compression(&json);
        assert!(result.was_compressed);
        assert!(!result.compressed.contains("\x1b["));
        // Should still be valid JSON
        let parsed: serde_json::Value = serde_json::from_str(&result.compressed).unwrap();
        let stdout_val = parsed["stdout"].as_str().unwrap();
        assert!(stdout_val.starts_with("hello"));
    }

    #[test]
    fn light_compression_json_no_ansi_not_compressed() {
        // Use content with trailing newline so collapse_blank_lines doesn't add one
        let json = serde_json::json!({
            "command": "echo",
            "stdout": "clean output\n",
            "stderr": "no ansi here\n",
            "exit_code": 0,
        })
        .to_string();
        let result = apply_light_compression(&json);
        assert!(!result.was_compressed);
    }

    // ── Scenario detection edge cases ──

    #[test]
    fn detect_piped_cargo_test() {
        let args = r#"{"command": "cargo test 2>&1 | tee output.log"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashTest);
    }

    #[test]
    fn detect_bashoutput_uses_bash_detection() {
        let args = r#"{"command": "git status"}"#;
        assert_eq!(detect_scenario("BashOutput", args), OutputScenario::BashGit);
    }

    #[test]
    fn detect_command_with_env_prefix() {
        let args = r#"{"command": "CI=true cargo test"}"#;
        assert_eq!(detect_scenario("Bash", args), OutputScenario::BashTest);
    }

    // ── Task-aware truncation (Phase 4) ──

    #[test]
    fn truncate_preserves_task_relevant_middle_lines() {
        use bamboo_compression::TokenCounter;
        let counter = bamboo_compression::TiktokenTokenCounter::default();
        let mut lines: Vec<String> = (0..400)
            .map(|i| format!("filler line number {i} with some padding words"))
            .collect();
        lines.insert(200, "CRITICAL zephyrqux module test FAILED here".to_string());
        let text = lines.join("\n");
        let max_tokens = 80;
        assert!(counter.count_text(&text) > max_tokens, "text must exceed budget");

        let hint = TaskCompressionHint::from_phrases(vec!["zephyrqux module passes".to_string()]);
        let out = truncate_to_token_budget(&text, max_tokens, &counter, Some(&hint));
        assert!(
            out.contains("task-relevant lines preserved"),
            "preserved-section marker present"
        );
        assert!(
            out.contains("zephyrqux"),
            "the matching middle line must survive truncation"
        );
    }

    #[test]
    fn truncate_without_hint_keeps_original_format() {
        let counter = bamboo_compression::TiktokenTokenCounter::default();
        let text = (0..400)
            .map(|i| format!("line {i} content"))
            .collect::<Vec<_>>()
            .join("\n");
        let out = truncate_to_token_budget(&text, 50, &counter, None);
        assert!(out.contains("[... tool output truncated:"));
        assert!(!out.contains("task-relevant lines preserved"));
    }

    #[test]
    fn task_hint_extracts_significant_terms_and_matches() {
        let hint =
            TaskCompressionHint::from_phrases(vec!["All tests pass for module X".to_string()]);
        assert!(!hint.is_empty());
        assert!(hint.matches("re-running the module checks"));
        assert!(!hint.matches("xyz qrs unrelated"));
    }
}
