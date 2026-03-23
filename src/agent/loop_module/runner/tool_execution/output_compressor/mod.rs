//! Post-execution output compression pipeline.
//!
//! Compresses tool output *after* execution but *before* the result enters
//! the session message list.  This keeps the LLM context window lean while
//! the full output is optionally tee-saved for on-demand inspection.

pub(crate) mod filters;
pub(crate) mod scenarios;
pub(crate) mod tee;

use super::per_call::ToolExecutionOutcome;

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
) -> ToolExecutionOutcome {
    let result = match outcome.result {
        Ok(ref mut result) => result,
        Err(_) => return outcome, // error path — never compress
    };

    let scenario = detect_scenario(tool_name, args_json);
    if scenario == OutputScenario::PassThrough {
        return outcome;
    }

    let original = result.result.clone();
    let compressed = compress_by_scenario(scenario, &original, args_json);

    if !compressed.was_compressed {
        return outcome;
    }

    // Tee-save full output when compression occurred.
    let tee_note = tee::tee_save_if_needed(session_id, args_json, &original, &compressed.compressed).await;

    // Replace result with compressed version (+ optional tee note).
    result.result = match tee_note {
        Some(note) => format!("{}\n\n{}", compressed.compressed, note),
        None => compressed.compressed,
    };

    outcome
}

/// Dispatch to the appropriate scenario compressor.
fn compress_by_scenario(
    scenario: OutputScenario,
    raw_result: &str,
    _args_json: &str,
) -> CompressionResult {
    match scenario {
        OutputScenario::BashTest => scenarios::bash_test::compress(raw_result),
        OutputScenario::BashBuild => scenarios::bash_build::compress(raw_result),
        OutputScenario::BashGit => scenarios::bash_git::compress(raw_result),
        OutputScenario::BashPackage => scenarios::bash_package::compress(raw_result),
        OutputScenario::BashGeneric => scenarios::bash_generic::compress(raw_result),
        OutputScenario::ReadCode => scenarios::read_code::compress(raw_result),
        OutputScenario::GrepResults => scenarios::grep_results::compress(raw_result),
        OutputScenario::WebFetchHtml => scenarios::web_fetch::compress(raw_result),
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
        assert_eq!(detect_scenario("WebFetch", "{}"), OutputScenario::WebFetchHtml);
    }

    #[test]
    fn detect_passthrough_tools() {
        assert_eq!(detect_scenario("Edit", "{}"), OutputScenario::PassThrough);
        assert_eq!(detect_scenario("Write", "{}"), OutputScenario::PassThrough);
        assert_eq!(detect_scenario("Task", "{}"), OutputScenario::PassThrough);
        assert_eq!(detect_scenario("ask_user", "{}"), OutputScenario::PassThrough);
    }

    #[test]
    fn detect_invalid_args_json() {
        // Invalid JSON should fall through gracefully
        assert_eq!(detect_scenario("Bash", "not-json"), OutputScenario::BashGeneric);
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
}
