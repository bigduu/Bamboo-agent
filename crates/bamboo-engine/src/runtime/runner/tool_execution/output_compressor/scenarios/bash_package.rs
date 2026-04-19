//! Compression for package manager output: `npm install`, `cargo build`, `pip install`, etc.
//!
//! **Strategy:**
//! - Success → single-line summary: `"✅ npm install: added N packages in Xs"`
//! - Drop progress bars, download indicators, tree output
//! - Keep warnings and audit notices

use regex::Regex;

use crate::runtime::runner::tool_execution::output_compressor::filters;
use crate::runtime::runner::tool_execution::output_compressor::CompressionResult;

/// Minimum result length (chars) before compression kicks in.
const MIN_COMPRESS_LEN: usize = 1500;

/// Max lines for compressed package output.
const MAX_OUTPUT_LINES: usize = 80;

lazy_static::lazy_static! {
    // ── npm ──

    /// Matches: `added 150 packages in 8s` or `added 150 packages, and audited 200 packages in 12s`
    static ref NPM_ADDED_RE: Regex = Regex::new(
        r"added (\d+) packages?(?:.*in\s+(\S+))?"
    ).expect("NPM_ADDED_RE must compile");

    /// Matches npm audit summary: `found 0 vulnerabilities`
    static ref NPM_AUDIT_RE: Regex = Regex::new(
        r"found (\d+) vulnerabilit"
    ).expect("NPM_AUDIT_RE must compile");

    /// Matches npm progress/download noise
    static ref NPM_NOISE_RE: Regex = Regex::new(
        r"(?:npm (?:warn|notice|http)|GET |reify:|timing |idealTree)"
    ).expect("NPM_NOISE_RE must compile");

    // ── cargo build ──

    /// Matches: `Compiling foo v0.1.0`
    static ref CARGO_COMPILE_RE: Regex = Regex::new(
        r"^\s+(Compiling|Downloading|Downloaded|Updating)\s+\S+"
    ).expect("CARGO_COMPILE_RE must compile");

    /// Matches: `Finished dev [unoptimized + debuginfo] target(s) in 12.34s`
    static ref CARGO_FINISHED_RE: Regex = Regex::new(
        r"Finished\s+.+in\s+(\S+)"
    ).expect("CARGO_FINISHED_RE must compile");

    // ── pip ──

    /// Matches: `Successfully installed foo-1.0.0 bar-2.0.0`
    static ref PIP_INSTALLED_RE: Regex = Regex::new(
        r"Successfully installed (.+)"
    ).expect("PIP_INSTALLED_RE must compile");

    /// Matches pip download progress: `Downloading foo-1.0.tar.gz (100 kB)` or `━━━━`
    static ref PIP_NOISE_RE: Regex = Regex::new(
        r"(?:Downloading |━+|Collecting |Using cached |Preparing metadata)"
    ).expect("PIP_NOISE_RE must compile");

    // ── yarn / pnpm ──

    /// Matches: `Done in 3.5s.` or `✨ Done in 3.5s.`
    static ref YARN_DONE_RE: Regex = Regex::new(
        r"Done in\s+(\S+)"
    ).expect("YARN_DONE_RE must compile");

    // ── Go ──

    /// Matches: `go: downloading github.com/foo/bar v1.2.3`
    static ref GO_DOWNLOAD_RE: Regex = Regex::new(
        r"(?m)^go: (?:downloading|finding|extracting)\s+\S+"
    ).expect("GO_DOWNLOAD_RE must compile");

    /// Matches: `go: added github.com/foo/bar v1.2.3` (go get output)
    static ref GO_ADDED_RE: Regex = Regex::new(
        r"(?m)^go: added\s+\S+"
    ).expect("GO_ADDED_RE must compile");

    // ── Composer (PHP) ──

    /// Matches: `- Installing foo/bar (v1.2.3)` or `- Upgrading foo/bar (v1.0 => v2.0)`
    static ref COMPOSER_INSTALL_RE: Regex = Regex::new(
        r"(?m)^\s+-\s+(?:Installing|Upgrading|Removing)\s+\S+"
    ).expect("COMPOSER_INSTALL_RE must compile");

    /// Matches: `Downloading (100%)`
    static ref COMPOSER_NOISE_RE: Regex = Regex::new(
        r"(?m)^(?:\s+Downloading|\s+-\s+Loading)"
    ).expect("COMPOSER_NOISE_RE must compile");

    /// Matches: `Package manifest generated successfully.` or `Generating optimized autoload files`
    static ref COMPOSER_DONE_RE: Regex = Regex::new(
        r"(?m)^(?:Package manifest generated|Generating (?:optimized )?autoload)"
    ).expect("COMPOSER_DONE_RE must compile");

    // ── Bundler (Ruby) ──

    /// Matches: `Fetching gem metadata from https://rubygems.org/`
    static ref BUNDLER_FETCH_RE: Regex = Regex::new(
        r"(?m)^(?:Fetching |Downloading |Installing )\S+"
    ).expect("BUNDLER_FETCH_RE must compile");

    /// Matches: `Bundle complete! 42 Gemfile dependencies, 120 gems now installed.`
    static ref BUNDLER_DONE_RE: Regex = Regex::new(
        r"(?m)^Bundle complete!\s+(\d+)\s+Gemfile dependencies?,\s+(\d+)\s+gems?"
    ).expect("BUNDLER_DONE_RE must compile");

    // ── Gradle deps ──

    /// Matches Gradle dependency tree lines: `+--- org.foo:bar:1.0`
    static ref GRADLE_DEP_RE: Regex = Regex::new(
        r"(?m)^[|+\\]\-\-\-"
    ).expect("GRADLE_DEP_RE must compile");

    // ── dotnet restore / NuGet ──

    /// Matches: `  Restored /path/project.csproj`
    static ref DOTNET_PKG_RESTORE_RE: Regex = Regex::new(
        r"(?m)^\s+Restored\s+\S+\.csproj"
    ).expect("DOTNET_PKG_RESTORE_RE must compile");
}

// ── Public Entry Point ─────────────────────────────────────────────────────

pub(crate) fn compress(raw_result: &str) -> CompressionResult {
    if raw_result.len() < MIN_COMPRESS_LEN {
        return CompressionResult {
            compressed: raw_result.to_string(),
            was_compressed: false,
        };
    }

    // Parse the Bash JSON envelope
    let parsed: serde_json::Value = match serde_json::from_str(raw_result) {
        Ok(v) => v,
        Err(_) => return compress_plain_package_text(raw_result),
    };

    let stdout = parsed.get("stdout").and_then(|v| v.as_str()).unwrap_or("");
    let stderr = parsed.get("stderr").and_then(|v| v.as_str()).unwrap_or("");
    let exit_code = parsed
        .get("exit_code")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);
    let command = parsed.get("command").and_then(|v| v.as_str()).unwrap_or("");

    let clean_stdout = filters::strip_ansi(stdout);
    let clean_stderr = filters::strip_ansi(stderr);
    let combined = format!("{}\n{}", clean_stdout, clean_stderr);

    // Try package-manager-specific compression
    if command.contains("npm") {
        if let Some(result) = compress_npm(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if command.contains("cargo build") || command.contains("cargo install") {
        if let Some(result) = compress_cargo_build(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if command.contains("pip") {
        if let Some(result) = compress_pip(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if command.contains("yarn") || command.contains("pnpm") {
        if let Some(result) = compress_yarn_pnpm(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if command.contains("go mod") || command.contains("go get") {
        if let Some(result) = compress_go_mod(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if command.contains("composer") {
        if let Some(result) = compress_composer(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if command.contains("bundle") || command.contains("gem install") {
        if let Some(result) = compress_bundler(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if command.contains("gradle dep") || command.contains("gradlew dep") {
        if let Some(result) = compress_gradle_deps(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if command.contains("dotnet restore") || command.contains("nuget") {
        if let Some(result) = compress_dotnet_restore(&combined, exit_code, &parsed) {
            return result;
        }
    }

    // Auto-detect from output
    if NPM_ADDED_RE.is_match(&combined) {
        if let Some(result) = compress_npm(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if CARGO_FINISHED_RE.is_match(&combined) {
        if let Some(result) = compress_cargo_build(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if GO_DOWNLOAD_RE.is_match(&combined) {
        if let Some(result) = compress_go_mod(&combined, exit_code, &parsed) {
            return result;
        }
    }
    if BUNDLER_DONE_RE.is_match(&combined) {
        if let Some(result) = compress_bundler(&combined, exit_code, &parsed) {
            return result;
        }
    }

    // Fallback: generic compression
    super::bash_generic::compress(raw_result)
}

// ── npm ─────────────────────────────────────────────────────────────────────

fn compress_npm(
    combined: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    if !NPM_ADDED_RE.is_match(combined) && !NPM_AUDIT_RE.is_match(combined) {
        return None;
    }

    if exit_code == 0 {
        // Extract summary info
        let added = NPM_ADDED_RE.captures(combined).map(|c| {
            let pkgs = &c[1];
            let time = c.get(2).map(|m| m.as_str()).unwrap_or("");
            if time.is_empty() {
                format!("added {} packages", pkgs)
            } else {
                format!("added {} packages in {}", pkgs, time)
            }
        });

        let audit = NPM_AUDIT_RE
            .captures(combined)
            .map(|c| {
                let vulns = c[1].parse::<u32>().unwrap_or(0);
                if vulns == 0 {
                    String::new()
                } else {
                    format!(", {} vulnerabilities", vulns)
                }
            })
            .unwrap_or_default();

        let summary = match added {
            Some(added_str) => format!("✅ npm install: {}{}", added_str, audit),
            None => "✅ npm install completed".to_string(),
        };

        // Also keep any npm warn lines
        let warnings = extract_warnings(combined);
        let mut result = summary;
        if !warnings.is_empty() {
            result.push('\n');
            result.push_str(&warnings);
        }

        return Some(rebuild_json(original, &result, ""));
    }

    // Failed npm install: keep error lines, drop noise
    Some(compress_keeping_important_lines(combined, original))
}

// ── cargo build ─────────────────────────────────────────────────────────────

fn compress_cargo_build(
    combined: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    if !CARGO_FINISHED_RE.is_match(combined) && !CARGO_COMPILE_RE.is_match(combined) {
        return None;
    }

    // Count compiled crates
    let compiled_count = combined
        .lines()
        .filter(|l| l.trim_start().starts_with("Compiling"))
        .count();

    if exit_code == 0 {
        let time = CARGO_FINISHED_RE
            .captures(combined)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str())
            .unwrap_or("");

        let mut summary = format!("✅ cargo build: compiled {} crates", compiled_count);
        if !time.is_empty() {
            summary.push_str(&format!(" in {}", time));
        }

        // Keep warnings from stderr
        let warnings = extract_rust_warnings(combined);
        if !warnings.is_empty() {
            summary.push('\n');
            summary.push_str(&warnings);
        }

        return Some(rebuild_json(original, &summary, ""));
    }

    // Failed build: keep error lines
    Some(compress_keeping_important_lines(combined, original))
}

// ── pip ─────────────────────────────────────────────────────────────────────

fn compress_pip(
    combined: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    if !PIP_INSTALLED_RE.is_match(combined) && !PIP_NOISE_RE.is_match(combined) {
        return None;
    }

    if exit_code == 0 {
        let installed = PIP_INSTALLED_RE
            .captures(combined)
            .map(|c| c[1].to_string())
            .unwrap_or_else(|| "packages".to_string());

        // Count installed packages
        let pkg_count = installed.split_whitespace().count();
        let summary = format!("✅ pip install: {} packages installed", pkg_count);

        return Some(rebuild_json(original, &summary, ""));
    }

    Some(compress_keeping_important_lines(combined, original))
}

// ── yarn / pnpm ─────────────────────────────────────────────────────────────

fn compress_yarn_pnpm(
    combined: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    if exit_code == 0 {
        let time = YARN_DONE_RE
            .captures(combined)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str())
            .unwrap_or("");

        if !time.is_empty() {
            let summary = format!("✅ install completed in {}", time);
            return Some(rebuild_json(original, &summary, ""));
        }
    }

    // Fallback
    Some(compress_keeping_important_lines(combined, original))
}

// ── Shared Helpers ──────────────────────────────────────────────────────────

/// Extract warning/notice lines from combined output.
fn extract_warnings(text: &str) -> String {
    let mut warnings = String::new();
    for line in text.lines() {
        let lower = line.to_lowercase();
        if (lower.contains("warn") || lower.contains("deprecated"))
            && (!NPM_NOISE_RE.is_match(line) || line.contains("WARN"))
        {
            warnings.push_str(line);
            warnings.push('\n');
        }
    }
    let (capped, _) = filters::cap_lines(&warnings, 20);
    capped
}

/// Extract Rust warning/error lines.
fn extract_rust_warnings(text: &str) -> String {
    let mut result = String::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("warning:") || trimmed.starts_with("error:") {
            result.push_str(line);
            result.push('\n');
        }
    }
    let (capped, _) = filters::cap_lines(&result, 30);
    capped
}

/// For failed installs: keep error/warning lines, drop progress noise.
fn compress_keeping_important_lines(text: &str, original: &serde_json::Value) -> CompressionResult {
    let mut result = String::new();

    for line in text.lines() {
        let lower = line.to_lowercase();
        // Keep: error lines, warning lines, summary lines
        if lower.contains("error")
            || lower.contains("warn")
            || lower.contains("fail")
            || lower.contains("not found")
            || lower.contains("permission denied")
            || lower.contains("EACCES")
            || lower.contains("ENOENT")
            || lower.contains("ERR!")
            || lower.contains("Could not")
        {
            result.push_str(line);
            result.push('\n');
            continue;
        }

        // Keep added/finished summary lines
        if NPM_ADDED_RE.is_match(line)
            || CARGO_FINISHED_RE.is_match(line)
            || PIP_INSTALLED_RE.is_match(line)
            || YARN_DONE_RE.is_match(line)
        {
            result.push_str(line);
            result.push('\n');
        }
    }

    if result.is_empty() {
        // Nothing important extracted — fall back to capped full output
        let (capped, _) = filters::cap_lines(text, MAX_OUTPUT_LINES);
        return rebuild_json(original, &capped, "");
    }

    let (capped, _) = filters::cap_lines(&result, MAX_OUTPUT_LINES);
    rebuild_json(original, &capped, "")
}

// ── Go mod ──────────────────────────────────────────────────────────────────

fn compress_go_mod(
    combined: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let download_count = GO_DOWNLOAD_RE.captures_iter(combined).count();
    let added_count = GO_ADDED_RE.captures_iter(combined).count();

    if download_count == 0 && added_count == 0 {
        return None;
    }

    let mut result = String::new();
    if exit_code == 0 {
        result.push_str("✅ go mod:");
    } else {
        result.push_str("❌ go mod failed:");
    }
    if download_count > 0 {
        result.push_str(&format!(" {} modules downloaded", download_count));
    }
    if added_count > 0 {
        result.push_str(&format!(" {} added", added_count));
    }
    result.push('\n');

    let warnings = extract_warnings(combined);
    if !warnings.is_empty() {
        result.push_str(&warnings);
    }

    let (capped, _) = filters::cap_lines(&result, MAX_OUTPUT_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── Composer ────────────────────────────────────────────────────────────────

fn compress_composer(
    combined: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let install_count = COMPOSER_INSTALL_RE.captures_iter(combined).count();
    let noise_count = COMPOSER_NOISE_RE.captures_iter(combined).count();

    if install_count == 0 && !COMPOSER_DONE_RE.is_match(combined) {
        return None;
    }

    let mut result = String::new();
    if exit_code == 0 {
        result.push_str(&format!(
            "✅ composer: {} packages installed",
            install_count
        ));
    } else {
        result.push_str(&format!("❌ composer failed ({} packages)", install_count));
    }
    result.push('\n');

    if noise_count > 0 {
        result.push_str(&format!(
            "[{} download/progress lines stripped]\n",
            noise_count
        ));
    }

    let warnings = extract_warnings(combined);
    if !warnings.is_empty() {
        result.push_str(&warnings);
    }

    let (capped, _) = filters::cap_lines(&result, MAX_OUTPUT_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── Bundler ─────────────────────────────────────────────────────────────────

fn compress_bundler(
    combined: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let fetch_count = BUNDLER_FETCH_RE.captures_iter(combined).count();

    let mut result = String::new();

    if let Some(cap) = BUNDLER_DONE_RE.captures(combined) {
        let deps = cap.get(1).map(|m| m.as_str()).unwrap_or("?");
        let gems = cap.get(2).map(|m| m.as_str()).unwrap_or("?");
        result.push_str(&format!(
            "✅ bundle: {} dependencies, {} gems installed\n",
            deps, gems
        ));
    } else if exit_code == 0 {
        result.push_str(&format!(
            "✅ bundle install complete ({} fetched)\n",
            fetch_count
        ));
    } else {
        result.push_str("❌ bundle install failed\n");
    }

    if fetch_count > 0 {
        result.push_str(&format!("[{} fetch/install lines stripped]\n", fetch_count));
    }

    let warnings = extract_warnings(combined);
    if !warnings.is_empty() {
        result.push_str(&warnings);
    }

    let (capped, _) = filters::cap_lines(&result, MAX_OUTPUT_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── Gradle dependencies ─────────────────────────────────────────────────────

fn compress_gradle_deps(
    combined: &str,
    _exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let dep_count = GRADLE_DEP_RE.captures_iter(combined).count();
    if dep_count == 0 {
        return None;
    }

    let result = format!(
        "Gradle dependency tree: {} entries\n[tree output stripped]\n",
        dep_count
    );

    let (capped, _) = filters::cap_lines(&result, MAX_OUTPUT_LINES);
    Some(rebuild_json(original, &capped, ""))
}

// ── dotnet restore ──────────────────────────────────────────────────────────

fn compress_dotnet_restore(
    combined: &str,
    exit_code: i64,
    original: &serde_json::Value,
) -> Option<CompressionResult> {
    let restore_count = DOTNET_PKG_RESTORE_RE.captures_iter(combined).count();
    if restore_count == 0 {
        return None;
    }

    let mut result = String::new();
    if exit_code == 0 {
        result.push_str(&format!(
            "✅ dotnet restore: {} projects restored\n",
            restore_count
        ));
    } else {
        result.push_str(&format!(
            "❌ dotnet restore failed ({} partial)\n",
            restore_count
        ));
    }

    // Keep error lines
    for line in combined.lines() {
        let trimmed = line.trim();
        if trimmed.contains("error NU") || trimmed.contains("error :") {
            result.push_str(line);
            result.push('\n');
        }
    }

    let (capped, _) = filters::cap_lines(&result, MAX_OUTPUT_LINES);
    Some(rebuild_json(original, &capped, ""))
}

fn rebuild_json(
    original: &serde_json::Value,
    new_stdout: &str,
    new_stderr: &str,
) -> CompressionResult {
    let mut obj = original.clone();
    if let Some(map) = obj.as_object_mut() {
        map.insert(
            "stdout".to_string(),
            serde_json::Value::String(new_stdout.to_string()),
        );
        map.insert(
            "stderr".to_string(),
            serde_json::Value::String(new_stderr.to_string()),
        );
    }

    let compressed = serde_json::to_string(&obj).unwrap_or_else(|_| new_stdout.to_string());

    CompressionResult {
        compressed,
        was_compressed: true,
    }
}

fn compress_plain_package_text(text: &str) -> CompressionResult {
    let clean = filters::strip_ansi(text);
    let (capped, was_capped) = filters::cap_lines(&clean, MAX_OUTPUT_LINES);
    let (final_text, was_byte_capped) = filters::cap_bytes(&capped, filters::DEFAULT_MAX_BYTES);

    let any_change = was_capped || was_byte_capped || final_text.len() != text.len();

    CompressionResult {
        compressed: final_text,
        was_compressed: any_change,
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_bash_json(cmd: &str, stdout: &str, stderr: &str, exit_code: i64) -> String {
        serde_json::json!({
            "command": cmd,
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
            "timed_out": false,
            "stdout_truncated": false,
            "stderr_truncated": false,
        })
        .to_string()
    }

    fn pad(s: &str) -> String {
        format!("{}{}", s, "x".repeat(MIN_COMPRESS_LEN))
    }

    // ── npm ──

    #[test]
    fn npm_install_success() {
        let stdout = &pad("\
npm warn deprecated inflight@1.0.6: This module is not supported
npm warn deprecated glob@7.2.3: Glob versions prior to v9 are no longer supported

added 150 packages, and audited 200 packages in 8s

found 0 vulnerabilities
");
        let input = make_bash_json("npm install", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅"));
        assert!(result.compressed.contains("150 packages"));
    }

    #[test]
    fn npm_install_with_vulns() {
        let stdout = &pad("\
added 100 packages in 5s

5 vulnerabilities (3 moderate, 2 high)
found 5 vulnerabilities
");
        let input = make_bash_json("npm install", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅"));
        assert!(result.compressed.contains("100 packages"));
    }

    // ── cargo build ──

    #[test]
    fn cargo_build_success() {
        let stderr = &pad("\
   Compiling libc v0.2.150
   Compiling cfg-if v1.0.0
   Compiling serde v1.0.193
   Compiling tokio v1.35.0
   Compiling myapp v0.1.0
    Finished dev [unoptimized + debuginfo] target(s) in 12.34s
");
        let input = make_bash_json("cargo build", "", stderr, 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ cargo build"));
        assert!(result.compressed.contains("5 crates"));
        assert!(result.compressed.contains("12.34s"));
    }

    // ── pip ──

    #[test]
    fn pip_install_success() {
        let stdout = &pad("\
Collecting requests
  Downloading requests-2.31.0-py3-none-any.whl (62 kB)
     ━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 62.6/62.6 kB 3.2 MB/s eta 0:00:00
Collecting urllib3<3,>=1.21.1
  Using cached urllib3-2.1.0-py3-none-any.whl (104 kB)
Successfully installed certifi-2023.11.17 charset-normalizer-3.3.2 idna-3.6 requests-2.31.0 urllib3-2.1.0
");
        let input = make_bash_json("pip install requests", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ pip install"));
        assert!(result.compressed.contains("5 packages"));
    }

    // ── yarn ──

    #[test]
    fn yarn_install_success() {
        let stdout = &pad("\
yarn install v1.22.19
[1/4] Resolving packages...
[2/4] Fetching packages...
[3/4] Linking dependencies...
[4/4] Building fresh packages...
Done in 3.5s.
");
        let input = make_bash_json("yarn install", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅"));
        assert!(result.compressed.contains("3.5s"));
    }

    // ── Fallback ──

    #[test]
    fn short_output_not_compressed() {
        let input = make_bash_json("npm install", "added 5 packages", "", 0);
        let result = compress(&input);
        assert!(!result.was_compressed);
    }

    #[test]
    fn failed_install_keeps_errors() {
        let stderr = &pad("\
npm ERR! code ERESOLVE
npm ERR! ERESOLVE unable to resolve dependency tree
npm ERR!
npm ERR! Could not resolve dependency:
npm ERR! peer react@\"^16.8.0\" from some-package@1.0.0
");
        let input = make_bash_json("npm install", "", stderr, 1);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("ERR!") || result.compressed.contains("ERESOLVE"));
    }

    // ── Go mod tests ──

    #[test]
    fn go_mod_download_success() {
        let stdout = &pad("\
go: downloading github.com/foo/bar v1.2.3
go: downloading github.com/baz/qux v0.5.0
go: downloading github.com/some/pkg v2.1.0
go: downloading golang.org/x/text v0.14.0
go: downloading golang.org/x/crypto v0.18.0
go: finding github.com/foo/bar v1.2.3
go: extracting github.com/foo/bar v1.2.3
");
        let input = make_bash_json("go mod download", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ go mod:"));
        assert!(result.compressed.contains("modules downloaded"));
    }

    #[test]
    fn go_get_success() {
        let stdout = &pad("\
go: downloading github.com/new/pkg v1.0.0
go: added github.com/new/pkg v1.0.0
go: added github.com/dep/one v0.3.0
go: added github.com/dep/two v0.7.1
");
        let input = make_bash_json("go get github.com/new/pkg", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("✅ go mod:"));
        assert!(result.compressed.contains("added"));
    }

    // ── Composer tests ──

    #[test]
    fn composer_install_success() {
        let stdout = &pad("\
Loading composer repositories with package information
Updating dependencies
  - Installing monolog/monolog (v3.5.0)
  - Installing symfony/console (v7.0.2)
  - Installing symfony/http-kernel (v7.0.2)
  - Installing laravel/framework (v11.0.0)
  - Loading from cache
  - Loading from cache
  Downloading (100%)
  Downloading (100%)
Generating optimized autoload files
Package manifest generated successfully.
");
        let input = make_bash_json("composer install", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result
            .compressed
            .contains("✅ composer: 4 packages installed"));
        assert!(result
            .compressed
            .contains("download/progress lines stripped"));
    }

    // ── Bundler tests ──

    #[test]
    fn bundle_install_success() {
        let stdout = &pad("\
Fetching gem metadata from https://rubygems.org/..........
Fetching rake 13.1.0
Installing rake 13.1.0
Fetching minitest 5.21.2
Installing minitest 5.21.2
Fetching concurrent-ruby 1.2.3
Installing concurrent-ruby 1.2.3
Fetching rails 7.1.3
Installing rails 7.1.3
Bundle complete! 42 Gemfile dependencies, 120 gems now installed.
");
        let input = make_bash_json("bundle install", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result
            .compressed
            .contains("✅ bundle: 42 dependencies, 120 gems installed"));
    }

    // ── Gradle deps tests ──

    #[test]
    fn gradle_dependencies_tree() {
        let stdout = &pad("\
+--- org.springframework.boot:spring-boot-starter:3.2.0
|    +--- org.springframework.boot:spring-boot:3.2.0
|    |    +--- org.springframework:spring-core:6.1.1
|    |    \\--- org.springframework:spring-context:6.1.1
|    \\--- org.springframework.boot:spring-boot-autoconfigure:3.2.0
+--- com.fasterxml.jackson.core:jackson-databind:2.16.0
\\--- org.projectlombok:lombok:1.18.30
");
        let input = make_bash_json("gradle dependencies", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result.compressed.contains("Gradle dependency tree:"));
        assert!(result.compressed.contains("entries"));
    }

    // ── dotnet restore tests ──

    #[test]
    fn dotnet_restore_success() {
        let stdout = &pad("\
  Determining projects to restore...
  Restored /src/MyApp/MyApp.csproj (in 1.23 s).
  Restored /src/MyLib/MyLib.csproj (in 0.45 s).
  Restored /src/Tests/Tests.csproj (in 0.67 s).
");
        let input = make_bash_json("dotnet restore", stdout, "", 0);
        let result = compress(&input);
        assert!(result.was_compressed);
        assert!(result
            .compressed
            .contains("✅ dotnet restore: 3 projects restored"));
    }
}
