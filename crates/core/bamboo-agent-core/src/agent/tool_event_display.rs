//! Display-only projection for native tool events before they reach live clients.
//!
//! The execution channel and session history keep the authoritative events.
//! This projector belongs to one run and only trusts a `ToolStart` from the
//! current round to identify subsequent call-id-only events.

use std::collections::HashMap;

use bamboo_domain::{AgentHookPoint, HookResult};
use serde_json::{json, Value};

use super::events::AgentEvent;
use crate::tools::ToolResult;

#[derive(Debug, Clone)]
enum CallIdentity {
    Download,
    Other(String),
    UnknownBrowser,
}

/// Sanitizes the outward copy of native events, without changing execution.
#[derive(Debug, Default)]
pub struct NativeToolEventDisplay {
    calls: HashMap<String, CallIdentity>,
    round: Option<u32>,
}

fn canonical_tool_name(tool_name: &str) -> &str {
    tool_name.trim().rsplit("::").next().unwrap_or("")
}

fn is_browser(tool_name: &str) -> bool {
    canonical_tool_name(tool_name).eq_ignore_ascii_case("browser")
}

fn is_download(arguments: &Value) -> bool {
    arguments
        .get("action")
        .and_then(Value::as_str)
        .is_some_and(|action| action.eq_ignore_ascii_case("download"))
}

fn is_known_browser_action(arguments: &Value) -> bool {
    matches!(
        arguments.get("action").and_then(Value::as_str),
        Some(
            "tabs"
                | "new_tab"
                | "activate_tab"
                | "close_tab"
                | "navigate"
                | "history"
                | "viewport"
                | "snapshot"
                | "click"
                | "click_at"
                | "hover"
                | "drag"
                | "fill"
                | "select_option"
                | "set_file_input"
                | "type"
                | "press"
                | "key"
                | "scroll"
                | "screenshot"
                | "dialog_respond"
        )
    )
}

fn safe_download_arguments(arguments: &Value) -> Value {
    let mut display = json!({"action":"download"});
    if let Some(epoch) = arguments.get("expected_epoch").and_then(Value::as_u64) {
        display["expected_epoch"] = json!(epoch);
    }
    display
}

fn hidden_hook_result(result: HookResult, depth: usize) -> HookResult {
    if depth >= 8 {
        return HookResult::Deny {
            reason: "Hook result hidden".into(),
        };
    }
    match result {
        HookResult::Deny { .. } => HookResult::Deny {
            reason: "Hook reason hidden".into(),
        },
        HookResult::InjectContext { .. } => HookResult::InjectContext {
            text: "Hook context hidden".into(),
        },
        HookResult::WithContext { result, .. } => HookResult::WithContext {
            result: Box::new(hidden_hook_result(*result, depth + 1)),
            text: "Hook context hidden".into(),
        },
        HookResult::Suspend { .. } => HookResult::Suspend {
            reason: "Hook reason hidden".into(),
        },
        HookResult::Abort { .. } => HookResult::Abort {
            reason: "Hook reason hidden".into(),
        },
        other => other,
    }
}

impl NativeToolEventDisplay {
    /// Return a display copy of an event. A missing or stale call identity may
    /// be a dropped `ToolStart`, so call-id-only payloads fail closed.
    pub fn project(&mut self, event: AgentEvent) -> AgentEvent {
        match event {
            AgentEvent::RunnerProgress {
                session_id,
                round_count,
            } => {
                if self.round != Some(round_count) {
                    self.calls.clear();
                    self.round = Some(round_count);
                }
                AgentEvent::RunnerProgress {
                    session_id,
                    round_count,
                }
            }
            AgentEvent::ToolStart {
                tool_call_id,
                tool_name,
                arguments,
            } => {
                // A call ID is unique within one runner round. If another
                // start or a late terminal frame reuses it, its identity is
                // ambiguous until the next RunnerProgress boundary.
                if self.calls.contains_key(&tool_call_id) {
                    self.calls
                        .insert(tool_call_id.clone(), CallIdentity::UnknownBrowser);
                    return AgentEvent::ToolStart {
                        tool_call_id,
                        tool_name: "tool".to_string(),
                        arguments: json!({"arguments":"[redacted]"}),
                    };
                }
                // The namespace is model-supplied and can itself contain a
                // private selector or URL. Outward browser identity is fixed.
                let display_tool_name = if is_browser(&tool_name) {
                    "browser".to_string()
                } else {
                    tool_name.clone()
                };
                let identity = if is_browser(&tool_name) && is_download(&arguments) {
                    CallIdentity::Download
                } else if is_browser(&tool_name) && !is_known_browser_action(&arguments) {
                    CallIdentity::UnknownBrowser
                } else {
                    CallIdentity::Other(canonical_tool_name(&tool_name).to_ascii_lowercase())
                };
                let arguments = match &identity {
                    CallIdentity::Download => safe_download_arguments(&arguments),
                    CallIdentity::UnknownBrowser => {
                        json!({"action":"browser","arguments":"[redacted]"})
                    }
                    CallIdentity::Other(_) => arguments,
                };
                self.calls.insert(tool_call_id.clone(), identity);
                AgentEvent::ToolStart {
                    tool_call_id,
                    tool_name: display_tool_name,
                    arguments,
                }
            }
            AgentEvent::ToolToken {
                tool_call_id,
                content,
            } => {
                let content =
                    if matches!(self.calls.get(&tool_call_id), Some(CallIdentity::Other(_))) {
                        content
                    } else {
                        "Tool output hidden".to_string()
                    };
                AgentEvent::ToolToken {
                    tool_call_id,
                    content,
                }
            }
            AgentEvent::ToolComplete {
                tool_call_id,
                result,
            } => {
                let result = match self.calls.get(&tool_call_id) {
                    Some(CallIdentity::Other(_)) => result,
                    Some(CallIdentity::Download) => {
                        ToolResult::text(result.success, "Browser download result hidden")
                    }
                    _ => ToolResult::text(result.success, "Tool result hidden"),
                };
                self.calls
                    .insert(tool_call_id.clone(), CallIdentity::UnknownBrowser);
                AgentEvent::ToolComplete {
                    tool_call_id,
                    result,
                }
            }
            AgentEvent::ToolError {
                tool_call_id,
                error,
            } => {
                let error = match self.calls.get(&tool_call_id) {
                    Some(CallIdentity::Other(_)) => error,
                    Some(CallIdentity::Download) => "Browser download error hidden".to_string(),
                    _ => "Tool error hidden".to_string(),
                };
                self.calls
                    .insert(tool_call_id.clone(), CallIdentity::UnknownBrowser);
                AgentEvent::ToolError {
                    tool_call_id,
                    error,
                }
            }
            AgentEvent::ToolLifecycle {
                tool_call_id,
                tool_name,
                phase,
                elapsed_ms,
                is_mutating,
                auto_approved,
                summary,
                error,
            } => {
                let known_other = matches!(
                    self.calls.get(&tool_call_id),
                    Some(CallIdentity::Other(name))
                        if name.eq_ignore_ascii_case(canonical_tool_name(&tool_name))
                );
                let display_tool_name = if known_other {
                    tool_name.clone()
                } else if is_browser(&tool_name) {
                    "browser".to_string()
                } else {
                    "tool".to_string()
                };
                let phase = if known_other
                    || matches!(phase.as_str(), "begin" | "finished" | "error" | "cancelled")
                {
                    phase
                } else {
                    "activity".to_string()
                };
                // A lifecycle from another tool with a reused call ID must
                // not inherit an earlier ordinary tool's display authority.
                if matches!(self.calls.get(&tool_call_id), Some(CallIdentity::Other(_)))
                    && !known_other
                {
                    self.calls
                        .insert(tool_call_id.clone(), CallIdentity::UnknownBrowser);
                }
                AgentEvent::ToolLifecycle {
                    tool_call_id,
                    tool_name: display_tool_name,
                    phase,
                    elapsed_ms,
                    is_mutating,
                    auto_approved,
                    summary: summary.map(|text| {
                        if known_other {
                            text
                        } else {
                            "Tool activity hidden".to_string()
                        }
                    }),
                    error: error.map(|text| {
                        if known_other {
                            text
                        } else {
                            "Tool error hidden".to_string()
                        }
                    }),
                }
            }
            AgentEvent::HookLifecycle {
                hook_name,
                point,
                phase,
                duration_ms,
                decision,
            } => {
                if !matches!(
                    point,
                    AgentHookPoint::BeforeToolExecution | AgentHookPoint::AfterToolExecution
                ) {
                    return AgentEvent::HookLifecycle {
                        hook_name,
                        point,
                        phase,
                        duration_ms,
                        decision,
                    };
                }
                // Tool hooks carry no call ID. A dropped browser ToolStart can
                // coexist with an unrelated Read, so active calls cannot
                // authorize any free text in this outward event.
                AgentEvent::HookLifecycle {
                    hook_name: "Tool hook hidden".into(),
                    point,
                    phase: if phase == "completed" {
                        phase
                    } else {
                        "hidden".into()
                    },
                    duration_ms,
                    decision: hidden_hook_result(decision, 0),
                }
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::ToolResultImage;

    fn start(id: &str, name: &str, arguments: Value) -> AgentEvent {
        AgentEvent::ToolStart {
            tool_call_id: id.into(),
            tool_name: name.into(),
            arguments,
        }
    }

    fn complete(id: &str, private: &str) -> AgentEvent {
        AgentEvent::ToolComplete {
            tool_call_id: id.into(),
            result: ToolResult {
                success: true,
                result: private.into(),
                display_preference: Some(private.into()),
                images: vec![ToolResultImage {
                    mime_type: "image/png".into(),
                    data: private.into(),
                }],
            },
        }
    }

    #[test]
    fn download_projection_hides_every_result_field_and_lifecycle_text() {
        let secret = "private-selector-url-filename-and-bytes";
        let mut display = NativeToolEventDisplay::default();
        let authoritative = complete("call-1", secret);
        let projected = display.project(start(
            "call-1",
            &format!("{secret}::browser"),
            json!({"action":"download","selector":secret,"expected_epoch":17,"extra":{"url":secret}}),
        ));
        assert!(!serde_json::to_string(&projected).unwrap().contains(secret));
        assert!(
            matches!(projected, AgentEvent::ToolStart { tool_name, arguments, .. }
            if tool_name == "browser" && arguments == json!({"action":"download","expected_epoch":17}))
        );
        let lifecycle = display.project(AgentEvent::ToolLifecycle {
            tool_call_id: "call-1".into(),
            tool_name: format!("{secret}::browser"),
            phase: "error".into(),
            elapsed_ms: Some(2),
            is_mutating: false,
            auto_approved: false,
            summary: Some(secret.into()),
            error: Some(secret.into()),
        });
        let lifecycle_json = serde_json::to_string(&lifecycle).unwrap();
        assert!(!lifecycle_json.contains(secret));
        let token = display.project(AgentEvent::ToolToken {
            tool_call_id: "call-1".into(),
            content: secret.into(),
        });
        assert!(!serde_json::to_string(&token).unwrap().contains(secret));
        let result = display.project(authoritative.clone());
        let AgentEvent::ToolComplete { result, .. } = result else {
            panic!("expected completion");
        };
        assert!(result.success);
        assert_eq!(result.result, "Browser download result hidden");
        assert!(result.images.is_empty());
        assert_eq!(result.display_preference, None);
        assert!(serde_json::to_string(&authoritative)
            .unwrap()
            .contains(secret));
    }

    #[test]
    fn missing_start_and_reused_id_fail_closed_without_hiding_ordinary_tools() {
        let secret = "private-download-payload";
        let mut display = NativeToolEventDisplay::default();
        let unknown = display.project(complete("missing", secret));
        assert!(!serde_json::to_string(&unknown).unwrap().contains(secret));
        let orphan_lifecycle = display.project(AgentEvent::ToolLifecycle {
            tool_call_id: "missing".into(),
            tool_name: format!("{secret}::browser"),
            phase: "error".into(),
            elapsed_ms: None,
            is_mutating: false,
            auto_approved: false,
            summary: Some(secret.into()),
            error: Some(secret.into()),
        });
        assert!(!serde_json::to_string(&orphan_lifecycle)
            .unwrap()
            .contains(secret));

        display.project(start("shared", "Read", json!({"path":"readme.md"})));
        assert!(
            matches!(display.project(complete("shared", "ordinary Read result")),
            AgentEvent::ToolComplete { result, .. } if result.result == "ordinary Read result")
        );
        let late = display.project(complete("shared", secret));
        assert!(!serde_json::to_string(&late).unwrap().contains(secret));

        display.project(start("same-round", "Read", json!({"path":"readme.md"})));
        display.project(AgentEvent::ToolLifecycle {
            tool_call_id: "same-round".into(),
            tool_name: "browser".into(),
            phase: "begin".into(),
            elapsed_ms: None,
            is_mutating: false,
            auto_approved: false,
            summary: None,
            error: None,
        });
        let mismatched = display.project(complete("same-round", secret));
        assert!(!serde_json::to_string(&mismatched).unwrap().contains(secret));

        display.project(start("across-rounds", "Read", json!({"path":"readme.md"})));
        display.project(AgentEvent::RunnerProgress {
            session_id: "chat".into(),
            round_count: 2,
        });
        let reused = display.project(complete("across-rounds", secret));
        assert!(!serde_json::to_string(&reused).unwrap().contains(secret));

        display.project(start(
            "duplicate",
            "browser",
            json!({"action":"download","selector":secret}),
        ));
        let second_start = display.project(start("duplicate", "Read", json!({"path":secret})));
        assert!(!serde_json::to_string(&second_start)
            .unwrap()
            .contains(secret));
        let token = display.project(AgentEvent::ToolToken {
            tool_call_id: "duplicate".into(),
            content: secret.into(),
        });
        assert!(!serde_json::to_string(&token).unwrap().contains(secret));
        let completed = display.project(complete("duplicate", secret));
        assert!(!serde_json::to_string(&completed).unwrap().contains(secret));
        let after_terminal = display.project(start("duplicate", "Read", json!({"path":secret})));
        assert!(!serde_json::to_string(&after_terminal)
            .unwrap()
            .contains(secret));
        display.project(AgentEvent::RunnerProgress {
            session_id: "chat".into(),
            round_count: 3,
        });
        display.project(start("duplicate", "Read", json!({"path":"readme.md"})));
        assert!(
            matches!(display.project(complete("duplicate", "ordinary after round")), AgentEvent::ToolComplete { result, .. } if result.result == "ordinary after round")
        );
    }

    #[test]
    fn unknown_lifecycle_hides_private_name_and_phase() {
        let secret = "private-download-namespace-and-phase";
        let mut display = NativeToolEventDisplay::default();
        let event = display.project(AgentEvent::ToolLifecycle {
            tool_call_id: "missing".into(),
            tool_name: format!("{secret}::Read"),
            phase: secret.into(),
            elapsed_ms: None,
            is_mutating: false,
            auto_approved: false,
            summary: Some(secret.into()),
            error: Some(secret.into()),
        });
        assert!(!serde_json::to_string(&event).unwrap().contains(secret));
        assert!(
            matches!(event, AgentEvent::ToolLifecycle { tool_name, phase, .. } if tool_name == "tool" && phase == "activity")
        );

        display.project(start("download", "browser", json!({"action":"download"})));
        let event = display.project(AgentEvent::ToolLifecycle {
            tool_call_id: "download".into(),
            tool_name: "browser".into(),
            phase: secret.into(),
            elapsed_ms: None,
            is_mutating: false,
            auto_approved: false,
            summary: Some(secret.into()),
            error: Some(secret.into()),
        });
        assert!(!serde_json::to_string(&event).unwrap().contains(secret));
    }

    #[test]
    fn malformed_browser_start_hides_arguments_and_error_but_snapshot_stays_visible() {
        let secret = "private-selector-and-url";
        let mut display = NativeToolEventDisplay::default();
        let malformed = display.project(start(
            "bad",
            "browser",
            json!({"selector":secret,"url":secret}),
        ));
        assert!(!serde_json::to_string(&malformed).unwrap().contains(secret));
        let error = display.project(AgentEvent::ToolError {
            tool_call_id: "bad".into(),
            error: secret.into(),
        });
        assert!(!serde_json::to_string(&error).unwrap().contains(secret));

        display.project(start("snapshot", "browser", json!({"action":"snapshot"})));
        assert!(
            matches!(display.project(complete("snapshot", "page_epoch: 17")),
            AgentEvent::ToolComplete { result, .. } if result.result == "page_epoch: 17")
        );
    }

    fn hook(point: AgentHookPoint, secret: &str, decision: HookResult) -> AgentEvent {
        AgentEvent::HookLifecycle {
            hook_name: format!("hook-{secret}"),
            point,
            phase: format!("phase-{secret}"),
            duration_ms: 7,
            decision,
        }
    }

    #[test]
    fn native_download_hook_outcomes_hide_all_free_text_before_publication() {
        let secret = "private-download-selector-url-and-bytes";
        let decisions = [
            HookResult::Deny {
                reason: secret.into(),
            },
            HookResult::InjectContext {
                text: secret.into(),
            },
            HookResult::WithContext {
                result: Box::new(HookResult::WithContext {
                    result: Box::new(HookResult::Suspend {
                        reason: secret.into(),
                    }),
                    text: secret.into(),
                }),
                text: secret.into(),
            },
            HookResult::Abort {
                reason: secret.into(),
            },
        ];
        let mut display = NativeToolEventDisplay::default();
        display.project(start(
            "download",
            &format!("{secret}::browser"),
            json!({"action":"download","selector":secret,"expected_epoch":17}),
        ));
        for point in [
            AgentHookPoint::BeforeToolExecution,
            AgentHookPoint::AfterToolExecution,
        ] {
            for decision in &decisions {
                let original = hook(point, secret, decision.clone());
                let projected = display.project(original.clone());
                let wire = serde_json::to_string(&projected).unwrap();
                assert!(
                    !wire.contains(secret),
                    "private hook text reached the display event"
                );
                assert!(wire.contains("Tool hook hidden"));
                assert!(serde_json::to_string(&original).unwrap().contains(secret));
            }
        }
        let nested = display.project(hook(
            AgentHookPoint::AfterToolExecution,
            secret,
            HookResult::WithContext {
                result: Box::new(HookResult::Deny {
                    reason: secret.into(),
                }),
                text: secret.into(),
            },
        ));
        assert!(matches!(nested,
            AgentEvent::HookLifecycle {
                decision: HookResult::WithContext { result, .. }, ..
            } if matches!(*result, HookResult::Deny { .. })));
    }

    #[test]
    fn unkeyed_tool_hooks_hide_text_even_with_only_read_visible() {
        let secret = "private-hook-reason";
        let mut display = NativeToolEventDisplay::default();
        let make_hook = || {
            hook(
                AgentHookPoint::BeforeToolExecution,
                secret,
                HookResult::Deny {
                    reason: secret.into(),
                },
            )
        };
        let missing = display.project(make_hook());
        assert!(!serde_json::to_string(&missing).unwrap().contains(secret));

        display.project(start("read", "Read", json!({"path":"readme.md"})));
        // The browser ToolStart could have been dropped while Read remains
        // visible. HookLifecycle has no call ID to disprove that case.
        let dropped_browser_start = display.project(make_hook());
        assert!(!serde_json::to_string(&dropped_browser_start)
            .unwrap()
            .contains(secret));
        let ordinary_result = display.project(complete("read", "ordinary Read result"));
        assert!(matches!(ordinary_result,
            AgentEvent::ToolComplete { result, .. } if result.result == "ordinary Read result"));
        display.project(start("download", "browser", json!({"action":"download"})));
        let overlap = display.project(make_hook());
        assert!(!serde_json::to_string(&overlap).unwrap().contains(secret));

        display.project(AgentEvent::RunnerProgress {
            session_id: "chat".into(),
            round_count: 2,
        });
        display.project(start("reused", "Read", json!({"path":"readme.md"})));
        display.project(complete("reused", "ordinary"));
        display.project(start("reused", "Read", json!({"path":"readme.md"})));
        let reused = display.project(make_hook());
        assert!(!serde_json::to_string(&reused).unwrap().contains(secret));

        display.project(AgentEvent::RunnerProgress {
            session_id: "chat".into(),
            round_count: 3,
        });
        display.project(start("reused", "Read", json!({"path":"readme.md"})));
        let next_round = display.project(make_hook());
        assert!(!serde_json::to_string(&next_round).unwrap().contains(secret));

        let unrelated = display.project(hook(
            AgentHookPoint::AfterRound,
            secret,
            HookResult::Deny {
                reason: secret.into(),
            },
        ));
        assert!(serde_json::to_string(&unrelated).unwrap().contains(secret));
    }
}
