//! Root-agent browser tool. The target chat session comes only from ToolCtx.

use std::sync::Arc;

use async_trait::async_trait;
use bamboo_agent_core::tools::{
    Tool, ToolClass, ToolCtx, ToolError, ToolOutcome, ToolResult, ToolResultImage,
};
use serde_json::{json, Value};

use crate::browser::{BrowserError, BrowserManager};

pub struct BrowserTool {
    browser: Arc<BrowserManager>,
}

impl BrowserTool {
    pub fn new(browser: Arc<BrowserManager>) -> Self {
        Self { browser }
    }
}

fn text_arg<'a>(args: &'a Value, name: &str) -> Result<&'a str, ToolError> {
    args.get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ToolError::InvalidArguments(format!("browser requires nonempty {name}")))
}

fn tab_id_arg(args: &Value) -> Result<&str, ToolError> {
    let tab_id = text_arg(args, "tab_id")?;
    if tab_id.len() != 24
        || !tab_id
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(ToolError::InvalidArguments(
            "browser tab_id must be a 24-character lowercase hex ID".into(),
        ));
    }
    Ok(tab_id)
}

fn dialog_id_arg(args: &Value) -> Result<&str, ToolError> {
    let dialog_id = text_arg(args, "dialog_id")?;
    if dialog_id.len() != 24
        || !dialog_id
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(ToolError::InvalidArguments(
            "browser dialog_id must be a 24-character lowercase hex ID".into(),
        ));
    }
    Ok(dialog_id)
}

fn dialog_response_args(args: &Value, epoch: u64) -> Result<Value, ToolError> {
    let accept = args
        .get("accept")
        .and_then(Value::as_bool)
        .ok_or_else(|| ToolError::InvalidArguments("browser dialog requires accept".into()))?;
    let text = match args.get("text") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_str()
                .filter(|text| text.encode_utf16().count() <= 4096)
                .ok_or_else(|| ToolError::InvalidArguments("invalid browser dialog text".into()))?,
        ),
    };
    if !accept && text.is_some() {
        return Err(ToolError::InvalidArguments(
            "dismissed browser dialog cannot include text".into(),
        ));
    }
    Ok(json!({
        "dialog_id":dialog_id_arg(args)?,"accept":accept,"text":text,"expected_epoch":epoch
    }))
}

fn number_arg(args: &Value, name: &str) -> Result<f64, ToolError> {
    args.get(name)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value >= 0.0)
        .ok_or_else(|| ToolError::InvalidArguments(format!("browser requires nonnegative {name}")))
}

fn pointer_coordinate(args: &Value, name: &str, maximum: f64) -> Result<f64, ToolError> {
    number_arg(args, name).and_then(|value| {
        (value < maximum).then_some(value).ok_or_else(|| {
            ToolError::InvalidArguments(format!("browser {name} must be within the viewport"))
        })
    })
}

fn pointer_selector<'a>(args: &'a Value, name: &str) -> Result<&'a str, ToolError> {
    let selector = text_arg(args, name)?;
    if selector.trim().is_empty() || selector.encode_utf16().count() > 512 {
        return Err(ToolError::InvalidArguments(format!(
            "browser {name} must be 1..512 UTF-16 code units"
        )));
    }
    Ok(selector)
}

fn pointer_button(args: &Value) -> Result<&str, ToolError> {
    let button = match args.get("button") {
        None => "left",
        Some(value) => value.as_str().ok_or_else(|| {
            ToolError::InvalidArguments("browser button must be left, right, or middle".into())
        })?,
    };
    if !matches!(button, "left" | "right" | "middle") {
        return Err(ToolError::InvalidArguments(
            "browser button must be left, right, or middle".into(),
        ));
    }
    Ok(button)
}

fn pointer_request(
    action: &str,
    args: &Value,
    epoch: u64,
) -> Result<(&'static str, Value), ToolError> {
    let invalid =
        || ToolError::InvalidArguments("browser pointer target is ambiguous or incomplete".into());
    match action {
        "hover" => {
            if args.get("target").is_some_and(|value| !value.is_null())
                || args.get("button").is_some()
                || args.get("source_selector").is_some()
                || args.get("target_selector").is_some()
                || args.get("to_x").is_some()
                || args.get("to_y").is_some()
            {
                return Err(invalid());
            }
            if args.get("selector").is_some_and(|value| !value.is_null()) {
                if args.get("x").is_some() || args.get("y").is_some() {
                    return Err(invalid());
                }
                Ok((
                    "hover_selector",
                    json!({"selector":pointer_selector(args,"selector")?,"expected_epoch":epoch}),
                ))
            } else {
                Ok((
                    "hover_at",
                    json!({
                        "x":pointer_coordinate(args,"x",1200.0)?,
                        "y":pointer_coordinate(args,"y",1000.0)?,
                        "expected_epoch":epoch,
                    }),
                ))
            }
        }
        "drag" => {
            if args.get("target").is_some_and(|value| !value.is_null())
                || args.get("selector").is_some()
            {
                return Err(invalid());
            }
            let button = pointer_button(args)?;
            if args.get("source_selector").is_some() || args.get("target_selector").is_some() {
                if args.get("x").is_some()
                    || args.get("y").is_some()
                    || args.get("to_x").is_some()
                    || args.get("to_y").is_some()
                    || button != "left"
                {
                    return Err(invalid());
                }
                Ok((
                    "drag_selector",
                    json!({
                        "source_selector":pointer_selector(args,"source_selector")?,
                        "target_selector":pointer_selector(args,"target_selector")?,
                        "expected_epoch":epoch,
                    }),
                ))
            } else {
                Ok((
                    "drag_at",
                    json!({
                        "x":pointer_coordinate(args,"x",1200.0)?,
                        "y":pointer_coordinate(args,"y",1000.0)?,
                        "to_x":pointer_coordinate(args,"to_x",1200.0)?,
                        "to_y":pointer_coordinate(args,"to_y",1000.0)?,
                        "button":button,
                        "expected_epoch":epoch,
                    }),
                ))
            }
        }
        _ => Err(ToolError::InvalidArguments(
            "unknown browser pointer action".into(),
        )),
    }
}

fn viewport_arg(args: &Value, name: &str, min: u64, max: u64) -> Result<u64, ToolError> {
    args.get(name)
        .and_then(Value::as_u64)
        .filter(|value| (*value >= min) && (*value <= max))
        .ok_or_else(|| {
            ToolError::InvalidArguments(format!("browser {name} must be within {min}..{max}"))
        })
}

fn semantic_target(target: &Value) -> Result<(), ToolError> {
    let invalid = || ToolError::InvalidArguments("invalid browser semantic target".into());
    let object = target.as_object().ok_or_else(invalid)?;
    let kind = object
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(invalid)?;
    let string = |name: &str, maximum: usize| -> Result<Option<&str>, ToolError> {
        match object.get(name) {
            None => Ok(None),
            Some(value) => value
                .as_str()
                .filter(|value| !value.trim().is_empty() && value.encode_utf16().count() <= maximum)
                .map(Some)
                .ok_or_else(invalid),
        }
    };
    let _ = string("frame_selector", 512)?;
    if object.get("exact").is_some_and(|value| !value.is_boolean()) {
        return Err(invalid());
    }
    let allowed: &[&str] = match kind {
        "role" => {
            let role = string("role", 64)?.ok_or_else(invalid)?;
            if !role
                .chars()
                .all(|character| character.is_ascii_lowercase() || character == '-')
            {
                return Err(invalid());
            }
            let _ = string("name", 256)?;
            &["kind", "role", "name", "exact", "frame_selector"]
        }
        "label" | "text" => {
            string("value", 256)?.ok_or_else(invalid)?;
            &["kind", "value", "exact", "frame_selector"]
        }
        _ => return Err(invalid()),
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(invalid());
    }
    Ok(())
}

fn locator_request(args: &Value, epoch: u64, allow_focused: bool) -> Result<Value, ToolError> {
    let selector = args.get("selector").filter(|value| !value.is_null());
    let target = args.get("target").filter(|value| !value.is_null());
    match (selector, target) {
        (Some(_), Some(_)) => Err(ToolError::InvalidArguments(
            "browser selector and target are mutually exclusive".into(),
        )),
        (Some(_), None) => Ok(json!({
            "selector":text_arg(args, "selector")?,
            "expected_epoch":epoch,
        })),
        (None, Some(target)) => {
            semantic_target(target)?;
            Ok(json!({"target":target,"expected_epoch":epoch}))
        }
        (None, None) if allow_focused => Ok(json!({"expected_epoch":epoch})),
        (None, None) => Err(ToolError::InvalidArguments(
            "browser requires selector or target".into(),
        )),
    }
}

fn select_option_request(args: &Value, epoch: u64) -> Result<Value, ToolError> {
    if args.get("target").is_some_and(|value| !value.is_null()) {
        return Err(ToolError::InvalidArguments(
            "browser select_option accepts only a CSS selector".into(),
        ));
    }
    let selector = text_arg(args, "selector")?;
    if selector.trim().is_empty() || selector.encode_utf16().count() > 512 {
        return Err(ToolError::InvalidArguments(
            "browser select requires a bounded CSS selector".into(),
        ));
    }
    let values = args
        .get("values")
        .and_then(Value::as_array)
        .filter(|values| (1..=16).contains(&values.len()))
        .ok_or_else(|| {
            ToolError::InvalidArguments("browser select requires 1..16 option values".into())
        })?;
    if values
        .iter()
        .any(|value| value.as_str().is_none_or(|value| value.len() > 512))
    {
        return Err(ToolError::InvalidArguments(
            "browser select option values must be strings of at most 512 bytes".into(),
        ));
    }
    Ok(json!({"selector":selector,"values":values,"expected_epoch":epoch}))
}

fn download_request(args: &Value, epoch: u64) -> Result<Value, ToolError> {
    let fields = args.as_object().ok_or_else(|| {
        ToolError::InvalidArguments(
            "browser download accepts only a CSS selector and expected_epoch".into(),
        )
    })?;
    if fields
        .keys()
        .any(|key| !matches!(key.as_str(), "action" | "selector" | "expected_epoch"))
    {
        return Err(ToolError::InvalidArguments(
            "browser download accepts only a CSS selector and expected_epoch".into(),
        ));
    }
    Ok(json!({"selector":pointer_selector(args,"selector")?,"expected_epoch":epoch}))
}

fn input_request(action: &str, args: &Value, epoch: u64) -> Result<Value, ToolError> {
    match action {
        "click_at" => {
            let x = number_arg(args, "x")?;
            let y = number_arg(args, "y")?;
            let button = match args.get("button") {
                None => "left",
                Some(value) => value.as_str().ok_or_else(|| {
                    ToolError::InvalidArguments(
                        "browser button must be left, right, or middle".into(),
                    )
                })?,
            };
            if !matches!(button, "left" | "right" | "middle") {
                return Err(ToolError::InvalidArguments(
                    "browser button must be left, right, or middle".into(),
                ));
            }
            Ok(json!({"kind":"click","x":x,"y":y,"button":button,"expected_epoch":epoch}))
        }
        "type" => Ok(json!({
            "kind":"type",
            "text":args.get("text").and_then(Value::as_str).ok_or_else(|| ToolError::InvalidArguments("browser requires text for type".into()))?,
            "expected_epoch":epoch,
        })),
        "key" => Ok(json!({"kind":"key","key":text_arg(args,"key")?,"expected_epoch":epoch})),
        _ => Err(ToolError::InvalidArguments(
            "unknown browser input action".into(),
        )),
    }
}

fn browser_error(error: BrowserError) -> ToolError {
    ToolError::Execution(error.to_string())
}

#[async_trait]
impl Tool for BrowserTool {
    fn name(&self) -> &str {
        "browser"
    }

    fn description(&self) -> &str {
        "Operate the browser context shared with this chat's right workbench. List, create, activate or close tabs; read the active tab's DOM snapshot or screenshot; navigate, use history, resize the viewport, click, hover, drag, fill or press a target, select native HTML options, set one in-memory file input, type into the focused element, scroll, or download one file through a CSS selector. set_file_input requires a CSS selector, basename filename, MIME type and strict base64 bytes of at most 1 MiB; it never reads a local path. Download resolves an actionable main-frame CSS <a href> in the shared page, then follows its direct HTTP(S) link from a script-free private page in the same BrowserContext; it does not run the source page's onclick/JavaScript or change shared tabs. It returns at most 256 KiB as Base64 with filename, byte_count and SHA-256. Script-triggered or Blob downloads, HTTP redirects, sites requiring Referer, enforcing CSP sandbox, and pages with unverified response provenance (including some popups) return download_unverifiable. It cannot fetch an arbitrary URL or save to a chosen path. Hover accepts a CSS selector or viewport x/y; drag accepts source_selector/target_selector or x/y/to_x/to_y. A page handler may navigate during hover or drag and advance page_epoch; use the returned state before the next action. A page JavaScript dialog appears as pending_dialog in the action result or tabs state; answer its exact dialog_id and page_epoch with dialog_respond before another mutation. Snapshot [ref=e...] markers are not stable locators; use a target or CSS selector. The tabs belong to the current chat session; no session ID argument is accepted. Take a snapshot and pass its page_epoch before interacting with a previously seen view."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type":"object",
            "properties": {
                "action":{"type":"string","enum":["tabs","new_tab","activate_tab","close_tab","navigate","history","viewport","snapshot","click","click_at","hover","drag","fill","select_option","set_file_input","type","press","key","scroll","download","screenshot","dialog_respond"]},
                "tab_id":{"type":"string","description":"Opaque tab ID from tabs/state; required for activate_tab and close_tab"},
                "dialog_id":{"type":"string","description":"Opaque pending_dialog ID from this chat; required for dialog_respond"},
                "accept":{"type":"boolean","description":"Accept or dismiss the exact pending JavaScript dialog"},
                "text":{"type":"string","description":"Text for fill or type, or for an accepted prompt dialog. Omit prompt text to use pending_dialog.default_value; prompt text is private"},
                "url":{"type":"string","description":"HTTP(S) URL for navigate"},
                "direction":{"type":"string","enum":["back","forward","reload"],"description":"Direction for history"},
                "width":{"type":"integer","minimum":320,"maximum":1200,"description":"CSS viewport width for viewport"},
                "height":{"type":"integer","minimum":240,"maximum":1000,"description":"CSS viewport height for viewport"},
                "selector":{"type":"string","description":"CSS selector for click, fill, hover, select_option, set_file_input, a direct HTTP(S) anchor download, or optional press; mutually exclusive with target or hover coordinates","maxLength":512},
                "source_selector":{"type":"string","description":"CSS source selector for drag; pair with target_selector"},
                "target_selector":{"type":"string","description":"CSS destination selector for drag; pair with source_selector"},
                "target":{"type":"object","description":"Semantic target for click, fill, or press; mutually exclusive with selector. Use kind=role with role and optional name, or kind=label/text with value. Optional frame_selector is a CSS selector for one iframe. Exact matching defaults to true.","properties":{"kind":{"type":"string","enum":["role","label","text"]},"role":{"type":"string"},"name":{"type":"string"},"value":{"type":"string"},"exact":{"type":"boolean"},"frame_selector":{"type":"string"}},"required":["kind"],"additionalProperties":false},
                "values":{"type":"array","description":"Native select option values for select_option, including the empty value","minItems":1,"maxItems":16,"items":{"type":"string","maxLength":512}},
                "filename":{"type":"string","description":"Basename only for set_file_input; no directory or path","maxLength":128},
                "mime_type":{"type":"string","description":"Bounded MIME type for set_file_input, such as text/plain","maxLength":128},
                "data_base64":{"type":"string","description":"Strict standard base64 contents of one in-memory file, decoded size at most 1 MiB","maxLength":1398104},
                "key":{"type":"string","description":"Keyboard key for press or key, e.g. Enter"},
                "x":{"type":"number","description":"CSS viewport x for click_at, scroll, coordinate hover, or coordinate drag"},
                "y":{"type":"number","description":"CSS viewport y for click_at, scroll, coordinate hover, or coordinate drag"},
                "to_x":{"type":"number","description":"CSS viewport destination x for coordinate drag"},
                "to_y":{"type":"number","description":"CSS viewport destination y for coordinate drag"},
                "button":{"type":"string","enum":["left","right","middle"],"description":"Mouse button for click_at or coordinate drag; defaults to left"},
                "delta_x":{"type":"number"},
                "delta_y":{"type":"number"},
                "expected_epoch":{"type":"integer","description":"Required for new_tab/activate_tab/close_tab/history/viewport/click/click_at/hover/drag/fill/select_option/set_file_input/type/press/key/scroll/download/dialog_respond: page_epoch from a prior snapshot or action result; rejects stale actions"},
                "include_html":{"type":"boolean","description":"Include bounded raw HTML in snapshot output"}
            },
            "required":["action"],
            "additionalProperties":false
        })
    }

    fn classify(&self, args: &Value) -> ToolClass {
        if matches!(
            args.get("action").and_then(Value::as_str),
            Some("tabs" | "snapshot" | "screenshot")
        ) {
            ToolClass::READONLY_PARALLEL
        } else {
            ToolClass::MUTATING_SERIAL
        }
    }

    async fn invoke(&self, args: Value, ctx: ToolCtx) -> Result<ToolOutcome, ToolError> {
        if args.get("session_id").is_some() {
            return Err(ToolError::InvalidArguments(
                "browser session is bound to the current chat".into(),
            ));
        }
        let session_id = ctx
            .session_id()
            .ok_or_else(|| ToolError::Execution("browser requires a chat session".into()))?;
        let action = text_arg(&args, "action")?;
        if action != "set_file_input" && args.get("data_base64").is_some() {
            return Err(ToolError::InvalidArguments(
                "browser file bytes require set_file_input".into(),
            ));
        }
        if matches!(
            action,
            "new_tab"
                | "activate_tab"
                | "close_tab"
                | "history"
                | "viewport"
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
                | "dialog_respond"
                | "download"
        ) && args.get("expected_epoch").and_then(Value::as_u64).is_none()
        {
            return Err(ToolError::InvalidArguments(
                "browser interaction requires expected_epoch from a snapshot".into(),
            ));
        }
        if matches!(action, "activate_tab" | "close_tab") {
            tab_id_arg(&args)?;
        }
        if action == "dialog_respond" {
            dialog_id_arg(&args)?;
        }
        if action == "set_file_input" {
            // The model's bytes and path-shaped extras must be rejected before
            // an invalid request can start a Chromium session.
            bamboo_tools::permission::validate_browser_file_input(&args).map_err(|_| {
                ToolError::InvalidArguments("invalid browser in-memory file input".into())
            })?;
        }
        let state = self.browser.open(session_id).await.map_err(browser_error)?;
        let epoch = args
            .get("expected_epoch")
            .and_then(Value::as_u64)
            .or_else(|| state.get("page_epoch").and_then(Value::as_u64))
            .ok_or_else(|| ToolError::Execution("browser page epoch missing".into()))?;
        let result = match action {
            "tabs" => state,
            "new_tab" => self.browser.command(session_id, "tab_create", json!({"expected_epoch":epoch})).await.map_err(browser_error)?,
            "activate_tab" => self.browser.command(session_id, "tab_activate", json!({"tab_id":tab_id_arg(&args)?,"expected_epoch":epoch})).await.map_err(browser_error)?,
            "close_tab" => self.browser.command(session_id, "tab_close", json!({"tab_id":tab_id_arg(&args)?,"expected_epoch":epoch})).await.map_err(browser_error)?,
            "navigate" => {
                let url = text_arg(&args, "url")?;
                let parsed = url::Url::parse(url).map_err(|error| ToolError::InvalidArguments(error.to_string()))?;
                if !matches!(parsed.scheme(), "http" | "https") || !parsed.username().is_empty() || parsed.password().is_some() {
                    return Err(ToolError::InvalidArguments("browser requires an http(s) URL without credentials".into()));
                }
                self.browser.command(session_id, "navigate", json!({"url":parsed.as_str(),"expected_epoch":epoch})).await.map_err(browser_error)?
            }
            "history" => {
                let direction = text_arg(&args, "direction")?;
                if !matches!(direction, "back" | "forward" | "reload") {
                    return Err(ToolError::InvalidArguments("browser history direction must be back, forward, or reload".into()));
                }
                self.browser.command(session_id, "history", json!({"direction":direction,"expected_epoch":epoch})).await.map_err(browser_error)?
            }
            "viewport" => self.browser.command(session_id, "viewport", json!({
                "width":viewport_arg(&args,"width",320,1200)?,
                "height":viewport_arg(&args,"height",240,1000)?,
                "expected_epoch":epoch,
            })).await.map_err(browser_error)?,
            "click" => self.browser.command(session_id, "click_selector", locator_request(&args, epoch, false)?).await.map_err(browser_error)?,
            "click_at" | "type" | "key" => self.browser.command(session_id, "input", input_request(action, &args, epoch)?).await.map_err(browser_error)?,
            "hover" | "drag" => {
                let (command, request) = pointer_request(action, &args, epoch)?;
                self.browser.command(session_id, command, request).await.map_err(browser_error)?
            },
            "fill" => {
                let mut request = locator_request(&args, epoch, false)?;
                request["text"] = json!(args.get("text").and_then(Value::as_str).ok_or_else(|| ToolError::InvalidArguments("browser requires text for fill".into()))?);
                self.browser.command(session_id, "fill_selector", request).await.map_err(browser_error)?
            },
            "select_option" => self.browser.command(session_id, "select_option", select_option_request(&args, epoch)?).await.map_err(browser_error)?,
            "download" => self.browser.command(session_id, "download", download_request(&args, epoch)?).await.map_err(browser_error)?,
            "set_file_input" => self.browser.command(session_id, "set_file_input", json!({
                "selector":args["selector"],
                "filename":args["filename"],
                "mime_type":args["mime_type"],
                "data_base64":args["data_base64"],
                "expected_epoch":epoch,
            })).await.map_err(browser_error)?,
            "press" => {
                let mut request = locator_request(&args, epoch, true)?;
                request["key"] = json!(text_arg(&args,"key")?);
                self.browser.command(session_id, "press_selector", request).await.map_err(browser_error)?
            },
            "scroll" => self.browser.command(session_id, "input", json!({"kind":"scroll","x":args.get("x").and_then(Value::as_f64).unwrap_or(500.0),"y":args.get("y").and_then(Value::as_f64).unwrap_or(360.0),"delta_x":args.get("delta_x").and_then(Value::as_f64).unwrap_or(0.0),"delta_y":args.get("delta_y").and_then(Value::as_f64).unwrap_or(500.0),"expected_epoch":epoch})).await.map_err(browser_error)?,
            "dialog_respond" => self.browser.command(session_id, "dialog_respond", dialog_response_args(&args, epoch)?).await.map_err(browser_error)?,
            "snapshot" => {
                let dom = self.browser.command(session_id, "dom", json!({})).await.map_err(browser_error)?;
                let mut text = format!("page_epoch: {}\nactive_tab_id: {}\nurl: {}\ntitle: {}\n\n{}", dom["page_epoch"], dom["active_tab_id"].as_str().unwrap_or(""), dom["url"].as_str().unwrap_or(""), dom["title"].as_str().unwrap_or(""), dom["snapshot"].as_str().unwrap_or(""));
                if args.get("include_html").and_then(Value::as_bool) == Some(true) {
                    text.push_str("\n\nHTML:\n");
                    text.push_str(dom["html"].as_str().unwrap_or(""));
                }
                return Ok(ToolOutcome::Completed(ToolResult::text(true, text)));
            }
            "screenshot" => {
                let image = self.browser.command(session_id, "screenshot", json!({})).await.map_err(browser_error)?;
                let data = image.get("data").and_then(Value::as_str).ok_or_else(|| ToolError::Execution("browser screenshot missing data".into()))?;
                return Ok(ToolOutcome::Completed(ToolResult {
                    success: true,
                    result: format!("Screenshot of {} (tab {}, page_epoch {})", image["url"].as_str().unwrap_or(""), image["active_tab_id"].as_str().unwrap_or(""), image["page_epoch"]),
                    display_preference: None,
                    images: vec![ToolResultImage { mime_type: "image/jpeg".into(), data: data.into() }],
                }));
            }
            _ => return Err(ToolError::InvalidArguments("unknown browser action".into())),
        };
        Ok(ToolOutcome::Completed(ToolResult::text(
            true,
            result.to_string(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_tool_advertises_existing_host_controls_as_mutating_actions() {
        let tool = BrowserTool::new(Arc::new(BrowserManager::default()));
        let schema = tool.parameters_schema();
        let actions = schema["properties"]["action"]["enum"].as_array().unwrap();
        for action in [
            "history",
            "viewport",
            "click_at",
            "hover",
            "drag",
            "type",
            "key",
            "dialog_respond",
            "set_file_input",
        ] {
            assert!(actions.contains(&json!(action)), "missing {action}");
            assert_eq!(
                tool.classify(&json!({"action":action})),
                ToolClass::MUTATING_SERIAL
            );
        }
        assert_eq!(schema["properties"]["width"]["minimum"], 320);
        assert_eq!(schema["properties"]["height"]["maximum"], 1000);
        for action in ["new_tab", "activate_tab", "close_tab"] {
            assert!(actions.contains(&json!(action)), "missing {action}");
            assert_eq!(
                tool.classify(&json!({"action":action})),
                ToolClass::MUTATING_SERIAL
            );
        }
        assert_eq!(
            tool.classify(&json!({"action":"tabs"})),
            ToolClass::READONLY_PARALLEL
        );
        assert_eq!(schema["properties"]["tab_id"]["type"], "string");
    }

    #[test]
    fn coordinate_and_keyboard_actions_map_to_epoch_checked_host_inputs() {
        assert_eq!(
            input_request("click_at", &json!({"x":12.5,"y":20,"button":"right"}), 17).unwrap(),
            json!({"kind":"click","x":12.5,"y":20.0,"button":"right","expected_epoch":17})
        );
        assert_eq!(
            input_request("type", &json!({"text":"Lotus"}), 17).unwrap(),
            json!({"kind":"type","text":"Lotus","expected_epoch":17})
        );
        assert_eq!(
            input_request("key", &json!({"key":"Shift+Tab"}), 17).unwrap(),
            json!({"kind":"key","key":"Shift+Tab","expected_epoch":17})
        );
        for args in [
            json!({"x":-1,"y":20}),
            json!({"x":"12","y":20}),
            json!({"x":12,"y":20,"button":"invalid"}),
        ] {
            assert!(input_request("click_at", &args, 17).is_err());
        }
    }

    #[test]
    fn native_select_action_requires_bounded_values_and_current_epoch() {
        let tool = BrowserTool::new(Arc::new(BrowserManager::default()));
        let actions = tool.parameters_schema()["properties"]["action"]["enum"]
            .as_array()
            .unwrap()
            .clone();
        assert!(actions.contains(&json!("select_option")));
        assert_eq!(
            tool.classify(&json!({"action":"select_option"})),
            ToolClass::MUTATING_SERIAL
        );
        assert_eq!(
            select_option_request(&json!({"selector":"#choices","values":["", "blue"]}), 17)
                .unwrap(),
            json!({"selector":"#choices","values":["", "blue"],"expected_epoch":17})
        );
        for args in [
            json!({"selector":" ","values":["red"]}),
            json!({"selector":"#choices","target":{"kind":"role","role":"combobox"},"values":["red"]}),
            json!({"selector":"x".repeat(513),"values":["red"]}),
            json!({"selector":"#choices","values":[]}),
            json!({"selector":"#choices","values":[7]}),
            json!({"selector":"#choices","values":["x".repeat(513)]}),
            json!({"selector":"#choices","values":vec!["red"; 17]}),
        ] {
            assert!(select_option_request(&args, 17).is_err(), "{args}");
        }
    }

    #[tokio::test]
    async fn in_memory_file_input_rejects_invalid_payload_before_browser_open() {
        let tool = BrowserTool::new(Arc::new(BrowserManager::default()));
        let mut ctx = ToolCtx::none("browser-test");
        ctx.session_id = Some(Arc::from("current-chat"));
        let base = json!({
            "action":"set_file_input", "selector":"#upload", "filename":"sample.txt",
            "mime_type":"text/plain", "data_base64":"YQ==", "expected_epoch":17,
        });
        for mut invalid in [
            json!({"action":"set_file_input","selector":"#upload","filename":"sample.txt","mime_type":"text/plain","data_base64":"YQ=="}),
            json!({"action":"set_file_input","selector":"#upload","filename":"../secret.txt","mime_type":"text/plain","data_base64":"YQ==","expected_epoch":17}),
            json!({"action":"set_file_input","selector":"#upload","filename":"sample.txt","mime_type":"text/plain","data_base64":"YQ=","expected_epoch":17}),
            base.clone(),
        ] {
            if invalid == base {
                invalid["path"] = json!("/tmp/secret");
            }
            let error = tool.invoke(invalid, ctx.clone()).await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments(_)));
            assert!(!error.to_string().contains("secret"));
        }
        for action in ["click", "tabs"] {
            let mut poisoned = base.clone();
            poisoned["action"] = json!(action);
            let error = tool.invoke(poisoned, ctx.clone()).await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments(_)));
            assert!(!error.to_string().contains("YQ=="));
        }
        let error = tool.invoke(
            json!({"action":"set_file_input","session_id":"other-chat","selector":"#upload","filename":"sample.txt","mime_type":"text/plain","data_base64":"YQ==","expected_epoch":17}),
            ctx,
        ).await.unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[test]
    fn download_action_passes_only_a_bounded_selector_and_epoch_to_host() {
        let tool = BrowserTool::new(Arc::new(BrowserManager::default()));
        let schema = tool.parameters_schema();
        assert!(schema["properties"]["action"]["enum"]
            .as_array()
            .unwrap()
            .contains(&json!("download")));
        assert!(tool.description().contains("direct HTTP(S) link"));
        assert!(tool.description().contains("script-free private page"));
        assert!(tool.description().contains("download_unverifiable"));
        assert_eq!(
            tool.classify(&json!({"action":"download"})),
            ToolClass::MUTATING_SERIAL
        );
        assert_eq!(
            download_request(
                &json!({"action":"download","selector":"a#invoice","expected_epoch":17}),
                17
            )
            .unwrap(),
            json!({"selector":"a#invoice","expected_epoch":17})
        );
        for args in [
            json!({"action":"download","selector":" ","expected_epoch":17}),
            json!({"action":"download","selector":"x".repeat(513),"expected_epoch":17}),
            json!({"action":"download","selector":"#file","url":"https://example.test/file","expected_epoch":17}),
            json!({"action":"download","selector":"#file","path":"/tmp/file","expected_epoch":17}),
            json!({"action":"download","selector":"#file","target":{"kind":"text","value":"Save"},"expected_epoch":17}),
        ] {
            assert!(download_request(&args, 17).is_err(), "{args}");
        }
    }

    #[test]
    fn hover_and_drag_requests_are_bounded_and_unambiguous() {
        assert_eq!(
            pointer_request("hover", &json!({"selector":"#tip"}), 17).unwrap(),
            (
                "hover_selector",
                json!({"selector":"#tip","expected_epoch":17})
            )
        );
        assert_eq!(
            pointer_request("hover", &json!({"x":12.5,"y":20}), 17).unwrap(),
            ("hover_at", json!({"x":12.5,"y":20.0,"expected_epoch":17}))
        );
        assert_eq!(
            pointer_request(
                "drag",
                &json!({"source_selector":"#source","target_selector":"#drop"}),
                17
            )
            .unwrap(),
            (
                "drag_selector",
                json!({"source_selector":"#source","target_selector":"#drop","expected_epoch":17})
            )
        );
        assert_eq!(
            pointer_request(
                "drag",
                &json!({"x":10,"y":20,"to_x":30,"to_y":40,"button":"right"}),
                17
            )
            .unwrap(),
            (
                "drag_at",
                json!({"x":10.0,"y":20.0,"to_x":30.0,"to_y":40.0,"button":"right","expected_epoch":17})
            )
        );
        for (action, args) in [
            ("hover", json!({"selector":"#tip","x":10,"y":20})),
            ("hover", json!({"selector":" "})),
            ("hover", json!({"x":1200,"y":20})),
            ("hover", json!({"x":10,"y":1000})),
            ("drag", json!({"source_selector":"#source"})),
            (
                "drag",
                json!({"source_selector":"#source","target_selector":"#drop","x":10}),
            ),
            (
                "drag",
                json!({"source_selector":"#source","target_selector":"#drop","button":"right"}),
            ),
            ("drag", json!({"x":10,"y":20,"to_x":1200,"to_y":40})),
            (
                "drag",
                json!({"x":10,"y":20,"to_x":30,"to_y":40,"button":"invalid"}),
            ),
        ] {
            assert!(
                pointer_request(action, &args, 17).is_err(),
                "{action}: {args}"
            );
        }
    }

    #[test]
    fn dialog_response_is_bound_to_an_opaque_id_epoch_and_optional_prompt_text() {
        let dialog_id = "a".repeat(24);
        assert_eq!(
            dialog_response_args(
                &json!({"dialog_id":dialog_id,"accept":true,"text":"answer"}),
                17,
            )
            .unwrap(),
            json!({"dialog_id":dialog_id,"accept":true,"text":"answer","expected_epoch":17})
        );
        assert_eq!(
            dialog_response_args(&json!({"dialog_id":dialog_id,"accept":false}), 17).unwrap()
                ["text"],
            Value::Null
        );
        for args in [
            json!({"dialog_id":"short","accept":true}),
            json!({"dialog_id":dialog_id}),
            json!({"dialog_id":dialog_id,"accept":false,"text":"answer"}),
            json!({"dialog_id":dialog_id,"accept":true,"text":"x".repeat(4097)}),
        ] {
            assert!(dialog_response_args(&args, 17).is_err(), "{args}");
        }
    }

    #[test]
    fn semantic_locator_arguments_are_bounded_and_exclusive_with_css() {
        let role =
            json!({"kind":"role","role":"button","name":"Save","frame_selector":"iframe#checkout"});
        assert_eq!(
            locator_request(&json!({"target":role}), 17, false).unwrap(),
            json!({"target":role,"expected_epoch":17})
        );
        assert_eq!(
            locator_request(&json!({"selector":"#save"}), 17, false).unwrap(),
            json!({"selector":"#save","expected_epoch":17})
        );
        assert_eq!(
            locator_request(&json!({}), 17, true).unwrap(),
            json!({"expected_epoch":17})
        );
        for args in [
            json!({}),
            json!({"selector":"#save","target":role}),
            json!({"target":{"kind":"role","role":"button","value":"Save"}}),
            json!({"target":{"kind":"label"}}),
            json!({"target":{"kind":"text","value":" "}}),
            json!({"target":{"kind":"text","value":"Save","exact":"yes"}}),
            json!({"target":{"kind":"text","value":"Save","frame_selector":" "}}),
            json!({"target":{"kind":"role","role":"BUTTON"}}),
        ] {
            assert!(locator_request(&args, 17, false).is_err(), "{args}");
        }
        let tool = BrowserTool::new(Arc::new(BrowserManager::default()));
        assert!(tool
            .description()
            .contains("[ref=e...] markers are not stable"));
        assert_eq!(
            tool.parameters_schema()["properties"]["target"]["type"],
            "object"
        );
    }

    #[tokio::test]
    async fn new_mutations_require_snapshot_epoch_before_starting_browser() {
        let tool = BrowserTool::new(Arc::new(BrowserManager::default()));
        let mut ctx = ToolCtx::none("browser-test");
        ctx.session_id = Some(Arc::from("current-chat"));
        for (action, args) in [
            ("new_tab", json!({})),
            ("activate_tab", json!({"tab_id":"other"})),
            ("close_tab", json!({"tab_id":"other"})),
            ("history", json!({"direction":"back"})),
            ("viewport", json!({"width":640,"height":480})),
            ("click_at", json!({"x":12,"y":20})),
            ("hover", json!({"x":12,"y":20})),
            ("drag", json!({"x":12,"y":20,"to_x":30,"to_y":40})),
            ("type", json!({"text":"Lotus"})),
            ("key", json!({"key":"Enter"})),
            (
                "set_file_input",
                json!({"selector":"#upload","filename":"a.txt","mime_type":"text/plain","data_base64":"YQ=="}),
            ),
            (
                "dialog_respond",
                json!({"dialog_id":"a".repeat(24),"accept":true}),
            ),
        ] {
            let mut args = args;
            args["action"] = json!(action);
            let error = tool.invoke(args, ctx.clone()).await.unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments(_)), "{action}");
        }
        let oversized = "a".repeat(10000);
        for tab_id in ["short", "AAAAAAAAAAAAAAAAAAAAAAAA", oversized.as_str()] {
            let error = tool
                .invoke(
                    json!({"action":"activate_tab","tab_id":tab_id,"expected_epoch":17}),
                    ctx.clone(),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, ToolError::InvalidArguments(_)));
        }
    }

    #[tokio::test]
    async fn browser_tool_never_accepts_a_model_selected_session_id() {
        let tool = BrowserTool::new(Arc::new(BrowserManager::default()));
        let mut ctx = ToolCtx::none("browser-test");
        ctx.session_id = Some(Arc::from("current-chat"));
        let error = tool
            .invoke(json!({"action":"snapshot","session_id":"other-chat"}), ctx)
            .await
            .unwrap_err();
        assert!(matches!(error, ToolError::InvalidArguments(_)));
    }

    #[tokio::test]
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn model_download_returns_binary_bytes_without_replacing_shared_page() {
        use base64::Engine as _;
        use sha2::{Digest as _, Sha256};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        const BINARY: &[u8] = &[0, 1, 2, 3, 0, 255, 254, 128, 42, 10, 13];
        const PAGE: &[u8] = br#"<!doctype html><title>Shared download page</title>
            <main id="still-here">The page remains open</main>
            <a id="binary" href="/file" download="sample.bin">Download binary</a>"#;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let fixture = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut request = [0u8; 2048];
                    let read = socket.read(&mut request).await.unwrap_or(0);
                    let file = request[..read].starts_with(b"GET /file ");
                    let (body, headers): (&[u8], &str) = if file {
                        (
                            BINARY,
                            "Content-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"sample.bin\"\r\n",
                        )
                    } else {
                        (PAGE, "Content-Type: text/html\r\n")
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                });
            }
        });

        let browser = Arc::new(BrowserManager::default());
        let tool = BrowserTool::new(browser.clone());
        let mut ctx = ToolCtx::none("browser-test");
        ctx.session_id = Some(Arc::from("download-chat"));
        let opened = browser.open("download-chat").await.unwrap();
        let ToolOutcome::Completed(navigated) = tool
            .invoke(
                json!({"action":"navigate","url":url,"expected_epoch":opened["page_epoch"]}),
                ctx.clone(),
            )
            .await
            .unwrap()
        else {
            panic!("navigation must complete");
        };
        let navigated: Value = serde_json::from_str(&navigated.result).unwrap();
        let epoch = navigated["page_epoch"].as_u64().unwrap();
        let active_tab_id = navigated["active_tab_id"].as_str().unwrap();

        let ToolOutcome::Completed(download) = tool
            .invoke(
                json!({"action":"download","selector":"#binary","expected_epoch":epoch}),
                ctx.clone(),
            )
            .await
            .unwrap()
        else {
            panic!("download must complete");
        };
        let result: Value = serde_json::from_str(&download.result).unwrap();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(result["data_base64"].as_str().unwrap())
            .unwrap();
        assert_eq!(bytes, BINARY);
        assert_eq!(result["byte_count"], BINARY.len());
        assert_eq!(result["sha256"], hex::encode(Sha256::digest(BINARY)));
        assert_eq!(result["filename"], "sample.bin");
        assert_eq!(result["page_epoch"], epoch);
        assert_eq!(result["active_tab_id"], active_tab_id);
        assert_eq!(result["url"], url);
        assert!(result.get("path").is_none());

        let dom = browser
            .command("download-chat", "dom", json!({}))
            .await
            .unwrap();
        assert_eq!(dom["page_epoch"], epoch);
        assert_eq!(dom["active_tab_id"], active_tab_id);
        assert!(dom["html"].as_str().unwrap().contains("id=\"still-here\""));
        let image = browser
            .command("download-chat", "screenshot", json!({}))
            .await
            .unwrap();
        assert_eq!(image["page_epoch"], epoch);
        assert_eq!(image["active_tab_id"], active_tab_id);
        assert!(image["data"].as_str().unwrap().len() > 1000);
        browser.close("download-chat").await.unwrap();
        fixture.abort();
    }

    #[tokio::test]
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn model_hover_and_drag_share_the_workbench_dom_and_screenshot() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let fixture = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut request = [0u8; 2048];
                    let read = socket.read(&mut request).await.unwrap_or(0);
                    let body: &[u8] = if request[..read].starts_with(b"GET /after-drop ") {
                        b"<main>After model drag navigation</main>"
                    } else if request[..read].starts_with(b"GET /navigate-on-drop ") {
                        br#"<!doctype html><style>
                        #source{position:absolute;left:20px;top:80px;width:80px;height:80px;background:blue}
                        #drop{position:absolute;left:220px;top:80px;width:80px;height:80px;background:green}
                    </style>
                    <div id="source" draggable="true" ondragstart="event.dataTransfer.setData('text/plain','moved')">Drag</div>
                    <div id="drop" ondragover="event.preventDefault()" ondrop="event.preventDefault();location.href='/after-drop'">Drop</div>"#
                    } else {
                        br#"<!doctype html><style>
                        #source{position:absolute;left:20px;top:80px;width:80px;height:80px;background:blue}
                        #drop{position:absolute;left:220px;top:80px;width:80px;height:80px;background:green}
                    </style>
                    <button id="hover" onpointerenter="document.querySelector('#hovered').textContent='yes'">Hover</button>
                    <div id="source" draggable="true" ondragstart="event.dataTransfer.setData('text/plain','moved')">Drag</div>
                    <div id="drop" ondragover="event.preventDefault()" ondrop="event.preventDefault();document.querySelector('#dropped').textContent=event.dataTransfer.getData('text/plain')">Drop</div>
                    <output id="hovered">no</output><output id="dropped">no</output>"#
                    };
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(headers.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                });
            }
        });

        let browser = Arc::new(BrowserManager::default());
        let tool = BrowserTool::new(browser.clone());
        let mut ctx = ToolCtx::none("browser-test");
        ctx.session_id = Some(Arc::from("shared-chat"));
        let opened = browser.open("shared-chat").await.unwrap();
        let navigated = browser
            .command(
                "shared-chat",
                "navigate",
                json!({"url":url,"expected_epoch":opened["page_epoch"]}),
            )
            .await
            .unwrap();
        let epoch = navigated["page_epoch"].as_u64().unwrap();
        for args in [
            json!({"action":"hover","selector":"#hover","expected_epoch":epoch}),
            json!({"action":"drag","source_selector":"#source","target_selector":"#drop","expected_epoch":epoch}),
        ] {
            let ToolOutcome::Completed(result) = tool.invoke(args, ctx.clone()).await.unwrap()
            else {
                panic!("browser action must complete");
            };
            let state: Value = serde_json::from_str(&result.result).unwrap();
            assert_eq!(state["page_epoch"], epoch);
            assert_eq!(state["url"], url);
        }
        let workbench_dom = browser
            .command("shared-chat", "dom", json!({}))
            .await
            .unwrap();
        assert!(workbench_dom["html"]
            .as_str()
            .unwrap()
            .contains("id=\"hovered\">yes"));
        assert!(workbench_dom["html"]
            .as_str()
            .unwrap()
            .contains("id=\"dropped\">moved"));
        assert_eq!(workbench_dom["page_epoch"], epoch);
        let workbench_image = browser
            .command("shared-chat", "screenshot", json!({}))
            .await
            .unwrap();
        assert_eq!(workbench_image["page_epoch"], epoch);
        assert_eq!(
            workbench_image["active_tab_id"],
            workbench_dom["active_tab_id"]
        );
        assert!(workbench_image["data"].as_str().unwrap().len() > 1000);

        let navigation_url = format!("{url}navigate-on-drop");
        let ready = browser
            .command(
                "shared-chat",
                "navigate",
                json!({"url":navigation_url,"expected_epoch":epoch}),
            )
            .await
            .unwrap();
        let ready_epoch = ready["page_epoch"].as_u64().unwrap();
        let ToolOutcome::Completed(result) = tool
            .invoke(
                json!({
                    "action":"drag",
                    "source_selector":"#source",
                    "target_selector":"#drop",
                    "expected_epoch":ready_epoch
                }),
                ctx.clone(),
            )
            .await
            .unwrap()
        else {
            panic!("model drag navigation must complete");
        };
        let state: Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(state["url"], format!("{url}after-drop"));
        assert_ne!(state["page_epoch"], ready_epoch);
        let after_dom = browser
            .command("shared-chat", "dom", json!({}))
            .await
            .unwrap();
        assert_eq!(after_dom["page_epoch"], state["page_epoch"]);
        assert_eq!(after_dom["active_tab_id"], state["active_tab_id"]);
        assert!(after_dom["html"]
            .as_str()
            .unwrap()
            .contains("After model drag navigation"));
        let after_image = browser
            .command("shared-chat", "screenshot", json!({}))
            .await
            .unwrap();
        assert_eq!(after_image["page_epoch"], state["page_epoch"]);
        assert_eq!(after_image["active_tab_id"], state["active_tab_id"]);
        assert!(after_image["data"].as_str().unwrap().len() > 1000);
        browser.close("shared-chat").await.unwrap();
        fixture.abort();
    }

    #[tokio::test]
    #[ignore = "requires the Playwright Chromium runtime"]
    async fn model_file_input_updates_only_its_chat_page_and_current_epoch() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let fixture = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut request = [0u8; 1024];
                    let _ = socket.read(&mut request).await;
                    let body = br#"<!doctype html><input id="upload" type="file" onchange="const file=this.files[0];const reader=new FileReader();reader.onload=()=>document.querySelector('#result').textContent=[file.name,file.type,file.size,reader.result].join('|');reader.readAsText(file)"><output id="result">No file</output>"#;
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(headers.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                });
            }
        });
        let browser = Arc::new(BrowserManager::default());
        let tool = BrowserTool::new(browser.clone());
        let mut ctx = ToolCtx::none("browser-test");
        ctx.session_id = Some(Arc::from("file-chat"));
        let opened = browser.open("file-chat").await.unwrap();
        let navigated = browser
            .command(
                "file-chat",
                "navigate",
                json!({
                    "url":url,"expected_epoch":opened["page_epoch"],
                }),
            )
            .await
            .unwrap();
        let epoch = navigated["page_epoch"].as_u64().unwrap();
        let args = json!({
            "action":"set_file_input","selector":"#upload","filename":"sample.txt",
            "mime_type":"text/plain","data_base64":"bWVtb3J5LW9ubHkgZmlsZQ==",
            "expected_epoch":epoch,
        });
        let ToolOutcome::Completed(result) = tool.invoke(args.clone(), ctx.clone()).await.unwrap()
        else {
            panic!("file input must complete");
        };
        assert!(!result.result.contains("bWVtb3J5"));
        let state: Value = serde_json::from_str(&result.result).unwrap();
        assert_eq!(state["page_epoch"], epoch);
        let mut dom = json!({});
        for _ in 0..30 {
            dom = browser
                .command("file-chat", "dom", json!({}))
                .await
                .unwrap();
            if dom["html"]
                .as_str()
                .unwrap_or("")
                .contains("sample.txt|text/plain|16|memory-only file")
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(dom["html"]
            .as_str()
            .unwrap()
            .contains("sample.txt|text/plain|16|memory-only file"));
        assert_eq!(dom["page_epoch"], epoch);
        let screenshot = browser
            .command("file-chat", "screenshot", json!({}))
            .await
            .unwrap();
        assert_eq!(screenshot["page_epoch"], epoch);
        assert!(screenshot["data"].as_str().unwrap().len() > 1000);

        let other = browser.open("other-chat").await.unwrap();
        let other = browser
            .command(
                "other-chat",
                "navigate",
                json!({
                    "url":url,"expected_epoch":other["page_epoch"],
                }),
            )
            .await
            .unwrap();
        let other_dom = browser
            .command("other-chat", "dom", json!({}))
            .await
            .unwrap();
        assert_eq!(other_dom["page_epoch"], other["page_epoch"]);
        assert!(other_dom["html"].as_str().unwrap().contains("No file"));
        let mut stale_args = args;
        stale_args["expected_epoch"] = opened["page_epoch"].clone();
        assert!(tool.invoke(stale_args, ctx).await.is_err());
        browser.close("other-chat").await.unwrap();
        browser.close("file-chat").await.unwrap();
        fixture.abort();
    }
}
