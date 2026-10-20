# Native and portable hooks

## Three distinct surfaces

`hooks` is the existing provider request-preflight namespace (for example image
fallback). Native agent lifecycle configuration is `lifecycle_hooks`. Portable
plugin hooks are declared by a plugin's `provides.hooks`. Do not interchange
their schemas or translate timeout units implicitly.

## Native lifecycle hooks

Native lifecycle handlers support command and external script execution, JSON
stdin and event-specific responses. Their timeout field is `timeout_ms` in
milliseconds. Events include SessionStart, SessionEnd, UserPromptSubmit,
PreToolUse, PostToolUse, Stop, PreCompact and Notification. The global native
lifecycle switch defaults to disabled. Runs snapshot lifecycle configuration
at their established runtime boundary; changing settings is not proof that an
already running loop has adopted them.

Scripts are not sandboxed by the hook mechanism. A settings “dry-run” executes
the script even if it does not persist configuration. Never invoke it merely
to inspect or diagnose a hook.

### Make a small native script

After reviewing and authorizing execution, save this Python example as
`.bamboo/hooks/add-context.py` in the session workspace. Python must be installed;
Bamboo does not bundle the interpreter. Relative script paths resolve from the
session workspace (server-owned events have their documented fallback root).

```python
import json
import sys

event = json.load(sys.stdin)
print(json.dumps({
    "additional_context": "Local hook observed " + event["hook_event_name"]
}))
```

The following is a compatibility-view configuration example, not a replacement
for a migrated `hooks.json` section envelope. Use the installed settings editor
to add the handler and preserve the section's revision/ownership rules:

```json
{
  "lifecycle_hooks": {
    "enabled": true,
    "SessionStart": [{
      "hooks": [{
        "type": "script",
        "path": ".bamboo/hooks/add-context.py",
        "runner": "python",
        "timeout_ms": 5000
      }]
    }]
  }
}
```

Handlers receive a schema-versioned JSON envelope on stdin, including
`hook_event_name` and event payload. Exit 0 with empty stdout or one JSON
response; send debug output to stderr, not alongside response JSON. Native
decision-capable events accept `decision: "allow"`, `"block"` or `"ask"`
according to that event's contract, and may return `additional_context`.
Exit 2 requests a block. Timeouts, malformed output and most other failures are
non-blocking failures; hooks are not a fail-closed security sandbox. Observer
events cannot change a settled result. An authorized settings dry-run can
check the selected runtime and captured output without persisting the handler;
review the code first because that check actually executes it.

## Portable plugin command hooks

The supported phase-one subset is synchronous command hooks on
UserPromptSubmit, PreToolUse, PostToolUse and Stop. The portable `timeout` is an
explicit 1–600 seconds. Native `timeout_ms` does not apply to that schema.
Unsupported events, output shapes and matcher features must be reported rather
than treated as general Claude/Codex compatibility. There is no automatic
importer for another tool's plugin manifest.

Installation does not grant execution trust. The complete current bundle must
match the exact reviewed digest, identity and version before execution. The
compatibility/review report can show needs-review, disabled, active or
unsupported. Changes to reviewed bytes or relevant metadata invalidate trust.
Reviewing a bundle does not sandbox commands or pin external interpreters,
PATH binaries and downloaded dependencies.

A PreToolUse `deny` can reject execution; `allow` is a no-op and cannot grant
permission. Portable `ask` is unsupported. PostToolUse feedback cannot undo
side effects that already occurred. A rejection is sticky; a later allow/no-op
does not reverse it. Stop continuation is bounded by existing host behavior.

Inspect the existing review report only with the user's authorized scope.
Enabling a hook, posting a review receipt or executing a test requires its own
authorization; reading this chapter never provides it.

Source: `docs/lifecycle-hooks.md`, `docs/plugin-command-hooks.md`,
`crates/infra/bamboo-hooks/src/configured.rs`,
`crates/infra/bamboo-plugin/examples/portable-hooks`,
`crates/app/bamboo-server/src/handlers/settings/lifecycle_hooks.rs`.
