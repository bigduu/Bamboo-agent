# TUI keybindings

The Bamboo TUI resolves every key through one action registry. Press `F1` to
see the bindings that are active after configuration; the same resolved labels
are used in tab headers, dialog footers, and the command palette.

## Load a keymap

Pass a JSON file to either CLI surface:

```console
bamboo tui --keymap ./tui-keymap.json
bamboo-tui --keymap ./tui-keymap.json
```

`BAMBOO_TUI_KEYMAP` supplies the path when `--keymap` is omitted. The flag has
precedence over the environment variable.

```json
{
  "version": 1,
  "leader": "Ctrl+\\",
  "leader_timeout_ms": 750,
  "bindings": [
    {
      "context": "global",
      "action": "show-help",
      "keys": ["F6", "Leader h"]
    },
    {
      "context": "chat",
      "action": "insert-newline",
      "keys": ["Alt+Enter"]
    },
    {
      "context": "navigation",
      "action": "switch-tab-6",
      "unbind": true
    }
  ]
}
```

An override replaces all defaults for that exact context/action pair. Use
`unbind: true` instead of `keys` to remove it. A sequence is written with
space-separated strokes, such as `"Leader h"`; alternatives are separate
items in `keys`. Key names are case-insensitive and support `Ctrl`, `Alt`,
`Shift`, arrows, `Home`, `End`, `PageUp`, `PageDown`, `Tab`, `Backspace`,
`Delete`, `Esc`, `Enter`, and `F1` through `F24`.

The leader timeout must be 200–5000 ms. `Esc` cancels a pending sequence, a
focus change cancels it, and an unmatched continuation reports the exact
sequence instead of falling through to another action. If an input event wins
the timer race after expiry, it is reprocessed as a fresh key instead of being
discarded. A single-stroke global quit binding always preempts a pending
sequence.

## Contexts and action IDs

Action and context IDs are stable kebab-case strings:

- `global`: `quit-or-stop`, `show-help`, `show-notifications`,
  `open-command-palette`, `new-session`, `reopen-pending-question`,
  `open-model-picker`, `open-session-picker`, `stop-run`, `open-config-tab`,
  `open-schedules-tab`, `next-tab`, `previous-tab`
- `navigation`: `show-help`, `switch-tab-1` through `switch-tab-6`
- `chat`: `stop-run`, `toggle-details`, `open-slash-palette`, `send-message`,
  `insert-newline`, transcript scrolling, and `focus-conversation-blocks`
- `conversation-block`: focus/scroll/copy/activate actions and
  `toggle-details`
- `help`, `notifications`, `question-options`, `question-custom`,
  `question-number`, `question-inspect`: their displayed navigation,
  answer, inspect, copy, and cancel actions; numbered shortcuts use
  `quick-answer-1` through `quick-answer-9`
- `serve-offer`, `session-delete-confirm`, `schedule-delete-confirm`:
  `confirm` and `reject`
- `sessions`, `mcp`, `schedules`, `schedule-form`, `skills`, `config`,
  `config-editor`: the actions shown in the corresponding F1 group
- `session-picker-browse`, `session-picker-rename`,
  `session-picker-pinning`, `model-picker`, `command-palette`: the actions
  shown in each picker group

Use the F1 reference to confirm the exact resolved action labels before
distributing a keymap.

## Validation and terminal safety

The complete custom layer is applied atomically. Unknown fields, versions,
contexts, actions, or key names; duplicate overrides; collisions; ambiguous
prefixes; and unreachable required actions reject the file. The TUI reports
the path and reason, then uses all built-in defaults—never a partially applied
map.

Global sequences whose first stroke is printable are rejected so normal Chat
text cannot be captured as the start of an application action. When bindings
from several active contexts share a prefix, the focused/modal context has
priority over a shorter binding from a lower context; compatible longer
continuations remain available across the context stack. Custom `Ctrl+S`,
`Ctrl+Q`, and `Ctrl+Z` bindings are
rejected because terminal flow control, multiplexers, SSH, or signal handling
can consume them. Built-in compatibility aliases for `Ctrl+S`/`Ctrl+Q` always
have leader or function-key alternatives. `Alt+Enter` is the portable newline
default; `Shift+Enter` remains an additional enhanced-keyboard alias. Release
events are ignored, and held-key repeats cannot repeat confirmation,
submission, deletion, or lifecycle actions.
