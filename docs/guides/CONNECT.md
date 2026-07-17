# How-to: drive Bamboo from an IM platform (Telegram / Feishu)

`bamboo-connect` lets you talk to a running `bamboo serve` instance from
Telegram or Feishu/Lark instead of (or alongside) the HTTP API/UI — send a
message in the chat, it runs a normal agent session, replies (and tool-call
progress, for platforms that support editable messages) stream back into the
same chat.

It is **fully inert by default**: with no `connect.json` (and no legacy
`connect` key in `config.json`), zero background tasks are started. Nothing
listens for IM traffic until you configure at least one platform.

## 1. Create `connect.json`

This is a **separate file** from `config.json` — `${data_dir}/connect.json`
(same directory, typically `~/.bamboo/connect.json`). Create it by hand (there
is currently no `bamboo connect add` CLI verb — this is the one config surface
you still hand-edit):

```json
{
  "platforms": [
    {
      "type": "telegram",
      "token": "123456789:AAH...your-bot-token...",
      "allow_from": ["987654321"]
    }
  ]
}
```

Restart `bamboo serve` (or your sidecar) to pick it up — `connect.json` is
read at startup.

## 2. Telegram setup

1. Talk to [@BotFather](https://t.me/BotFather) on Telegram, `/newbot`, and
   copy the bot token it gives you into `token` above.
2. Send your bot any message, then check the server logs (or Telegram's own
   `getUpdates` API) for your numeric chat id — put that in `allow_from`.
3. **`allow_from` defaults to deny-all when empty.** This is deliberately
   stricter than other allowlists in Bamboo, because an IM bridge is
   internet-facing by nature (Telegram's servers reach your bot, not the
   other way around) — an empty or missing `allow_from` means the bot ignores
   every sender.
4. No public IP or webhook needed: the Telegram adapter long-polls
   `getUpdates` outbound over HTTPS, so it works from behind NAT/firewalls
   exactly like `bamboo serve` running on a laptop.

Each Telegram chat maps to one Bamboo session; a message in a new chat starts
a new session, replies stream back as edited/appended messages, and
approval/clarification prompts (`AgentEvent::NeedClarification` /
`ToolApprovalRequested`) render as inline buttons where Telegram supports
them.

## 3. Feishu / Lark setup

Feishu uses a persistent WebSocket connection (no public endpoint needed
either) and app credentials instead of a bot token:

```json
{
  "platforms": [
    {
      "type": "feishu",
      "app_id": "cli_a1b2c3d4e5f6",
      "app_secret": "your-app-secret",
      "domain": "feishu",
      "allow_from": ["ou_xxxxxxxxxxxxxxxxxxxxxxxxxxxx"]
    }
  ]
}
```

1. Create a custom app in the [Feishu Open Platform
   console](https://open.feishu.cn/app) (or [Lark's](https://open.larksuite.com/app)
   for the international product), enable the bot capability, and subscribe to
   message + card-interaction events over the "long connection" (WebSocket)
   transport — no public callback URL to configure.
2. Copy the App ID / App Secret into `app_id`/`app_secret`.
3. `domain` selects which cloud: omit or `"feishu"` for `open.feishu.cn`
   (mainland China), `"lark"` for `open.larksuite.com` (international), or an
   explicit `https://...` base URL for a self-hosted/enterprise deployment.
4. `allow_from` takes the sender's Feishu `open_id`; same deny-all-when-empty
   default as Telegram.

Feishu conversations render approval/clarification prompts as interactive
cards with buttons, matching the Telegram inline-keyboard experience.

## 4. Multiple platforms / multiple bots

`platforms` is an array — add as many entries as you like, mixing platform
types. Each gets its own `id` (auto-assigned on first save if you don't set
one) and runs an independent long-poll/WebSocket task.

## 5. Secrets

`token` (Telegram) and `app_secret` (Feishu) are encrypted at rest the same
way every other secret in Bamboo is — see [config
reference](../config-reference.md#secrets-and-masking): once loaded, a
`GET`/inspect of the resolved config shows `****...****` in place of the real
value, and re-submitting that placeholder unchanged (via a settings UI) is
treated as "keep the existing secret," not a new value.

## Troubleshooting

- **Bot doesn't respond at all:** check `allow_from` actually contains the
  sender's id — an empty list silently drops every message (by design, not a
  bug).
- **`connect.json` seems ignored:** it's only read at `bamboo serve` startup;
  restart after editing. A malformed `connect.json` is quarantined to
  `connect.json.bak` and treated as empty rather than crashing the server —
  check the startup logs for a parse warning.
- **A legacy inline `connect` key in `config.json`:** older Bamboo versions
  stored this section inside `config.json` directly. It's auto-migrated into
  `connect.json` (and stripped from `config.json`) the next time the config
  loads — no action needed.
