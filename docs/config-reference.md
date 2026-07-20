# Configuration reference

Everything Bamboo reads from `${data_dir}/config.json` (`data_dir` defaults to
`${HOME}/.bamboo`, override with `BAMBOO_DATA_DIR` or `--data-dir`), plus the
handful of sibling files and environment variables that participate in
configuration.

Prefer not to hand-edit JSON? `bamboo init` writes a starter config,
`bamboo config set <dotted.key> <value>` changes one value at a time (secret
keys are encrypted automatically — see [Secrets](#secrets-and-masking) below),
and `bamboo config [--show-secrets]` prints the resolved config. This document
is for when you need to know exactly what a key does or edit the file by hand.

**Precedence:** `config.json` < environment variables < CLI flags (`bamboo
serve --port ...` wins over everything). Provider selection specifically is
`providers.<name>` / `provider_instances.<id>` (file) → `BAMBOO_PROVIDER` /
`BAMBOO_<PROVIDER>_API_KEY` (env, in-memory only, never persisted) →
`--provider` (CLI).

Source of truth for every struct below: `crates/infra/bamboo-config/src/config.rs`
(`pub struct Config`, around line 1069) unless noted otherwise. Struct field
lists here are derived directly from that code — if the two disagree, the code
wins; please file an issue.

- [Top-level shape](#top-level-shape)
- [Providers](#providers)
- [Server](#server)
- [Tools, skills, hooks](#tools-skills-hooks)
- [LLM stream timeouts](#llm-stream-timeouts)
- [Memory / auto-dream / gardener](#memory--auto-dream--gardener)
- [Sub-agents + the `claude_code` executor](#sub-agents--the-claude_code-executor)
- [MCP servers](#mcp-servers)
- [Notifications](#notifications)
- [`connect` — the IM bridge](#connect--the-im-bridge)
- [`plugin_trust`](#plugin_trust)
- [Keyword masking](#keyword-masking)
- [Permissions](#permissions)
- [Model limits (`model_limits.json`)](#model-limits-model_limitsjson)
- [Schedules (`schedules.json`)](#schedules-schedulesjson)
- [Environment variables](#environment-variables)
- [Secrets and masking](#secrets-and-masking)
- [Encryption at rest](#encryption-at-rest)
- [Corrupt-config recovery](#corrupt-config-recovery)

## Top-level shape

```json
{
  "provider": "anthropic",
  "providers": { "anthropic": { "api_key": "sk-ant-...", "model": "claude-sonnet-4-6" } },
  "server": { "port": 9562, "bind": "127.0.0.1" }
}
```

Every top-level key is optional (`#[serde(default)]`) — a config with only
`provider`/`providers` is valid; every field below silently falls back to its
default. The full field list of `Config`:

| Field | Type | Notes |
|---|---|---|
| `http_proxy` / `https_proxy` | `String` | Outbound proxy URLs for provider HTTP calls. |
| `proxy_auth_encrypted` | `Option<String>` | Encrypted proxy credentials; hydrated in memory as `proxy_auth`. |
| `provider` | `String` | Default provider name. Default `"anthropic"`. |
| `defaults` | `Option<DefaultsConfig>` | Per-role model routing (`chat`/`fast`/`vision`/`planning`/...); only consulted when `features.provider_model_ref` is on. |
| `providers` | `ProviderConfigs` | Legacy single-instance-per-type provider configs. See [Providers](#providers). |
| `provider_instances` | `HashMap<String, ProviderInstanceConfig>` | Multi-instance provider configs, keyed by an id you choose (e.g. two Anthropic keys under different labels). Takes precedence over `providers` when non-empty. |
| `default_provider_instance` | `Option<String>` | Which `provider_instances` entry is the default; overrides legacy `provider` when set. |
| `server` | `ServerConfig` | HTTP bind/port/TLS. See [Server](#server). |
| `keyword_masking` | `KeywordMaskingConfig` | Outbound-body secret scrubbing. See [Keyword masking](#keyword-masking). |
| `anthropic_model_mapping` / `gemini_model_mapping` | `{ mappings: HashMap<String,String> }` | Alias an OpenAI-shaped model id (e.g. `"gemini-pro"`) to the real upstream model id for that provider's compat endpoint. |
| `hooks` | `HooksConfig` | Request preflight hooks; today just `image_fallback` (text-only-model image handling). |
| `tools` | `ToolsConfig` | `{ disabled: Vec<String> }` — tool names omitted from every session's schema globally. |
| `skills` | `SkillsConfig` | `{ disabled: Vec<String> }` — skill ids excluded from selection/loading globally. |
| `env_vars` | `Vec<EnvVarEntry>` | User-managed env vars injected into `Bash`-tool child processes (`{name, value, secret, value_encrypted}`); `secret: true` entries are encrypted at rest. |
| `default_work_area` | `Option<DefaultWorkAreaConfig>` | `{ path: Option<String> }` — default workspace when a session has none set. |
| `access_control` | `Option<AccessControlConfig>` | Password gate for the HTTP API/UI (`password_enabled`, hashed+salted). |
| `features` | `FeatureFlags` | `{ provider_model_ref: bool, dynamic_model_routing: bool }` — incremental rollout toggles, both off by default. |
| `stream_timeout` | `StreamTimeoutConfig` | Independent transport, first-semantic, and midstream-semantic watchdog deadlines. See below. |
| `memory` | `Option<MemoryConfig>` | Memory/auto-dream/gardener settings. See below. |
| `subagents` | `SubagentsConfig` | Sub-agent execution + the `claude_code` executor. See below. |
| `cluster_fabric` | `ClusterFabricConfig` | Operator-managed remote nodes for deploying `broker-agent` workers over SSH; empty by default. SSH secrets encrypted at rest. |
| `mcp` (on-disk key `mcpServers`) | `McpConfig` | External tool servers. See [MCP servers](#mcp-servers). |
| `notifications` | `NotificationsConfig` | Desktop/ntfy/Bark delivery channels. See below. |
| `connect` | `ConnectConfig` | **Not actually stored here** — see [connect](#connect--the-im-bridge). |
| `plugin_trust` | `PluginTrustConfig` | Plugin install trust policy. See below. |
| `extra` | `BTreeMap<String, Value>` | Catch-all flatten for keys not (yet) promoted to a typed field — `permissions`, `externalAgents`, `subagentRouting`, setup-wizard state, etc. live here. Round-trips losslessly even for fields this version of Bamboo doesn't know about. |

## Providers

Two shapes coexist; a fresh `bamboo init` writes the legacy single-instance
`providers` shape, which is simplest for one key per provider:

```json
{
  "provider": "anthropic",
  "providers": {
    "anthropic": { "api_key": "sk-ant-...", "model": "claude-sonnet-4-6" }
  }
}
```

Each provider stanza (`OpenAIConfig` / `AnthropicConfig` / `GeminiConfig` /
`CopilotConfig` / `BodhiConfig`, all in `config.rs`) shares this core shape —
`api_key` (write-only; persisted as `api_key_encrypted`, never re-emitted
plaintext by `GET`), `base_url` (override the upstream endpoint — self-hosted
proxies, Azure-style deployments, etc.), `model`, `fast_model`, `vision_model`,
`reasoning_effort`, `responses_only_models: Vec<String>` (force these models
onto the OpenAI Responses API path), `request_overrides` (provider-specific
per-endpoint HTTP header/body tweaks), and an `extra` flatten for
forward-compat fields. `AnthropicConfig` adds `max_tokens` and
`thinking_replay_always` (needed by some Anthropic-compatible upstreams, e.g.
GLM's `/anthropic` endpoint). `BodhiConfig` adds `target_provider` (which of
openai/anthropic/gemini the Bodhi proxy should present as). `CopilotConfig`
has no `api_key` at all — it authenticates via a cached OAuth token
(`headless_auth` for headless/CI login).

For more than one instance of a provider type (e.g. two separate Anthropic
keys/workspaces), use `provider_instances` instead:

```json
{
  "default_provider_instance": "work",
  "provider_instances": {
    "work": { "provider_type": "anthropic", "api_key": "sk-ant-work-...", "model": "claude-sonnet-4-6" },
    "personal": { "provider_type": "anthropic", "api_key": "sk-ant-personal-...", "enabled": true }
  }
}
```

`provider_instances` entries have the same field set as the legacy stanzas
plus `provider_type` (which of the five kinds this is) and `enabled` (default
`true`). When `provider_instances` is non-empty it takes precedence over
`providers`/`provider` as the routing source.

## Server

```json
{ "server": { "port": 9562, "bind": "127.0.0.1", "workers": 10 } }
```

`port` (default `9562`), `bind` (default `127.0.0.1`), `static_dir` (serve the
bundled frontend from a custom path), `workers` (Actix worker threads, default
`10`), `tls: Option<TlsConfig>` (`cert_file`/`key_file` PEM paths for manual
TLS termination — no ACME/auto-cert). All overridable per-invocation with
`bamboo serve --port/--bind/--workers`.

## Tools, skills, hooks

- `tools.disabled: Vec<String>` — tool names (e.g. `"Bash"`) hidden from every
  session's tool schema, globally. Compare to the SDK's per-agent
  `AgentBuilder::tools([...])`, which scopes selection to one in-process
  `Agent` instead.
- `skills.disabled: Vec<String>` — skill ids excluded from selection/loading
  globally.
- `hooks.image_fallback` — how image parts are handled when the effective
  model/path is text-only (drop, OCR-replace, etc. — see
  `ImageFallbackHookConfig`).

## LLM stream timeouts

```json
{
  "stream_timeout": {
    "transport_idle_timeout_secs": 120,
    "first_semantic_timeout_secs": 600,
    "semantic_idle_timeout_secs": 600
  }
}
```

The three watchdogs measure different signals and apply identically to the
main response stream and auxiliary silent model calls:

| Field | Default | Meaning |
|---|---:|---|
| `transport_idle_timeout_secs` | `120` | Maximum gap between valid provider transport frames. Parsed SSE ping/lifecycle frames count even when they contain no token. |
| `first_semantic_timeout_secs` | `600` | Maximum time from request dispatch to the first text, reasoning, or tool-call delta. Transport keepalives do not extend it. |
| `semantic_idle_timeout_secs` | `600` | Maximum semantic-progress gap after output starts. Transport keepalives do not extend it. |

Every value must be between `1` and `86400` seconds. Invalid persisted values
are rejected by config loading; invalid values constructed by an embedding are
replaced with the safe defaults. Timeout errors report the expired phase,
deadline, provider/model identifiers, and last transport/semantic activity,
but never include prompts or raw provider payloads. A stream timeout is not
automatically retried, because replay after partial output or tool-call deltas
could duplicate externally visible state.

## Memory / auto-dream / gardener

Key `memory` (`Option<MemoryConfig>` — absent means every default below
applies). All the dream/gardener toggles default **on**; the values below are
the shipped defaults, so an empty `{}` is already reasonable:

| Field | Default | What it does |
|---|---|---|
| `background_model` | `None` | Model used for memory extraction/consolidation background work; falls back to the primary model. |
| `auto_dream_enabled` | `true` | Distill conversation stretches into candidate memories + notebook entries as the session runs. |
| `auto_dream_interval_secs` | `1800` | How often the dream pass runs. |
| `project_prompt_injection` | `true` | Inject relevant project-scoped memory into the system prompt. |
| `relevant_recall` | `true` | Retrieve relevant durable memories for the current turn. |
| `relevant_recall_rerank` | `false` | Rerank recalled memories (extra model call) before injecting. |
| `project_first_dream` | `true` | Prefer project-scoped memory on a session's first dream pass. |
| `ledger_agenda_injection` / `ledger_gardener_enabled` / `ledger_distillation_enabled` | `true` | Personal-assistant ledger subsystem toggles. |
| `ledger_gardener_interval_secs` | `21600` (6h) | Ledger gardener cadence. |
| `gardener_enabled` | `true` | Background job that splits "multi-topic blob" memories; **calls no LLM when its deterministic pre-screen finds no candidates.** |
| `gardener_interval_secs` | `86400` (daily) | Gardener cadence. |
| `gardener_volume_trigger` | `25` | Run early once this many new memories have accrued, instead of waiting for the interval. |
| `gardener_max_splits_per_run` / `gardener_min_sections` | `8` / `5` | Cost guardrails on one gardener pass. |
| `dedup_gardener_enabled` | `true` | Background near-duplicate memory merge pass. |
| `dedup_gardener_min_score` | `0.6` | Jaccard similarity threshold to merge. |
| `dedup_gardener_max_merges_per_run` | `8` | Cap per pass. |
| `memory_active_capacity` | `0` (unbounded/off) | Cap on "active" memory count before older ones archive. |
| `capacity_max_archivals_per_run` | `50` | Cap per capacity-enforcement pass. |
| `granularity_freshness_gardener_enabled` | `true` | Background staleness/granularity pass. |

`project_prompt_injection` / `relevant_recall` / `relevant_recall_rerank` /
`project_first_dream` can also be flipped via env vars — see
[Environment variables](#environment-variables) — which is handy for a
one-off container run without touching `config.json`.

`auto_dream_enabled`/`gardener_enabled` intentionally consume model tokens
when on; turn them off (`{"memory": {"auto_dream_enabled": false}}`) for a
minimal-cost deployment.

## Sub-agents + the `claude_code` executor

Key `subagents` (`SubagentsConfig`). Sub-agents always run as independent
actor subprocesses (crash isolation + real parallelism) — there is no
in-process runtime toggle.

| Field | Purpose |
|---|---|
| `max_concurrent` | Cap on simultaneously running sub-agents. |
| `worker_bin` / `worker_args` | Override the sub-agent worker binary/args (defaults to the current `bamboo` binary's `subagent-worker` mode). |
| `fabric_dir` | Where the actor fabric's mailbox/state files live. |
| `executor` | Which executor spawns a child: `"echo"` (test stub) \| `"bamboo_runtime"` (default — a full nested Bamboo agent loop) \| `"claude_code"` (shell out to the `claude` CLI instead). |
| `claude_code_binary` | Path to the `claude` binary; `None` resolves `claude` via `PATH`. |
| `claude_code_model` | `--model` passed to `claude`. |
| `claude_code_permission_mode` | `--permission-mode` passed to `claude` (always sent explicitly, even `"default"`, once this executor is selected). |
| `claude_code_inherit_user_config` | `false`/unset adds `--strict-mcp-config --setting-sources project`, sandboxing the child from your personal `claude` config. |
| `claude_code_forward_env` | Extra environment variable **names** forwarded verbatim into the child (on top of a fixed allowlist: `HOME`/`PATH`/`SHELL`/`TERM`/`LANG`/`LC_*`/`TMPDIR`/`USER`/`LOGNAME`). |
| `remote_placements` / `schedulable_placements` | Where a sub-agent may run (local / a named Cluster Fabric node) and whether schedules may target it. |
| `mcp_role_allowlist` | Restrict which MCP servers a sub-agent role may see. |

```json
{
  "subagents": {
    "executor": "claude_code",
    "claude_code_model": "claude-sonnet-4-6",
    "claude_code_permission_mode": "acceptEdits",
    "claude_code_forward_env": ["MY_TOOL_TOKEN"]
  }
}
```

The same `claude_code_*` field set is duplicated per-agent under
`ExternalAgentProfile` (`Config.extra["externalAgents"]`,
`bamboo-engine/src/external_agents/config.rs`) when you need different
`claude` executor settings for different named external agents rather than
one global default.

The concrete spawn implementation is `src/claude_code_executor.rs`
(`ClaudeCodeExecutor`): it runs `claude --output-format stream-json
--input-format stream-json --permission-prompt-tool stdio
--replay-user-messages --verbose [--model ...] [--permission-mode ...]
[--resume <id>]`, in a fully `env_clear()`'d child process (only the allowlist
above is passed through) — the session id maps to `claude`'s own `--resume`
via a small `claude-code-session.json` state file per sub-agent workspace.

## MCP servers

On-disk key `mcpServers` (legacy `mcp` alias still read), typed field
`Config.mcp: McpConfig`:

```json
{
  "mcpServers": {
    "version": 1,
    "servers": [
      {
        "id": "filesystem",
        "name": "Local filesystem",
        "enabled": true,
        "transport": {
          "type": "stdio",
          "command": "npx",
          "args": ["-y", "@modelcontextprotocol/server-filesystem", "/path/to/allow"]
        },
        "request_timeout_ms": 60000,
        "healthcheck_interval_ms": 30000,
        "allowed_tools": [],
        "denied_tools": []
      }
    ]
  }
}
```

`transport` is one of three shapes (tagged by `type`): `stdio`
(`command`/`args`/`cwd`/`env`/`startup_timeout_ms` — spawns a child process),
`sse` (`url`/`headers`/`connect_timeout_ms`), or `streamable_http` (same shape
as `sse`, MCP's newer single-endpoint transport). `reconnect` controls
auto-reconnect backoff (`enabled`, `initial_backoff_ms`, `max_backoff_ms`,
`max_attempts`, 0 = unlimited). `allowed_tools`/`denied_tools` filter which of
the server's advertised tools are actually exposed (empty `allowed_tools` =
all allowed). See the [`bamboo mcp` CLI verbs](../README.md#other-subcommands)
for managing this without hand-editing JSON, or
[`examples/mcp_client.rs`](../examples/mcp_client.rs) for wiring a server
programmatically via the SDK.

## Notifications

Key `notifications` (`NotificationsConfig`):

```json
{
  "notifications": {
    "desktop": { "enabled": true },
    "ntfy": { "enabled": true, "base_url": "https://ntfy.sh", "topic": "my-bamboo-alerts", "token": "..." },
    "bark": { "enabled": false, "base_url": "https://api.day.app", "device_key": "..." }
  }
}
```

`desktop.enabled: Option<bool>` — `None` auto-detects (on for a standalone
`bamboo serve`, off when running under a `--parent-pid` sidecar, since the
host app usually owns notifications there). `ntfy`/`bark` are push-relay
channels; `ntfy.token`/`bark.device_key` are secrets, encrypted at rest as
`token_encrypted`/`device_key_encrypted`. All three channels feed the same
`AgentEvent::Notification` category/priority policy — see
`crates/infra/bamboo-notification`.

## `connect` — the IM bridge

Drives sessions from IM platforms (Telegram, Feishu/Lark). **Despite `Config`
having a typed `connect` field, this is NOT stored in `config.json`** — it
lives in its own sibling file, `${data_dir}/connect.json`, loaded/merged by
`Config::merge_connect_config` and saved by `Config::save_connect_config`
(both in `config.rs`). A `config.json` with no `connect.json` next to it and
no legacy inline `connect` key starts **zero** background tasks — fully inert
by default.

```json
{
  "platforms": [
    {
      "id": "b3f5...",
      "type": "telegram",
      "token": "123456:ABC-DEF...",
      "allow_from": ["123456789"],
      "admin_from": []
    }
  ]
}
```

Fields (`ConnectPlatformConfig`): `id` (stable UUID, auto-backfilled on save —
never assume it's present on a hand-written entry), `type` (`"telegram"` \|
`"feishu"`; unrecognized values are skipped with a startup warning, not a hard
failure), `token`/`token_encrypted` (bot token, Telegram), `app_id` (Feishu,
not a secret), `app_secret`/`app_secret_encrypted` (Feishu), `domain` (Feishu
only — `None`/`"feishu"` → `open.feishu.cn`, `"lark"` → `open.larksuite.com`,
or an explicit `https://` base for self-hosted deployments), `allow_from`
(**empty = deny-all** — deliberately stricter default than other allowlists in
this codebase, since IM bridges are internet-facing by nature), `admin_from`
(parsed, currently unused).

A legacy inline `connect` key found inside `config.json` (from before this was
split out) is migrated automatically on next load: adopted into
`connect.json`, then stripped from `config.json`. A corrupt `connect.json` is
quarantined to `connect.json.bak` and treated as empty (fail-safe — never
silently falls back to a stale inline copy).

## `plugin_trust`

Key `plugin_trust` (`PluginTrustConfig`) — the trust policy for `bamboo
plugin install <url>` (see [Plugins how-to](guides/PLUGINS.md)):

```json
{
  "plugin_trust": {
    "trusted_hosts": ["github.com/bigduu/"],
    "trusted_keys": [
      { "label": "nova official", "algorithm": "ed25519", "public_key": "<hex>" }
    ],
    "enforcement": "strict"
  }
}
```

`trusted_hosts` — host+path prefixes a `url`-source install's URL must match
to skip `--allow-untrusted-host`. `trusted_keys` — ed25519 public keys (hex)
trusted to sign plugin bundles (defaults ship the official nova + magpie
keys); a bundle signed by one of these skips `--allow-unsigned` AND, per the
trust model, also satisfies the checksum requirement (a verified signature is
strictly stronger than a pasted `sha256`). `enforcement` — `"strict"`
(default) or `"off"` (accepts a bool too: `true`==strict, `false`==off);
`"off"` is the config-level equivalent of passing `--insecure` to every `url`
install, for a private/dev instance that never wants confirmation prompts.
Local (`local_dir`/`local_archive`) installs are never subject to this policy
— it only gates network downloads.

## Keyword masking

Key `keyword_masking` (`KeywordMaskingConfig { entries: Vec<KeywordEntry> }`,
each `{ pattern, match_type: "exact" | "regex", enabled }`). Applied as a
value-aware scan over the FINAL serialized outbound provider request body
(not field-by-field) — every string value matching a pattern is masked before
the request leaves the process, catching secrets that end up embedded in tool
output, file contents, etc., not just ones typed directly into chat.

## Permissions

Lives under the `"permissions"` key inside `Config.extra` (the flatten
catch-all — not yet promoted to a typed top-level field). Shape
(`SerializablePermissionConfig`, `crates/infra/bamboo-permission/src/config.rs`):
`whitelist: Vec<PermissionRule>`, `enabled: bool`, `session_grant_duration_secs`
(default `1800`), `mode: Option<PermissionMode>`, `confirm_threshold:
Option<RiskLevel>`, `ask_rules: Vec<String>` — glob-ish patterns like
`"Bash(rm -rf *)"` that force a confirmation prompt even under a bypass
permission mode. The design invariant: bypass mode means "run everything
without prompting" *except* the user's own `ask_rules` and a small hard-coded
set of catastrophic commands (`sudo`, `curl | sh`, `dd`, `rm -rf /`, …), which
always prompt regardless of mode.

## Model limits (`model_limits.json`)

A separate file, `${data_dir}/model_limits.json` — user-supplied
context/output token limit overrides, consulted only when provider runtime
metadata doesn't already know a model's limits:

```json
[
  { "model_pattern": "my-custom-model-*", "max_context_tokens": 128000, "max_output_tokens": 8192 }
]
```

`model_pattern` matches against the model id (glob-style), `safety_margin` is
an optional token buffer subtracted from the limit. With no match anywhere
(provider metadata nor this file), Bamboo falls back to a global default of
200K context / 64K output — deliberately with **no built-in per-model table**,
so the fallback never goes stale as models are updated upstream. Manage this
via `PUT`/`DELETE` on the model-limits HTTP routes rather than hand-editing,
if you're running the server.

## Schedules (`schedules.json`)

Also a separate file, `${data_dir}/schedules.json` (not part of
`config.json`) — timed/cron tasks managed by `bamboo schedules
list|show|create|delete|run|runs` or the `/bamboo/schedules` HTTP routes. Each
entry (`ScheduleSpec`) has an `id`/`name`/`enabled`, a `trigger` (`Interval`
\| `Once` \| `Daily` \| `Weekly` \| `Monthly` \| `Cron`), an optional
`timezone`, `start_at`/`end_at` bounds, a `misfire_policy` (what happens if
the process was down when a fire was due: `RunOnce` (default) \| `Skip` \|
`CatchUpAll` \| `CatchUpWindow`), an `overlap_policy` (`Allow` \| `Skip` \|
`QueueOne`, default `QueueOne`), and `run_config` (the prompt/session
parameters for the fired run). Not meant for hand-editing — use the CLI/HTTP
verbs, which validate the trigger shape.

## Environment variables

Every `BAMBOO_*` variable Bamboo reads, grouped by what it affects. All are
optional; file config plus built-in defaults cover a fresh install.

**Bootstrapping / core:**

| Var | Effect |
|---|---|
| `BAMBOO_DATA_DIR` | Data directory (default `${HOME}/.bamboo`). |
| `BAMBOO_PORT` | Server port override. |
| `BAMBOO_BIND` | Server bind address override. |
| `BAMBOO_PROVIDER` | Default provider override. |
| `BAMBOO_HEADLESS` | Enable headless auth mode. |
| `BAMBOO_WORKERS` | Actix worker-count override (CLI-level). |

**Provider API keys** (in-memory only — never persisted to `config.json`,
even after `bamboo config set`; the point is a plaintext-key-free config file
for Docker/CI/secret-manager deploys):

`BAMBOO_OPENAI_API_KEY`, `BAMBOO_ANTHROPIC_API_KEY`, `BAMBOO_GEMINI_API_KEY`.

**Memory toggles** (override the matching `memory.*` config field):
`BAMBOO_MEMORY_PROJECT_PROMPT_INJECTION`, `BAMBOO_MEMORY_RELEVANT_RECALL`,
`BAMBOO_MEMORY_RELEVANT_RECALL_RERANK`, `BAMBOO_MEMORY_PROJECT_FIRST_DREAM`.

**Server hardening / networking:**

| Var | Effect |
|---|---|
| `BAMBOO_RATE_LIMIT_PER_SECOND` / `BAMBOO_RATE_LIMIT_BURST` | Governor rate-limiter tuning. |
| `BAMBOO_RATE_LIMIT_TRUST_XFF` / `BAMBOO_RATE_LIMIT_TRUSTED_HOPS` | Trust `X-Forwarded-For` behind N reverse-proxy hops. |
| `BAMBOO_CSP` | Full Content-Security-Policy header override. |
| `BAMBOO_CSP_CONNECT_SRC` | Just the CSP `connect-src` directive. |
| `BAMBOO_CORS_ALLOW_ORIGINS` | CORS allowlist. |
| `BAMBOO_ENABLE_DEV_ENDPOINTS` | Gate dev-only HTTP endpoints. |
| `BAMBOO_WS_AUTH_DEADLINE_MS` | WS v2 auth handshake timeout. |

**Workspace / paths:**

| Var | Effect |
|---|---|
| `BAMBOO_WORKSPACE_DIR` | Project/workspace directory override. |
| `BAMBOO_WORKSPACE_ROOT` | Root dir for session workspaces (default `{data_dir}/workspaces`). |
| `BAMBOO_WORKSPACE_CONFINE` | `1`/`true`/`yes` forces workspace paths to stay under `BAMBOO_WORKSPACE_ROOT`; implied when that var is set. |
| `BAMBOO_SKILL_MODE` | Active skill mode override. |

**Provider/runtime tuning:**

| Var | Effect |
|---|---|
| `BAMBOO_LLM_MAX_RETRIES` / `BAMBOO_LLM_RETRY_BASE_DELAY_MS` / `BAMBOO_LLM_RETRY_MAX_DELAY_MS` | LLM HTTP request retry policy. |
| `BAMBOO_RESPONSES_DEBUG` / `BAMBOO_RESPONSES_DEBUG_FILE` | Dump raw OpenAI Responses API traffic to a file for debugging. |
| `BAMBOO_JS_REPL_NODE_PATH` | Node binary used by the `js_repl` tool. |
| `BAMBOO_PYTHON` | Python interpreter override. |

**Windows-specific:** `BAMBOO_WINDOWS_BASH_PATH`, `BAMBOO_WINDOWS_CMD_TRACE`
(also honors `BODHI_WINDOWS_CMD_TRACE`).

**Secrets / plugins / broker:**

| Var | Effect |
|---|---|
| `BAMBOO_CONFIG_ENCRYPTION_KEY` | Master AES-256 key for at-rest secret encryption — see [Encryption at rest](#encryption-at-rest). |
| `BAMBOO_BROKER_TOKEN` | Auth token for `bamboo broker`/`broker-agent` subcommands. |
| `BAMBOO_PLUGIN_SERVICE_CONFIG` | Config path passed into a plugin service's own subprocess. |
| `BAMBOO_FRONTEND_PACKAGE` | Override the bundled frontend static package path. |

Everything above is read via plain `std::env::var`, so it can also be set
through your process manager / Docker Compose / systemd unit rather than
exported in a shell.

## Secrets and masking

Every secret field (`providers.*.api_key`, `provider_instances.*.api_key`,
`notifications.ntfy.token`, `notifications.bark.device_key`,
`connect.platforms[].token`/`.app_secret`, `subagents.broker.token`,
`cluster_fabric` node SSH credentials, secret `env_vars` entries) follows one
contract everywhere it's read or written:

- **Read (`GET`/`bamboo config`):** a configured secret is never echoed back
  plaintext. It's replaced with exactly the literal string `****...****`; if
  the field isn't configured at all, the key is omitted entirely (not sent as
  `""`).
- **Write (`PATCH`/`bamboo config set`):** a submitted value counts as **"keep
  the existing secret unchanged"** if and only if, after trimming, it consists
  **entirely of `*` and/or `.` characters** — i.e. it matches the masked
  placeholder shape exactly. This is a whole-value check, not a substring
  check: `is_masked_api_key()` in `crates/infra/bamboo-config/src/patch.rs`.
  An **empty string** explicitly clears the secret. Anything else — including
  a string that still starts with the placeholder because a UI's prefill
  wasn't fully cleared before pasting (e.g. `****...****sk-newkey123`) — is
  treated as a **real new secret** and applied.

  This whole-value rule is deliberate: an earlier substring-based check (fixed
  as issue #430) could silently discard a user's pasted key when the
  placeholder wasn't fully selected/overwritten first. Any client embedding
  Bamboo's settings UI must never pre-fill an editable secret field with the
  masked placeholder — leave it blank to mean "keep."

## Encryption at rest

Every `*_encrypted` field uses AES-256-GCM (`crates/infra/bamboo-config/src/encryption.rs`);
on disk the ciphertext is stored as `hex(nonce):hex(ciphertext)`, a fresh
random nonce per encryption. The master key is resolved once per process, in
priority order:

1. **`BAMBOO_CONFIG_ENCRYPTION_KEY`** — hex-encoded, must decode to exactly 32
   bytes. Highest priority; use this for reproducible/ephemeral deployments
   (containers, CI) where you manage the key externally.
2. **Key file** `${data_dir}/.bamboo_encryption_key` — hex-encoded 32 bytes,
   written with `0600` permissions atomically on Unix.
3. **Machine-derived key** — SHA-256 of a machine identifier (`/etc/machine-id`
   on Linux, registry `MachineGuid` on Windows, `ioreg IOPlatformUUID` on
   macOS) with domain separation, then **persisted to the key file** so
   subsequent runs don't re-derive it.
4. **Last resort** — cryptographically random 32 bytes, persisted to the key
   file.

**Backup/disaster-recovery implication:** losing the key file on a host with
no stable machine identifier (and no `BAMBOO_CONFIG_ENCRYPTION_KEY` set) makes
every `*_encrypted` field in that data directory permanently undecryptable —
back up `.bamboo_encryption_key` alongside `config.json` if you back up your
Bamboo data directory at all.

## Corrupt-config recovery

If `${data_dir}/config.json` exists but fails to parse, `Config::from_data_dir`
does not crash or silently reset to defaults — it runs a recovery flow
(issue #493 and predecessors), roughly:

1. **Quarantine** the unparseable original by *copying* it to
   `config.json.corrupted.<timestamp>` (the corrupt file is never deleted or
   moved — the original stays at `config.json` untouched).
2. **Recover**, trying strategies in order:
   - **Salvage** — parse the corrupt file as generic JSON and adopt each
     top-level key individually onto the best available baseline, keeping a
     key only if the whole `Config` still deserializes with it applied.
   - **Backup** — if salvage found nothing usable, fall back to
     `config.json.bak`, `.bak.1`, `.bak.2` (newest first; 3 generations are
     kept, rotated on every successful save).
   - **Defaults** — if neither works, a fresh default `Config`.
3. The recovered config is tagged in memory (`recovery_status`, never
   persisted) with which strategy produced it and which fields were salvaged.
4. **The recovered config is never auto-saved.** `Config::save_to_dir` refuses
   to write while recovery is unconfirmed, so the quarantined corrupt original
   on disk is preserved until something explicitly calls
   `Config::confirm_recovery()` / `confirm_recovery_and_save_to_dir()` (the
   settings UI/CLI does this after showing the user what was recovered).

Net effect: a corrupted `config.json` never causes silent data loss — you
always get either your own values back (salvage/backup) or an explicit,
confirmable prompt before anything is overwritten.
