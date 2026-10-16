---
name: bamboo-manual
description: Explain this installed Bamboo version's configuration keys, memory and Jiandu recall, lifecycle or plugin hooks, providers, MCP, and troubleshooting. Use for Bamboo product questions, 配置 key、记忆原理、Hook 用法、排障, and evidence-based configuration diagnosis. Do not use as proof of a running instance's settings or to grant configuration changes, script execution, or repairs.
---

# Bamboo product manual

This is the offline manual embedded in this build. Read `manifest.json` first
and `references/index.md` to select a chapter. Use the package and resource
identifiers returned by `skills_list` / `skills_read`; follow `next_cursor` to
EOF. Preserve code blocks and whitespace. If a resource is unavailable or a
cursor expires, report that gap rather than assuming you read the whole file.

For legacy hosts that expose `load_skill` and `read_skill_resource`, load this
skill before reading resources. Request bounded line pages, follow
`next_offset`, and check `truncated`; reduce the line limit and reread that page
if it was truncated. A returned line boundary alone does not prove full bytes
were received.

1. Separate product behavior from observed instance state. This bundle is
   product knowledge only. It cannot tell you a user's provider, paths, memory,
   credential values, or what a live consumer has applied.
2. Locate key names in `references/config-keys.json`. It is an explicitly
   bounded, typed-tested catalog, not a complete JSON Schema. An absent key is
   unverified here, not necessarily unsupported. Read the owning chapter.
3. Cite the manifest's package version, source revision/status, content hash,
   and chapter path. Development version `0.0.0`, unknown revision,
   `checkout_unverified` source, dirty source archives, or missing dependency-lock
   identity must remain explicit. Never label
   this bundle the latest release or substitute online docs silently.
4. Give the explanation or next safe check. Only make an instance-specific
   diagnosis from authorized, current evidence; state its observation time,
   scope and coverage. Do not dump raw config, logs, memory, credentials or
   private paths into a model request to obtain that evidence.
5. Reading this manual does not authorize writes, restarts, reloads, migration,
   repair, rebuilding memory, connecting services, model calls, enabling hooks,
   or hook dry-runs. Ask separately where the user's permissions require it.

The source code paths listed by each chapter are review provenance. They are
not runtime dependencies and may be absent from a binary-only installation.
