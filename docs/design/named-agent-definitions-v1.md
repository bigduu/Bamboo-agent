# Global named-agent definitions v1

Issue #1326 adds a host-only parser/catalog in `bamboo-skills::named_agents`.
`NamedAgentCatalog::load_configured()` reads the stabilized global Bamboo data
directory (`bamboo_config::paths::bamboo_dir()`) and its immediate `agents/*.md`
children. `discover(data_root, limits)` accepts a trusted host configuration root.
There is no public HTTP endpoint and no automatic application to a SubAgent.

## Contract

```markdown
---
schema_version: 1
name: rust-reviewer
description: Reviews Rust changes
model_hint: provider:model-1
tools:
  allow: [Read, mcp__files__read]
  deny: [Write]
---
Review the assigned patch and report concrete findings.
```

- UTF-8; LF or CRLF; the first line and closing frontmatter line are exactly
  `---`. A nonempty system-prompt body follows. Its surrounding whitespace is
  trimmed; no includes or template expansion occur.
- `schema_version` is the integer `1`. Unknown versions have an inspectable
  `unsupported_schema_version` diagnostic. Wrong types, missing/duplicate fields,
  unknown fields, YAML aliases/anchors/tags and malformed YAML are rejected.
- `name` is exact, case sensitive, and independent of the filename: 1–64 ASCII
  bytes, starting with a lowercase letter, then lowercase letters, digits, `_`
  or `-`. Names are neither normalized nor aliased.
- `description` is a nonempty, trimmed plain display string of at most 512 UTF-8
  bytes. Controls, newlines, bidi embedding/isolation controls and `/` or `\` are
  rejected. Thus descriptions cannot contain physical paths or URL destinations.
- `model_hint` is optional. It and each tool identifier are 1–128 ASCII bytes,
  starting with a letter/digit, then letters, digits, `_`, `-`, `.`, or `:`. An
  endpoint, credential or filesystem path is not a model hint.
- `tools` is optional; `allow`/`deny` default to empty lists, each with at most 32
  distinct identifiers. Duplicate entries or overlap between lists are invalid.
  These are declarations only: they grant no permission and resolve no tool.
- Frontmatter is at most 16 KiB, file contents at most 64 KiB, and the trimmed
  prompt body at most 48 KiB. Limits are byte counts, not character/token counts.

## Discovery and resource boundary

The initial capability reader supports **macOS and Linux**. Windows and other
platforms return `unsupported_platform` with no definitions or metadata rows;
they are not reported as malformed. Windows handle-based reading is tracked in
#1336. This module makes no all-platform safety claim.

The configured root must be absolute and contain no parent traversal. Every
ancestor from `/`, the `agents` directory, and each final candidate are opened
with no-follow semantics. Each child is opened relative to the retained parent
directory descriptor. Enumeration is tied to that descriptor too. There is no
path check followed by an unguarded reopen. If a configured ancestor is itself a
symlink (for example macOS `/var`), configure its real physical path instead.
The catalog does not canonicalize a symlink into an accepted location.

Only immediate, case-sensitive `.md` candidates are considered. Nested trees
are not traversed. Symlink files, directories, FIFOs and other nonregular files
are rejected; reads use nonblocking opens before checking the opened file type.
A missing `agents` directory is an empty catalog; an unavailable/unsafe global
root is a typed catalog rejection.

Hard ceilings are 128 candidates, 1,024 scanned directory entries (including
non-Markdown entries), and 1 MiB aggregate bytes. A caller can tighten these
limits but cannot raise them. Reads are bounded by both the file ceiling and
remaining aggregate budget, plus one byte solely to detect overflow. Actual
bytes read count, including malformed/secret sources and files that grow after
stat. Declared oversized files are rejected without reading content. The
aggregate budget also bounds serialized metadata plus retained definition
strings. Exceeding candidate, scan or aggregate limits rejects the entire
publication; there is no arbitrary partial winner or retained previous snapshot.

## Invalid, conflict and metadata views

Each invalid candidate produces an anonymous `invalid` row with a static
diagnostic code. Filenames, paths, parser errors and source excerpts are absent.
All valid candidates claiming a duplicate name become `conflict` rows with
`duplicate_name`; none is available through exact-name lookup. A valid definition
has a SHA-256 revision of the original file bytes, so even a line-ending change
changes the identity.

The serializable catalog view is `NamedAgentCatalogMetadata`: safe name,
description, revision, status and diagnostic code, plus an optional catalog
diagnostic code. Definitions retain their prompt, routing hint and tool
declarations in host memory, have no serialization implementation, and their
Debug output is restricted to the same safe metadata. The catalog stores no
physical paths. Metadata is internal inspection/display data, never model-facing
instructions or permission authority.

## Credential boundary and deferred work

The parser rejects credential-shaped literal fields such as `api_key`,
`client_secret`, `password`, `authorization` and `private_key` in frontmatter or
prompt assignments, recognizable private-key blocks and selected familiar token
families (OpenAI-style, GitHub, AWS access IDs and Slack tokens). It rejects the
whole definition without echoing its name, description, secret value or source.
Both original source and every decoded YAML scalar/list value are checked, so
quoted escape sequences do not bypass the credential check.
This is a conservative syntax check, **not detection of every secret embedded in
arbitrary prose**. Do not put credentials or sensitive material in these files.

Project/plugin precedence, last-known-good reload, profile application and host
permission intersection (#909), curated default roles (#1315), UI and HTTP
surfaces remain separate work under #912. There is no watcher, disk writer,
additional persistence protocol or runtime model-routing decision in this slice.
