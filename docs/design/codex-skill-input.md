# Skill bundle metadata input

Bamboo uses the OpenAI Codex metadata parser pinned at
`7f892275e31002f0422477c6219189284560e689` for ordinary `SKILL.md` input.
The bundle directory remains a safe, stable kebab-case ID. Its display name may
be Unicode, contain spaces, or differ from the directory. An absent or blank
name falls back to the directory ID. Names are limited to 64 Unicode characters.
Descriptions are required and normalized to one line, without a parse-time
length limit or angle-bracket restriction. Unknown frontmatter is tolerated.
The parser applies Codex's narrow line-oriented YAML scalar repair.

`license`, `compatibility`, arbitrary `metadata`, and instruction content remain
available through the existing Skill definition JSON. Optional descriptive
license/compatibility values with unsupported shapes are ignored. The optional
`short_description` JSON field defaults to absent for older payloads.
`metadata.short-description` is normalized for that field while the original
metadata value is retained. A valid `interface.short_description` in
`agents/openai.yaml` supplies the fallback when frontmatter has no summary;
blank or over-1024-character interface summaries are ignored.
Instruction parsing preserves interior bytes, including CRLF, and keeps the
existing outer whitespace trim convention. Files are not rewritten on discovery.

## Invocation and host boundaries

A valid `policy.allow_implicit_invocation: false` in `agents/openai.yaml`
disables automatic invocation. A true value cannot enable a host-denied
invocation. Invalid optional OpenAI YAML/policy is ignored as in Codex. Invalid
optional descriptive interface fields do not discard a valid implicit deny.

`workflow.yaml` owns orchestration classification, version and argument schema.
Restrictions from `agents/bamboo.yaml` are also applied when a workflow file is
present. Invocation permissions are intersected; legacy manual-only input only
disables automatic invocation and preserves an explicit deny. Malformed Bamboo
host policy retains the existing invalid/last-known-good publication behavior.

Bamboo additionally validates its safe managed IDs, `allowed-tools` (or
`allowed_tools`) scalar/sequence forms, and recognized host control flags in
metadata. Scalar tool lists split at whitespace or commas and use existing host
normalization; unknown references are preserved and do not grant tools.
YAML repair does not quote malformed tool restrictions or a malformed metadata
container into harmless prose. This is a deliberate host boundary: acceptance
by the Codex metadata parser alone does not make an unsafe Bamboo bundle valid.

Optional OpenAI sidecar reads use the existing descriptor-confined `agents/`
reader on Linux/macOS. Other platforms use the existing bounded Skill resource
inventory and no-follow file reader. Linked files or linked `agents/` directories
cannot supply optional metadata. Sidecar reads retain the host per-file bound.

Source selection, mode overrides, refresh and metadata publish through the
existing same-generation SkillStore snapshots. These input changes do not
alter selected-content reading, activation, orchestration execution, host tool
permission rules, or Root Ultra's Skill/Workflow restrictions.

The copied parser, six upstream fixtures and metadata projection retain the
[Codex license and source notices](../../THIRD_PARTY_NOTICES.md).
