# Immutable Session context files

`session_history` supports `action=export_context` for a persisted Root caller.
It creates small Markdown files for one selected session in that Root's tree,
so the caller can read status first and continue with selected task previews.
This is an on-demand read projection. It does not start, modify, cancel or
approve work, and it does not provide global supervisor authority.

```json
{"action":"export_context","session_id":"owned-child-123"}
```

The receipt includes `schema_version`, `revision`, `source_digest`, `scope`,
`manifest_path`, `status_path`, `brief_path`, and per-file byte/line counts and
SHA-256 hashes. `reused` indicates that the same immutable content was already
present. Pass the returned absolute paths to `Read`, for example:

```json
{"file_path":"<returned status_path>","offset":0,"limit":12}
```

Keep that path/revision for subsequent offsets. Reading a newly exported
revision between offsets could combine observations from different times.
Read limits the content returned to the model; its implementation reads the
whole bounded file before slicing lines. This does not claim partial disk I/O.

## Scope and source

The caller comes from trusted `ToolCtx`, and both caller and target are loaded
with `Storage::load_runtime_control_plane`. The caller must be a Root with its
own logical root. The target must be that Root or a Child with the same logical
root, and their optional valid Project identities must match exactly. Assigned
and Unassigned sessions do not match. Invalid persisted identities fail closed.
The action accepts only `action` and `session_id`; no caller, grant or output
path can be supplied. Existing history actions retain their existing behavior.
Child tool surfaces still exclude `session_history`.

The exporter requests the control-plane API only; it never calls `load_session`
or `save_session` itself. With a valid runtime sidecar, SessionStoreV2 reads
that sidecar without loading the transcript. Its existing compatibility path
may read `session.json` when the runtime sidecar is missing or corrupt and then
clear messages. That storage behavior is unchanged: this feature guarantees
the exported allowlist, not the absence of all physical transcript I/O on legacy
data.

The explicit source allowlist is title, tree/Project identity, creation/update
timestamps, title/metadata versions, a recognized last persisted run status,
whether a pending question exists, and structured task title/ID/description/
status. Unknown status strings render as `unknown`. Messages, raw metadata,
system prompts, summaries, question payloads, task notes/evidence, credentials,
tool arguments, and workspace configuration are not exported. Arbitrary free
text in allowed title/task fields remains observed data, never authority.

`status.md` labels its state as the last persisted observation. It does not
consult the live runner registry and cannot prove that a persisted `running`
status is still current. The source timestamp is the saved Session's update
time, not a claim about when the exporter observed a live run.

`brief.md` is a bounded structured-task preview, not a generated summary or a
complete delegation contract. Truncation is explicit; required instructions
must be obtained from the original task before acting on a preview.

## Publication and budgets

Files are published under the configured Bamboo home:

```text
coordination/session-context/v1/<root-id-sha256>/<revision>/
  status.md
  brief.md
  manifest.json
```

No raw session ID is interpolated into the output path. The revision hashes the
schema and selected safe source, including checksums for abbreviated text.
The manifest records scope, source digest/time, filenames and content hashes.
Repeated exports of identical source data reuse and verify the existing files.
A changed source produces a new revision without rewriting previous snapshots.

Limits are 8 KiB/40 lines for status, 16 KiB/120 lines for the brief, 8 KiB for
the manifest, and 512 UTF-8 bytes per Markdown line. At most 32 structured tasks
are shown. Titles and task fields are escaped, flattened to one line and
shortened on UTF-8 boundaries; the brief indicates omitted content.

Publication is serialized per Root across cooperating exporter processes.
Files are flushed in a private staging directory, the complete manifest is
written last, and a directory rename exposes the bundle. The tool returns
paths only after publication. It never edits canonical Session state or inbox.
This is a rebuildable projection, not a new crash-recovery protocol.

There is a hard limit of **64 snapshots per Root**, across all its exported
targets. Reusing an existing complete revision remains possible at the limit;
new revisions return `context_snapshot_quota`. There is no automatic eviction
or hidden deletion of referenced history. Symlinked output components, changed
immutable files, partial snapshots and abandoned publication directories fail
explicitly. The operator must resolve such errors; the tool does not repair
canonical state or silently discard old views. Failed calls may remove only
their own unpublished staging directory.

These paths are local files under the existing Bamboo data boundary, not new
read grants or an OS sandbox. The exporter rejects symlinked output components
below its trusted configured home; it does not claim isolation from an actor
with arbitrary concurrent filesystem access as the same OS user. Cross-Project
supervisor views, restricted child grants, subscriptions, live state, check-
points and Plan artifacts require separate capabilities.
