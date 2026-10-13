# Compressed output recovery files

When scenario compression saves at least 500 bytes, Bamboo can save a recovery
file under `~/.bamboo/tee/<session_id>/` and append a `Read` hint to the compressed
tool result. Each generated filename uses a timestamp and UUID; commands and
arguments never enter filenames. New files are private to the owner on Unix.

These are **secret-screened output files**, not a promise of byte-identical
secret recovery. Bamboo reuses its AutoDream credential predicate: ordinary
output is saved unchanged, including contact email addresses, phone numbers and
newlines. If an output item contains credential-like material (for example a
token, authorization value or private key), the entire saved item is an explicit
redaction marker. The hint says when this occurred. Screening cannot recognize
every possible secret format and does not rewrite the live tool result or the
canonical Session transcript.

Files expire after 30 days by default. Set `BAMBOO_TEE_RETENTION_DAYS` to a
positive whole number of days to change this; omitted, invalid, zero or overflowing
values use 30 days. A successful tee write schedules a background sweep at most
once per hour. The sweep streams immediate Session directories and removes only
expired regular `.log` files, using their modification times. It preserves
symlinks, fresh files and unrelated entries. It does not run when tee is unused,
so removal occurs on a later tee write rather than exactly at the expiry instant.

After cleanup, `Read` reports the ordinary missing-file result. Tee is temporary
tool-output recovery, separate from `session_history_current`, native provider
transcripts and Jiandu durable memory. Existing files receive expiry cleanup;
this change does not retrospectively sanitize them or securely erase filesystem
snapshots and backups.
