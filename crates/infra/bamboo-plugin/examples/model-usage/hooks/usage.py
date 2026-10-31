"""Store a small, deduplicated usage sample in the plugin's own data directory."""
import json
import os
import sqlite3
import sys
from pathlib import Path

payload = json.load(sys.stdin)
observation = payload.get("observation", {})
if payload.get("hook_event_name") != "BambooModelCallUsage" or observation.get("schema_version") != 1:
    raise SystemExit("unsupported usage event")
usage = observation.get("usage") or {}
data_dir = Path(os.environ["CLAUDE_PLUGIN_DATA"])
data_dir.mkdir(parents=True, exist_ok=True)
with sqlite3.connect(data_dir / "usage.sqlite3", timeout=1) as db:
    db.execute("CREATE TABLE IF NOT EXISTS calls (event_id TEXT PRIMARY KEY, input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cache_creation_tokens INTEGER, cache_write_tokens INTEGER, reasoning_tokens INTEGER)")
    db.execute("INSERT OR IGNORE INTO calls VALUES (?, ?, ?, ?, ?, ?, ?)", (
        observation["event_id"], usage.get("input_tokens"), usage.get("output_tokens"),
        usage.get("cache_read_input_tokens"), usage.get("cache_creation_input_tokens"),
        usage.get("cache_write_input_tokens"), usage.get("reasoning_tokens"),
    ))
    db.execute("DELETE FROM calls WHERE rowid NOT IN (SELECT rowid FROM calls ORDER BY rowid DESC LIMIT 1000)")
print("{}")
