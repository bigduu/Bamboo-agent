"""Read actual Bamboo tool names/inputs; contribute a bounded advisory note."""
import json
import sys

payload = json.load(sys.stdin)
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "additionalContext": "Example plugin observed tool " + payload["tool_name"]
    }
}))
