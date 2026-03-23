# Jira API Workflows Reference

Read this file when the user needs practical Jira REST execution patterns, reusable API flows, or explicit examples for create, update, comment, transition, subtask, or summary-related reads. This file complements the main skill and the Windows PowerShell reference by focusing on task-oriented API sequences rather than just isolated payload snippets.

## When to use this reference

Use this file when the user wants to:
- Actually create or update Jira issues through REST APIs
- Add comments or transition workflow states safely
- Create subtasks under a parent issue
- Build JQL queries for personal, pod, or team summaries
- Understand the minimal read-before-write pattern for Jira updates
- See cross-platform request strategy at the workflow level

## Core read-before-write principle

For Jira updates, do the minimum safe reads first.

```mermaid
flowchart TD
    A[Need Jira change] --> B{Change type}
    B -->|Create new issue| C[Collect required fields and build payload]
    B -->|Update existing issue| D[Read current issue or relevant fields]
    B -->|Transition status| E[Read available transitions]
    B -->|Summary/report| F[Run bounded JQL search]
    D --> G[Plan minimal patch]
    E --> H[Choose transition ID]
    G --> I[Apply update]
    H --> I
    C --> I
    F --> J[Synthesize result]
```

## Common Jira REST endpoints

These are the main endpoints this skill is expected to use conceptually. Local Jira instances may differ slightly.

- Read one issue: `GET /rest/api/2/issue/{issueKey}`
- Search issues with JQL: `GET /rest/api/2/search` or `POST /rest/api/2/search`
- Create issue: `POST /rest/api/2/issue`
- Update issue fields: `PUT /rest/api/2/issue/{issueKey}`
- Add comment: `POST /rest/api/2/issue/{issueKey}/comment`
- List transitions: `GET /rest/api/2/issue/{issueKey}/transitions`
- Apply transition: `POST /rest/api/2/issue/{issueKey}/transitions`

Use `/rest/api/3/...` if the user’s Jira Cloud instance prefers that version. Do not silently switch versions without checking the environment.

## Authentication guidance

Prefer environment variables and avoid echoing secrets.

### Bash pattern

```bash
curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  -H "Accept: application/json" \
  "$JIRA_BASE_URL/rest/api/2/issue/PROJ-123"
```

### PowerShell pattern

For Windows PowerShell, read `references/windows-powershell.md` and prefer `Invoke-RestMethod` plus an explicit Authorization header.

## Workflow 1: Create a new issue

Use when the user wants to create a bug, story, task, epic, or subtask.

### Minimum fields
- Project key
- Issue type
- Summary
- Description or source notes
- Parent issue key if subtask

### Bash create example

```bash
payload=$(python3 - <<'PY'
import json
print(json.dumps({
  "fields": {
    "project": {"key": "PLAT"},
    "issuetype": {"name": "Story"},
    "summary": "Automate pod-level weekly Jira summary",
    "description": "Goal\n\nCreate a reusable pod-level Jira reporting workflow.",
    "labels": ["jira-automation", "reporting"]
  }
}))
PY
)

curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -X POST "$JIRA_BASE_URL/rest/api/2/issue" \
  -d "$payload"
```

### Create checklist
- Confirm whether this is draft-only or create-now
- Confirm project and issue type
- Normalize notes into clear summary plus description
- Include acceptance criteria when the request is feature or delivery oriented

## Workflow 2: Update issue fields

Use when the user wants to change summary, description, labels, assignee, priority, due date, story points, or similar fields.

### Recommended flow
1. Resolve issue key
2. Read relevant current fields if overwrite risk exists
3. Explain the intended delta
4. Apply minimal update
5. Return changed fields and next steps

### Bash update example

```bash
payload=$(python3 - <<'PY'
import json
print(json.dumps({
  "fields": {
    "summary": "Clarify OPS-132 scope and acceptance criteria",
    "labels": ["ops", "clarified"]
  }
}))
PY
)

curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -X PUT "$JIRA_BASE_URL/rest/api/2/issue/OPS-132" \
  -d "$payload"
```

### When to read first
Read first when:
- Rewriting description
- Replacing labels/components rather than appending
- Updating fields the user might want preserved
- The user asked for “make this clearer” rather than a literal overwrite

## Workflow 3: Add a comment

Use when the user wants a progress note, blocker note, or stakeholder update without changing core issue fields.

### Bash comment example

```bash
payload=$(python3 - <<'PY'
import json
print(json.dumps({
  "body": "Status update\n- Progress made: clarified scope and updated acceptance criteria.\n- Blockers: waiting on platform confirmation.\n- Next: move to In Progress after review."
}))
PY
)

curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -X POST "$JIRA_BASE_URL/rest/api/2/issue/OPS-132/comment" \
  -d "$payload"
```

### Comment use cases
- Short execution update
- Handoff note
- Blocker announcement
- Human-readable summary after a larger change

## Workflow 4: Transition status safely

Do not guess transition IDs. They are workflow-specific.

### Step 1: Read available transitions

```bash
curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  -H "Accept: application/json" \
  "$JIRA_BASE_URL/rest/api/2/issue/OPS-132/transitions"
```

### Step 2: Apply chosen transition

```bash
payload='{"transition":{"id":"31"}}'

curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -X POST "$JIRA_BASE_URL/rest/api/2/issue/OPS-132/transitions" \
  -d "$payload"
```

### Transition checklist
- Confirm desired destination state
- Read transitions first
- Use the instance-specific ID
- If the state name is ambiguous, ask the user before executing

## Workflow 5: Create subtasks

Use when the user wants the work broken down under a known parent issue.

### Bash subtask example

```bash
payload=$(python3 - <<'PY'
import json
print(json.dumps({
  "fields": {
    "project": {"key": "PLAT"},
    "parent": {"key": "PLAT-101"},
    "issuetype": {"name": "Sub-task"},
    "summary": "Draft pod summary query and output structure",
    "description": "Create the first version of the pod-level Jira reporting workflow."
  }
}))
PY
)

curl -sS -u "$JIRA_EMAIL:$JIRA_API_TOKEN" \
  -H "Accept: application/json" \
  -H "Content-Type: application/json" \
  -X POST "$JIRA_BASE_URL/rest/api/2/issue" \
  -d "$payload"
```

### Subtask checklist
- Confirm parent issue key
- Confirm whether one parent plus multiple subtasks are needed
- Make subtask summaries concrete and individually actionable

## Workflow 6: Personal summary query pattern

Use for one person’s standup, daily update, or weekly summary.

### Typical JQL selectors
- `assignee = currentUser()`
- `assignee = "user@company.com"`
- `updated >= startOfDay()`
- `updated >= startOfWeek()`
- `sprint = 123`

### Example JQL

```text
project = PROJ AND assignee = currentUser() AND updated >= startOfDay() ORDER BY updated DESC
```

### What to extract
- Issues that moved forward
- Issues currently in progress
- Blockers or stalled items
- Suggested next actions

## Workflow 7: Pod summary query pattern

Use for a bounded pod or squad.

### Common pod scoping options
- project + label
- project + component
- project + assignee set
- board or sprint scoped query
- explicit user-provided JQL

### Example JQL

```text
project = PLAT AND labels = pod-alpha AND updated >= startOfWeek() ORDER BY priority DESC, updated DESC
```

### What to extract
- Highlights
- Shared blockers or dependencies
- Attention items
- Ticket movement themes
- Recommended next actions

## Workflow 8: Team summary query pattern

Use for a broader leadership or stakeholder view.

### Common team scoping options
- whole project
- current sprint
- multiple components
- specific board or release label

### Example JQL

```text
project = MOBILE AND sprint in openSprints() ORDER BY priority DESC, updated DESC
```

### What to extract
- Overall progress
- Major wins
- High-impact blockers
- Aging or due-soon work
- Escalations or asks

## Cross-platform workflow guide

```mermaid
flowchart TD
    A[Need Jira API workflow] --> B{User platform}
    B -->|macOS/Linux| C[Use Bash plus curl]
    B -->|Windows PowerShell| D[Read windows-powershell.md]
    C --> E[Build payloads with Python or jq]
    D --> F[Build payloads with hashtables plus ConvertTo-Json]
    E --> G[Execute create/update/search workflow]
    F --> G
```

## Practical safety rules

- Do not claim a create or update succeeded unless execution actually happened.
- If credentials or base URL are missing, stop and ask for the minimum required info.
- If the user wants a draft only, produce the draft and do not execute write calls.
- If the user requests a status transition, read transitions first instead of guessing IDs.
- If the request is summary-only, do bounded reads and synthesize rather than dumping raw JSON.
- If the user is on Windows, do not make Bash the default answer path.

## Guidance for the main skill

When the user asks for practical execution help rather than just planning:
- Read this file for end-to-end REST workflow guidance.
- Read `references/windows-powershell.md` as well if the user is on Windows.
- Read `references/templates.md` if the request also needs structured output wording for issue bodies or summaries.
