# Confluence Server/Data Center REST API Cheat Sheet

Use this reference when the task is API-heavy and you want quick reminders instead of rereading the full skill.

## Common endpoints

| Purpose | Method | Example path |
| --- | --- | --- |
| Find pages by title in a space | GET | `/rest/api/content?type=page&spaceKey=ENG&title=Runbook` |
| Search with CQL | GET | `/rest/api/content/search?cql=space=ENG%20and%20text~%22rollback%22` |
| Read a page body and version | GET | `/rest/api/content/123456?expand=body.storage,version,space,ancestors` |
| Create a page | POST | `/rest/api/content` |
| Update a page | PUT | `/rest/api/content/123456` |
| List child pages | GET | `/rest/api/content/123456/child/page` |
| Read page labels | GET | `/rest/api/content/123456/label` |

## CQL patterns

Use CQL when title-only matching is too narrow.

- `space=ENG and title~"onboarding"`
- `space=OPS and text~"rollback"`
- `label=runbook and space=PLAT`
- `space=ENG and type=page order by lastmodified desc`

If the instance version behaves differently, verify against the local REST API docs.

## Fetch patterns

```bash
BASE_URL="${CONFLUENCE_BASE_URL%/}"

# Basic auth
curl -sS -u "$CONFLUENCE_USERNAME:$CONFLUENCE_PASSWORD" \
  "$BASE_URL/rest/api/content/123456?expand=body.storage,version,space,ancestors"

# Bearer token
curl -sS -H "Authorization: Bearer $CONFLUENCE_PAT" \
  "$BASE_URL/rest/api/content/123456?expand=body.storage,version,space,ancestors"
```

## Create payload reminder

```json
{
  "type": "page",
  "title": "Title here",
  "space": { "key": "ENG" },
  "ancestors": [{ "id": 123456 }],
  "body": {
    "storage": {
      "value": "<h1>Heading</h1><p>Body</p>",
      "representation": "storage"
    }
  }
}
```

## Update payload reminder

Always fetch the current page first, then increment the version.

```json
{
  "id": "123456",
  "type": "page",
  "title": "Existing title",
  "version": { "number": 8 },
  "body": {
    "storage": {
      "value": "<p>Updated body</p>",
      "representation": "storage"
    }
  }
}
```

## Safe markup tips

- Prefer simple storage markup: paragraphs, headings, lists, tables, code blocks
- Preserve macros if the page already has them
- Avoid rewriting the whole page when a section-level edit is enough
- If Confluence rejects the body, simplify the markup before retrying

## Common error interpretation

- `401` or `403`: auth or permissions issue
- `404`: wrong page ID, base URL, or instance-specific REST path mismatch
- `409`: stale version, refetch and retry
- `400`: malformed JSON or invalid storage markup
