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

**Basic filters:**
- `space=ENG and title~"onboarding"` — fuzzy title match in a space
- `space=OPS and text~"rollback"` — full-text body search
- `label=runbook and space=PLAT` — by label
- `label in (runbook, sop) and space=ENG` — multiple labels (OR)
- `creator=alice and space=ENG` — by author

**Date filters:**
- `space=OPS and lastmodified > "2025-01-01"` — modified after date
- `space=ENG and created >= "2025-06-01"` — created after date
- `space=PLAT and lastmodified > "2025-03-01" and label=postmortem` — combine date + label

**Subtree scoping:**
- `ancestor=123456 and type=page` — all pages under a parent
- `space=ENG and ancestor=456789 and label=runbook` — scoped to subtree + label

**Ordering:**
- `space=ENG and type=page order by lastmodified desc`
- `space=OPS and label=incident order by created desc`

**Pitfalls:**
- `text~` is fuzzy: may match comments/macros/metadata. Use `--keywords` for precise client-side filtering.
- `title=` is exact match; `title~` is contains.
- Dates must be `"YYYY-MM-DD"` with double quotes.
- Labels are lowercase: `label=release-notes` not `label=Release-Notes`.

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
