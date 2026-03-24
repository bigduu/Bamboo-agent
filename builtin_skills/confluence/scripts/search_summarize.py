#!/usr/bin/env python3
"""Search Confluence pages with body expansion and extract keyword context snippets.

Designed for batch summarization: searches with body.storage expanded in a single
request (no recursive per-page fetches), strips HTML tags, and extracts keyword
context windows (default 200 chars before/after each match) for AI-friendly output.

Usage:
    # CQL search with keyword context extraction
    python3 search_summarize.py --cql 'space=ENG and text~"rollback"' --keywords rollback

    # Title search, extract context around multiple keywords
    python3 search_summarize.py --title "incident" --space OPS --keywords "root cause" "remediation"

    # Top 5 results, 300-char context window
    python3 search_summarize.py --cql 'space=PLAT and label=postmortem' \
        --keywords "timeline" --top 5 --context-chars 300

    # No keywords: returns first N chars as excerpt per page
    python3 search_summarize.py --cql 'space=ENG and lastmodified > "2025-03-01"' --top 10

Output: JSON with page metadata + text snippets ready for AI summarization.
"""

import argparse
import html
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


# ── HTML stripping ────────────────────────────────────────────────────

_TAG_RE = re.compile(r"<[^>]+>")
_MULTI_WS_RE = re.compile(r"[ \t]+")
_MULTI_NL_RE = re.compile(r"\n{3,}")


def strip_html(storage_value: str) -> str:
    """Remove HTML/XML tags from Confluence storage markup and return clean text.

    Handles common Confluence patterns:
    - Block-level tags (p, div, h1-h6, li, tr, td) → newlines
    - Inline tags stripped silently
    - HTML entities unescaped
    - Whitespace normalised
    """
    if not storage_value:
        return ""

    text = storage_value

    # Replace block-level closing/opening tags with newlines for readability
    text = re.sub(r"</(?:p|div|h[1-6]|li|tr|td|th|table|ul|ol|blockquote|pre)>", "\n", text, flags=re.I)
    text = re.sub(r"<(?:br|hr)\s*/?>", "\n", text, flags=re.I)

    # Strip all remaining tags
    text = _TAG_RE.sub("", text)

    # Unescape HTML entities
    text = html.unescape(text)

    # Normalise whitespace
    text = _MULTI_WS_RE.sub(" ", text)
    text = _MULTI_NL_RE.sub("\n\n", text)

    return text.strip()


# ── Keyword context extraction ────────────────────────────────────────

def extract_keyword_snippets(text: str, keywords: list[str], context_chars: int = 200,
                              max_snippets_per_keyword: int = 3) -> list[dict]:
    """Find keyword matches in text and extract surrounding context.

    Returns a list of snippet dicts:
        {"keyword": str, "position": int, "snippet": str}

    Overlapping snippets for the same keyword are merged.
    """
    if not text or not keywords:
        return []

    snippets = []
    text_lower = text.lower()

    for kw in keywords:
        kw_lower = kw.lower()
        # Find all match positions
        positions = []
        start = 0
        while True:
            idx = text_lower.find(kw_lower, start)
            if idx == -1:
                break
            positions.append(idx)
            start = idx + 1

        if not positions:
            continue

        # Merge overlapping context windows
        merged_ranges = []
        for pos in positions[:max_snippets_per_keyword * 2]:  # scan more, merge down
            ctx_start = max(0, pos - context_chars)
            ctx_end = min(len(text), pos + len(kw) + context_chars)

            if merged_ranges and ctx_start <= merged_ranges[-1][1]:
                # Overlap with previous range: extend
                merged_ranges[-1] = (merged_ranges[-1][0], max(merged_ranges[-1][1], ctx_end))
            else:
                merged_ranges.append((ctx_start, ctx_end))

        # Extract snippets, cap at max_snippets_per_keyword
        for rng_start, rng_end in merged_ranges[:max_snippets_per_keyword]:
            snippet_text = text[rng_start:rng_end].strip()

            # Add ellipsis markers for truncation
            prefix = "..." if rng_start > 0 else ""
            suffix = "..." if rng_end < len(text) else ""

            snippets.append({
                "keyword": kw,
                "position": rng_start,
                "snippet": f"{prefix}{snippet_text}{suffix}",
            })

    return snippets


# ── Page processing ───────────────────────────────────────────────────

def process_page(page: dict, keywords: list[str], context_chars: int,
                 excerpt_length: int, max_snippets: int) -> dict:
    """Process a single page result into a summary-ready dict."""
    info = {
        "id": page.get("id"),
        "title": page.get("title"),
    }

    # Space
    space = page.get("space")
    if space:
        info["space_key"] = space.get("key")

    # Version / date
    ver = page.get("version")
    if ver:
        info["version"] = ver.get("number")
        info["last_modified"] = ver.get("when")
        by = ver.get("by")
        if by:
            info["modified_by"] = by.get("displayName") or by.get("username")

    # URL
    links = page.get("_links", {})
    base = links.get("base", "")
    webui = links.get("webui", "")
    if base and webui:
        info["url"] = base + webui

    # Labels
    labels_obj = page.get("metadata", {}).get("labels")
    if labels_obj and "results" in labels_obj:
        info["labels"] = [lb.get("name") for lb in labels_obj["results"]]

    # Body → plain text
    body_html = page.get("body", {}).get("storage", {}).get("value", "")
    plain_text = strip_html(body_html)
    info["text_length"] = len(plain_text)

    if keywords:
        # Extract keyword context snippets
        snippets = extract_keyword_snippets(
            plain_text, keywords,
            context_chars=context_chars,
            max_snippets_per_keyword=max_snippets,
        )
        info["snippets"] = snippets
        info["keyword_hit_count"] = len(snippets)
    else:
        # No keywords: return excerpt (first N chars)
        if plain_text:
            excerpt = plain_text[:excerpt_length]
            if len(plain_text) > excerpt_length:
                excerpt += "..."
            info["excerpt"] = excerpt

    return info


# ── Search functions ──────────────────────────────────────────────────

def _search_expand():
    """Expand string for search with body."""
    return "body.storage,version,space,metadata.labels"


def search_cql(cql: str, limit: int) -> list[dict]:
    """CQL search with body expansion in a single request."""
    status, data = cc.get("/rest/api/content/search",
                          cql=cql, limit=str(limit), expand=_search_expand())
    if status != 200:
        cc.fail(f"CQL search failed (HTTP {status})", status_code=status, body=str(data))
    return data.get("results", [])


def search_title(title: str, space: str | None, limit: int) -> list[dict]:
    """Title search with body expansion in a single request."""
    params = {
        "type": "page",
        "title": title,
        "limit": str(limit),
        "expand": _search_expand(),
    }
    if space:
        params["spaceKey"] = space
    status, data = cc.get("/rest/api/content", **params)
    if status != 200:
        cc.fail(f"Search failed (HTTP {status})", status_code=status, body=str(data))
    return data.get("results", [])


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Search Confluence and extract keyword context snippets for AI summarization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  # Search with keyword context extraction
  python3 search_summarize.py --cql 'space=ENG and text~"rollback"' --keywords rollback

  # Multiple keywords, top 5 results
  python3 search_summarize.py --title "incident" --space OPS \\
      --keywords "root cause" "remediation" --top 5

  # No keywords: get excerpts of first 10 results
  python3 search_summarize.py --cql 'space=ENG and lastmodified > "2025-01-01"' --top 10
""")
    parser.add_argument("--title", help="Page title to search for")
    parser.add_argument("--space", help="Space key (e.g. ENG, OPS)")
    parser.add_argument("--cql", help="CQL query string")
    parser.add_argument("--keywords", nargs="+", default=[],
                        help="Keywords to extract context for (case-insensitive)")
    parser.add_argument("--top", type=int, default=10,
                        help="Max pages to process (default: 10)")
    parser.add_argument("--context-chars", type=int, default=200,
                        help="Characters before/after each keyword match (default: 200)")
    parser.add_argument("--excerpt-length", type=int, default=500,
                        help="Excerpt length when no keywords given (default: 500)")
    parser.add_argument("--max-snippets", type=int, default=3,
                        help="Max snippets per keyword per page (default: 3)")

    args = parser.parse_args()

    if not args.title and not args.cql:
        cc.fail("Provide --title or --cql")

    cc.init()

    # Single search request with body expansion — no recursive per-page fetches
    if args.cql:
        raw_pages = search_cql(args.cql, limit=args.top)
    else:
        raw_pages = search_title(args.title, space=args.space, limit=args.top)

    # Process each page: strip HTML, extract keyword context
    processed = []
    for page in raw_pages:
        processed.append(process_page(
            page,
            keywords=args.keywords,
            context_chars=args.context_chars,
            excerpt_length=args.excerpt_length,
            max_snippets=args.max_snippets,
        ))

    # Summary stats
    total_snippets = sum(p.get("keyword_hit_count", 0) for p in processed)
    pages_with_hits = sum(1 for p in processed if p.get("keyword_hit_count", 0) > 0)

    output = {
        "query": args.cql or f"title={args.title}" + (f" space={args.space}" if args.space else ""),
        "keywords": args.keywords or None,
        "pages_returned": len(processed),
        "pages_with_keyword_hits": pages_with_hits if args.keywords else None,
        "total_snippets": total_snippets if args.keywords else None,
        "context_chars": args.context_chars,
        "pages": processed,
    }

    # Remove None values for cleaner output
    output = {k: v for k, v in output.items() if v is not None}

    cc.ok(output)


if __name__ == "__main__":
    main()
