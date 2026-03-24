#!/usr/bin/env python3
"""Page tree viewer for Confluence — minimal API calls.

Fetches ALL descendant pages in bulk via /descendant/page (with pagination),
then rebuilds the tree structure client-side using ancestor chains.

API call count:
  - 1 call to fetch root page info
  - ceil(total_descendants / 200) calls for paginated descendant listing
  - Example: 500 descendants → 1 + 3 = 4 API calls total
  - Old recursive approach: 1 per node → 500+ calls

Usage:
    python3 page_tree.py --page-id 123456
    python3 page_tree.py --page-id 123456 --depth 2
    python3 page_tree.py --page-id 123456 --depth 5 --flat

Output: JSON with nested (or flat) page tree.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


# ── Bulk descendant fetch ─────────────────────────────────────────

def _fetch_all_descendants(page_id, page_size=200):
    """Fetch ALL descendant pages in bulk with pagination.

    Uses /rest/api/content/{id}/descendant/page?expand=ancestors,version
    which returns all pages under the root in a flat list.
    Each page includes its ancestors chain so we can rebuild the tree.

    Returns a list of raw page dicts.
    """
    all_pages = []
    start = 0

    while True:
        status, data = cc.get(
            f"/rest/api/content/{page_id}/descendant/page",
            limit=str(page_size),
            start=str(start),
            expand="ancestors,version",
        )
        if status == 404:
            cc.fail(f"Page {page_id} not found", status_code=404)
        if status != 200:
            cc.fail(f"Failed to fetch descendants of {page_id} (HTTP {status})",
                    status_code=status, body=str(data))

        results = data.get("results", [])
        all_pages.extend(results)

        # Check for more pages
        if data.get("_links", {}).get("next") and len(results) == page_size:
            start += page_size
        else:
            break

    return all_pages


def _fetch_root_info(page_id):
    """Fetch root page metadata (1 API call)."""
    status, data = cc.get(
        f"/rest/api/content/{page_id}",
        expand="version,space",
    )
    if status == 404:
        cc.fail(f"Page {page_id} not found", status_code=404)
    if status != 200:
        cc.fail(f"Failed to fetch page {page_id}", status_code=status, body=str(data))
    return data


# ── Tree building (client-side, zero extra API calls) ────────────

def _compute_depth(page, root_id):
    """Compute depth of a page relative to root using its ancestors chain.

    ancestors is ordered from space root → immediate parent.
    Depth = number of ancestors after (and including) root_id.
    """
    ancestors = page.get("ancestors", [])
    ancestor_ids = [str(a.get("id")) for a in ancestors]
    root_str = str(root_id)

    if root_str in ancestor_ids:
        idx = ancestor_ids.index(root_str)
        return len(ancestor_ids) - idx
    # If root not in ancestors, this page is the root itself or orphaned
    return 0


def _find_parent_id(page, root_id):
    """Find the direct parent ID from the ancestors chain.

    The last element in ancestors is the immediate parent.
    """
    ancestors = page.get("ancestors", [])
    if ancestors:
        return str(ancestors[-1].get("id"))
    return str(root_id)


def build_tree(root_id, max_depth=3):
    """Build complete page tree with minimal API calls.

    Strategy:
    1. Fetch root page info (1 call)
    2. Fetch ALL descendants in bulk (paginated, ~ceil(N/200) calls)
    3. Rebuild tree structure client-side using ancestor chains
    4. Apply depth filter client-side

    Total API calls: 1 + ceil(descendants/200)
    """
    # Step 1: root info
    root_data = _fetch_root_info(root_id)
    root_str = str(root_id)

    root_node = {
        "id": root_data.get("id"),
        "title": root_data.get("title"),
        "depth": 0,
    }
    space = root_data.get("space")
    if space:
        root_node["space_key"] = space.get("key")
    ver = root_data.get("version")
    if ver:
        root_node["version"] = ver.get("number")

    # Step 2: bulk fetch all descendants
    all_descendants = _fetch_all_descendants(root_id)

    # Step 3: build a lookup of id → node, filtering by depth
    # Also track which nodes at max_depth have deeper children (for truncation markers)
    nodes = {root_str: root_node}
    children_map = {}  # parent_id → [child_ids]
    truncated_parents = set()  # parents whose children got cut by max_depth

    for page in all_descendants:
        page_id = str(page.get("id"))
        depth = _compute_depth(page, root_id)
        parent_id = _find_parent_id(page, root_id)

        # Pages beyond max depth: don't add to tree, just mark parent as truncated
        if depth > max_depth:
            truncated_parents.add(parent_id)
            continue

        node = {
            "id": page_id,
            "title": page.get("title"),
            "depth": depth,
        }
        v = page.get("version")
        if v:
            node["version"] = v.get("number")
            node["last_modified"] = v.get("when")

        nodes[page_id] = node

        if parent_id not in children_map:
            children_map[parent_id] = []
        children_map[parent_id].append(page_id)

    # Step 4: assemble tree structure (in-memory only, zero API calls)
    def _assemble(node_id):
        node = nodes.get(node_id, {"id": node_id})
        child_ids = children_map.get(node_id, [])
        children = [_assemble(cid) for cid in child_ids]
        node["children"] = children
        node["children_count"] = len(children)
        if node_id in truncated_parents:
            node["truncated"] = True
        return node

    return _assemble(root_str), len(all_descendants)


# ── Flatten ──────────────────────────────────────────────────────

def flatten_tree(node, result=None):
    """Flatten a nested tree into a list with indent/depth info."""
    if result is None:
        result = []

    entry = {k: v for k, v in node.items() if k != "children"}
    result.append(entry)

    for child in node.get("children", []):
        flatten_tree(child, result)

    return result


# ── Main ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Page tree viewer for Confluence (bulk fetch, minimal API calls)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python3 page_tree.py --page-id 123456 --depth 3
  python3 page_tree.py --page-id 123456 --depth 2 --flat

API efficiency:
  Uses /descendant/page to fetch ALL descendants in bulk,
  then rebuilds the tree client-side. Total API calls:
  1 (root) + ceil(total_descendants / 200) (pagination).
""")
    parser.add_argument("--page-id", required=True, help="Root page ID")
    parser.add_argument("--depth", type=int, default=3,
                        help="Max tree depth to show (default: 3)")
    parser.add_argument("--flat", action="store_true",
                        help="Output flat list instead of nested tree")
    args = parser.parse_args()

    cc.init()
    tree, total_descendants = build_tree(args.page_id, max_depth=args.depth)

    if args.flat:
        flat = flatten_tree(tree)
        cc.ok({
            "root_id": args.page_id,
            "max_depth": args.depth,
            "total_descendants_fetched": total_descendants,
            "total_pages_in_tree": len(flat),
            "pages": flat,
        })
    else:
        cc.ok({
            "root_id": args.page_id,
            "max_depth": args.depth,
            "total_descendants_fetched": total_descendants,
            "tree": tree,
        })


if __name__ == "__main__":
    main()
