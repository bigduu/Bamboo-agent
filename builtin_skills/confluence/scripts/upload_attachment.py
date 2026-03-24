#!/usr/bin/env python3
"""Upload a file attachment to a Confluence page.

Usage:
    python3 upload_attachment.py --page-id 123456 --file /path/to/checklist.pdf
    python3 upload_attachment.py --page-id 123456 --file report.xlsx --comment "Q3 report"

Output: JSON with attachment details (id, title, download link).
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import confluence_client as cc


def upload_attachment(page_id, file_path, comment=None):
    """Upload a file as an attachment to a Confluence page."""
    if not os.path.isfile(file_path):
        cc.fail(f"File not found: {file_path}")

    status, data = cc.multipart_upload(
        f"/rest/api/content/{page_id}/child/attachment",
        file_path=file_path,
        comment=comment,
    )

    if status == 404:
        cc.fail(f"Page {page_id} not found", status_code=404)
    if status not in (200, 201):
        msg = "Attachment upload failed"
        if isinstance(data, dict):
            msg = data.get("message", msg)
        cc.fail(msg, status_code=status, body=str(data))

    # Parse response — may be a single result or a list
    results = data.get("results", [data]) if isinstance(data, dict) else [data]
    attachments = []
    for att in results:
        if not isinstance(att, dict):
            continue
        info = {
            "id": att.get("id"),
            "title": att.get("title"),
            "type": att.get("type"),
        }
        # download link
        links = att.get("_links", {})
        download = links.get("download")
        base = links.get("base", "")
        if download:
            info["download_url"] = (base + download) if base else download

        # version
        ver = att.get("version")
        if ver:
            info["version"] = ver.get("number")

        # file size
        extensions = att.get("extensions", {})
        if "fileSize" in extensions:
            info["file_size"] = extensions["fileSize"]
        if "mediaType" in extensions:
            info["media_type"] = extensions["mediaType"]

        attachments.append(info)

    return attachments


def main():
    parser = argparse.ArgumentParser(description="Upload attachment to Confluence page")
    parser.add_argument("--page-id", required=True, help="Target page ID")
    parser.add_argument("--file", required=True, help="Path to file to upload")
    parser.add_argument("--comment", default=None, help="Attachment comment")
    args = parser.parse_args()

    cc.init()
    result = upload_attachment(args.page_id, args.file, comment=args.comment)
    cc.ok({"page_id": args.page_id, "attachments": result})


if __name__ == "__main__":
    main()
