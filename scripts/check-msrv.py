#!/usr/bin/env python3
"""Verify that the locked dependency graph supports Bamboo's declared MSRV.

``cargo check`` only compiles dependencies selected for the current host. Cargo
metadata with all features includes every resolved package in Cargo.lock,
including packages selected only on other targets, so auditing it prevents
optional or target-specific dependency drift from silently raising the real
minimum Rust version.

Run from the workspace root. Pass ``--metadata PATH`` only for tests or
diagnostics that already captured ``cargo metadata --locked`` output.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)(?:\.(\d+))?$")


def parse_rust_version(value: str) -> tuple[int, int, int]:
    """Return a comparable Rust release tuple."""
    match = VERSION_PATTERN.fullmatch(value)
    if not match:
        raise ValueError(f"unsupported rust_version value: {value!r}")
    major, minor, patch = match.groups()
    return int(major), int(minor), int(patch or 0)


def audit_metadata(metadata: dict) -> tuple[list[str], str, int, int]:
    """Return validation errors plus the declared MSRV and audited counts."""
    packages = {package["id"]: package for package in metadata["packages"]}
    workspace_ids = metadata["workspace_members"]
    resolve = metadata.get("resolve")
    if resolve is None:
        return ["cargo metadata did not include a resolved dependency graph"], "", 0, 0

    root_id = resolve.get("root")
    if root_id not in packages:
        return ["cargo metadata did not identify the root package"], "", 0, 0

    declared = packages[root_id].get("rust_version")
    if not declared:
        return ["root package does not declare rust-version"], "", 0, 0

    try:
        declared_tuple = parse_rust_version(declared)
    except ValueError as error:
        return [str(error)], declared, 0, 0

    errors = []
    for package_id in workspace_ids:
        package = packages[package_id]
        package_msrv = package.get("rust_version")
        if not package_msrv:
            errors.append(f"workspace package {package['name']} does not declare rust-version")
            continue
        try:
            package_tuple = parse_rust_version(package_msrv)
        except ValueError as error:
            errors.append(f"workspace package {package['name']}: {error}")
            continue
        if package_tuple != declared_tuple:
            errors.append(
                f"workspace package {package['name']} declares Rust {package_msrv}, "
                f"but the root package declares Rust {declared}"
            )

    resolved_ids = [node["id"] for node in resolve["nodes"]]
    for package_id in resolved_ids:
        package = packages[package_id]
        package_msrv = package.get("rust_version")
        if not package_msrv:
            continue
        try:
            package_tuple = parse_rust_version(package_msrv)
        except ValueError as error:
            errors.append(f"{package['name']} {package['version']}: {error}")
            continue
        if package_tuple > declared_tuple:
            errors.append(
                f"{package['name']} {package['version']} requires Rust "
                f"{package_msrv}, above the declared Rust {declared}"
            )

    return errors, declared, len(workspace_ids), len(resolved_ids)


def load_metadata(path: Path | None) -> dict:
    if path is not None:
        return json.loads(path.read_text(encoding="utf-8"))
    output = subprocess.check_output(
        [
            "cargo",
            "metadata",
            "--locked",
            "--all-features",
            "--format-version",
            "1",
        ]
    )
    return json.loads(output)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()

    try:
        metadata = load_metadata(args.metadata)
        errors, declared, workspace_count, resolved_count = audit_metadata(metadata)
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"MSRV audit failed: {error}", file=sys.stderr)
        return 1

    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1

    print(
        f"MSRV audit passed: {workspace_count} workspace packages and "
        f"{resolved_count} locked packages support Rust {declared}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
