#!/usr/bin/env python3
"""Allow temporary workspace version stamping without unlocking tested dependencies."""
from pathlib import Path
import re
import subprocess
import sys
import tomllib


def assert_external_lock_unchanged(source, stamped):
    def coordinates(lock):
        return sorted(
            (package["name"], package["version"], package["source"], package.get("checksum", ""))
            for package in lock["package"]
            if "source" in package
        )

    if coordinates(source) != coordinates(stamped):
        raise ValueError("External dependency lock changed while stamping the release; refusing to publish")


def stamp_manifests(target_version):
    root = Path(".")
    # Workspace members live two levels below crates/.
    manifest_paths = [root / "Cargo.toml", *sorted((root / "crates").glob("*/*/Cargo.toml"))]

    workspace_crates = set()
    for path in manifest_paths:
        name = tomllib.loads(path.read_text()).get("package", {}).get("name")
        if name:
            workspace_crates.add(name)

    dependency_sections = ("dependencies", "dev-dependencies", "build-dependencies")

    for path in manifest_paths:
        lines = path.read_text().splitlines()
        current_section = None
        package_version_done = False

        for idx, line in enumerate(lines):
            stripped = line.strip()

            match = re.match(r"^\[(.+)\]\s*$", stripped)
            if match:
                current_section = match.group(1)
                continue

            # Inherited versions are stamped in [workspace.package].
            if (
                current_section in ("package", "workspace.package")
                and not package_version_done
                and re.match(r'^version\s*=\s*"[^"]+"\s*$', stripped)
            ):
                lines[idx] = re.sub(r'version\s*=\s*"[^"]+"',
                                    f'version = "{target_version}"', line, count=1)
                package_version_done = True
                continue

            if not current_section or current_section.rsplit(".", 1)[-1] not in dependency_sections:
                continue

            match = re.match(r'^(\s*)([A-Za-z0-9_-]+)\s*=\s*\{(.*)\}\s*$', line)
            if not match:
                continue

            indent, dep_name, body = match.groups()
            if dep_name not in workspace_crates or "path" not in body:
                continue

            if re.search(r'\bversion\s*=\s*"[^"]+"', body):
                updated_body = re.sub(r'\bversion\s*=\s*"[^"]+"', f'version = "={target_version}"', body)
            else:
                updated_body = body.rstrip()
                if updated_body and not updated_body.rstrip().endswith(","):
                    updated_body += ","
                updated_body += f' version = "={target_version}"'

            lines[idx] = f"{indent}{dep_name} = {{{updated_body}}}"

        path.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        assert len(sys.argv) == 3 and sys.argv[1] == "stamp"
        stamp_manifests(sys.argv[2])
        sys.exit(0)
    original = subprocess.check_output(["git", "show", "HEAD:Cargo.lock"], text=True)
    assert_external_lock_unchanged(
        tomllib.loads(original), tomllib.loads(Path("Cargo.lock").read_text())
    )
    print("Preserved every source-locked external dependency coordinate and checksum")
