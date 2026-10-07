#!/usr/bin/env python3
"""Allow temporary workspace version stamping without unlocking tested dependencies."""
from pathlib import Path
import subprocess
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


if __name__ == "__main__":
    original = subprocess.check_output(["git", "show", "HEAD:Cargo.lock"], text=True)
    assert_external_lock_unchanged(
        tomllib.loads(original), tomllib.loads(Path("Cargo.lock").read_text())
    )
    print("Preserved every source-locked external dependency coordinate and checksum")
