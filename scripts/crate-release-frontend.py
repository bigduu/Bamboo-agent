#!/usr/bin/env python3
"""Bind restored ZIP payloads to this run's independently verified npm staging."""
from pathlib import Path
import stat
import sys
from zipfile import ZipFile


def verify_payloads(directory):
    with ZipFile(directory / "staged.zip") as staged, ZipFile(directory / "restored.zip") as restored:
        expected = {item.filename: item for item in staged.infolist()}
        actual = {item.filename: item for item in restored.infolist()}
        if len(actual) != len(restored.infolist()) or set(actual) != set(expected):
            raise ValueError("Preserved frontend ZIP entry inventory differs from the verified pinned package")
        for name, item in actual.items():
            original = expected[name]
            if stat.S_IFMT(item.external_attr >> 16) != stat.S_IFMT(original.external_attr >> 16):
                raise ValueError("Preserved frontend ZIP entry type differs from the verified pinned package")
            if name == "frontend-manifest.json":
                if restored.read(item) != (directory / "restored.json").read_bytes():
                    raise ValueError("Preserved frontend embedded manifest differs from its sidecar")
                if staged.read(original) != (directory / "staged.json").read_bytes():
                    raise ValueError("Verified staged frontend embedded manifest differs from its sidecar")
            elif item.file_size != original.file_size or restored.read(item) != staged.read(original):
                raise ValueError("Preserved frontend ZIP payload differs from the verified pinned package")


if __name__ == "__main__":
    verify_payloads(Path(sys.argv[1]))
