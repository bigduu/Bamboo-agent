#!/usr/bin/env python3
"""Package the three existing browser host sources; no Cargo or network work."""
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import sys
import tarfile

FILES = ("browser-runtime/host.cjs", "browser-runtime/package-lock.json", "browser-runtime/package.json")
LIMIT = 8 * 1024 * 1024


def package(source_root, destination):
    root = Path(source_root).absolute()
    output = Path(destination).absolute()
    for directory in (root, root / "browser-runtime", output.parent):
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise ValueError("Browser host directories must be ordinary directories")
    if output.exists() or output.is_symlink():
        raise ValueError("Refusing to overwrite an existing browser host archive")
    contents = []
    for name in FILES:
        path = root / name
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_size < 1 or before.st_size > LIMIT:
            raise ValueError("Expected a bounded regular browser host file: " + name)
        descriptor = os.open(str(path), os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "rb") as stream:
            opened = os.fstat(stream.fileno())
            if (opened.st_dev, opened.st_ino, opened.st_size) != (before.st_dev, before.st_ino, before.st_size):
                raise ValueError("Browser host file changed while opening")
            data = stream.read(LIMIT + 1)
            after = os.fstat(stream.fileno())
        if len(data) != before.st_size or after.st_mtime_ns != before.st_mtime_ns:
            raise ValueError("Browser host file changed while reading")
        contents.append((name, data))
    if sum(len(data) for _, data in contents) > LIMIT:
        raise ValueError("Browser host content exceeds its size bound")
    temporary = output.with_name(output.name + ".partial-" + str(os.getpid()))
    try:
        with temporary.open("xb") as raw:
            with gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0, compresslevel=9) as compressed:
                with tarfile.open(fileobj=compressed, mode="w", format=tarfile.USTAR_FORMAT) as archive:
                    for name, data in contents:
                        member = tarfile.TarInfo(name)
                        member.size = len(data)
                        member.mode = 0o644
                        member.uid = member.gid = member.mtime = 0
                        member.uname = member.gname = ""
                        member.type = tarfile.REGTYPE
                        archive.addfile(member, io.BytesIO(data))
        if temporary.stat().st_size > LIMIT:
            raise ValueError("Browser host archive exceeds its size bound")
        # A concurrent invocation must not replace a previously produced asset.
        os.link(str(temporary), str(output))
    finally:
        temporary.unlink(missing_ok=True)
    data = output.read_bytes()
    return {"assetName": output.name, "sha256": hashlib.sha256(data).hexdigest(), "sizeBytes": len(data),
            "files": [{"path": name, "sha256": hashlib.sha256(content).hexdigest(), "sizeBytes": len(content)}
                      for name, content in contents]}


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: sidecar-browser-host.py SOURCE_ROOT OUTPUT_ARCHIVE")
    print(json.dumps(package(sys.argv[1], sys.argv[2]), sort_keys=True))
