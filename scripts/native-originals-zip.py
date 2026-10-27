#!/usr/bin/env python3
"""Read the original two-file Actions ZIP; never create/recompress an archive."""
import os
from pathlib import Path
import stat
import struct
import sys
from zipfile import ZipFile, ZIP_STORED, ZIP_DEFLATED

MAX_BINARY = 1024 * 1024 * 1024


def extract(archive, directory, binary_name):
    archive, directory = Path(archive), Path(directory)
    before = archive.lstat()
    output = directory.lstat()
    if (not stat.S_ISREG(before.st_mode) or before.st_size > MAX_BINARY + 4 * 1024 * 1024
            or not stat.S_ISDIR(output.st_mode) or stat.S_ISLNK(output.st_mode)
            or list(directory.iterdir()) or "/" in binary_name or "\\" in binary_name):
        raise ValueError("Expected an original bounded ZIP and empty ordinary output directory")
    with os.fdopen(os.open(archive, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)), "rb") as raw:
        opened = os.fstat(raw.fileno())
        if (opened.st_dev, opened.st_ino, opened.st_size) != (before.st_dev, before.st_ino, before.st_size):
            raise ValueError("Original archive changed while opening")
        # Bound the central directory BEFORE ZipFile allocates its entry table.
        raw.seek(-22, os.SEEK_END)
        end = raw.read(22)
        signature, disk, central_disk, entries_disk, entries, size, offset, comment = struct.unpack("<4s4H2LH", end)
        if (signature != b"PK\x05\x06" or disk or central_disk or comment or entries != 2
                or entries_disk != 2 or size > 4096 or offset + size != opened.st_size - 22):
            raise ValueError("Original transport must have exactly two bounded ordinary ZIP entries")
        raw.seek(0)
        with ZipFile(raw) as zipped:
            members = zipped.infolist()
            expected = {binary_name: (65536, MAX_BINARY), "build-receipt.json": (1, 1024 * 1024)}
            if len(members) != 2 or {item.filename for item in members} != set(expected):
                raise ValueError("Original ZIP inventory differs from the native output")
            for item in members:
                low, high = expected[item.filename]
                mode = stat.S_IFMT(item.external_attr >> 16)
                if (mode not in (0, stat.S_IFREG) or item.is_dir() or item.flag_bits & 1
                        or item.compress_type not in (ZIP_STORED, ZIP_DEFLATED)
                        or not low <= item.file_size <= high):
                    raise ValueError("Unsafe original ZIP member")
                written = 0
                with zipped.open(item) as source, (directory / item.filename).open("xb") as destination:
                    while data := source.read(1024 * 1024):
                        written += len(data)
                        if written > high:
                            raise ValueError("Original ZIP member exceeded its bound")
                        destination.write(data)
                if written != item.file_size:
                    raise ValueError("Original ZIP member length changed")
        after = os.fstat(raw.fileno())
        if (after.st_size, after.st_mtime_ns) != (opened.st_size, opened.st_mtime_ns):
            raise ValueError("Original archive changed while reading")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("usage: native-originals-zip.py ORIGINAL_ZIP EMPTY_OUTPUT BINARY_NAME")
    extract(*sys.argv[1:])
