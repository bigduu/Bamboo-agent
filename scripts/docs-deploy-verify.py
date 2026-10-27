#!/usr/bin/env python3
"""Read retained Docs data. Never run Rustdoc, source code, or a tar writer."""
import contextlib
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import tarfile
import tomllib
import zipfile

MAX_TAR, MAX_RECEIPT = 1 << 30, 32 << 20
COMMAND = ["cargo", "doc", "--locked", "--no-deps", "--all-features", "--document-private-items"]
REDIRECT = b'<meta http-equiv="refresh" content="0;url=bamboo_agent/index.html">\n'


@contextlib.contextmanager
def regular(path, maximum):
    assert not path.is_symlink()
    info = path.lstat()
    identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_nlink)
    assert stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and 0 < info.st_size <= maximum
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), "rb") as stream:
        assert identity(os.fstat(stream.fileno())) == identity(info)
        yield stream
        assert identity(os.fstat(stream.fileno())) == identity(info)


def digest(stream):
    value = hashlib.sha256()
    while block := stream.read(1 << 20):
        value.update(block)
    return value.hexdigest()


def file_hash(path, maximum):
    with regular(path, maximum) as stream:
        return digest(stream)


def read_json(path, maximum):
    with regular(path, maximum) as stream:
        return json.loads(stream.read(maximum + 1).decode("utf-8"))


def safe_path(name):
    path = PurePosixPath(name)
    assert name and name != "." and not path.is_absolute() and ".." not in path.parts
    assert path.as_posix() == name and "\\" not in name and not re.match("[A-Za-z]:", name)
    assert len(name.encode()) <= 4096 and all(ord(char) >= 32 for char in name)


def zip_entries(archive):
    entries = archive.infolist()
    assert len(entries) == 2 and {entry.filename for entry in entries} == {"artifact.tar", "receipt.json"}
    for entry in entries:
        assert not entry.is_dir() and not entry.flag_bits & 1 and entry.compress_type in {0, 8}
        assert stat.S_IFMT(entry.external_attr >> 16) in {0, stat.S_IFREG}
        assert 0 < entry.file_size <= (MAX_TAR if entry.filename == "artifact.tar" else MAX_RECEIPT)
    return entries


def unpack(root):
    with regular(root / "origin.zip", MAX_TAR) as raw, zipfile.ZipFile(raw) as archive:
        for entry in zip_entries(archive):
            maximum = MAX_TAR if entry.filename == "artifact.tar" else MAX_RECEIPT
            total = 0
            with archive.open(entry) as source, (root / entry.filename).open("xb") as target:
                while block := source.read(1 << 20):
                    total += len(block)
                    assert total <= maximum and total <= entry.file_size
                    target.write(block)
            assert total == entry.file_size


def verify(root):
    # Rebind local extracted files to the authenticated original ZIP on every
    # verification, including after Pages upload. Matching self-described files
    # and a rewritten receipt cannot substitute a different site.
    with regular(root / "origin.zip", MAX_TAR) as raw, zipfile.ZipFile(raw) as archive:
        for entry in zip_entries(archive):
            with archive.open(entry) as stream:
                assert digest(stream) == file_hash(root / entry.filename, MAX_TAR if entry.filename == "artifact.tar" else MAX_RECEIPT)
            assert (root / entry.filename).stat().st_size == entry.file_size
    receipt = read_json(root / "receipt.json", MAX_RECEIPT)
    expected = read_json(root / "expected.json", 24 << 20)
    assert set(receipt) == {"source", "controller", "run", "artifactName", "configuration", "inputs", "toolchain", "runner",
                            "schema", "complete", "configurationSha256", "tar", "files", "fileCount", "siteBytes"}
    assert receipt["schema"] == "bamboo.feature-docs.v1" and receipt["complete"] is True
    assert receipt["source"] == {"sha": expected["snapshot"], "tree": expected["tree"]}
    ref = "refs/pull/" + str(expected["pr"]) + "/merge"
    assert receipt["run"] == {"repository": "bigduu/Bamboo-agent", "id": expected["run"], "physicalAttempt": expected["attempt"], "event": "pull_request", "ref": ref}
    assert type(receipt["run"]["id"]) is int and type(receipt["run"]["physicalAttempt"]) is int
    assert set(receipt["controller"]) == {"sha", "ref"}
    assert receipt["controller"]["sha"] in {expected["workflowDefinition"], expected["snapshot"]}
    assert receipt["controller"]["ref"] == "bigduu/Bamboo-agent/.github/workflows/ci.yml@" + ref
    assert receipt["artifactName"] == "feature-docs-" + str(expected["run"]) + "-" + str(expected["attempt"])
    assert receipt["runner"] == {"os": "Linux", "arch": "X64"}
    assert set(receipt["toolchain"]) == {"rustc", "cargo", "rustdoc"}
    for tool, value in receipt["toolchain"].items():
        assert isinstance(value, str) and value.startswith(tool + " ") and len(value.encode()) <= 65536
    source = expected["sourceFiles"]
    for value in source.values():
        assert set(value) == {"sha256", "text"}
        assert hashlib.sha256(value["text"].encode()).hexdigest() == value["sha256"]
    defaults = tomllib.loads(source["Cargo.toml"]["text"])["workspace"]["default-members"]
    assert len(defaults) == len(set(defaults)) == 30
    members = []
    for member in defaults:
        manifest = "Cargo.toml" if member == "." else member + "/Cargo.toml"
        safe_path(manifest)
        name = tomllib.loads(source[manifest]["text"])["package"]["name"]
        members.append({"name": name, "manifest": manifest, "manifestSha256": source[manifest]["sha256"]})
    assert len({item["name"] for item in members}) == 30 and not {"bamboo-analytics", "bamboo-tool-event-recorder-example"} & {item["name"] for item in members}
    assert set(source) == {"Cargo.toml", "Cargo.lock", ".cargo/config.toml", ".github/workflows/ci.yml"} | {item["manifest"] for item in members}
    configuration = {"command": COMMAND, "defaultMembers": sorted(members, key=lambda item: item["name"]),
                     "sourceConfigSha256": {name: source[name]["sha256"] for name in ["Cargo.toml", ".cargo/config.toml"]}}
    assert receipt["configuration"] == configuration
    # Preserve d6a's Python sorted JSON hash, including its default separators.
    assert receipt["configurationSha256"] == hashlib.sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest()
    assert source[".github/workflows/ci.yml"]["sha256"] == expected["producerDigest"]
    assert receipt["inputs"] == {"cargoLockSha256": source["Cargo.lock"]["sha256"], "sourceWorkflowSha256": expected["producerDigest"]}
    archive = root / "artifact.tar"
    assert receipt["tar"] == {"file": "artifact.tar", "sha256": file_hash(archive, MAX_TAR), "sizeBytes": archive.stat().st_size}
    assert type(receipt["tar"]["sizeBytes"]) is int
    files = receipt["files"]
    assert isinstance(files, list) and 0 < len(files) <= 100000 and type(receipt["fileCount"]) is int and receipt["fileCount"] == len(files)
    for item in files:
        assert set(item) == {"path", "sizeBytes", "sha256"}
        safe_path(item["path"])
        assert type(item["sizeBytes"]) is int and 0 <= item["sizeBytes"] <= MAX_TAR
        assert re.fullmatch("[0-9a-f]{64}", item["sha256"])
    selected = {item["path"]: item for item in files}
    assert len(selected) == len(files) and list(selected) == sorted(selected)
    assert type(receipt["siteBytes"]) is int and receipt["siteBytes"] == sum(item["sizeBytes"] for item in files)
    assert 0 < receipt["siteBytes"] <= MAX_TAR and selected["bamboo_agent/index.html"]["sizeBytes"] > 0
    seen, found, total = set(), set(), 0
    with regular(archive, MAX_TAR) as raw, tarfile.open(fileobj=raw, mode="r:") as tar:
        for entry in tar:
            safe_path(entry.name)
            assert entry.name not in seen and len(seen) < 200000
            seen.add(entry.name)
            assert entry.isdir() or entry.isfile()
            assert not entry.issparse()
            if entry.isdir():
                continue
            item = selected[entry.name]
            total += entry.size
            assert total <= MAX_TAR and entry.size == item["sizeBytes"]
            with tar.extractfile(entry) as stream:
                assert digest(stream) == item["sha256"]
            if entry.name == "index.html":
                with tar.extractfile(entry) as stream:
                    assert stream.read(len(REDIRECT) + 1) == REDIRECT
            found.add(entry.name)
    assert found == set(selected) and "index.html" in found
    return {"tarSha256": receipt["tar"]["sha256"], "sizeBytes": receipt["tar"]["sizeBytes"],
            "receiptSha256": file_hash(root / "receipt.json", MAX_RECEIPT), "fileCount": len(files), "siteBytes": total}


if __name__ == "__main__":
    assert len(sys.argv) == 3 and sys.argv[1] in {"unpack", "verify"}
    root = Path(sys.argv[2])
    assert root.is_absolute() and root.is_dir() and not root.is_symlink() and root.resolve() == root
    if sys.argv[1] == "unpack":
        unpack(root)
    else:
        print(json.dumps(verify(root), sort_keys=True))
