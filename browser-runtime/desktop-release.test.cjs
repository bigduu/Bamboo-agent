const { execFileSync } = require("node:child_process");
const path = require("node:path");
const { test } = require("node:test");

test("the desktop runtime publisher refuses an existing conflicting asset before uploading", () => {
  const releaseScript = path.resolve(__dirname, "../scripts/desktop-runtime-release.py");
  execFileSync("python3", ["-BI", "-c", `
from pathlib import Path
import hashlib
import importlib.util
import io
import json
import sys
import tarfile
import tempfile

spec = importlib.util.spec_from_file_location('publisher', sys.argv[1])
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)
sha = lambda data: hashlib.sha256(data).hexdigest()
files = {publisher.HOST: b'host', publisher.PACKAGE: b'package', publisher.PACKAGE_LOCK: b'lock'}
selection = {'version': '2026.11.99', 'source': 'a' * 40, 'manifest_sha': 'b' * 64,
             'host_sha': sha(files[publisher.HOST]), 'package_sha': sha(files[publisher.PACKAGE]),
             'package_lock_sha': sha(files[publisher.PACKAGE_LOCK])}
publisher.plan = lambda: selection
uploads = []
publisher.subprocess.run = lambda *args, **kwargs: uploads.append(args)
checked = []
original = publisher.proof.original
def observe(assets, expected, name):
    checked.append(name)
    return original(assets, expected, name)
publisher.proof.original = observe

with tempfile.TemporaryDirectory() as temporary:
    directory = Path(temporary)
    binding = {}
    for target in publisher.TARGETS:
        name = publisher.archive_name(target)
        receipt = json.dumps({'schemaVersion': 1, 'target': target, 'sourceRevision': selection['source'],
                              'hostSha256': selection['host_sha'], 'packageLockSha256': selection['package_lock_sha']}).encode()
        with tarfile.open(directory / name, 'w:gz') as archive:
            for member, data in {**files, 'browser-runtime/receipt.json': receipt}.items():
                info = tarfile.TarInfo(member)
                info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
        raw = (directory / name).read_bytes()
        artifact = {'assetName': name, 'archive': {'sha256': sha(raw), 'sizeBytes': len(raw)}, 'receiptSha256': sha(receipt)}
        binding[target.replace('-', '_')] = json.dumps({'target': target, 'sourceRevision': selection['source'], 'artifact': artifact})
    conflict = publisher.archive_name(publisher.TARGETS[-1])
    publisher.read_release = lambda value: {'tag_name': 'v2026.11.99', 'id': 12,
        'assets': [{'name': conflict, 'id': 1, 'state': 'uploaded', 'size': 1, 'digest': 'sha256:' + '0' * 64}]}
    try:
        publisher.publish(directory, binding)
    except AssertionError:
        assert checked == [conflict], 'The test must reach the published asset identity check'
        assert not uploads, 'A conflicting original allowed a write'
    else:
        raise AssertionError('A conflicting original was accepted')
`, releaseScript], { stdio: "pipe" });
});
