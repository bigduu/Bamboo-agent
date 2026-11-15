"""Assemble the npm package from an authenticated Bamboo v3 Release."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import shutil
import sys
import tarfile


ROOT = Path(__file__).resolve().parent.parent
HOST_FILES = ('browser-runtime/host.cjs', 'browser-runtime/package.json', 'browser-runtime/package-lock.json')
proof_spec = importlib.util.spec_from_file_location('bamboo_release_proof', ROOT / 'scripts/docker-release.py')
release_proof = importlib.util.module_from_spec(proof_spec)
proof_spec.loader.exec_module(release_proof)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def read_browser_host(raw, host):
    assert len(raw) == host['sizeBytes'] and sha256(raw) == host['sha256'], 'Browser Host archive changed'
    entries = host['files']
    assert sorted(entry['path'] for entry in entries) == sorted(HOST_FILES), 'Unexpected Browser Host manifest'
    expected = {entry['path']: entry for entry in entries}
    files = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r|gz') as archive:
        for member in archive:
            assert member.name in expected and member.name not in files, 'Unexpected Browser Host member'
            assert member.type in (tarfile.REGTYPE, tarfile.AREGTYPE) and not member.pax_headers, 'Browser Host members must be regular files'
            entry = expected[member.name]
            assert 0 < member.size == entry['sizeBytes'] <= 4 * 1024 * 1024, 'Invalid Browser Host member size'
            data = archive.extractfile(member).read()
            assert len(data) == member.size and sha256(data) == entry['sha256'], 'Browser Host member changed'
            files[member.name] = data
    assert set(files) == set(HOST_FILES), 'Incomplete Browser Host archive'
    return files


def browser_dependencies(files):
    dependencies = json.loads(files['browser-runtime/package.json'])['dependencies']
    assert set(dependencies) == {'playwright-core'}, 'Unexpected Browser Host dependencies'
    version = dependencies['playwright-core']
    assert re.fullmatch(release_proof.VERSION, version), 'Browser Host requires an exact dependency version'
    lock = json.loads(files['browser-runtime/package-lock.json'])['packages']
    assert lock['']['dependencies'] == dependencies
    assert lock['node_modules/playwright-core']['version'] == version
    return dependencies


def assemble(output, manifest_bytes, raw_host):
    manifest = json.loads(manifest_bytes)
    assert manifest['schemaVersion'] == 3 and re.fullmatch(release_proof.VERSION, manifest['version'])
    files = read_browser_host(raw_host, manifest['browserHost'])
    metadata = json.loads((ROOT / 'npm/package.json').read_text(encoding='utf-8'))
    assert metadata['name'] == '@bigduu/bamboo-agent' and metadata.pop('private') is True
    metadata['version'] = manifest['version']
    metadata['dependencies'] = browser_dependencies(files)

    output.mkdir()
    (output / 'bin').mkdir()
    (output / 'browser-runtime').mkdir()
    shutil.copy2(ROOT / 'npm/bin/bamboo.cjs', output / 'bin/bamboo.cjs')
    shutil.copyfile(ROOT / 'npm/README.md', output / 'README.md')
    shutil.copyfile(ROOT / 'LICENSE', output / 'LICENSE')
    (output / 'package.json').write_text(json.dumps(metadata, indent=2) + '\n', encoding='utf-8')
    (output / 'bamboo-sidecars-v3.json').write_bytes(manifest_bytes)
    for name, data in files.items():
        (output / name).write_bytes(data)


def main():
    assert len(sys.argv) == 2, 'Usage: npm-release.py <new-package-directory>'
    event_run, version, source = release_proof.release_inputs()
    assert event_run is None, 'npm publication requires a protected Dev dispatch'
    verified = release_proof.verify_release_assets(event_run, version, source)
    assert verified['contract'] == 3, 'npm publication requires a completed v3 release'
    asset = verified['browser_host']
    raw = release_proof.api(f"releases/assets/{asset['id']}", binary=True)
    assemble(Path(sys.argv[1]), verified['manifest'], raw)
    print(f'Prepared @bigduu/bamboo-agent@{version} from Bamboo {source} with five verified executables')


if __name__ == '__main__':
    main()
