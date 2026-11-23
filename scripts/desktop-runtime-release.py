"""Publish native browser runtime originals for a verified Bamboo release."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile


spec = importlib.util.spec_from_file_location('bamboo_release_proof', Path(__file__).with_name('docker-release.py'))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)
TARGETS = proof.TARGETS[3]
MARKER = 'browser-runtime/desktop.lock.json'
MANIFEST = 'bamboo-desktop-browser-runtimes-v1.json'
HOST = 'browser-runtime/host.cjs'
PACKAGE_LOCK = 'browser-runtime/package-lock.json'
MAX_ARCHIVE = 768 * 1024 ** 2


def plan():
    assert os.environ['GITHUB_REF'] == 'refs/heads/dev' and os.environ['GITHUB_REF_PROTECTED'] == 'true'
    event, version, source = proof.release_inputs()
    assert event, 'This release consumes the original protected Bamboo publisher'
    try:
        lock = json.loads(proof.frozen_file(MARKER, source))
    except subprocess.CalledProcessError as error:
        if b'(HTTP 404)' in (error.stderr or b''):
            return None
        raise
    assert lock['schemaVersion'] == 1 and sorted(lock['targets']) == TARGETS
    verified = proof.verify_release_assets(event, version, source)
    assert verified['contract'] == 3
    manifest_sha = hashlib.sha256(verified['manifest']).hexdigest()
    manifest = json.loads(verified['manifest'])
    host = {entry['path']: entry['sha256'] for entry in manifest['browserHost']['files']}
    return {'version': version, 'source': source, 'manifest_sha': manifest_sha,
            'host_sha': host[HOST], 'package_lock_sha': host[PACKAGE_LOCK]}


def read_release(selection):
    release = proof.api('releases/tags/v' + selection['version'])
    assert release['tag_name'] == 'v' + selection['version'] and release['target_commitish'] == selection['source']
    assert not release['draft'] and not release['prerelease'] and release['published_at']
    ref = proof.api('git/ref/tags/' + release['tag_name'])['object']
    assert ref['type'] == 'commit' and ref['sha'] == selection['source']
    asset = proof.asset(release['assets'], 'bamboo-sidecars-v3.json')
    assert asset['digest'] == 'sha256:' + selection['manifest_sha']
    return release


def write_outputs(values):
    with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as file:
        for key, value in values.items():
            file.write(f'{key}={value}\n')


def plan_outputs():
    selection = plan()
    if selection is None:
        write_outputs({'ready': 'false'})
        return
    release = read_release(selection)
    existing = [asset for asset in release['assets'] if asset['name'] == MANIFEST]
    if existing:
        asset = proof.asset(release['assets'], MANIFEST)
        raw = proof.api(f"releases/assets/{asset['id']}", binary=True)
        assert len(raw) == asset['size'] and asset['digest'] == 'sha256:' + hashlib.sha256(raw).hexdigest()
        verify_published_manifest(json.loads(raw), selection, release)
    elif any(asset['name'] in {archive_name(target) for target in TARGETS} for asset in release['assets']):
        assert int(os.environ['GITHUB_RUN_ATTEMPT']) > 1, 'Recover the original runtime publication attempt'
    write_outputs({**selection, 'ready': 'false' if existing else 'true'})


def prevent_repacking(target):
    assert target in TARGETS
    attempt = int(os.environ['GITHUB_RUN_ATTEMPT'])
    run_id = int(os.environ['GITHUB_RUN_ID'])
    assert 1 <= attempt <= 51 and run_id > 0
    for previous in range(1, attempt):
        jobs = proof.complete_list(proof.api(f'actions/runs/{run_id}/attempts/{previous}/jobs?per_page=100'), 'jobs')
        for job in jobs:
            if job['name'] == f'Pack browser runtime ({target})':
                assert not any(step['name'] == 'Pack the native browser runtime' and step['conclusion'] == 'success'
                               for step in job['steps']), f'Reuse the verified original {target} archive'


def archive_name(target):
    assert target in TARGETS
    return f'bamboo-browser-runtime-v1-{target}.tar.gz'


def validate_record(record, target):
    assert set(record) == {'assetName', 'archive', 'receiptSha256'}
    assert record['assetName'] == archive_name(target)
    archive = record['archive']
    assert set(archive) == {'sha256', 'sizeBytes'}
    assert re.fullmatch(proof.DIGEST, archive['sha256']) and re.fullmatch(proof.DIGEST, record['receiptSha256'])
    assert type(archive['sizeBytes']) is int and 0 < archive['sizeBytes'] <= MAX_ARCHIVE


def runtime_receipt(file, record, target, selection):
    assert file.is_file() and not file.is_symlink() and file.stat().st_size == record['archive']['sizeBytes']
    checksum = hashlib.sha256()
    with file.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            checksum.update(block)
    assert checksum.hexdigest() == record['archive']['sha256']
    with tarfile.open(file, 'r|gz') as archive:
        receipt = None
        for member in archive:
            if member.name == 'browser-runtime/receipt.json':
                assert receipt is None and member.isfile() and 0 < member.size <= 64 * 1024
                raw = archive.extractfile(member).read()
                assert hashlib.sha256(raw).hexdigest() == record['receiptSha256']
                receipt = json.loads(raw)
        assert receipt and receipt['schemaVersion'] == 1
        assert receipt['target'] == target and receipt['sourceRevision'] == selection['source']
        assert receipt['hostSha256'] == selection['host_sha'] and receipt['packageLockSha256'] == selection['package_lock_sha']


def verify_published_manifest(manifest, selection, release):
    assert set(manifest) == {'kind', 'version', 'sourceRevision', 'bambooManifestSha256', 'artifacts'}
    assert manifest['kind'] == 'bamboo-desktop-browser-runtimes/v1'
    assert manifest['version'] == selection['version'] and manifest['sourceRevision'] == selection['source']
    assert manifest['bambooManifestSha256'] == selection['manifest_sha']
    assert [item['target'] for item in manifest['artifacts']] == TARGETS
    for item in manifest['artifacts']:
        target = item['target']
        record = {key: value for key, value in item.items() if key != 'target'}
        validate_record(record, target)
        proof.original(release['assets'], {'assetName': record['assetName'], **record['archive']}, record['assetName'])


def publish(directory, bindings):
    selection = plan()
    assert selection is not None
    assert set(bindings) == {target.replace('-', '_') for target in TARGETS}
    transported = sorted(file.name for file in directory.iterdir())
    assert transported == sorted(archive_name(target) for target in TARGETS), 'Unexpected native transport inventory'
    records = []
    for target in TARGETS:
        native = json.loads(bindings[target.replace('-', '_')])
        assert set(native) == {'target', 'sourceRevision', 'artifact'}
        assert native['target'] == target and native['sourceRevision'] == selection['source']
        record = native['artifact']
        validate_record(record, target)
        runtime_receipt(directory / record['assetName'], record, target, selection)
        records.append({'target': target, **record})

    manifest = {'kind': 'bamboo-desktop-browser-runtimes/v1', 'version': selection['version'], 'sourceRevision': selection['source'],
                'bambooManifestSha256': selection['manifest_sha'], 'artifacts': records}
    raw = (json.dumps(manifest, indent=2) + '\n').encode()
    manifest_file = directory / MANIFEST
    with manifest_file.open('xb') as stream:
        stream.write(raw)
    release = read_release(selection)
    expected = {record['assetName']: record['archive'] for record in records}
    expected[MANIFEST] = {'sha256': hashlib.sha256(raw).hexdigest(), 'sizeBytes': len(raw)}
    for name, identity in expected.items():
        if not any(asset['name'] == name for asset in release['assets']):
            subprocess.run(['gh', 'release', 'upload', release['tag_name'], str(directory / name), '--repo', proof.REPOSITORY],
                           check=True, timeout=300)
            updated = read_release(selection)
            assert updated['id'] == release['id']
            release = updated
        proof.original(release['assets'], {'assetName': name, **identity}, name)
    verify_published_manifest(manifest, selection, release)
    print(json.dumps({'release': release['tag_name'], 'manifest': expected[MANIFEST], 'targets': TARGETS}, separators=(',', ':')))


if __name__ == '__main__':
    operation = sys.argv[1]
    if operation == 'plan':
        plan_outputs()
    elif operation == 'guard':
        prevent_repacking(sys.argv[2])
    else:
        assert operation == 'publish'
        publish(Path(sys.argv[2]), json.loads(os.environ['BAMBOO_BROWSER_BUILD_OUTPUTS']))
