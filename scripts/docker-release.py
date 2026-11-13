"""Select immutable release inputs with read-only GitHub evidence."""

import base64
from datetime import datetime, timedelta
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import zipfile


REPOSITORY = 'bigduu/Bamboo-agent'
REPOSITORY_ID = 1164779771
PUBLISHER = 'Publish Crate'
PUBLISHER_PATH = '.github/workflows/publish-crate.yml'
FINALIZER = 'Publish and verify complete executable assets'
PUBLISH_STEP = 'Verify official provenance and publish immutable shared assets'
SHA = r'[0-9a-f]{40}'
DIGEST = r'[0-9a-f]{64}'
VERSION = r'[1-9][0-9]*\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)'
TARGETS = {
    2: ['aarch64-apple-darwin', 'x86_64-pc-windows-msvc', 'x86_64-unknown-linux-gnu'],
    3: ['aarch64-apple-darwin', 'aarch64-pc-windows-msvc', 'aarch64-unknown-linux-gnu',
        'x86_64-pc-windows-msvc', 'x86_64-unknown-linux-gnu'],
}


def api(route, *, binary=False, log=False, archive=False):
    args = ['gh', 'api', '--hostname', 'github.com', f'repos/{REPOSITORY}/{route}']
    if binary:
        args += ['-H', 'Accept: application/octet-stream']
    if log:
        args += ['-H', 'X-GitHub-Api-Version: 2026-03-10', '--allow-escape-sequences']
    if archive:
        args += ['-H', 'Accept: application/vnd.github+json', '-H', 'X-GitHub-Api-Version: 2026-03-10']
    data = subprocess.check_output(args, timeout=60, stderr=subprocess.PIPE)
    assert 0 < len(data) <= 4 * 1024 * 1024, 'GitHub publication response is empty or exceeds its bound'
    return data if binary or log or archive else json.loads(data)


def positive(value):
    assert type(value) is int and value > 0, 'Invalid GitHub Actions identity'
    return value


def instant(value):
    match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?Z', value)
    assert match, 'Invalid Actions timestamp'
    micros = (match[2] or '').ljust(6, '0')[:6]
    return datetime.fromisoformat(f'{match[1]}.{micros}+00:00')


def interval(data):
    start, end = instant(data['started_at']), instant(data['completed_at'])
    assert start <= end, 'Invalid publishing job or step execution interval'
    return start, end


def complete_list(data, field):
    rows = data[field]
    assert isinstance(rows, list) and type(data['total_count']) is int and data['total_count'] == len(rows) <= 100, (
        'Incomplete or excessive GitHub publisher history'
    )
    assert len({positive(row['id']) for row in rows}) == len(rows), 'Duplicate Actions identity'
    return rows


def frozen_file(file, revision):
    content = api(f'contents/{file}?ref={revision}')
    assert content['type'] == 'file' and content['path'] == file and content['encoding'] == 'base64'
    assert type(content['size']) is int and 0 < content['size'] <= 128 * 1024
    data = base64.b64decode(''.join(content['content'].splitlines()), validate=True)
    assert len(data) == content['size']
    return data.decode()


def publisher_identity(run, workflow_id):
    assert run['workflow_id'] == workflow_id and run['name'] == PUBLISHER and run['path'] == PUBLISHER_PATH
    assert run['event'] == 'workflow_dispatch' and run['head_branch'] == 'dev'
    for field in ('repository', 'head_repository'):
        assert run[field]['full_name'] == REPOSITORY and run[field]['id'] == REPOSITORY_ID
    assert re.fullmatch(SHA, run['head_sha'])
    positive(run['id'])
    positive(run['check_suite_id'])
    assert 1 <= positive(run['run_attempt']) <= 51, 'Excessive publisher attempt history'


def ancestor(earlier, later):
    proof = api(f'compare/{earlier}...{later}')
    assert proof['base_commit']['sha'] == proof['merge_base_commit']['sha'] == earlier
    assert proof['status'] in {'ahead', 'identical'}, 'Publisher or release source left protected Dev history'


def finalizer(run, attempt):
    jobs = complete_list(api(f"actions/runs/{run['id']}/attempts/{attempt}/jobs?per_page=100"), 'jobs')
    named = [job for job in jobs if job['name'] == FINALIZER]
    assert len(named) == 1, 'Missing or ambiguous publishing job'
    job = named[0]
    assert job['run_id'] == run['id'] and job['run_attempt'] == attempt
    assert job['head_sha'] == run['head_sha'] and job['head_branch'] == 'dev' and job['workflow_name'] == PUBLISHER
    assert job['status'] == 'completed'
    if job['conclusion'] == 'skipped':
        assert job['steps'] == []
        return job, None
    named = [step for step in job['steps'] if step['name'] == PUBLISH_STEP]
    assert len(named) == 1, 'Missing or ambiguous publishing step'
    step = named[0]
    assert step['status'] == 'completed'
    positive(step['number'])
    return job, step


def physical_finalizer(run, attempt):
    # GitHub can assign a new job ID and attempt to a prior physical execution.
    # Its original job and publishing-step time intervals remain unchanged.
    job, step = finalizer(run, attempt['run_attempt'])
    assert job['conclusion'] == 'success' and step and step['conclusion'] == 'success'
    job_times, step_times = interval(job), interval(step)
    assert job_times[0] <= step_times[0] <= step_times[1] <= job_times[1]
    attempt_start = instant(attempt['run_started_at'])
    assert job_times[1] <= instant(attempt['updated_at'])
    if job_times[0] >= attempt_start:
        return job, step
    assert job_times[1] < attempt_start, 'Ambiguous physical publishing job'
    original = None
    for prior in range(1, attempt['run_attempt']):
        previous = exact_attempt(run, prior)
        prior_job, prior_step = finalizer(run, prior)
        if (prior_step and prior_step['conclusion'] == 'success'
                and interval(prior_job) == job_times and interval(prior_step) == step_times
                and instant(previous['run_started_at']) <= job_times[0] <= job_times[1] <= instant(previous['updated_at'])):
            assert original is None, 'Ambiguous physical publishing attempt'
            original = prior_job, prior_step
    assert original is not None, 'Missing original publishing execution'
    return original


def exact_attempt(run, number):
    attempt = api(f"actions/runs/{run['id']}/attempts/{number}")
    publisher_identity(attempt, run['workflow_id'])
    for field in ('id', 'head_sha', 'check_suite_id'):
        assert attempt[field] == run[field], f'Publisher attempt differs in {field}'
    assert attempt['run_attempt'] == number and attempt['status'] == 'completed', 'Unexpected publisher attempt'
    return attempt


def published_proof(run, attempt):
    assert attempt['conclusion'] == 'success', 'The exact publisher attempt did not succeed'
    job, step = physical_finalizer(run, attempt)
    job_id = job['id']
    assert job['check_run_url'] == f'https://api.github.com/repos/{REPOSITORY}/check-runs/{job_id}'
    check = api(f'check-runs/{job_id}')
    assert check['id'] == job_id and check['name'] == FINALIZER and check['app']['id'] == 15368
    assert check['check_suite']['id'] == run['check_suite_id'] and check['head_sha'] == run['head_sha']
    assert check['details_url'] == f"https://github.com/{REPOSITORY}/actions/runs/{run['id']}/job/{job_id}"
    assert check['status'] == 'completed' and check['conclusion'] == job['conclusion']
    assert interval(check) == interval(job), 'Actions check is not the original job execution'
    log = api(f"actions/jobs/{job_id}/steps/{step['number']}/logs", log=True)
    records = []
    start, end = interval(step)
    for line in log.decode('utf-8-sig').splitlines():
        match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\S+Z) (\{"status":"published-and-verified",.*\})', line)
        if match:
            # GitHub job metadata rounds its timestamps down to seconds.
            assert start <= instant(match[1]) < end + timedelta(seconds=1)
            records.append(json.loads(match[2]))
    assert len(records) == 1, 'Missing or ambiguous verified publication record'
    record = records[0]
    assert set(record) == {'status', 'version', 'sourceRevision', 'manifestSha256', 'contractVersion', 'targets'}
    assert re.fullmatch(VERSION, record['version']) and re.fullmatch(SHA, record['sourceRevision'])
    assert re.fullmatch(DIGEST, record['manifestSha256'])
    assert type(record['contractVersion']) is int and record['contractVersion'] in (1, 2, 3)
    return record


def verify_v3_publication(event_run, version, source, manifest_sha):
    workflow = api('actions/workflows/publish-crate.yml')
    assert workflow['path'] == PUBLISHER_PATH and workflow['name'] == PUBLISHER
    workflow_id = positive(workflow['id'])
    dev = api('branches/dev')
    assert dev['name'] == 'dev' and dev['protected'] is True
    dev_sha = dev['commit']['sha']
    assert re.fullmatch(SHA, dev_sha)
    ancestor(source, dev_sha)

    if event_run:
        run = event_run
        assert run['status'] == 'completed' and run['conclusion'] == 'success'
        number = run['run_attempt']
    else:
        run_id = os.environ.get('INPUT_PUBLISHER_RUN_ID', '')
        attempt_input = os.environ.get('INPUT_PUBLISHER_ATTEMPT', '')
        assert re.fullmatch(r'[1-9][0-9]{0,15}', run_id) and re.fullmatch(r'[1-9][0-9]?', attempt_input), (
            'Manual v3 recovery requires publisher_run_id and publisher_attempt'
        )
        number = int(attempt_input)
        run = api(f'actions/runs/{run_id}')
        assert run['id'] == int(run_id), 'Unexpected publisher run'
    publisher_identity(run, workflow_id)
    assert 1 <= number <= run['run_attempt'], 'Unknown publisher attempt'
    head = run['head_sha']
    if head != source:
        ancestor(head, dev_sha)
    workflow_source = frozen_file(PUBLISHER_PATH, head)
    markers = re.findall(r'^# BAMBOO_SHARED_EXECUTABLES_V([0-9]+)\r?$', workflow_source, re.M)
    assert markers == ['3'] and '# BAMBOO_FROZEN_DEV_PUBLISHER_V1' in workflow_source.splitlines(), (
        'Publisher controller does not support the frozen v3 contract'
    )
    record = published_proof(run, exact_attempt(run, number))
    assert (record['version'], record['sourceRevision']) == (version, source), 'Publisher verified a different release'
    assert record['contractVersion'] == 3 and record['targets'] == TARGETS[3], 'Publisher verified a different contract'
    assert record['manifestSha256'] == manifest_sha, 'Manifest differs from the original verified v3 publication'


def completed_handoff(run):
    run_id, attempt = positive(run['id']), positive(run['run_attempt'])
    name = f'bamboo-completed-release-{run_id}-{attempt}'
    artifacts = complete_list(api(f'actions/runs/{run_id}/artifacts?name={name}&per_page=100'), 'artifacts')
    assert len(artifacts) == 1, 'Missing or ambiguous completed-release handoff'
    artifact = artifacts[0]
    assert artifact['name'] == name and artifact['expired'] is False
    assert positive(artifact['size_in_bytes']) <= 64 * 1024
    publisher = {
        'id': run_id, 'head_sha': run['head_sha'], 'head_branch': 'dev',
        'repository_id': REPOSITORY_ID, 'head_repository_id': REPOSITORY_ID,
    }
    assert all(artifact['workflow_run'][key] == value for key, value in publisher.items()), (
        'Completed-release handoff belongs to a different publisher'
    )
    raw = api(f"actions/artifacts/{artifact['id']}/zip", archive=True)
    assert len(raw) == artifact['size_in_bytes'] and artifact['digest'] == 'sha256:' + hashlib.sha256(raw).hexdigest()
    # Read only the expected JSON member. The archive never changes runner files.
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = archive.infolist()
        assert len(members) == 1 and members[0].filename == 'release.json', 'Unexpected completed-release handoff contents'
        member = members[0]
        assert 0 < member.file_size <= 4096 and not stat.S_ISLNK(member.external_attr >> 16)
        handoff = json.loads(archive.read(member))
    assert set(handoff) == {'runId', 'version', 'source'} and str(handoff['runId']) == str(run_id)
    return handoff


def release_inputs():
    assert os.environ['GITHUB_REPOSITORY'] == REPOSITORY
    event_run = None
    if os.environ['GITHUB_EVENT_NAME'] == 'workflow_run':
        with open(os.environ['GITHUB_EVENT_PATH']) as stream:
            event_run = json.load(stream)['workflow_run']
        assert event_run['event'] == 'workflow_dispatch' and event_run['conclusion'] == 'success'
        assert event_run['path'] == PUBLISHER_PATH and event_run['head_branch'] == 'dev'
        assert event_run['repository']['full_name'] == event_run['head_repository']['full_name'] == REPOSITORY
        assert re.fullmatch(SHA, event_run['head_sha'])
        handoff = completed_handoff(event_run)
        version, source = handoff['version'], handoff['source']
    else:
        assert os.environ['GITHUB_EVENT_NAME'] == 'workflow_dispatch'
        assert os.environ['GITHUB_REF'] == 'refs/heads/dev' and os.environ['GITHUB_REF_PROTECTED'] == 'true'
        version, source = os.environ['INPUT_VERSION'], os.environ['INPUT_SOURCE']
    assert re.fullmatch(VERSION, version)
    assert re.fullmatch(SHA, source)
    return event_run, version, source


def asset(assets, name):
    found = [item for item in assets if item['name'] == name]
    assert len(found) == 1, f'Missing published asset: {name}'
    item = found[0]
    assert item['state'] == 'uploaded' and type(item['size']) is int and item['size'] > 0
    assert type(item['id']) is int and item['id'] > 0
    return item


def original(assets, entry, name):
    assert entry['assetName'] == name and re.fullmatch(DIGEST, entry['sha256'])
    assert type(entry['sizeBytes']) is int
    executable = asset(assets, name)
    assert executable['size'] == entry['sizeBytes'] and executable['digest'] == 'sha256:' + entry['sha256']
    return {'id': executable['id'], 'size': executable['size'], 'sha256': entry['sha256']}


def verify_release():
    event_run, version, source = release_inputs()
    release = api('releases/tags/v' + version)
    assert release['tag_name'] == 'v' + version and release['target_commitish'] == source
    assert not release['draft'] and not release['prerelease'] and release['published_at']
    tag = api('git/ref/tags/v' + version)
    assert tag['object']['type'] == 'commit' and tag['object']['sha'] == source
    policy = frozen_file('scripts/sidecar-release.cjs', source)
    markers = re.findall(r'^// BAMBOO_SHARED_EXECUTABLES_V([0-9]+)\r?$', policy, re.M)
    assert markers in (['2'], ['3']), 'Unsupported or ambiguous frozen executable contract'
    contract = int(markers[0])
    assets = release['assets']
    assert len({item['name'] for item in assets}) == len(assets)
    assert len({item['id'] for item in assets}) == len(assets)
    manifest_name = f'bamboo-sidecars-v{contract}.json'
    provenance_name = f'bamboo-executables-provenance-v{contract}.json'
    for item in assets:
        if re.fullmatch(r'bamboo-(sidecars|executables-provenance)-v[0-9]+\.json', item['name']):
            assert item['name'] in {manifest_name, provenance_name}, 'Mixed executable release contracts'
    asset(assets, provenance_name)
    asset(assets, 'release-provenance.json')
    manifest_asset = asset(assets, manifest_name)
    assert manifest_asset['size'] < 1024 * 1024
    raw = api('releases/assets/' + str(manifest_asset['id']), binary=True)
    assert len(raw) == manifest_asset['size']
    manifest_sha = hashlib.sha256(raw).hexdigest()
    assert manifest_asset['digest'] == 'sha256:' + manifest_sha
    manifest = json.loads(raw)
    assert manifest['schemaVersion'] == contract and manifest['version'] == version and manifest['sourceRevision'] == source
    entries = manifest['artifacts']
    assert [entry['target'] for entry in entries] == TARGETS[contract], 'Incomplete or unexpected executable targets'
    binary_names = {item['name'] for item in assets if
                    re.fullmatch(r'bamboo-(aarch64|x86_64)-(apple-darwin|pc-windows-msvc\.exe|unknown-linux-gnu)', item['name'])}
    assert binary_names == {entry['assetName'] for entry in entries}, 'Unexpected release binaries'
    linux_assets = {}
    for entry in entries:
        target = entry['target']
        suffix = '.exe' if target.endswith('-windows-msvc') else ''
        assert entry['format'] == 'raw' and entry['buildMode'] == 'embedded' and entry['embeddedFrontend'] is True
        assert 65536 <= entry['sizeBytes'] <= 1024 ** 3
        identity = original(assets, entry, f'bamboo-{target}{suffix}')
        if target == 'x86_64-unknown-linux-gnu':
            linux_assets['amd64'] = identity
        if target == 'aarch64-unknown-linux-gnu':
            linux_assets['arm64'] = identity
    original(assets, manifest['browserHost'], 'bamboo-browser-host-v1.tar.gz')
    if contract == 3:
        verify_v3_publication(event_run, version, source, manifest_sha)
    else:
        linux_assets['arm64'] = {'id': '', 'size': '', 'sha256': ''}
    return {'source': source, 'version': version, 'contract': contract,
            'linux_assets': json.dumps(linux_assets, separators=(',', ':'))}


if __name__ == '__main__':
    values = verify_release()
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        for key, value in values.items():
            output.write(f'{key}={value}\n')
