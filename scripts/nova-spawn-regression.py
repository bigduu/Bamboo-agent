"""Isolated Linux R/G evidence controller, never a release-source gate."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

RED = 'b9d72ba72cffb7e6fa550881647599c7a95e841c'
GREEN = 'fda0f5f2459b3d52c7be5997d9b5655d6b811aa3'
BASELINE = '6963521d987a71a09ef6ccf69927521e594c65d3'
TREES = {RED: '85e0623d08e06c40d2fa7530af35fa24bcfa8e45',
         GREEN: 'e0f4c5f87dac5716ce206086ea88bfdb85ba3eb6'}
PREFIX = 'handlers::agent::plugin::official_nova::'
BUSY = PREFIX + 'tests::spawn_busy::'
CASES = {BUSY + suffix for suffix in [
    'busy_snapshot_recovers_after_writer_release_without_duplicate_execution',
    'persistent_busy_writer_stops_at_the_original_operation_budget',
    'permanent_spawn_error_fails_without_a_retry_yield_or_execution',
    'persistent_busy_writer_bounds_real_process_creation_attempts',
]}
EXPECTED_RED_FAILURES = {BUSY + 'persistent_busy_writer_bounds_real_process_creation_attempts'}
OUT = Path(os.environ['RUNNER_TEMP']) / 'nova-spawn-red-green'
OUT.mkdir(exist_ok=False)
WORKSPACE = Path.cwd().resolve()
CONTROLLER = os.environ['GITHUB_SHA']
report = {'controllerEventSource': CONTROLLER, 'redSource': RED,
          'greenSource': GREEN, 'historicalFailureErrnoStillUnknown': True,
          'releaseGate': False, 'localMacRustUsed': False, 'stages': []}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()


def clean(source):
    assert git('rev-parse', 'HEAD') == source
    assert git('rev-parse', 'HEAD^{tree}') == TREES[source]
    assert not git('status', '--porcelain', '--untracked-files=all')


def write(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2) + '\n')


def execute(command, name):
    started = time.monotonic()
    print(json.dumps({'stage': name, 'command': command, 'source': git('rev-parse', 'HEAD')}), flush=True)
    stdout = OUT / (name + '.stdout')
    stderr = OUT / (name + '.stderr')
    with stdout.open('xb') as output, stderr.open('xb') as error:
        result = subprocess.run(command, stdout=output, stderr=error)
    receipt = {'command': command, 'exitCode': result.returncode,
               'seconds': time.monotonic() - started,
               'stdout': stdout.name, 'stdoutSha256': sha(stdout),
               'stderr': stderr.name, 'stderrSha256': sha(stderr)}
    write(name + '.json', receipt)
    print(json.dumps({'stage': name, **receipt}), flush=True)
    return result.returncode, stdout, stderr, receipt


def compile_source(source, stage):
    git('checkout', '--detach', source)
    clean(source)
    code, output, error, receipt = execute([
        'cargo', 'test', '--locked', '--all-features', '-p', 'bamboo-server',
        '--lib', '--no-run', '--message-format=json'], stage + '-compile')
    assert code == 0, error.read_text()[-5000:]
    units = [json.loads(line) for line in output.read_text().splitlines() if line.strip()]
    artifacts = [unit for unit in units if unit.get('reason') == 'compiler-artifact'
                 and unit.get('target', {}).get('name') == 'bamboo_server'
                 and unit.get('profile', {}).get('test') and unit.get('executable')]
    artifact, = artifacts
    assert artifact['fresh'] is False, 'Changed bamboo_server test unit must be compiled for this source'
    assert Path(artifact['manifest_path']).resolve() == WORKSPACE / 'crates/app/bamboo-server/Cargo.toml'
    binary = Path(artifact['executable']).resolve()
    assert binary.is_relative_to(WORKSPACE / 'target'), binary
    receipt.update({'source': source, 'tree': TREES[source],
                    'binary': str(binary), 'binarySha256': sha(binary),
                    'workspaceTestUnit': artifact,
                    'cargoLockSha256': sha(WORKSPACE / 'Cargo.lock')})
    write(stage + '-compile.json', receipt)
    clean(source)
    return binary, receipt


def run_cases(binary, stage, prefix):
    before = sha(binary)
    code, log, error, receipt = execute([
        str(binary), prefix, '--test-threads=1', '--nocapture'], stage + '-runtime')
    assert sha(binary) == before
    text = log.read_text() + '\n' + error.read_text()
    results = re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', text)
    result, = results
    receipt.update({'summary': {'status': result[0], 'passed': int(result[1]),
                               'failed': int(result[2]), 'ignored': int(result[3])},
                    'binarySha256': before,
                    'passedCases': sorted(re.findall(r'^test (\S+) \.\.\. ok$', text, re.M)),
                    'failedCases': sorted(re.findall(r'^test (\S+) \.\.\. FAILED$', text, re.M))})
    write(stage + '-runtime.json', receipt)
    return code, receipt


try:
    assert git('rev-parse', 'HEAD') == CONTROLLER
    assert git('rev-parse', 'HEAD^') == GREEN
    assert not git('status', '--porcelain', '--untracked-files=all')
    assert sorted(git('diff', '--name-only', GREEN, CONTROLLER).splitlines()) == sorted([
        '.github/workflows/nova-spawn-regression.yml', 'scripts/nova-spawn-regression.py'])
    assert git('rev-parse', GREEN + '^') == RED
    assert git('rev-parse', RED + '^') == BASELINE
    compiler = subprocess.check_output(['rustc', '-Vv'], text=True)
    assert compiler.startswith('rustc 1.99.1 (58cf8f9c3 2026-10-13)\n')
    assert os.environ['RUST_MIN_STACK'] == '8388608'
    report['compiler'] = compiler
    report['controllerScriptSha256'] = sha(Path(__file__))
    write('source.json', report)

    red_binary, red_build = compile_source(RED, 'red')
    listed = subprocess.check_output([str(red_binary), BUSY, '--list'], text=True)
    assert {line.removesuffix(': test') for line in listed.splitlines()
            if line.endswith(': test')} == CASES
    red_code, red_run = run_cases(red_binary, 'red', BUSY)
    report['stages'].append({'name': 'red', 'compile': red_build, 'runtime': red_run})
    clean(RED)
    assert red_code == 101, red_run
    assert red_run['summary'] == {'status': 'FAILED', 'passed': 3, 'failed': 1, 'ignored': 0}, red_run
    assert set(red_run['failedCases']) == EXPECTED_RED_FAILURES, red_run
    write('red-verified.json', {'source': RED, 'tree': TREES[RED], 'expectedNativeRedVerified': True,
                                'runtime': red_run, 'historicalFailureCauseProven': False})

    green_binary, green_build = compile_source(GREEN, 'green')
    green_code, green_run = run_cases(green_binary, 'green', BUSY)
    report['stages'].append({'name': 'green', 'compile': green_build, 'runtime': green_run})
    clean(GREEN)
    assert green_code == 0, green_run
    assert green_run['summary'] == {'status': 'ok', 'passed': 4, 'failed': 0, 'ignored': 0}, green_run
    assert CASES <= set(green_run['passedCases']), green_run
    report['boundedLinuxRegressionPassed'] = True
except BaseException as error:
    report['errorType'] = type(error).__name__
    report['error'] = str(error)
    report['boundedLinuxRegressionPassed'] = False
    raise
finally:
    report['finishedAt'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    report['actualFinalHead'] = git('rev-parse', 'HEAD')
    report['finalSourceStatus'] = git('status', '--porcelain', '--untracked-files=all')
    write('result.json', report)
