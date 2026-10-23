'use strict';
// Controller fixtures only: no Cargo, Rust compilation, providers or real release acceptance.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawnSync } = require('node:child_process');
const r = require('./test-groups.cjs');
const clone = value => JSON.parse(JSON.stringify(value));
const record = (pkg, target, names, kind = ['lib']) => ({ id: pkg + '/' + target, package: pkg, target, kind,
  tests: names.map(name => ({ name, ignored: false })) });
function temp(t) {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-groups-')));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true })); return directory;
}
function git(root, args) {
  const result = spawnSync('git', args, { cwd: root, encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr); return result.stdout;
}
function fixture(t, profileName = 'default') {
  const base = temp(t), root = path.join(base, 'source'), evidence = path.join(base, 'evidence');
  fs.mkdirSync(root); fs.writeFileSync(path.join(root, '.gitignore'), '/target/\n');
  fs.writeFileSync(path.join(root, 'Cargo.toml'), '[workspace]\n');
  git(root, ['init', '-q']); git(root, ['add', '.']);
  git(root, ['-c', 'user.name=Controller Fixture', '-c', 'user.email=fixture@invalid', '-c', 'commit.gpgsign=false', 'commit', '-qm', 'fixture source']);
  const targetDir = path.join(root, 'target'); fs.mkdirSync(targetDir);
  const sysroot = path.join(base, 'toolchain'), libdir = path.join(sysroot, 'lib');
  fs.mkdirSync(path.join(sysroot, 'bin'), { recursive: true }); fs.mkdirSync(libdir);
  for (const tool of ['cargo', 'rustc']) fs.writeFileSync(path.join(sysroot, 'bin', tool + (process.platform === 'win32' ? '.exe' : '')), 'fixture toolchain bytes');
  const config = clone(r.CONFIG);
  // Fixture executable protocol uses a Node entrypoint on POSIX. Production Windows keeps .exe artifacts.
  const samples = [
    record('bamboo-config', 'bamboo_config', ['credentials::encrypt', 'credentials::ignored', 'credential_migration::share_transaction::tests::native::crash']),
    record('bamboo-llm', 'bamboo_llm', ['retry::bounded']),
    record('bamboo-engine', 'bamboo_engine', ['session_app::approval_replay::live', 'runtime::compression::budget']),
    record('bamboo-server', 'frontend_build_contract', ['frontend_build::manifest'], ['test']),
    record('bamboo-server', 'ordinary_root_owned_inbox_restart', ['real_host_loss'], ['test']),
    record('bamboo-server', 'sprint_browser_fixture', ['browser_fixture'], ['test']),
    record('bamboo-server', 'empty_platform_target', [], ['test']),
  ];
  samples[0].tests[1].ignored = true;
  const marker = path.join(base, 'fail-llm');
  const packages = [...new Set(samples.map(s => s.package))].map(name => {
    const packageDir = name === 'bamboo-config' ? root : path.join(root, 'target', name); fs.mkdirSync(packageDir, { recursive: true });
    const manifest_path = path.join(packageDir, 'Cargo.toml');
    if (packageDir !== root) fs.writeFileSync(manifest_path, '[package]\n');
    return { id: 'fixture#' + name, name, version: '0.0.0', manifest_path, features: { default: [], 'test-utils': [] },
      targets: samples.filter(s => s.package === name).map(s => ({ name: s.target, kind: s.kind, test: true })) };
  });
  const units = samples.map((sample, index) => {
    const executable = path.join(targetDir, `libtest-${index}.cjs`);
    const body = `#!/usr/bin/env node
const fs = require('node:fs');
const tests = ${JSON.stringify(sample.tests)};
const args = process.argv.slice(2);
const filter = args.find(a => !a.startsWith('--'));
let selected = tests.filter(t => !filter || (args.includes('--exact') ? t.name === filter : t.name.includes(filter)));
if (args.includes('--ignored')) selected = selected.filter(t => t.ignored);
if (args.includes('--list')) {
  for (const t of selected) console.log(t.name + ': test');
  console.log(selected.length + (selected.length === 1 ? ' test' : ' tests') + ', 0 benchmarks');
} else {
  if (process.env.CARGO_MANIFEST_DIR !== process.cwd() || process.env.CARGO_PKG_NAME !== ${JSON.stringify(sample.package)} || process.env.RUST_MIN_STACK !== '8388608') process.exit(91);
  const ignored = selected.filter(t => t.ignored && !args.includes('--ignored') && !args.includes('--include-ignored')).length;
  const failed = ${sample.package === 'bamboo-llm'} && fs.existsSync(${JSON.stringify(marker)}) ? 1 : 0;
  console.log('running ' + selected.length + ' tests');
  console.error('fixture stderr retained');
  console.log('test result: ' + (failed ? 'FAILED' : 'ok') + '. ' + (selected.length - ignored - failed) + ' passed; ' + failed + ' failed; ' + ignored + ' ignored; 0 measured; ' + (tests.length - selected.length) + ' filtered out; finished in 0.01s');
  process.exitCode = failed ? 7 : 0;
}
`;
    fs.writeFileSync(executable, body); fs.chmodSync(executable, 0o755);
    const pkg = packages.find(p => p.name === sample.package);
    return { reason: 'compiler-artifact', package_id: pkg.id, manifest_path: pkg.manifest_path, target: { name: sample.target, kind: sample.kind },
      profile: { test: true, opt_level: '0' }, executable, filenames: [executable], features: [] };
  });
  const app = path.join(targetDir, 'bamboo-application'); fs.writeFileSync(app, 'application bytes referenced by CARGO_BIN_EXE');
  const dylib = path.join(targetDir, 'fixture.dylib'); fs.writeFileSync(dylib, 'runtime shared library bytes');
  units.push({ reason: 'compiler-artifact', package_id: packages[0].id, target: { name: 'bamboo', kind: ['bin'] },
    profile: { test: false }, executable: app, filenames: [app, dylib], features: [] });
  const metadata = { packages, workspace_default_members: packages.map(p => p.id) };
  const cargoOutput = () => [...units,
    { reason: 'build-script-executed', package_id: packages[0].id, out_dir: targetDir, linked_paths: [], env: [['BAMBOO_GROUP_BUILD_FIXTURE', 'build-script-context']] },
    { reason: 'build-finished', success: true }].map(JSON.stringify).join('\n');
  const calls = [];
  const execute = async (command, args, options) => {
    if (!['cargo', 'rustc'].includes(command)) return r.processRun(command, args, options);
    calls.push({ command, args });
    const stdout = args[0] === '--version' ? 'fixture ' + command + ' 1.95' : args.includes('sysroot') ? sysroot : args.includes('target-libdir') ? libdir : args[0] === 'metadata' ? JSON.stringify(metadata) : cargoOutput();
    fs.writeFileSync(options.log + '.stdout.log', stdout, { flag: 'wx' }); fs.writeFileSync(options.log + '.stderr.log', '', { flag: 'wx' });
    return { command, args, cwd: options.cwd, log: options.log, exitCode: 0, signal: null, timedOut: false, interrupted: false, error: null, durationMs: 1, stdout, stderr: '' };
  };
  return { root, evidence, config, marker, packages, units, calls, execute, app, dylib,
    prepare: () => r.compile(root, evidence, profileName, { config, execute }) };
}

test('actual package/target routes separate costly cases before broad thematic groups', () => {
  r.validateConfig(r.CONFIG);
  const cases = [
    ['bamboo-config', 'bamboo_config', ['lib'], 'encryption::roundtrip', 'config-credentials'],
    ['bamboo-config', 'bamboo_config', ['lib'], 'credential_migration::share_transaction::tests::native::crash', 'cross-process'],
    ['bamboo-llm', 'bamboo_llm', ['lib'], 'retry::tests::timeout', 'llm-retry'],
    ['bamboo-engine', 'bamboo_engine', ['lib'], 'session_app::approval_replay::test', 'session-permissions'],
    ['bamboo-engine', 'bamboo_engine', ['lib'], 'runtime::compression::budget', 'engine-compression'],
    ['bamboo-server', 'frontend_build_contract', ['test'], 'frontend_build::tests::zip', 'frontend-resources'],
    ['bamboo-server', 'ordinary_root_owned_inbox_restart', ['test'], 'claimed_owned_input_survives_real_host_process_loss_and_restarts_once', 'restart-takeover'],
    ['bamboo-orchestration', 'patrol_restart', ['test'], 'restart', 'restart-takeover'],
    ['bamboo-subagent', 'e2e_subprocess', ['test'], 'child', 'cross-process'],
    ['bamboo-broker', 'bus_e2e', ['test'], 'bus', 'cross-process'],
    ['bamboo-server', 'sprint_browser_fixture', ['test'], 'lotus_sprint_browser_fixture', 'browser'],
    ['bamboo-server', 'bamboo_server', ['lib'], 'server::tls::tests::handshake', 'platform'],
    ['bamboo-storage', 'bamboo_storage', ['lib'], 'v2::actor_snapshot_windows_tests::regular_file_symlink_replacement_is_rejected', 'platform'],
    ['bamboo-storage', 'bamboo_storage', ['lib'], 'v2::actor_directory::tests::persisted_session_precedes_claim_and_attempt_survives_restart', 'platform'],
    ['unmapped-package', 'new_target', ['test'], 'new_test', 'other'],
  ];
  for (const [pkg, target, kind, name, group] of cases) assert.equal(r.route(record(pkg, target, [], kind), name), group);
  const ambiguous = clone(r.CONFIG); ambiguous.routes.push({ ...ambiguous.routes[0], group: 'cross-process' });
  assert.throws(() => r.route(record('bamboo-server', 'ordinary_root_owned_inbox_restart', [], ['test']), 'test', ambiguous), /Ambiguous/);
});

test('strict --list catches wrong counts, duplicate/missing names, benchmarks and unexpected output', () => {
  assert.deepEqual(r.parseList('one: test\n1 test, 0 benchmarks\n'), ['one']);
  assert.deepEqual(r.parseList('0 tests, 0 benchmarks\n'), []);
  for (const output of ['one: test\n0 tests, 0 benchmarks', 'one: test\none: test\n2 tests, 0 benchmarks',
    'one: test', 'one: benchmark\n0 tests, 1 benchmark', 'noise\n0 tests, 0 benchmarks']) assert.throws(() => r.parseList(output));
  assert.throws(() => r.equalNames(['wrong'], ['expected']), /names differ/);
});

test('partition covers each name exactly once despite substring collisions and new namespaces', () => {
  const target = record('x', 'x', ['one::a', 'one::b', 'two::a', 'prefix::one::c', 'root', 'root_more']);
  const commands = r.partition(target, ['one::a', 'one::b', 'root']);
  assert.deepEqual(commands.flatMap(c => c.names).sort(), ['one::a', 'one::b', 'root']);
  assert(commands.every(c => c.args.includes('--exact')), 'impure namespace and name-prefix collision require exact selections');
  const groups = r.plan({ targets: [target] });
  assert.equal(Object.values(groups).flat().flatMap(c => c.names).length, target.tests.length);
  const duplicate = clone(target); duplicate.tests.push(duplicate.tests[0]);
  assert.throws(() => r.plan({ targets: [duplicate] }), /Duplicate inventory/);
  assert.throws(() => r.plan({ targets: [record('x', 'x', [])] }), /No tests/);
});

test('unknown, zero, duplicate or implicit selections fail closed', () => {
  const groups = { one: [{ names: ['a'] }], empty: [] };
  assert.deepEqual(r.selectGroups(groups, [], true), ['one']);
  for (const [names, all] of [[['unknown'], false], [['empty'], false], [['one', 'one'], false], [[], false], [['one'], true]])
    assert.throws(() => r.selectGroups(groups, names, all));
  assert.throws(() => r.argumentsFor(['run', '--evidence', '/tmp/e', '--typo']), /Unknown option/);
  assert.throws(() => r.argumentsFor(['run', '--profile', 'default']), /profile comes/);
  assert.throws(() => r.argumentsFor(['compile', '--profile', 'default', '--all']), /requires only/);
});

test('runtime counts preserve ignored semantics and reject zero, measured or duplicate summaries', () => {
  const summary = 'test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 9 filtered out; finished in 0.1s';
  assert.deepEqual(r.parseCounts(summary, 3, 1), { passed: 2, failed: 0, ignored: 1, measured: 0, filtered: 9 });
  assert.throws(() => r.parseCounts(summary, 3, 0), /counts differ/);
  assert.throws(() => r.parseCounts(summary, 0, 1), /counts differ/);
  assert.throws(() => r.parseCounts(summary + '\n' + summary, 3, 1), /duplicate/);
  assert.throws(() => r.parseCounts(summary.replace('0 measured', '1 measured'), 3, 1), /counts differ/);
});

test('profile identity retains Cargo target/features, required-feature omissions and native contracts', () => {
  assert.deepEqual(r.CONFIG.profiles.default.cargoArgs, []);
  assert.deepEqual(r.CONFIG.profiles['all-features'].cargoArgs, ['--all-features', '--lib', '--tests']);
  assert.deepEqual(r.CONFIG.profiles['storage-standalone'].cargoArgs, ['-p', 'bamboo-storage', '--lib']);
  assert.deepEqual(r.CONFIG.profiles['macos-server-dev'].cargoArgs, ['--profile', 'dev', '-p', 'bamboo-server', '--all-features', '--lib']);
  assert.equal(r.CONFIG.profiles['macos-server-dev'].rejectCompileDiagnostic, '__eh_frame section too large');
  const metadata = { workspace_default_members: ['server'], packages: [{ id: 'server', name: 'bamboo-server', features: { 'test-utils': [] },
    manifest_path: '/fixture/Cargo.toml', targets: [{ name: 'env_cache_isolation', kind: ['test'], test: true, 'required-features': ['test-utils'] }] }] };
  assert.equal(r.metadataTargets(metadata, r.CONFIG.profiles.default, []).excluded.length, 1);
  assert.equal(r.metadataTargets(metadata, r.CONFIG.profiles['all-features'], []).expected.length, 1);
  assert.throws(() => r.cargoArtifacts('{"reason":"build-finished","success":false}'), /successful/);
  assert.throws(() => r.cargoArtifacts(''), /successful/);
  assert.notEqual(r.digest(r.CONFIG.profiles.default), r.digest(r.CONFIG.profiles['all-features']));
});

test('compile once, strict list, per-group execution and failed-only retry retain earlier evidence', async t => {
  const f = fixture(t), manifest = await f.prepare();
  assert.equal(f.calls.filter(c => c.args[0] === 'test').length, 1);
  assert.equal(manifest.targets.length, 7);
  assert.equal(manifest.targets[0].buildEnv.BAMBOO_GROUP_BUILD_FIXTURE, 'build-script-context');
  assert.equal(manifest.targets[0].outDir, path.join(f.root, 'target'));
  assert(manifest.artifacts.some(a => a.path === f.app)); assert(manifest.artifacts.some(a => a.path === f.dylib));
  const listing = await r.runGroups(f.root, f.evidence, [], { all: true, listOnly: true, config: f.config });
  assert(listing.listResults.every(g => g.status === 'listed'));
  assert.equal(listing.summary.completeWithinProfile, false);
  assert.equal(listing.summary.counts.passed, 0);
  fs.writeFileSync(f.marker, 'fail the synthetic LLM test');
  const initial = await r.runGroups(f.root, f.evidence, [], { all: true, config: f.config });
  assert.equal(initial.summary.completeWithinProfile, false);
  assert.equal(initial.summary.groups['llm-retry'].status, 'failed');
  assert.equal(initial.summary.counts.failed, 1); assert.equal(initial.summary.counts.ignored, 1);
  const failed = initial.results.find(g => g.group === 'llm-retry');
  assert.equal(failed.exitCode, 1); assert(failed.processes.some(p => p.exitCode === 7));
  const failedLog = failed.processes.find(p => p.exitCode === 7).log;
  assert.match(fs.readFileSync(failedLog + '.stderr.log', 'utf8'), /stderr retained/);
  assert(initial.summary.zeroTestTargets.includes('bamboo-server/empty_platform_target'));
  assert.equal(initial.summary.fullAcceptance, false); assert(initial.summary.notCovered.some(s => /Doctests/.test(s)));
  fs.unlinkSync(f.marker);
  const retried = await r.runGroups(f.root, f.evidence, [], { retryFailed: true, config: f.config });
  assert.deepEqual(retried.selected, ['llm-retry']);
  assert.equal(retried.summary.completeWithinProfile, true); assert.equal(retried.summary.counts.failed, 0);
  assert.equal(retried.summary.groups['config-credentials'].attempt, initial.summary.groups['config-credentials'].attempt);
  assert(fs.existsSync(failedLog + '.stdout.log'), 'failed attempt is retained');
  assert.equal(f.calls.filter(c => c.args[0] === 'test').length, 1, 'runtime never invokes Cargo');
  await assert.rejects(() => r.runGroups(f.root, f.evidence, [], { retryFailed: true, config: f.config }), /Select --group/);
});

test('partial runs stay incomplete; only/include are distinct explicit cache identities', async t => {
  const f = fixture(t), manifest = await f.prepare();
  const partial = await r.runGroups(f.root, f.evidence, ['config-credentials'], { config: f.config });
  assert.equal(partial.summary.completeWithinProfile, false);
  assert.equal(partial.summary.groups['llm-retry'].status, 'not-run');
  const only = await r.runGroups(f.root, f.evidence, ['config-credentials'], { mode: 'only', config: f.config });
  assert.equal(only.results[0].counts.ignored, 0); assert.equal(only.results[0].counts.passed, 1);
  assert.equal(only.summary.completeWithinSelection, true);
  assert.equal(only.summary.completeWithinProfile, false, 'ignored-only success cannot certify the regular inventory');
  assert.equal(only.summary.coverage.outsideIgnoredMode, 8);
  const include = await r.runGroups(f.root, f.evidence, ['config-credentials'], { mode: 'include', config: f.config });
  assert.equal(include.results[0].counts.ignored, 0); assert.equal(include.results[0].counts.passed, 2);
  assert.notEqual(include.results[0].selectionKey, partial.results[0].selectionKey);
  assert.notEqual(only.results[0].selectionKey, include.results[0].selectionKey);
  await assert.rejects(() => r.runGroups(f.root, f.evidence, ['llm-retry'], { mode: 'only', config: f.config }), /zero matching/);
  await assert.rejects(() => r.runGroups(f.root, f.evidence, ['missing'], { config: f.config }), /Unknown group/);
  const groups = r.plan(manifest), result = clone(partial.results[0]);
  result.counts.passed++;
  assert.throws(() => r.aggregate(manifest, groups, [result]), /Incomplete passed/);
  result.inputKey = 'old'; assert.throws(() => r.aggregate(manifest, groups, [result]), /stale result/);
});

test('missing/duplicate Cargo targets cannot produce a compiled receipt', async t => {
  for (const mutation of ['missing', 'duplicate']) {
    const f = fixture(t);
    if (mutation === 'missing') f.units.splice(0, 1); else f.units.push(f.units[0]);
    await assert.rejects(() => f.prepare(), /Expected one Cargo test artifact/);
    assert(!fs.existsSync(path.join(f.evidence, 'artifacts.json')));
    assert(fs.existsSync(path.join(f.evidence, 'compile-failure.json')));
  }
});

test('cache binding rejects source, profile, application, shared library and test executable changes', async t => {
  const f = fixture(t), manifest = await f.prepare();
  for (const filename of [f.app, f.dylib, manifest.targets[0].executable]) {
    const before = fs.readFileSync(filename); fs.writeFileSync(filename, 'changed bytes');
    assert.throws(() => r.assertManifest(manifest, f.root, f.config), /Artifact bytes changed/); fs.writeFileSync(filename, before);
  }
  const config = clone(f.config); config.profiles.default.cargoArgs.push('--all-features');
  assert.throws(() => r.assertManifest(manifest, f.root, config), /inputs changed/);
  const cargoConfig = path.join(path.dirname(f.root), '.cargo', 'config.toml');
  fs.mkdirSync(path.dirname(cargoConfig)); fs.writeFileSync(cargoConfig, '[build]\nrustflags = ["-C", "debuginfo=0"]\n');
  assert.throws(() => r.assertManifest(manifest, f.root, f.config), /inputs changed/); fs.unlinkSync(cargoConfig);
  const tampered = clone(manifest); tampered.profileName = 'all-features';
  assert.throws(() => r.assertManifest(tampered, f.root, f.config), /cache identity/);
  fs.writeFileSync(path.join(f.root, 'untracked'), 'source drift');
  assert.throws(() => r.assertManifest(manifest, f.root, f.config), /Source must be clean/);
  git(f.root, ['add', 'untracked']); git(f.root, ['-c', 'user.name=Controller Fixture', '-c', 'user.email=fixture@invalid', '-c', 'commit.gpgsign=false', 'commit', '-qm', 'new SHA']);
  assert.throws(() => r.assertManifest(manifest, f.root, f.config), /inputs changed/);
});

test('nonzero exit cannot be hidden by green counts; all other groups still finish', async t => {
  const f = fixture(t); await f.prepare();
  const execute = async (command, args, options) => {
    const result = await r.processRun(command, args, options);
    if (command.includes('libtest-1') && !args.includes('--list')) result.exitCode = 23;
    return result;
  };
  const run = await r.runGroups(f.root, f.evidence, [], { all: true, config: f.config, execute });
  assert.equal(run.summary.groups['llm-retry'].status, 'failed');
  assert.equal(run.summary.counts.failed, 0, 'libtest claimed green, but process failure remains visible');
  assert.equal(run.summary.completeWithinProfile, false);
  assert.equal(run.summary.groups.browser.status, 'passed');
});

test('changed --list names fail before runtime and retain list exit/log evidence', async t => {
  const f = fixture(t); await f.prepare(); let runtimeCalls = 0;
  const execute = async (command, args, options) => {
    if (!args.includes('--list')) runtimeCalls++;
    const result = await r.processRun(command, args, options);
    return { ...result, stdout: 'surprise: test\n1 test, 0 benchmarks\n' };
  };
  const run = await r.runGroups(f.root, f.evidence, ['llm-retry'], { config: f.config, execute });
  assert.equal(runtimeCalls, 0); assert.equal(run.results[0].status, 'failed');
  assert.equal(run.results[0].processes[0].exitCode, 0); assert.match(run.results[0].error, /names differ/);
});

test('group timeout fails its receipt and the following independent groups still run', async t => {
  const f = fixture(t); f.config.groups['llm-retry'].timeoutSeconds = 0.001; await f.prepare();
  const run = await r.runGroups(f.root, f.evidence, ['llm-retry', 'frontend-resources'], { config: f.config });
  assert.equal(run.summary.groups['llm-retry'].status, 'failed');
  assert(run.summary.groups['llm-retry'].processes.some(p => p.timedOut));
  assert.equal(run.summary.groups['frontend-resources'].status, 'passed');
  assert.equal(run.summary.completeWithinProfile, false);
});

test('timeout retains live stdout/stderr, kills only its own process group and records failure', async t => {
  const directory = temp(t), log = path.join(directory, 'timeout');
  const result = await r.processRun(process.execPath, ['-e', 'console.log("before timeout"); console.error("before stderr"); setTimeout(() => {}, 10000);'],
    { cwd: directory, timeoutMs: 300, log });
  assert.equal(result.timedOut, true); assert.notEqual(result.exitCode, 0);
  assert.match(fs.readFileSync(log + '.stdout.log', 'utf8'), /before timeout/);
  assert.match(fs.readFileSync(log + '.stderr.log', 'utf8'), /before stderr/);
});
