'use strict';
// No Cargo, Rust, artifact service or provider calls in these fixtures.
const {test} = require('node:test');
const {spawnSync} = require('node:child_process');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const r = require('./test-groups.cjs');
const ci = require('./test-groups-ci.cjs');
const copy = value => JSON.parse(JSON.stringify(value));
function fixture(profile = 'default') {
  const target = (pkg, name, length, kind = ['lib']) => ({id: pkg + '/' + name, package: pkg, target: name, kind,
    tests: Array.from({length}, (_, i) => ({name: `tests::case_${String(i).padStart(4, '0')}`, ignored: i === 0}))});
  const manifest = {profileName: profile, identity: {sha: 'a'.repeat(40), tree: 'b'.repeat(40)},
    targets: [target('bamboo-config', 'bamboo_config', 260), target('bamboo-engine', 'runtime', 35, ['test'])]};
  manifest.inputKey = r.digest(manifest);
  const docs = [{id: 'bamboo-config-doc', package: 'bamboo-config', names: ['src/lib.rs - (line 1)'], ignoredNames: []}];
  const plan = {schema: 'bamboo-ci-test-plan/v1', manifest, docs, producerAttempt: 1, blocks: ci.makeBlocks(manifest, docs)};
  plan.planKey = r.digest(plan); return plan;
}
function actions(t) {
  const previous = {...process.env};
  process.env.GITHUB_RUN_ID = '77'; process.env.GITHUB_RUN_ATTEMPT = '2';
  t.after(() => { for (const key of ['GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT']) if (previous[key] === undefined) delete process.env[key]; else process.env[key] = previous[key]; });
}
function receipts(plan) {
  const jobs = [], results = [];
  for (const [index, block] of plan.blocks.entries()) {
    const attempt = index % 2 + 1;
    jobs.push({id: 100 + index, run_id: 77, run_attempt: attempt, name: ci.jobName(plan, block), status: 'completed', conclusion: 'success'});
    const ignored = block.kind === 'doctest' ? 0 : block.commands.reduce((n, c) => n + c.ignored, 0);
    results.push({planKey: plan.planKey, block: block.id, package: block.package, profile: plan.manifest.profileName,
      sha: plan.manifest.identity.sha, sourceTree: plan.manifest.identity.tree, attempt, status: 'passed', exitCode: 0,
      expected: block.names.length, counts: {passed: block.names.length - ignored, failed: 0, ignored}, testDurationMs: 123,
      timeoutSeconds: 600, processes: Array.from({length: block.kind === 'doctest' ? 2 : 2 * block.commands.length}, () => ({exitCode: 0, timedOut: false, interrupted: false, signal: null, error: null}))});
  }
  return {jobs, results};
}
test('each block contains exactly one crate/target, small stable inventories and a hard 600s cap', () => {
  const plan = fixture(); ci.validatePlan(plan);
  assert.equal(plan.blocks.filter(b => b.package === 'bamboo-config' && b.kind === 'libtest').length, 3);
  assert.equal(plan.blocks.filter(b => b.package === 'bamboo-engine').length, 3);
  for (const block of plan.blocks) {
    assert.equal(block.timeoutSeconds, 600);
    if (block.commands) for (const command of block.commands) assert(command.targetId.startsWith(block.package + '/'));
  }
  assert.deepEqual(ci.makeBlocks(plan.manifest, plan.docs), plan.blocks);
  assert.equal(plan.blocks.filter(b => b.kind !== 'doctest').reduce((n, b) => n + b.names.length, 0), 295);
  const changed = copy(plan); changed.blocks.pop(); changed.planKey = r.digest({...changed, planKey: undefined});
  assert.throws(() => ci.validatePlan(changed), /Missing, duplicated/);
  const duplicate = copy(plan.manifest); duplicate.targets[0].tests.push(duplicate.targets[0].tests[0]);
  assert.throws(() => ci.makeBlocks(duplicate), /Duplicate|names differ/);
});
test('zero-test crates and matrix overflow fail instead of omitting or skipping coverage', () => {
  const plan = fixture(), zero = copy(plan.manifest); zero.targets[0].tests = [];
  assert.throws(() => ci.makeBlocks(zero), /zero tests/);
  const overflow = copy(plan.manifest);
  overflow.targets = Array.from({length: 257}, (_, i) => ({...copy(plan.manifest.targets[0]), id: 'target-' + i, target: 'target_' + i,
    tests: [{name: 'case', ignored: false}]}));
  assert.throws(() => ci.makeBlocks(overflow), /matrix exceeds/);
});
test('same-run mixed attempts retain successes and require the actual latest attempt per block', t => {
  actions(t); const plan = fixture(), {jobs, results} = receipts(plan);
  const result = ci.selectJobResults(plan, jobs, results);
  assert.equal(result.status, 'passed'); assert.equal(result.inventory, 295); assert.equal(result.doctests, 1);
  assert.equal(result.counts.passed + result.counts.ignored, 296);
  const stale = copy(results); stale[1].attempt = 1;
  assert.throws(() => ci.selectJobResults(plan, jobs, stale), /latest-attempt/);
});
test('missing, skipped, cancelled, failed, duplicate and unexpected jobs cannot hide behind aggregates', t => {
  actions(t); const plan = fixture(), {jobs, results} = receipts(plan);
  assert.throws(() => ci.selectJobResults(plan, jobs.slice(1), results), /Missing\/ambiguous/);
  for (const conclusion of ['skipped', 'cancelled', 'failure', null]) {
    const changed = copy(jobs); changed[0].conclusion = conclusion;
    assert.throws(() => ci.selectJobResults(plan, changed, results), /Failed, skipped/);
  }
  assert.throws(() => ci.selectJobResults(plan, [...jobs, jobs[0]], results), /ambiguous/);
  assert.throws(() => ci.selectJobResults(plan, [...jobs, {...jobs[0], name: 'Test Block (foreign)'}], results), /Unexpected/);
  assert.throws(() => ci.selectJobResults(plan, jobs, results.slice(1)), /latest-attempt/);
});
test('false-green count/ignored/exit/time/source/cache receipts fail closed', t => {
  actions(t); const plan = fixture(), {jobs, results} = receipts(plan);
  for (const mutate of [
    v => { v.planKey = 'wrong'; }, v => { v.sha = 'c'.repeat(40); }, v => { v.sourceTree = 'd'.repeat(40); },
    v => { v.package = 'other-crate'; }, v => { v.profile = 'all-features'; }, v => { v.status = 'skipped'; },
    v => { v.exitCode = 23; }, v => { v.expected--; }, v => { v.counts.ignored++; v.counts.passed--; },
    v => { v.testDurationMs = 600001; }, v => { v.timeoutSeconds = 601; }, v => { v.processes = []; },
    v => { v.processes[0].timedOut = true; }, v => { v.processes[0].exitCode = 23; },
  ]) {
    const changed = copy(results); mutate(changed[0]);
    assert.throws(() => ci.selectJobResults(plan, jobs, changed), /Invalid block/);
  }
});
test('doctest merged/unmerged summaries preserve total inventory and real failures', () => {
  const good = 'test result: ok. 2 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.2s';
  assert.deepEqual(ci.docCounts(good, 3), {passed: 2, failed: 0, ignored: 1});
  assert.deepEqual(ci.docCounts(good + '\n' + good, 6), {passed: 4, failed: 0, ignored: 2});
  assert.throws(() => ci.docCounts(good, 4), /inventory/);
  assert.throws(() => ci.docCounts(good.replace('0 failed', '1 failed'), 4), /Invalid doctest/);
});
test('CI jobs keep runtime Cargo-free, independent build inputs and non-cancelling matrices', () => {
  const workflow = fs.readFileSync('.github/workflows/ci.yml', 'utf8');
  function job(id) {
    const tail = workflow.split(`\n  ${id}:\n`)[1]; assert(tail, id);
    return tail.split(/\n  [a-z][a-z0-9-]*:\n/)[0];
  }
  for (const id of ['test-blocks', 'e2e-blocks', 'test-doctests', 'test-ssh']) {
    const source = job(id); assert.match(source, /timeout-minutes: 10/);
    assert.doesNotMatch(source, /cargo (test|build)|continue-on-error/);
    assert.match(source, /BAMBOO_BLOCK_STARTED_AT/); assert.match(source, /command: run/);
    assert.match(source, /uses: \.\/\.github\/actions\/grouped-tests/);
    if (id !== 'test-ssh') assert.match(source, /fail-fast: false/);
  }
  for (const [id, profile] of [['test-build', 'default'], ['e2e-build', 'all-features'], ['test-ssh-build', 'ssh-live']])
    { assert.match(job(id), new RegExp(`profile: ${profile}`)); assert(job(id).includes(`workspaces: . -> target/grouped-${profile}`)); }
  assert.match(job('test'), /name: Test\n/); assert.match(job('e2e-test'), /name: E2E Tests\n/);
  assert.doesNotMatch(job('test'), /cargo test --locked --no-fail-fast/);
  assert.doesNotMatch(job('e2e-test'), /cargo test/);
  assert.match(job('test'), /command: summary/); assert.match(job('e2e-test'), /command: summary/);
});
test('CI identity permits job/attempt changes while binding source, flags and runner image', t => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-ci-identity-'));
  t.after(() => fs.rmSync(directory, {recursive: true, force: true}));
  fs.writeFileSync(path.join(directory, 'source'), 'source');
  const git = args => { const p = spawnSync('git', args, {cwd: directory, encoding: 'utf8'}); assert.equal(p.status, 0, p.stderr); return p.stdout.trim(); };
  git(['init', '-q']); git(['add', '.']);
  git(['-c', 'user.name=Fixture', '-c', 'user.email=fixture@invalid', '-c', 'commit.gpgsign=false', 'commit', '-qm', 'fixture']);
  const changes = {GITHUB_ACTIONS: 'true', GITHUB_SHA: git(['rev-parse', 'HEAD']), GITHUB_RUN_ID: '77', GITHUB_JOB: 'build', GITHUB_RUN_ATTEMPT: '1', ImageVersion: 'image-one'};
  const previous = Object.fromEntries(Object.keys({...changes, RUSTFLAGS: ''}).map(key => [key, process.env[key]]));
  t.after(() => { for (const [key, value] of Object.entries(previous)) if (value === undefined) delete process.env[key]; else process.env[key] = value; });
  Object.assign(process.env, changes);
  const original = r.identity(directory, r.CONFIG, 'ci');
  process.env.GITHUB_JOB = 'runtime'; process.env.GITHUB_RUN_ATTEMPT = '2';
  assert.deepEqual(r.identity(directory, r.CONFIG, 'ci'), original);
  process.env.RUSTFLAGS = '-C debuginfo=0'; assert.notDeepEqual(r.identity(directory, r.CONFIG, 'ci'), original);
  if (previous.RUSTFLAGS === undefined) delete process.env.RUSTFLAGS; else process.env.RUSTFLAGS = previous.RUSTFLAGS;
  process.env.ImageVersion = 'image-two'; assert.notDeepEqual(r.identity(directory, r.CONFIG, 'ci'), original);
});
test('the JavaScript action forwards hyphenated inputs without exposing runtime credentials', () => {
  const action = fs.readFileSync('.github/actions/grouped-tests/index.cjs', 'utf8');
  const metadata = fs.readFileSync('.github/actions/grouped-tests/action.yml', 'utf8');
  assert.match(metadata, /using: node24/);
  let observed;
  vm.runInNewContext(action, {process: {env: {INPUT_COMMAND: 'run', INPUT_DIRECTORY: 'target/logs',
    'INPUT_PLAN-ID': '123', 'INPUT_PLAN-KEY': 'key', 'INPUT_BLOCK-ID': 'crate-block', ACTIONS_RUNTIME_TOKEN: 'private-runtime-token'}},
    require: name => { assert.equal(name, '../../../scripts/test-groups-ci.cjs'); return {main: async args => { observed = args; }}; }, console});
  assert.deepEqual(Array.from(observed), ['run', '123', 'key', 'crate-block', 'target/logs']);
});
test('transport preserves executable bytes/mode and rejects changed manifests and archives', t => {
  // The production packer accepts only this checkout's ignored target paths.
  fs.mkdirSync(path.resolve('target'), {recursive: true});
  const base = fs.mkdtempSync(path.join(path.resolve('target'), 'ci-transport-'));
  t.after(() => fs.rmSync(base, {recursive: true, force: true}));
  const binary = path.join(base, 'binary'); fs.writeFileSync(binary, 'fixture bytes'); fs.chmodSync(binary, 0o755);
  const packed = ci.pack(base, 'bundle', [binary]); fs.unlinkSync(binary);
  ci.unpack(packed.archive, packed, path.join(base, 'restore'));
  assert.equal(fs.readFileSync(binary, 'utf8'), 'fixture bytes'); assert.equal(fs.statSync(binary).mode & 0o777, 0o755);
  const wrong = {...packed, sha256: 'bad'};
  assert.throws(() => ci.unpack(packed.archive, wrong, path.join(base, 'wrong')), /archive bytes/);
});
