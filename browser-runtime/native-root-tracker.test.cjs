'use strict';
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

function fixture(policy) {
  const scope = vm.createContext({
    require(name) { return name === 'playwright-core' ? { chromium: {} } : require(name); },
    module: { exports: {} }, process: { env: {}, platform: process.platform },
    Buffer, URL, setTimeout, clearTimeout, setInterval, clearInterval,
  });
  new vm.Script(fs.readFileSync(path.join(__dirname, 'host.cjs'), 'utf8')).runInContext(scope);
  const children = [];
  const launchOptions = [];
  const original = (exe, args, options) => { launchOptions.push(options); const child = Object.assign(new EventEmitter(), {
    pid: 7000 + children.length, signals: [],
    kill(signal) { this.signals.push(signal); return true; },
  }); children.push(child); return child; };
  const spawns = { spawn: original };
  scope.fixture = { spawns, policy };
  const tracker = vm.runInContext(`createNativeRootTracker(fixture.spawns, '/synthetic/account-profile', fixture.policy)`, scope);
  return { tracker, children, spawns, original, launchOptions, scope,
    launch: () => tracker.launch(() => spawns.spawn('/synthetic/chromium', ['--user-data-dir=/synthetic/account-profile'], {})) };
}

test('normal launch records the actual returned native ChildProcess and restores spawn hook', async () => {
  const f = fixture(); await f.launch();
  assert.equal(f.spawns.spawn, f.original);
  assert.equal(f.tracker.snapshot().roots[0].pid, f.children[0].pid);
  assert.equal(f.tracker.snapshot().allDescendantsProven, false);
  f.children[0].emit('close', 0, null);
  const result = await f.tracker.waitClosed();
  assert.equal(result.roots[0].code, 0);
});

test('managed production root cannot detach out of the native owner process group', async () => {
  const f = fixture({ managedGroup: true });
  await f.tracker.launch(() => f.spawns.spawn('/synthetic/chromium',
    ['--user-data-dir=/synthetic/account-profile'], { detached: true }));
  assert.equal(f.launchOptions[0].detached, false);
  f.children[0].emit('close', 0, null); await f.tracker.waitClosed();
});

test('managed group requires actual own PID/PGID equality; a caller boolean is not that observation', () => {
  const f = fixture();
  f.scope.fixture.read = (exe, args) => {
    assert.equal(exe, '/bin/ps');
    assert.deepEqual(Array.from(args), ['-o', 'pgid=', '-p', '7012']);
    return ' 7012\n';
  };
  assert.equal(vm.runInContext('verifyOwnedHostGroup(fixture.read, 7012)', f.scope), true);
  f.scope.fixture.read = () => ' 99\n';
  assert.throws(() => vm.runInContext('verifyOwnedHostGroup(fixture.read, 7012)', f.scope),
    e => e.code === 'browser_writer_exit_unconfirmed');
});

test('process-group query failure refuses instead of switching to detached roots', () => {
  const f = fixture();
  f.scope.fixture.read = () => { throw new Error('synthetic ps failure'); };
  assert.throws(() => vm.runInContext('verifyOwnedHostGroup(fixture.read, 7012)', f.scope),
    e => e.code === 'browser_writer_exit_unconfirmed');
});

test('a fulfilled Playwright close and exit event do not bypass native close/wait', async () => {
  const f = fixture(); await f.launch(); let settled = false;
  const wait = f.tracker.waitClosed().then(() => { settled = true; });
  f.children[0].emit('exit', 0, null); await Promise.resolve();
  assert.equal(settled, false);
  f.children[0].emit('close', 0, null); await wait;
  assert.equal(settled, true);
});

test('missing native capture is refused rather than accepting a JS browser object', async () => {
  const f = fixture();
  await assert.rejects(f.tracker.launch(async () => ({ browser: 'not-native-proof' })),
    e => e.code === 'browser_writer_exit_unconfirmed');
  assert.equal(f.spawns.spawn, f.original);
});

test('unexpected profile and second physical root are rejected before their spawn', async () => {
  const f = fixture();
  await assert.rejects(f.tracker.launch(() => f.spawns.spawn('/synthetic/chromium',
    ['--user-data-dir=/synthetic/other'], {})), e => e.code === 'browser_writer_exit_unconfirmed');
  assert.equal(f.children.length, 0);
  const g = fixture();
  await assert.rejects(g.tracker.launch(() => {
    g.spawns.spawn('/synthetic/chromium', ['--user-data-dir=/synthetic/account-profile'], {});
    g.spawns.spawn('/synthetic/chromium', ['--user-data-dir=/synthetic/account-profile'], {});
  }), e => e.code === 'browser_writer_exit_unconfirmed');
  assert.equal(g.children.length, 1);
  g.children[0].emit('close', 0, null);
});

for (const [code, signal] of [[1, null], [null, 'SIGKILL']]) test(`native close ${code}/${signal} remains unconfirmed`, async () => {
  const f = fixture(); await f.launch(); f.children[0].emit('close', code, signal);
  await assert.rejects(f.tracker.waitClosed(), e => e.code === 'browser_writer_exit_unconfirmed');
});

test('failed Playwright launch still retains a captured native writer for closing', async () => {
  const f = fixture();
  await assert.rejects(f.tracker.launch(() => {
    f.spawns.spawn('/synthetic/chromium', ['--user-data-dir=/synthetic/account-profile'], {});
    throw new Error('synthetic launch failed after spawn');
  }), /synthetic launch failed/);
  assert.equal(f.tracker.snapshot().roots.length, 1);
  f.children[0].emit('close', 0, null);
  await f.tracker.waitClosed();
});

test('unconfirmed close can stop only captured live ChildProcess handles without claiming descendants', async () => {
  const f = fixture(); await f.launch();
  const result = f.tracker.stopCapturedRoots();
  assert.deepEqual(f.children[0].signals, ['SIGKILL']);
  assert.equal(result.allDescendantsProven, false);
  assert.equal(result.rootSignalsAttempted, 1);
  f.children[0].emit('close', null, 'SIGKILL');
  await assert.rejects(f.tracker.waitClosed(), e => e.code === 'browser_writer_exit_unconfirmed');
});

test('a waited native handle is never signalled through a stale numeric PID', async () => {
  const f = fixture(); await f.launch(); f.children[0].emit('close', 0, null);
  assert.equal(f.tracker.stopCapturedRoots().rootSignalsAttempted, 0);
  assert.deepEqual(f.children[0].signals, []);
  await f.tracker.waitClosed();
});
