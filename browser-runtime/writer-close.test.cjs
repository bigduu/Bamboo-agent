'use strict';
// Actual host close functions, isolated from Chromium and all real profiles.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const source = fs.readFileSync(path.join(__dirname, 'host.cjs'), 'utf8');
const closeStart = source.indexOf('function closeHost() {');
const closeEnd = source.indexOf('async function main() {', closeStart);
const retiredStart = source.indexOf('  if (retireAfterReply) {\n    // Never keep a timed-out Chromium transfer');
const retiredEnd = source.indexOf('\n  await closeHost();', retiredStart);
assert.ok(closeStart >= 0 && closeEnd > closeStart && retiredStart >= 0 && retiredEnd > retiredStart);

function deferred() {
  let resolve;
  const promise = new Promise(answer => { resolve = answer; });
  return { promise, resolve };
}

function fixture(options = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-writer-close-unit-'));
  const scratch = path.join(directory, 'owned-scratch');
  fs.mkdirSync(scratch);
  const marker = path.join(scratch, 'synthetic-writer-marker');
  fs.writeFileSync(marker, 'synthetic data, no credential');
  const counts = { context: 0, browser: 0 };
  const scope = vm.createContext({
    fs: fs.promises, rmSync: fs.rmSync, clearCaptureTimer() {},
    clearTimeout, clearInterval, setTimeout,
    checkpointProfile: options.checkpoint || (async () => {}),
    context: { pages: () => [], close: async () => { counts.context++; await options.contextClose?.(); } },
    browser: { close: async () => { counts.browser++; await options.browserClose?.(); } },
    nativeRoots: options.nativeRoots,
    process: { exit(code) { scope.forcedExit = code; } },
    shuttingDown: false, hostClose: undefined, browserClosed: false,
    hostRuntime: null,
    captureGeneration: 0, desiredCapture: undefined, activeCapture: undefined,
    activeDownloadAttempt: undefined, profileTimer: undefined, profileInterval: undefined,
    downloadCdp: {}, downloadContextId: 'synthetic', downloadDir: scratch,
    retireAfterReply: options.retire || false, TEST_RETIRE_CLOSE_DELAY_MS: 0,
  });
  vm.runInContext(source.slice(closeStart, closeEnd) +
    '\nasync function boundedRetiredClose() {\n' + source.slice(retiredStart, retiredEnd) + '\n}', scope);
  return { scope, marker, counts };
}

test('bounded retirement preserves scratch while the actual close promise is pending', async () => {
  const gate = deferred();
  const f = fixture({ contextClose: () => gate.promise, retire: true });
  try {
    await f.scope.boundedRetiredClose();
    assert.equal(f.scope.forcedExit, 1);
    assert.equal(f.scope.browserClosed, false);
    assert.equal(fs.existsSync(f.marker), true,
      'a retirement timer is not evidence that the writer stopped');
  } finally {
    gate.resolve();
    await f.scope.closeHost();
  }
});

test('context close rejection keeps scratch and does not claim browserClosed', async () => {
  const f = fixture({ contextClose: async () => { throw new Error('synthetic context close failed'); } });
  await assert.rejects(f.scope.closeHost(), /synthetic context close failed/);
  assert.equal(f.counts.browser, 1, 'still attempt the independent browser close');
  assert.equal(f.scope.browserClosed, false);
  assert.equal(fs.existsSync(f.marker), true);
});

test('browser close rejection keeps scratch and does not claim browserClosed', async () => {
  const f = fixture({ browserClose: async () => { throw new Error('synthetic browser close failed'); } });
  await assert.rejects(f.scope.closeHost(), /synthetic browser close failed/);
  assert.equal(f.scope.browserClosed, false);
  assert.equal(fs.existsSync(f.marker), true);
});

test('fulfilled close remains idempotent and cleans its owned scratch', async () => {
  const f = fixture();
  const first = f.scope.closeHost();
  assert.equal(f.scope.closeHost(), first);
  await first;
  assert.deepEqual(f.counts, { context: 1, browser: 1 });
  assert.equal(f.scope.browserClosed, true);
  assert.equal(fs.existsSync(f.marker), false);
});

test('checkpoint failure stays an error after both writer close calls fulfill', async () => {
  const f = fixture({ checkpoint: async () => { throw new Error('synthetic checkpoint failed'); } });
  await assert.rejects(f.scope.closeHost(), /synthetic checkpoint failed/);
  assert.deepEqual(f.counts, { context: 1, browser: 1 });
  assert.equal(f.scope.browserClosed, true);
  assert.equal(fs.existsSync(f.marker), false);
});

test('abnormal retirement retains scratch even after the native close calls fulfill', async () => {
  const f = fixture({ retire: true });
  await f.scope.closeHost();
  assert.equal(f.scope.browserClosed, true);
  assert.equal(fs.existsSync(f.marker), true);
});

test('normal cleanup waits for the captured native root close after Playwright fulfills', async () => {
  const gate = deferred();
  const f = fixture({ nativeRoots: { waitClosed: () => gate.promise } });
  const close = f.scope.closeHost();
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(f.counts, { context: 1, browser: 1 });
  const before = { closed: f.scope.browserClosed, scratch: fs.existsSync(f.marker) };
  gate.resolve(); await close;
  assert.equal(before.closed, false);
  assert.equal(before.scratch, true);
  assert.equal(f.scope.browserClosed, true);
  assert.equal(fs.existsSync(f.marker), false);
});

test('native root wait refusal retains scratch even when both Playwright closes fulfill', async () => {
  let stops = 0;
  const f = fixture({ nativeRoots: { waitClosed: async () => {
    throw Object.assign(new Error('synthetic native root unconfirmed'), { code: 'browser_writer_exit_unconfirmed' });
  }, stopCapturedRoots() { stops++; } } });
  await assert.rejects(f.scope.closeHost(), e => e.code === 'browser_writer_exit_unconfirmed');
  assert.deepEqual(f.counts, { context: 1, browser: 1 });
  assert.equal(f.scope.browserClosed, false);
  assert.equal(fs.existsSync(f.marker), true);
  assert.equal(stops, 1, 'request a stop only through the captured native handles');
});
