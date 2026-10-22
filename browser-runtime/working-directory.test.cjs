'use strict';
// Actual main ordering, with no filesystem mutation or native process.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');
const host = process.env.BAMBOO_TEST_HOST_SOURCE || path.join(__dirname, 'host.cjs');
const source = fs.readFileSync(host, 'utf8');
const start = source.indexOf('async function main() {');
const end = source.indexOf('\nif (require.main === module)', start);
assert(start >= 0 && end > start);

function fixture({ legacy = false, cwdFailure = false, permitPending = false } = {}) {
  const events = [];
  const cwdError = new Error('synthetic cwd refusal');
  const playwright = new Error('stop before loading Playwright');
  const settingsEnvironments = [];
  let grant, refuse;
  const permit = new Promise((resolve, reject) => { grant = resolve; refuse = reject; });
  const scope = vm.createContext({
    path, os: { tmpdir: () => '/synthetic/unbounded' },
    process: { env: { BAMBOO_BROWSER_DOWNLOAD_ROOT: '/synthetic/volume/tmp/owned' },
      chdir(target) { events.push(['cwd', target]); if (cwdFailure) throw cwdError; } },
    productionStorageSettings(environment) { settingsEnvironments.push(environment); events.push(['admission']); return legacy ? null : { scratch: '/synthetic/volume/tmp/owned' }; },
    prepareNativeLaunch: async () => permitPending ? permit : null,
    fs: { async mkdtemp() { events.push(['mkdir']); return '/synthetic/volume/tmp/owned/downloads'; } },
    readProfile: async () => { events.push(['profile']); return []; },
    verifyOwnedHostGroup: () => { events.push(['group']); return true; },
    createNativeRootTracker: () => ({ launch(fn) { return fn(); } }),
    require(name) { if (name === 'playwright-core') { events.push(['playwright']); throw playwright; } return {}; },
    profileDirectory: '/synthetic/volume/profiles/account',
  });
  vm.runInContext(source.slice(start, end), scope);
  return { events, cwdError, playwright, grant, refuse, settingsEnvironments, run: () => scope.main() };
}

test('bounded main enters its verified scratch before any artifact or Playwright launch', async () => {
  const f = fixture(); await assert.rejects(f.run(), error => error === f.playwright);
  assert.deepEqual(f.events[0], ['admission']);
  assert.deepEqual(f.events[1], ['cwd', '/synthetic/volume/tmp/owned']);
  assert(f.events.findIndex(([kind]) => kind === 'cwd') < f.events.findIndex(([kind]) => kind === 'mkdir'));
});

test('cwd refusal fails closed before creating artifacts or importing Playwright', async () => {
  const f = fixture({ cwdFailure: true }); await assert.rejects(f.run(), error => error === f.cwdError);
  assert.deepEqual(f.events, [['admission'], ['cwd', '/synthetic/volume/tmp/owned']]);
});

test('explicit legacy component main does not change the caller working directory', async () => {
  const f = fixture({ legacy: true }); await assert.rejects(f.run(), error => error === f.playwright);
  assert(!f.events.some(([kind]) => kind === 'cwd'));
});

test('actual main waits for native permission before any writable artifact or Playwright import', async () => {
  const f = fixture({ permitPending: true });
  const running = f.run(); await Promise.resolve();
  assert.deepEqual(f.events, []);
  const environment = { BAMBOO_BROWSER_STORAGE_GENERATION: '52446f1e-2b3e-498d-a7a8-fc65b19885d7' };
  f.grant({ environment });
  await assert.rejects(running, error => error === f.playwright);
  assert.equal(f.settingsEnvironments[0], environment);
  assert.deepEqual(f.events[0], ['admission']);
});

test('actual main prepermission refusal never reads the profile or creates an artifact', async () => {
  const f = fixture({ permitPending: true }); const refusal = new Error('synthetic permit refusal');
  const running = f.run(); f.refuse(refusal);
  await assert.rejects(running, error => error === refusal);
  assert.deepEqual(f.events, []);
});
