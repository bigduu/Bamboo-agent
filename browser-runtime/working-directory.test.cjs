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

function fixture({ legacy = false, cwdFailure = false } = {}) {
  const events = [];
  const cwdError = new Error('synthetic cwd refusal');
  const playwright = new Error('stop before loading Playwright');
  const scope = vm.createContext({
    path, os: { tmpdir: () => '/synthetic/unbounded' },
    process: { env: { BAMBOO_BROWSER_DOWNLOAD_ROOT: '/synthetic/volume/tmp/owned' },
      chdir(target) { events.push(['cwd', target]); if (cwdFailure) throw cwdError; } },
    productionStorageSettings() { events.push(['admission']); return legacy ? null : { scratch: '/synthetic/volume/tmp/owned' }; },
    fs: { async mkdtemp() { events.push(['mkdir']); return '/synthetic/volume/tmp/owned/downloads'; } },
    readProfile: async () => { events.push(['profile']); return []; },
    verifyOwnedHostGroup: () => { events.push(['group']); return true; },
    createNativeRootTracker: () => ({ launch(fn) { return fn(); } }),
    require(name) { if (name === 'playwright-core') { events.push(['playwright']); throw playwright; } return {}; },
    profileDirectory: '/synthetic/volume/profiles/account',
  });
  vm.runInContext(source.slice(start, end), scope);
  return { events, cwdError, playwright, run: () => scope.main() };
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
