'use strict';
// New production admission/error paths with synthetic native observations only.
// No image, mount, Chromium, credential or real profile is used by this file.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

function fixture() {
  const scope = vm.createContext({
    require(name) { return name === 'playwright-core' ? { chromium: {} } : require(name); },
    module: { exports: {} }, process: { env: {}, platform: 'darwin', getuid: () => 501 },
    Buffer, URL, setTimeout, clearTimeout, setInterval, clearInterval,
  });
  new vm.Script(fs.readFileSync(path.join(__dirname, 'host.cjs'), 'utf8')).runInContext(scope);
  const api = vm.runInContext(`({
    settings: (...args) => productionStorageSettings(...args),
    error: (...args) => storageOperationError(...args),
    plan: (...args) => macSharedStoragePlan(...args),
  })`, scope);
  // The canonical planner itself is already covered by 33 separate tests.
  const account = '45d7594f96ae15be9295de7e679970f3ebfbd0828313e4fb791ec6a8a801895b';
  const data = '/synthetic/bamboo';
  const mount = `${data}/browser-storage-v1/mounted`;
  const env = { BAMBOO_BROWSER_HOST_PROTOCOL: '2', BAMBOO_BROWSER_STORAGE_DATA_ROOT: data,
    BAMBOO_BROWSER_PROFILE_CONTROL_DIR: `${data}/browser-profiles/${account}`,
    BAMBOO_BROWSER_PROFILE_DIR: `${mount}/profiles/${account}`,
    HOME: `${mount}/home`, XDG_CACHE_HOME: `${mount}/cache`, XDG_CONFIG_HOME: `${mount}/config`,
    XDG_DATA_HOME: `${mount}/data`, TMPDIR: `${mount}/tmp/owned-scratch`,
    TEMP: `${mount}/tmp/owned-scratch`, TMP: `${mount}/tmp/owned-scratch`,
    BAMBOO_BROWSER_DOWNLOAD_ROOT: `${mount}/tmp/owned-scratch` };
  const calls = [];
  const readers = { platform: 'darwin', uid: 501,
    filesystem: {
      realpathSync: p => p,
      lstatSync: p => ({ isDirectory: () => true, isSymbolicLink: () => false,
        uid: 501, mode: 0o40700, dev: p.startsWith(mount + '/') ? 22 : 11 }),
      readdirSync: () => ['.bamboo-profile.lock'],
    },
    inspect(plan) { calls.push(plan); return { observation: { mount: { device: 22 } } }; },
  };
  return { api, env, readers, calls, mount };
}

test('production shared host refuses missing bounded storage instead of selecting ordinary paths', () => {
  const f = fixture(); delete f.env.BAMBOO_BROWSER_STORAGE_DATA_ROOT;
  assert.throws(() => f.api.settings(f.env, f.readers), e => e.code === 'browser_storage_unavailable');
  assert.equal(f.calls.length, 0);
});

for (const platform of ['linux', 'win32']) test(`unqualified ${platform} provider fails explicitly before native queries`, () => {
  const f = fixture(); f.readers.platform = platform;
  assert.throws(() => f.api.settings(f.env, f.readers), e =>
    e.code === 'browser_storage_unavailable' && e.reason === 'browser_storage_provider_unsupported');
  assert.equal(f.calls.length, 0);
});

test('native owner settings bind one actual account profile, outside control and in-envelope scratch', () => {
  const f = fixture(); const result = f.api.settings(f.env, f.readers);
  assert.equal(result.profile, f.env.BAMBOO_BROWSER_PROFILE_DIR);
  assert.equal(result.scratch, f.env.TMPDIR);
  assert.equal(result.control, f.env.BAMBOO_BROWSER_PROFILE_CONTROL_DIR);
  assert.equal(f.calls.length, 1);
  assert.equal(f.calls[0].quotaBytes, 402653184);
});

for (const field of ['HOME', 'XDG_CACHE_HOME', 'TMPDIR', 'BAMBOO_BROWSER_PROFILE_DIR', 'BAMBOO_BROWSER_DOWNLOAD_ROOT']) {
  test(`runtime refuses ${field} outside the verified envelope`, () => {
    const f = fixture(); f.env[field] = '/synthetic/outside';
    assert.throws(() => f.api.settings(f.env, f.readers), e => e.code === 'browser_storage_unavailable');
  });
}

test('old unbounded site data causes migration-required refusal without reading or deleting it', () => {
  const f = fixture(); f.readers.filesystem.readdirSync = () => ['.bamboo-profile.lock', 'Default'];
  assert.throws(() => f.api.settings(f.env, f.readers), e =>
    e.reason === 'browser_storage_migration_required');
  assert.equal(f.calls.length, 0);
});

test('native query failure remains refusal and does not create any storage', () => {
  const f = fixture(); f.readers.inspect = () => { throw Object.assign(new Error('native read failed'), { code: 'EIO' }); };
  assert.throws(() => f.api.settings(f.env, f.readers), e =>
    e.code === 'browser_storage_unavailable' && e.reason === 'browser_storage_observation_failed');
});

test('native ENOSPC and EDQUOT retain their actual capacity meanings', () => {
  const f = fixture();
  for (const code of ['ENOSPC', 'EDQUOT']) {
    const error = f.api.error(Object.assign(new Error('synthetic native failure'), { code }), 'native_fs');
    assert.equal(error.code, 'browser_storage_unavailable');
    assert.equal(error.reason, 'browser_storage_full');
    assert.equal(error.cause.code, code);
  }
});

test('actual Chromium DataError/IOError is unconfirmed I/O rather than fabricated QuotaExceededError', () => {
  const f = fixture();
  const cause = Object.assign(new Error('Failed to write blobs (IOError)'), { name: 'DataError' });
  const error = f.api.error(cause, 'chromium_storage');
  assert.equal(error.reason, 'browser_storage_io_unconfirmed');
  assert.equal(error.cause, cause);
  assert.equal(error.cause.name, 'DataError');
});

test('a page message or an origin quota error does not prove native envelope exhaustion', () => {
  const f = fixture();
  const textOnly = new Error('ENOSPC QuotaExceededError DataError Failed to write blobs (IOError)');
  assert.equal(f.api.error(textOnly, 'page').reason, 'browser_storage_outcome_unconfirmed');
  const originQuota = Object.assign(new Error('synthetic origin quota'), { name: 'QuotaExceededError' });
  assert.equal(f.api.error(originQuota, 'chromium_storage').reason, 'browser_origin_quota_exceeded');
});
