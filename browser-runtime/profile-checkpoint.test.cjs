'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const { constants } = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

// Execute the actual host checkpoint function with real private files. Only
// directory sync is gated/faulted; no Chromium, account or user profile enters.
async function fixture(t, { failSync = false, platform = process.platform } = {}) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'bamboo-checkpoint-barrier-'));
  const profileKey = 'a'.repeat(64);
  const directory = path.join(root, profileKey);
  await fs.mkdir(directory, { mode: 0o700 });
  const target = path.join(directory, '.bamboo-session-cookies-v1.json');
  const oldBytes = JSON.stringify({ schemaVersion: 1, profileKey, cookies: [{
    name: 'synthetic-session', value: 'temporary-fixture', domain: 'example.invalid',
    path: '/', expires: -1, httpOnly: true, secure: true, sameSite: 'Strict',
  }] }) + '\n';
  await fs.writeFile(target, oldBytes, { mode: 0o600 });
  const entered = deferred();
  const release = deferred();
  const events = [];
  const opens = [];
  const instrumented = {
    ...fs,
    async rename(from, to) {
      await fs.rename(from, to);
      events.push('renamed');
    },
    async open(file, flags, mode) {
      const handle = await fs.open(file, flags, mode);
      if (file !== directory) return handle;
      opens.push(flags);
      return {
        async sync() {
          events.push('directory-sync-entered');
          entered.resolve();
          if (failSync) throw Object.assign(new Error('synthetic directory sync failure'), { code: 'EIO' });
          await release.promise;
          await handle.sync();
          events.push('directory-synced');
        },
        async close() {
          await handle.close();
          events.push('directory-closed');
        },
      };
    },
  };
  const sandbox = vm.createContext({
    require(name) {
      if (name === 'playwright-core') return { chromium: {} };
      return name === 'node:fs/promises' ? instrumented : require(name);
    },
    module: { exports: {} },
    process: { env: {}, platform }, Buffer,
    setTimeout, clearTimeout, setInterval, clearInterval,
    fixture: { directory, profileKey, oldBytes },
  });
  new vm.Script(await fs.readFile(path.join(__dirname, 'host.cjs'), 'utf8'), {
    filename: 'host.cjs',
  }).runInContext(sandbox);
  const api = vm.runInContext(`
    profileDirectory = fixture.directory;
    profileKey = fixture.profileKey;
    profileCookieState = fixture.oldBytes;
    context = { cookies: async () => [] };
    ({ checkpoint: checkpointProfile,
       state: () => ({ bytes: profileCookieState, failed: !!profileFailure, retire: retireAfterReply }) });
  `, sandbox);
  t.after(async () => {
    release.resolve();
    await fs.rm(root, { recursive: true, force: true });
  });
  return { api, target, oldBytes, entered, release, events, opens };
}

test('cookie checkpoint awaits its renamed profile directory before acknowledging logout', {
  skip: process.platform === 'win32',
}, async t => {
  const f = await fixture(t);
  let settled = false;
  const checkpoint = f.api.checkpoint();
  checkpoint.then(() => { settled = true; }, () => { settled = true; });
  const reachedBarrier = await Promise.race([
    f.entered.promise.then(() => true),
    checkpoint.then(() => false, () => false),
  ]);
  const before = { settled, bytes: f.api.state().bytes };
  const renamedCookies = JSON.parse(await fs.readFile(f.target, 'utf8')).cookies;
  f.release.resolve();
  await checkpoint;
  assert.equal(reachedBarrier, true, 'checkpoint must sync its parent directory after rename');
  assert.equal(before.settled, false, 'logout cannot succeed before the directory barrier');
  assert.equal(before.bytes, f.oldBytes, 'the in-memory checkpoint advances only after the barrier');
  assert.deepEqual(renamedCookies, []);
  assert.deepEqual(f.events, ['renamed', 'directory-sync-entered', 'directory-synced', 'directory-closed']);
  assert.ok(f.opens[0] & constants.O_DIRECTORY);
  assert.ok(f.opens[0] & constants.O_NOFOLLOW);
  assert.deepEqual(JSON.parse(f.api.state().bytes).cookies, []);
  assert.equal((await fs.stat(f.target)).mode & 0o777, 0o600);
});

test('cookie checkpoint directory-sync failure fails closed without advancing its cache', {
  skip: process.platform === 'win32',
}, async t => {
  const f = await fixture(t, { failSync: true });
  await assert.rejects(f.api.checkpoint(), error => error.code === 'browser_profile_unavailable');
  assert.equal(f.api.state().bytes, f.oldBytes);
  assert.equal(f.api.state().failed, true);
  assert.equal(f.api.state().retire, true);
  assert.deepEqual(f.events, ['renamed', 'directory-sync-entered', 'directory-closed']);
  assert.deepEqual(JSON.parse(await fs.readFile(f.target, 'utf8')).cookies, []);
  await assert.rejects(f.api.checkpoint(), error => error.code === 'browser_profile_unavailable');
  assert.equal(f.events.filter(event => event === 'renamed').length, 1);
});

test('cookie checkpoint keeps the existing Windows path without a directory fsync', async t => {
  const f = await fixture(t, { platform: 'win32' });
  await f.api.checkpoint();
  assert.deepEqual(f.events, ['renamed']);
  assert.deepEqual(JSON.parse(f.api.state().bytes).cookies, []);
});
