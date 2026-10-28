'use strict';
const assert = require('node:assert/strict');
const http = require('node:http');
const { spawn } = require('node:child_process');
const { once } = require('node:events');
const { createHash } = require('node:crypto');
const readline = require('node:readline');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { test } = require('node:test');

// Fixture synchronization must fail with the blocked operation instead of
// leaving the feature shard silent until its 30-minute job limit.
async function within(promise, label, timeoutMs = 30000) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function cleanupFixture(children, server, root) {
  const failures = [];
  for (const client of [...children]) {
    try {
      await client.close();
    } catch (error) {
      failures.push(error);
      try { await client.kill(); } catch (killError) { failures.push(killError); }
    }
  }
  try {
    server.closeAllConnections();
    await within(new Promise(resolve => server.close(resolve)), 'synthetic server close');
  } catch (error) {
    failures.push(error);
  }
  if (children.size) failures.push(new Error(`${children.size} owned host(s) have not exited`));
  if (failures.length) throw new AggregateError(failures, `profile fixture cleanup failed; retained ${root}`);
  fs.rmSync(root, { recursive: true, force: true });
}

// All browser state in this file belongs to this synthetic origin and these
// disposable profiles. Never attach an installed browser or copy its cookies.
const fixtureHtml = `<!doctype html><title>Isolated profile fixture</title><a id="download" href="/download">Synthetic download</a><script>
const dbOpen = () => new Promise((resolve, reject) => {
  const r = indexedDB.open('profile-fixture-db', 1);
  r.onupgradeneeded = () => r.result.createObjectStore('values');
  r.onsuccess = () => resolve(r.result); r.onerror = reject;
});
window.seed = async () => {
  await fetch('/seed-cookies');
  localStorage.setItem('profile-fixture', 'synthetic-A');
  sessionStorage.setItem('profile-fixture', 'one-tab');
  window.fixtureMemory = true;
  const db = await dbOpen();
  await new Promise((resolve, reject) => {
    const tx = db.transaction('values', 'readwrite');
    tx.objectStore('values').put('synthetic-A', 'marker');
    tx.oncomplete = resolve; tx.onerror = reject;
  });
  db.close();
  return true;
};
window.probe = async () => {
  const cookies = await (await fetch('/cookie-status')).json();
  const db = await dbOpen();
  const idb = await new Promise((resolve, reject) => {
    const r = db.transaction('values').objectStore('values').get('marker');
    r.onsuccess = () => resolve(r.result === 'synthetic-A'); r.onerror = reject;
  });
  db.close();
  return { ...cookies, localStorage: localStorage.getItem('profile-fixture') === 'synthetic-A',
    indexedDB: idb, sessionStorage: sessionStorage.getItem('profile-fixture') === 'one-tab',
    pageMemory: window.fixtureMemory === true };
};
window.clearSiteData = async () => {
  await fetch('/clear-cookies');
  localStorage.clear(); sessionStorage.clear(); window.fixtureMemory = false;
  await new Promise((resolve, reject) => {
    const r = indexedDB.deleteDatabase('profile-fixture-db');
    r.onsuccess = resolve; r.onerror = reject; r.onblocked = reject;
  });
  return true;
};
</script>`;

const durable = { persistentCookie: true, sessionCookie: true, localStorage: true,
  indexedDB: true, sessionStorage: false, pageMemory: false };
const empty = Object.fromEntries(Object.keys(durable).map(key => [key, false]));

async function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-profile-test-'));
  fs.chmodSync(root, 0o700);
  const home = path.join(root, 'home'); fs.mkdirSync(home, { mode: 0o700 });
  const children = new Set();
  let clearGate;
  let markGateReady; const gateReady = new Promise(resolve => { markGateReady = resolve; });
  let markCleared; const cleared = new Promise(resolve => { markCleared = resolve; });
  const server = http.createServer((request, response) => {
    if (request.url === '/clear-gate') { clearGate = response; markGateReady(); return; }
    if (request.url === '/clear-observed') { markCleared(!(request.headers.cookie || '').includes('fixture-session=')); }
    if (request.url === '/download') {
      response.setHeader('content-type', 'application/octet-stream');
      response.setHeader('content-disposition', 'attachment; filename="synthetic.bin"');
      response.end('synthetic-profile-download');
      return;
    }
    if (request.url === '/seed-cookies') {
      response.setHeader('set-cookie', [
        'fixture-persistent=synthetic-A; Max-Age=3600; HttpOnly; SameSite=Strict; Path=/',
        'fixture-session=synthetic-A; HttpOnly; SameSite=Strict; Path=/',
      ]);
    }
    if (request.url === '/clear-cookies') {
      response.setHeader('set-cookie', [
        'fixture-persistent=; Max-Age=0; HttpOnly; SameSite=Strict; Path=/',
        'fixture-session=; Max-Age=0; HttpOnly; SameSite=Strict; Path=/',
      ]);
    }
    response.setHeader('content-type', request.url === '/cookie-status' ? 'application/json' : 'text/html');
    const cookie = request.headers.cookie || '';
    response.end(request.url === '/cookie-status' ? JSON.stringify({
      persistentCookie: cookie.includes('fixture-persistent=synthetic-A'),
      sessionCookie: cookie.includes('fixture-session=synthetic-A'),
    }) : fixtureHtml);
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const url = `http://127.0.0.1:${server.address().port}/`;
  t.after(() => cleanupFixture(children, server, root));
  const profile = id => {
    const directory = path.join(root, createHash('sha256').update(id).digest('hex'));
    if (!fs.existsSync(directory)) fs.mkdirSync(directory, { mode: 0o700 });
    return fs.realpathSync(directory);
  };
  const start = directory => {
    const scratch = fs.mkdtempSync(path.join(root, 'scratch-'));
    const child = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
      detached: process.platform !== 'win32',
      env: {
        PATH: process.env.PATH, HOME: home, TMPDIR: scratch, TEMP: scratch, TMP: scratch,
        NODE_PATH: process.env.NODE_PATH || '',
        ...(process.env.BAMBOO_BROWSER_EXECUTABLE ? { BAMBOO_BROWSER_EXECUTABLE: process.env.BAMBOO_BROWSER_EXECUTABLE } : {}),
        ...(process.env.PLAYWRIGHT_BROWSERS_PATH ? { PLAYWRIGHT_BROWSERS_PATH: process.env.PLAYWRIGHT_BROWSERS_PATH } : {}),
        BAMBOO_BROWSER_DOWNLOAD_ROOT: scratch, BAMBOO_BROWSER_PROFILE_DIR: directory,
      }, stdio: ['pipe', 'pipe', 'pipe'],
    });
    let stderr = ''; let nextId = 1; const pending = new Map();
    child.stderr.on('data', bytes => { stderr = (stderr + bytes.toString()).slice(-8000); });
    const exit = once(child, 'exit').then(([code, signal]) => {
      for (const call of pending.values()) { clearTimeout(call.timer); call.reject(new Error(`owned host exited ${code}/${signal}: ${stderr}`)); }
      pending.clear(); children.delete(client); return { code, signal };
    });
    readline.createInterface({ input: child.stdout }).on('line', line => {
      const response = JSON.parse(line); const call = pending.get(response.id);
      if (!call) return;
      pending.delete(response.id); clearTimeout(call.timer);
      response.ok ? call.resolve(response.result) : call.reject(new Error(response.code || 'host request failed'));
    });
    const call = (action, args = {}) => new Promise((resolve, reject) => {
      const id = nextId++;
      const timer = setTimeout(() => { pending.delete(id); reject(new Error(`${action} timed out: ${stderr}`)); }, 30000);
      pending.set(id, { resolve, reject, timer }); child.stdin.write(JSON.stringify({ id, action, args }) + '\n');
    });
    const client = {
      call, exit,
      async visit() { const state = await call('state'); return call('navigate', { expected_epoch: state.page_epoch, url }); },
      async evaluate(code) { const state = await call('state'); return (await call('eval', { expected_epoch: state.page_epoch, expected_url: state.url, code })).value; },
      async close() { if (child.exitCode === null && child.signalCode === null) await call('close'); return within(exit, 'host close'); },
      async disconnect() { child.stdin.end(); return within(exit, 'host exit after stdin EOF'); },
      async disconnectOutput() {
        const readerClosed = once(child.stdout, 'close');
        child.stdout.destroy();
        await within(readerClosed, 'output reader close');
        // Closing a pipe reader does not itself deliver EPIPE to the writer.
        // An invalid v1 envelope produces an error reply without checkpointing
        // first, even when Chromium stops producing screencast frames.
        let inputError;
        child.stdin.on('error', error => { inputError = error; });
        if (child.exitCode === null && child.signalCode === null && !child.stdin.destroyed) {
          const writeError = await within(new Promise(resolve => {
            child.stdin.write('{"id":0,"action":"state"}\n', resolve);
          }), 'EPIPE trigger write');
          // A frame may have already hit EPIPE and closed the host's input.
          if (writeError && !['EPIPE', 'ERR_STREAM_DESTROYED'].includes(writeError.code)) throw writeError;
        }
        const result = await within(exit, 'host exit after stdout disconnect');
        if (inputError && !['EPIPE', 'ERR_STREAM_DESTROYED'].includes(inputError.code)) throw inputError;
        return result;
      },
      async kill() { if (child.exitCode === null && child.signalCode === null) {
        // Only this newly spawned, detached child's process group is signalled.
        if (process.platform === 'win32') child.kill('SIGKILL'); else process.kill(-child.pid, 'SIGKILL');
      } return within(exit, 'host exit after forced retirement'); },
    };
    children.add(client); return client;
  };
  return { root, profile, start, cleared, async releaseClear() { await within(gateReady, 'synthetic clear gate'); clearGate.end('release synthetic logout'); } };
}

test('fixture synchronization deadlines report the blocked operation', async () => {
  await assert.rejects(within(new Promise(() => {}), 'synthetic gate', 10), /synthetic gate timed out after 10ms/);
});

test('fixture cleanup retains profiles if an owned host exit is unconfirmed', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-profile-cleanup-test-'));
  const sentinel = path.join(root, 'sentinel'); fs.writeFileSync(sentinel, 'retained');
  const server = http.createServer(); server.listen(0, '127.0.0.1'); await once(server, 'listening');
  let secondClosed = false;
  const children = new Set();
  const first = { async close() { throw new Error('close failed'); }, async kill() { throw new Error('exit unconfirmed'); } };
  const second = { async close() { secondClosed = true; children.delete(second); }, async kill() { throw new Error('unexpected kill'); } };
  children.add(first); children.add(second);
  try {
    await assert.rejects(cleanupFixture(children, server, root), /profile fixture cleanup failed; retained/);
    assert.equal(secondClosed, true);
    assert.equal(server.listening, false);
    assert.equal(fs.readFileSync(sentinel, 'utf8'), 'retained');
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('stdout EPIPE alone finishes a streaming profile and its final logout checkpoint', async t => {
  const f = await fixture(t); const directory = f.profile('streaming-chat');
  const browser = f.start(directory); await browser.visit(); await browser.evaluate('window.seed()');
  await browser.evaluate(`setInterval(() => { document.body.style.backgroundColor = Date.now() % 2 ? 'red' : 'blue'; }, 16);
    void fetch('/clear-gate').then(() => fetch('/clear-cookies')).then(() => fetch('/clear-observed')); true`);
  await f.releaseClear(); assert.equal(await within(f.cleared, 'synthetic clear observation'), true);
  const checkpoint = path.join(directory, '.bamboo-session-cookies-v1.json');
  assert.equal(JSON.parse(fs.readFileSync(checkpoint)).cookies.some(cookie => cookie.name === 'fixture-session'), true,
    'the fixture must lose its output reader before the background checkpoint');
  assert.equal((await browser.disconnectOutput()).code, 0);
  assert.deepEqual(JSON.parse(fs.readFileSync(checkpoint)).cookies, []);
});

test('an empty persistent host closes on stdin EOF and reopens without a shutdown tab', async t => {
  const f = await fixture(t); const directory = f.profile('empty-chat');
  const first = f.start(directory);
  assert.equal((await first.call('state')).active_tab_id, null);
  assert.equal((await first.disconnect()).code, 0);
  const reopened = f.start(directory);
  assert.equal((await reopened.call('state')).active_tab_id, null);
  assert.equal((await reopened.close()).code, 0);
});

test('persistent profile restores login cookies, localStorage and IndexedDB after host process restart', async t => {
  const f = await fixture(t); const directory = f.profile('chat-A');
  const first = f.start(directory); await first.visit(); await first.evaluate('window.seed()');
  assert.deepEqual(await first.evaluate('window.probe()'), { ...durable, sessionStorage: true, pageMemory: true });
  assert.equal((await first.close()).code, 0);
  const reopened = f.start(directory); await reopened.visit();
  assert.deepEqual(await reopened.evaluate('window.probe()'), durable);
});

test('repeated open and last-tab window reconstruction preserve durable data but not page memory', async t => {
  const f = await fixture(t); const browser = f.start(f.profile('chat-A'));
  await browser.visit(); await browser.evaluate('window.seed()');
  for (let i = 0; i < 3; i++) await browser.call('state');
  let state = await browser.call('state');
  await browser.call('tab_close', { expected_epoch: state.page_epoch, tab_id: state.active_tab_id });
  await browser.visit();
  assert.deepEqual(await browser.evaluate('window.probe()'), durable);
});

test('explicitly separate standalone profile directories keep independent storage and restart state', async t => {
  const f = await fixture(t); const a = f.start(f.profile('chat-A'));
  await a.visit(); await a.evaluate('window.seed()');
  const b = f.start(f.profile('chat-B')); await b.visit();
  assert.deepEqual(await b.evaluate('window.probe()'), empty);
  await a.close(); const reopened = f.start(f.profile('chat-A')); await reopened.visit();
  assert.deepEqual(await reopened.evaluate('window.probe()'), durable);
  assert.deepEqual(await b.evaluate('window.probe()'), empty);
});

test('explicit site-data clearing is durable and never resurrects old login after restart', async t => {
  const f = await fixture(t); const directory = f.profile('chat-A'); const a = f.start(directory);
  await a.visit(); await a.evaluate('window.seed()'); await a.evaluate('window.clearSiteData()');
  assert.deepEqual(await a.evaluate('window.probe()'), empty);
  await a.close(); const reopened = f.start(directory); await reopened.visit();
  assert.deepEqual(await reopened.evaluate('window.probe()'), empty);
});

test('confirmed browser data survives forced owned-host retirement', { skip: process.platform === 'win32' }, async t => {
  const f = await fixture(t); const directory = f.profile('chat-A'); const a = f.start(directory);
  await a.visit(); await a.evaluate('window.seed()'); await a.evaluate('window.probe()');
  await a.kill(); const reopened = f.start(directory); await reopened.visit();
  assert.deepEqual(await reopened.evaluate('window.probe()'), durable);
});

test('an invalid requested profile never silently opens an ephemeral context', async t => {
  const f = await fixture(t); const outside = path.join(f.root, 'outside'); fs.mkdirSync(outside, { mode: 0o700 });
  const link = path.join(f.root, 'symlink-profile'); fs.symlinkSync(outside, link, process.platform === 'win32' ? 'junction' : 'dir');
  const host = f.start(link);
  await assert.rejects(host.call('state'), /profile|host exited/);
});

// The production BrowserManager owns the OS profile lease. Its Rust profile
// tests verify competing managers; bare host processes are not admission APIs.

test('a corrupt or foreign cookie checkpoint fails closed without logging its contents', async t => {
  const f = await fixture(t); const directory = f.profile('chat-A');
  const file = path.join(directory, '.bamboo-session-cookies-v1.json');
  const bytes = JSON.stringify({ schemaVersion: 1, profileKey: 'another-profile', cookies: 'synthetic-private-marker' });
  fs.writeFileSync(file, bytes, { mode: 0o600 });
  const host = f.start(directory);
  await assert.rejects(host.call('state'), error => /profile|host exited/.test(error.message) && !error.message.includes('synthetic-private-marker'));
  assert.equal(fs.readFileSync(file, 'utf8'), bytes);
});

test('persistent default context keeps bounded downloads on the selected tab and retains login', async t => {
  const f = await fixture(t); const browser = f.start(f.profile('chat-A'));
  await browser.visit(); await browser.evaluate('window.seed()');
  const before = await browser.call('state');
  const result = await browser.call('download', { selector: '#download', expected_epoch: before.page_epoch });
  assert.equal(result.page_epoch, before.page_epoch);
  assert.equal(result.active_tab_id, before.active_tab_id);
  assert.equal(Buffer.from(result.data_base64, 'base64').toString(), 'synthetic-profile-download');
  assert.deepEqual(await browser.evaluate('window.probe()'), { ...durable, sessionStorage: true, pageMemory: true });
});
