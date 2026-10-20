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
  const server = http.createServer((request, response) => {
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
  t.after(async () => {
    for (const client of children) await client.close().catch(() => client.kill());
    server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
    fs.rmSync(root, { recursive: true, force: true });
  });
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
      async close() { if (child.exitCode !== null || child.signalCode !== null) return exit; await call('close'); return exit; },
      async kill() { if (child.exitCode === null && child.signalCode === null) {
        // Only this newly spawned, detached child's process group is signalled.
        if (process.platform === 'win32') child.kill('SIGKILL'); else process.kill(-child.pid, 'SIGKILL');
      } return exit; },
    };
    children.add(client); return client;
  };
  return { root, profile, start };
}

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

test('separate chat profiles never share site login or storage and keep their own restart state', async t => {
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
