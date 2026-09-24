const assert = require('node:assert/strict');
const http = require('node:http');
const { spawn } = require('node:child_process');
const readline = require('node:readline');
const { once } = require('node:events');
const { test } = require('node:test');
const path = require('node:path');
const fs = require('node:fs');
const os = require('node:os');
const { createHash } = require('node:crypto');

test('one isolated page supplies DOM, screenshot, and interactive changes without an iframe', async () => {
  const fixture = http.createServer((_request, response) => {
    response.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'x-frame-options': 'DENY',
      'content-security-policy': "frame-ancestors 'none'",
    });
    response.end('<style>button,input,output,select{display:block}</style><button id="increment" onclick="const output=document.querySelector(\'output\');output.textContent=String(Number(output.textContent)+1)">Increment</button><input id="name" aria-label="Name"><output>0</output><select id="single" onchange="document.querySelector(\'#chosen\').textContent=\'Single \'+this.value"><option value="">None</option><option value="red">Red</option></select><select id="multi" multiple onchange="document.querySelector(\'#chosen\').textContent=\'Multi \'+Array.from(this.selectedOptions).map(option=>option.value).join(\',\')"><option value="green">Green</option><option value="blue">Blue</option><option value="yellow">Yellow</option></select><output id="chosen">No selection</output><input id="upload" type="file" onchange="const file=this.files[0];const reader=new FileReader();reader.onload=()=>document.querySelector(\'#file-result\').textContent=[file.name,file.type,file.size,reader.result].join(\'|\');reader.readAsText(file)"><output id="file-result">No file</output>');
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const url = `http://127.0.0.1:${fixture.address().port}/`;
  const host = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
    env: process.env,
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const pending = new Map();
  let nextId = 1;
  let frames = 0;
  const lines = readline.createInterface({ input: host.stdout });
  lines.on('line', line => {
    const message = JSON.parse(line);
    if (message.event === 'frame') { frames++; return; }
    const resolve = pending.get(message.id);
    if (resolve) { pending.delete(message.id); resolve(message); }
  });
  const call = (action, args = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timeout = setTimeout(() => { pending.delete(id); reject(new Error(`${action} timed out`)); }, 30_000);
    pending.set(id, message => { clearTimeout(timeout); resolve(message); });
    host.stdin.write(`${JSON.stringify({ id, action, args })}\n`);
  });
  try {
    const initial = await call('state');
    assert.equal(initial.ok, true);
    const epoch = initial.result.page_epoch;
    const navigated = await call('navigate', { url, expected_epoch: epoch });
    assert.equal(navigated.ok, true);
    assert.notEqual(navigated.result.page_epoch, epoch);
    const pageEpoch = navigated.result.page_epoch;
    const before = await call('dom');
    assert.match(before.result.snapshot, /Increment/);
    assert.match(before.result.html, /<output>0<\/output>/);
    const clicked = await call('click_selector', { selector: '#increment', expected_epoch: pageEpoch });
    assert.equal(clicked.ok, true);
    const uiClick = await call('input', { kind: 'click', x: 40, y: 20, expected_epoch: pageEpoch });
    assert.equal(uiClick.ok, true);
    const filled = await call('fill_selector', { selector: '#name', text: 'Lotus', expected_epoch: pageEpoch });
    assert.equal(filled.ok, true);
    const single = await call('select_option', { selector: '#single', values: ['red'], expected_epoch: pageEpoch });
    assert.equal(single.ok, true);
    assert.deepEqual(single.result.selected_values, ['red']);
    assert.equal(single.result.page_epoch, pageEpoch);
    const cleared = await call('select_option', { selector: '#single', values: [''], expected_epoch: pageEpoch });
    assert.equal(cleared.ok, true);
    assert.deepEqual(cleared.result.selected_values, ['']);
    const multiple = await call('select_option', { selector: '#multi', values: ['green', 'blue'], expected_epoch: pageEpoch });
    assert.equal(multiple.ok, true);
    assert.deepEqual(multiple.result.selected_values, ['green', 'blue']);
    assert.equal((await call('select_option', { selector: 'select', values: ['red'], expected_epoch: pageEpoch })).code, 'ambiguous_target');
    assert.equal((await call('select_option', { selector: '#name', values: ['red'], expected_epoch: pageEpoch })).code, 'invalid_target');
    assert.equal((await call('select_option', { selector: '#single', values: ['red', 'blue'], expected_epoch: pageEpoch })).code, 'invalid_target');
    assert.equal((await call('select_option', { selector: '#single', values: [], expected_epoch: pageEpoch })).code, 'invalid_target');
    assert.equal((await call('select_option', { selector: '#single', values: Array(17).fill('red'), expected_epoch: pageEpoch })).code, 'invalid_target');
    assert.equal((await call('select_option', { selector: '#single', values: ['x'.repeat(513)], expected_epoch: pageEpoch })).code, 'invalid_target');
    const unavailable = await call('select_option', { selector: '#single', values: ['private-option-not-present'], expected_epoch: pageEpoch });
    assert.equal(unavailable.code, 'selection_failed');
    assert.doesNotMatch(unavailable.error, /private-option-not-present/);
    const file = {
      selector: '#upload', filename: 'sample.txt', mime_type: 'text/plain',
      data_base64: Buffer.from('memory-only file').toString('base64'), expected_epoch: pageEpoch,
    };
    const uploaded = await call('set_file_input', file);
    assert.equal(uploaded.ok, true);
    assert.equal(uploaded.result.page_epoch, pageEpoch);
    assert.doesNotMatch(JSON.stringify(uploaded.result), /bWVtb3J5/);
    let fileDom;
    for (let attempt = 0; attempt < 20; attempt++) {
      fileDom = (await call('dom')).result;
      if (fileDom.html.includes('sample.txt|text/plain|16|memory-only file')) break;
      await new Promise(resolve => setTimeout(resolve, 25));
    }
    assert.match(fileDom.html, /sample.txt\|text\/plain\|16\|memory-only file/);
    for (const invalid of [
      { ...file, data_base64: 'bWVtb3J5*' },
      { ...file, data_base64: 'YQ===' },
      { ...file, data_base64: Buffer.alloc(1024 * 1024 + 1).toString('base64') },
      { ...file, filename: '../private.txt' },
      { ...file, filename: 'C:\\private.txt' },
      { ...file, mime_type: 'text/plain; charset=utf-8' },
      { ...file, path: '/tmp/private' },
      { ...file, selector: '#name' },
    ]) {
      const rejected = await call('set_file_input', invalid);
      assert.equal(rejected.ok, false);
      assert.doesNotMatch(rejected.error, /memory-only file|bWVtb3J5|private.txt/);
    }
    assert.equal((await call('set_file_input', { ...file, expected_epoch: epoch })).code, 'stale_epoch');
    assert.match((await call('dom')).result.html, /sample.txt\|text\/plain\|16\|memory-only file/);
    const after = await call('dom');
    assert.match(after.result.html, /<output>2<\/output>/);
    assert.match(after.result.snapshot, /Lotus/);
    assert.match(after.result.html, /Multi green,blue/);
    const screenshot = await call('screenshot');
    assert.equal(screenshot.ok, true);
    assert.ok(Buffer.from(screenshot.result.data, 'base64').length > 1000);
    assert.equal(screenshot.result.page_epoch, pageEpoch);
    assert.ok(frames > 0);
    const resized = await call('viewport', { width: 640, height: 480, expected_epoch: pageEpoch });
    assert.equal(resized.ok, true);
    assert.notEqual(resized.result.page_epoch, pageEpoch);
    assert.deepEqual(resized.result.viewport, { width: 640, height: 480 });
    const stale = await call('input', { kind: 'key', key: 'Enter', expected_epoch: epoch });
    assert.equal(stale.code, 'stale_epoch');
    const oldFrame = await call('input', { kind: 'click', x: 40, y: 20, expected_epoch: pageEpoch });
    assert.equal(oldFrame.code, 'stale_epoch');
    assert.equal((await call('select_option', { selector: '#single', values: [''], expected_epoch: pageEpoch })).code, 'stale_epoch');
    const unsafe = await call('navigate', { url: 'file:///tmp/secret', expected_epoch: resized.result.page_epoch });
    assert.equal(unsafe.code, 'invalid_url');
  } finally {
    host.stdin.end();
    host.kill();
    fixture.close();
    await once(fixture, 'close');
  }
});

test('bounded download returns exact bytes and cleans unsolicited, oversized, and timed-out artifacts', async () => {
  const bytes = Buffer.from(Array.from({ length: 4096 }, (_, index) => index % 256));
  const maximumBytes = Buffer.alloc(256 * 1024, 0x5a);
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-download-test-'));
  const temporaryFilesIn = directory => fs.readdirSync(directory, { withFileTypes: true })
    .flatMap(entry => entry.isDirectory()
      ? temporaryFilesIn(path.join(directory, entry.name))
      : [path.join(directory, entry.name)]);
  const temporaryDownloadFiles = () => fs.readdirSync(tempRoot)
    .filter(name => name.startsWith('bamboo-browser-download-'))
    .flatMap(name => temporaryFilesIn(path.join(tempRoot, name)));
  let oversizedChunks = 0;
  let unsolicitedRequests = 0;
  let hangingClosed = 0;
  let raceAutoMarkers = 0;
  let raceMarked = false;
  const priorRequests = { inflight: 0, completed: 0 };
  let priorInflightStarted;
  let priorCompletedFinished;
  let smallReferer;
  let namedReferer;
  let smallRequests = 0;
  let spaRequests = 0;
  let staleResponseRequests = 0;
  let concurrentStarted = false;
  let backgroundBlobMarkers = 0;
  let sandboxRequests = 0;
  let redirectedRequests = 0;
  let privateScriptRequests = 0;
  let privatePopupRequests = 0;
  let privateResourceRequests = 0;
  const inflightStarted = new Promise(resolve => { priorInflightStarted = resolve; });
  const completedFinished = new Promise(resolve => { priorCompletedFinished = resolve; });
  const fixture = http.createServer((request, response) => {
    if (request.url === '/mark-auto') {
      raceAutoMarkers++;
      raceMarked = true;
      response.end('marked');
      return;
    }
    if (request.url === '/race-file') {
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': 'attachment; filename="race.bin"',
      });
      response.end(raceMarked ? 'ambient-download' : 'selected-download');
      raceMarked = false;
      return;
    }
    const priorKind = /^\/prior-(inflight|completed)-file$/.exec(request.url)?.[1];
    if (priorKind) {
      const requestIndex = ++priorRequests[priorKind];
      if (priorKind === 'inflight' && requestIndex === 1) priorInflightStarted();
      const send = () => {
        if (response.destroyed) return;
        response.writeHead(200, {
          'content-type': 'application/octet-stream',
          'content-disposition': 'attachment; filename="prior.bin"',
        });
        response.end(`${requestIndex === 1 ? 'ambient' : 'selected'}-${priorKind}`,
          priorKind === 'completed' && requestIndex === 1 ? priorCompletedFinished : undefined);
      };
      if (priorKind === 'inflight' && requestIndex === 1) setTimeout(send, 800);
      else send();
      return;
    }
    if (request.url === '/small' || request.url === '/unsolicited') {
      if (request.url === '/unsolicited') unsolicitedRequests++;
      else { smallReferer = request.headers.referer; smallRequests++; }
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': 'attachment; filename="../private.bin"',
      });
      response.end(bytes);
      return;
    }
    if (request.url === '/named-file') {
      namedReferer = request.headers.referer;
      response.writeHead(200, { 'content-type': 'application/octet-stream' });
      response.end('named-by-anchor');
      return;
    }
    if (request.url === '/redirect-file') {
      response.writeHead(302, { location: '/redirected-file' });
      response.end();
      return;
    }
    if (request.url === '/redirected-file') {
      redirectedRequests++;
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': 'attachment; filename="redirected.bin"',
      });
      response.end('redirected-bytes');
      return;
    }
    if (request.url === '/redirect-html') {
      response.writeHead(302, { location: '/html-page' });
      response.end();
      return;
    }
    if (request.url === '/html-page') {
      response.writeHead(200, { 'content-type': 'text/html' });
      response.end('<title>Private login</title><img src="/private-resource"><iframe src="/private-resource"></iframe><script>fetch("/private-script");window.open("/private-popup")</script>');
      return;
    }
    if (request.url === '/private-script') privateScriptRequests++;
    if (request.url === '/private-popup') privatePopupRequests++;
    if (request.url === '/private-resource') privateResourceRequests++;
    if (request.url === '/sandbox-file') sandboxRequests++;
    if (request.url === '/spa') spaRequests++;
    if (request.url === '/background-blob-ready') {
      response.end(concurrentStarted ? 'yes' : 'no');
      return;
    }
    if (request.url === '/background-blob-marker') {
      backgroundBlobMarkers++;
      response.end('marked');
      return;
    }
    if (request.url === '/background-blob') {
      response.end('<script>async function fire(){if(await fetch("/background-blob-ready").then(r=>r.text())!=="yes"){setTimeout(fire,20);return}const a=document.createElement("a");a.href=URL.createObjectURL(new Blob(["ambient-private-bytes"]));a.download="ambient.bin";document.body.append(a);a.click();await fetch("/background-blob-marker")}fire()</script>');
      return;
    }
    if (request.url === '/concurrent-file') {
      concurrentStarted = true;
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': 'attachment; filename="selected.bin"',
      });
      setTimeout(() => response.end('selected-concurrent'), 800);
      return;
    }
    if (request.url === '/sandbox-page') {
      response.writeHead(200, [
        ['content-type', 'text/html'],
        ['Content-Security-Policy', "default-src 'self'"],
        ['content-security-policy', 'report-uri /sandbox, SaNdBoX allow-scripts'],
      ]);
      response.end('<a id="sandbox" href="/sandbox-file" download>Blocked</a>');
      return;
    }
    if (request.url === '/stale-response') {
      if (++staleResponseRequests === 1) {
        response.end('<a id="small" href="/small">Small</a>');
      } else {
        // A no-document response for the same URL must not replace the
        // committed document's policy when a later hash navigation fires.
        response.writeHead(204, { 'content-security-policy': 'sandbox' });
        response.end();
      }
      return;
    }
    if (request.url === '/exact-limit' || request.url === '/over-limit') {
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': 'attachment; filename="boundary.bin"',
      });
      response.end(request.url === '/exact-limit'
        ? maximumBytes : Buffer.concat([maximumBytes, Buffer.from([0])]));
      return;
    }
    if (request.url === '/oversized') {
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': 'attachment; filename="large.bin"',
        'transfer-encoding': 'chunked',
      });
      const timer = setInterval(() => {
        if (response.destroyed || oversizedChunks >= 64) {
          clearInterval(timer);
          response.end();
          return;
        }
        response.write(Buffer.alloc(64 * 1024, oversizedChunks));
        oversizedChunks++;
      }, 60);
      response.on('close', () => clearInterval(timer));
      return;
    }
    if (request.url === '/hanging') {
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': 'attachment; filename="hanging.bin"',
        'transfer-encoding': 'chunked',
      });
      response.flushHeaders();
      response.write(Buffer.alloc(1024));
      response.on('close', () => { hangingClosed++; });
      return;
    }
    if (request.url === '/failed') {
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-disposition': 'attachment; filename="failed.bin"',
        'transfer-encoding': 'chunked',
      });
      response.flushHeaders();
      response.write(Buffer.alloc(1024));
      setTimeout(() => response.destroy(), 100);
      return;
    }
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8',
      'referrer-policy': 'no-referrer',
      'content-security-policy': 'report-uri /sandbox',
      'content-security-policy-report-only': 'sandbox' });
    if (request.url === '/unsolicited-page') {
      response.end('<main>Unsolicited page</main><script>setTimeout(() => { const link = document.createElement("a"); link.href = "/unsolicited"; document.body.append(link); link.click(); }, 100)</script>');
      return;
    }
    if (request.url === '/race') {
      response.end('<a id="race" href="/race-file" download="race.bin">Selected</a><script>const race=document.querySelector("#race");const original=race.getAttribute.bind(race);race.getAttribute=name=>{if(name==="href"&&!race.dataset.armed){race.dataset.armed="1";setTimeout(async()=>{await fetch("/mark-auto");const link=document.createElement("a");link.href="/race-file";document.body.append(link);link.click()},80)}return original(name)}</script>');
      return;
    }
    if (request.url === '/prior-inflight' || request.url === '/prior-completed') {
      const href = request.url === '/prior-inflight'
        ? '/prior-inflight-file' : '/prior-completed-file';
      response.end(`<a id="selected" href="${href}" download="prior.bin">Selected</a><script>setTimeout(()=>{const a=document.createElement("a");a.href="${href}";a.download="prior.bin";document.body.append(a);a.click()},100)</script>`);
      return;
    }
    response.end(`<a id="credential" href="http://user:password@${request.headers.host}/small">Credential</a>` +
      '<a id="small" href="/small">Small</a><a id="fragment" href="/small#section">Fragment</a><a id="concurrent" href="/concurrent-file">Concurrent</a><a id="hidden" href="/small" style="display:none">Hidden</a><a id="named" href="/named-file" download="chosen.txt">Named</a><a id="redirect" href="/redirect-file" download>Redirect</a><a id="html" href="/redirect-html" target="_blank">HTML</a><a id="exact-limit" href="/exact-limit">Exact limit</a><a id="over-limit" href="/over-limit">Over limit</a><a id="oversized" href="/oversized">Oversized</a><a id="hanging" href="/hanging">Hanging</a><a id="failed" href="/failed">Failed</a><button id="blob-button" onclick="const a=document.createElement(\'a\');a.href=URL.createObjectURL(new Blob([\'dynamic\']));a.download=\'dynamic.bin\';a.click()">Scripted Blob</button><button id="async-button" onclick="setTimeout(()=>{const a=document.createElement(\'a\');a.href=\'/small\';a.click()},100)">Async</button><button id="after" onclick="document.querySelector(\'output\').textContent=\'Scripts restored\'">Check scripts</button><output>Page remains open</output><script>const blob=document.createElement("a");blob.id="static-blob";blob.href=URL.createObjectURL(new Blob(["static-blob-bytes"]));blob.download="static.bin";document.body.append(blob)</script>');
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const base = `http://127.0.0.1:${fixture.address().port}`;
  const host = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
    env: {
      ...process.env,
      NODE_ENV: 'test',
      BAMBOO_BROWSER_TEST_DOWNLOAD_BUDGET_MS: '5000',
      BAMBOO_BROWSER_TEST_DOWNLOAD_CLICK_DELAY_MS: '200',
      TMPDIR: os.tmpdir(),
      BAMBOO_BROWSER_DOWNLOAD_ROOT: tempRoot,
    },
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const pending = new Map();
  let nextId = 1;
  const lines = readline.createInterface({ input: host.stdout });
  lines.on('line', line => {
    const message = JSON.parse(line);
    if (message.event) return;
    const resolve = pending.get(message.id);
    if (resolve) { pending.delete(message.id); resolve(message); }
  });
  const call = (action, args = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`${action} host response timed out`));
    }, 30_000);
    pending.set(id, message => { clearTimeout(timer); resolve(message); });
    host.stdin.write(`${JSON.stringify({ id, action, args })}\n`);
  });
  try {
    const initial = (await call('state')).result;
    assert.ok(fs.readdirSync(tempRoot).some(name => name.startsWith('bamboo-browser-download-')),
      'an explicit session-owned root overrides Node platform temp defaults');
    const ready = (await call('navigate', { url: base + '/', expected_epoch: initial.page_epoch })).result;
    const epoch = ready.page_epoch;
    const first = await call('download', { selector: '#small', expected_epoch: epoch });
    assert.equal(first.ok, true, JSON.stringify(first));
    assert.equal(first.result.page_epoch, epoch);
    assert.equal(first.result.active_tab_id, ready.active_tab_id);
    assert.equal(first.result.url, base + '/');
    assert.match(first.result.filename, /private\.bin$/);
    assert.doesNotMatch(first.result.filename, /[\\/]|\.\./);
    assert.equal(first.result.byte_count, bytes.length);
    assert.equal(first.result.sha256, createHash('sha256').update(bytes).digest('hex'));
    assert.deepEqual(Buffer.from(first.result.data_base64, 'base64'), bytes);
    assert.equal(smallReferer, undefined, 'private request never adds a forbidden Referer');
    const credential = await call('download', { selector: '#credential', expected_epoch: epoch });
    assert.equal(credential.code, 'download_unverifiable', JSON.stringify(credential));
    assert.equal(credential.result, undefined);
    assert.equal(smallRequests, 1, 'embedded credentials never start a private request');
    assert.deepEqual(temporaryDownloadFiles(), [], 'rejected credential URL left no artifact');
    const named = await call('download', { selector: '#named', expected_epoch: epoch });
    assert.equal(named.ok, true, JSON.stringify(named));
    assert.equal(named.result.filename, 'chosen.txt');
    assert.equal(namedReferer, undefined);
    const hidden = await call('download', { selector: '#hidden', expected_epoch: epoch });
    assert.equal(hidden.code, 'download_unverifiable', JSON.stringify(hidden));
    assert.equal(smallRequests, 1, 'hidden target did not start a private request');
    const fragment = await call('download', { selector: '#fragment', expected_epoch: epoch });
    assert.equal(fragment.ok, true, JSON.stringify(fragment));
    assert.deepEqual(Buffer.from(fragment.result.data_base64, 'base64'), bytes);
    const redirected = await call('download', { selector: '#redirect', expected_epoch: epoch });
    assert.equal(redirected.code, 'download_unverifiable', JSON.stringify(redirected));
    assert.equal(redirected.result, undefined);
    assert.equal(redirectedRequests, 1, 'the redirect reached the file but its bytes were rejected');
    const html = await call('download', { selector: '#html', expected_epoch: epoch });
    assert.equal(html.code, 'download_unverifiable', JSON.stringify(html));
    assert.equal(privateScriptRequests, 0, 'HTML reached through the private page never ran script');
    assert.equal(privatePopupRequests, 0, 'private HTML did not open a shared popup');
    assert.equal(privateResourceRequests, 0, 'private HTML did not load site resources');
    assert.equal((await call('state')).result.tabs.length, 1,
      'the private page and its descendants did not enter shared tabs');
    assert.match((await call('dom')).result.snapshot, /Page remains open/);
    assert.deepEqual(temporaryDownloadFiles(), [], 'successful download artifact was deleted');
    assert.equal((await call('download', { selector: '#small', expected_epoch: initial.page_epoch })).code, 'stale_epoch');

    const exactLimit = await call('download', { selector: '#exact-limit', expected_epoch: epoch });
    assert.equal(exactLimit.ok, true, JSON.stringify(exactLimit));
    assert.equal(exactLimit.result.byte_count, maximumBytes.length);
    assert.equal(exactLimit.result.sha256, createHash('sha256').update(maximumBytes).digest('hex'));
    assert.deepEqual(Buffer.from(exactLimit.result.data_base64, 'base64'), maximumBytes);
    assert.deepEqual(temporaryDownloadFiles(), [], 'exact-limit download artifact was deleted');
    const overLimit = await call('download', { selector: '#over-limit', expected_epoch: epoch });
    assert.equal(overLimit.code, 'download_too_large', JSON.stringify(overLimit));
    assert.equal(overLimit.result, undefined);
    assert.deepEqual(temporaryDownloadFiles(), [], 'over-limit download artifact was deleted');

    const oversized = await call('download', { selector: '#oversized', expected_epoch: epoch });
    assert.equal(oversized.code, 'download_too_large', JSON.stringify(oversized));
    assert.equal(oversized.result, undefined);
    assert.ok(oversizedChunks < 64, `oversized stream sent ${oversizedChunks} chunks`);
    assert.deepEqual(temporaryDownloadFiles(), [], 'oversized download artifact was deleted');

    const timeoutStartedAt = performance.now();
    const timedOut = await call('download', { selector: '#hanging', expected_epoch: epoch });
    const timeoutElapsedMs = performance.now() - timeoutStartedAt;
    assert.equal(timedOut.code, 'download_timeout', JSON.stringify(timedOut));
    assert.equal(timedOut.result, undefined);
    assert.ok(timeoutElapsedMs <= 5_500,
      `download plus cancellation exceeded the 5-second test budget: ${timeoutElapsedMs}ms`);
    for (let attempt = 0; hangingClosed === 0 && attempt < 40; attempt++) {
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.equal(hangingClosed, 1, 'timed-out transfer was cancelled');
    assert.deepEqual(temporaryDownloadFiles(), [], 'timed-out download artifact was deleted');
    const failed = await call('download', { selector: '#failed', expected_epoch: epoch });
    assert.equal(failed.code, 'download_failed', JSON.stringify(failed));
    assert.equal(failed.result, undefined);
    assert.deepEqual(temporaryDownloadFiles(), [], 'failed download artifact was deleted');
    assert.match((await call('dom')).result.snapshot, /Page remains open/);
    assert.equal((await call('screenshot')).ok, true);
    const staticBlob = await call('download', { selector: '#static-blob', expected_epoch: epoch });
    assert.equal(staticBlob.code, 'download_unverifiable', JSON.stringify(staticBlob));
    assert.equal(staticBlob.result, undefined);
    for (const selector of ['#blob-button', '#async-button']) {
      const scripted = await call('download', { selector, expected_epoch: epoch });
      assert.equal(scripted.code, 'download_unverifiable', JSON.stringify(scripted));
      assert.equal(scripted.result, undefined);
    }
    assert.equal((await call('click_selector', { selector: '#after', expected_epoch: epoch })).ok, true);
    assert.match((await call('dom')).result.snapshot, /Scripts restored/);
    assert.deepEqual(temporaryDownloadFiles(), [], 'direct and rejected downloads left no artifacts');

    const pushed = await call('eval', {
      code: 'history.pushState({}, "", "/spa")', expected_epoch: epoch, expected_url: base + '/',
    });
    assert.equal(pushed.code, 'stale_epoch', JSON.stringify(pushed));
    const spa = (await call('state')).result;
    assert.equal(spa.url, base + '/spa');
    assert.equal(spaRequests, 0, 'same-document history did not fetch a new response');
    const spaDownload = await call('download', { selector: '#small', expected_epoch: spa.page_epoch });
    assert.equal(spaDownload.ok, true, JSON.stringify(spaDownload));
    assert.deepEqual(Buffer.from(spaDownload.result.data_base64, 'base64'), bytes);
    const hashed = await call('eval', {
      code: 'location.hash = "#view"', expected_epoch: spa.page_epoch, expected_url: base + '/spa',
    });
    assert.equal(hashed.code, 'stale_epoch', JSON.stringify(hashed));
    const hashPage = (await call('state')).result;
    assert.equal(hashPage.url, base + '/spa#view');
    const hashDownload = await call('download', {
      selector: '#small', expected_epoch: hashPage.page_epoch,
    });
    assert.equal(hashDownload.ok, true, JSON.stringify(hashDownload));
    assert.deepEqual(Buffer.from(hashDownload.result.data_base64, 'base64'), bytes);

    const background = (await call('tab_create', { expected_epoch: hashPage.page_epoch })).result;
    const backgroundPage = (await call('navigate', {
      url: base + '/background-blob', expected_epoch: background.page_epoch,
    })).result;
    const restored = (await call('tab_activate', {
      tab_id: hashPage.active_tab_id, expected_epoch: backgroundPage.page_epoch,
    })).result;
    const concurrent = await call('download', {
      selector: '#concurrent', expected_epoch: restored.page_epoch,
    });
    assert.equal(concurrent.ok, true, JSON.stringify(concurrent));
    assert.equal(Buffer.from(concurrent.result.data_base64, 'base64').toString(),
      'selected-concurrent');
    assert.equal(backgroundBlobMarkers, 1,
      'a background tab started a Blob download during the approved direct transfer');
    assert.deepEqual(temporaryDownloadFiles(), [], 'the ambient Blob left no artifact');
    const stillShared = (await call('state')).result;
    assert.equal(stillShared.active_tab_id, hashPage.active_tab_id);
    assert.equal(stillShared.tabs.length, 2, 'only the known background tab remains');
    const afterBackground = (await call('tab_close', {
      tab_id: background.active_tab_id, expected_epoch: stillShared.page_epoch,
    })).result;
    assert.equal(afterBackground.tabs.length, 1);

    const stalePage = (await call('navigate', {
      url: base + '/stale-response', expected_epoch: afterBackground.page_epoch,
    })).result;
    await call('history', { direction: 'reload', expected_epoch: stalePage.page_epoch });
    assert.equal(staleResponseRequests, 2);
    const beforeHash = (await call('state')).result;
    await call('eval', {
      code: 'location.hash = "#after-no-document"',
      expected_epoch: beforeHash.page_epoch, expected_url: base + '/stale-response',
    });
    const afterHash = (await call('state')).result;
    assert.equal(afterHash.url, base + '/stale-response#after-no-document');
    const preservedPolicy = await call('download', {
      selector: '#small', expected_epoch: afterHash.page_epoch,
    });
    assert.equal(preservedPolicy.ok, true, JSON.stringify(preservedPolicy));
    assert.deepEqual(Buffer.from(preservedPolicy.result.data_base64, 'base64'), bytes);

    const racePage = (await call('navigate', {
      url: base + '/race', expected_epoch: afterHash.page_epoch,
    })).result;
    const race = await call('download', { selector: '#race', expected_epoch: racePage.page_epoch });
    assert.equal(race.ok, true, JSON.stringify(race));
    assert.equal(Buffer.from(race.result.data_base64, 'base64').toString(), 'selected-download');
    assert.equal(raceAutoMarkers, 0, 'page timer did not claim the selected same-URL download');
    assert.match((await call('dom')).result.snapshot, /Selected/);
    assert.equal((await call('screenshot')).ok, true);

    // Neither in-flight nor completed same-URL bytes may claim the private frame.
    let sharedPage = racePage;
    for (const [kind, started] of [['inflight', inflightStarted], ['completed', completedFinished]]) {
      sharedPage = (await call('navigate', {
        url: base + `/prior-${kind}`, expected_epoch: sharedPage.page_epoch,
      })).result;
      await Promise.race([started, new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`ambient ${kind} download did not start`)), 5_000))]);
      if (kind === 'completed') await new Promise(resolve => setTimeout(resolve, 100));
      const selected = await call('download', {
        selector: '#selected', expected_epoch: sharedPage.page_epoch,
      });
      assert.equal(selected.ok, true, JSON.stringify(selected));
      assert.equal(Buffer.from(selected.result.data_base64, 'base64').toString(), `selected-${kind}`);
      assert.equal(priorRequests[kind], 2, 'selected click started its own request');
      const unchanged = (await call('state')).result;
      assert.equal(unchanged.page_epoch, sharedPage.page_epoch);
      assert.equal(unchanged.active_tab_id, sharedPage.active_tab_id);
      assert.equal(unchanged.tabs.length, 1, 'private page did not enter shared tabs');
      assert.match((await call('dom')).result.snapshot, /Selected/);
      assert.equal((await call('screenshot')).ok, true);
    }

    const sandboxPage = (await call('navigate', {
      url: base + '/sandbox-page', expected_epoch: sharedPage.page_epoch,
    })).result;
    const sandbox = await call('download', {
      selector: '#sandbox', expected_epoch: sandboxPage.page_epoch,
    });
    assert.equal(sandbox.code, 'download_unverifiable', JSON.stringify(sandbox));
    assert.equal(sandboxRequests, 0, 'synthetic page did not bypass source CSP sandbox');

    const unsolicited = (await call('navigate', {
      url: base + '/unsolicited-page', expected_epoch: sandboxPage.page_epoch,
    })).result;
    await new Promise(resolve => setTimeout(resolve, 450));
    assert.equal(unsolicitedRequests, 1);
    assert.deepEqual(temporaryDownloadFiles(), [], 'unsolicited download artifact was deleted');
    assert.equal((await call('state')).result.page_epoch, unsolicited.page_epoch);
    assert.equal((await call('close')).result.closed, true);
    if (host.exitCode === null) await once(host, 'exit');
    assert.deepEqual(fs.readdirSync(tempRoot), [], 'host removed its temporary download directory');

    // Failed cleanup/private-page creation must retire with no temp artifact.
    for (const mode of ['cleanup', 'reject', 'timeout']) {
      const retiringHost = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath,
        [path.join(__dirname, 'host.cjs')], {
          env: {
            ...process.env, NODE_ENV: 'test', TMPDIR: os.tmpdir(),
            BAMBOO_BROWSER_DOWNLOAD_ROOT: tempRoot,
            BAMBOO_BROWSER_TEST_DOWNLOAD_BUDGET_MS: '5000',
            BAMBOO_BROWSER_TEST_DOWNLOAD_CLEANUP_DELAY_MS: mode === 'cleanup' ? '2000' : '0',
            BAMBOO_BROWSER_TEST_PRIVATE_PAGE_FAILURE: mode === 'cleanup' ? '' : mode,
            BAMBOO_BROWSER_TEST_RETIRE_CLOSE_DELAY_MS: mode === 'reject' ? '500' : '0',
          },
          stdio: ['pipe', 'pipe', 'inherit'],
        });
      const retiringPending = new Map();
      let retiringId = 1;
      const retiringLines = readline.createInterface({ input: retiringHost.stdout });
      retiringLines.on('line', line => {
        const message = JSON.parse(line);
        if (message.event) return;
        const resolve = retiringPending.get(message.id);
        if (resolve) { retiringPending.delete(message.id); resolve(message); }
      });
      const retiringCall = (action, args = {}) => new Promise((resolve, reject) => {
        const id = retiringId++;
        const timer = setTimeout(() => reject(new Error(`${action} on retiring host timed out`)), 10_000);
        retiringPending.set(id, message => { clearTimeout(timer); resolve(message); });
        retiringHost.stdin.write(`${JSON.stringify({ id, action, args })}\n`);
      });
      try {
        const initial = (await retiringCall('state')).result;
        const ready = (await retiringCall('navigate', {
          url: base + '/', expected_epoch: initial.page_epoch,
        })).result;
        const startedAt = performance.now();
        const result = await retiringCall('download', {
          selector: mode === 'cleanup' ? '#hanging' : '#small',
          expected_epoch: ready.page_epoch,
        });
        assert.equal(result.code, mode === 'reject' ? 'download_failed' : 'download_timeout',
          `${mode}: ${JSON.stringify(result)}`);
        assert.ok(performance.now() - startedAt <= 5_500, `${mode} exceeded total 5-second budget`);
        if (retiringHost.exitCode === null) await once(retiringHost, 'exit');
        assert.notEqual(retiringHost.exitCode, null, `${mode} retired the old host`);
        assert.deepEqual(fs.readdirSync(tempRoot), [], `${mode} removed private download files`);
      } finally {
        retiringLines.close();
        retiringHost.stdin.destroy();
        retiringHost.kill();
      }
    }
  } finally {
    host.stdin.end();
    host.kill();
    fixture.closeAllConnections();
    fixture.close();
    await once(fixture, 'close');
    fs.rmSync(tempRoot, { recursive: true, force: true });
  }
});

test('popup and explicit tabs keep active DOM, frames, and epochs on one page', async () => {
  let popupDownloadRequests = 0;
  const fixture = http.createServer((request, response) => {
    if (request.url === '/popup-file') popupDownloadRequests++;
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    if (request.url === '/one') {
      response.end('<title>One</title><button id="popup" onclick="window.open(\'/two\', \'_blank\')">Open Two</button><main>One page</main>');
    } else if (request.url === '/two') {
      response.end('<title>Two</title><main>Two page</main><a id="popup-download" href="/popup-file" download>Download</a>');
    } else {
      response.end('<title>Three</title><main>Three page</main>');
    }
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const base = 'http://127.0.0.1:' + fixture.address().port;
  const host = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
    env: process.env,
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const pending = new Map();
  const frames = [];
  let nextId = 1;
  const lines = readline.createInterface({ input: host.stdout });
  lines.on('line', line => {
    const message = JSON.parse(line);
    if (message.event === 'frame') { frames.push(message); return; }
    if (message.event === 'frame_reset') return;
    const resolve = pending.get(message.id);
    if (resolve) { pending.delete(message.id); resolve(message); }
  });
  const call = (action, args = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timeout = setTimeout(() => { pending.delete(id); reject(new Error(action + ' timed out')); }, 30_000);
    pending.set(id, message => { clearTimeout(timeout); resolve(message); });
    host.stdin.write(JSON.stringify({ id, action, args }) + '\n');
  });
  const waitForFrame = async (tabId, after, epoch) => {
    for (let attempt = 0; attempt < 100; attempt++) {
      const frame = frames.find(frame => frame.active_tab_id === tabId &&
        frame.frame_seq > after && frame.page_epoch === epoch);
      if (frame) return frame;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    throw new Error('no active frame for tab ' + tabId);
  };
  try {
    const initial = (await call('state')).result;
    assert.equal(initial.tabs.length, 1);
    assert.equal((await call('tab_activate', {
      tab_id: 'A'.repeat(10000), expected_epoch: initial.page_epoch,
    })).code, 'invalid_request');
    assert.equal((await call('tab_close', {
      tab_id: 'A'.repeat(24), expected_epoch: initial.page_epoch,
    })).code, 'invalid_request');
    const firstId = initial.active_tab_id;
    assert.equal(initial.tabs[0].tab_id, firstId);
    const first = (await call('navigate', { url: base + '/one', expected_epoch: initial.page_epoch })).result;
    const firstFrame = await waitForFrame(firstId, 0, first.page_epoch);
    assert.equal(firstFrame.page_epoch, first.page_epoch);
    const popup = await call('click_selector', { selector: '#popup', expected_epoch: first.page_epoch });
    assert.equal(popup.ok, true);
    let popupState = popup.result;
    for (let attempt = 0; popupState.tabs.length < 2 && attempt < 20; attempt++) {
      await new Promise(resolve => setTimeout(resolve, 50));
      popupState = (await call('state')).result;
    }
    assert.equal(popupState.tabs.length, 2);
    assert.notEqual(popupState.active_tab_id, firstId);
    assert.notEqual(popupState.page_epoch, first.page_epoch);
    const popupId = popupState.active_tab_id;
    const popupDom = (await call('dom')).result;
    assert.equal(popupDom.active_tab_id, popupId);
    assert.match(popupDom.html, /Two page/);
    assert.doesNotMatch(popupDom.html, /One page/);
    const popupDownload = await call('download', {
      selector: '#popup-download', expected_epoch: popupState.page_epoch,
    });
    assert.equal(popupDownload.code, 'download_unverifiable', JSON.stringify(popupDownload));
    assert.equal(popupDownloadRequests, 0, 'popup response policy was not proven');
    assert.equal((await call('state')).result.tabs.length, 2,
      'private download page did not enter the shared tab registry');
    const popupEval = await call('eval', {
      expected_epoch: popupState.page_epoch, expected_url: base + '/two',
      code: '({popup: document.title})',
    });
    assert.equal(popupEval.ok, true, JSON.stringify(popupEval));
    assert.deepEqual(popupEval.result.value, { popup: 'Two' });
    const popupFrame = await waitForFrame(popupId, firstFrame.frame_seq, popupState.page_epoch);
    assert.equal(popupFrame.page_epoch, popupState.page_epoch);
    assert.ok(Buffer.from(popupFrame.data, 'base64').length > 1000);
    assert.equal((await call('input', { kind: 'key', key: 'Enter', expected_epoch: first.page_epoch })).code, 'stale_epoch');

    const activated = (await call('tab_activate', { tab_id: firstId, expected_epoch: popupState.page_epoch })).result;
    assert.equal(activated.active_tab_id, firstId);
    assert.notEqual(activated.page_epoch, popupState.page_epoch);
    assert.match((await call('dom')).result.html, /One page/);
    assert.equal((await call('screenshot')).result.active_tab_id, firstId);
    const firstAgainFrame = await waitForFrame(firstId, popupFrame.frame_seq, activated.page_epoch);
    assert.equal(firstAgainFrame.page_epoch, activated.page_epoch);

    const closedInactive = (await call('tab_close', { tab_id: popupId, expected_epoch: activated.page_epoch })).result;
    assert.equal(closedInactive.tabs.length, 1);
    assert.equal(closedInactive.page_epoch, activated.page_epoch);
    const created = (await call('tab_create', { expected_epoch: closedInactive.page_epoch })).result;
    assert.equal(created.tabs.length, 2);
    assert.notEqual(created.active_tab_id, firstId);
    assert.notEqual(created.page_epoch, closedInactive.page_epoch);
    const newTabEval = await call('eval', {
      expected_epoch: created.page_epoch, expected_url: 'about:blank',
      code: '({new_tab: true})',
    });
    assert.equal(newTabEval.ok, true, JSON.stringify(newTabEval));
    assert.deepEqual(newTabEval.result.value, { new_tab: true });
    const thirdId = created.active_tab_id;
    const third = (await call('navigate', { url: base + '/three', expected_epoch: created.page_epoch })).result;
    assert.equal(third.active_tab_id, thirdId);
    assert.match((await call('dom')).result.html, /Three page/);
    const thirdFrame = await waitForFrame(thirdId, firstAgainFrame.frame_seq, third.page_epoch);
    assert.equal(thirdFrame.page_epoch, third.page_epoch);
    assert.equal((await call('navigate', { url: 'file:///tmp/secret', expected_epoch: third.page_epoch })).code, 'invalid_url');

    // Queued stop/start operations from older activations must not interrupt
    // the final active tab's capture or emit frames for another tab afterward.
    let switching = third;
    for (let count = 0; count < 12; count++) {
      const nextTabId = switching.active_tab_id === firstId ? thirdId : firstId;
      switching = (await call('tab_activate', {
        tab_id: nextTabId, expected_epoch: switching.page_epoch,
      })).result;
    }
    assert.equal(switching.active_tab_id, thirdId);
    const afterSwitch = frames.at(-1)?.frame_seq || 0;
    const subsequentFrames = frames.length;
    await waitForFrame(thirdId, afterSwitch, switching.page_epoch);
    await new Promise(resolve => setTimeout(resolve, 250));
    assert.ok(frames.slice(subsequentFrames).every(frame =>
      frame.active_tab_id === thirdId && frame.page_epoch === switching.page_epoch));

    const closedActive = (await call('tab_close', { tab_id: thirdId, expected_epoch: switching.page_epoch })).result;
    assert.equal(closedActive.active_tab_id, firstId);
    assert.notEqual(closedActive.page_epoch, switching.page_epoch);
    assert.match((await call('dom')).result.html, /One page/);
    const lastClosed = (await call('tab_close', { tab_id: firstId, expected_epoch: closedActive.page_epoch })).result;
    assert.equal(lastClosed.tabs.length, 1);
    assert.notEqual(lastClosed.active_tab_id, firstId);
    assert.equal(lastClosed.url, 'about:blank');
    let bounded = lastClosed;
    for (let count = 1; count < 8; count++) {
      bounded = (await call('tab_create', { expected_epoch: bounded.page_epoch })).result;
    }
    assert.equal(bounded.tabs.length, 8);
    assert.equal((await call('tab_create', { expected_epoch: bounded.page_epoch })).code, 'invalid_request');
    assert.equal((await call('close')).result.closed, true);
    if (host.exitCode === null) {
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('browser host did not exit after close')), 5000);
        host.once('exit', () => { clearTimeout(timeout); resolve(); });
      });
    }
  } finally {
    host.stdin.end();
    host.kill();
    fixture.close();
    await once(fixture, 'close');
  }
});

test('hover and straight drag change the shared page and reject stale coordinates', async () => {
  let wrongPagePointerEvents = 0;
  const noContentWaiters = [];
  const fixture = http.createServer((request, response) => {
    if (request.url === '/bad-pointer') {
      wrongPagePointerEvents++;
      response.end('ok');
      return;
    }
    if (request.url?.startsWith('/no-document/')) {
      noContentWaiters.shift()?.();
      if (request.url === '/no-document/redirect') response.writeHead(302, { location: '/no-document/204' }).end();
      else if (request.url === '/no-document/download') {
        response.writeHead(200, { 'content-disposition': 'attachment; filename="sample.txt"' }).end('sample');
      } else response.writeHead(request.url === '/no-document/205' ? 205 : 204).end();
      return;
    }
    if (request.url?.startsWith('/no-document-start/')) {
      const destination = request.url.replace('/no-document-start/', '/no-document/');
      response.end(`<button id="hover" onpointerenter="document.querySelector('output').textContent='hovered'">Hover</button><output>idle</output><script>setTimeout(()=>location.href=${JSON.stringify(destination)},100)</script>`);
      return;
    }
    if (request.url === '/very-slow-hover') {
      setTimeout(() => {
        response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        response.end('<main>Very slow hover destination</main>');
      }, 19_000);
      return;
    }
    if (request.url?.startsWith('/slow-')) {
      setTimeout(() => {
        response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        response.end(`<main>${request.url}</main>`);
      }, 1_400);
      return;
    }
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    if (request.url === '/hover-navigate') {
      response.end('<style>#hover{position:absolute;left:20px;top:20px;width:80px;height:30px}</style><button id="hover" onpointerenter="location.href=\'/slow-hover\'">Hover to navigate</button>');
      return;
    }
    if (request.url === '/deadline-hover') {
      response.end('<script>setTimeout(() => { const button = document.createElement("button"); button.id = "late-hover"; button.hidden = true; button.textContent = "Late hover"; button.onpointerenter = () => { location.href = "/very-slow-hover" }; document.body.append(button); setTimeout(() => { button.hidden = false }, 8000) }, 8000)</script>');
      return;
    }
    if (request.url === '/hover-popup') {
      response.end('<style>#hover{position:absolute;left:20px;top:20px;width:80px;height:30px}</style><button id="hover" onpointerenter="window.open(\'/slow-popup-hover\',\'_blank\')">Hover to open popup</button>');
      return;
    }
    if (request.url === '/hover-popup-and-navigate') {
      response.end('<style>#hover{position:absolute;left:20px;top:20px;width:80px;height:30px}</style><button id="hover" onpointerenter="window.open(\'/slow-popup-hover\',\'_blank\');location.href=\'/after-drop\'">Hover to open popup and navigate</button>');
      return;
    }
    if (request.url === '/frame-start') {
      response.end('<main>Initial iframe</main>');
      return;
    }
    if (request.url === '/hover-iframe' || request.url === '/drag-iframe') {
      const hover = request.url === '/hover-iframe'
        ? 'onpointerenter="document.querySelector(\'#child\').src=\'/slow-iframe\'"' : '';
      const drop = request.url === '/drag-iframe'
        ? 'document.querySelector(\'#child\').src=\'/slow-iframe\'' : '';
      response.end(`<!doctype html><style>
        #hover { position: absolute; left: 20px; top: 20px; width: 80px; height: 30px; }
        #source { position: absolute; left: 20px; top: 80px; width: 80px; height: 80px; }
        #drop { position: absolute; left: 220px; top: 80px; width: 100px; height: 80px; }
      </style><button id="hover" ${hover}>Hover</button>
      <div id="source" draggable="true" ondragstart="event.dataTransfer.setData('text/plain','source')">Drag</div>
      <div id="drop" ondragover="event.preventDefault()" ondrop="event.preventDefault();${drop}">Drop</div>
      <iframe id="child" hidden src="/frame-start" onload="document.querySelector('#frame-status').textContent=this.contentWindow.location.pathname"></iframe>
      <output id="frame-status">idle</output>`);
      return;
    }
    if (request.url === '/chain-first') {
      response.end('<script>location.href="/slow-chain-second"</script><main>Intermediate page</main>');
      return;
    }
    if (request.url === '/long-drag') {
      response.end(`<!doctype html><style>
        body { margin: 0; min-height: 3000px; }
        #source { position: absolute; left: 20px; top: 80px; width: 80px; height: 80px; background: blue; }
        #drop { position: absolute; left: 220px; top: 2200px; width: 100px; height: 80px; background: green; }
      </style><div id="source" draggable="true" ondragstart="event.dataTransfer.setData('text/plain','long')">Drag</div>
      <div id="drop" ondragover="event.preventDefault()" ondrop="event.preventDefault();document.querySelector('output').textContent=event.dataTransfer.getData('text/plain')">Drop</div>
      <output>idle</output>`);
      return;
    }
    if (request.url === '/scroll-navigate') {
      response.end(`<!doctype html><style>
        body { margin: 0; min-height: 3000px; }
        #hover { position: absolute; left: 120px; top: 2200px; width: 80px; height: 80px; }
        #source { position: absolute; left: 20px; top: 2200px; width: 80px; height: 80px; background: blue; }
        #drop { position: absolute; left: 220px; top: 2320px; width: 100px; height: 80px; background: green; }
      </style><button id="hover" onpointerenter="fetch('/bad-pointer')">Hover</button>
      <div id="source" draggable="true">Drag</div><div id="drop">Drop</div>
      <script>window.scrollTo(0,0);let armed=false,going=false;setTimeout(()=>armed=true,100);
      addEventListener('scroll',()=>{if(armed&&!going){going=true;location.href='/slow-scroll-navigation'}});
      addEventListener('mousedown',()=>fetch('/bad-pointer'))</script>`);
      return;
    }
    if (request.url?.startsWith('/covered-drag?')) {
      const covered = new URL(request.url, 'http://fixture').searchParams.get('at');
      const left = covered === 'source' ? 20 : 220;
      const width = covered === 'source' ? 80 : 100;
      response.end(`<!doctype html><style>
        body { margin: 0; }
        #source { position: absolute; left: 20px; top: 80px; width: 80px; height: 80px; }
        #drop { position: absolute; left: 220px; top: 80px; width: 100px; height: 80px; }
        #cover { position: fixed; left: ${left}px; top: 80px; width: ${width}px; height: 80px; z-index: 9; }
      </style><div id="source" draggable="true" ondragstart="event.dataTransfer.setData('text/plain','source')">Drag</div>
      <div id="drop" ondragover="event.preventDefault()" ondrop="event.preventDefault();document.querySelector('#dropped').textContent='yes'">Drop</div>
      <div id="cover" onmousedown="markOverlay()" onmouseup="markOverlay()" ondrop="markOverlay()"></div>
      <output id="overlay-events">0</output><output id="dropped">idle</output>
      <script>function markOverlay(){const out=document.querySelector('#overlay-events');out.textContent=String(Number(out.textContent)+1)}</script>`);
      return;
    }
    if (request.url === '/after-drop') {
      response.end('<main>After drop navigation</main>');
      return;
    }
    if (request.url === '/after-down') {
      response.end('<button id="probe" onclick="document.querySelector(\'output\').textContent=\'clicked\'">Probe</button><output>idle</output>');
      return;
    }
    if (request.url === '/after-move') {
      response.end('<main>After move navigation</main>');
      return;
    }
    if (['/navigate-on-drop', '/navigate-on-down', '/navigate-on-move', '/navigate-on-slow-drop', '/navigate-on-chain-drop', '/navigate-on-popup-drop'].includes(request.url)) {
      const onDown = request.url === '/navigate-on-down' ? 'onmousedown="location.href=\'/after-down\'"' : '';
      const onMove = request.url === '/navigate-on-move' ? 'onpointermove="if(event.buttons)location.href=\'/after-move\'"' : '';
      const onDrop = request.url === '/navigate-on-drop' ? 'location.href=\'/after-drop\'' :
        request.url === '/navigate-on-slow-drop' ? 'location.href=\'/slow-drop\'' :
          request.url === '/navigate-on-chain-drop' ? 'location.href=\'/chain-first\'' :
            request.url === '/navigate-on-popup-drop' ? 'window.open(\'/slow-popup-drop\',\'_blank\')' : '';
      response.end(`<!doctype html><style>
        body { margin: 0; }
        #source { position: absolute; left: 20px; top: 80px; width: 80px; height: 80px; background: blue; }
        #drop { position: absolute; left: 220px; top: 80px; width: 100px; height: 80px; background: green; }
      </style><div id="source" draggable="true" ${onDown} ${onMove} ondragstart="event.dataTransfer.setData('text/plain','source')">Drag</div>
      <div id="drop" ondragover="event.preventDefault()" ondrop="event.preventDefault();${onDrop}">Drop</div>`);
      return;
    }
    response.end(`<!doctype html><style>
      body { margin: 0; }
      #hover { position: absolute; left: 20px; top: 20px; width: 80px; height: 30px; }
      #source { position: absolute; left: 20px; top: 80px; width: 80px; height: 80px; background: blue; }
      #drop { position: absolute; left: 220px; top: 80px; width: 100px; height: 80px; background: green; }
    </style>
    <button id="hover" onpointerenter="const out=document.querySelector('#hovered');out.textContent=String(Number(out.textContent)+1)">Hover</button>
    <div id="source" draggable="true" ondragstart="event.dataTransfer.setData('text/plain','source')">Drag</div>
    <div id="drop" ondragover="event.preventDefault()" ondrop="event.preventDefault(); document.querySelector('#dropped').textContent=event.dataTransfer.getData('text/plain')+'-'+(++window.dropCount)">Drop</div>
    <output id="hovered">0</output><output id="dropped">idle</output><script>window.dropCount=0</script>`);
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const url = `http://127.0.0.1:${fixture.address().port}/`;
  const host = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
    env: process.env,
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const pending = new Map();
  const frames = [];
  let nextId = 1;
  const lines = readline.createInterface({ input: host.stdout });
  lines.on('line', line => {
    const message = JSON.parse(line);
    if (message.event === 'frame') { frames.push(message); return; }
    const resolve = pending.get(message.id);
    if (resolve) { pending.delete(message.id); resolve(message); }
  });
  const call = (action, args = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timeout = setTimeout(() => { pending.delete(id); reject(new Error(`${action} timed out`)); }, 30_000);
    pending.set(id, message => { clearTimeout(timeout); resolve(message); });
    host.stdin.write(`${JSON.stringify({ id, action, args })}\n`);
  });
  const waitForFrame = async (tabId, pageEpoch) => {
    for (let attempt = 0; attempt < 100; attempt++) {
      const frame = frames.find(frame => frame.active_tab_id === tabId && frame.page_epoch === pageEpoch);
      if (frame) return frame;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    throw new Error('no current popup frame');
  };
  try {
    const initial = (await call('state')).result;
    const navigated = await call('navigate', { url, expected_epoch: initial.page_epoch });
    assert.equal(navigated.ok, true);
    const epoch = navigated.result.page_epoch;
    assert.equal((await call('hover_selector', { selector: '#hover', expected_epoch: epoch })).ok, true);
    assert.match((await call('dom')).result.html, /<output id="hovered">1<\/output>/);
    assert.equal((await call('hover_at', { x: 150, y: 35, expected_epoch: epoch })).ok, true);
    assert.equal((await call('hover_at', { x: 60, y: 35, expected_epoch: epoch })).ok, true);
    assert.match((await call('dom')).result.html, /<output id="hovered">2<\/output>/);
    assert.equal((await call('drag_selector', {
      source_selector: '#source', target_selector: '#drop', expected_epoch: epoch,
    })).ok, true);
    assert.match((await call('dom')).result.html, /<output id="dropped">source-1<\/output>/);
    assert.equal((await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: epoch,
    })).ok, true);
    assert.match((await call('dom')).result.html, /<output id="dropped">source-2<\/output>/);
    const image = await call('screenshot');
    assert.equal(image.ok, true);
    assert.equal(image.result.page_epoch, epoch);
    assert.equal(image.result.active_tab_id, navigated.result.active_tab_id);
    assert.ok(Buffer.from(image.result.data, 'base64').length > 1000);
    assert.equal((await call('hover_at', { x: 1500, y: 20, expected_epoch: epoch })).code, 'invalid_request');
    const resized = await call('viewport', { width: 640, height: 480, expected_epoch: epoch });
    assert.equal(resized.ok, true);
    assert.equal((await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: epoch,
    })).code, 'stale_epoch');
    assert.equal((await call('hover_selector', { selector: '#hover', expected_epoch: epoch })).code, 'stale_epoch');

    const dropReady = await call('navigate', { url: url + 'navigate-on-drop', expected_epoch: resized.result.page_epoch });
    assert.equal(dropReady.ok, true);
    const dropNav = await call('drag_selector', {
      source_selector: '#source', target_selector: '#drop', expected_epoch: dropReady.result.page_epoch,
    });
    assert.equal(dropNav.ok, true);
    assert.match(dropNav.result.url, /\/after-drop$/);
    assert.notEqual(dropNav.result.page_epoch, dropReady.result.page_epoch);
    assert.match((await call('dom')).result.html, /After drop navigation/);
    assert.equal((await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: dropReady.result.page_epoch,
    })).code, 'stale_epoch');

    const downReady = await call('navigate', { url: url + 'navigate-on-down', expected_epoch: dropNav.result.page_epoch });
    assert.equal(downReady.ok, true);
    const downNav = await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: downReady.result.page_epoch,
    });
    assert.equal(downNav.ok, true);
    assert.match(downNav.result.url, /\/after-down$/);
    assert.notEqual(downNav.result.page_epoch, downReady.result.page_epoch);
    assert.equal((await call('click_selector', { selector: '#probe', expected_epoch: downNav.result.page_epoch })).ok, true);
    assert.match((await call('dom')).result.html, /<output>clicked<\/output>/);

    const moveReady = await call('navigate', { url: url + 'navigate-on-move', expected_epoch: downNav.result.page_epoch });
    assert.equal(moveReady.ok, true);
    const moveNav = await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: moveReady.result.page_epoch,
    });
    assert.equal(moveNav.ok, true);
    assert.match(moveNav.result.url, /\/after-move$/);
    assert.notEqual(moveNav.result.page_epoch, moveReady.result.page_epoch);
    assert.match((await call('dom')).result.html, /After move navigation/);

    const slowReady = await call('navigate', { url: url + 'navigate-on-slow-drop', expected_epoch: moveNav.result.page_epoch });
    const dragStarted = Date.now();
    const slowDrop = await call('drag_selector', {
      source_selector: '#source', target_selector: '#drop', expected_epoch: slowReady.result.page_epoch,
    });
    assert.equal(slowDrop.ok, true);
    assert.ok(Date.now() - dragStarted >= 1_200);
    assert.match(slowDrop.result.url, /\/slow-drop$/);
    assert.notEqual(slowDrop.result.page_epoch, slowReady.result.page_epoch);

    const chainReady = await call('navigate', {
      url: url + 'navigate-on-chain-drop', expected_epoch: slowDrop.result.page_epoch,
    });
    const chainStarted = Date.now();
    const chainedDrop = await call('drag_selector', {
      source_selector: '#source', target_selector: '#drop', expected_epoch: chainReady.result.page_epoch,
    });
    assert.equal(chainedDrop.ok, true);
    assert.ok(Date.now() - chainStarted >= 1_200);
    assert.match(chainedDrop.result.url, /\/slow-chain-second$/);
    assert.notEqual(chainedDrop.result.page_epoch, chainReady.result.page_epoch);
    assert.match((await call('dom')).result.html, /slow-chain-second/);

    for (const action of ['hover_selector', 'hover_at']) {
      const ready = await call('navigate', { url: url + 'hover-navigate', expected_epoch: (await call('state')).result.page_epoch });
      const hoverStarted = Date.now();
      const hovered = await call(action, action === 'hover_at'
        ? { x: 60, y: 35, expected_epoch: ready.result.page_epoch }
        : { selector: '#hover', expected_epoch: ready.result.page_epoch });
      assert.equal(hovered.ok, true, action);
      assert.ok(Date.now() - hoverStarted >= 1_200, action);
      assert.match(hovered.result.url, /\/slow-hover$/, action);
      assert.notEqual(hovered.result.page_epoch, ready.result.page_epoch, action);
    }

    for (const action of ['hover_selector', 'hover_at', 'drag_selector', 'drag_at']) {
      const hover = action.startsWith('hover');
      const opened = await call('navigate', {
        url: url + (hover ? 'hover-iframe' : 'drag-iframe'),
        expected_epoch: (await call('state')).result.page_epoch,
      });
      assert.equal(opened.ok, true, action);
      for (let attempt = 0; attempt < 20; attempt++) {
        const dom = await call('dom');
        if (dom.ok && dom.result.html.includes('>/frame-start</output>')) break;
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      assert.match((await call('dom')).result.html,
        /<output id="frame-status">\/frame-start<\/output>/, action);
      const ready = await call('state');
      assert.equal(ready.ok, true, action);
      if (hover) {
        assert.equal((await call('hover_at', {
          x: 150, y: 35, expected_epoch: ready.result.page_epoch,
        })).ok, true, action);
      }
      const started = Date.now();
      const response = await call(action, action === 'hover_selector'
        ? { selector: '#hover', expected_epoch: ready.result.page_epoch }
        : action === 'hover_at'
          ? { x: 60, y: 35, expected_epoch: ready.result.page_epoch }
          : action === 'drag_selector'
            ? { source_selector: '#source', target_selector: '#drop', expected_epoch: ready.result.page_epoch }
            : { x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: ready.result.page_epoch });
      assert.equal(response.ok, true, `${action}: ${JSON.stringify(response)}`);
      assert.ok(Date.now() - started >= 1_200, action);
      assert.notEqual(response.result.page_epoch, ready.result.page_epoch, action);
      const dom = await call('dom');
      assert.equal(dom.result.page_epoch, response.result.page_epoch, action);
      assert.match(dom.result.html, /<output id="frame-status">\/slow-iframe<\/output>/, action);
      assert.equal((await waitForFrame(response.result.active_tab_id, response.result.page_epoch)).page_epoch,
        response.result.page_epoch, action);
      assert.equal((await call('hover_at', {
        x: 150, y: 35, expected_epoch: ready.result.page_epoch,
      })).code, 'stale_epoch', action);
    }

    const popupHoverReady = await call('navigate', {
      url: url + 'hover-popup', expected_epoch: (await call('state')).result.page_epoch,
    });
    const popupHoverStarted = Date.now();
    const popupHover = await call('hover_selector', {
      selector: '#hover', expected_epoch: popupHoverReady.result.page_epoch,
    });
    assert.equal(popupHover.ok, true);
    assert.ok(Date.now() - popupHoverStarted >= 1_200,
      JSON.stringify({ elapsed: Date.now() - popupHoverStarted, state: popupHover.result }));
    assert.match(popupHover.result.url, /\/slow-popup-hover$/);
    assert.notEqual(popupHover.result.active_tab_id, popupHoverReady.result.active_tab_id);
    assert.match((await call('dom')).result.html, /slow-popup-hover/);
    assert.equal((await call('screenshot')).result.active_tab_id, popupHover.result.active_tab_id);
    assert.equal((await waitForFrame(popupHover.result.active_tab_id, popupHover.result.page_epoch)).page_epoch,
      popupHover.result.page_epoch);
    assert.equal((await call('hover_at', {
      x: 60, y: 35, expected_epoch: popupHoverReady.result.page_epoch,
    })).code, 'stale_epoch');

    const popupDropReady = await call('navigate', {
      url: url + 'navigate-on-popup-drop', expected_epoch: popupHover.result.page_epoch,
    });
    const popupDropStarted = Date.now();
    const popupDrop = await call('drag_selector', {
      source_selector: '#source', target_selector: '#drop', expected_epoch: popupDropReady.result.page_epoch,
    });
    assert.equal(popupDrop.ok, true);
    assert.ok(Date.now() - popupDropStarted >= 1_200);
    assert.match(popupDrop.result.url, /\/slow-popup-drop$/);
    assert.notEqual(popupDrop.result.active_tab_id, popupDropReady.result.active_tab_id);
    assert.match((await call('dom')).result.html, /slow-popup-drop/);
    assert.equal((await waitForFrame(popupDrop.result.active_tab_id, popupDrop.result.page_epoch)).page_epoch,
      popupDrop.result.page_epoch);

    const popupHoverAtReady = await call('navigate', {
      url: url + 'hover-popup', expected_epoch: popupDrop.result.page_epoch,
    });
    const popupHoverAtStarted = Date.now();
    const popupHoverAt = await call('hover_at', {
      x: 60, y: 35, expected_epoch: popupHoverAtReady.result.page_epoch,
    });
    assert.equal(popupHoverAt.ok, true);
    assert.ok(Date.now() - popupHoverAtStarted >= 1_200);
    assert.match(popupHoverAt.result.url, /\/slow-popup-hover$/);
    assert.notEqual(popupHoverAt.result.active_tab_id, popupHoverAtReady.result.active_tab_id);

    const popupDragAtReady = await call('navigate', {
      url: url + 'navigate-on-popup-drop', expected_epoch: popupHoverAt.result.page_epoch,
    });
    const popupDragAtStarted = Date.now();
    const popupDragAt = await call('drag_at', {
      x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: popupDragAtReady.result.page_epoch,
    });
    assert.equal(popupDragAt.ok, true);
    assert.ok(Date.now() - popupDragAtStarted >= 1_200);
    assert.match(popupDragAt.result.url, /\/slow-popup-drop$/);
    assert.notEqual(popupDragAt.result.active_tab_id, popupDragAtReady.result.active_tab_id);
    assert.equal((await waitForFrame(popupDragAt.result.active_tab_id, popupDragAt.result.page_epoch)).page_epoch,
      popupDragAt.result.page_epoch);

    const dualReady = await call('navigate', {
      url: url + 'hover-popup-and-navigate', expected_epoch: popupDragAt.result.page_epoch,
    });
    const dualStarted = Date.now();
    const dual = await call('hover_selector', {
      selector: '#hover', expected_epoch: dualReady.result.page_epoch,
    });
    assert.equal(dual.ok, true);
    assert.ok(Date.now() - dualStarted >= 1_200);
    assert.match(dual.result.url, /\/slow-popup-hover$/);
    assert.notEqual(dual.result.active_tab_id, dualReady.result.active_tab_id);

    const longReady = await call('navigate', { url: url + 'long-drag', expected_epoch: (await call('state')).result.page_epoch });
    const longDrag = await call('drag_selector', {
      source_selector: '#source', target_selector: '#drop', expected_epoch: longReady.result.page_epoch,
    });
    assert.equal(longDrag.ok, true);
    assert.match((await call('dom')).result.html, /<output>long<\/output>/);

    const hoverScrollReady = await call('navigate', {
      url: url + 'scroll-navigate', expected_epoch: (await call('state')).result.page_epoch,
    });
    await new Promise(resolve => setTimeout(resolve, 150));
    const hoverScrolled = await call('hover_selector', {
      selector: '#hover', expected_epoch: hoverScrollReady.result.page_epoch,
    });
    assert.equal(hoverScrolled.ok, true, JSON.stringify(hoverScrolled));
    assert.match(hoverScrolled.result.url, /\/slow-scroll-navigation$/);
    assert.notEqual(hoverScrolled.result.page_epoch, hoverScrollReady.result.page_epoch);
    assert.equal(wrongPagePointerEvents, 0, 'scroll-triggered navigation must prevent hover pointer');

    const scrollReady = await call('navigate', {
      url: url + 'scroll-navigate', expected_epoch: (await call('state')).result.page_epoch,
    });
    await new Promise(resolve => setTimeout(resolve, 150));
    const scrolled = await call('drag_selector', {
      source_selector: '#source', target_selector: '#drop', expected_epoch: scrollReady.result.page_epoch,
    });
    assert.equal(scrolled.ok, true, JSON.stringify(scrolled));
    assert.match(scrolled.result.url, /\/slow-scroll-navigation$/);
    assert.notEqual(scrolled.result.page_epoch, scrollReady.result.page_epoch);
    assert.equal(wrongPagePointerEvents, 0, 'scroll-triggered navigation must prevent mouse down');

    for (const covered of ['source', 'destination']) {
      const ready = await call('navigate', {
        url: url + `covered-drag?at=${covered}`, expected_epoch: (await call('state')).result.page_epoch,
      });
      assert.equal(ready.ok, true);
      const rejected = await call('drag_selector', {
        source_selector: '#source', target_selector: '#drop', expected_epoch: ready.result.page_epoch,
      });
      assert.equal(rejected.code, 'target_not_actionable', `${covered}: ${JSON.stringify(rejected)}`);
      const html = (await call('dom')).result.html;
      assert.match(html, /<output id="overlay-events">0<\/output>/, covered);
      assert.match(html, /<output id="dropped">idle<\/output>/, covered);
    }

    for (const outcome of ['204', '205', 'redirect', 'download']) {
      const noDocumentRequest = new Promise(resolve => noContentWaiters.push(resolve));
      const ready = await call('navigate', {
        url: url + `no-document-start/${outcome}`, expected_epoch: (await call('state')).result.page_epoch,
      });
      assert.equal(ready.ok, true);
      await Promise.race([
        noDocumentRequest,
        new Promise((_, reject) => setTimeout(() => reject(new Error(`${outcome} navigation not requested`)), 3000)),
      ]);
      await new Promise(resolve => setTimeout(resolve, 150));
      const surviving = (await call('state')).result;
      assert.match(surviving.url, new RegExp(`/no-document-start/${outcome}$`));
      const hover = await call('hover_selector', {
        selector: '#hover', expected_epoch: surviving.page_epoch,
      });
      assert.equal(hover.ok, true, `${outcome}: ${JSON.stringify(hover)}`);
      assert.match((await call('dom')).result.html, /<output>hovered<\/output>/);
    }

    // Target discovery and actionability consume the same budget as the slow
    // navigation they trigger. Rust would retire this host at 30 seconds.
    const deadlineReady = await call('navigate', {
      url: url + 'deadline-hover', expected_epoch: (await call('state')).result.page_epoch,
    });
    const originalTabId = deadlineReady.result.active_tab_id;
    const deadlineStarted = Date.now();
    const boundedHover = await call('hover_selector', {
      selector: '#late-hover', expected_epoch: deadlineReady.result.page_epoch,
    });
    const elapsed = Date.now() - deadlineStarted;
    assert.equal(boundedHover.code, 'navigation_timeout', JSON.stringify(boundedHover));
    assert.ok(elapsed >= 20_000 && elapsed < 28_000, `pointer action took ${elapsed}ms`);
    assert.equal(host.exitCode, null);
    const surviving = await call('state');
    assert.equal(surviving.ok, true);
    assert.equal(surviving.result.active_tab_id, originalTabId);
  } finally {
    host.stdin.end();
    host.kill();
    fixture.close();
    await once(fixture, 'close');
  }
});

test('navigation during observer setup rejects stale hover and drag before pointer events', async () => {
  let releaseNavigation = false;
  let wrongPagePointerEvents = 0;
  const slowNavigationWaiters = [];
  const fixture = http.createServer((request, response) => {
    response.setHeader('cache-control', 'no-store');
    if (request.url === '/go') {
      response.end(releaseNavigation ? 'yes' : 'no');
    } else if (request.url === '/bad-pointer') {
      wrongPagePointerEvents++;
      response.end('ok');
    } else if (request.url === '/race') {
      response.setHeader('content-type', 'text/html; charset=utf-8');
      response.end('<script>setInterval(async () => { if (window.going) return; if (await fetch("/go", {cache:"no-store"}).then(r => r.text()) === "yes") { window.going = true; location.href = "/new" } }, 20)</script><main>Old document</main>');
    } else if (request.url === '/slow-race') {
      response.setHeader('content-type', 'text/html; charset=utf-8');
      response.end('<script>setInterval(async () => { if (window.going) return; if (await fetch("/go", {cache:"no-store"}).then(r => r.text()) === "yes") { window.going = true; location.href = "/slow-new" } }, 20)</script><main style="position:absolute;inset:0" onpointermove="fetch(\'/bad-pointer\')" onmousedown="fetch(\'/bad-pointer\')">Navigating old document</main>');
    } else if (request.url === '/slow-new') {
      slowNavigationWaiters.shift()?.();
      setTimeout(() => response.end('<main>Slow new document</main>'), 3000);
    } else if (request.url === '/new') {
      response.setHeader('content-type', 'text/html; charset=utf-8');
      response.end('<div style="position:absolute;inset:0" onpointermove="fetch(\'/bad-pointer\')" onmousedown="fetch(\'/bad-pointer\')">New document</div>');
    } else {
      response.writeHead(404);
      response.end();
    }
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const base = `http://127.0.0.1:${fixture.address().port}`;
  const host = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
    env: { ...process.env, NODE_ENV: 'test', BAMBOO_BROWSER_TEST_OBSERVER_DELAY_MS: '1500' },
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const pending = new Map();
  const observerWaiters = [];
  const epochWaiters = [];
  let latestFrameEpoch;
  let nextId = 1;
  const lines = readline.createInterface({ input: host.stdout });
  lines.on('line', line => {
    const message = JSON.parse(line);
    if (message.event === 'test_observer_setup_waiting') {
      observerWaiters.shift()?.();
      return;
    }
    if (message.event === 'frame_reset') {
      latestFrameEpoch = message.page_epoch;
      const waiter = epochWaiters.find(waiter => waiter.before !== message.page_epoch);
      if (waiter) {
        epochWaiters.splice(epochWaiters.indexOf(waiter), 1);
        waiter.resolve();
      }
      return;
    }
    if (message.event) return;
    const resolve = pending.get(message.id);
    if (resolve) { pending.delete(message.id); resolve(message); }
  });
  const call = (action, args = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timeout = setTimeout(() => { pending.delete(id); reject(new Error(`${action} timed out`)); }, 10_000);
    pending.set(id, message => { clearTimeout(timeout); resolve(message); });
    host.stdin.write(`${JSON.stringify({ id, action, args })}\n`);
  });
  const within = (promise, label) => new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`${label} timed out`)), 5000);
    promise.then(value => { clearTimeout(timeout); resolve(value); }, error => {
      clearTimeout(timeout);
      reject(error);
    });
  });
  try {
    for (const action of ['hover_at', 'drag_at']) {
      releaseNavigation = false;
      const previous = (await call('state')).result;
      const ready = await call('navigate', { url: base + '/race', expected_epoch: previous.page_epoch });
      assert.equal(ready.ok, true);
      const observing = new Promise(resolve => observerWaiters.push(resolve));
      const responsePromise = call(action, action === 'hover_at'
        ? { x: 60, y: 35, expected_epoch: ready.result.page_epoch }
        : { x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: ready.result.page_epoch });
      await Promise.race([
        within(observing, `${action} observer setup`),
        responsePromise.then(response => { throw new Error(`${action} ended before observer setup: ${JSON.stringify(response)}`); }),
      ]);
      const changedEpoch = new Promise(resolve => epochWaiters.push({ before: ready.result.page_epoch, resolve }));
      releaseNavigation = true;
      await within(changedEpoch, `${action} page navigation`);
      const response = await responsePromise;
      assert.equal(response.code, 'stale_epoch', `${action}: ${JSON.stringify(response)}`);
      await new Promise(resolve => setTimeout(resolve, 100));
      assert.equal(wrongPagePointerEvents, 0, action);
      assert.match((await call('state')).result.url, /\/new$/, action);
    }
    for (const action of ['hover_at', 'drag_at']) {
      releaseNavigation = false;
      const previous = (await call('state')).result;
      const ready = await call('navigate', { url: base + '/slow-race', expected_epoch: previous.page_epoch });
      assert.equal(ready.ok, true);
      const observing = new Promise(resolve => observerWaiters.push(resolve));
      const responsePromise = call(action, action === 'hover_at'
        ? { x: 60, y: 35, expected_epoch: ready.result.page_epoch }
        : { x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: ready.result.page_epoch });
      await within(observing, `${action} observer setup before slow navigation`);
      const requestStarted = new Promise(resolve => slowNavigationWaiters.push(resolve));
      releaseNavigation = true;
      await within(requestStarted, `${action} slow navigation request`);
      await new Promise(resolve => setTimeout(resolve, 1700));
      assert.equal(latestFrameEpoch, ready.result.page_epoch, `${action} request is still uncommitted`);
      assert.equal(wrongPagePointerEvents, 0, `${action} must not act while navigation is in flight`);
      const response = await responsePromise;
      assert.equal(response.ok, true, `${action}: ${JSON.stringify(response)}`);
      assert.match(response.result.url, /\/slow-new$/, action);
      assert.notEqual(response.result.page_epoch, ready.result.page_epoch);
      assert.equal(wrongPagePointerEvents, 0, `${action} must not move or press on the navigating document`);
    }
    for (const action of ['hover_at', 'drag_at']) {
      releaseNavigation = false;
      const ready = await call('navigate', {
        url: base + '/slow-race', expected_epoch: (await call('state')).result.page_epoch,
      });
      assert.equal(ready.ok, true);
      const requestStarted = new Promise(resolve => slowNavigationWaiters.push(resolve));
      releaseNavigation = true;
      await within(requestStarted, `${action} navigation before observer subscription`);
      assert.equal(latestFrameEpoch, ready.result.page_epoch, `${action} request has not committed`);
      const responsePromise = call(action, action === 'hover_at'
        ? { x: 60, y: 35, expected_epoch: ready.result.page_epoch }
        : { x: 60, y: 120, to_x: 270, to_y: 120, expected_epoch: ready.result.page_epoch });
      await new Promise(resolve => setTimeout(resolve, 1700));
      assert.equal(latestFrameEpoch, ready.result.page_epoch, `${action} request remains in flight`);
      assert.equal(wrongPagePointerEvents, 0, `${action} must not act on a preexisting navigation`);
      const response = await responsePromise;
      if (response.ok) {
        assert.match(response.result.url, /\/slow-new$/, action);
        assert.notEqual(response.result.page_epoch, ready.result.page_epoch);
      } else {
        assert.equal(response.code, 'stale_epoch', `${action}: ${JSON.stringify(response)}`);
        assert.match((await call('state')).result.url, /\/slow-new$/, action);
      }
      assert.equal(wrongPagePointerEvents, 0, action);
    }
  } finally {
    host.stdin.end();
    host.kill();
    fixture.closeAllConnections();
    fixture.close();
    await once(fixture, 'close');
  }
});
test('JavaScript dialogs return pending state, accept or dismiss by identity, and keep the host responsive', async () => {
  const fixture = http.createServer((request, response) => {
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    if (request.url === '/passive') {
      response.end('<script>setTimeout(() => alert("Passive dialog"), 100)</script><main>Passive page</main>');
      return;
    }
    if (request.url === '/background') {
      response.end('<button id="schedule" onclick="setTimeout(() => { alert(\'Background dialog\'); document.querySelector(\'output\').textContent=\'background answered\' }, 1000)">Schedule</button><output>idle</output>');
      return;
    }
    response.end(`<!doctype html>
      <style>#drag-source{position:absolute;left:20px;top:160px;width:80px;height:80px;background:blue}#drag-drop{position:absolute;left:220px;top:160px;width:80px;height:80px;background:green}</style>
      <button id="plain" onclick="document.querySelector('#result').textContent='plain'">Plain</button>
      <button id="alert" onclick="alert('Private alert message');document.querySelector('#result').textContent='alert done'">Alert</button>
      <button id="hover-dialog" onpointerenter="alert('Hover dialog');document.querySelector('#result').textContent='hover answered'">Hover dialog</button>
      <button id="confirm" onclick="document.querySelector('#result').textContent=confirm('Private confirm message')?'yes':'no'">Confirm</button>
      <button id="prompt" onclick="document.querySelector('#result').textContent=prompt('Private prompt message','default text')">Prompt</button>
      <button id="chain" onclick="alert('First dialog');document.querySelector('#result').textContent=confirm('Second dialog')?'chain yes':'chain no'">Chain</button>
      <button id="timer-chain" onclick="alert('Timer first');setTimeout(() => { alert('Timer second');document.querySelector('#result').textContent='timer answered' }, 30)">Timer chain</button>
      <button id="schedule-read-dialog" onclick="setTimeout(() => alert('Read dialog'), 300)">Schedule read dialog</button>
      <button id="long" onclick="prompt('m'.repeat(5000),'d'.repeat(5000))">Long</button>
      <button id="unicode-boundary" onclick="prompt('m'.repeat(4095)+String.fromCodePoint(0x1F600),'d'.repeat(4095)+String.fromCodePoint(0x1F600))">Unicode boundary</button>
      <button id="lone-surrogate" onclick="prompt('message'+String.fromCharCode(0xD800)+'end','default'+String.fromCharCode(0xDC00)+'end')">Lone surrogate</button>
      <div id="drag-source" draggable="true" ondragstart="event.dataTransfer.setData('text/plain','moved')">Drag</div>
      <div id="drag-drop" ondragover="event.preventDefault()" ondrop="event.preventDefault();document.querySelector('#result').textContent=confirm('Drag dialog')?event.dataTransfer.getData('text/plain'):'dismissed'">Drop</div>
      <output id="result">idle</output>`);
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const url = `http://127.0.0.1:${fixture.address().port}/`;
  const host = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
    env: { ...process.env, NODE_ENV: 'test', BAMBOO_BROWSER_TEST_DIALOG_STATE_DELAY_MS: '250', BAMBOO_BROWSER_TEST_DIALOG_READ_DELAY_MS: '750' },
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const pending = new Map();
  let nextId = 1;
  readline.createInterface({ input: host.stdout }).on('line', line => {
    const message = JSON.parse(line);
    if (message.event) return;
    const resolve = pending.get(message.id);
    if (resolve) { pending.delete(message.id); resolve(message); }
  });
  const call = (action, args = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timeout = setTimeout(() => { pending.delete(id); reject(new Error(`${action} timed out`)); }, 10_000);
    pending.set(id, message => { clearTimeout(timeout); resolve(message); });
    host.stdin.write(`${JSON.stringify({ id, action, args })}\n`);
  });
  try {
    const initial = (await call('state')).result;
    const navigated = (await call('navigate', { url, expected_epoch: initial.page_epoch })).result;
    const epoch = navigated.page_epoch;
    assert.equal((await call('click_selector', { selector: '#plain', expected_epoch: epoch })).ok, true);
    assert.match((await call('dom')).result.html, /<output id="result">plain<\/output>/);

    const alert = await call('click_selector', { selector: '#alert', expected_epoch: epoch });
    assert.equal(alert.ok, true);
    assert.equal(alert.result.pending_dialog.type, 'alert');
    assert.equal(alert.result.pending_dialog.message, 'Private alert message');
    assert.equal(alert.result.pending_dialog.page_epoch, epoch);
    assert.equal(alert.result.pending_dialog.url, url);
    const alertId = alert.result.pending_dialog.dialog_id;
    assert.match(alertId, /^[0-9a-f]{24}$/);
    const pendingState = await call('state');
    assert.equal(pendingState.result.pending_dialog.dialog_id, alertId);
    for (const [action, args] of [
      ['click_selector', { selector: '#plain' }],
      ['hover_selector', { selector: '#hover-dialog' }],
      ['drag_selector', { source_selector: '#drag-source', target_selector: '#drag-drop' }],
      ['select_option', { selector: '#plain', values: ['private'] }],
    ]) {
      assert.equal((await call(action, { ...args, expected_epoch: epoch })).code, 'dialog_pending', action);
    }
    assert.equal((await call('dialog_respond', { dialog_id: '0'.repeat(24), accept: true, expected_epoch: epoch })).code, 'stale_dialog');
    assert.equal((await call('dialog_respond', { dialog_id: alertId, accept: true, expected_epoch: epoch - 1 })).code, 'stale_epoch');
    assert.equal((await call('dialog_respond', { dialog_id: alertId, accept: true, text: 'invalid', expected_epoch: epoch })).code, 'invalid_request');
    const accepted = await call('dialog_respond', {
      dialog_id: alertId, accept: true, text: null, expected_epoch: epoch,
    });
    assert.equal(accepted.ok, true);
    assert.equal(accepted.result.pending_dialog, undefined);
    assert.match((await call('dom')).result.html, /<output id="result">alert done<\/output>/);
    assert.equal((await call('dialog_respond', { dialog_id: alertId, accept: true, expected_epoch: epoch })).code, 'stale_dialog');

    for (const [action, args, message, result] of [
      ['hover_selector', { selector: '#hover-dialog' }, 'Hover dialog', 'hover answered'],
      ['drag_selector', { source_selector: '#drag-source', target_selector: '#drag-drop' }, 'Drag dialog', 'moved'],
    ]) {
      const gesture = await call(action, { ...args, expected_epoch: epoch });
      assert.equal(gesture.ok, true, `${action}: ${JSON.stringify(gesture)}`);
      assert.equal(gesture.result.pending_dialog.message, message);
      const answered = await call('dialog_respond', {
        dialog_id: gesture.result.pending_dialog.dialog_id, accept: true, expected_epoch: epoch,
      });
      assert.equal(answered.ok, true, `${action}: ${JSON.stringify(answered)}`);
      assert.equal(answered.result.pending_dialog, undefined);
      assert.match((await call('dom')).result.html, new RegExp(`<output id="result">${result}<\\/output>`));
    }

    const confirm = await call('click_selector', { selector: '#confirm', expected_epoch: epoch });
    assert.equal(confirm.result.pending_dialog.type, 'confirm');
    const dismissed = await call('dialog_respond', {
      dialog_id: confirm.result.pending_dialog.dialog_id, accept: false, text: null, expected_epoch: epoch,
    });
    assert.equal(dismissed.ok, true);
    assert.match((await call('dom')).result.html, /<output id="result">no<\/output>/);

    const prompt = await call('click_selector', { selector: '#prompt', expected_epoch: epoch });
    assert.equal(prompt.result.pending_dialog.type, 'prompt');
    assert.equal(prompt.result.pending_dialog.default_value, 'default text');
    const answer = await call('dialog_respond', {
      dialog_id: prompt.result.pending_dialog.dialog_id,
      accept: true, text: 'approved value', expected_epoch: epoch,
    });
    assert.equal(answer.ok, true);
    assert.match((await call('dom')).result.html, /<output id="result">approved value<\/output>/);
    const defaultPrompt = await call('click_selector', { selector: '#prompt', expected_epoch: epoch });
    assert.equal((await call('dialog_respond', {
      dialog_id: defaultPrompt.result.pending_dialog.dialog_id,
      accept: true, text: null, expected_epoch: epoch,
    })).ok, true);
    assert.match((await call('dom')).result.html, /<output id="result">default text<\/output>/);
    const chain = await call('click_selector', { selector: '#chain', expected_epoch: epoch });
    assert.equal(chain.result.pending_dialog.message, 'First dialog');
    const secondDialog = await call('dialog_respond', {
      dialog_id: chain.result.pending_dialog.dialog_id, accept: true, text: null, expected_epoch: epoch,
    });
    assert.equal(secondDialog.ok, true);
    assert.equal(secondDialog.result.pending_dialog.message, 'Second dialog');
    assert.notEqual(secondDialog.result.pending_dialog.dialog_id, chain.result.pending_dialog.dialog_id);
    const chainDone = await call('dialog_respond', {
      dialog_id: secondDialog.result.pending_dialog.dialog_id, accept: false, text: null, expected_epoch: epoch,
    });
    assert.equal(chainDone.ok, true);
    assert.match((await call('dom')).result.html, /<output id="result">chain no<\/output>/);
    const timerChain = await call('click_selector', { selector: '#timer-chain', expected_epoch: epoch });
    assert.equal(timerChain.result.pending_dialog.message, 'Timer first');
    const timerSecond = await call('dialog_respond', {
      dialog_id: timerChain.result.pending_dialog.dialog_id, accept: true, expected_epoch: epoch,
    });
    assert.equal(timerSecond.result.pending_dialog.message, 'Timer second');
    assert.notEqual(timerSecond.result.pending_dialog.dialog_id, timerChain.result.pending_dialog.dialog_id);
    const timerDone = await call('dialog_respond', {
      dialog_id: timerSecond.result.pending_dialog.dialog_id, accept: true, expected_epoch: epoch,
    });
    assert.equal(timerDone.ok, true);
    assert.match((await call('dom')).result.html, /<output id="result">timer answered<\/output>/);
    for (const readAction of ['dom', 'screenshot']) {
      assert.equal((await call('click_selector', { selector: '#schedule-read-dialog', expected_epoch: epoch })).ok, true);
      const interrupted = await call(readAction);
      assert.equal(interrupted.code, 'dialog_pending', readAction);
      const pendingRead = (await call('state')).result.pending_dialog;
      assert.equal(pendingRead.message, 'Read dialog');
      assert.equal((await call('dialog_respond', {
        dialog_id: pendingRead.dialog_id, accept: false, expected_epoch: epoch,
      })).ok, true);
    }
    const long = await call('click_selector', { selector: '#long', expected_epoch: epoch });
    assert.equal(long.result.pending_dialog.message.length, 4096);
    assert.equal(long.result.pending_dialog.default_value.length, 4096);
    assert.equal(long.result.pending_dialog.message_truncated, true);
    assert.equal(long.result.pending_dialog.default_value_truncated, true);
    assert.equal((await call('dialog_respond', {
      dialog_id: long.result.pending_dialog.dialog_id, accept: false, expected_epoch: epoch,
    })).ok, true);
    const boundary = await call('click_selector', { selector: '#unicode-boundary', expected_epoch: epoch });
    assert.equal(boundary.result.pending_dialog.message, 'm'.repeat(4095));
    assert.equal(boundary.result.pending_dialog.default_value, 'd'.repeat(4095));
    assert.equal(boundary.result.pending_dialog.message_truncated, true);
    assert.equal(boundary.result.pending_dialog.default_value_truncated, true);
    assert.equal((await call('dialog_respond', {
      dialog_id: boundary.result.pending_dialog.dialog_id, accept: false, expected_epoch: epoch,
    })).ok, true);
    const lone = await call('click_selector', { selector: '#lone-surrogate', expected_epoch: epoch });
    assert.equal(lone.result.pending_dialog.message, 'message\uFFFDend');
    assert.equal(lone.result.pending_dialog.default_value, 'default\uFFFDend');
    assert.equal(lone.result.pending_dialog.message_truncated, false);
    assert.equal(lone.result.pending_dialog.default_value_truncated, false);
    assert.equal((await call('dialog_respond', {
      dialog_id: lone.result.pending_dialog.dialog_id, accept: false, expected_epoch: epoch,
    })).ok, true);
    const screenshot = await call('screenshot');
    assert.equal(screenshot.result.page_epoch, epoch);
    assert.ok(Buffer.from(screenshot.result.data, 'base64').length > 1000);

    const passiveNavigation = await call('navigate', { url: `${url}passive`, expected_epoch: epoch });
    assert.equal(passiveNavigation.ok, true);
    let passive = passiveNavigation.result;
    for (let attempt = 0; !passive.pending_dialog && attempt < 30; attempt++) {
      await new Promise(resolve => setTimeout(resolve, 20));
      passive = (await call('state')).result;
    }
    assert.equal(passive.pending_dialog.message, 'Passive dialog');
    assert.equal(passive.pending_dialog.page_epoch, passive.page_epoch);
    const passiveAccepted = await call('dialog_respond', {
      dialog_id: passive.pending_dialog.dialog_id,
      accept: true, text: null, expected_epoch: passive.page_epoch,
    });
    assert.equal(passiveAccepted.ok, true);
    assert.equal(passiveAccepted.result.pending_dialog, undefined);

    const foregroundTabId = navigated.active_tab_id;
    const created = await call('tab_create', { expected_epoch: passiveAccepted.result.page_epoch });
    assert.equal(created.ok, true);
    const backgroundTabId = created.result.active_tab_id;
    const background = await call('navigate', {
      url: `${url}background`, expected_epoch: created.result.page_epoch,
    });
    assert.equal(background.ok, true);
    const scheduled = await call('click_selector', {
      selector: '#schedule', expected_epoch: background.result.page_epoch,
    });
    assert.equal(scheduled.ok, true);
    const foreground = await call('tab_activate', {
      tab_id: foregroundTabId, expected_epoch: scheduled.result.page_epoch,
    });
    assert.equal(foreground.ok, true);
    let backgroundPending;
    for (let attempt = 0; attempt < 40; attempt++) {
      backgroundPending = (await call('state')).result;
      if (backgroundPending.pending_dialog) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.equal(backgroundPending.active_tab_id, foregroundTabId);
    assert.equal(backgroundPending.pending_dialog.tab_id, backgroundTabId);
    assert.equal(backgroundPending.pending_dialog.page_epoch, backgroundPending.page_epoch);
    assert.equal(backgroundPending.pending_dialog.url, `${url}background`);
    const answeredBackground = await call('dialog_respond', {
      dialog_id: backgroundPending.pending_dialog.dialog_id,
      accept: true, expected_epoch: backgroundPending.page_epoch,
    });
    assert.equal(answeredBackground.ok, true);
    assert.equal(answeredBackground.result.active_tab_id, foregroundTabId);
    const restored = await call('tab_activate', {
      tab_id: backgroundTabId, expected_epoch: answeredBackground.result.page_epoch,
    });
    assert.equal(restored.ok, true);
    assert.match((await call('dom')).result.html, /<output>background answered<\/output>/);
  } finally {
    host.stdin.end();
    host.kill();
    fixture.close();
    await once(fixture, 'close');
  }
});

test('unanswered dialog expires and a pending dialog dismisses on host close', async () => {
  const fixture = http.createServer((_request, response) => {
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    response.end('<button id="ask" onclick="document.querySelector(\'output\').textContent=confirm(\'private dialog\')?\'yes\':\'no\'">Ask</button><output>idle</output>');
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const url = `http://127.0.0.1:${fixture.address().port}/`;
  const host = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
    env: { ...process.env, BAMBOO_BROWSER_DIALOG_TIMEOUT_MS: '150' },
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const pending = new Map();
  let nextId = 1;
  readline.createInterface({ input: host.stdout }).on('line', line => {
    const message = JSON.parse(line);
    if (message.event) return;
    const resolve = pending.get(message.id);
    if (resolve) { pending.delete(message.id); resolve(message); }
  });
  const call = (action, args = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timeout = setTimeout(() => { pending.delete(id); reject(new Error(`${action} timed out`)); }, 5_000);
    pending.set(id, message => { clearTimeout(timeout); resolve(message); });
    host.stdin.write(`${JSON.stringify({ id, action, args })}\n`);
  });
  try {
    const initial = (await call('state')).result;
    const navigated = (await call('navigate', { url, expected_epoch: initial.page_epoch })).result;
    const epoch = navigated.page_epoch;
    const first = (await call('click_selector', { selector: '#ask', expected_epoch: epoch })).result;
    assert.equal(first.pending_dialog.status, 'pending');
    let state;
    for (let attempt = 0; attempt < 30; attempt++) {
      state = (await call('state')).result;
      if (!state.pending_dialog) break;
      await new Promise(resolve => setTimeout(resolve, 20));
    }
    assert.equal(state.pending_dialog, undefined);
    assert.equal((await call('dialog_respond', {
      dialog_id: first.pending_dialog.dialog_id, accept: true, expected_epoch: epoch,
    })).code, 'stale_dialog');
    assert.match((await call('dom')).result.html, /<output>no<\/output>/);

    const second = (await call('click_selector', { selector: '#ask', expected_epoch: epoch })).result;
    assert.notEqual(second.pending_dialog.dialog_id, first.pending_dialog.dialog_id);
    const closed = await call('close');
    assert.equal(closed.result.closed, true);
    if (host.exitCode === null) {
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('host did not exit')), 5_000);
        host.once('exit', () => { clearTimeout(timeout); resolve(); });
      });
    }
  } finally {
    host.stdin.end();
    host.kill();
    fixture.close();
    await once(fixture, 'close');
  }
});

test('bounded page eval changes the same DOM and rejects stale or unsafe results', async () => {
  let slowReloadRequests = 0;
  let releaseSlowPopup;
  const slowPopupGate = new Promise(resolve => { releaseSlowPopup = resolve; });
  let markSlowPopupStarted;
  const slowPopupStarted = new Promise(resolve => { markSlowPopupStarted = resolve; });
  let releaseSlowReload;
  const slowReloadGate = new Promise(resolve => { releaseSlowReload = resolve; });
  let markSlowReloadStarted;
  const slowReloadStarted = new Promise(resolve => { markSlowReloadStarted = resolve; });
  const fixture = http.createServer((request, response) => {
    if (request.url === '/slow-popup-eval') {
      markSlowPopupStarted();
      void slowPopupGate.then(() => {
        if (response.destroyed) return;
        response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        response.end('<title>Popup destination</title><main>New popup tab</main>');
      });
      return;
    }
    if (request.url === '/slow-reload') {
      slowReloadRequests++;
      const sendPage = () => {
        if (response.destroyed) return;
        response.writeHead(200, {
          'content-type': 'text/html; charset=utf-8',
          'cache-control': 'no-store',
        });
        response.end('<title>Slow reload</title><output>Current document</output>');
      };
      if (slowReloadRequests === 1) sendPage();
      else {
        markSlowReloadStarted();
        void slowReloadGate.then(sendPage);
      }
      return;
    }
    if (request.url === '/lexical-window.js' || request.url === '/lexical-global.js') {
      response.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8' });
      response.end(request.url === '/lexical-window.js'
        ? "let window = new Proxy({}, { get: () => () => 'x'.repeat(1_000_000) });"
        : "let globalThis = new Proxy({}, { get: () => () => 'x'.repeat(1_000_000) });");
      return;
    }
    if (request.url === '/prepatch.js') {
      response.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8' });
      response.end(`
        Object.create = () => ({ injected: 'x'.repeat(1_000_000) });
        Object.keys = () => ['spoof'];
        Object.getOwnPropertyDescriptor = () => ({ value: 'spoof' });
        Array.isArray = () => false;
        Number.isSafeInteger = () => true;
        Array.prototype.map = () => ['spoof'];
        Array.prototype.toJSON = () => 'x'.repeat(1_000_000);
        Object.prototype.toJSON = () => 'x'.repeat(1_000_000);
        window.eval = () => () => 'x'.repeat(1_000_000);
      `);
      return;
    }
    response.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'content-security-policy': "script-src 'self'",
    });
    response.end(request.url === '/next'
      ? '<title>Next</title><main>New page</main>'
      : request.url === '/prepatched'
        ? '<title>Prepatched</title><script src="/prepatch.js"></script><output>0</output>'
        : request.url === '/lexical-window' || request.url === '/lexical-global'
          ? `<title>Lexical</title><script src="/${request.url.slice(1)}.js"></script><output>0</output>`
        : '<title>Eval</title><output>0</output>');
  });
  fixture.listen(0, '127.0.0.1');
  await once(fixture, 'listening');
  const url = `http://127.0.0.1:${fixture.address().port}/`;
  const host = spawn(process.env.BAMBOO_BROWSER_NODE || process.execPath, [path.join(__dirname, 'host.cjs')], {
    env: process.env,
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const pending = new Map();
  let nextId = 1;
  const lines = readline.createInterface({ input: host.stdout });
  lines.on('line', line => {
    const message = JSON.parse(line);
    if (message.event) return;
    const resolve = pending.get(message.id);
    if (resolve) { pending.delete(message.id); resolve(message); }
  });
  const call = (action, args = {}) => new Promise((resolve, reject) => {
    const id = nextId++;
    const timeout = setTimeout(() => { pending.delete(id); reject(new Error(`${action} timed out`)); }, 30_000);
    pending.set(id, message => { clearTimeout(timeout); resolve(message); });
    host.stdin.write(`${JSON.stringify({ id, action, args })}\n`);
  });
  try {
    const initial = (await call('state')).result;
    const blank = await call('eval', {
      expected_epoch: initial.page_epoch, expected_url: 'about:blank', code: '({blank: true})',
    });
    assert.equal(blank.ok, true, JSON.stringify(blank));
    assert.deepEqual(blank.result.value, { blank: true });
    const prepatchedUrl = `${url}prepatched`;
    const prepatched = (await call('navigate', { url: prepatchedUrl, expected_epoch: initial.page_epoch })).result;
    const prepatchedResult = await call('eval', {
      expected_epoch: prepatched.page_epoch, expected_url: prepatchedUrl, code: '({actual: 11})',
    });
    assert.equal(prepatchedResult.ok, true, JSON.stringify(prepatchedResult));
    assert.deepEqual(prepatchedResult.result.value, { actual: 11 });
    const prepatchedArray = await call('eval', {
      expected_epoch: prepatched.page_epoch, expected_url: prepatchedUrl, code: '[3]',
    });
    assert.equal(prepatchedArray.ok, true, JSON.stringify(prepatchedArray));
    assert.deepEqual(prepatchedArray.result.value, [3]);
    const lexicalWindowUrl = `${url}lexical-window`;
    const lexicalWindow = (await call('navigate', {
      url: lexicalWindowUrl, expected_epoch: prepatched.page_epoch,
    })).result;
    const lexicalWindowResult = await call('eval', {
      expected_epoch: lexicalWindow.page_epoch, expected_url: lexicalWindowUrl,
      code: '({actual: 15})',
    });
    assert.equal(lexicalWindowResult.ok, true, JSON.stringify(lexicalWindowResult));
    assert.deepEqual(lexicalWindowResult.result.value, { actual: 15 });
    const lexicalGlobalUrl = `${url}lexical-global`;
    const lexicalGlobal = (await call('navigate', {
      url: lexicalGlobalUrl, expected_epoch: lexicalWindow.page_epoch,
    })).result;
    const lexicalGlobalResult = await call('eval', {
      expected_epoch: lexicalGlobal.page_epoch, expected_url: lexicalGlobalUrl,
      code: '({actual: 16})',
    });
    assert.equal(lexicalGlobalResult.ok, true, JSON.stringify(lexicalGlobalResult));
    assert.deepEqual(lexicalGlobalResult.result.value, { actual: 16 });
    const navigated = (await call('navigate', { url, expected_epoch: lexicalGlobal.page_epoch })).result;
    const expected = { expected_epoch: navigated.page_epoch, expected_url: url };
    const read = await call('eval', { ...expected, code: 'document.querySelector("output").textContent' });
    assert.equal(read.ok, true);
    assert.equal(read.result.value, '0');
    assert.equal(read.result.url, url);
    assert.equal(read.result.active_tab_id, navigated.active_tab_id);
    assert.equal((await call('eval', { ...expected, code: 'typeof process' })).result.value, 'undefined');
    const duringOverride = await call('eval', {
      ...expected,
      code: '(() => { JSON.stringify = () => "\\\"spoofed\\\""; return {actual: 7}; })()',
    });
    assert.equal(duringOverride.ok, true);
    assert.deepEqual(duringOverride.result.value, { actual: 7 });
    const existingOverride = await call('eval', { ...expected, code: '({actual: 8})' });
    assert.equal(existingOverride.ok, true);
    assert.deepEqual(existingOverride.result.value, { actual: 8 });
    const hugeOverride = await call('eval', {
      ...expected,
      code: '(() => { JSON.stringify = () => "x".repeat(1_000_000); return {actual: 9}; })()',
    });
    assert.equal(hugeOverride.ok, true);
    assert.deepEqual(hugeOverride.result.value, { actual: 9 });
    const intrinsicOverride = await call('eval', {
      ...expected,
      code: `(() => {
        Object.create = () => ({ injected: 'x'.repeat(1_000_000) });
        Object.keys = () => ['spoof'];
        Object.getOwnPropertyDescriptor = () => ({ value: 'spoof' });
        Array.isArray = () => false;
        Number.isSafeInteger = () => true;
        Array.prototype.map = () => ['spoof'];
        Array.prototype.toJSON = () => 'x'.repeat(1_000_000);
        Object.prototype.toJSON = () => 'x'.repeat(1_000_000);
        window.eval = () => () => 'x'.repeat(1_000_000);
        return { actual: 12 };
      })()`,
    });
    assert.equal(intrinsicOverride.ok, true, JSON.stringify(intrinsicOverride));
    assert.deepEqual(intrinsicOverride.result.value, { actual: 12 });
    const overriddenArray = await call('eval', { ...expected, code: '[4]' });
    assert.equal(overriddenArray.ok, true, JSON.stringify(overriddenArray));
    assert.deepEqual(overriddenArray.result.value, [4]);
    const changingLength = await call('eval', {
      ...expected,
      code: `(() => {
        const values = [3];
        let reads = 0;
        return new Proxy(values, { get(target, property) {
          if (property === 'length') return ++reads === 1 ? 1 : 1_000_000;
          return Reflect.get(target, property);
        }});
      })()`,
    });
    assert.equal(changingLength.ok, true, JSON.stringify(changingLength));
    assert.deepEqual(changingLength.result.value, [3]);

    const changed = await call('eval', {
      ...expected,
      code: '(() => { const output = document.querySelector("output"); output.textContent = "1"; return {count: 1, ok: true}; })()',
    });
    assert.equal(changed.ok, true);
    assert.deepEqual(changed.result.value, { count: 1, ok: true });
    const dom = (await call('dom')).result;
    const screenshot = (await call('screenshot')).result;
    assert.match(dom.html, /<output>1<\/output>/);
    assert.equal(dom.page_epoch, changed.result.page_epoch);
    assert.equal(screenshot.page_epoch, changed.result.page_epoch);
    assert.equal(screenshot.active_tab_id, changed.result.active_tab_id);
    assert.ok(Buffer.from(screenshot.data, 'base64').length > 1000);

    const scheduledDialog = await call('eval', {
      ...expected,
      code: 'setTimeout(() => alert("Pending eval dialog"), 120); "scheduled"',
    });
    assert.equal(scheduledDialog.ok, true, JSON.stringify(scheduledDialog));
    let dialogState;
    for (let attempt = 0; attempt < 50; attempt++) {
      dialogState = (await call('state')).result;
      if (dialogState.pending_dialog) break;
      await new Promise(resolve => setTimeout(resolve, 20));
    }
    assert.equal(dialogState.pending_dialog.message, 'Pending eval dialog');
    const blockedEval = await call('eval', {
      ...expected, code: 'document.querySelector("output").textContent = "not-run"',
    });
    assert.equal(blockedEval.code, 'dialog_pending', JSON.stringify(blockedEval));
    assert.equal((await call('dialog_respond', {
      dialog_id: dialogState.pending_dialog.dialog_id,
      accept: false,
      expected_epoch: expected.expected_epoch,
    })).ok, true);
    assert.match((await call('dom')).result.html, /<output>1<\/output>/);

    assert.equal((await call('eval', { ...expected, expected_url: `${url}wrong`, code: '1' })).code, 'stale_epoch');
    assert.equal((await call('eval', { ...expected, expected_epoch: initial.page_epoch, code: '1' })).code, 'stale_epoch');
    assert.equal((await call('eval', { ...expected, code: 'x'.repeat(8193) })).code, 'invalid_request');
    assert.equal((await call('eval', { ...expected, code: '(() => { const x = {}; x.self = x; return x; })()' })).code, 'browser_eval_error');
    assert.equal((await call('eval', { ...expected, code: '"x".repeat(70000)' })).code, 'browser_eval_error');
    const multibyte = await call('eval', { ...expected, code: '"汉".repeat(30000)' });
    assert.equal(multibyte.code, 'browser_eval_error');
    assert.equal(multibyte.error, 'browser_eval JavaScript failed');
    const emoji = await call('eval', { ...expected, code: '"💥".repeat(20000)' });
    assert.equal(emoji.code, 'browser_eval_error');
    assert.equal(emoji.error, 'browser_eval JavaScript failed');
    const exception = await call('eval', { ...expected, code: 'throw new Error("E".repeat(5000))' });
    assert.equal(exception.code, 'browser_eval_error');
    assert.equal(exception.error, 'browser_eval JavaScript failed');
    const hugeString = await call('eval', { ...expected, code: 'throw "S".repeat(1_000_000)' });
    assert.equal(hugeString.code, 'browser_eval_error');
    assert.equal(hugeString.error, 'browser_eval JavaScript failed');
    const hugeRejection = await call('eval', {
      ...expected, code: 'Promise.reject(new Error("R".repeat(1_000_000)))',
    });
    assert.equal(hugeRejection.code, 'browser_eval_error');
    assert.equal(hugeRejection.error, 'browser_eval JavaScript failed');
    const hostileError = await call('eval', {
      ...expected,
      code: `throw new Proxy({}, { get() { throw new Error('hostile error property'); } });`,
    });
    assert.equal(hostileError.code, 'browser_eval_error');
    assert.equal(hostileError.error, 'browser_eval JavaScript failed');
    const navigation = await call('eval', {
      ...expected,
      code: '(() => { location.href = "/next"; return "old"; })()',
    });
    assert.equal(navigation.code, 'stale_epoch', JSON.stringify(navigation));
    assert.match((await call('dom')).result.html, /New page/);
    const finalState = (await call('state')).result;
    const promiseOverride = await call('eval', {
      expected_epoch: finalState.page_epoch, expected_url: `${url}next`,
      code: `(() => {
        window.Promise = function () { throw new Error('page Promise invoked'); };
        window.setTimeout = function () { throw new Error('page timeout invoked'); };
        return { actual: 14 };
      })()`,
    });
    assert.equal(promiseOverride.ok, true, JSON.stringify(promiseOverride));
    assert.deepEqual(promiseOverride.result.value, { actual: 14 });
    const identityOverride = await call('eval', {
      expected_epoch: finalState.page_epoch, expected_url: `${url}next`,
      code: `(() => {
        window.globalThis = new Proxy({}, { get: () => () => 'x'.repeat(1_000_000) });
        window.window = new Proxy({}, { get: () => () => 'x'.repeat(1_000_000) });
        return { actual: 13 };
      })()`,
    });
    assert.equal(identityOverride.ok, true, JSON.stringify(identityOverride));
    assert.deepEqual(identityOverride.result.value, { actual: 13 });

    const slowUrl = `${url}slow-reload`;
    const slowPage = (await call('navigate', {
      url: slowUrl, expected_epoch: finalState.page_epoch,
    })).result;
    const slowResultPromise = call('eval', {
      expected_epoch: slowPage.page_epoch, expected_url: slowUrl,
      code: '(() => { location.reload(); return "old-document"; })()',
    });
    let reloadTimeout;
    try {
      await Promise.race([
        slowReloadStarted,
        new Promise((_, reject) => {
          reloadTimeout = setTimeout(() => reject(new Error('slow reload did not start')), 5000);
        }),
      ]);
    } finally {
      clearTimeout(reloadTimeout);
    }
    // The old document remains at the same URL and epoch while the response
    // is withheld. A successful result here would be stale once it commits.
    await new Promise(resolve => setTimeout(resolve, 150));
    releaseSlowReload();
    const slowResult = await slowResultPromise;
    assert.equal(slowResult.code, 'stale_epoch', JSON.stringify(slowResult));
    assert.ok(slowReloadRequests >= 2);

    let popupReady;
    for (let attempt = 0; attempt < 50; attempt++) {
      popupReady = (await call('state')).result;
      if (popupReady.page_epoch !== slowPage.page_epoch) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.notEqual(popupReady.page_epoch, slowPage.page_epoch, 'reload committed before popup eval');
    const popupResult = await call('eval', {
      expected_epoch: popupReady.page_epoch, expected_url: slowUrl,
      code: '(() => { window.open("/slow-popup-eval", "_blank"); return "old-tab"; })()',
    });
    assert.equal(popupResult.code, 'stale_epoch', JSON.stringify(popupResult));
    let popupTimer;
    try {
      await Promise.race([
        slowPopupStarted,
        new Promise((_, reject) => {
          popupTimer = setTimeout(() => reject(new Error('slow eval popup did not start')), 5000);
        }),
      ]);
    } finally {
      clearTimeout(popupTimer);
    }
    releaseSlowPopup();
    let popupState;
    for (let attempt = 0; attempt < 50; attempt++) {
      popupState = (await call('state')).result;
      if (popupState.tabs.length === 2 && popupState.url.endsWith('/slow-popup-eval')) break;
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    assert.equal(popupState.tabs.length, 2);
    assert.match(popupState.url, /\/slow-popup-eval$/);
    assert.notEqual(popupState.active_tab_id, popupReady.active_tab_id);
  } finally {
    releaseSlowReload();
    releaseSlowPopup();
    await call('close').catch(() => {});
    host.stdin.end();
    host.kill();
    fixture.closeAllConnections();
    fixture.close();
    await once(fixture, 'close');
  }
});
