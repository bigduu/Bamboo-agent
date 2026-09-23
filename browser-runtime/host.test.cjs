const assert = require('node:assert/strict');
const http = require('node:http');
const { spawn } = require('node:child_process');
const readline = require('node:readline');
const { once } = require('node:events');
const { test } = require('node:test');
const path = require('node:path');

test('one isolated page supplies DOM, screenshot, and interactive changes without an iframe', async () => {
  const fixture = http.createServer((_request, response) => {
    response.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'x-frame-options': 'DENY',
      'content-security-policy': "frame-ancestors 'none'",
    });
    response.end('<style>button,input,output,select{display:block}</style><button id="increment" onclick="const output=document.querySelector(\'output\');output.textContent=String(Number(output.textContent)+1)">Increment</button><input id="name" aria-label="Name"><output>0</output><select id="single" onchange="document.querySelector(\'#chosen\').textContent=\'Single \'+this.value"><option value="">None</option><option value="red">Red</option></select><select id="multi" multiple onchange="document.querySelector(\'#chosen\').textContent=\'Multi \'+Array.from(this.selectedOptions).map(option=>option.value).join(\',\')"><option value="green">Green</option><option value="blue">Blue</option><option value="yellow">Yellow</option></select><output id="chosen">No selection</output>');
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

test('popup and explicit tabs keep active DOM, frames, and epochs on one page', async () => {
  const fixture = http.createServer((request, response) => {
    response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    if (request.url === '/one') {
      response.end('<title>One</title><button id="popup" onclick="window.open(\'/two\', \'_blank\')">Open Two</button><main>One page</main>');
    } else if (request.url === '/two') {
      response.end('<title>Two</title><main>Two page</main>');
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

test('bounded page eval changes the same DOM and rejects stale or unsafe results', async () => {
  const fixture = http.createServer((request, response) => {
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
    assert.ok(lexicalGlobalResult.ok || lexicalGlobalResult.code === 'browser_eval_error', JSON.stringify(lexicalGlobalResult));
    if (lexicalGlobalResult.ok) assert.deepEqual(lexicalGlobalResult.result.value, { actual: 16 });
    else assert.ok(lexicalGlobalResult.error.length <= 2048);
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

    assert.equal((await call('eval', { ...expected, expected_url: `${url}wrong`, code: '1' })).code, 'stale_epoch');
    assert.equal((await call('eval', { ...expected, expected_epoch: initial.page_epoch, code: '1' })).code, 'stale_epoch');
    assert.equal((await call('eval', { ...expected, code: 'x'.repeat(8193) })).code, 'invalid_request');
    assert.equal((await call('eval', { ...expected, code: '(() => { const x = {}; x.self = x; return x; })()' })).code, 'browser_eval_error');
    assert.equal((await call('eval', { ...expected, code: '"x".repeat(70000)' })).code, 'browser_eval_error');
    const multibyte = await call('eval', { ...expected, code: '"汉".repeat(30000)' });
    assert.equal(multibyte.code, 'browser_eval_error');
    assert.match(multibyte.error, /64 KiB/);
    const emoji = await call('eval', { ...expected, code: '"💥".repeat(20000)' });
    assert.equal(emoji.code, 'browser_eval_error');
    assert.match(emoji.error, /64 KiB/);
    const exception = await call('eval', { ...expected, code: 'throw new Error("E".repeat(5000))' });
    assert.equal(exception.code, 'browser_eval_error');
    assert.ok(exception.error.length <= 2048);
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
    assert.ok(identityOverride.ok || identityOverride.code === 'browser_eval_error', JSON.stringify(identityOverride));
    if (identityOverride.ok) assert.deepEqual(identityOverride.result.value, { actual: 13 });
    else assert.ok(identityOverride.error.length <= 2048);
  } finally {
    await call('close').catch(() => {});
    host.stdin.end();
    host.kill();
    fixture.close();
    await once(fixture, 'close');
  }
});
