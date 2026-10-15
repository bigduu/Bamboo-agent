'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const CLI = path.join(__dirname, 'bamboo-task.cjs');
const BIRTH = '2026-10-15T10:00:00.123456789Z';
const SECRET = 'fixture-private-prompt-or-key-NEVER-LOG';
const SUP = 'bamboo-default-supervisor';
const session = (id = 'root', extra = {}) => ({ id, created_at: BIRTH, kind: 'root', parent_session_id: null,
  is_running: false, has_pending_question: false, message_count: 2, title: SECRET, last_run_error: SECRET, ...extra });
const bootstrap = { schema_version: 1, server: { product: 'bamboo', version: '0.0.0' },
  api: { name: 'bamboo.agent', canonical_base_path: '/api/v1', min_version: 1, max_version: 1 }, capabilities: ['realtime.stop_control.v1'] };
function json(res, value, status = 200, headers = {}) { res.writeHead(status, { 'Content-Type': 'application/json', ...headers }); res.end(JSON.stringify(value)); }
function apiError(res, status, message = SECRET) { json(res, { error: { type: 'api_error', message } }, status); }
async function fixture(t, override = () => false) {
  const calls = [];
  const server = http.createServer(async (req, res) => {
    const chunks = []; for await (const c of req) chunks.push(c);
    const text = Buffer.concat(chunks).toString();
    const call = { method: req.method, path: req.url, headers: req.headers, body: text ? JSON.parse(text) : undefined };
    calls.push(call);
    try {
      if (await override(call, res)) return;
      if (call.path === '/api/v1/bootstrap') return json(res, bootstrap);
      if (/^\/api\/v1\/sessions\/bamboo-cli-probe-/.test(call.path)) return apiError(res, 400, 'Exact output requires one Bamboo-Session-Created-At header');
      if (call.path === '/api/v1/supervisor/default') return json(res, { session_id: SUP, incarnation_id: 'incarnation-1', created: false });
      if (call.path === '/api/v1/sessions' && call.method === 'POST') return json(res, { session: session() }, 201);
      if (call.path === '/api/v1/sessions/root' || call.path === `/api/v1/sessions/${SUP}`) return json(res, { session: session(call.path.split('/').at(-1)) });
      if (call.path === '/api/v1/chat') return json(res, { session_id: call.body.session_id, message_id: call.body.message_id || 'native-message-1', ingress_seq: 7, status: 'queued', stream_url: '/events/root' });
      if (call.path.endsWith('/execute')) return json(res, { session_id: call.path.split('/').at(-2), run_id: 'run-1', status: 'started', events_url: '/events/root' }, 202,
        { 'Bamboo-Input-Id': call.headers['bamboo-input-id'] || '', 'Bamboo-Ingress-Seq': '7', 'Bamboo-Output-Scope': 'activation-receipt', 'Bamboo-Session-Created-At': BIRTH });
      if (call.path.endsWith('/stop')) return json(res, { session_id: call.path.split('/').at(-2), run_id: call.headers['bamboo-run-id'], success: true });
      return apiError(res, 404);
    } catch { if (!res.destroyed) apiError(res, 500); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'bamboo-task-test-'));
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const prompt = path.join(directory, 'prompt.txt'); fs.writeFileSync(prompt, SECRET);
  return { calls, prompt, base: `http://127.0.0.1:${server.address().port}`, server };
}
async function cli(f, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [CLI, ...args, ...(f ? ['--server-url', f.base] : [])], { stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', c => { stdout += c; options.onOutput?.(child, stdout); });
    child.stderr.on('data', c => { stderr += c; });
    child.on('error', reject);
    const timer = setTimeout(() => { child.kill('SIGKILL'); reject(new Error('fixture CLI stalled')); }, 6000);
    child.on('close', (code, signal) => {
      clearTimeout(timer);
      try { resolve({ code, signal, stdout, stderr, lines: stdout.trim() ? stdout.trim().split('\n').map(s => JSON.parse(s)) : [] }); }
      catch (e) { reject(e); }
    });
    child.stdin.end(options.input || '');
    options.started?.(child);
  });
}
function last(r) { assert.equal(r.stderr, ''); return r.lines.at(-1); }
function privateSafe(r) { assert.ok(!`${r.stdout}${r.stderr}`.includes(SECRET)); }
function posts(f) { return f.calls.filter(c => c.method === 'POST'); }
const submit = f => ['submit', '--session-id', 'root', '--prompt-file', f.prompt, '--request-id', 'request-1'];

test('loopback, path identities, unknown/duplicate flags and incomplete selections fail before HTTP', async t => {
  const f = await fixture(t);
  for (const args of [
    ['stop', '--session-id', 'root'], ['stop', '--session-id', '../root', '--run-id', 'x'],
    ['status', '--session-id', 'root', '--run-id', 'x'], ['status', '--session-id', 'root', '--include-output'],
    ['events', '--session-id', 'x%2fy'], ['events', '--session-id', 'root', '--max-events', '0'],
    ['inspect', '--unknown'], ['inspect', '--timeout-ms', '10', '--timeout-ms', '20'],
    ['submit', '--supervisor', '--session-id', 'root', '--prompt-file', '-'],
    ['submit', '--session-id', SUP, '--prompt-file', '-'],
    [...submit(f), '--permission-mode', 'bypass'], [...submit(f), '--model', 'model'],
    [...submit(f), '--workspace', '/tmp'],
  ]) { const r = await cli(f, args); assert.equal(r.code, 2, r.stdout); privateSafe(r); }
  for (const base of ['http://example.com', 'http://localhost:9562', 'file:///tmp/api', 'http://u:p@127.0.0.1', 'http://127.0.0.1/api/v1?key=secret', 'http://127.0.0.1/api/v1/../secret']) {
    const r = await cli(null, ['inspect', '--server-url', base]); assert.equal(r.code, 2); assert.ok(!r.stdout.includes(base));
  }
  assert.equal(f.calls.length, 0);
});

test('inspect distinguishes healthy bootstrap from receipt/run support and rejects mismatched product', async t => {
  const f = await fixture(t);
  const r = await cli(f, ['inspect']); assert.equal(r.code, 0); assert.equal(last(r).mutations_supported, true);
  const old = await fixture(t, (c, res) => { if (c.path.includes('bamboo-cli-probe-')) { res.writeHead(404); res.end('legacy'); return true; } });
  const read = await cli(old, ['inspect']); assert.equal(read.code, 0); assert.equal(last(read).mutations_supported, false);
  const wrong = await fixture(t, (c, res) => { if (c.path.endsWith('/bootstrap')) { json(res, { ...bootstrap, server: { product: SECRET } }); return true; } });
  const bad = await cli(wrong, ['inspect']); assert.equal(bad.code, 6); privateSafe(bad);
});

test('old server that ignores run headers receives no submit/bootstrap/stop POST', async t => {
  let cancelledSuccessor = false;
  const f = await fixture(t, (c, res) => {
    if (c.path.includes('bamboo-cli-probe-')) { apiError(res, 404); return true; }
    if (c.path.endsWith('/stop')) { cancelledSuccessor = true; json(res, { success: true }); return true; }
  });
  for (const args of [submit(f), ['submit', '--supervisor', '--prompt-file', f.prompt], ['stop', '--session-id', 'root', '--run-id', 'old']]) {
    const r = await cli(f, args); assert.equal(r.code, 6); assert.equal(last(r).error.code, 'receipt_run_api_unavailable'); privateSafe(r);
  }
  assert.equal(posts(f).length, 0); assert.equal(cancelledSuccessor, false);
});

test('ordinary submission binds original birth and accepted receipt to real run without retries', async t => {
  const f = await fixture(t);
  const r = await cli(f, submit(f)); const out = last(r);
  assert.equal(r.code, 0); assert.equal(out.run_id, 'run-1'); assert.equal(out.execution_scope, 'selected_input');
  assert.equal(out.session_created_at, BIRTH); assert.equal(out.message_id, 'native-message-1'); assert.equal(out.event_scope, 'session');
  const p = posts(f); assert.equal(p.length, 2);
  assert.equal(p[0].body.message, SECRET); assert.equal(p[0].headers['idempotency-key'], 'cli:request-1:chat');
  assert.equal(p[1].headers['idempotency-key'], 'cli:request-1:execute');
  assert.equal(p[1].headers['bamboo-session-created-at'], BIRTH); assert.equal(p[1].headers['bamboo-input-id'], 'native-message-1');
  assert.deepEqual(p[1].body, {}); assert.ok(!Object.hasOwn(out, 'task_id')); privateSafe(r);
});

test('ordinary selected input keeps Native chat path instead of referenced legacy ingress', async t => {
  let referencedIngress = false;
  const f = await fixture(t, (c, res) => {
    if (c.path === '/api/v1/chat') {
      referencedIngress = Object.hasOwn(c.body, 'message_id');
      json(res, { session_id: 'root', message_id: 'native-message', ingress_seq: 9 }); return true;
    }
    if (c.path.endsWith('/execute')) {
      if (referencedIngress) apiError(res, 409, 'Receipt execute requires an ordinary Native Root without a specific wait or legacy ingress');
      else json(res, { session_id: 'root', run_id: 'native-run', status: 'started' }, 202, {
        'Bamboo-Input-Id': 'native-message', 'Bamboo-Ingress-Seq': '9', 'Bamboo-Output-Scope': 'activation-receipt', 'Bamboo-Session-Created-At': BIRTH });
      return true;
    }
  });
  const r = await cli(f, submit(f));
  assert.equal(r.code, 0, r.stdout);
  assert.equal(last(r).message_id, 'native-message');
  assert.equal(last(r).run_id, 'native-run');
  assert.equal(referencedIngress, false);
});

test('Supervisor bootstrap preserves session activation scope and does not fabricate selected binding', async t => {
  const f = await fixture(t);
  const r = await cli(f, ['submit', '--supervisor', '--prompt-file', '-'], { input: SECRET });
  assert.equal(r.code, 0); assert.equal(last(r).supervisor_incarnation_id, 'incarnation-1'); assert.equal(last(r).execution_scope, 'session_activation');
  assert.deepEqual(posts(f).map(c => c.path), ['/api/v1/supervisor/default', '/api/v1/chat', `/api/v1/sessions/${SUP}/execute`]);
  assert.equal(posts(f).at(-1).headers['bamboo-input-id'], undefined);
  privateSafe(r);
});

test('denied Supervisor bootstrap and child target append no chat', async t => {
  const denied = await fixture(t, (c, res) => { if (c.path.endsWith('/supervisor/default')) { apiError(res, 403); return true; } });
  const r = await cli(denied, ['submit', '--supervisor', '--prompt-file', denied.prompt]);
  assert.equal(r.code, 4); assert.equal(last(r).admission.chat, 'not_sent'); assert.equal(posts(denied).length, 1); privateSafe(r);
  const child = await fixture(t, (c, res) => { if (c.path === '/api/v1/sessions/root') { json(res, { session: session('root', { kind: 'child', parent_session_id: 'parent' }) }); return true; } });
  const c = await cli(child, submit(child)); assert.equal(c.code, 6); assert.equal(posts(child).length, 0);
});

test('new Root creation precedes birth read/chat and leaves created identity visible on partial failure', async t => {
  const f = await fixture(t, (c, res) => { if (c.path === '/api/v1/chat') { apiError(res, 409); return true; } });
  const r = await cli(f, ['submit', '--new', '--prompt-file', f.prompt, '--request-id', 'new-1', '--workspace', '/tmp/public-fixture', '--model', 'model', '--provider', 'provider']);
  assert.equal(r.code, 7); assert.equal(last(r).session_id, 'root'); assert.equal(last(r).admission.create, 'accepted');
  assert.equal(last(r).admission.chat, 'rejected'); assert.equal(last(r).admission.execute, 'not_sent');
  assert.equal(posts(f)[0].headers['idempotency-key'], 'cli:new-1:create');
  assert.equal(posts(f)[0].body.workspace_path, '/tmp/public-fixture'); assert.equal(posts(f)[0].body.permission_mode, undefined);
});

test('accepted chat + HTTP/auth/server execute rejection stays partial/unknown; no duplicate or rollback', async t => {
  for (const code of [401, 403, 409, 503]) {
    const f = await fixture(t, (c, res) => { if (c.path.endsWith('/execute')) { apiError(res, code); return true; } });
    const r = await cli(f, submit(f)); const out = last(r);
    assert.equal(r.code, 7); assert.equal(out.message_id, 'native-message-1'); assert.equal(out.session_id, 'root');
    assert.equal(out.admission.chat, 'accepted'); assert.equal(out.admission.execute, code >= 500 ? 'unknown' : 'rejected');
    assert.equal(out.error.http_status, code); assert.equal(posts(f).length, 2); privateSafe(r);
  }
});

test('lost/invalid chat response reports uncertain admission with stable request ID and never executes', async t => {
  for (const mode of ['disconnect', 'non-json', 'oversize']) {
    const f = await fixture(t, (c, res) => {
      if (c.path !== '/api/v1/chat') return;
      if (mode === 'disconnect') res.destroy();
      else { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(mode === 'non-json' ? SECRET : ' '.repeat(1024 * 1024 + 1)); }
      return true;
    });
    const r = await cli(f, submit(f)); assert.equal(r.code, 7); assert.equal(last(r).request_id, 'request-1');
    assert.equal(last(r).admission.execute, 'not_sent'); assert.equal(posts(f).length, 1); privateSafe(r);
  }
});

test('selected execute must acknowledge real scope, identity and run; already_running is never adopted', async t => {
  for (const value of [{ session_id: 'other', run_id: 'run-1', status: 'started' }, { session_id: 'root', status: 'started' }, { session_id: 'root', run_id: 'successor', status: 'already_running' }]) {
    const f = await fixture(t, (c, res) => { if (c.path.endsWith('/execute')) { json(res, value); return true; } });
    const r = await cli(f, submit(f)); assert.equal(r.code, 7); privateSafe(r);
  }
});

test('stale fenced stop cannot cancel successor and successful acknowledgement must match IDs', async t => {
  let cancelled = false;
  const f = await fixture(t, (c, res) => {
    if (c.path.endsWith('/stop')) { if (c.headers['bamboo-run-id'] !== 'successor') { apiError(res, 409); } else { cancelled = true; json(res, { success: true, session_id: 'root', run_id: 'successor' }); } return true; }
  });
  const stale = await cli(f, ['stop', '--session-id', 'root', '--run-id', 'old']); assert.equal(stale.code, 5); assert.equal(cancelled, false);
  const stop = await cli(f, ['stop', '--session-id', 'root', '--run-id', 'successor']); assert.equal(stop.code, 0); assert.equal(last(stop).cancellation_acknowledged, true); assert.equal(cancelled, true);
  assert.equal(posts(f).length, 2);
  const wrong = await fixture(t, (c, res) => { if (c.path.endsWith('/stop')) { json(res, { success: true }); return true; } });
  const uncertain = await cli(wrong, ['stop', '--session-id', 'root', '--run-id', 'old']); assert.equal(uncertain.code, 7); assert.equal(last(uncertain).error.code, 'fenced_stop_not_acknowledged');
  assert.equal(last(uncertain).admission_outcome, 'unknown');
});

test('metadata status works on old backend and excludes private title, errors and transcript', async t => {
  const f = await fixture(t);
  const r = await cli(f, ['status', '--session-id', 'root']); assert.equal(r.code, 0); assert.equal(last(r).observation_scope, 'session'); privateSafe(r);
  assert.deepEqual(f.calls.map(c => c.path), ['/api/v1/sessions/root']);
});

const output = () => ({ session_id: 'root', run_id: 'run-1', session_created_at: BIRTH, output_scope: 'visible_messages', runner_status: 'completed',
  input: { message_id: 'request-1', ingress_seq: 7 }, output: { version: 4, messages: [{ id: 'assistant-1', content: SECRET }], terminal: 'complete', history_committed: true } });
const exact = ['status', '--session-id', 'root', '--run-id', 'run-1', '--session-created-at', BIRTH];
test('exact status uses original nanosecond birth; visible output requires explicit opt-in', async t => {
  const f = await fixture(t, (c, res) => { if (c.path.endsWith('/output')) { assert.equal(c.headers['bamboo-session-created-at'], BIRTH); json(res, output()); return true; } });
  const r = await cli(f, exact); assert.equal(r.code, 0); assert.equal(last(r).runner_status, 'completed'); assert.equal(last(r).output.message_count, 1); privateSafe(r);
  const raw = await cli(f, [...exact, '--include-output']); assert.equal(raw.code, 0); assert.equal(last(raw).output.messages[0].content, SECRET);
});

test('replaced/wrong-kind/unretained runs preserve HTTP distinctions without history/successor fallback', async t => {
  for (const code of [403, 409, 410, 503]) {
    const f = await fixture(t, (c, res) => { if (c.path.endsWith('/output')) { apiError(res, code); return true; } });
    const r = await cli(f, exact); assert.equal(r.code, code === 403 ? 4 : 5); assert.equal(last(r).error.http_status, code); privateSafe(r);
    assert.equal(f.calls.length, 2); assert.equal(posts(f).length, 0);
  }
  for (const change of [{ run_id: 'successor' }, { session_created_at: '2026-10-15T10:00:00.123456780Z' }]) {
    const f = await fixture(t, (c, res) => { if (c.path.endsWith('/output')) { json(res, { ...output(), ...change }); return true; } });
    const r = await cli(f, exact); assert.equal(r.code, 6);
  }
});

test('redirects are never followed and backend auth/HTML errors never leak response bodies', async t => {
  for (const code of [302, 401, 403, 500]) {
    const f = await fixture(t, (c, res) => { res.writeHead(code, { Location: '/steal', 'Content-Type': 'text/html' }); res.end(SECRET); return true; });
    const r = await cli(f, ['status', '--session-id', 'root']); assert.notEqual(r.code, 0); assert.equal(f.calls.length, 1); privateSafe(r);
  }
});

test('SSE handles split UTF-8, CRLF, multiline JSON, comments and keepalive, preserving only original run IDs', async t => {
  const f = await fixture(t, (c, res) => {
    if (!c.path.endsWith('/events')) return;
    res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8' });
    const bytes = Buffer.from(': heartbeat\r\n\r\ndata: [KEEPALIVE]\r\n\r\ndata: {"type":"execution_started",\r\ndata: "session_id":"root","run_id":"run-1"}\r\n\r\ndata: {"type":"token","content":"竹子"}\r\n\r\ndata: {"type":"complete"}\r\n\r\n');
    let i = 0; const timer = setInterval(() => { if (i < bytes.length) res.write(bytes.subarray(i, ++i)); else { clearInterval(timer); res.end(); } }, 1);
    res.on('close', () => clearInterval(timer)); return true;
  });
  const r = await cli(f, ['events', '--session-id', 'root', '--include-output']); assert.equal(r.code, 0); assert.equal(r.lines.length, 4);
  assert.equal(r.lines[0].event.run_id, 'run-1'); assert.equal(r.lines[1].event.content, '竹子'); assert.equal(r.lines[1].event.run_id, undefined);
  assert.equal(r.lines[2].event.run_id, undefined); assert.ok(r.lines.every(v => v.event_scope === 'session')); assert.equal(last(r).stream_end, 'session_boundary');
});

test('SSE defaults omit token/tool/approval/reasoning content and clarification closes with pause', async t => {
  const f = await fixture(t, (c, res) => {
    if (!c.path.endsWith('/events')) return;
    res.writeHead(200, { 'Content-Type': 'text/event-stream' });
    res.end(['token', 'tool_start', 'reasoning_token', 'tool_approval_requested', 'need_clarification'].map(type => `data: ${JSON.stringify({ type, content: SECRET, parameters: SECRET, arguments: SECRET })}\n\n`).join('')); return true;
  });
  const r = await cli(f, ['events', '--session-id', 'root']); assert.equal(r.code, 0); privateSafe(r); assert.equal(last(r).stream_end, 'session_pause');
});

test('SSE event limit, DONE and clean EOF are bounded observations, not goal completion', async t => {
  for (const [data, max, reason] of [['data: {"type":"token"}\n\ndata: {"type":"token"}\n\n', '1', 'event_limit'], ['data: [DONE]\n\n', '10', 'session_boundary'], [': hi\n\n', '10', 'eof']]) {
    const f = await fixture(t, (c, res) => { if (c.path.endsWith('/events')) { res.writeHead(200, { 'Content-Type': 'text/event-stream' }); res.end(data); return true; } });
    const r = await cli(f, ['events', '--session-id', 'root', '--max-events', max]); assert.equal(r.code, 0); assert.equal(last(r).stream_end, reason); assert.equal(last(r).task_complete, undefined);
  }
});

test('malformed, truncated, invalid UTF-8 and oversized SSE fail with sanitized protocol error', async t => {
  for (const data of [`data: ${SECRET}\n\n`, 'data: {"type":"token"}\n', Buffer.from([0xff]), `data: ${'x'.repeat(256 * 1024)}\n\n`, 'data: {"type":"token","run_id":"../evil"}\n\n']) {
    const f = await fixture(t, (c, res) => { if (c.path.endsWith('/events')) { res.writeHead(200, { 'Content-Type': 'text/event-stream' }); res.end(data); return true; } });
    const r = await cli(f, ['events', '--session-id', 'root']); assert.equal(r.code, 6, r.stdout); privateSafe(r);
  }
});

test('stream timeout/interrupt close observer without sending any stop or execute', async t => {
  const f = await fixture(t, (c, res) => { if (c.path.endsWith('/events')) { res.writeHead(200, { 'Content-Type': 'text/event-stream' }); res.write('data: {"type":"token"}\n\n'); return true; } });
  const timed = await cli(f, ['events', '--session-id', 'root', '--timeout-ms', '120']); assert.equal(timed.code, 124);
  let signalled = false;
  const interrupted = await cli(f, ['events', '--session-id', 'root'], { onOutput(child) { if (!signalled) { signalled = true; child.kill('SIGINT'); } } });
  assert.equal(interrupted.code, 130); assert.equal(posts(f).length, 0);
});

test('timeout after chat may have been admitted is unknown, includes request ID, never retries', async t => {
  const f = await fixture(t, (c, res) => { if (c.path === '/api/v1/chat') return true; });
  const r = await cli(f, [...submit(f), '--timeout-ms', '200']); assert.equal(r.code, 7); assert.equal(last(r).admission.chat, 'unknown');
  assert.equal(last(r).error.cause_exit_code, 124); assert.equal(posts(f).length, 1); privateSafe(r);
});

test('invalid/oversize/empty prompt is rejected before any HTTP or Supervisor creation', async t => {
  const f = await fixture(t);
  for (const input of ['', '  ', Buffer.alloc(128 * 1024 + 1, 'x'), Buffer.from([0xff]), '/goal turn on']) {
    const r = await cli(f, ['submit', '--supervisor', '--prompt-file', '-'], { input }); assert.equal(r.code, 2);
  }
  const missing = await cli(f, ['submit', '--supervisor', '--prompt-file', path.join(os.tmpdir(), 'bamboo-no-such-prompt-1997')]); assert.equal(missing.code, 2);
  assert.equal(f.calls.length, 0);
});
