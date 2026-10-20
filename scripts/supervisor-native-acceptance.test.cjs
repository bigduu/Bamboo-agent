'use strict';
// All cases here are protocol/component tests. The HTTP Host below is synthetic.
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const { spawnSync } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const d = require('./supervisor-native-acceptance.cjs');
const BIRTH = '2026-10-21T01:02:03.123456789Z';
const SECRET = 'PRIVATE_FIXTURE_SENTINEL_NOT_LOGGED';
const FIXTURE_ROOT = path.join(os.tmpdir(), 'bamboo-v2-native-fixture-example');
const root = (id, mode = 'standard') => ({ id, created_at: BIRTH, kind: 'root', parent_session_id: null,
  root_session_id: id, spawn_depth: 0, permission_mode: 'default', bypass_permissions: false,
  thinking_mode: mode, is_running: false, has_pending_question: false, message_count: 0, last_run_status: null });
const tools = ['spawn_session', 'SubAgent', 'Read', 'session_history'].map(name => ({ type: 'function', function: { name } }));
const user = content => ({ role: 'user', content });
const tool = content => ({ role: 'tool', tool_call_id: 'fixture', content: typeof content === 'string' ? content : JSON.stringify(content) });
const request = content => ({ messages: [user(content)], tools });
const c = mode => ({ mode, api_base: 'http://127.0.0.1:39001', host_pid: 123, host_started_at: 'fixture-start',
  data_dir: path.join(FIXTURE_ROOT, 'data-' + mode), workspace_path: path.join(FIXTURE_ROOT, 'work-' + mode),
  project_id: 'project-' + mode, supervisor_created_at: BIRTH, ordinary_session_id: 'ordinary-' + mode,
  ordinary_created_at: BIRTH, provider_port: mode === 'standard' ? 39002 : 39004,
  operation_id: randomUUID(), expected_standard_ticket_denial: false });
function receipt() {
  const standard = c('standard'), ultra = c('ultra'); ultra.api_base = 'http://127.0.0.1:39003';
  return { schema: 'bamboo.v2-native-fixture.v1', source_sha: d.SOURCE, tree_sha: d.TREE, fixture_id: randomUUID(),
    fixture_root: FIXTURE_ROOT, injected_provider: 'no_auth_local_scripted_no_fallback',
    binary: { path: path.join(os.tmpdir(), 'readonly-fixtures', 'bamboo'), sha256: 'a'.repeat(64) }, cases: [standard, ultra] };
}
const fails = (fn, code) => assert.throws(fn, e => e instanceof d.Failure && e.code === code);

test('exact source/tree required; old a473, v2-only and version-looking values are not build evidence', () => {
  assert.equal(d.validateReceipt(receipt()).source_sha, d.SOURCE);
  for (const source of ['a473f69b856962dd3dbdbfb2051b4fa4aa6e3041', '2bc1706ba9b23e70f8478d97d47754f3aff864a4', '0.0.0']) {
    const v = receipt(); v.source_sha = source; fails(() => d.validateReceipt(v), 'host_source_mismatch');
  }
  const v = receipt(); v.tree_sha = 'b'.repeat(40); fails(() => d.validateReceipt(v), 'host_source_mismatch');
});
test('remote/credential/query/redirect-looking origins are refused before HTTP', () => {
  for (const url of ['http://localhost:39001', 'https://127.0.0.1:39001', 'http://u:p@127.0.0.1:39001',
    'http://127.0.0.1:39001?key=' + SECRET, 'http://127.0.0.1:39001/api/v1', 'http://example.com:39001', 'http://127.0.0.1:80']) {
    fails(() => d.origin(url), 'literal_loopback_required');
  }
});
test('receipt cannot add credentials, provider fallback, cache paths or nonisolated directories', () => {
  let v = receipt(); v.token = SECRET; fails(() => d.validateReceipt(v), 'unknown_receipt_fields');
  v = receipt(); v.cases[0].cookie = SECRET; fails(() => d.validateReceipt(v), 'unknown_case_fields');
  v = receipt(); v.injected_provider = 'configured_default'; fails(() => d.validateReceipt(v), 'injected_local_provider_required');
  v = receipt(); v.binary.path = path.join(os.tmpdir(), 'target', 'debug', 'bamboo'); fails(() => d.validateReceipt(v), 'exported_host_artifact_required');
  v = receipt(); v.cases[0].workspace_path = path.resolve(os.tmpdir(), 'unrelated-user'); fails(() => d.validateReceipt(v), 'fixture_paths_required');
});
test('mode hosts and provider ports must be separate; no pretending an Ultra denial is Standard coverage', () => {
  let v = receipt(); v.cases[1].mode = 'standard'; fails(() => d.validateReceipt(v), 'unique_modes_required');
  v = receipt(); v.cases[1].api_base = v.cases[0].api_base; fails(() => d.validateReceipt(v), 'separate_mode_hosts_required');
  v = receipt(); v.cases[1].expected_standard_ticket_denial = true; fails(() => d.validateReceipt(v), 'explicit_standard_boundary_required');
});
test('bounded nofollow file reader and artifact hashing use actual bytes', async t => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'v2-protocol-files-')); t.after(() => fs.rm(dir, { recursive: true }));
  const file = path.join(dir, 'fixture'); await fs.writeFile(file, 'abc');
  assert.equal((await d.readBounded(file, 3)).toString(), 'abc');
  await assert.rejects(d.readBounded(file, 2), e => e.code === 'file_budget_or_kind');
  assert.equal(await d.hashArtifact(file), 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
  if (process.platform !== 'win32') { const link = path.join(dir, 'link'); await fs.symlink(file, link); await assert.rejects(d.readBounded(link)); }
});
test('role classification uses the first attributable user input, rejects ambiguity and does not read tool-body markers as identity', () => {
  const flow = d.scenario(c('ultra'), randomUUID());
  assert.equal(d.roleOf({ messages: [user(flow.markers.worker), tool(flow.markers.child)] }, flow), 'worker');
  fails(() => d.roleOf({ messages: [user(flow.markers.worker + flow.markers.child)] }, flow), 'ambiguous_fixture_role');
  fails(() => d.roleOf({ messages: [tool(flow.markers.worker)] }, flow), 'unknown_fixture_role');
});
test('v2 create retry reuses exact complete request and compares actual durable receipts', async () => {
  const mode = c('ultra'), flow = d.scenario(mode, randomUUID());
  flow.createRequest = { operation_id: mode.operation_id, project_id: mode.project_id, expected_state_revision: 1, workspace_path: mode.workspace_path, message: flow.assignment };
  const req = request(flow.markers.supervisor), first = await d.nextReply(req, mode, flow, {});
  const actual = { action: 'create', ...flow.createRequest, target_session_id: 'work-root', created_durable: true, linked: true, admitted: true, receipt_id: 'first-input', generation: 1 };
  const second = await d.nextReply({ ...req, messages: [...req.messages, tool(actual)] }, mode, flow, {});
  assert.equal(first.tool_calls[0].function.arguments, second.tool_calls[0].function.arguments);
  assert.equal(flow.creations.length, 1);
  for (const change of [{ admitted: false }, { linked: false }, { operation_id: randomUUID() }, { receipt_id: null }, { generation: 0 }]) fails(() => d.creationReceipt({ ...actual, ...change }, flow.createRequest), 'durable_creation_receipt_required');
});
test('compact Child and real Read forms, bounded plain report and notification are distinct from typed-report claims', async () => {
  const mode = c('ultra'), flow = d.scenario(mode, randomUUID());
  const child = await d.nextReply(request(flow.markers.worker), mode, flow, {});
  const args = JSON.parse(child.tool_calls[0].function.arguments);
  assert.deepEqual(Object.keys(args), ['message', 'role']); assert.equal(args.role, 'explorer');
  const read = await d.nextReply(request(flow.markers.child), mode, flow, {});
  assert.deepEqual(JSON.parse(read.tool_calls[0].function.arguments), { file_path: flow.fixtureFile });
  await assert.rejects(d.nextReply({ ...request(flow.markers.child), messages: [user(flow.markers.child), tool('wrong bytes')] }, mode, flow, {}), e => e.code === 'actual_fixture_read_missing');
});
test('extra post-summary provider turns fail the fixed role budget; silence is not fabricated completion', async () => {
  const mode = c('ultra'), flow = d.scenario(mode, randomUUID()); flow.steps.supervisor = 4;
  await assert.rejects(d.nextReply(request(flow.markers.supervisor), mode, flow, {}), e => e.code === 'provider_role_budget_exhausted');
  flow.steps.child = 0; await assert.rejects(d.nextReply({ ...request(flow.markers.child), tools: [] }, mode, flow, {}), e => e.code === 'required_native_tool_not_advertised');
});
test('canonical history rejects truncated tails and bounded result mismatches', () => {
  assert.deepEqual(d.messages({ messages: [] }), []);
  fails(() => d.messages({ messages: [], truncated: true }), 'bounded_canonical_messages_required');
  fails(() => d.messages({ messages: Array(129).fill({}) }), 'bounded_canonical_messages_required');
});
test('CLI missing/mismatched receipts is native UNRUN without network or credential output', async t => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'v2-protocol-cli-')); t.after(() => fs.rm(dir, { recursive: true }));
  const file = path.join(dir, 'old.json'); await fs.writeFile(file, JSON.stringify({ ...receipt(), source_sha: SECRET }));
  for (const args of [['preflight'], ['run', '--receipt', file], ['run', '--token', SECRET]]) {
    const r = spawnSync(process.execPath, [path.join(__dirname, 'supervisor-native-acceptance.cjs'), ...args], { encoding: 'utf8' });
    assert.equal(r.status, 3); assert.equal(r.stderr, ''); assert.ok(!r.stdout.includes(SECRET));
    const value = JSON.parse(r.stdout); assert.equal(value.status, 'UNRUN'); assert.equal(value.native_started, false);
  }
});

async function listen(server, port = 0) {
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', resolve); });
  return server.address().port;
}
async function mockHost(t, mode, options = {}) {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'v2-http-protocol-')); t.after(() => fs.rm(directory, { recursive: true }));
  const fixtureId = options.fixtureId || randomUUID(), modeCase = c(mode);
  modeCase.workspace_path = directory; modeCase.provider_port = 0; modeCase.expected_standard_ticket_denial = !!options.standardDenial;
  const flow = d.scenario(modeCase, fixtureId), sessions = new Map(), histories = new Map(), calls = [], errors = [];
  const sup = root('bamboo-default-supervisor', mode), ordinary = root(modeCase.ordinary_session_id, 'ultra');
  sessions.set(sup.id, sup); sessions.set(ordinary.id, ordinary); histories.set(sup.id, []); histories.set(ordinary.id, []);
  const scope = { scope: { supervisor: { session_id: sup.id }, state_revision: 1, allowed_projects: [modeCase.project_id] } };
  const provider = async (msgs, stream = false) => {
    const response = await fetch(`http://127.0.0.1:${modeCase.provider_port}/v1/chat/completions`, { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'synthetic-no-upstream', messages: msgs, tools, stream }) });
    assert.equal(response.status, 200);
    if (!stream) return (await response.json()).choices[0].message;
    const text = await response.text(), frames = text.split('\n\n').filter(s => s.startsWith('data: {')).map(s => JSON.parse(s.slice(6)));
    assert.ok(text.endsWith('data: [DONE]\n\n')); return frames[0].choices[0].delta;
  };
  async function driveWorker(work, inputReceipt) {
    const msgs = [user(flow.assignment)];
    const childCall = await provider(msgs), args = JSON.parse(childCall.tool_calls[0].function.arguments);
    assert.equal(args.role, 'explorer'); assert.ok(!Object.hasOwn(args, 'workspace'));
    const child = { ...root('fixture-child'), kind: 'child', parent_session_id: work.id, root_session_id: work.id, spawn_depth: 1,
      project_id: modeCase.project_id, workspace_path: modeCase.workspace_path, is_running: true };
    sessions.set(child.id, child);
    const childMessages = [user(args.message)], read = await provider(childMessages, true);
    const file = JSON.parse(read.tool_calls[0].function.arguments).file_path;
    const childReply = await provider([...childMessages, tool(await fs.readFile(file, 'utf8'))]);
    Object.assign(child, { is_running: false, last_run_status: 'completed' });
    histories.set(child.id, [user(args.message), childReply]);
    const inspect = await provider([...msgs, tool({ target: 'logical-child' })]);
    assert.deepEqual(JSON.parse(inspect.tool_calls[0].function.arguments), { intent: 'inspect', message: 'tree' });
    const notice = `Runtime notification: child session \`${child.id}\` finished with status \`completed\`. Remaining child sessions: 0.\n\nChild final response:\n${childReply.content}`;
    const finish = await provider([...msgs, user(notice), tool({ nodes: [child.id] })]);
    histories.set(work.id, [{ id: inputReceipt, role: 'user', content: flow.assignment, metadata: { session_message: { id: inputReceipt, kind: 'peer_message' } } }, finish]);
    Object.assign(work, { is_running: false, last_run_status: 'completed' });
  }
  async function driveSupervisor() {
    const msgs = [user(flow.markers.supervisor)], one = await provider(msgs);
    const req = JSON.parse(one.tool_calls[0].function.arguments);
    const work = { ...root('fixture-work'), project_id: modeCase.project_id, workspace_path: modeCase.workspace_path, is_running: true };
    sessions.set(work.id, work);
    if (options.extraRoot) sessions.set('unexpected-root', root('unexpected-root'));
    const creation = { action: 'create', operation_id: req.operation_id, project_id: modeCase.project_id, target_session_id: work.id,
      created_durable: true, linked: true, admitted: true, receipt_id: 'first-input', generation: 1, replayed: false };
    const worker = driveWorker(work, creation.receipt_id);
    const two = await provider([...msgs, tool(creation)]);
    assert.equal(two.tool_calls[0].function.arguments, one.tool_calls[0].function.arguments);
    const replay = { ...creation, replayed: true };
    const threePromise = provider([...msgs, tool(replay)]); await worker;
    const three = await threePromise; assert.equal(JSON.parse(three.tool_calls[0].function.arguments).session_id, work.id);
    const final = await provider([...msgs, tool({ messages: histories.get(work.id) })]);
    histories.set(sup.id, [user(flow.markers.supervisor), final]); Object.assign(sup, { is_running: false, last_run_status: 'completed' });
  }
  async function driveOrdinary() {
    const msgs = [user(flow.markers.ordinary)], first = await provider(msgs);
    assert.equal(first.tool_calls[0].function.name, 'session_control');
    await provider([...msgs, tool('session_control is not allowed in root orchestration-only mode')]);
    Object.assign(ordinary, { is_running: false, last_run_status: 'completed' });
  }
  const server = http.createServer(async (req, res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks)) : undefined; calls.push({ method: req.method, url: req.url, body });
    const send = (status, value) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(value)); };
    if (req.url === '/api/v1/bootstrap') return send(200, { server: { product: 'bamboo' }, api: { name: 'bamboo.agent' }, capabilities: ['http.native_receipt_run.v1'] });
    if (req.url === '/api/v1/sessions') return send(200, { sessions: [...sessions.values()] });
    if (req.url === '/api/v1/supervisor/default/scope') return send(200, scope);
    if (req.url === '/api/v1/chat') {
      if (modeCase.expected_standard_ticket_denial) return send(403, { code: 'scope_denied', error: { message: options.wrongDenial ? 'unrelated access error' : 'Supervisor orchestration authority required' } });
      sessions.get(body.session_id).message_count++; return send(201, { session_id: body.session_id });
    }
    const match = /^\/api\/v1\/sessions\/([^/]+)(\/history|\/execute)?$/.exec(req.url);
    if (match && sessions.has(match[1])) {
      const s = sessions.get(match[1]);
      if (match[2] === '/history') return send(200, { messages: histories.get(s.id), truncated: false });
      if (match[2] === '/execute') {
        s.is_running = true; send(202, { status: 'started', session_id: s.id, run_id: 'synthetic-run' });
        (s.id === sup.id ? driveSupervisor() : driveOrdinary()).catch(e => { errors.push(e); Object.assign(s, { is_running: false, last_run_status: 'failed' }); }); return;
      }
      return send(200, { session: s });
    }
    send(404, {});
  });
  modeCase.api_base = 'http://127.0.0.1:' + await listen(server);
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  return { case: modeCase, fixtureId, calls, errors, sessions };
}
test('FRAMEWORK HTTP: both modes create one Root, replay one input, share Child workspace, observe notification and deny ordinary control', { timeout: 10000 }, async t => {
  const id = randomUUID(), a = await mockHost(t, 'standard', { fixtureId: id }), b = await mockHost(t, 'ultra', { fixtureId: id });
  const result = await d.runSuite({ fixture_id: id, cases: [a.case, b.case] });
  assert.equal(result.status, 'PASS'); assert.deepEqual(result.cases.map(c => c.provider_requests), [11, 11]);
  assert.equal(result.evidence_kind, undefined, 'internal synthetic transport must never certify a native binary');
  assert.deepEqual(a.errors, []); assert.deepEqual(b.errors, []);
  for (const f of [a, b]) {
    assert.equal([...f.sessions.values()].filter(s => s.kind === 'root').length, 3);
    assert.ok(f.calls.every(c => c.method === 'GET' || c.url === '/api/v1/chat' || c.url.endsWith('/execute')));
  }
});
test('FRAMEWORK HTTP: real-shaped Standard Ticket 403 remains BLOCKED/PARTIAL with zero provider calls and no mutation retry', { timeout: 10000 }, async t => {
  const id = randomUUID(), a = await mockHost(t, 'standard', { fixtureId: id, standardDenial: true }), b = await mockHost(t, 'ultra', { fixtureId: id });
  const result = await d.runSuite({ fixture_id: id, cases: [a.case, b.case] });
  assert.equal(result.status, 'PARTIAL'); assert.equal(result.cases[0].status, 'BLOCKED_EXISTING_STANDARD_TICKET_GATE');
  assert.equal(result.cases[0].provider_requests, 0); assert.equal(result.cases[0].v2_positive, 'UNRUN');
  assert.equal(a.calls.filter(c => c.method === 'POST').length, 1);
});
test('FRAMEWORK HTTP: an unrelated 403 cannot masquerade as the known Standard boundary', { timeout: 5000 }, async t => {
  const a = await mockHost(t, 'standard', { standardDenial: true, wrongDenial: true });
  await assert.rejects(d.runSuite({ fixture_id: a.fixtureId, cases: [a.case] }), e => e.code === 'unexpected_standard_denial');
});

test('composition validation rejects Child-as-Root, workspace/permission drift, duplicated input and missing notification', () => {
  const mode = c('ultra'), flow = d.scenario(mode, randomUUID());
  const work = { ...root('work'), project_id: mode.project_id, workspace_path: mode.workspace_path };
  const child = { ...root('child'), kind: 'child', root_session_id: work.id, parent_session_id: work.id, spawn_depth: 1,
    project_id: mode.project_id, workspace_path: mode.workspace_path };
  const creation = { target_session_id: work.id, receipt_id: 'input', generation: 1 };
  Object.assign(flow, { creations: [creation, { ...creation, replayed: true }], noticeChild: child.id, noticeCount: 1, readObserved: true, historyObserved: true });
  const input = { role: 'user', content: flow.assignment, metadata: { session_message: { id: 'input', kind: 'peer_message' } } };
  const history = [input, { role: 'assistant', content: flow.workerReport }], summary = [{ role: 'assistant', content: flow.supervisorReport }];
  d.validateComposition(mode, work, child, history, summary, flow);
  fails(() => d.validateComposition(mode, { ...work, kind: 'child' }, child, history, summary, flow), 'independent_root_contract_failed');
  fails(() => d.validateComposition(mode, work, { ...child, workspace_path: path.dirname(mode.workspace_path) }, history, summary, flow), 'shared_workspace_child_contract_failed');
  fails(() => d.validateComposition(mode, { ...work, bypass_permissions: true }, child, history, summary, flow), 'independent_root_contract_failed');
  fails(() => d.validateComposition(mode, work, child, [input, ...history], summary, flow), 'first_instruction_not_exactly_once');
  fails(() => d.validateComposition(mode, work, child, history, summary, { ...flow, noticeCount: 2 }), 'exact_child_completion_notice_required');
  fails(() => d.validateComposition(mode, work, child, history, summary, { ...flow, historyObserved: false }), 'actual_read_and_history_results_required');
});
test('parent provider notification must be actual runtime-shaped, exactly one, and contain the returned Child report', async () => {
  const mode = c('ultra');
  for (const variant of ['missing', 'duplicate', 'wrong-report']) {
    const flow = d.scenario(mode, randomUUID()); flow.steps.worker = 2;
    const notice = user('Runtime notification: child session `child` finished with status `completed`.\n' + (variant === 'wrong-report' ? 'wrong' : flow.childReport));
    const notices = variant === 'missing' ? [] : variant === 'duplicate' ? [notice, notice] : [notice];
    await assert.rejects(d.nextReply({ messages: [user(flow.markers.worker), ...notices], tools }, mode, flow, {}), e => e.code === 'parent_provider_notification_missing_or_duplicate');
  }
});
test('FRAMEWORK HTTP: private direct transport refuses redirects, invalid/oversize JSON and never uses global fetch', async t => {
  const server = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'application/json');
    if (req.url.endsWith('/redirect')) { res.writeHead(302, { Location: 'http://example.com/' }); res.end('{}'); }
    else if (req.url.endsWith('/huge')) res.end('"' + 'x'.repeat(1024 * 1024) + '"');
    else if (req.url.endsWith('/invalid')) res.end('not JSON');
    else { assert.equal(req.headers.authorization, undefined); assert.equal(req.headers.cookie, undefined); res.end('{"ok":true}'); }
  });
  const base = 'http://127.0.0.1:' + await listen(server);
  t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const original = globalThis.fetch; globalThis.fetch = () => { throw new Error('global fetch must not execute'); };
  try {
    assert.deepEqual((await d.jsonRequest(base, '/api/v1/direct', undefined, AbortSignal.timeout(2000))).body, { ok: true });
    for (const [route, code] of [['redirect', 'http_redirect_refused'], ['huge', 'http_response_budget'], ['invalid', 'invalid_http_json']]) {
      await assert.rejects(d.jsonRequest(base, '/api/v1/' + route, undefined, AbortSignal.timeout(2000)), e => e.code === code);
    }
  } finally { globalThis.fetch = original; }
});
test('FRAMEWORK HTTP: fake provider refuses credentials and unknown routes without any upstream forwarding', async t => {
  const mode = c('ultra'); mode.provider_port = 0;
  const flow = d.scenario(mode, randomUUID()), controller = new AbortController();
  const close = await d.startProvider(mode, flow, {}, controller.signal); t.after(close);
  const base = 'http://127.0.0.1:' + mode.provider_port;
  for (const [route, headers] of [['/v1/chat/completions', { Authorization: 'Bearer ' + SECRET }], ['/wrong', {}]]) {
    const r = await fetch(base + route, { method: 'POST', headers, body: '{}' }); const text = await r.text();
    assert.equal(r.status, 422); assert.ok(!text.includes(SECRET));
  }
  assert.equal(flow.requests, 0); assert.deepEqual(flow.errors, ['provider_route_or_credentials_refused', 'provider_route_or_credentials_refused']);
});
test('FRAMEWORK HTTP: an extra created Root cannot hide behind equal returned retry receipts', { timeout: 5000 }, async t => {
  const f = await mockHost(t, 'ultra', { extraRoot: true });
  await assert.rejects(d.runSuite({ fixture_id: f.fixtureId, cases: [f.case] }), e => e.code === 'unexpected_extra_sessions');
});
test('artifact preflight verifies canonical temp paths and actual bytes while remaining native UNRUN', async t => {
  const base = await fs.realpath(os.tmpdir()), directory = await fs.mkdtemp(path.join(base, 'bamboo-v2-native-fixture-'));
  t.after(() => fs.rm(directory, { recursive: true }));
  const v = receipt(); v.fixture_root = directory;
  for (const mode of v.cases) {
    mode.data_dir = path.join(directory, 'data-' + mode.mode); mode.workspace_path = path.join(directory, 'work-' + mode.mode);
    await fs.mkdir(mode.data_dir); await fs.mkdir(mode.workspace_path);
  }
  v.binary.path = path.join(directory, 'synthetic-Mach-O-bytes-not-a-Bamboo-host');
  await fs.writeFile(v.binary.path, Buffer.concat([Buffer.from('cffaedfe', 'hex'), Buffer.from('synthetic component fixture only')]));
  v.binary.sha256 = await d.hashArtifact(v.binary.path);
  const file = path.join(directory, 'receipt.json'); await fs.writeFile(file, JSON.stringify(v));
  const result = await d.verifyReceipt(file); assert.equal(result.config.source_sha, d.SOURCE);
  const cli = spawnSync(process.execPath, [path.join(__dirname, 'supervisor-native-acceptance.cjs'), 'preflight', '--receipt', file], { encoding: 'utf8' });
  assert.equal(cli.status, 0); assert.equal(JSON.parse(cli.stdout).status, 'ARTIFACT_BYTES_MATCH_RECEIPT_NATIVE_UNRUN');
  await fs.appendFile(v.binary.path, 'changed');
  await assert.rejects(d.verifyReceipt(file), e => e.code === 'host_binary_hash_mismatch');
  await fs.writeFile(v.binary.path, '#!/bin/sh\nnot-a-native-host');
  await assert.rejects(d.verifyReceipt(file), e => e.code === 'native_host_artifact_required');
});
test('OS COMPONENT ONLY: bind the test process/listener and reject changed process identity before a write', async t => {
  if (!['darwin', 'linux'].includes(process.platform)) {
    await assert.rejects(d.verifyProcess({ cases: [] }), e => e.code === 'native_process_binding_unsupported'); return;
  }
  const server = http.createServer((req, res) => res.end('{}'));
  const port = await listen(server); t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
  const started = spawnSync('ps', ['-p', String(process.pid), '-o', 'lstart='], { encoding: 'utf8' });
  assert.equal(started.status, 0);
  const config = { binary: { path: process.execPath }, cases: [{ host_pid: process.pid, host_started_at: started.stdout.trim(), api_base: 'http://127.0.0.1:' + port }] };
  await d.verifyProcess(config);
  await assert.rejects(d.verifyProcess({ ...config, cases: [{ ...config.cases[0], host_started_at: 'old process' }] }), e => e.code === 'host_process_changed');
});
