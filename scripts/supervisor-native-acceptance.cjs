#!/usr/bin/env node
'use strict';

// Dependency-free, fixture-only acceptance driver. Native execution requires
// a producer receipt for the exact audited Host; --preflight never sends HTTP.
// Framework self-tests are not native Bamboo acceptance.

const fs = require('node:fs/promises');
const { constants } = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const http = require('node:http');
const net = require('node:net');
const { createHash } = require('node:crypto');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const exec = promisify(execFile);

const SOURCE = '3682cf35171316d3b281ea49b90dd722a32488cb';
const TREE = '5e8cd3c84fa6fb2dae42868ebcaa5bf181319cf9';
const LIMIT = 1024 * 1024;
const SUPERVISOR = 'bamboo-default-supervisor';
const HELP = `Fixture-only v2 Supervisor acceptance (Node 22+, no dependencies)
  node scripts/supervisor-native-acceptance.cjs preflight [--receipt FILE]
  node scripts/supervisor-native-acceptance.cjs run --receipt FILE
  node --test scripts/supervisor-native-acceptance.test.cjs

preflight is local/read-only; missing or mismatched artifacts are UNRUN (exit 3).
run uses two ALREADY PREPARED, producer-owned, isolated loopback Hosts. It starts
only its own scripted provider; it never launches/builds/stops Bamboo, creates a
Project/grant, changes mode/settings, logs in, or automatically approves a tool.
Normal typed fixture approval, if needed, must be completed by its owner while
the driver waits. Standard Ticket refusal is reported BLOCKED, not success.

Receipt schema bamboo.v2-native-fixture.v1:
  source_sha, tree_sha: exact audited Host build above (not version 0.0.0)
  binary: {path, sha256}; an exported artifact, not target/intermediate output
  fixture_root: canonical OS temp directory named bamboo-v2-native-fixture-<id>
  fixture_id: UUID; injected_provider: no_auth_local_scripted_no_fallback
  cases: exactly standard and ultra, each with api_base (literal loopback HTTP),
    host_pid, host_started_at (ps lstart), data_dir, workspace_path, project_id,
    supervisor_created_at, ordinary_session_id, ordinary_created_at,
    provider_port, operation_id, expected_standard_ticket_denial (boolean).
Each Host must already use its case's provider_port at /v1/chat/completions,
have a fresh v2 Supervisor and ordinary Ultra Root, an explicit existing bound
Project/workspace and its normal permissions. No auth/cookie/proof is accepted.
The receipt is operator/build provenance, not authentication or a model grant.
Use an independently audited producer handoff. A matching file hash alone does
not prove how it was compiled; never edit an old build's source fields to fit.
Native results require the actual canonical receipts, histories and roles.
No raw prompts, backend errors, paths or model request bodies are printed.`;

class Failure extends Error {
  constructor(code, extra = {}) { super(code); this.code = code; Object.assign(this, extra); }
}
const requireFact = (condition, code) => { if (!condition) throw new Failure(code); };
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const uuid = value => typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
const identity = value => typeof value === 'string' && /^[A-Za-z0-9_-][A-Za-z0-9_.:-]{0,127}$/.test(value) && !value.includes('..');
function origin(value) {
  let u; try { u = new URL(value); } catch { throw new Failure('literal_loopback_required'); }
  requireFact(u.protocol === 'http:' && ['127.0.0.1', '[::1]'].includes(u.hostname)
    && !u.username && !u.password && !u.search && !u.hash && u.pathname === '/'
    && Number(u.port) >= 1024, 'literal_loopback_required');
  return u.origin;
}
function inside(root, file) {
  const relative = path.relative(root, file);
  return relative !== '' && relative !== '..' && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
}
function validateReceipt(value) {
  requireFact(value?.schema === 'bamboo.v2-native-fixture.v1', 'fixture_receipt_schema_required');
  requireFact(Object.keys(value).every(k => ['schema', 'source_sha', 'tree_sha', 'fixture_id', 'fixture_root', 'injected_provider', 'binary', 'cases'].includes(k))
    && Object.keys(value.binary || {}).every(k => ['path', 'sha256'].includes(k)), 'unknown_receipt_fields');
  requireFact(value.source_sha === SOURCE && value.tree_sha === TREE, 'host_source_mismatch');
  requireFact(uuid(value.fixture_id), 'fixture_identity_required');
  requireFact(value.injected_provider === 'no_auth_local_scripted_no_fallback', 'injected_local_provider_required');
  requireFact(path.isAbsolute(value.fixture_root || '') && /^bamboo-v2-native-fixture-[a-zA-Z0-9_-]+$/.test(path.basename(value.fixture_root)), 'isolated_fixture_required');
  requireFact(path.isAbsolute(value.binary?.path || '') && /^[0-9a-f]{64}$/.test(value.binary?.sha256 || ''), 'host_artifact_required');
  requireFact(!value.binary.path.split(path.sep).some(part => ['target', 'intermediate'].includes(part)), 'exported_host_artifact_required');
  requireFact(Array.isArray(value.cases) && value.cases.length === 2, 'two_mode_cases_required');
  const bases = new Set(), ports = new Set(), modes = new Set();
  for (const c of value.cases) {
    requireFact(Object.keys(c).every(k => ['mode', 'api_base', 'host_pid', 'host_started_at', 'data_dir', 'workspace_path', 'project_id',
      'supervisor_created_at', 'ordinary_session_id', 'ordinary_created_at', 'provider_port', 'operation_id', 'expected_standard_ticket_denial'].includes(k)), 'unknown_case_fields');
    requireFact(['standard', 'ultra'].includes(c.mode) && !modes.has(c.mode), 'unique_modes_required'); modes.add(c.mode);
    const base = origin(c.api_base); requireFact(!bases.has(base), 'separate_mode_hosts_required'); bases.add(base);
    requireFact(Number.isSafeInteger(c.provider_port) && c.provider_port >= 1024 && c.provider_port <= 65535
      && !ports.has(c.provider_port) && Number(new URL(base).port) !== c.provider_port, 'dedicated_provider_port_required'); ports.add(c.provider_port);
    requireFact(Number.isSafeInteger(c.host_pid) && c.host_pid > 1 && typeof c.host_started_at === 'string' && c.host_started_at.length < 80, 'host_process_identity_required');
    requireFact(identity(c.project_id) && identity(c.ordinary_session_id) && c.ordinary_session_id !== SUPERVISOR
      && uuid(c.operation_id), 'case_identity_required');
    requireFact(typeof c.supervisor_created_at === 'string' && typeof c.ordinary_created_at === 'string', 'session_birth_required');
    requireFact(path.isAbsolute(c.data_dir || '') && path.isAbsolute(c.workspace_path || '')
      && inside(value.fixture_root, c.data_dir) && inside(value.fixture_root, c.workspace_path), 'fixture_paths_required');
    requireFact(typeof c.expected_standard_ticket_denial === 'boolean'
      && (!c.expected_standard_ticket_denial || c.mode === 'standard'), 'explicit_standard_boundary_required');
  }
  return value;
}
async function readBounded(file, limit = LIMIT) {
  const handle = await fs.open(file, constants.O_RDONLY | (constants.O_NOFOLLOW || 0));
  try {
    const stat = await handle.stat(); requireFact(stat.isFile() && stat.size <= limit, 'file_budget_or_kind');
    const bytes = Buffer.alloc(limit + 1); let size = 0;
    while (size <= limit) { const r = await handle.read(bytes, size, bytes.length - size, null); if (!r.bytesRead) break; size += r.bytesRead; }
    requireFact(size <= limit, 'file_budget_or_kind'); return bytes.subarray(0, size);
  } finally { await handle.close(); }
}
async function hashArtifact(file, requireNativeMagic = false) {
  const handle = await fs.open(file, constants.O_RDONLY | (constants.O_NOFOLLOW || 0));
  try {
    const stat = await handle.stat(); requireFact(stat.isFile() && stat.size > 0 && stat.size <= 2 * 1024 ** 3, 'host_artifact_size_or_kind');
    const hash = createHash('sha256'), bytes = Buffer.alloc(1024 * 1024); let total = 0;
    for (;;) { const { bytesRead } = await handle.read(bytes, 0, bytes.length, null); if (!bytesRead) break;
      if (total === 0 && requireNativeMagic) requireFact(['cffaedfe', 'feedfacf', 'cafebabe', 'bebafeca', 'cefaedfe', 'feedface', '7f454c46'].includes(bytes.subarray(0, 4).toString('hex'))
        || bytes.subarray(0, 2).toString('hex') === '4d5a', 'native_host_artifact_required');
      total += bytesRead; requireFact(total <= stat.size, 'host_artifact_changed'); hash.update(bytes.subarray(0, bytesRead)); }
    const after = await handle.stat(); requireFact(total === stat.size && after.size === stat.size && after.mtimeMs === stat.mtimeMs, 'host_artifact_changed');
    return hash.digest('hex');
  } finally { await handle.close(); }
}
async function verifyReceipt(file) {
  const bytes = await readBounded(file, 64 * 1024); let config;
  try { config = validateReceipt(JSON.parse(bytes)); } catch (e) { if (e instanceof Failure) throw e; throw new Failure('invalid_fixture_receipt'); }
  const temp = await fs.realpath(os.tmpdir()), root = await fs.realpath(config.fixture_root);
  requireFact(inside(temp, root) && root === config.fixture_root, 'canonical_temp_fixture_required');
  for (const c of config.cases) for (const p of [c.data_dir, c.workspace_path]) {
    requireFact(await fs.realpath(p) === p && inside(root, p), 'canonical_fixture_paths_required');
  }
  requireFact(await hashArtifact(config.binary.path, true) === config.binary.sha256, 'host_binary_hash_mismatch');
  return { config, receipt_sha256: sha(bytes) };
}
async function verifyProcess(config) {
  requireFact(['darwin', 'linux'].includes(process.platform), 'native_process_binding_unsupported');
  const binary = await fs.realpath(config.binary.path);
  for (const c of config.cases) {
    const started = (await exec('ps', ['-p', String(c.host_pid), '-o', 'lstart='], { timeout: 2000, maxBuffer: 4096 })).stdout.trim();
    const command = (await exec('ps', ['-p', String(c.host_pid), '-o', 'comm='], { timeout: 2000, maxBuffer: 4096 })).stdout.trim();
    requireFact(started === c.host_started_at && await fs.realpath(command) === binary, 'host_process_changed');
    const executable = process.platform === 'darwin' ? '/usr/sbin/lsof' : 'lsof';
    const listen = (await exec(executable, ['-nP', '-a', '-p', String(c.host_pid), '-iTCP:' + new URL(c.api_base).port, '-sTCP:LISTEN', '-Fp'], { timeout: 2000, maxBuffer: 4096 })).stdout;
    requireFact(listen.split('\n').includes('p' + c.host_pid), 'host_listener_unverified');
  }
}
async function jsonRequest(base, route, body, signal) {
  requireFact(route.startsWith('/api/v1/') && !route.includes('..') && !route.includes('?'), 'invalid_api_route');
  const url = new URL(origin(base)), data = body === undefined ? undefined : Buffer.from(JSON.stringify(body));
  requireFact(data === undefined || data.length <= LIMIT, 'http_request_budget');
  // A private direct agent avoids global fetch dispatchers and environment
  // proxies. No credentials or cookies are taken from config/environment.
  const agent = new http.Agent({ keepAlive: false });
  agent.createConnection = () => net.createConnection({ host: url.hostname.replace(/^\[|\]$/g, ''), port: Number(url.port) });
  try {
    return await new Promise((resolve, reject) => {
      const req = http.request({ hostname: url.hostname, port: url.port, path: route, method: data ? 'POST' : 'GET', agent, signal,
        headers: data ? { 'Content-Type': 'application/json', 'Content-Length': data.length } : {} }, res => {
        const chunks = []; let size = 0;
        res.on('data', chunk => { size += chunk.length; if (size > LIMIT) res.destroy(new Failure('http_response_budget')); else chunks.push(chunk); });
        res.on('error', reject);
        res.on('end', () => { try { requireFact(res.statusCode < 300 || res.statusCode >= 400, 'http_redirect_refused');
          resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks)) });
        } catch (e) { reject(e instanceof Failure ? e : new Failure('invalid_http_json')); } });
      });
      req.on('error', reject); req.end(data);
    });
  } finally { agent.destroy(); }
}
function session(value, expectedId, expectedBirth) {
  const s = value?.session;
  requireFact(s?.id === expectedId && s.created_at === expectedBirth && s.kind === 'root'
    && s.parent_session_id === null && s.root_session_id === expectedId && s.spawn_depth === 0, 'canonical_root_identity_mismatch');
  requireFact(s.permission_mode === 'default' && s.bypass_permissions === false, 'ordinary_permissions_required');
  return s;
}
function stable(value) { return JSON.stringify(value); }
function creationReceipt(value, request) {
  requireFact(value?.action === 'create' && value.operation_id === request.operation_id && value.project_id === request.project_id
    && value.created_durable === true && value.linked === true && value.admitted === true
    && identity(value.target_session_id) && typeof value.receipt_id === 'string'
    && Number.isSafeInteger(value.generation) && value.generation > 0, 'durable_creation_receipt_required');
  return value;
}
function parseToolContent(content) {
  if (typeof content !== 'string') throw new Failure('tool_result_text_required');
  try { const value = JSON.parse(content); if (typeof value.content === 'string') return JSON.parse(value.content); return value; }
  catch { throw new Failure('structured_tool_receipt_required'); }
}
function latestTool(request) {
  const message = [...request.messages].reverse().find(m => m.role === 'tool');
  requireFact(message, 'expected_tool_result'); return parseToolContent(message.content);
}
function textContent(content) {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) return content.map(p => typeof p.text === 'string' ? p.text : '').join('\n');
  return '';
}
function toolName(request, name) {
  const tools = request.tools || [];
  const match = tools.find(t => t.type === 'function' && (t.function?.name === name || t.function?.name.endsWith('::' + name)));
  requireFact(match, 'required_native_tool_not_advertised'); return match.function.name;
}
function messages(value) {
  requireFact(Array.isArray(value?.messages) && value.messages.length <= 128
    && !value.truncated && !value.has_more && !value.next_cursor, 'bounded_canonical_messages_required'); return value.messages;
}
function validateComposition(c, root, child, rootMessages, supervisorMessages, flow) {
  requireFact(root.id !== SUPERVISOR && root.id !== c.ordinary_session_id && root.kind === 'root' && root.parent_session_id === null && root.root_session_id === root.id
    && root.spawn_depth === 0 && root.project_id === c.project_id && root.workspace_path === c.workspace_path
    && root.thinking_mode === 'standard' && root.permission_mode === 'default' && !root.bypass_permissions, 'independent_root_contract_failed');
  requireFact(child.kind === 'child' && child.parent_session_id === root.id && child.root_session_id === root.id
    && child.spawn_depth === 1 && child.project_id === c.project_id && child.workspace_path === root.workspace_path
    && child.permission_mode === 'default' && !child.bypass_permissions, 'shared_workspace_child_contract_failed');
  requireFact(flow.creations.length === 2 && flow.creations[0].target_session_id === root.id
    && flow.creations[1].target_session_id === root.id && flow.creations[1].replayed === true
    && flow.creations[0].receipt_id === flow.creations[1].receipt_id
    && flow.creations[0].generation === flow.creations[1].generation, 'root_retry_not_deduplicated');
  const inputs = rootMessages.filter(m => m.role === 'user' && m.metadata?.session_message?.kind === 'peer_message'
    && m.content.includes(flow.assignment));
  requireFact(inputs.length === 1 && inputs[0].metadata.session_message.id === flow.creations[0].receipt_id,
    'first_instruction_not_exactly_once');
  // The HTTP UI history intentionally hides runtime notifications. Observe the
  // actual parent provider request instead of inventing a history endpoint.
  requireFact(flow.noticeChild === child.id && flow.noticeCount === 1, 'exact_child_completion_notice_required');
  requireFact(rootMessages.some(m => m.role === 'assistant' && m.content === flow.workerReport)
    && supervisorMessages.some(m => m.role === 'assistant' && m.content === flow.supervisorReport), 'canonical_reports_missing');
  requireFact(flow.readObserved && flow.historyObserved, 'actual_read_and_history_results_required');
}

function scenario(c, fixtureId) {
  const tag = name => `BAMBOO_V2_FIXTURE_${name}:${fixtureId}:${c.mode}`;
  return { markers: { supervisor: tag('SUPERVISOR'), worker: tag('WORKER'), child: tag('CHILD'), ordinary: tag('ORDINARY') },
    assignment: tag('WORKER') + '\nDelegate the single synthetic fixture read and report its bytes.',
    fixtureFile: path.join(c.workspace_path, 'bamboo-v2-native-fixture.txt'),
    fileContent: tag('FILE') + '\n', childReport: tag('CHILD_REPORT'), workerReport: tag('WORKER_REPORT'),
    supervisorReport: tag('SUPERVISOR_REPORT'), creations: [], steps: { supervisor: 0, worker: 0, child: 0, ordinary: 0 },
    readObserved: false, historyObserved: false, noticeCount: 0, errors: [], requests: 0 };
}
function roleOf(request, flow) {
  requireFact(Array.isArray(request.messages) && request.messages.length <= 128, 'provider_messages_budget');
  for (const m of request.messages.filter(m => m.role === 'user')) {
    const text = textContent(m.content), roles = Object.keys(flow.markers).filter(k => text.includes(flow.markers[k]));
    requireFact(roles.length <= 1, 'ambiguous_fixture_role');
    if (roles.length) return roles[0];
  }
  throw new Failure('unknown_fixture_role');
}
function functionReply(request, name, args, number) {
  return { role: 'assistant', content: null, tool_calls: [{ id: 'fixture-call-' + number, type: 'function',
    function: { name: toolName(request, name), arguments: JSON.stringify(args) } }] };
}
function requestNotice(request, flow) {
  const notices = request.messages.filter(m => m.role === 'user').map(m => textContent(m.content))
    .filter(s => s.startsWith('Runtime notification: child session `') && s.includes(flow.childReport));
  requireFact(notices.length === 1, 'parent_provider_notification_missing_or_duplicate');
  const match = /^Runtime notification: child session `([^`]+)` finished with status `completed`\./.exec(notices[0]);
  requireFact(match && identity(match[1]), 'parent_provider_notification_invalid');
  flow.noticeChild = match[1]; flow.noticeCount = notices.length;
}
async function nextReply(request, c, flow, hooks) {
  requireFact(request.tool_choice === undefined || request.tool_choice === 'auto', 'unsupported_forced_tool_choice');
  const role = roleOf(request, flow), step = flow.steps[role]++;
  const caps = { supervisor: 4, worker: 3, child: 2, ordinary: 2 };
  requireFact(step < caps[role], 'provider_role_budget_exhausted');
  flow.requests++;
  requireFact(flow.requests <= 11, 'provider_request_budget_exhausted');
  const call = (name, args) => functionReply(request, name, args, role + '-' + step);
  if (role === 'supervisor') {
    if (step === 0 || step === 1) {
      if (step === 1) flow.creations.push(creationReceipt(latestTool(request), flow.createRequest));
      return call('spawn_session', flow.createRequest);
    }
    if (step === 2) {
      flow.creations.push(creationReceipt(latestTool(request), flow.createRequest));
      requireFact(flow.creations[0].target_session_id === flow.creations[1].target_session_id, 'duplicate_root_created');
      flow.rootId = flow.creations[0].target_session_id;
      await hooks.waitRoot(flow.rootId);
      return call('session_history', { action: 'read_messages', session_id: flow.rootId, from_end: true,
        limit: 20, truncate_chars: 4096, include_tool: true, include_system: false });
    }
    requireFact(textContent(request.messages.findLast(m => m.role === 'tool')?.content).includes(flow.workerReport), 'root_history_readback_missing');
    flow.historyObserved = true; return { role: 'assistant', content: flow.supervisorReport };
  }
  if (role === 'worker') {
    requireFact(!request.messages.some(m => textContent(m.content).includes(flow.markers.supervisor)), 'supervisor_context_leaked');
    if (step === 0) return call('SubAgent', { message: flow.markers.child + '\nRead only ' + flow.fixtureFile + ' and report its bytes.', role: 'explorer' });
    if (step === 1) { await hooks.waitChild(flow.rootId); return call('SubAgent', { intent: 'inspect', message: 'tree' }); }
    requestNotice(request, flow); return { role: 'assistant', content: flow.workerReport };
  }
  if (role === 'child') {
    if (step === 0) return call('Read', { file_path: flow.fixtureFile });
    requireFact(textContent(request.messages.findLast(m => m.role === 'tool')?.content).includes(flow.fileContent.trim()), 'actual_fixture_read_missing');
    flow.readObserved = true; return { role: 'assistant', content: flow.childReport };
  }
  if (step === 0) {
    // Intentionally unadvertised on an ordinary Ultra Root: test the final
    // authority gate rather than treating absence from the catalog as proof.
    return { role: 'assistant', content: null, tool_calls: [{ id: 'ordinary-denial', type: 'function',
      function: { name: 'session_control', arguments: '{"action":"scope"}' } }] };
  }
  const text = textContent(request.messages.findLast(m => m.role === 'tool')?.content);
  requireFact(/(not allowed|not available|unavailable|requires|denied|unknown tool)/i.test(text)
    && /(session_control|Supervisor|orchestration)/i.test(text), 'ordinary_management_not_denied');
  flow.ordinaryDenied = true; return { role: 'assistant', content: 'Fixture management denial observed.' };
}
async function startProvider(c, flow, hooks, signal) {
  let attempts = 0;
  const server = http.createServer(async (req, res) => {
    const reply = (status, value) => { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(value)); };
    try {
      requireFact(++attempts <= 16, 'provider_http_attempt_budget');
      requireFact(req.method === 'POST' && req.url === '/v1/chat/completions' && !req.headers.authorization
        && !req.headers.cookie && !signal.aborted, 'provider_route_or_credentials_refused');
      let size = 0; const chunks = [];
      for await (const chunk of req) { size += chunk.length; requireFact(size <= LIMIT, 'provider_request_size'); chunks.push(chunk); }
      let request; try { request = JSON.parse(Buffer.concat(chunks)); } catch { throw new Failure('provider_invalid_json'); }
      const message = await nextReply(request, c, flow, hooks);
      const finish = message.tool_calls ? 'tool_calls' : 'stop';
      if (request.stream === true) {
        res.writeHead(200, { 'Content-Type': 'text/event-stream' });
        const delta = message.tool_calls ? { role: 'assistant', tool_calls: message.tool_calls.map((v, index) => ({ index, ...v })) } : message;
        res.end(`data: ${JSON.stringify({ id: 'synthetic-v2', choices: [{ index: 0, delta, finish_reason: null }] })}\n\ndata: ${JSON.stringify({ id: 'synthetic-v2', choices: [{ index: 0, delta: {}, finish_reason: finish }], usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 } })}\n\ndata: [DONE]\n\n`);
      } else reply(200, { id: 'synthetic-v2', object: 'chat.completion', choices: [{ index: 0, message, finish_reason: finish }], usage: { prompt_tokens: 1, completion_tokens: 1, total_tokens: 2 } });
    } catch (e) { const code = e instanceof Failure ? e.code : 'provider_request_failed'; if (flow.errors.length < 16) flow.errors.push(code); if (!res.headersSent) reply(422, { error: { message: code, type: 'fixture_error' } }); else res.destroy(); }
  });
  server.maxConnections = 4; server.maxRequestsPerSocket = 16;
  server.requestTimeout = 5000; server.headersTimeout = 5000;
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(c.provider_port, '127.0.0.1', resolve); });
  c.provider_port = server.address().port;
  return async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); };
}
async function runCase(c, fixtureId, request, signal) {
  const api = async (route, body) => request(c.api_base, '/api/v1' + route, body, signal);
  const get = async route => { const r = await api(route); requireFact(r.status === 200, 'canonical_read_failed'); return r.body; };
  const detail = async id => (await get('/sessions/' + encodeURIComponent(id))).session;
  const list = async () => { const v = await get('/sessions'); requireFact(Array.isArray(v.sessions) && v.sessions.length <= 16 && !v.has_more, 'fresh_fixture_session_list_required'); return v.sessions; };
  const waitFor = async check => {
    while (!signal.aborted) { const value = await check(); if (value) return value; await new Promise(resolve => setTimeout(resolve, 100)); }
    throw new Failure('fixture_deadline');
  };
  const settled = s => s && !s.is_running && ['completed', 'error', 'failed', 'cancelled'].includes(s.last_run_status);
  const bootstrap = await get('/bootstrap');
  requireFact(bootstrap.server?.product === 'bamboo' && bootstrap.api?.name === 'bamboo.agent'
    && bootstrap.capabilities?.includes('http.native_receipt_run.v1'), 'native_receipt_capability_required');
  const initialList = await list(), sup = session(await get('/sessions/' + SUPERVISOR), SUPERVISOR, c.supervisor_created_at);
  const ordinary = session(await get('/sessions/' + c.ordinary_session_id), c.ordinary_session_id, c.ordinary_created_at);
  requireFact(sup.thinking_mode === c.mode && ordinary.thinking_mode === 'ultra'
    && !sup.is_running && !ordinary.is_running && sup.message_count === 0 && ordinary.message_count === 0
    && !sup.has_pending_question && !ordinary.has_pending_question && initialList.length === 2, 'fresh_mode_fixture_required');
  const scope = await get('/supervisor/default/scope');
  requireFact(scope.scope?.supervisor?.session_id === SUPERVISOR && scope.scope.allowed_projects?.length === 1
    && scope.scope.allowed_projects[0] === c.project_id && Number.isSafeInteger(scope.scope.state_revision), 'explicit_fixture_project_scope_required');
  const flow = scenario(c, fixtureId);
  flow.createRequest = Object.freeze({ operation_id: c.operation_id, expected_state_revision: scope.scope.state_revision,
    project_id: c.project_id, workspace_path: c.workspace_path, message: flow.assignment, title: 'Synthetic v2 fixture Root' });
  const hooks = { waitRoot: id => waitFor(async () => settled(await detail(id))),
    waitChild: () => waitFor(async () => { const candidates = (await list()).filter(s => s.kind === 'child'); return candidates.length === 1 && settled(await detail(candidates[0].id)); }) };
  const stopProvider = await startProvider(c, flow, hooks, signal);
  try {
    const submit = async (id, message) => {
      const r = await api('/chat', { session_id: id, message });
      if (r.status === 403 && c.expected_standard_ticket_denial && id === SUPERVISOR) {
        requireFact(JSON.stringify(r.body.error).includes('Supervisor orchestration authority required'), 'unexpected_standard_denial'); return false;
      }
      requireFact(r.status === 201 || r.status === 200, 'chat_admission_failed');
      const run = await api('/sessions/' + encodeURIComponent(id) + '/execute', {});
      requireFact(run.status === 202 || run.status === 200, 'execute_admission_failed'); return true;
    };
    const admitted = await submit(SUPERVISOR, flow.markers.supervisor);
    if (!admitted) {
      requireFact(stable(await list()) === stable(initialList) && stable(await get('/supervisor/default/scope')) === stable(scope)
        && flow.requests === 0, 'standard_refusal_had_side_effects');
      return { mode: c.mode, status: 'BLOCKED_EXISTING_STANDARD_TICKET_GATE', provider_requests: 0, v2_positive: 'UNRUN' };
    }
    const finalSupervisor = await waitFor(async () => { const s = await detail(SUPERVISOR); return settled(s) && s; });
    requireFact(finalSupervisor.last_run_status === 'completed' && flow.rootId, 'supervisor_did_not_complete');
    const root = await detail(flow.rootId), children = (await list()).filter(s => s.parent_session_id === root.id);
    requireFact(children.length === 1, 'exactly_one_canonical_child_required');
    const child = await detail(children[0].id);
    requireFact(stable((await list()).map(s => s.id).sort()) === stable([SUPERVISOR, c.ordinary_session_id, root.id, child.id].sort()), 'unexpected_extra_sessions');
    validateComposition(c, root, child, messages(await get('/sessions/' + root.id + '/history')),
      messages(await get('/sessions/' + SUPERVISOR + '/history')), flow);
    const beforeDenied = await get('/supervisor/default/scope'), beforeList = (await list()).map(s => s.id).sort();
    await submit(c.ordinary_session_id, flow.markers.ordinary);
    const denied = await waitFor(async () => { const s = await detail(c.ordinary_session_id); return settled(s) && s; });
    if (!flow.ordinaryDenied) requireFact(/session_control.*(not allowed|unavailable|requires|unknown)|Supervisor.*(required|identity)|orchestration.*(restricted|not allowed)/i.test(denied.last_run_error || ''), 'ordinary_denial_evidence_missing');
    requireFact(stable(await get('/supervisor/default/scope')) === stable(beforeDenied)
      && stable((await list()).map(s => s.id).sort()) === stable(beforeList), 'ordinary_management_side_effect');
    requireFact(flow.errors.length === 0, 'scripted_provider_errors');
    return { mode: c.mode, status: 'PASS', root_retry_deduplicated: true, child_notification_observed: true,
      independent_root: true, shared_workspace_child: true, ordinary_management_denied: true, provider_requests: flow.requests };
  } finally { await stopProvider(); }
}
async function runSuite(config, request = jsonRequest) {
  const controller = new AbortController(), timer = setTimeout(() => controller.abort(), 30000), cases = [];
  try {
    for (const c of config.cases) {
      const file = path.join(c.workspace_path, 'bamboo-v2-native-fixture.txt'), flow = scenario(c, config.fixture_id);
      const handle = await fs.open(file, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | (constants.O_NOFOLLOW || 0), 0o600);
      try { await handle.writeFile(flow.fileContent); } finally { await handle.close(); }
      cases.push(await runCase(c, config.fixture_id, request, controller.signal));
    }
    return { status: cases.every(c => c.status === 'PASS') ? 'PASS' : 'PARTIAL', cases };
  } finally { controller.abort(); clearTimeout(timer); }
}
function parseArgs(argv) {
  if (argv.length === 0 || argv[0] === '--help') return { command: 'help' };
  const [command, ...rest] = argv; requireFact(['preflight', 'run'].includes(command), 'unknown_command');
  requireFact(rest.length === 0 || (rest.length === 2 && rest[0] === '--receipt' && !rest[1].startsWith('--')), 'invalid_options');
  return { command, receipt: rest[1] };
}
async function main(argv = process.argv.slice(2)) {
  let command = 'unknown', nativeStarted = false;
  try {
    const options = parseArgs(argv); command = options.command;
    if (command === 'help') { process.stdout.write(HELP + '\n'); return; }
    if (!options.receipt) throw new Failure('matching_host_receipt_required');
    const verified = await verifyReceipt(options.receipt);
    if (command === 'preflight') {
      console.log(JSON.stringify({ schema_version: 1, status: 'ARTIFACT_BYTES_MATCH_RECEIPT_NATIVE_UNRUN', source_sha: SOURCE,
        source_identity_basis: 'independently_audited_producer_receipt_required',
        tree_sha: TREE, receipt_sha256: verified.receipt_sha256, binary_sha256: verified.config.binary.sha256, http_requests: 0 })); return;
    }
    await verifyProcess(verified.config);
    nativeStarted = true;
    const boundRequest = async (base, route, body, signal) => {
      if (body !== undefined) await verifyProcess({ ...verified.config, cases: verified.config.cases.filter(c => origin(c.api_base) === origin(base)) });
      return jsonRequest(base, route, body, signal);
    };
    const result = await runSuite(verified.config, boundRequest);
    console.log(JSON.stringify({ schema_version: 1, evidence_kind: 'actual_native_host', source_sha: SOURCE,
      receipt_sha256: verified.receipt_sha256, binary_sha256: verified.config.binary.sha256, ...result }));
    process.exitCode = result.status === 'PASS' ? 0 : 3;
  } catch (e) {
    const code = e instanceof Failure ? e.code : 'fixture_unavailable';
    console.log(JSON.stringify({ schema_version: 1, evidence_kind: 'native_unrun_or_incomplete', status: nativeStarted ? 'INCOMPLETE' : 'UNRUN',
      native_started: nativeStarted, operator_cleanup_required: nativeStarted, command, code,
      required_source_sha: SOURCE, required_tree_sha: TREE, no_automatic_retry: true })); process.exitCode = 3;
  }
}
module.exports = { SOURCE, TREE, Failure, validateReceipt, readBounded, hashArtifact, origin, scenario, roleOf,
  creationReceipt, nextReply, startProvider, validateComposition, messages, runSuite, parseArgs, verifyReceipt, jsonRequest };
if (require.main === module) main();
