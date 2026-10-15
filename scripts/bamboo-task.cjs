#!/usr/bin/env node
// Thin client for the existing local Bamboo HTTP runtime. No local task store.
'use strict';
const { randomUUID } = require('node:crypto');
const fs = require('node:fs/promises');
const path = require('node:path');

const MAX_JSON = 1024 * 1024;
const MAX_FRAME = 256 * 1024;
const MAX_STREAM = 16 * 1024 * 1024;
const DEFAULT_SUPERVISOR = 'bamboo-default-supervisor';
const COMMANDS = {
  inspect: [],
  submit: ['supervisor', 'new', 'session-id', 'prompt-file', 'request-id', 'model', 'provider', 'workspace', 'project-id'],
  status: ['session-id', 'run-id', 'session-created-at', 'include-output'],
  events: ['session-id', 'max-events', 'include-output'],
  stop: ['session-id', 'run-id'],
};
const BOOL = new Set(['supervisor', 'new', 'include-output', 'help']);
const HELP = `bamboo-task (Node 22+) — client of an already running loopback Bamboo
  inspect
  submit (--supervisor | --session-id ID | --new) --prompt-file PATH|-
         [--request-id ID] [--model ID --provider ID]
         [--workspace ABSOLUTE_PATH --project-id ID]  (new Root only)
  status --session-id ID [--run-id ID --session-created-at RFC3339] [--include-output]
  events --session-id ID [--max-events N] [--include-output]
  stop --session-id ID --run-id ID
Common: --server-url http://127.0.0.1:9562[/api/v1] --timeout-ms N
JSON/NDJSON stdout; errors omit backend bodies. No login, retry or permission override.
See docs/guides/SUPERVISOR_TASK_CLI.md for identity, retention and cost boundaries.`;

class Failure extends Error {
  constructor(code, exit, extra = {}) { super(code); Object.assign(this, { code, exit, ...extra }); }
}
const fail = (code, exit = 6, extra) => { throw new Failure(code, exit, extra); };
const emit = value => process.stdout.write(`${JSON.stringify({ schema_version: 1, ...value })}\n`);
const isId = s => typeof s === 'string' && /^[A-Za-z0-9_-][A-Za-z0-9_.:-]{0,127}$/.test(s) && s !== '.' && s !== '..';
function id(s, exit = 2) { if (!isId(s)) fail('invalid_identity', exit); return s; }
function birth(s, exit = 2) {
  // Preserve sub-millisecond precision. Never serialize through JS Date.
  if (typeof s !== 'string' || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?(?:Z|[+-]\d\d:\d\d)$/.test(s) || !Number.isFinite(Date.parse(s))) fail('invalid_session_birth', exit);
  return s;
}
function number(s, fallback, max) {
  if (s === undefined) return fallback;
  if (!/^[1-9]\d*$/.test(s) || !Number.isSafeInteger(Number(s)) || Number(s) > max) fail('invalid_limit', 2);
  return Number(s);
}
function parse(argv) {
  const command = argv.shift();
  if (command === '--help' || command === '-h') return { help: true };
  if (!Object.hasOwn(COMMANDS, command || '')) fail('unknown_command', 2);
  const allowed = new Set([...COMMANDS[command], 'server-url', 'timeout-ms', 'help']);
  const o = { command };
  while (argv.length) {
    const arg = argv.shift();
    if (!arg.startsWith('--') || !allowed.has(arg.slice(2)) || Object.hasOwn(o, arg.slice(2))) fail('invalid_option', 2);
    const key = arg.slice(2);
    if (BOOL.has(key)) o[key] = true;
    else { const value = argv.shift(); if (!value || value.startsWith('--')) fail('missing_option_value', 2); o[key] = value; }
  }
  if (o.help) return o;
  const raw = o['server-url'] || 'http://127.0.0.1:9562';
  let u; try { u = new URL(raw); } catch { fail('invalid_server_url', 2); }
  if (!['http:', 'https:'].includes(u.protocol) || !['127.0.0.1', '[::1]'].includes(u.hostname) || u.username || u.password || u.search || u.hash || !['/', '/api/v1', '/api/v1/'].includes(u.pathname)) fail('server_must_be_loopback_api', 2);
  o.base = `${u.origin}/api/v1`;
  o.timeout = number(o['timeout-ms'], 30000, 600000);
  o.maxEvents = number(o['max-events'], 1000, 10000);
  for (const key of ['session-id', 'run-id', 'project-id', 'request-id']) if (o[key] !== undefined) id(o[key]);
  if (o['request-id']?.length > 64) fail('request_id_too_long', 2);
  if (o['session-created-at']) birth(o['session-created-at']);
  if (command === 'submit') {
    if ([o.supervisor, o.new, o['session-id']].filter(Boolean).length !== 1 || !o['prompt-file']) fail('submit_requires_target_and_prompt_file', 2);
    if (o['session-id'] === DEFAULT_SUPERVISOR) fail('use_explicit_supervisor_bootstrap', 2);
    if (Boolean(o.model) !== Boolean(o.provider)) fail('model_and_provider_required_together', 2);
    for (const key of ['model', 'provider']) if (o[key] && (Buffer.byteLength(o[key]) > 256 || /[\x00-\x1f\x7f]/.test(o[key]))) fail('invalid_model_selection', 2);
    if ((o.workspace || o['project-id']) && !o.new) fail('workspace_and_project_are_new_root_only', 2);
    if (o.workspace && !path.isAbsolute(o.workspace)) fail('workspace_must_be_absolute', 2);
  } else if (command !== 'inspect') {
    if (!o['session-id']) fail('session_id_required', 2);
    if (command === 'stop' && !o['run-id']) fail('run_id_required', 2);
    if (command === 'status' && (Boolean(o['run-id']) !== Boolean(o['session-created-at']) || (o['include-output'] && !o['run-id']))) fail('exact_status_requires_run_and_original_birth', 2);
  }
  return o;
}

async function readPrompt(file, signal) {
  const chunks = []; let size = 0;
  const add = chunk => { size += chunk.length; if (size > 128 * 1024) fail('prompt_too_large', 2); chunks.push(chunk); };
  if (file === '-') {
    const abort = () => process.stdin.destroy(new Failure('input_aborted', 2));
    signal.addEventListener('abort', abort, { once: true });
    try { for await (const chunk of process.stdin) add(Buffer.from(chunk)); }
    finally { signal.removeEventListener('abort', abort); }
  } else {
    let handle;
    try {
      handle = await fs.open(file, 'r');
      const stat = await handle.stat();
      if (!stat.isFile()) fail('prompt_must_be_regular_file', 2);
      const data = Buffer.alloc(128 * 1024 + 1);
      let count = 0;
      while (count < data.length) { const r = await handle.read(data, count, data.length - count); if (!r.bytesRead) break; count += r.bytesRead; }
      add(data.subarray(0, count));
    } catch (e) { if (e instanceof Failure) throw e; fail('prompt_file_unreadable', 2); }
    finally { await handle?.close(); }
  }
  let text; try { text = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks)); } catch { fail('prompt_must_be_utf8', 2); }
  if (!text.trim()) fail('prompt_empty', 2);
  if (/^\s*\/goal(?:\s|$)/.test(text)) fail('goal_control_is_not_task_submission', 2);
  return text;
}

class Client {
  constructor(o, signal, context) { Object.assign(this, { o, signal, context }); }
  async response(route, method = 'GET', body, headers = {}) {
    this.signal.throwIfAborted();
    try {
      return await fetch(`${this.o.base}${route}`, { method, redirect: 'error', credentials: 'omit', signal: this.signal,
        headers: { Accept: 'application/json', ...(body === undefined ? {} : { 'Content-Type': 'application/json' }), ...headers },
        body: body === undefined ? undefined : JSON.stringify(body) });
    } catch { fail('transport_failed', 3); }
  }
  async json(route, method = 'GET', body, headers, allowError = false) {
    const r = await this.response(route, method, body, headers);
    if ((r.status === 401 || r.status === 403) || (!allowError && !r.ok)) {
      await r.body?.cancel();
      fail(r.status === 401 || r.status === 403 ? 'access_denied' : 'http_rejected', r.status === 401 || r.status === 403 ? 4 : 5, { http_status: r.status });
    }
    const chunks = []; let size = 0;
    for await (const chunk of r.body || []) {
      size += chunk.length;
      if (size > MAX_JSON) { await r.body?.cancel().catch(() => {}); fail('response_too_large'); }
      chunks.push(chunk);
    }
    let data;
    try { data = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks))); }
    catch { if (allowError) return { r, data: null }; fail('invalid_json_response'); }
    if (!data || typeof data !== 'object' || Array.isArray(data)) fail('invalid_json_envelope');
    return { r, data };
  }
  async compatibility(required = false) {
    this.context.phase = 'compatibility';
    const { data: b } = await this.json('/bootstrap');
    if (b.schema_version !== 1 || b.server?.product !== 'bamboo' || b.api?.name !== 'bamboo.agent' || b.api?.canonical_base_path !== '/api/v1' || b.api.min_version !== 1 || !(b.api.max_version >= 1)) fail('incompatible_bootstrap');
    // Read-only feature detection of the native receipt/run API added after
    // fenced stop (#1951/#1953). No POST probe or claim about arbitrary servers.
    const probe = `/sessions/bamboo-cli-probe-${randomUUID()}/runs/cli-probe/output`;
    const { r, data } = await this.json(probe, 'GET', undefined, undefined, true);
    const supported = r.status === 400 && data?.error?.type === 'api_error' && data?.error?.message === 'Exact output requires one Bamboo-Session-Created-At header';
    if (required && !supported) fail('receipt_run_api_unavailable');
    return { bootstrap_schema: 1, receipt_run_api_detected: supported, mutations_supported: supported, probe_http_status: r.status };
  }
  async session(sid) {
    const { data } = await this.json(`/sessions/${encodeURIComponent(sid)}`);
    const s = data.session;
    if (!s || s.id !== sid) fail('session_identity_mismatch');
    birth(s.created_at, 6);
    return s;
  }
  async post(route, body, headers, field) {
    this.context.phase = field;
    this.context.admission[field] = 'unknown';
    try {
      // A 2xx alone does not prove the semantic acknowledgement. The caller
      // marks accepted only after validating this operation's actual receipt.
      return await this.json(route, 'POST', body, headers);
    } catch (e) {
      if (e.http_status && e.http_status < 500) this.context.admission[field] = 'rejected';
      throw e;
    }
  }
}

function summary(s) {
  const result = { session_id: id(s.id, 6), session_created_at: birth(s.created_at, 6), observation_scope: 'session' };
  for (const key of ['is_running', 'has_pending_question']) if (typeof s[key] === 'boolean') result[key] = s[key];
  for (const key of ['running_child_count', 'message_count']) if (Number.isSafeInteger(s[key]) && s[key] >= 0) result[key] = s[key];
  if (['root', 'child'].includes(s.kind)) result.kind = s.kind;
  if (['completed', 'cancelled', 'error', 'running', 'pending'].includes(s.last_run_status)) result.last_run_status = s.last_run_status;
  return result;
}
async function submit(c, o, ctx) {
  const prompt = await readPrompt(o['prompt-file'], c.signal);
  await c.compatibility(true);
  const key = phase => ({ 'Idempotency-Key': `cli:${ctx.request_id}:${phase}` });
  const model = o.model ? { model: o.model, provider: o.provider } : {};
  let sid = o['session-id'];
  if (o.supervisor) {
    const { data } = await c.post('/supervisor/default', {}, undefined, 'bootstrap');
    if (data.session_id !== DEFAULT_SUPERVISOR || !isId(data.incarnation_id)) fail('invalid_supervisor_receipt');
    ctx.admission.bootstrap = 'accepted';
    sid = data.session_id;
    ctx.supervisor_incarnation_id = data.incarnation_id;
  } else if (o.new) {
    const { data } = await c.post('/sessions', { ...model, ...(o.workspace ? { workspace_path: o.workspace } : {}), ...(o['project-id'] ? { project_id: o['project-id'] } : {}) }, key('create'), 'create');
    sid = id(data.session?.id, 6);
    ctx.admission.create = 'accepted';
  }
  ctx.session_id = sid;
  ctx.phase = 'session_read';
  const s = await c.session(sid);
  ctx.session_created_at = s.created_at;
  if (s.kind !== 'root' || s.parent_session_id != null) fail('submission_requires_root');
  // An explicit message_id selects referenced/legacy ingress on the server.
  // Ordinary selected execution requires Native chat; its idempotency receipt
  // preserves the server-generated message ID. Supervisor uses User ingress.
  const { data: chat } = await c.post('/chat', { session_id: sid, message: prompt, ...(o.supervisor ? { message_id: ctx.request_id } : {}), ...model }, key('chat'), 'chat');
  if (chat.session_id !== sid || !isId(chat.message_id) || !Number.isSafeInteger(chat.ingress_seq) || chat.ingress_seq <= 0) fail('invalid_chat_receipt');
  ctx.admission.chat = 'accepted';
  ctx.message_id = chat.message_id;
  ctx.ingress_seq = chat.ingress_seq;
  const selected = o.supervisor ? {} : { 'Bamboo-Input-Id': chat.message_id, 'Bamboo-Ingress-Seq': String(chat.ingress_seq), 'Bamboo-Session-Created-At': s.created_at };
  const { r, data: run } = await c.post(`/sessions/${encodeURIComponent(sid)}/execute`, {}, { ...key('execute'), ...selected }, 'execute');
  if (run.session_id !== sid || !isId(run.run_id) || !['started', 'already_running'].includes(run.status)) fail('invalid_execution_receipt');
  ctx.run_id = run.run_id;
  if (!o.supervisor && (r.status !== 202 || run.status !== 'started' || r.headers.get('Bamboo-Input-Id') !== chat.message_id || r.headers.get('Bamboo-Ingress-Seq') !== String(chat.ingress_seq) || r.headers.get('Bamboo-Output-Scope') !== 'activation-receipt' || instant(r.headers.get('Bamboo-Session-Created-At')) !== instant(s.created_at))) fail('selected_execution_not_acknowledged');
  ctx.admission.execute = 'accepted';
  return { ...ctx, execution_scope: o.supervisor ? 'session_activation' : 'selected_input', execution_status: run.status, event_scope: 'session' };
}
async function status(c, o, ctx) {
  ctx.phase = 'session_read';
  const s = await c.session(o['session-id']);
  if (!o['run-id']) return { ...summary(s) };
  ctx.phase = 'exact_output';
  const { data } = await c.json(`/sessions/${encodeURIComponent(s.id)}/runs/${encodeURIComponent(o['run-id'])}/output`, 'GET', undefined, { 'Bamboo-Session-Created-At': o['session-created-at'] });
  if (data.session_id !== s.id || data.run_id !== o['run-id'] || data.output_scope !== 'visible_messages' || !['pending', 'running', 'completed', 'cancelled', 'error'].includes(data.runner_status) || !data.output || !Array.isArray(data.output.messages)) fail('invalid_exact_output');
  birth(data.session_created_at, 6);
  // Compare instants without dropping nanoseconds (offsets allowed by API).
  if (instant(data.session_created_at) !== instant(o['session-created-at'])) fail('output_birth_mismatch');
  const output = data.output;
  if (!Number.isSafeInteger(output.version) || output.version < 0 || typeof output.history_committed !== 'boolean' || ![null, 'complete', 'error', 'cancelled'].includes(output.terminal)) fail('invalid_visible_output');
  const result = { session_id: s.id, run_id: data.run_id, session_created_at: data.session_created_at, observation_scope: 'exact_run', runner_status: data.runner_status,
    output: { version: output.version, terminal: output.terminal, history_committed: output.history_committed, message_count: output.messages.length } };
  if (o['include-output']) result.output.messages = output.messages.map(m => {
    if (!isId(m.id) || typeof m.content !== 'string') fail('invalid_visible_message');
    return { id: m.id, content: m.content };
  });
  return result;
}
function instant(s) {
  birth(s, 6);
  const fraction = (s.match(/\.(\d+)/)?.[1] || '').padEnd(9, '0');
  return BigInt(Date.parse(s.replace(/\.\d+/, ''))) * 1000000n + BigInt(fraction);
}
async function events(c, o, ctx) {
  ctx.phase = 'events';
  const r = await c.response(`/sessions/${encodeURIComponent(o['session-id'])}/events`);
  if (!r.ok) { await r.body?.cancel(); fail(r.status === 401 || r.status === 403 ? 'access_denied' : 'http_rejected', r.status === 401 || r.status === 403 ? 4 : 5, { http_status: r.status }); }
  if (!r.headers.get('content-type')?.toLowerCase().startsWith('text/event-stream')) { await r.body?.cancel(); fail('expected_event_stream'); }
  const decoder = new TextDecoder('utf-8', { fatal: true });
  let buffer = '', lines = [], frameBytes = 0, total = 0, count = 0, reason;
  const line = value => {
    frameBytes += Buffer.byteLength(value) + 1;
    if (frameBytes > MAX_FRAME) fail('event_frame_too_large');
    if (value !== '') { if (value.startsWith('data:')) lines.push(value.slice(5).replace(/^ /, '')); return; }
    const data = lines.join('\n'); lines = []; frameBytes = 0;
    if (!data || data === '[KEEPALIVE]') return;
    if (data === '[DONE]') { reason = 'session_boundary'; return; }
    let event; try { event = JSON.parse(data); } catch { fail('invalid_event_json'); }
    if (!event || typeof event.type !== 'string' || !/^[a-z][a-z0-9_]{0,63}$/.test(event.type)) fail('invalid_event_envelope');
    const view = { ok: true, command: 'events', session_id: o['session-id'], event_scope: 'session', event: { type: event.type } };
    // Never infer run identity for untagged tokens, child events or terminals.
    if (event.run_id !== undefined) view.event.run_id = id(event.run_id, 6);
    if (event.session_id !== undefined) view.event.session_id = id(event.session_id, 6);
    if (o['include-output'] && event.type === 'token' && typeof event.content === 'string') view.event.content = event.content;
    emit(view); count++;
    if (['complete', 'cancelled', 'error'].includes(event.type)) reason = 'session_boundary';
    else if (event.type === 'need_clarification') reason = 'session_pause';
    else if (count >= o.maxEvents) reason = 'event_limit';
  };
  try {
    for await (const chunk of r.body || []) {
      total += chunk.length; if (total > MAX_STREAM) fail('event_stream_too_large');
      try { buffer += decoder.decode(chunk, { stream: true }); } catch { fail('invalid_event_utf8'); }
      while (!reason) {
        const i = buffer.search(/[\r\n]/); if (i < 0 || (buffer[i] === '\r' && i === buffer.length - 1)) break;
        const width = buffer[i] === '\r' && buffer[i + 1] === '\n' ? 2 : 1;
        const value = buffer.slice(0, i); buffer = buffer.slice(i + width); line(value);
      }
      if (reason) break;
      if (Buffer.byteLength(buffer) + frameBytes > MAX_FRAME) fail('event_frame_too_large');
    }
    if (!reason) {
      try { buffer += decoder.decode(); } catch { fail('invalid_event_utf8'); }
      if (buffer === '\r') { line(''); buffer = ''; }
      if (buffer || lines.length) fail('truncated_event_frame');
      reason = 'eof';
    }
  } finally { await r.body?.cancel().catch(() => {}); }
  return { session_id: o['session-id'], event_scope: 'session', stream_end: reason, event_count: count };
}

async function main() {
  let o, ctx = { command: 'unknown', phase: 'arguments' }, timer;
  const controller = new AbortController();
  const interrupt = () => controller.abort('interrupted');
  process.once('SIGINT', interrupt);
  try {
    o = parse(process.argv.slice(2));
    if (o.help) { emit({ ok: true, help: HELP }); return; }
    if (Number(process.versions.node.split('.')[0]) < 22) fail('node_22_required', 2);
    ctx = { command: o.command, phase: 'start' };
    if (o['session-id']) ctx.session_id = o['session-id'];
    if (o['run-id']) ctx.run_id = o['run-id'];
    if (o.command === 'submit') { ctx.request_id = o['request-id'] || randomUUID(); ctx.admission = { chat: 'not_sent', execute: 'not_sent' }; }
    if (o.command === 'stop') ctx.admission = { stop: 'not_sent' };
    timer = setTimeout(() => controller.abort('timeout'), o.timeout);
    const c = new Client(o, controller.signal, ctx);
    let result;
    if (o.command === 'inspect') result = await c.compatibility();
    if (o.command === 'submit') result = await submit(c, o, ctx);
    if (o.command === 'status') result = await status(c, o, ctx);
    if (o.command === 'events') result = await events(c, o, ctx);
    if (o.command === 'stop') {
      await c.compatibility(true);
      const { data } = await c.post(`/sessions/${encodeURIComponent(o['session-id'])}/stop`, {}, { 'Bamboo-Run-Id': o['run-id'] }, 'stop');
      if (data.success !== true || data.session_id !== o['session-id'] || data.run_id !== o['run-id']) fail('fenced_stop_not_acknowledged');
      ctx.admission.stop = 'accepted';
      result = { session_id: data.session_id, run_id: data.run_id, cancellation_acknowledged: true };
    }
    emit({ ok: true, ...ctx, ...result });
  } catch (e) {
    let error = e instanceof Failure ? e : new Failure('transport_or_protocol_failed', 3);
    if (controller.signal.aborted) error = new Failure(controller.signal.reason === 'interrupted' ? 'interrupted' : 'timeout', controller.signal.reason === 'interrupted' ? 130 : 124);
    const values = Object.values(ctx.admission || {});
    const partial = values.some(v => v === 'accepted' || v === 'unknown');
    const exit = partial ? 7 : error.exit;
    emit({ ok: false, ...ctx, ...(partial ? { admission_outcome: values.includes('unknown') ? 'unknown' : 'partial' } : {}),
      error: { code: error.code, exit_code: exit, ...(partial ? { cause_exit_code: error.exit } : {}), ...(error.http_status ? { http_status: error.http_status } : {}) } });
    process.exitCode = exit;
  } finally { clearTimeout(timer); process.removeListener('SIGINT', interrupt); }
}

process.stdout.on('error', () => { process.exit(3); });
if (require.main === module) main();
