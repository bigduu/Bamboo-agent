'use strict';
// Execute the actual outgoing encoder, with no child process/browser/storage.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const source = fs.readFileSync(path.join(__dirname, 'host.cjs'), 'utf8');
const start = source.indexOf('function replyEnvelope(');
const end = source.indexOf('\nfunction isScopeCleanup(', start);
assert(start >= 0 && end > start);
const scope = vm.createContext({});
vm.runInContext(source.slice(start, end), scope);
const encode = vm.runInContext('replyEnvelope', scope);
const request = { id: 17, scope_id: 'a'.repeat(24), scope_generation: 'b'.repeat(24) };
const generation = 'c'.repeat(24);
const wire = (payload, v2 = true) => JSON.parse(JSON.stringify(encode(request, payload, v2, generation)));

for (const code of ['browser_download_busy', 'browser_scope_busy']) {
  test(`v2 ${code} emits the existing typed busy code and keeps the owner tuple`, () => {
    const payload = Object.freeze({ ok: false, code, error: 'synthetic capacity refusal', retry_after_ms: 25 });
    assert.deepEqual(wire(payload), { id: 17, ok: false, code: 'browser_busy',
      error: payload.error, retry_after_ms: 25, version: 2, host_generation: generation,
      scope_id: request.scope_id, scope_generation: request.scope_generation });
    assert.equal(payload.code, code, 'Internal error subtype must remain intact');
  });
}

test('v2 existing busy and all unrelated typed errors retain their codes and messages', () => {
  for (const code of ['browser_busy', 'browser_scope_closed', 'browser_profile_mode_conflict',
    'stale_epoch', 'stale_dialog', 'dialog_pending', 'browser_storage_unavailable', 'unknown_error']) {
    const result = wire({ ok: false, code, error: `synthetic ${code}` });
    assert.equal(result.code, code); assert.equal(result.error, `synthetic ${code}`);
  }
});

test('successful data resembling a busy error is not rewritten', () => {
  const payload = { ok: true, code: 'browser_scope_busy', result: { code: 'browser_download_busy' } };
  const result = wire(payload);
  assert.equal(result.code, payload.code); assert.deepEqual(result.result, payload.result);
});

test('legacy transport retains its existing subtype and envelope', () => {
  for (const code of ['browser_download_busy', 'browser_scope_busy']) {
    assert.deepEqual(wire({ ok: false, code, error: 'legacy capacity' }, false), {
      id: 17, ok: false, code, error: 'legacy capacity',
    });
  }
});

test('a payload without an explicit failure is not classified as backpressure', () => {
  assert.equal(wire({ code: 'browser_scope_busy' }).code, 'browser_scope_busy');
});
