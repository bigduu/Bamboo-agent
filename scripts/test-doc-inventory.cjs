#!/usr/bin/env node
'use strict';
// Cargo supplies the real rustdoc argv/env. Inventory only: no doctest body runs.
const fs = require('node:fs');
const path = require('node:path');
const r = require('./test-groups.cjs');
async function main() {
  const real = process.env.BAMBOO_REAL_RUSTDOC, directory = process.env.BAMBOO_DOC_INVENTORY;
  if (!real || !directory) throw new Error('Explicit rustdoc inventory context required');
  const args = process.argv.slice(2);
  if (!args.includes('--test')) {
    const {spawnSync} = require('node:child_process');
    const p = spawnSync(real, args, {stdio: 'inherit'}); process.exitCode = p.status ?? 1; return;
  }
  const pkg = process.env.CARGO_PKG_NAME;
  if (!pkg || !/^[a-z0-9-]+$/.test(pkg)) throw new Error('Missing Cargo doctest package identity');
  fs.mkdirSync(directory, {recursive: true});
  const id = pkg + '-' + r.digest(args).slice(0, 12), log = path.join(directory, id);
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => /^(CARGO_PKG_|CARGO_MANIFEST_|CARGO_CRATE_NAME$|CARGO_TARGET_TMPDIR$|OUT_DIR$|RUSTUP_TOOLCHAIN$)/.test(key)));
  const invocation = {id, package: pkg, executable: real, args, env, cwd: process.cwd()};
  r.writeJSON(log + '.invocation.json', invocation);
  const result = await r.processRun(real, [...args, '--test-args=--list'], {cwd: invocation.cwd, env: process.env, timeoutMs: 60000, log});
  const ignored = await r.processRun(real, [...args, '--test-args=--list', '--test-args=--ignored'],
    {cwd: invocation.cwd, env: process.env, timeoutMs: 60000, log: log + '-ignored'});
  const names = r.parseList(r.checked(result)), ignoredNames = r.parseList(r.checked(ignored));
  if (ignoredNames.some(name => !names.includes(name))) throw new Error('Ignored doctest inventory differs');
  r.writeJSON(log + '.result.json', {...r.reportProcess(result), names, ignoredNames});
}
main().catch(e => { console.error(e.message); process.exitCode = 1; });
