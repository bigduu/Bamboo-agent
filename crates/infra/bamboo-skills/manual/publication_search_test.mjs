// Run against the actual exported version/bundle directory, including copied executable exports.
import {readFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
import {webcrypto} from 'node:crypto';
import assert from 'node:assert/strict';
import test from 'node:test';
globalThis.crypto ??= webcrypto;
const root = resolve(process.argv[2]);
const {verify, sha256} = await import(pathToFileURL(`${root}/verify.mjs`));
const {search} = await import(pathToFileURL(`${root}/search.mjs`));
const manifest = await readFile(`${root}/manifest.json`);
const index = JSON.parse(await readFile(`${root}/index.json`, 'utf8'));
test('exported bundle verifies and contains current diagnostic/manual chapters', async () => {
  assert.deepEqual(await verify(manifest, index), index);
  for (const query of ['Hook','memory','stream_timeout']) assert.ok(search(index.documents, query).length, query);
  assert.ok(index.documents.find(d => d.path === 'references/troubleshooting.md').text.includes('bamboo_diagnostics'));
});
test('literal AND search is case insensitive, stable, non-mutating and empty safe', () => {
  const before = JSON.stringify(index);
  const tied = [{path:'references/z.md',title:'same',text:'same'}, {path:'references/a.md',title:'same',text:'same'}];
  assert.deepEqual(search(tied,'same').map(d=>d.path), ['references/a.md','references/z.md']);
  assert.deepEqual(search(index.documents,'HOOK'),search(index.documents,'hook'));
  assert.equal(search(index.documents,'no_such_manual_term_46735').length,0);
  assert.equal(search(index.documents,'').length,index.documents.length);
  assert.equal(search(index.documents,'[.*').length,0);
  for (const doc of search(index.documents,'memory session')) assert.ok(`${doc.path} ${doc.title} ${doc.text}`.toLowerCase().includes('session'));
  assert.equal(JSON.stringify(index),before);
});
test('mixed identity and changed or missing documents fail closed', async () => {
  for (const mutate of [x=>x.bundle_id='0'.repeat(64),x=>x.package_version='wrong',x=>x.source={},x=>x.documents.pop(),x=>x.documents.push(x.documents[0]),x=>x.documents[0].text+='x',x=>x.documents[0].title='wrong',x=>x.documents[0].path='../secret',x=>x.documents[0].bytes++]) {
    const changed=structuredClone(index); mutate(changed); await assert.rejects(verify(manifest,changed));
  }
});
test('published resources preserve manifest bytes and exact indexed text', async () => {
  const parsed=JSON.parse(manifest);
  for (const entry of parsed.resources) {
    const bytes=await readFile(`${root}/resources/${entry.path}`);
    assert.equal(bytes.length,entry.bytes);
    const doc=index.documents.find(d=>d.path===entry.path);
    if(doc) assert.equal(bytes.toString('utf8'),doc.text);
  }
});

test('invalid manifest paths and duplicate resources fail even with matching manifest identity', async () => {
  for (const mutate of [m=>m.resources[0].path='../secret',m=>m.resources.push(m.resources[0])]) {
    const changed=JSON.parse(manifest); mutate(changed);
    const bytes=new TextEncoder().encode(JSON.stringify(changed));
    const paired=structuredClone(index); paired.bundle_id=await sha256(bytes);
    await assert.rejects(verify(bytes, paired));
  }
});
