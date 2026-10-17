const encode = text => new TextEncoder().encode(text);
export async function sha256(bytes) {
  return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)), b => b.toString(16).padStart(2,'0')).join('');
}
export async function verify(manifestBytes, index) {
  const manifest = JSON.parse(new TextDecoder('utf-8', {fatal:true}).decode(manifestBytes));
  const fail = () => { throw new Error('Manual bundle integrity mismatch'); };
  if (manifest.format_version !== 1 || index.schema !== 1 || manifest.package !== 'bamboo-skills' || index.bundle_id !== await sha256(manifestBytes)) fail();
  for (const key of ['package','package_version','content_sha256','source']) if (JSON.stringify(index[key]) !== JSON.stringify(manifest[key])) fail();
  if (!Array.isArray(manifest.resources) || !Array.isArray(index.documents)) fail();
  const paths = new Set();
  for (const entry of manifest.resources) {
    if (typeof entry.path !== 'string' || !/^[a-zA-Z0-9_.-]+(?:\/[a-zA-Z0-9_.-]+)*$/.test(entry.path) || entry.path.split('/').some(p => p === '.' || p === '..') || paths.has(entry.path)) fail();
    paths.add(entry.path);
  }
  const entries = manifest.resources.filter(entry => entry.path.startsWith('references/'));
  if (entries.length !== index.documents.length) fail();
  const seen = new Set();
  for (const doc of index.documents) {
    const entry = entries.find(entry => entry.path === doc.path);
    if (!entry || seen.has(doc.path) || typeof doc.text !== 'string') fail();
    seen.add(doc.path);
    const bytes = encode(doc.text);
    const title = doc.text.split(/\r?\n/).find(line => line.startsWith('# '))?.slice(2) ?? doc.path;
    if (doc.title !== title || doc.bytes !== bytes.length || entry.bytes !== bytes.length || doc.sha256 !== entry.sha256 || doc.sha256 !== await sha256(bytes)) fail();
  }
  return index;
}
