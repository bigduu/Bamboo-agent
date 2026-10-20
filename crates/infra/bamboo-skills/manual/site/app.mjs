import {verify} from './verify.mjs';
import {search} from './search.mjs';
const el = id => document.getElementById(id);
try {
  const loopback = ['localhost','127.0.0.1','[::1]'].includes(location.hostname);
  if (location.protocol !== 'https:' && !(location.protocol === 'http:' && loopback)) throw new Error('Serve over HTTPS or localhost');
  const fetchLocal = async path => {
    const response = await fetch(new URL(path, location.href), {credentials:'omit', redirect:'error'});
    if (!response.ok) throw new Error('Missing manual resource');
    return response;
  };
  const [manifest, index] = await Promise.all([fetchLocal('./manifest.json').then(r => r.arrayBuffer()), fetchLocal('./index.json').then(r => r.json())]);
  const bundle = await verify(manifest, index);
  el('identity').textContent = `Bamboo ${bundle.package_version} · source ${JSON.stringify(bundle.source)} · bundle ${bundle.bundle_id}`;
  function show(doc) { el('title').textContent = doc.title; el('document').textContent = doc.text; }
  function render() {
    const results = search(bundle.documents, el('query').value);
    el('results').replaceChildren();
    el('status').textContent = `${results.length} matching chapters`;
    for (const doc of results) {
      const li = document.createElement('li'), button = document.createElement('button');
      button.textContent = `${doc.title} (${doc.path})`;
      button.addEventListener('click', () => show(doc)); li.append(button); el('results').append(li);
    }
  }
  el('query').disabled = false; el('query').addEventListener('input', render); render();
  show(bundle.documents.find(doc => doc.path === 'references/index.md') ?? bundle.documents[0]);
} catch {
  el('identity').textContent = 'Manual unavailable or failed integrity verification. Use the installed bamboo-manual Skill. This site requires HTTPS or localhost and a complete matching bundle.';
}
