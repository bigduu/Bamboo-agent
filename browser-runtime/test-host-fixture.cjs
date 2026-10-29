'use strict';
// Component fixture ONLY. This file is excluded from the three-file publisher.
// Existing page/routing/EOF tests supply synthetic private directories rather
// than native disk images. They mock storage admission explicitly here; their
// actual Chromium behavior does not qualify the production quota/provider.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const host = path.join(__dirname, 'host.cjs');
// Only this unpublished component loader owns the IPC barrier. Pause the real
// CDP history read, after dispatch admission, without replacing host decisions.
const readBarriers = new Map();
process.on('message', message => {
  if (message?.type === 'hold-read') {
    let release;
    const done = new Promise(resolve => { release = resolve; });
    readBarriers.set(message.token, { url: message.url, release, done, entered: false });
    process.send({ type: 'read-armed', token: message.token });
  } else if (message?.type === 'release-read') {
    readBarriers.get(message.token)?.release();
    readBarriers.delete(message.token);
    process.send({ type: 'read-released', token: message.token });
  }
});
process.on('disconnect', () => {
  for (const barrier of readBarriers.values()) barrier.release();
  readBarriers.clear();
});
const componentReadBarrier = async url => {
  for (const [token, barrier] of readBarriers) {
    if (barrier.entered || barrier.url !== url) continue;
    barrier.entered = true;
    process.send({ type: 'read-entered', token });
    await barrier.done;
    return;
  }
};
const sandbox = { require, module: { exports: {} }, process, Buffer, URL,
  setTimeout, clearTimeout, setInterval, clearInterval, componentReadBarrier };
vm.runInNewContext(fs.readFileSync(host, 'utf8') + `
  productionStorageSettings = () => null;
  prepareNativeLaunch = async () => null;
  const originalCreateChatScope = createChatScope;
  const observedContexts = new WeakSet();
  createChatScope = options => {
    const context = options.context;
    if (!observedContexts.has(context)) {
      observedContexts.add(context);
      const createSession = context.newCDPSession.bind(context);
      context.newCDPSession = async page => {
        const session = await createSession(page);
        const send = session.send.bind(session);
        session.send = async (method, ...args) => {
          if (method === 'Page.getNavigationHistory') await componentReadBarrier(page.url());
          return send(method, ...args);
        };
        return session;
      };
    }
    return originalCreateChatScope(options);
  };
  main().catch(async error => {
    process.stderr.write('synthetic host fixture failed: ' + error.message + '\\n');
    process.exitCode = 1;
    await closeHost().catch(() => {});
  });`, sandbox, { filename: host });
