'use strict';

// This fixture runs the unmodified host and reports only synthetic download
// lifecycle metadata to its owning test over IPC.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const host = path.join(__dirname, 'host.cjs');
const sandbox = { require, module: { exports: {} }, process, Buffer, URL,
  console, setTimeout, clearTimeout, setInterval, clearInterval };
vm.runInNewContext(fs.readFileSync(host, 'utf8') + `
  const originalCreateHostRuntime = createHostRuntime;
  createHostRuntime = options => {
    options.downloadCdp.on('Browser.downloadWillBegin', event => {
      process.send({ type: 'download-start', guid: event.guid });
    });
    options.downloadCdp.on('Browser.downloadProgress', event => {
      if (event.state !== 'inProgress') {
        process.send({ type: 'download-terminal', guid: event.guid, state: event.state });
      }
    });
    return originalCreateHostRuntime(options);
  };
  main().catch(async error => {
    process.stderr.write('observed test host failed: ' + error.message + '\\n');
    process.exitCode = 1;
    await closeHost().catch(() => {});
  });`, sandbox, { filename: host });
