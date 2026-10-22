'use strict';
// Component fixture ONLY. This file is excluded from the three-file publisher.
// Existing page/routing/EOF tests supply synthetic private directories rather
// than native disk images. They mock storage admission explicitly here; their
// actual Chromium behavior does not qualify the production quota/provider.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const host = path.join(__dirname, 'host.cjs');
const sandbox = { require, module: { exports: {} }, process, Buffer, URL,
  setTimeout, clearTimeout, setInterval, clearInterval };
vm.runInNewContext(fs.readFileSync(host, 'utf8') + `
  productionStorageSettings = () => null;
  main().catch(async error => {
    process.stderr.write('synthetic host fixture failed: ' + error.message + '\\n');
    process.exitCode = 1;
    await closeHost().catch(() => {});
  });`, sandbox, { filename: host });
