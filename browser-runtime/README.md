# Bamboo browser runtime

The server starts `host.cjs` in a separate Node process for each open chat browser. The host uses the pinned `playwright-core` package and Chromium headless shell; Lotus and Nova are not runtime dependencies.

For local development on macOS (Node 20 or newer):

```sh
cd browser-runtime
npm ci
./node_modules/.bin/playwright-core install --only-shell chromium
npm run test:runtime
```

`BAMBOO_BROWSER_NODE`, `BAMBOO_BROWSER_HOST_SCRIPT`, and `BAMBOO_BROWSER_EXECUTABLE` may point to absolute bundled paths. Without those overrides, Bamboo uses `node` on `PATH`, this source `host.cjs`, and Playwright's installed browser cache. `PLAYWRIGHT_BROWSERS_PATH` can select a different development browser cache. Bodhi sets all three bundled paths for the desktop app.

## Desktop runtime package

From a checked-out source commit, run `node scripts/pack-browser-runtime.cjs <native-target> <existing-output-directory>` on the target's native runner. The command fetches Node and the browser using the exact `desktop.lock.json` digests, copies the Browser Host and its npm lock, and emits `bamboo-browser-runtime-v1-<native-target>.tar.gz` plus its identity on stdout. It records the source, target, and full runtime file digest in `receipt.json`. Windows ARM64 uses native Node and the pinned Windows x64 Chromium. Linux ARM64 uses the matching Playwright Chromium archive.

This command stages an original output for feature verification or later publication. The frozen release workflow owns transport and public asset registration.
