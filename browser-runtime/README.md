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
