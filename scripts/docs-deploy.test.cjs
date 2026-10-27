"use strict";
const assert = require("node:assert/strict");
const { test } = require("node:test");
const admission = require("./main-admission.cjs");

// Transport contracts only. Every subprocess/network port is inert; these
// fixtures never provide Source, publication or Pages authorization.
const environment = { PATH: "/inert/bin", HOME: "/inert/home", TMPDIR: "/inert/tmp", GH_TOKEN: "inert-read-token",
  BAMBOO_RELEASE_TOKEN: "unused", BAMBOO_RELEASE_SIGNING_KEY: "unused", CARGO_REGISTRY_TOKEN: "unused",
  ACTIONS_ID_TOKEN_REQUEST_TOKEN: "unused", NODE_OPTIONS: "unused" };
const noNetwork = () => assert.fail("No network request is permitted in this transport contract");

function register() {
  test("Docs2355 artifact transport preserves binary bytes on one fixed read-only repository route", async () => {
    const bytes = Buffer.from([80, 75, 3, 4, 0, 255, 128, 10]), calls = [];
    const reads = admission.createReads(environment, (command, args, options) => {
      calls.push({ command, args, options }); return { status: 0, stdout: Buffer.from(bytes) };
    }, noNetwork);
    assert.deepEqual(await reads.artifactBytes(71, bytes.length), bytes);
    assert.equal(calls.length, 1); const call = calls[0];
    assert.equal(call.command, "gh");
    assert.deepEqual(call.args, ["api", "--hostname", "github.com", "--method", "GET",
      "repos/bigduu/Bamboo-agent/actions/artifacts/71/zip"]);
    assert.equal(call.options.encoding, null); assert.equal(call.options.maxBuffer, bytes.length);
    assert.equal(call.options.timeout, 30000);
    assert.deepEqual(call.options.env, { PATH: environment.PATH, HOME: environment.HOME, TMPDIR: environment.TMPDIR,
      GH_TOKEN: environment.GH_TOKEN, GH_PROMPT_DISABLED: "1" });
  });

  test("Docs2355 artifact transport rejects invalid locators, excessive bounds and unknown read outcomes", async () => {
    let calls = 0;
    const reads = admission.createReads(environment, () => { calls++; return { status: 0, stdout: Buffer.alloc(8) }; }, noNetwork);
    for (const id of [0, -1, "1e2", "../71", "https://example.invalid", NaN, Number.MAX_SAFE_INTEGER + 1]) {
      await assert.rejects(reads.artifactBytes(id, 8));
    }
    for (const maximum of [undefined, 0, -1, "8", Infinity, 1024 ** 3 + 1]) {
      await assert.rejects(reads.artifactBytes(71, maximum));
    }
    assert.equal(calls, 0, "Invalid inputs must stop before any command");
    for (const outcome of [{ status: 1, stdout: Buffer.alloc(0) }, { error: Error("inert transport error"), status: null },
      { status: 0, stdout: Buffer.alloc(9) }]) {
      const failed = admission.createReads(environment, () => outcome, noNetwork);
      await assert.rejects(failed.artifactBytes(71, 8), /Read-only GitHub request failed|Oversized GitHub response/);
    }
  });

  test("Docs2355 public read factory adds only artifact bytes and accepts no policy or credentials", () => {
    const reads = admission.createPublicReads();
    assert.deepEqual(Object.keys(reads).sort(), ["artifactBytes", "assetBytes", "checkSuite", "jobLog", "json", "registryChecksum"]);
    for (const value of Object.values(reads)) assert.equal(typeof value, "function");
    for (const override of [null, environment, { policy: {} }, { authority: true }]) {
      assert.throws(() => admission.createPublicReads(override), /no policy, authority or credential override/);
    }
  });
}
module.exports = { register };
if (require.main === module) register();
