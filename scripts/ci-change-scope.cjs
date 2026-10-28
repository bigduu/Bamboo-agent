#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");

function documentationOnly(files) {
  return files.length > 0 && files.every((file) =>
    /^[^/]+\.md$/u.test(file) || /^docs\/(?:[^/]+\/)*[^/]+\.md$/u.test(file));
}

function classify(base, merge) {
  assert.match(base, /^[0-9a-f]{40}$/u);
  assert.match(merge, /^[0-9a-f]{40}$/u);
  const parents = execFileSync("git", ["show", "-s", "--format=%P", merge], { encoding: "utf8" }).trim().split(" ");
  assert.equal(parents.length, 2, "Expected the pull request merge snapshot");
  assert.equal(parents[0], base, "Pull request base differs from the tested merge");
  const changed = execFileSync("git", ["diff", "--name-only", "--no-renames", "-z", base, merge, "--"], { encoding: "utf8" });
  return documentationOnly(changed.split("\0").filter(Boolean));
}

module.exports = { documentationOnly, classify };
if (require.main === module) {
  try { console.log(classify(process.argv[2], process.argv[3]) ? "true" : "false"); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
