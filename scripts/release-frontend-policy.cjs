#!/usr/bin/env node

"use strict";

const fs = require("node:fs");
const path = require("node:path");

const {
  LOTUS_NEXT_PACKAGE_NAME,
  readArtifactLock,
} = require("./lotus-next-artifact.cjs");

const ROOT = path.resolve(__dirname, "..");
const DEFAULT_LOCK_PATH = path.join(
  ROOT,
  "scripts",
  "frontend-package-lock.json",
);
const LEGACY_LOTUS_PACKAGE_NAME = "@bigduu/lotus";
const LEGACY_LOTUS_VERSION = "2026.8.28";
const FROM_LOCK = "from_lock";

function normalizeInput(value, fallback, label) {
  if (value === undefined || value === null) return fallback;
  if (typeof value !== "string") {
    throw new Error(`${label} must be a string`);
  }
  return value.trim() || fallback;
}

function resolveReleaseFrontend({
  packageName,
  requestedVersion,
  lockPath = DEFAULT_LOCK_PATH,
} = {}) {
  const selectedPackage = normalizeInput(
    packageName,
    LOTUS_NEXT_PACKAGE_NAME,
    "frontend package",
  );
  const selectedVersion = normalizeInput(
    requestedVersion,
    FROM_LOCK,
    "frontend version",
  );
  const lock = readArtifactLock(lockPath);

  let packageVersion;
  let frontendName;
  if (selectedPackage === LOTUS_NEXT_PACKAGE_NAME) {
    packageVersion = lock.packageVersion;
    frontendName = "lotus-next";
  } else if (selectedPackage === LEGACY_LOTUS_PACKAGE_NAME) {
    packageVersion = LEGACY_LOTUS_VERSION;
    frontendName = "lotus";
  } else {
    throw new Error(
      `Unsupported release frontend package ${JSON.stringify(selectedPackage)}; expected ${LOTUS_NEXT_PACKAGE_NAME} or ${LEGACY_LOTUS_PACKAGE_NAME}`,
    );
  }

  if (selectedVersion !== FROM_LOCK && selectedVersion !== packageVersion) {
    throw new Error(
      `Requested frontend version ${JSON.stringify(selectedVersion)} does not match the allowed ${selectedPackage} version ${JSON.stringify(packageVersion)}`,
    );
  }

  return Object.freeze({
    packageName: selectedPackage,
    packageVersion,
    frontendName,
  });
}

function assertGithubOutputValue(label, value) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${label} must be a non-empty string`);
  }
  if (/[\r\n]/.test(value)) {
    throw new Error(`${label} must not contain a line break`);
  }
}

function formatGithubOutputs(resolution) {
  const fields = [
    ["package_name", resolution.packageName],
    ["package_version", resolution.packageVersion],
    ["frontend_name", resolution.frontendName],
  ];
  for (const [label, value] of fields) {
    assertGithubOutputValue(label, value);
  }
  return `${fields.map(([label, value]) => `${label}=${value}`).join("\n")}\n`;
}

function main() {
  const resolution = resolveReleaseFrontend({
    packageName: process.env.FRONTEND_PACKAGE,
    requestedVersion: process.env.FRONTEND_VERSION,
  });
  const output = formatGithubOutputs(resolution);

  if (process.env.GITHUB_OUTPUT) {
    fs.appendFileSync(process.env.GITHUB_OUTPUT, output, "utf8");
  } else {
    process.stdout.write(`${JSON.stringify(resolution)}\n`);
  }
  process.stderr.write(
    `Resolved release frontend ${resolution.packageName}@${resolution.packageVersion} (${resolution.frontendName})\n`,
  );
}

if (require.main === module) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  }
}

module.exports = {
  DEFAULT_LOCK_PATH,
  FROM_LOCK,
  LEGACY_LOTUS_PACKAGE_NAME,
  LEGACY_LOTUS_VERSION,
  formatGithubOutputs,
  resolveReleaseFrontend,
};
