---
name: review
description: Review code changes, pull requests, patches, or a scoped code area for actionable correctness, security, compatibility, and test risks with file and line evidence. Use for review or audit requests; do not use for general proofreading, feature implementation, or debugging a reported failure when the user wants a fix.
---

# Review

Review the requested scope without changing it unless the user explicitly asks for fixes.

1. Resolve the exact diff, files, revision, and repository instructions in scope.
2. Read enough surrounding code, tests, and public contracts to judge behavior rather than style in isolation.
3. Check correctness, data loss, security boundaries, concurrency, error handling, compatibility, and missing tests in proportion to risk.
4. Validate suspected problems with concrete control flow, a focused command, or another direct source of evidence. For language, library, or runtime semantics, run a minimal reproducer or consult authoritative documentation instead of relying on memory. This is a hard gate: if neither verification path is available, omit the claim or state it only as an unverified coverage limit, never as a finding. Do not report speculation as a finding.
5. Distinguish defects from documented behavior and unspecified caller expectations. Do not report a language primitive's normal semantics, a hypothetical contract, or a style preference unless the reviewed code or its callers establish that the behavior is wrong.
6. Report only actionable findings, ordered by severity. Give each finding a precise file and line, impact, trigger, and evidence. Calibrate severity to demonstrated reachability and impact; do not label a local panic or edge case critical without evidence of a critical boundary.

If no actionable findings remain, say so plainly. Always summarize validation performed and residual risks or untested paths. If tools, permissions, or missing artifacts limit coverage, identify the limit instead of implying a complete review.
