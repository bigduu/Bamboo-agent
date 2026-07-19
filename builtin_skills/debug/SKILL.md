---
name: debug
description: Diagnose a concrete failure, regression, crash, hang, flaky test, or incorrect runtime behavior by reproducing it, testing hypotheses, and identifying the evidence-backed root cause. Use when symptoms or failing output exist; do not use for feature implementation without a failure, general code review, or a conceptual explanation.
---

# Debug

Diagnose before changing code.

1. Capture the exact symptom, expected behavior, environment, and smallest known trigger from the request and available artifacts.
2. Reproduce the failure with the narrowest safe command. Establish whether it also occurs on the relevant clean baseline when that distinction matters.
3. Form a small set of falsifiable hypotheses. Inspect logs, state transitions, call sites, and tests to eliminate alternatives.
4. State the root cause only when evidence connects the trigger to the symptom; keep remaining hypotheses labeled as uncertain.
5. Modify files only when the user asked for a fix. Make the smallest root-cause change and verify the reproducer first, then the affected suite.

Report the symptom, root cause or best-supported hypothesis, evidence, change status, verification, and remaining risk. Preserve these facts across context compaction. When a tool fails or permission is denied, try safe evidence sources already in scope and describe what remains unverified.
