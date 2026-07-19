---
name: simplify
description: Reduce unnecessary code complexity, duplication, indirection, or abstraction while preserving observable behavior and validating equivalence. Use for simplify, cleanup, or behavior-preserving refactor requests; do not use for adding features, diagnosing an unexplained failure, broad code review without a simplification goal, or cosmetic churn without a complexity benefit.
---

# Simplify

Make the code easier to understand for the next maintainer without changing its contract.

1. Define the scoped behavior that must remain stable and read its callers, tests, error semantics, and compatibility constraints.
2. Identify concrete complexity: duplicated decisions, unnecessary layers, misleading state, premature generality, or control flow that can be made direct.
3. Prefer deletion, consolidation, and existing project patterns over a new abstraction. Keep the diff focused and avoid opportunistic cleanup.
4. Modify files only when the user asked for the refactor. For a read-only request, present the smallest behavior-preserving proposal instead.
5. Run the narrow equivalence tests first, then affected suites. Add a regression test when existing coverage cannot prove the preserved behavior.

Explain what became simpler and why, the behavior evidence, validation, and any residual risk. If the proposed simplification would alter behavior or public contracts, stop and present it as a separate decision. Respect permission failures and preserve the established behavior baseline across context compaction.
