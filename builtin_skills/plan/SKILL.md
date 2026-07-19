---
name: plan
description: Build an executable implementation, migration, or rollout plan from read-only repository exploration, including constraints, dependencies, ordered changes, verification, risks, and rollback points. Use when the user asks for a plan or decomposition before implementation; do not use when they want a small change implemented now, a status summary, or a general explanation.
---

# Plan

Explore without modifying files or external state.

1. Define the desired outcome, scope, non-goals, and acceptance criteria from the request.
2. Inspect repository instructions, current implementation, tests, interfaces, and related work. Resolve inexpensive factual unknowns from source before asking questions.
3. Identify dependencies, sequencing constraints, compatibility boundaries, data migrations, permissions, and rollout or rollback needs.
4. Produce ordered, implementation-ready steps. Name concrete files or components, describe the contract changed at each step, and attach verification to the step that creates the risk.
5. Call out assumptions, open decisions, and failure modes. Ask only when a missing decision would materially change the plan.

Do not edit code, claim implementation is complete, or turn the plan into hidden execution. If tools or permissions limit exploration, mark affected steps as provisional. Preserve the outcome, constraints, decisions, and remaining questions across context compaction.
