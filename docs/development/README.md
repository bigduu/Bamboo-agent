# Development Documentation

This directory collects active design documents, implementation plans, and deeper research that support Bamboo's runtime evolution.

## Purpose

Use this directory for:

- design proposals and architecture decisions
- technical specifications for upcoming features
- runtime research and comparative analysis
- implementation planning for active refactors

## Current Documents

### Active Design And Planning
- [Schedule System Redesign](schedule-system-redesign.md) - Long-term scheduler redesign covering trigger abstraction, calendar scheduling, misfire handling, run history, metrics, and migration strategy.
- [ScheduleState Refactor Plan](schedule-state-refactor-plan.md) - Focused migration plan for making `ScheduleState` authoritative while preserving `ScheduleEntry` compatibility.
- [Session-Level Model Selection Refactor Plan](session-model-refactor-plan.md) - Plan for moving model and reasoning configuration to the session level.
- [Memory Recall Architecture Refactor Plan](memory-recall-architecture-refactor-plan.md) - Detailed implementation plan for making project durable memory the primary recall layer, adding relevant memory recall, and demoting Dream to an auxiliary synthesis layer.
- [Bamboo Strengthening Roadmap and Backlog](bamboo-strengthening-roadmap-and-backlog.md) - Phased roadmap for hardening Bamboo’s runtime, governance, and capability model.
- [Memory System E2E Checklist](memory-system-e2e-checklist.md) - End-to-end validation checklist for Bamboo memory system behavior.

### Research And Comparative Analysis
See [`research/`](./research/) for deeper analysis that would clutter the main docs surface:

- [Claude Code vs Bamboo Runtime Deep Dive](research/claude-code-vs-bamboo-runtime-deep-dive.md)
- [Zenith vs Claude Code: Context Memory Comparison](research/claude-code-vs-zenith-context-memory-comparison.md)
- [Context Compression Comparison Notes](research/context-compression-comparison-notes.md)
- [Core Tools Implementation Comparison](research/core-tools-implementation-comparison.md)
- [Prompt Architecture Learning Notes](research/prompt-architecture-learning-notes.md)
- [Frontend Memory / Prompt Enhancement Assessment](research/frontend-memory-prompt-enhancement-assessment.md)
- [Memory Metrics Feasibility for Lotus](research/memory-metrics-feasibility-for-lotus.md)
- [Zenith / Bamboo Collapse Store Design](research/zenith-collapse-store-design.md)

## Usage Guidelines

When adding development documentation:

1. prefer clear, descriptive kebab-case file names
2. keep user-facing product messaging out of this directory
3. place comparative research in `research/`
4. move completed historical artifacts to `../archive/`
5. update this README when adding important new documents

## Related

- [Docs index](../README.md)
- [Archive](../archive/)
- [Guides](../guides/)
