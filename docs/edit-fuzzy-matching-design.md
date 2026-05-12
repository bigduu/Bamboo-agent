# Bamboo Edit Fuzzy Matching Design

## Background

Bamboo `Edit` currently relies on exact substring matching with line-ending normalization (`LF` / `CRLF`). This is safe and predictable, but it creates a practical failure mode for LLM-driven editing:

- the model often reproduces the target block with minor whitespace drift
- exact matching then fails with `not found`
- the model may respond by shortening the `SEARCH` block or using `replace_all`
- that fallback behavior increases the risk of larger-than-intended edits

We already tightened `replace_all` and large-scope safeguards. The next step is to improve matching ergonomics without weakening safety.

---

## Goals

1. Reduce `not found` failures caused only by harmless whitespace variation.
2. Preserve Bamboo's current safety model:
   - read-before-edit
   - ambiguity rejection by default
   - touched-lines scope limits
   - patch-mode preference for larger edits
3. Keep matching behavior explainable and testable.
4. Avoid silently turning a precise edit into a broad structural rewrite.

---

## Non-goals

1. Do not support semantic AST rewriting in the first iteration.
2. Do not fuzzy-match arbitrary unrelated text blocks.
3. Do not auto-pick among several weak matches.
4. Do not apply fuzzy matching to `replace_all` in the first phase.

---

## Current State

`Edit` currently does:

- exact search in legacy mode (`old_string` / `new_string`)
- exact `SEARCH` block matching in patch mode
- `LF` / `CRLF` normalization variants
- duplicate detection with `line_number`
- rejection on ambiguity

This is implemented primarily in:

- `crates/bamboo-tools/src/tools/edit.rs`
- `crates/bamboo-tools/src/tools/file_change.rs`

---

## Design Principles

### 1. Exact-first, fuzzy-second

Matching order should be:

1. exact match
2. normalized-exact match (already present)
3. fuzzy whitespace-aware match
4. otherwise fail

This preserves current behavior for all existing exact matches.

### 2. Fuzzy match must still be unique

A fuzzy match is only acceptable if:

- there is exactly one sufficiently strong candidate, and
- any runner-up candidate is meaningfully worse, and
- the touched-lines scope still passes current safety guards

If several candidates are similarly good, return an ambiguity error.

### 3. Fuzzy matching should only forgive formatting drift

First phase fuzzy matching should tolerate:

- indentation width differences
- trailing whitespace differences
- blank-line normalization in a narrow sense
- LF/CRLF differences

It should not tolerate:

- reordered lines
- inserted or deleted non-whitespace tokens
- identifier or punctuation drift
- matching across very distant regions

---

## Proposed 3-Phase Rollout

## Phase 1: Whitespace-normalized block matching

### Scope

Apply only to patch mode first.

### Behavior

If exact matching fails for a `SEARCH` block:

1. split both `SEARCH` and candidate windows into lines
2. normalize each line by:
   - trimming trailing whitespace
   - converting tabs to a canonical representation or preserving them but comparing indentation width separately
3. compare lines after removing common indentation offset
4. require identical non-whitespace token content line-by-line

### Candidate generation

Instead of scanning every possible byte offset in the file, generate candidate windows by line span:

- if `SEARCH` block has `n` lines, compare against contiguous windows of `n` lines
- optionally also compare `n +/- 1` only if blank-line normalization is enabled, but not in Phase 1 by default

### Acceptance rule

Accept only if exactly one window satisfies:

- same line count
- same per-line non-whitespace token content
- indentation differences allowed
- no token changes

### Why patch-only first

Patch mode already encourages richer context and is the safer place to introduce fuzzy behavior.

---

## Phase 2: Legacy mode fuzzy fallback

Apply a narrower version of fuzzy matching to legacy mode, but only when:

- `replace_all == false`
- `line_number` is absent or points near a single candidate
- `old_string` spans multiple lines or is sufficiently specific

### Additional guardrails

Do not use fuzzy matching in legacy mode when:

- `old_string` is a single short line
- `old_string` is fewer than a configurable token threshold
- the best match would touch a large diff region

This avoids fuzzy-matching tiny fragments like `}` or `foo`.

---

## Phase 3: Structural matching (optional, later)

Optional future work for specific languages:

- Rust: use parser-aware block boundaries
- TS/JS: use lightweight AST node anchoring
- JSON/YAML/TOML: key-path aware edits

This should likely be separate from the generic `Edit` algorithm and may become specialized helpers rather than one universal fuzzy layer.

---

## Matching Algorithm Recommendation

## Phase 1 algorithm: normalized line fingerprint match

For each line:

- preserve original line text for replacement boundaries
- compute a comparison fingerprint:
  - remove trailing whitespace
  - convert runs of leading whitespace into an `INDENT(n)` marker or ignore exact width
  - keep interior non-whitespace characters exact

Example:

```text
"    let x = 1;   " -> fingerprint: "let x = 1;"
"\tlet x = 1;"      -> fingerprint: "let x = 1;"
```

For a block:

- compute fingerprints for all lines
- require exact equality of the fingerprint sequence
- optionally require the same count of blank lines in Phase 1

### Advantages

- simple
- deterministic
- easy to explain in errors
- low risk of false positives compared with edit-distance search

### Why not Levenshtein first

A pure edit-distance approach is harder to reason about and easier to abuse:

- multiple weakly similar blocks may appear equivalent
- punctuation/token loss may still score highly enough
- threshold tuning becomes brittle

For Bamboo, a token-preserving whitespace-normalized strategy is a better first step.

---

## Proposed Internal API Shape

Inside `edit.rs`, introduce a match mode abstraction such as:

```rust
enum MatchStrategy {
    Exact,
    NormalizedWhitespace,
}
```

And candidate collection functions like:

```rust
fn collect_exact_candidates(...)
fn collect_whitespace_normalized_candidates(...)
```

Then orchestrate with:

```rust
fn collect_candidates(...) -> Vec<ReplacementCandidate>
```

Where the exact collector runs first and fuzzy collector runs only if exact returns empty.

### Important

Do not mix exact and fuzzy candidates into one undifferentiated pool without metadata.
Add provenance such as:

```rust
enum MatchKind {
    Exact,
    NormalizedWhitespace,
}
```

This enables:

- clearer error messages
- telemetry later
- policy decisions such as “allow fuzzy only in patch mode”

---

## Safety Rules for Fuzzy Matching

1. **No fuzzy matching for `replace_all` in Phase 1 or 2**.
2. **No fuzzy matching when more than one candidate passes the threshold**.
3. **No fuzzy matching for extremely short search text**.
4. **Always apply existing touched-lines guard after replacement**.
5. **Error messages must say whether fuzzy matching was attempted**.

Example error:

```text
SEARCH content not found exactly. A whitespace-normalized match was attempted but found 2 ambiguous candidates at lines 120 and 188. Add more context.
```

---

## Error Message Strategy

We should improve errors so the model learns the right retry behavior.

### Good retry guidance

- add more surrounding lines to `SEARCH`
- prefer patch mode over `replace_all`
- use `line_number` only when the target block is known

### Avoid

- suggesting `replace_all=true` too eagerly
- vague `not found` without context

---

## Testing Plan

## Unit tests

### Exact behavior unchanged

- exact single-match still works
- duplicate exact match still rejects without `line_number`
- `line_number` still disambiguates exact duplicates

### Whitespace-normalized success cases

- different indentation width, same tokens
- tabs vs spaces in leading indentation
- trailing whitespace differences
- LF vs CRLF (already present, should remain green)

### Whitespace-normalized rejection cases

- different identifier names
- different punctuation
- missing line in middle of block
- two equally good whitespace-normalized candidates
- fuzzy match would exceed touched-lines guard

### Legacy mode restrictions

- legacy fuzzy disabled for short single-line search
- replace_all does not use fuzzy logic

## E2E tests

- patch request with indentation drift succeeds
- patch request with two whitespace-equivalent duplicate blocks returns ambiguity error
- short replace_all still rejected
- large-scope fuzzy candidate still rejected by touched-lines limit

---

## Telemetry / Observability (optional but recommended)

Return additional payload fields when fuzzy matching is introduced:

- `match_kind: exact | normalized_whitespace`
- `fuzzy_match_attempted: bool`
- `fuzzy_candidate_count: number`

This is useful for evaluating:

- how often fuzzy is needed
- whether ambiguity is common
- whether exact matching remains dominant

---

## Migration Path

### Step 1

Current step already completed:

- touched-lines real diff accounting
- stronger replace_all guardrails
- no legacy compatibility dependence on `estimated_touched_lines`

### Step 2

Implement patch-mode-only whitespace-normalized matching behind an internal feature flag or conservative default.

### Step 3

Add targeted tests and compare:

- exact success rate
- not-found rate
- ambiguity rate
- accidental large-edit rate

### Step 4

Only if metrics look good, consider legacy-mode fuzzy fallback.

---

## Recommendation Summary

The recommended next implementation is:

1. keep exact matching as primary behavior
2. add **patch-mode-only whitespace-normalized matching**
3. reject on any ambiguity
4. do not enable fuzzy for `replace_all`
5. preserve current touched-lines and read-before-edit safety guards

This gives Bamboo most of the practical UX win of Claude-style matching, without taking on the full risk of a generic fuzzy text search engine.
