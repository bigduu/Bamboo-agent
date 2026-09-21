# Retrieval-window rollout evidence v1

This report is generated deterministically from privacy-safe aggregate JSONL. It contains no prompts, answers, queries, messages, memory, tool arguments/results, file paths, or provider payloads.

## Evidence identity

- Schema: `1`
- Corpus: `retrieval-window-synthetic-v1`
- Models: `deterministic-fixture-v1`
- Providers: `offline`
- Configs: `retrieval-window-opt-in-v1`, `summary-default-v1`
- Sources: synthetic aggregate
- Paired scenario count: `6`
- Aggregate sample count: `12`
- Query classes: `cjk_longer`, `cjk_two_character`, `latin`, `mixed`, `negative`, `path_punctuation`
- Evidence kinds: `historical_fact`, `identifier`, `prior_decision`, `tool_argument`, `tool_result`

## Deterministic search/index evidence

This is production-shaped index evidence from [Bamboo #1156](https://github.com/bigduu/Bamboo-agent/issues/1156) / PR #1157, not model/task-quality evidence.

- Migrated messages: `5,001`; schema-v5 rebuild: `42,936 us`.
- Live database bytes: `1,511,424 -> 2,772,992`; sampled peak: `4,779,272`.
- 5,000-message Latin infix warm latency p50/p95/p99: `1,094 / 1,440 / 1,653 us`.
- Bounded two-character literal fallback p50/p95/p99: `4,342 / 4,699 / 4,972 us`.

## Paired aggregate outcomes

| Metric | summary | retrieval_window |
|---|---:|---:|
| Samples | 6 | 6 |
| Task completion | 3/6 (50.0%) | 6/6 (100.0%) |
| Exact recovery | 3/6 (50.0%) | 6/6 (100.0%) |
| Provider/model calls | 16 | 22 |
| Tool rounds | 8 | 16 |
| Retrieval calls / hits | 0 / 0 | 12 / 5 |
| Retrieval hit rate | unavailable | 41.7% |
| Read-around calls | 0 | 5 |
| Retrieval truncations | 0 | 1 |
| Overflow signals | 1 | 0 |
| User restatements | 3 | 0 |
| Prompt input tokens | 105000 | 111100 |
| Output tokens | 5300 | 5860 |
| Cache read tokens | 69000 | 71300 |
| Cache creation tokens | 17300 | 19000 |
| Cache write tokens | unavailable | unavailable |
| Cached fraction | 65.7% | 64.2% |
| Task latency p50/p95/p99 ms | 1800 / 2800 / 2800 | 2000 / 3100 / 3100 |
| Retrieval latency p50/p95/p99 ms | unavailable | 140 / 210 / 210 |
| Context epochs / restarts / cache boundaries | 16 / 3 / 11 | 16 / 3 / 11 |

Paired delta (`retrieval_window - summary`): provider/model calls `+6`, tool rounds `+8`.

## Existing `session_history_current` tool metrics

Live `metrics.db` observations: unavailable. This report does not treat unavailable calls, success, or latency as zero. The runtime continues to use the existing `tool_call_metrics` writer; no parallel token-log writer was added.

## Default-strategy decision

**Decision: Keep `summary` as the default and keep `retrieval_window` opt-in.**

The checked evidence is synthetic aggregate coverage, not representative real-model task-quality evidence; it cannot justify a default promotion.
