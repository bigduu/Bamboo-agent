//! Retrieval-quality layer for durable-memory recall ("档0" of the memory
//! redesign): BM25(F) scoring + CJK-aware tokenization over the per-scope
//! `lexical.json` index.
//!
//! Two problems this replaces:
//! 1. **Chinese recall was broken.** The prior query/keyword tokenizer keyed on
//!    `is_ascii_alphanumeric`, so every non-ASCII (Chinese) character was a
//!    separator — a Chinese query produced ZERO tokens and recalled nothing,
//!    even though the library is bilingual (中文 + English).
//! 2. **Naive scoring.** The prior score was a flat field-weighted token-overlap
//!    SUM with no inverse-document-frequency and no length normalization, so a
//!    common word counted as much as a rare one and long docs were over-favored.
//!
//! This is embedding-FREE by design (no hosted model needed): the LLM already
//! extracts keywords/entities per doc at write time, so a proper BM25 over those
//! fields + CJK tokenization is the high-ROI first step. A vector-cosine term can
//! be added later as an ADDITIVE score without disturbing this or the
//! cache-stable ordering (#61).
//!
//! Pure READ-path: scores the existing `lexical.json` at recall time — no index
//! rebuild, no migration, no write-path change.

use std::collections::{HashMap, HashSet};

use super::{DurableMemoryStatus, LexicalIndexItem};

// BM25 parameters (Robertson/Zaragoza defaults).
const K1: f64 = 1.2;
const B: f64 = 0.75;

// Field boosts fold the prior importance ordering (title > keywords > tags >
// entities > summary) into the term-frequency weight, preserving intent while
// gaining IDF + length normalization.
const W_TITLE: f64 = 3.0;
const W_KEYWORD: f64 = 2.5;
const W_TAG: f64 = 2.0;
const W_ENTITY: f64 = 1.5;
const W_SUMMARY: f64 = 1.0;

/// Stale docs still recall but rank below equally-relevant Active ones.
const STALE_MULTIPLIER: f64 = 0.5;

/// Minimum latin token length (drops single-char noise; keeps 2-char terms like
/// `id`, `ci`, `ws`).
const MIN_LATIN_LEN: usize = 2;

/// Tokenize text CJK-aware: each maximal run of latin/digit chars → one
/// lowercased token (>= [`MIN_LATIN_LEN`] chars); each maximal run of CJK chars →
/// overlapping character bigrams (a single-char run → that unigram). Everything
/// else is a separator. Chinese becomes searchable without whitespace while
/// English behavior is preserved. Doc fields and the query are tokenized the same
/// way so their tokens align.
pub(super) fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut latin = String::new();
    let mut cjk: Vec<char> = Vec::new();

    for ch in text.chars() {
        // CJK must be checked BEFORE is_alphanumeric (Unicode considers CJK
        // ideographs alphanumeric, which would wrongly fold them into a latin run).
        if is_cjk(ch) {
            flush_latin(&mut latin, &mut tokens);
            cjk.push(ch);
        } else if ch.is_alphanumeric() {
            flush_cjk(&mut cjk, &mut tokens);
            latin.extend(ch.to_lowercase());
        } else {
            flush_latin(&mut latin, &mut tokens);
            flush_cjk(&mut cjk, &mut tokens);
        }
    }
    flush_latin(&mut latin, &mut tokens);
    flush_cjk(&mut cjk, &mut tokens);
    tokens
}

fn flush_latin(latin: &mut String, tokens: &mut Vec<String>) {
    if latin.chars().count() >= MIN_LATIN_LEN {
        tokens.push(std::mem::take(latin));
    } else {
        latin.clear();
    }
}

fn flush_cjk(cjk: &mut Vec<char>, tokens: &mut Vec<String>) {
    match cjk.len() {
        0 => {}
        1 => tokens.push(cjk[0].to_string()),
        _ => {
            for pair in cjk.windows(2) {
                tokens.push(pair.iter().collect());
            }
        }
    }
    cjk.clear();
}

fn is_cjk(ch: char) -> bool {
    matches!(ch as u32,
        0x4E00..=0x9FFF   // CJK Unified Ideographs
        | 0x3400..=0x4DBF // Ext A
        | 0xF900..=0xFAFF // Compatibility Ideographs
        | 0x3040..=0x30FF // Hiragana + Katakana
        | 0xAC00..=0xD7AF // Hangul syllables
    )
}

struct DocBag {
    scorable: bool,
    status: DurableMemoryStatus,
    /// Field-weighted term frequency.
    tf: HashMap<String, f64>,
    /// Document length = sum of weighted term frequencies.
    dl: f64,
}

/// BM25(F) scorer over one scope's lexical index. Corpus statistics (document
/// frequency, average length) are computed once per recall over the loaded
/// `lexical.json` items; scoring is then O(query terms) per doc.
pub(super) struct Bm25Corpus {
    docs: Vec<DocBag>, // aligned index-for-index with the input items slice
    df: HashMap<String, usize>,
    avgdl: f64,
    /// Scorable (Active + Stale) document count.
    n: usize,
}

impl Bm25Corpus {
    pub(super) fn build(items: &[LexicalIndexItem]) -> Self {
        let mut docs = Vec::with_capacity(items.len());
        let mut df: HashMap<String, usize> = HashMap::new();
        let mut total_dl = 0.0;
        let mut n = 0usize;

        for item in items {
            let scorable = matches!(
                item.status,
                DurableMemoryStatus::Active | DurableMemoryStatus::Stale
            );
            if !scorable {
                // Superseded / Contradicted / Archived never recall; keep the slot
                // so `docs` stays aligned with `items` for index-based scoring.
                docs.push(DocBag {
                    scorable,
                    status: item.status,
                    tf: HashMap::new(),
                    dl: 0.0,
                });
                continue;
            }

            let mut tf: HashMap<String, f64> = HashMap::new();
            add_field(&mut tf, &item.title, W_TITLE);
            for kw in &item.keywords {
                add_field(&mut tf, kw, W_KEYWORD);
            }
            for tag in &item.tags {
                add_field(&mut tf, tag, W_TAG);
            }
            for ent in &item.entities {
                add_field(&mut tf, ent, W_ENTITY);
            }
            add_field(&mut tf, &item.summary, W_SUMMARY);

            let dl: f64 = tf.values().sum();
            for term in tf.keys() {
                *df.entry(term.clone()).or_insert(0) += 1;
            }
            total_dl += dl;
            n += 1;
            docs.push(DocBag {
                scorable,
                status: item.status,
                tf,
                dl,
            });
        }

        let avgdl = if n > 0 { total_dl / n as f64 } else { 0.0 };
        Bm25Corpus { docs, df, avgdl, n }
    }

    /// BM25(F) score of `query_tokens` against the document at `index`, or `None`
    /// if the doc is non-scorable or matches no query term. Stale docs are scaled
    /// down so an equally-relevant Active doc ranks above them.
    pub(super) fn score(&self, index: usize, query_tokens: &[String]) -> Option<f64> {
        let doc = self.docs.get(index)?;
        if !doc.scorable || self.n == 0 {
            return None;
        }

        let mut score = 0.0;
        let mut matched = false;
        let mut seen = HashSet::new();
        for token in query_tokens {
            // A term repeated in the query must not double-count.
            if !seen.insert(token.as_str()) {
                continue;
            }
            let Some(&tf) = doc.tf.get(token) else {
                continue;
            };
            let df = *self.df.get(token).unwrap_or(&0);
            if df == 0 {
                continue;
            }
            // BM25 IDF with the `+1` form → always positive (never penalizes a
            // common term below zero).
            let idf = (((self.n as f64 - df as f64 + 0.5) / (df as f64 + 0.5)) + 1.0).ln();
            let denom = tf + K1 * (1.0 - B + B * (doc.dl / self.avgdl.max(f64::EPSILON)));
            score += idf * (tf * (K1 + 1.0)) / denom.max(f64::EPSILON);
            matched = true;
        }

        if !matched {
            return None;
        }
        if matches!(doc.status, DurableMemoryStatus::Stale) {
            score *= STALE_MULTIPLIER;
        }
        // Return the raw BM25 score (deterministic f64). No coarse rounding: BM25
        // deltas between close docs can live past the 2nd decimal, and the score is
        // only used for ordering, so finer precision gives a better tie-break.
        Some(score)
    }
}

fn add_field(tf: &mut HashMap<String, f64>, text: &str, weight: f64) {
    for tok in tokenize(text) {
        *tf.entry(tok).or_insert(0.0) += weight;
    }
}

#[cfg(test)]
mod tests {
    use super::super::{DurableMemoryStatus, DurableMemoryType, LexicalIndexItem, MemoryScope};
    use super::{tokenize, Bm25Corpus};

    fn item(
        id: &str,
        status: DurableMemoryStatus,
        title: &str,
        keywords: &[&str],
    ) -> LexicalIndexItem {
        LexicalIndexItem {
            id: id.to_string(),
            title: title.to_string(),
            scope: MemoryScope::Global,
            project_key: None,
            r#type: DurableMemoryType::Reference,
            status,
            tags: Vec::new(),
            keywords: keywords.iter().map(|s| s.to_string()).collect(),
            entities: Vec::new(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            summary: String::new(),
            granularity: None,
        }
    }

    #[test]
    fn tokenize_splits_latin_words_lowercased() {
        assert_eq!(
            tokenize("Hello  World-Foo_bar"),
            vec!["hello", "world", "foo", "bar"]
        );
        // digits ride with latin; single-char latin dropped.
        assert_eq!(tokenize("gpt4 a x9"), vec!["gpt4", "x9"]);
    }

    #[test]
    fn tokenize_cjk_produces_char_bigrams() {
        // 多租户 → 多租, 租户 ; single-char run 的 → 的
        assert_eq!(tokenize("多租户"), vec!["多租", "租户"]);
        assert_eq!(tokenize("的"), vec!["的"]);
    }

    #[test]
    fn tokenize_mixes_cjk_and_latin() {
        // The prior tokenizer dropped ALL of this Chinese; now both are captured.
        assert_eq!(
            tokenize("bamboo 多租户 SDK"),
            vec!["bamboo", "多租", "租户", "sdk"]
        );
    }

    #[test]
    fn chinese_query_recalls_a_chinese_memory() {
        // The core fix: this returned nothing before (zero query tokens).
        let items = vec![
            item(
                "zh",
                DurableMemoryStatus::Active,
                "多租户隔离设计",
                &["多租户", "隔离"],
            ),
            item(
                "en",
                DurableMemoryStatus::Active,
                "unrelated english note",
                &["cache"],
            ),
        ];
        let corpus = Bm25Corpus::build(&items);
        let q = tokenize("多租户");
        assert!(!q.is_empty(), "chinese query must tokenize to non-empty");
        assert!(
            corpus.score(0, &q).is_some(),
            "chinese memory must be recalled"
        );
        assert!(
            corpus.score(1, &q).is_none(),
            "unrelated memory must not match"
        );
    }

    #[test]
    fn rarer_term_outscores_common_term_via_idf() {
        // "cache" appears in every doc (low IDF); "guardian" is rare (high IDF).
        let items = vec![
            item("a", DurableMemoryStatus::Active, "", &["cache", "guardian"]),
            item("b", DurableMemoryStatus::Active, "", &["cache"]),
            item("c", DurableMemoryStatus::Active, "", &["cache"]),
        ];
        let corpus = Bm25Corpus::build(&items);
        let rare = corpus.score(0, &tokenize("guardian")).unwrap();
        let common = corpus.score(1, &tokenize("cache")).unwrap();
        assert!(
            rare > common,
            "a rare-term match should outscore a common-term match (IDF)"
        );
    }

    #[test]
    fn title_match_outranks_keyword_only_match() {
        let items = vec![
            item("t", DurableMemoryStatus::Active, "guardian", &[]),
            item("k", DurableMemoryStatus::Active, "", &["guardian"]),
        ];
        let corpus = Bm25Corpus::build(&items);
        let title = corpus.score(0, &tokenize("guardian")).unwrap();
        let keyword = corpus.score(1, &tokenize("guardian")).unwrap();
        assert!(
            title > keyword,
            "title match ({title}) should outrank keyword-only ({keyword})"
        );
    }

    #[test]
    fn active_outranks_stale_for_equal_relevance() {
        let items = vec![
            item(
                "a",
                DurableMemoryStatus::Active,
                "guardian resume",
                &["guardian"],
            ),
            item(
                "s",
                DurableMemoryStatus::Stale,
                "guardian resume",
                &["guardian"],
            ),
        ];
        let corpus = Bm25Corpus::build(&items);
        let active = corpus.score(0, &tokenize("guardian")).unwrap();
        let stale = corpus.score(1, &tokenize("guardian")).unwrap();
        assert!(
            active > stale,
            "active ({active}) must outrank equally-relevant stale ({stale})"
        );
    }

    #[test]
    fn superseded_and_archived_never_score() {
        let items = vec![
            item(
                "x",
                DurableMemoryStatus::Superseded,
                "guardian",
                &["guardian"],
            ),
            item(
                "y",
                DurableMemoryStatus::Archived,
                "guardian",
                &["guardian"],
            ),
            item(
                "z",
                DurableMemoryStatus::Contradicted,
                "guardian",
                &["guardian"],
            ),
        ];
        let corpus = Bm25Corpus::build(&items);
        let q = tokenize("guardian");
        assert!(corpus.score(0, &q).is_none());
        assert!(corpus.score(1, &q).is_none());
        assert!(corpus.score(2, &q).is_none());
    }
}
