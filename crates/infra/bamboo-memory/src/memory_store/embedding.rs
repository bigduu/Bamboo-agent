//! Optional semantic-retrieval seam for durable-memory recall ("L1" of the
//! memory redesign): the [`MemoryEmbedder`] interface a backend implements, plus
//! the hybrid-score building blocks ([`cosine`], [`HYBRID_COSINE_WEIGHT`],
//! [`SEMANTIC_FLOOR`]) that `lexical_bm25` combines as `bm25 + β·cosine`.
//!
//! **Embedding is OPTIONAL.** With no embedder configured, recall stays pure
//! BM25 — this seam is entirely inert (query vectors are `None`, so the cosine
//! term drops out and scoring is byte-identical to L0). Only the interface and
//! the hybrid-scoring path land here; the concrete backend (a local multilingual
//! ONNX embedder, provider-independent — or an optional hosted embedding
//! endpoint) is a deferred follow-up that just implements this trait, populates
//! `LexicalIndexItem.embedding` at index-build time, and embeds the query at
//! recall time. BM25 stays the PRIMARY term so the #61 cache-stable ordering and
//! the CJK/lexical guarantees survive; the vector term only re-ranks within that
//! and surfaces paraphrase matches lexical scoring would miss.

/// A text embedder. Backends SHOULD return a unit-normalized vector; an empty
/// vec means "no embedding available" (recall falls back to pure BM25 for that
/// item/query). Must be cheap enough to call once per query at recall time
/// (document vectors are precomputed at write/index time).
pub trait MemoryEmbedder: Send + Sync {
    /// Embed `text` into a vector. Empty return = no embedding.
    fn embed(&self, text: &str) -> Vec<f32>;
}

/// Weight (β) on the cosine term in the hybrid score `bm25 + β·cosine`.
pub const HYBRID_COSINE_WEIGHT: f64 = 1.0;

/// Minimum cosine for a NON-lexical (pure-semantic) match to be recalled — stops
/// the vector term from surfacing every embedded doc as a weak match. A doc still
/// recalls on lexical BM25 alone below this; the floor only gates cosine-only hits.
pub const SEMANTIC_FLOOR: f64 = 0.25;

/// Cosine similarity in [-1, 1]. Returns 0 for empty, mismatched-length, or
/// degenerate (zero-norm) vectors, so a missing/absent embedding is a no-op.
pub fn cosine(a: &[f32], b: &[f32]) -> f64 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0.0f64;
    let mut na = 0.0f64;
    let mut nb = 0.0f64;
    for (x, y) in a.iter().zip(b) {
        let (x, y) = (*x as f64, *y as f64);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom <= f64::EPSILON {
        0.0
    } else {
        dot / denom
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosine_identical_is_one_orthogonal_is_zero() {
        assert!((cosine(&[1.0, 0.0], &[2.0, 0.0]) - 1.0).abs() < 1e-9);
        assert!(cosine(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-9);
    }

    #[test]
    fn cosine_degenerate_and_mismatch_are_zero() {
        assert_eq!(cosine(&[], &[1.0]), 0.0);
        assert_eq!(cosine(&[1.0, 2.0], &[1.0]), 0.0);
        assert_eq!(cosine(&[0.0, 0.0], &[1.0, 1.0]), 0.0);
    }
}
