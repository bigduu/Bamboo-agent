//! Parallel execution strategies.

use serde::{Deserialize, Serialize};

/// Controls how parallel branches should be waited for.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ParallelWait {
    /// Wait for all branches to complete
    #[default]
    All,
    /// Wait for any branch to complete (first to finish)
    Any,
    /// Wait for at least N branches to complete
    N(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_wait_serialization() {
        let all = ParallelWait::All;
        let json = serde_json::to_string(&all).unwrap();
        assert!(json.contains("\"all\""));

        let any = ParallelWait::Any;
        let json = serde_json::to_string(&any).unwrap();
        assert!(json.contains("\"any\""));

        let n = ParallelWait::N(3);
        let json = serde_json::to_string(&n).unwrap();
        assert!(json.contains("\"N\":3") || json.contains("\"n\":3"));
    }

    #[test]
    fn test_parallel_wait_roundtrip() {
        let variants = vec![ParallelWait::All, ParallelWait::Any, ParallelWait::N(3)];
        for original in variants {
            let json = serde_json::to_string(&original).unwrap();
            let deserialized: ParallelWait = serde_json::from_str(&json).unwrap();
            assert_eq!(original, deserialized);
        }
    }
}
