pub(super) const MAX_ENTRIES: usize = 100;
pub(super) const MAX_PATTERN_LENGTH: usize = 500;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_entries_value() {
        assert_eq!(MAX_ENTRIES, 100);
    }

    #[test]
    fn test_max_pattern_length_value() {
        assert_eq!(MAX_PATTERN_LENGTH, 500);
    }

    #[test]
    fn test_max_entries_is_usize() {
        assert!(MAX_ENTRIES > 0);
        assert!(MAX_ENTRIES <= 10000); // Reasonable upper bound
    }

    #[test]
    fn test_max_pattern_length_is_usize() {
        assert!(MAX_PATTERN_LENGTH > 0);
        assert!(MAX_PATTERN_LENGTH <= 10000); // Reasonable upper bound
    }

    #[test]
    fn test_constants_are_not_equal() {
        assert_ne!(MAX_ENTRIES, MAX_PATTERN_LENGTH);
    }

    #[test]
    fn test_max_entries_is_reasonable() {
        // Should be large enough to hold useful number of patterns
        assert!(MAX_ENTRIES >= 10);
        // But not too large to cause memory issues
        assert!(MAX_ENTRIES <= 1000);
    }

    #[test]
    fn test_max_pattern_length_is_reasonable() {
        // Should be long enough for meaningful patterns
        assert!(MAX_PATTERN_LENGTH >= 10);
        // But not too long to cause performance issues
        assert!(MAX_PATTERN_LENGTH <= 10000);
    }
}
