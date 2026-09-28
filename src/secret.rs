//! Shared-secret comparison that does not leak the position of the first mismatching byte.

/// Compare two secrets in time that depends only on their lengths, never on their contents.
///
/// A short-circuiting `==` returns as soon as one byte differs, so a caller with many attempts
/// could learn how long a matching prefix is. This walks both inputs to the longer length and
/// folds every difference into one accumulator before deciding.
#[must_use]
pub(crate) fn secrets_equal(local: &[u8], peer: &[u8]) -> bool {
    let mut difference = local.len() ^ peer.len();
    for index in 0..local.len().max(peer.len()) {
        let a = local.get(index).copied().unwrap_or(0);
        let b = peer.get(index).copied().unwrap_or(0);
        difference |= usize::from(a ^ b);
    }
    std::hint::black_box(difference) == 0
}

#[cfg(test)]
mod tests {
    use super::secrets_equal;

    #[test]
    fn equal_secrets_match() {
        assert!(secrets_equal(b"correct horse", b"correct horse"));
        assert!(secrets_equal(b"", b""));
    }

    #[test]
    fn a_differing_last_byte_is_rejected() {
        assert!(!secrets_equal(b"correct horse", b"correct horsf"));
    }

    #[test]
    fn a_length_difference_is_rejected_even_with_a_matching_prefix() {
        assert!(!secrets_equal(b"correct horse", b"correct hors"));
        assert!(!secrets_equal(b"correct hors", b"correct horse"));
        assert!(!secrets_equal(b"", b"x"));
    }
}
