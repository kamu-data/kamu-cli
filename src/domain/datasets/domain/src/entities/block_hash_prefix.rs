// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Prefix of a block hash in its base16 multibase form (`f1620…`),
/// matched case-insensitively
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHashPrefix(String);

impl BlockHashPrefix {
    /// Multibase code `f` (base16), then the multihash header of block
    /// hashes: the SHA3 code `0x16` and the digest length `0x20`
    const MULTIHASH_HEADER: &'static str = "f1620";

    const DIGEST_LENGTH: usize = 32;

    pub fn parse(prefix: &str) -> Result<Self, InvalidBlockHashPrefixError> {
        let normalized = prefix.to_ascii_lowercase();

        let is_valid = normalized
            .strip_prefix('f')
            .is_some_and(|digits| digits.chars().all(|c| c.is_ascii_hexdigit()));
        if !is_valid {
            return Err(InvalidBlockHashPrefixError {
                prefix: prefix.to_string(),
            });
        }

        Ok(Self(normalized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn matches(&self, block_hash: &odf::Multihash) -> bool {
        block_hash.to_string().starts_with(self.0.as_str())
    }

    /// Range of block hash digests whose hashes start with this prefix
    pub fn digest_range(&self) -> BlockHashDigestRange {
        let Some(hex_digits) = self.0.strip_prefix(Self::MULTIHASH_HEADER) else {
            return if Self::MULTIHASH_HEADER.starts_with(self.0.as_str()) {
                BlockHashDigestRange::Any
            } else {
                BlockHashDigestRange::Empty
            };
        };

        if hex_digits.is_empty() {
            return BlockHashDigestRange::Any;
        }

        // An odd number of digits fixes the high half of the last byte only
        let (padded_digits, increment) = if hex_digits.len() % 2 == 0 {
            (hex_digits.to_string(), 1)
        } else {
            (format!("{hex_digits}0"), 0x10)
        };

        if padded_digits.len() > 2 * Self::DIGEST_LENGTH {
            return BlockHashDigestRange::Empty;
        }
        let start: Vec<u8> = padded_digits
            .as_bytes()
            .chunks(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();

        BlockHashDigestRange::Bounded {
            end: Self::increment(&start, increment),
            start,
        }
    }

    /// Smallest byte string above every digest starting with `bytes`, where
    /// `increment` is added to the last byte
    fn increment(bytes: &[u8], increment: u16) -> Vec<u8> {
        let mut end = bytes.to_vec();
        let mut carry = increment;
        for byte in end.iter_mut().rev() {
            let sum = u16::from(*byte) + carry;
            *byte = u8::try_from(sum & 0xff).unwrap();
            carry = sum >> 8;
            if carry == 0 {
                return end;
            }
        }

        // The prefix is all `f`: a byte string longer than any digest bounds
        // all of them
        vec![0xff; Self::DIGEST_LENGTH + 1]
    }
}

impl std::fmt::Display for BlockHashPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Digests of the blocks a [`BlockHashPrefix`] selects
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockHashDigestRange {
    /// Every block matches
    Any,
    /// No block matches
    Empty,
    /// Digests from `start` inclusive to `end` exclusive, compared bytewise
    Bounded { start: Vec<u8>, end: Vec<u8> },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Invalid block hash prefix '{prefix}': expected base16 multibase form, e.g. 'f1620a3c9'")]
pub struct InvalidBlockHashPrefixError {
    pub prefix: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    #[test]
    fn test_parse() {
        assert_eq!(
            BlockHashPrefix::parse("F1620AB").unwrap().as_str(),
            "f1620ab"
        );
        assert_matches!(BlockHashPrefix::parse("1620ab"), Err(_));
        assert_matches!(BlockHashPrefix::parse("f1620%"), Err(_));
        assert_matches!(BlockHashPrefix::parse(""), Err(_));
    }

    #[test]
    fn test_digest_range() {
        let range = |prefix: &str| BlockHashPrefix::parse(prefix).unwrap().digest_range();

        // Within the multihash header
        assert_eq!(range("f"), BlockHashDigestRange::Any);
        assert_eq!(range("f1620"), BlockHashDigestRange::Any);
        assert_eq!(range("f1621"), BlockHashDigestRange::Empty);
        assert_eq!(range("f2"), BlockHashDigestRange::Empty);

        // Whole and half bytes
        assert_eq!(
            range("f1620ab"),
            BlockHashDigestRange::Bounded {
                start: vec![0xab],
                end: vec![0xac],
            }
        );
        assert_eq!(
            range("f1620abc"),
            BlockHashDigestRange::Bounded {
                start: vec![0xab, 0xc0],
                end: vec![0xab, 0xd0],
            }
        );

        // Carry into the previous byte
        assert_eq!(
            range("f1620abff"),
            BlockHashDigestRange::Bounded {
                start: vec![0xab, 0xff],
                end: vec![0xac, 0x00],
            }
        );
        assert_eq!(
            range("f1620abf"),
            BlockHashDigestRange::Bounded {
                start: vec![0xab, 0xf0],
                end: vec![0xac, 0x00],
            }
        );

        // No byte string of the same length is above the prefix
        assert_eq!(
            range("f1620ff"),
            BlockHashDigestRange::Bounded {
                start: vec![0xff],
                end: vec![0xff; 33],
            }
        );

        // Longer than a digest
        assert_eq!(
            range(&format!("f1620{}", "0".repeat(66))),
            BlockHashDigestRange::Empty
        );
    }

    #[test]
    fn test_range_agrees_with_matches() {
        let hash = odf::Multihash::from_digest_sha3_256(b"block");
        let hash_str = hash.to_string();

        for len in 1..=hash_str.len() {
            let prefix = BlockHashPrefix::parse(&hash_str[..len]).unwrap();
            assert!(prefix.matches(&hash));

            match prefix.digest_range() {
                BlockHashDigestRange::Any => assert!(len <= 5),
                BlockHashDigestRange::Empty => panic!("Prefix {prefix} must match"),
                BlockHashDigestRange::Bounded { start, end } => {
                    let digest = hash.digest();
                    assert!(digest >= start.as_slice() && digest < end.as_slice());
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
