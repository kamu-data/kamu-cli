// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{GetIncrementError, MockDatasetIncrementQueryService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset whose history is `head(1)` .. `head(LENGTH)`, plus a head of a
/// history rewritten after the first block
pub(crate) struct LinearDatasetHistory;

impl LinearDatasetHistory {
    const LENGTH: u8 = 4;

    pub(crate) fn head(n: u8) -> odf::Multihash {
        assert!((1..=Self::LENGTH).contains(&n));
        odf::Multihash::from_digest_sha3_256(format!("head-{n}").as_bytes())
    }

    pub(crate) fn diverged_head() -> odf::Multihash {
        odf::Multihash::from_digest_sha3_256(b"diverged")
    }

    /// Increments within the history; any interval leaving it is invalid, as
    /// for a rewritten history
    pub(crate) fn increments() -> MockDatasetIncrementQueryService {
        let history: Vec<_> = (1..=Self::LENGTH).map(Self::head).collect();

        let mut mock = MockDatasetIncrementQueryService::new();
        mock.expect_get_increment_between()
            .returning(move |_, old_head, new_head| {
                let old_index = history.iter().position(|h| Some(h) == old_head);
                let new_index = history.iter().position(|h| h == new_head);
                match (old_index, new_index) {
                    (Some(old_index), Some(new_index)) if old_index <= new_index => {
                        Ok(odf::dataset::MetadataChainIncrementInterval {
                            num_blocks: u64::try_from(new_index - old_index).unwrap(),
                            num_records: 0,
                            updated_watermark: None,
                        })
                    }
                    _ => Err(GetIncrementError::InvalidInterval(
                        odf::dataset::InvalidIntervalError {
                            head: new_head.clone(),
                            tail: old_head.unwrap().clone(),
                        },
                    )),
                }
            });
        mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
