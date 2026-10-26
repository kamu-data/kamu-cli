// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use kamu_datasets::{DatasetHeadCommitter, ResolvedDataset};
use kamu_datasets_services::DatasetHeadCommitterImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Commits like `DatasetHeadCommitterImpl`, but when armed first moves `HEAD`
/// the way a concurrent writer would between the caller's read and its commit
#[derive(Default)]
pub struct RacingDatasetHeadCommitter {
    concurrent_head: Mutex<Option<odf::Multihash>>,
}

impl RacingDatasetHeadCommitter {
    /// Arms the next `commit_head` to move `HEAD` to `concurrent_head` first
    pub fn move_head_before_next_commit(&self, concurrent_head: odf::Multihash) {
        *self.concurrent_head.lock().unwrap() = Some(concurrent_head);
    }
}

#[async_trait::async_trait]
impl DatasetHeadCommitter for RacingDatasetHeadCommitter {
    async fn commit_head(
        &self,
        target: &ResolvedDataset,
        expected_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        let concurrent_head = self.concurrent_head.lock().unwrap().take();
        if let Some(concurrent_head) = concurrent_head {
            target
                .as_metadata_chain()
                .set_ref(
                    &odf::BlockRef::Head,
                    &concurrent_head,
                    odf::dataset::SetRefOpts {
                        validate_block_present: true,
                        check_ref_is: None,
                    },
                )
                .await
                .unwrap();
        }

        DatasetHeadCommitterImpl {}
            .commit_head(target, expected_head, new_head)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
