// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface};
use kamu_datasets::{DatasetHeadCommitter, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetHeadCommitter)]
pub struct DatasetHeadCommitterImpl {}

#[async_trait::async_trait]
impl DatasetHeadCommitter for DatasetHeadCommitterImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(dataset_id = %target.get_id(), ?expected_head, %new_head))]
    async fn commit_head(
        &self,
        target: &ResolvedDataset,
        expected_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        // Through the metadata chain rather than the reference service, so the
        // chain's reference cache stays coherent and the block is validated
        target
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(expected_head),
                },
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
