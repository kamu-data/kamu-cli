// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface};
use kamu_datasets::{DatasetReferenceUpdater, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetReferenceUpdater)]
pub struct DatasetReferenceUpdaterImpl {}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl DatasetReferenceUpdater for DatasetReferenceUpdaterImpl {
    #[tracing::instrument(name = DatasetReferenceUpdaterImpl_update_reference, level = "debug", skip_all, fields(dataset_id = %target.get_id(), %block_ref, ?expected_block_hash, %new_block_hash))]
    async fn update_reference(
        &self,
        target: &ResolvedDataset,
        block_ref: &odf::BlockRef,
        expected_block_hash: Option<&odf::Multihash>,
        new_block_hash: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        // Through the metadata chain rather than the reference service, so the
        // chain's reference cache stays coherent and the block is validated
        target
            .as_metadata_chain()
            .set_ref(
                block_ref,
                new_block_hash,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(expected_block_hash),
                },
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
