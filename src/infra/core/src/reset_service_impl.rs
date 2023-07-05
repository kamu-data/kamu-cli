// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_core::*;
use opendatafabric::*;

pub struct ResetServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
}

#[component(pub)]
impl ResetServiceImpl {
    pub fn new(dataset_repo: Arc<dyn DatasetRepository>) -> Self {
        Self { dataset_repo }
    }
}

#[async_trait::async_trait]
impl ResetService for ResetServiceImpl {
    async fn reset_dataset(
        &self,
        dataset_handle: &DatasetHandle,
        block_hash: &Multihash,
    ) -> Result<(), ResetError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        dataset
            .as_metadata_chain()
            .set_ref(
                &BlockRef::Head,
                block_hash,
                SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Option::None,
                },
            )
            .await?;

        Ok(())
    }
}
