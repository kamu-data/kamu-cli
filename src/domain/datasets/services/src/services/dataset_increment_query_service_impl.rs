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
use kamu_core::{DatasetRegistry, DatasetRegistryExt, ResolvedDataset};
use kamu_datasets::{DatasetIncrementQueryService, GetIncrementError};
use odf::dataset::MetadataChainIncrementInterval;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetIncrementQueryServiceImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetIncrementQueryService)]
impl DatasetIncrementQueryServiceImpl {
    pub fn new(dataset_registry: Arc<dyn DatasetRegistry>) -> Self {
        Self { dataset_registry }
    }

    async fn resolve_dataset_by_id(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<ResolvedDataset, GetIncrementError> {
        self.dataset_registry
            .get_dataset_by_ref(&dataset_id.as_local_ref())
            .await
            .map_err(|e| match e {
                odf::DatasetRefUnresolvedError::NotFound(e) => {
                    GetIncrementError::DatasetNotFound(e)
                }
                odf::DatasetRefUnresolvedError::Internal(e) => GetIncrementError::Internal(e),
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetIncrementQueryService for DatasetIncrementQueryServiceImpl {
    async fn get_increment_between<'a>(
        &'a self,
        dataset_id: &'a odf::DatasetID,
        old_head: Option<&'a odf::Multihash>,
        new_head: &'a odf::Multihash,
    ) -> Result<MetadataChainIncrementInterval, GetIncrementError> {
        let resolved_dataset = self.resolve_dataset_by_id(dataset_id).await?;

        use odf::dataset::MetadataChainExt;

        let increment = resolved_dataset
            .as_metadata_chain()
            .get_increment_for_interval(old_head, new_head)
            .await?;

        Ok(increment)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
