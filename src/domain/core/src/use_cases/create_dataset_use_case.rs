// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{DatasetAlias, MetadataBlockTyped, Seed};

use crate::{CreateDatasetError, CreateDatasetResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CreateDatasetUseCase: Send + Sync {
    // TODO: Add "options" that specify the visibility dataset
    //
    //       Based on: Private Datasets: Update use case: Pushing a dataset
    //                 https://github.com/kamu-data/kamu-cli/issues/728
    async fn execute(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, CreateDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
