// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{DatasetAlias, MetadataBlockTyped, Seed};

use crate::{CreateDatasetError, CreateDatasetResult, DatasetVisibility};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CreateDatasetUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct CreateDatasetUseCaseOptions {
    pub dataset_visibility: DatasetVisibility,
}

// Used primarily for tests
impl Default for CreateDatasetUseCaseOptions {
    fn default() -> Self {
        Self {
            dataset_visibility: DatasetVisibility::Public,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
