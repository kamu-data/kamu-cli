// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::DatasetSnapshot;

use crate::{CreateDatasetFromSnapshotError, CreateDatasetResult, DatasetVisibility};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CreateDatasetFromSnapshotUseCase: Send + Sync {
    async fn execute(
        &self,
        snapshot: DatasetSnapshot,
        options: CreateDatasetFromSnapshotUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct CreateDatasetFromSnapshotUseCaseOptions {
    pub dataset_visibility: DatasetVisibility,
}

// Used primarily for tests
impl Default for CreateDatasetFromSnapshotUseCaseOptions {
    fn default() -> Self {
        Self {
            dataset_visibility: DatasetVisibility::PubliclyAvailable,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
