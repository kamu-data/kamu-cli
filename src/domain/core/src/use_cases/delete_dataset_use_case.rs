// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{DatasetHandle, DatasetRef};

use crate::DeleteDatasetError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeleteDatasetUseCase: Send + Sync {
    async fn execute_via_ref(&self, dataset_ref: &DatasetRef) -> Result<(), DeleteDatasetError>;

    async fn execute_via_handle(
        &self,
        dataset_handle: &DatasetHandle,
    ) -> Result<(), DeleteDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
