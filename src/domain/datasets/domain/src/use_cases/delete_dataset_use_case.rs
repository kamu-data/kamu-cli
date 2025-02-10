// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeleteDatasetUseCase: Send + Sync {
    async fn execute_via_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<(), odf::dataset::DeleteDatasetError>;

    async fn execute_via_handle(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<(), odf::dataset::DeleteDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
