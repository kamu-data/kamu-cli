// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use opendatafabric::{AccountName, DatasetHandle, DatasetRef};

use crate::{Dataset, DatasetHandleStream, DatasetRepository, GetDatasetError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub DatasetRepository {}

    #[async_trait::async_trait]
    impl DatasetRepository for DatasetRepository {
        fn is_multi_tenant(&self) -> bool;

        async fn resolve_dataset_handle_by_ref(
            &self,
            dataset_ref: &DatasetRef,
        ) -> Result<DatasetHandle, GetDatasetError>;

        fn all_dataset_handles(&self) -> DatasetHandleStream<'_>;

        fn all_dataset_handles_by_owner(&self, account_name: &AccountName) -> DatasetHandleStream<'_>;

        fn get_dataset_by_handle(&self, dataset_handle: &DatasetHandle) -> Arc<dyn Dataset>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
