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
use url::Url;

use crate::{
    Dataset,
    DatasetHandleStream,
    DatasetRegistry,
    DatasetRepository,
    GetDatasetError,
    GetDatasetUrlError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub DatasetRepository {}

    #[async_trait::async_trait]
    impl DatasetRegistry for DatasetRepository {
        async fn get_dataset_url(&self, dataset_ref: &DatasetRef) -> Result<Url, GetDatasetUrlError>;
    }

    #[async_trait::async_trait]
    impl DatasetRepository for DatasetRepository {
        fn is_multi_tenant(&self) -> bool;

        async fn resolve_dataset_ref(
            &self,
            dataset_ref: &DatasetRef,
        ) -> Result<DatasetHandle, GetDatasetError>;

        fn get_all_datasets(&self) -> DatasetHandleStream<'_>;

        fn get_datasets_by_owner(&self, account_name: &AccountName) -> DatasetHandleStream<'_>;

        async fn find_dataset_by_ref(
            &self,
            dataset_ref: &DatasetRef,
        ) -> Result<Arc<dyn Dataset>, GetDatasetError>;

        fn get_dataset_by_handle(&self, dataset_handle: &DatasetHandle) -> Arc<dyn Dataset>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
