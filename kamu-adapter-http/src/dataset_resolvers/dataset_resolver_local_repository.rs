// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use kamu::domain::{Dataset, GetDatasetError, LocalDatasetRepository};
use opendatafabric::{DatasetName, DatasetRefLocal};
use url::Url;

use super::DatasetResolver;

pub struct DatasetResolverLocalRepository {
    local_repo: Arc<dyn LocalDatasetRepository>,
}

impl DatasetResolverLocalRepository {
    pub fn new(local_repo: Arc<dyn LocalDatasetRepository>) -> DatasetResolverLocalRepository {
        DatasetResolverLocalRepository { local_repo }
    }
}

#[async_trait]
impl DatasetResolver for DatasetResolverLocalRepository {
    async fn resolve_dataset(
        &self,
        dataset_name: &str,
        base_external_url: Url,
    ) -> Result<Arc<dyn Dataset>, GetDatasetError> {
        let dataset_url_suffix = String::from(dataset_name) + "/";
        let dataset_url = base_external_url.join(dataset_url_suffix.as_str()).unwrap();

        let dataset_ref: DatasetRefLocal =
            DatasetRefLocal::Name(DatasetName::from_str(dataset_name).unwrap());
        self.local_repo
            .get_dataset_with_external_url(&dataset_ref, dataset_url)
            .await
    }
}
