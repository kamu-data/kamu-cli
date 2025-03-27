// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::InternalError;
use kamu_datasets::{
    DatasetStatistics,
    DatasetStatisticsRepository,
    DatasetStatisticsService,
    GetDatasetStatisticsError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetStatisticsServiceImpl {
    dataset_statistics_repo: Arc<dyn DatasetStatisticsRepository>,
}

#[component(pub)]
#[interface(dyn DatasetStatisticsService)]
impl DatasetStatisticsServiceImpl {
    pub fn new(dataset_statistics_repo: Arc<dyn DatasetStatisticsRepository>) -> Self {
        Self {
            dataset_statistics_repo,
        }
    }
}

#[async_trait::async_trait]
impl DatasetStatisticsService for DatasetStatisticsServiceImpl {
    async fn get_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<DatasetStatistics, InternalError> {
        match self
            .dataset_statistics_repo
            .get_dataset_statistics(dataset_id, block_ref)
            .await
        {
            Ok(statistics) => Ok(statistics),
            Err(GetDatasetStatisticsError::NotFound(_)) => Ok(DatasetStatistics::default()),
            Err(GetDatasetStatisticsError::Internal(e)) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
