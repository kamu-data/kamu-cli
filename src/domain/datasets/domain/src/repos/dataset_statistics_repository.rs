// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::DatasetStatistics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetStatisticsRepository: Send + Sync {
    async fn has_any_stats(&self) -> Result<bool, InternalError>;

    async fn get_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<DatasetStatistics, GetDatasetStatisticsError>;

    async fn set_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        statistics: DatasetStatistics,
    ) -> Result<(), SetDatasetStatisticsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum SetDatasetStatisticsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetDatasetStatisticsError {
    #[error(transparent)]
    NotFound(#[from] DatasetStatisticsNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Statistics not found for dataset '{dataset_id}', ref '{block_ref}'")]
pub struct DatasetStatisticsNotFoundError {
    pub dataset_id: odf::DatasetID,
    pub block_ref: odf::BlockRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
