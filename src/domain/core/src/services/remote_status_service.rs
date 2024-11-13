// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::{DatasetHandle, DatasetRefRemote};
use thiserror::Error;

use crate::utils::metadata_chain_comparator::CompareChainsResult;
use crate::AccessError;
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum StatusCheckError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error("Remote dataset not found")]
    RemoteDatasetNotFound,
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

pub struct PushStatus {
    pub remote: DatasetRefRemote,
    pub check_result: Result<CompareChainsResult, StatusCheckError>,
}

pub struct DatasetPushStatuses {
    pub statuses: Vec<PushStatus>,
}

#[async_trait::async_trait]
pub trait RemoteStatusService: Send + Sync {
    /// Returns sync status of all push remotes connected with a given dataset
    async fn check_remotes_status(
        &self,
        dataset_handle: &DatasetHandle,
    ) -> Result<DatasetPushStatuses, InternalError>;
}
