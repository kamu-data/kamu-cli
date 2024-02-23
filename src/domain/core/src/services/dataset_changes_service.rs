// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use opendatafabric::{DatasetID, Multihash};
use thiserror::Error;

use crate::DatasetNotFoundError;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetChangesService: Sync + Send {
    async fn get_interval_increment(
        &self,
        dataset_id: &DatasetID,
        old_head: Option<&Multihash>,
        new_head: &Multihash,
    ) -> Result<DatasetIntervalIncrement, GetIntervalIncrementError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DatasetIntervalIncrement {
    pub num_blocks: u64,
    pub num_records: u64,
    pub updated_watermark: Option<DateTime<Utc>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetIntervalIncrementError {
    #[error(transparent)]
    DatasetNotFound(DatasetNotFoundError),

    #[error(transparent)]
    Internal(InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DummyDatasetChangesService {}

#[dill::component(pub)]
#[dill::interface(dyn DatasetChangesService)]
impl DummyDatasetChangesService {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl DatasetChangesService for DummyDatasetChangesService {
    async fn get_interval_increment(
        &self,
        _dataset_id: &DatasetID,
        _old_head: Option<&Multihash>,
        _new_head: &Multihash,
    ) -> Result<DatasetIntervalIncrement, GetIntervalIncrementError> {
        Ok(DatasetIntervalIncrement {
            num_blocks: 1,
            num_records: 12,
            updated_watermark: None,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
