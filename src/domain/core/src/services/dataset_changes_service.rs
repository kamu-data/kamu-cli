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

use crate::{AccessError, DatasetNotFoundError, RefNotFoundError};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetChangesService: Sync + Send {
    /// Computes incremental stats between two given blocks of the dataset
    async fn get_increment_between<'a>(
        &'a self,
        dataset_id: &'a DatasetID,
        old_head: Option<&'a Multihash>,
        new_head: &'a Multihash,
    ) -> Result<DatasetIntervalIncrement, GetIncrementError>;

    /// Computes incremental stats between the given block and the current head
    /// of the dataset
    async fn get_increment_since<'a>(
        &'a self,
        dataset_id: &'a DatasetID,
        old_head: Option<&'a Multihash>,
    ) -> Result<DatasetIntervalIncrement, GetIncrementError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
pub struct DatasetIntervalIncrement {
    pub num_blocks: u64,
    pub num_records: u64,
    pub updated_watermark: Option<DateTime<Utc>>,
}

impl std::ops::AddAssign for DatasetIntervalIncrement {
    fn add_assign(&mut self, rhs: Self) {
        self.num_blocks += rhs.num_blocks;
        self.num_records += rhs.num_records;
        self.updated_watermark = match self.updated_watermark {
            None => rhs.updated_watermark,
            Some(self_watermark) => match rhs.updated_watermark {
                None => Some(self_watermark),
                Some(rhs_watermark) => Some(std::cmp::max(self_watermark, rhs_watermark)),
            },
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetIncrementError {
    #[error(transparent)]
    DatasetNotFound(DatasetNotFoundError),

    #[error(transparent)]
    RefNotFound(RefNotFoundError),

    #[error(transparent)]
    Access(AccessError),

    #[error(transparent)]
    Internal(InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////
