// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::TryLoadError;
use internal_error::{ErrorIntoInternal, InternalError};
use opendatafabric::DatasetID;
use tokio_stream::Stream;

use crate::{Schedule, UpdateScheduleState};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UpdateScheduleService: Sync + Send {
    /// Lists update schedules, which are currently enabled
    fn list_enabled_schedules(&self) -> UpdateScheduleStateStream;

    /// Find current schedule, which may or may not be associated with the given
    /// dataset
    async fn find_schedule(
        &self,
        dataset_id: DatasetID,
    ) -> Result<Option<UpdateScheduleState>, FindScheduleError>;

    /// Set dataset update schedule
    async fn set_schedule(
        &self,
        dataset_id: DatasetID,
        schedule: Schedule,
    ) -> Result<UpdateScheduleState, SetScheduleError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type UpdateScheduleStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<UpdateScheduleState, InternalError>> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SetScheduleError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum FindScheduleError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<TryLoadError<UpdateScheduleState>> for FindScheduleError {
    fn from(value: TryLoadError<UpdateScheduleState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
