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

use crate::{UpdateConfigurationRule, UpdateConfigurationState};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UpdateConfigurationService: Sync + Send {
    /// Lists update configurations, which are currently enabled
    fn list_enabled_configurations(&self) -> UpdateConfigurationStateStream;

    /// Find current configuration, which may or may not be associated with the
    /// given dataset
    async fn find_configuration(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Option<UpdateConfigurationState>, FindConfigurationError>;

    /// Set or modify dataset update configuration
    async fn set_configuration(
        &self,
        dataset_id: DatasetID,
        paused: bool,
        rule: UpdateConfigurationRule,
    ) -> Result<UpdateConfigurationState, SetConfigurationError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type UpdateConfigurationStateStream<'a> = std::pin::Pin<
    Box<dyn Stream<Item = Result<UpdateConfigurationState, InternalError>> + Send + 'a>,
>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SetConfigurationError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum FindConfigurationError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<TryLoadError<UpdateConfigurationState>> for FindConfigurationError {
    fn from(value: TryLoadError<UpdateConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<TryLoadError<UpdateConfigurationState>> for SetConfigurationError {
    fn from(value: TryLoadError<UpdateConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
