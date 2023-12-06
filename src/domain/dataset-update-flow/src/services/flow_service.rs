// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::LoadError;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_core::DatasetNotFoundError;
use opendatafabric::{AccountID, AccountName, DatasetID};
use tokio_stream::Stream;

use crate::{
    DatasetFlowID,
    DatasetFlowState,
    DatasetFlowType,
    SystemFlowID,
    SystemFlowState,
    SystemFlowType,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowService: Sync + Send {
    /// Runs the update main loop
    async fn run(&self) -> Result<(), InternalError>;

    /// Creates a new manual dataset flow request
    async fn request_manual_dataset_flow(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
        initiator_account_id: AccountID,
        initiator_account_name: AccountName,
    ) -> Result<DatasetFlowState, RequestFlowError>;

    /// Creates a new manual system flow request
    async fn request_manual_system_flow(
        &self,
        flow_type: SystemFlowType,
        initiator_account_id: AccountID,
        initiator_account_name: AccountName,
    ) -> Result<SystemFlowState, RequestFlowError>;

    /// Returns states of flows of certian type associated with a given dataset
    /// ordered by creation time from newest to oldest
    fn list_specific_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<DatasetFlowStateStream, ListFlowsByDatasetError>;

    /// Returns states of system flows of certian type
    /// ordered by creation time from newest to oldest
    fn list_specific_system_flows(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<SystemFlowStateStream, ListSystemFlowsError>;

    /// Returns states of flows of any type associated with a given dataset
    /// ordered by creation time from newest to oldest
    fn list_all_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetFlowStateStream, ListFlowsByDatasetError>;

    /// Returns states of system flows of any type
    /// ordered by creation time from newest to oldest
    fn list_all_system_flows(&self) -> Result<SystemFlowStateStream, ListSystemFlowsError>;

    /// Returns state of the latest flow of certain type created for the given
    /// dataset
    async fn get_last_specific_flow_by_dataset(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<DatasetFlowState>, GetLastDatasetFlowError>;

    /// Returns state of the latest sstem flow of certain type
    async fn get_last_specific_system_flow(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<Option<SystemFlowState>, GetLastSystemtFlowError>;

    /// Returns current state of a given dataset flow
    async fn get_dataset_flow(
        &self,
        flow_id: DatasetFlowID,
    ) -> Result<DatasetFlowState, GetDatasetFlowError>;

    /// Returns current state of a given system flow
    async fn get_system_flow(
        &self,
        flow_id: SystemFlowID,
    ) -> Result<SystemFlowState, GetSystemFlowError>;

    /// Attempts to cancel the given dataset flow
    async fn cancel_dataset_flow(
        &self,
        flow_id: DatasetFlowID,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<DatasetFlowState, CancelDatasetFlowError>;

    /// Attempts to cancel the given system flow
    async fn cancel_system_flow(
        &self,
        flow_id: SystemFlowID,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<SystemFlowState, CancelSystemFlowError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetFlowStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<DatasetFlowState, InternalError>> + Send + 'a>>;

pub type SystemFlowStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<SystemFlowState, InternalError>> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum RequestFlowError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum ListFlowsByDatasetError {
    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum ListSystemFlowsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum GetLastDatasetFlowError {
    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum GetLastSystemtFlowError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum GetDatasetFlowError {
    #[error(transparent)]
    NotFound(#[from] DatasetFlowNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum GetSystemFlowError {
    #[error(transparent)]
    NotFound(#[from] SystemFlowNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum CancelDatasetFlowError {
    #[error(transparent)]
    NotFound(#[from] DatasetFlowNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum CancelSystemFlowError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Dataset flow {flow_id} not found")]
pub struct DatasetFlowNotFoundError {
    pub flow_id: DatasetFlowID,
}

#[derive(thiserror::Error, Debug)]
#[error("System flow {flow_id} not found")]
pub struct SystemFlowNotFoundError {
    pub flow_id: SystemFlowID,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<LoadError<DatasetFlowState>> for GetDatasetFlowError {
    fn from(value: LoadError<DatasetFlowState>) -> Self {
        match value {
            LoadError::NotFound(err) => {
                Self::NotFound(DatasetFlowNotFoundError { flow_id: err.query })
            }
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<LoadError<DatasetFlowState>> for CancelDatasetFlowError {
    fn from(value: LoadError<DatasetFlowState>) -> Self {
        match value {
            LoadError::NotFound(err) => {
                Self::NotFound(DatasetFlowNotFoundError { flow_id: err.query })
            }
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
