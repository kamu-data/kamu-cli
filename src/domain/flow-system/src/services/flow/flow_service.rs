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

use crate::{DatasetFlowType, FlowID, FlowKey, FlowState, SystemFlowType};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowService: Sync + Send {
    /// Runs the update main loop
    async fn run(&self) -> Result<(), InternalError>;

    /// Triggers the specified flow manually, unless it's already waiting
    async fn trigger_manual_flow(
        &self,
        flow_key: FlowKey,
        initiator_account_id: AccountID,
        initiator_account_name: AccountName,
    ) -> Result<FlowState, RequestFlowError>;

    /// Returns states of flows of certian type associated with a given dataset
    /// ordered by creation time from newest to oldest
    fn list_flows_by_dataset_of_type(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<FlowStateStream, ListFlowsByDatasetError>;

    /// Returns states of system flows of certian type
    /// ordered by creation time from newest to oldest
    fn list_system_flows_of_type(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<FlowStateStream, ListSystemFlowsError>;

    /// Returns states of flows of any type associated with a given dataset
    /// ordered by creation time from newest to oldest
    fn list_all_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<FlowStateStream, ListFlowsByDatasetError>;

    /// Returns states of system flows of any type
    /// ordered by creation time from newest to oldest
    fn list_all_system_flows(&self) -> Result<FlowStateStream, ListSystemFlowsError>;

    /// Returns state of the latest flow of certain type created for the given
    /// dataset
    async fn get_last_flow_by_dataset_of_type(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<FlowState>, GetLastDatasetFlowError>;

    /// Returns state of the latest sstem flow of certain type
    async fn get_last_system_flow_of_type(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<Option<FlowState>, GetLastSystemtFlowError>;

    /// Returns current state of a given flow
    async fn get_flow(&self, flow_id: FlowID) -> Result<FlowState, GetFlowError>;

    /// Attempts to cancel the given flow
    async fn cancel_flow(
        &self,
        flow_id: FlowID,
        by_account_id: AccountID,
        by_account_name: AccountName,
    ) -> Result<FlowState, CancelFlowError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type FlowStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<FlowState, InternalError>> + Send + 'a>>;

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
pub enum GetFlowError {
    #[error(transparent)]
    NotFound(#[from] FlowNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum CancelFlowError {
    #[error(transparent)]
    NotFound(#[from] FlowNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Flow {flow_id} not found")]
pub struct FlowNotFoundError {
    pub flow_id: FlowID,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<LoadError<FlowState>> for GetFlowError {
    fn from(value: LoadError<FlowState>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(FlowNotFoundError { flow_id: err.query }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<LoadError<FlowState>> for CancelFlowError {
    fn from(value: LoadError<FlowState>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(FlowNotFoundError { flow_id: err.query }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
