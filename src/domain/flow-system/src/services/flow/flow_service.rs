// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use chrono::{DateTime, Utc};
use event_sourcing::LoadError;
use internal_error::{ErrorIntoInternal, InternalError};
use opendatafabric::{AccountID, AccountName, DatasetID};
use tokio_stream::Stream;

use crate::{
    DatasetFlowFilters,
    DatasetFlowType,
    FlowID,
    FlowKey,
    FlowPaginationOpts,
    FlowState,
    SystemFlowFilters,
    SystemFlowType,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowService: Sync + Send {
    /// Runs the update main loop
    async fn run(&self, planned_start_time: DateTime<Utc>) -> Result<(), InternalError>;

    /// Triggers the specified flow manually, unless it's already waiting
    async fn trigger_manual_flow(
        &self,
        trigger_time: DateTime<Utc>,
        flow_key: FlowKey,
        initiator_account_id: AccountID,
        initiator_account_name: AccountName,
    ) -> Result<FlowState, RequestFlowError>;

    /// Returns states of flows associated with a given dataset
    /// ordered by creation time from newest to oldest.
    /// Applies specified filters/pagination
    fn list_all_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: DatasetFlowFilters,
        pagination: FlowPaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsByDatasetError>;

    /// Returns states of system flows associated with a given dataset
    /// ordered by creation time from newest to oldest.
    /// Applies specified filters/pagination
    fn list_all_system_flows(
        &self,
        filters: SystemFlowFilters,
        pagination: FlowPaginationOpts,
    ) -> Result<FlowStateListing, ListSystemFlowsError>;

    /// Returns state of all flows, whether they are system-level or
    /// dataset-bound, ordered by creation time from newest to oldest
    fn list_all_flows(&self) -> Result<FlowStateListing, ListFlowsError>;

    /// Returns state of the latest flow of certain type created for the given
    /// dataset
    async fn get_last_flow_by_dataset_of_type(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<FlowState>, GetLastDatasetFlowError>;

    /// Returns state of the latest system flow of certain type
    async fn get_last_system_flow_of_type(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<Option<FlowState>, GetLastSystemFlowError>;

    /// Returns current state of a given flow
    async fn get_flow(&self, flow_id: FlowID) -> Result<FlowState, GetFlowError>;

    /// Attempts to cancel the tasks already scheduled for the given flow
    async fn cancel_scheduled_tasks(
        &self,
        flow_id: FlowID,
    ) -> Result<FlowState, CancelScheduledTasksError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowStateListing<'a> {
    pub matched_stream: FlowStateStream<'a>,
    pub total_count_future: FlowCountFuture<'a>,
}

pub type FlowStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<FlowState, InternalError>> + Send + 'a>>;

pub type FlowCountFuture<'a> = std::pin::Pin<Box<dyn Future<Output = usize> + Send + 'a>>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum RequestFlowError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum ListFlowsByDatasetError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum ListSystemFlowsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum ListFlowsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum GetLastDatasetFlowError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum GetLastSystemFlowError {
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
pub enum CancelScheduledTasksError {
    #[error(transparent)]
    NotFound(#[from] FlowNotFoundError),
    #[error(transparent)]
    NotScheduled(#[from] FlowNotScheduledError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Flow {flow_id} not found")]
pub struct FlowNotFoundError {
    pub flow_id: FlowID,
}

#[derive(thiserror::Error, Debug)]
#[error("Flow {flow_id} is not scheduled yet.")]
pub struct FlowNotScheduledError {
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

impl From<LoadError<FlowState>> for CancelScheduledTasksError {
    fn from(value: LoadError<FlowState>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(FlowNotFoundError { flow_id: err.query }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FlowServiceRunConfig {
    pub awaiting_step: chrono::Duration,
}

impl FlowServiceRunConfig {
    pub fn new(awaiting_step: chrono::Duration) -> Self {
        Self { awaiting_step }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
