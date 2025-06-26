// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use event_sourcing::LoadError;
use internal_error::{ErrorIntoInternal, InternalError};
use tokio_stream::Stream;

use crate::{AccountFlowFilters, FlowFilters, FlowID, FlowState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowQueryService: Sync + Send {
    /// Returns states of flows associated with a given dataset
    /// ordered by creation time from newest to oldest.
    /// Applies specified filters/pagination
    async fn list_all_flows_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        filters: FlowFilters,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsByDatasetError>;

    /// Returns initiators of flows associated with a given dataset
    /// ordered by creation time from newest to oldest.
    /// Applies specified filters/pagination
    async fn list_all_flow_initiators_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<FlowInitiatorListing, ListFlowsByDatasetError>;

    /// Returns datasets with flows associated with a given account
    /// ordered by creation time from newest to oldest.
    /// Applies specified pagination
    async fn list_all_datasets_with_flow_by_account(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Vec<odf::DatasetID>, ListFlowsByDatasetError>;

    /// Returns states of flows associated with a given account
    /// ordered by creation time from newest to oldest.
    /// Applies specified pagination
    async fn list_all_flows_by_account(
        &self,
        account_id: &odf::AccountID,
        filters: AccountFlowFilters,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsByDatasetError>;

    /// Returns states of flows associated with the given dataset IDs,
    /// ordered by creation time from newest to oldest.
    async fn list_all_flows_by_dataset_ids(
        &self,
        dataset_ids: &[&odf::DatasetID],
        filters: FlowFilters,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsByDatasetError>;

    /// Returns states of system flows associated with a given dataset
    /// ordered by creation time from newest to oldest.
    /// Applies specified filters/pagination
    async fn list_all_system_flows(
        &self,
        filters: FlowFilters,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, ListSystemFlowsError>;

    /// Returns state of all flows, whether they are system-level or
    /// dataset-bound, ordered by creation time from newest to oldest
    async fn list_all_flows(
        &self,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, ListFlowsError>;

    /// Returns current state of a given flow
    async fn get_flow(&self, flow_id: FlowID) -> Result<FlowState, GetFlowError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowStateListing<'a> {
    pub matched_stream: FlowStateStream<'a>,
    pub total_count: usize,
}

pub type FlowStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<FlowState, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowInitiatorListing<'a> {
    pub matched_stream: InitiatorsStream<'a>,
}

pub type InitiatorsStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<odf::AccountID, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Flow {flow_id} not found")]
pub struct FlowNotFoundError {
    pub flow_id: FlowID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<LoadError<FlowState>> for GetFlowError {
    fn from(value: LoadError<FlowState>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(FlowNotFoundError { flow_id: err.query }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
