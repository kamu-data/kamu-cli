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

use crate::{FlowFilters, FlowID, FlowScope, FlowScopeQuery, FlowState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowQueryService: Sync + Send {
    /// Returns state of all flows, regardless of the scope,
    /// ordered by creation time from newest to oldest
    async fn list_all_flows(
        &self,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, InternalError>;

    /// Returns states of flows matching scope and other filters
    /// ordered by creation time from newest to oldest.
    /// Applies specified filters/pagination
    async fn list_scoped_flows(
        &self,
        scope_query: FlowScopeQuery,
        filters: FlowFilters,
        pagination: PaginationOpts,
    ) -> Result<FlowStateListing, InternalError>;

    /// Returns initiators of flows associated with matching scopes
    async fn list_scoped_flow_initiators(
        &self,
        scope_query: FlowScopeQuery,
    ) -> Result<FlowInitiatorListing, InternalError>;

    /// Returns subset of input scopes that have at least one flow
    async fn filter_flow_scopes_having_flows(
        &self,
        scopes: &[FlowScope],
    ) -> Result<Vec<FlowScope>, InternalError>;

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
