// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::OutboxMessageID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait OutboxMessageConsumptionRepository: Send + Sync {
    fn list_consumption_boundaries(&self) -> OutboxMessageConsumptionBoundariesStream<'_>;

    async fn find_consumption_boundary(
        &self,
        consumer_name: &str,
        producer_name: &str,
    ) -> Result<Option<OutboxMessageConsumptionBoundary>, InternalError>;

    async fn create_consumption_boundary(
        &self,
        boundary: OutboxMessageConsumptionBoundary,
    ) -> Result<(), CreateConsumptionBoundaryError>;

    async fn update_consumption_boundary(
        &self,
        boundary: OutboxMessageConsumptionBoundary,
    ) -> Result<(), UpdateConsumptionBoundaryError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct OutboxMessageConsumptionBoundary {
    pub producer_name: String,
    pub consumer_name: String,
    pub last_consumed_message_id: OutboxMessageID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateConsumptionBoundaryError {
    #[error(transparent)]
    DuplicateConsumptionBoundary(DuplicateConsumptionBoundaryError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error(
    "A boundary for consumer '{consumer_name}' and producer '{producer_name}' is already \
     registered"
)]
pub struct DuplicateConsumptionBoundaryError {
    pub consumer_name: String,
    pub producer_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpdateConsumptionBoundaryError {
    #[error(transparent)]
    ConsumptionBoundaryNotFound(ConsumptionBoundaryNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error(
    "A boundary for consumer '{consumer_name}' and producer '{producer_name}' is not registered"
)]
pub struct ConsumptionBoundaryNotFoundError {
    pub consumer_name: String,
    pub producer_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type OutboxMessageConsumptionBoundariesStream<'a> = std::pin::Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<OutboxMessageConsumptionBoundary, InternalError>>
            + Send
            + 'a,
    >,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
