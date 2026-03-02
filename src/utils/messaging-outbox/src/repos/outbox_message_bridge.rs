// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{MessageStoreWakeupDetector, OutboxMessage, OutboxMessageBoundary, OutboxMessageID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait OutboxMessageBridge: Send + Sync {
    /// Provides outbox message store wakeup detector instance
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector;

    /// Fetch next batch for the given producer-consumer pair;
    ///  order by global id.
    async fn fetch_next_batch(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        consumer_name: &str,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError>;

    /// List all registered producer-consumer pairs with their last consumed
    /// message id and tx id.
    fn list_consumption_boundaries(
        &self,
        transaction_catalog: &dill::Catalog,
    ) -> OutboxMessageConsumptionBoundariesStream<'_>;

    /// Mark these messages as applied for this producer-consumer pair
    /// (should be idempotent!).
    async fn mark_applied(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        consumer_name: &str,
        message_ids_with_tx_ids: &[(OutboxMessageID, i64)],
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct OutboxMessageConsumptionBoundary {
    pub producer_name: String,
    pub consumer_name: String,
    pub last_consumed_message_id: OutboxMessageID,
    pub last_tx_id: i64,
}

impl OutboxMessageConsumptionBoundary {
    pub fn boundary(&self) -> OutboxMessageBoundary {
        OutboxMessageBoundary {
            message_id: self.last_consumed_message_id,
            tx_id: self.last_tx_id,
        }
    }
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
