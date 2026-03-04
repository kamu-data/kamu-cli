// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{
    MessageStoreWakeupDetector,
    NewOutboxMessage,
    OutboxMessage,
    OutboxMessageBoundary,
    OutboxMessageID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait OutboxMessageBridge: Send + Sync {
    /// Provides outbox message store wakeup detector instance
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector;

    /// Pushes new message to the outbox
    async fn push_message(
        &self,
        transaction_catalog: &dill::Catalog,
        message: NewOutboxMessage,
    ) -> Result<(), InternalError>;

    /// Fetch unprocessed committed messages for all producers;
    ///   order by (tx_id,  message_id).
    /// Note: producer boundaries are mandatory
    async fn get_unprocessed_messages(
        &self,
        transaction_catalog: &dill::Catalog,
        above_boundaries_by_producer: Vec<(String, OutboxMessageBoundary)>,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError>;

    /// Fetch any messages for this producer that are above the given boundary
    /// regardless of their processing status; order by (tx_id, message_id).
    async fn get_messages_by_producer(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        above_boundary: OutboxMessageBoundary,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError>;

    /// Fetch the latest produced message boundaries for all producers
    async fn get_latest_message_boundaries_by_producer(
        &self,
        transaction_catalog: &dill::Catalog,
    ) -> Result<Vec<(String, OutboxMessageBoundary)>, InternalError>;

    /// List all registered producer-consumer pairs with their last consumed
    /// message id and tx id.
    async fn list_consumption_boundaries(
        &self,
        transaction_catalog: &dill::Catalog,
    ) -> Result<Vec<OutboxMessageConsumptionBoundary>, InternalError>;

    /// Mark this message boundary as consumed for this producer-consumer pair
    async fn mark_consumed(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &str,
        consumer_name: &str,
        boundary: OutboxMessageBoundary,
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
