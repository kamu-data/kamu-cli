// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{MessageStoreWakeupDetector, OutboxMessage, OutboxMessageID};

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
        producer_name: &'static str,
        consumer_name: &'static str,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError>;

    /// Mark these messages as applied for this producer-consumer pair
    /// (should be idempotent!).
    async fn mark_applied(
        &self,
        transaction_catalog: &dill::Catalog,
        producer_name: &'static str,
        consumer_name: &'static str,
        message_ids_with_tx_ids: &[(OutboxMessageID, i64)],
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
