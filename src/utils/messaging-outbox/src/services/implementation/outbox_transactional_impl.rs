// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use internal_error::InternalError;
use time_source::SystemTimeSource;

use crate::{NewOutboxMessage, Outbox, OutboxMessageBridge};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxTransactionalImpl {
    catalog: dill::CatalogWeakRef,
    outbox_message_bridge: Arc<dyn OutboxMessageBridge>,
    time_source: Arc<dyn SystemTimeSource>,
}

#[component(pub)]
impl OutboxTransactionalImpl {
    pub fn new(
        catalog: dill::CatalogWeakRef,
        outbox_message_bridge: Arc<dyn OutboxMessageBridge>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            catalog,
            outbox_message_bridge,
            time_source,
        }
    }
}

#[async_trait::async_trait]
impl Outbox for OutboxTransactionalImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(%producer_name))]
    async fn post_message_as_json(
        &self,
        producer_name: &str,
        content_json: &serde_json::Value,
        version: u32,
    ) -> Result<(), InternalError> {
        tracing::debug!(content_json = %content_json, "Saving outbox message into database");

        let new_outbox_message = NewOutboxMessage {
            producer_name: producer_name.to_string(),
            content_json: content_json.clone(),
            occurred_on: self.time_source.now(),
            version,
        };

        self.outbox_message_bridge
            .push_message(&self.catalog.upgrade(), new_outbox_message)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
