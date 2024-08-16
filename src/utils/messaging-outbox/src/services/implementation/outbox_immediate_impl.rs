// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use dill::{component, Catalog};
use internal_error::InternalError;

use crate::{group_message_dispatchers_by_producer, ConsumerFilter, MessageDispatcher, Outbox};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxImmediateImpl {
    catalog: Catalog,
    message_dispatchers_by_producers: HashMap<String, Arc<dyn MessageDispatcher>>,
    consumer_filter: ConsumerFilter<'static>,
}

#[component(pub)]
impl OutboxImmediateImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        catalog: Catalog,
        message_dispatchers: Vec<Arc<dyn MessageDispatcher>>,
        consumer_filter: ConsumerFilter<'static>,
    ) -> Self {
        Self {
            catalog,
            message_dispatchers_by_producers: group_message_dispatchers_by_producer(
                &message_dispatchers,
            ),
            consumer_filter,
        }
    }
}

#[async_trait::async_trait]
impl Outbox for OutboxImmediateImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(producer_name, content_json))]
    async fn post_message_as_json(
        &self,
        producer_name: &str,
        content_json: &serde_json::Value,
    ) -> Result<(), InternalError> {
        let maybe_dispatcher = self.message_dispatchers_by_producers.get(producer_name);
        if let Some(dispatcher) = maybe_dispatcher {
            let content_json = content_json.to_string();

            dispatcher
                .dispatch_message(&self.catalog, self.consumer_filter, &content_json)
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
