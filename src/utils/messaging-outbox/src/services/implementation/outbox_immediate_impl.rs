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

use dill::{component, interface, Catalog};
use internal_error::InternalError;

use crate::{organize_consumers_mediators, ConsumerFilter, MessageConsumersMediator, Outbox};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxImmediateImpl {
    catalog: Catalog,
    consumers_mediator_per_message_type: HashMap<String, Arc<dyn MessageConsumersMediator>>,
}

#[component(pub)]
#[interface(dyn Outbox)]
impl OutboxImmediateImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        catalog: Catalog,
        message_consumer_mediators: Vec<Arc<dyn MessageConsumersMediator>>,
    ) -> Self {
        Self {
            catalog,
            consumers_mediator_per_message_type: organize_consumers_mediators(
                &message_consumer_mediators,
            ),
        }
    }
}

#[async_trait::async_trait]
impl Outbox for OutboxImmediateImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(event_type, content_json))]
    async fn post_message_as_json(
        &self,
        _publisher_name: &str,
        message_type: &str,
        content_json: serde_json::Value,
    ) -> Result<(), InternalError> {
        let maybe_consumers_mediator = self.consumers_mediator_per_message_type.get(message_type);
        if let Some(consumers_mediator) = maybe_consumers_mediator {
            consumers_mediator
                .consume_message(
                    &self.catalog,
                    ConsumerFilter::AllConsumers,
                    message_type,
                    content_json.clone(),
                )
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
