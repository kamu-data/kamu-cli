// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use dill::*;
use internal_error::InternalError;

use super::{OutboxImmediateImpl, OutboxTransactionalImpl};
use crate::{MessageConsumer, MessageConsumerMeta, MessageConsumptionDurability, Outbox};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxDispatchingImpl {
    immediate_outbox: Arc<OutboxImmediateImpl>,
    transactional_outbox: Arc<OutboxTransactionalImpl>,
    durable_producers: HashSet<String>,
    best_effort_producers: HashSet<String>,
}

#[component(pub)]
impl OutboxDispatchingImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        catalog: Catalog,
        immediate_outbox: Arc<OutboxImmediateImpl>,
        transactional_outbox: Arc<OutboxTransactionalImpl>,
    ) -> Self {
        let (durable_producers, best_effort_producers) = Self::classify_message_routes(&catalog);

        Self {
            immediate_outbox,
            transactional_outbox,
            durable_producers,
            best_effort_producers,
        }
    }

    fn classify_message_routes(catalog: &Catalog) -> (HashSet<String>, HashSet<String>) {
        let mut durable_producers = HashSet::new();
        let mut best_effort_producers = HashSet::new();

        let all_consumer_builders = catalog.builders_for::<dyn MessageConsumer>();
        for consumer_builder in all_consumer_builders {
            let all_metadata: Vec<&MessageConsumerMeta> = consumer_builder.metadata_get_all();
            assert!(
                all_metadata.len() <= 1,
                "Multiple consumer metadata records unexpected for {}",
                consumer_builder.instance_type_name()
            );
            for metadata in all_metadata {
                for producer_name in metadata.feeding_producers {
                    match metadata.durability {
                        MessageConsumptionDurability::Durable => {
                            durable_producers.insert((*producer_name).to_string());
                        }
                        MessageConsumptionDurability::BestEffort => {
                            best_effort_producers.insert((*producer_name).to_string());
                        }
                    }
                }
            }
        }

        (durable_producers, best_effort_producers)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Outbox for OutboxDispatchingImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(producer_name, content_json))]
    async fn post_message_as_json(
        &self,
        producer_name: &str,
        content_json: &serde_json::Value,
    ) -> Result<(), InternalError> {
        if self.durable_producers.contains(producer_name) {
            self.transactional_outbox
                .post_message_as_json(producer_name, content_json)
                .await?;
        }

        if self.best_effort_producers.contains(producer_name) {
            self.immediate_outbox
                .post_message_as_json(producer_name, content_json)
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
