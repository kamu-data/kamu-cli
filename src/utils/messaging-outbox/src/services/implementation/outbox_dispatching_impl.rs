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

use internal_error::InternalError;

use super::{OutboxImmediateImpl, OutboxTransactionalImpl};
use crate::{MessageConsumer, MessageConsumerMeta, MessageDeliveryMechanism, Outbox};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxDispatchingImpl {
    immediate_outbox: Arc<OutboxImmediateImpl>,
    transactional_outbox: Arc<OutboxTransactionalImpl>,
    transactional_producers: HashSet<String>,
    immediate_producers: HashSet<String>,
}

#[dill::component(pub)]
impl OutboxDispatchingImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        catalog: &dill::Catalog,
        immediate_outbox: Arc<OutboxImmediateImpl>,
        transactional_outbox: Arc<OutboxTransactionalImpl>,
    ) -> Self {
        // TODO: PERF: This component is transient and will scan builder metadata on
        // every injection
        let (transactional_producers, immediate_producers) = Self::classify_message_routes(catalog);

        Self {
            immediate_outbox,
            transactional_outbox,
            transactional_producers,
            immediate_producers,
        }
    }

    fn classify_message_routes(catalog: &dill::Catalog) -> (HashSet<String>, HashSet<String>) {
        use dill::{Builder, BuilderExt};

        let mut transactional_producers = HashSet::new();
        let mut immediate_producers = HashSet::new();

        let all_consumer_builders = catalog.builders_for::<dyn MessageConsumer>();
        for consumer_builder in all_consumer_builders {
            let all_metadata: Vec<&MessageConsumerMeta> = consumer_builder.metadata_get_all();
            assert!(
                all_metadata.len() <= 1,
                "Multiple consumer metadata records unexpected for {}",
                consumer_builder.instance_type().name
            );
            for metadata in all_metadata {
                for producer_name in metadata.feeding_producers {
                    match metadata.delivery {
                        MessageDeliveryMechanism::Transactional => {
                            transactional_producers.insert((*producer_name).to_string());
                        }
                        MessageDeliveryMechanism::Immediate => {
                            immediate_producers.insert((*producer_name).to_string());
                        }
                    }
                }
            }
        }

        (transactional_producers, immediate_producers)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Outbox for OutboxDispatchingImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(%producer_name))]
    async fn post_message_as_json(
        &self,
        producer_name: &str,
        content_json: &serde_json::Value,
        version: u32,
    ) -> Result<(), InternalError> {
        tracing::debug!(content_json = %content_json, "Dispatching outbox message");

        if self.immediate_producers.contains(producer_name) {
            self.immediate_outbox
                .post_message_as_json(producer_name, content_json, version)
                .await?;
        }

        if self.transactional_producers.contains(producer_name) {
            self.transactional_outbox
                .post_message_as_json(producer_name, content_json, version)
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
