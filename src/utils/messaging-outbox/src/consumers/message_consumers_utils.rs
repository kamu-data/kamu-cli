// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dill::{Builder, BuilderExt, Catalog, TypecastBuilder};
use futures::{StreamExt, TryStreamExt};
use internal_error::{InternalError, ResultIntoInternal};

use super::{
    ConsumerFilter,
    MessageConsumer,
    MessageConsumerMeta,
    MessageDeliveryMechanism,
    MessageDispatcher,
};
use crate::{Message, MessageConsumerT, MessageSubscription};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn consume_deserialized_message<TMessage: Message + 'static>(
    catalog: &Catalog,
    consumer_filter: ConsumerFilter<'_>,
    content_json: &str,
    version: u32,
) -> Result<(), InternalError> {
    tracing::debug!(content_json = %content_json, "Consuming outbox message");

    if TMessage::version() != version {
        tracing::error!(
            content_json = %content_json,
            message_version = %version,
            expected_version = %TMessage::version(),
            "Cannot consume outbox message due to version mismatch"
        );
        return Ok(());
    }
    let message = serde_json::from_str::<TMessage>(content_json).int_err()?;

    let consumers = match consumer_filter {
        ConsumerFilter::AllConsumers => all_consumers_for::<TMessage>(catalog),
        ConsumerFilter::ImmediateConsumers => immediate_consumers_for::<TMessage>(catalog),
        ConsumerFilter::SelectedConsumer(consumer_name) => {
            particular_consumers_for::<TMessage>(catalog, consumer_name)
        }
    };

    let consumption_tasks = consumers.into_iter().map(|consumer| (consumer, &message));

    futures::stream::iter(consumption_tasks)
        .map(Ok)
        .try_for_each_concurrent(/* limit */ None, |(consumer, message)| async move {
            consumer.consume_message(catalog, message).await
        })
        .await?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn all_consumers_for<TMessage: Message + 'static>(
    catalog: &Catalog,
) -> Vec<Arc<dyn MessageConsumerT<TMessage>>> {
    consumers_from_builders(
        catalog,
        catalog.builders_for::<dyn MessageConsumerT<TMessage>>(),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn immediate_consumers_for<TMessage: Message + 'static>(
    catalog: &Catalog,
) -> Vec<Arc<dyn MessageConsumerT<TMessage>>> {
    consumers_from_builders(
        catalog,
        catalog.builders_for_with_meta::<dyn MessageConsumerT<TMessage>, _>(
            |meta: &MessageConsumerMeta| meta.delivery == MessageDeliveryMechanism::Immediate,
        ),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn particular_consumers_for<TMessage: Message + 'static>(
    catalog: &Catalog,
    consumer_name: &str,
) -> Vec<Arc<dyn MessageConsumerT<TMessage>>> {
    consumers_from_builders(
        catalog,
        catalog.builders_for_with_meta::<dyn MessageConsumerT<TMessage>, _>(
            |meta: &MessageConsumerMeta| meta.consumer_name == consumer_name,
        ),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn consumers_from_builders<'a, TMessage: Message + 'static>(
    catalog: &'a Catalog,
    builders: Box<dyn Iterator<Item = TypecastBuilder<'a, dyn MessageConsumerT<TMessage>>> + 'a>,
) -> Vec<Arc<dyn MessageConsumerT<TMessage>>> {
    let mut consumers = Vec::new();
    for b in builders {
        let consumer = b.get(catalog).unwrap();
        consumers.push(consumer);
    }

    consumers
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn group_message_dispatchers_by_producer(
    dispatchers: &[Arc<dyn MessageDispatcher>],
) -> HashMap<String, Arc<dyn MessageDispatcher>> {
    let mut dispatchers_by_producers = HashMap::new();
    for dispatcher in dispatchers {
        let producer_name = dispatcher.get_producer_name();
        assert!(
            !dispatchers_by_producers.contains_key(producer_name),
            "Duplicate dispatcher for producer '{producer_name}'"
        );
        dispatchers_by_producers.insert((*producer_name).to_string(), dispatcher.clone());
    }

    dispatchers_by_producers
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn enumerate_messaging_routes(
    catalog: &Catalog,
    durability: MessageDeliveryMechanism,
) -> Vec<MessageSubscription> {
    let mut res = Vec::new();

    let all_consumer_builders = catalog.builders_for::<dyn MessageConsumer>();
    for consumer_builder in all_consumer_builders {
        let all_metadata: Vec<&MessageConsumerMeta> = consumer_builder.metadata_get_all();
        assert!(
            all_metadata.len() <= 1,
            "Multiple consumer metadata records unexpected for {}",
            consumer_builder.instance_type_name()
        );
        for metadata in all_metadata {
            if metadata.delivery == durability {
                for producer_name in metadata.feeding_producers {
                    res.push(MessageSubscription::new(
                        producer_name,
                        metadata.consumer_name,
                    ));
                }
            }
        }
    }

    res
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn group_consumers_by_producers(
    meta_info_rows: &[MessageSubscription],
) -> HashMap<String, Vec<String>> {
    let mut unique_consumers_by_producer: HashMap<String, HashSet<String>> = HashMap::new();
    for row in meta_info_rows {
        unique_consumers_by_producer
            .entry(row.producer_name.to_string())
            .and_modify(|v| {
                v.insert(row.consumer_name.to_string());
            })
            .or_insert_with(|| HashSet::from([row.consumer_name.to_string()]));
    }

    let mut res = HashMap::new();
    for (producer_name, consumer_names) in unique_consumers_by_producer {
        res.insert(producer_name, consumer_names.into_iter().collect());
    }

    res
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
