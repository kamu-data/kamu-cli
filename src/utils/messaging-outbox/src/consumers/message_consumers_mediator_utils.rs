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

use dill::Catalog;
use internal_error::{InternalError, ResultIntoInternal};

use super::ConsumerFilter;
use crate::{Message, MessageConsumerMetaInfoRow, MessageConsumerT, MessageConsumersMediator};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "debug", skip_all, fields(%message_type, %content_json))]
pub async fn consume_deserialized_message<'a, TMessage: Message + 'static>(
    catalog: &Catalog,
    consumer_filter: ConsumerFilter<'a>,
    message_type: &str,
    content_json: serde_json::Value,
) -> Result<(), InternalError> {
    let message = serde_json::from_value::<TMessage>(content_json).int_err()?;

    // TODO: concurrent execution
    let consumers = consumers_for::<TMessage>(catalog);
    match consumer_filter {
        ConsumerFilter::AllConsumers => {
            for consumer in consumers {
                consumer.consume_message(catalog, message.clone()).await?;
            }
        }
        ConsumerFilter::SelectedConsumer(consumer_name) => {
            // TODO: dill metadata filter
            for consumer in consumers {
                if consumer.consumer_name() == consumer_name {
                    consumer.consume_message(catalog, message).await?;
                    return Ok(());
                }
            }

            tracing::warn!("Consumer {consumer_name} unresolved");
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn consumers_for<TMessage: Message + 'static>(
    catalog: &Catalog,
) -> Vec<Arc<dyn MessageConsumerT<TMessage>>> {
    let builders = catalog.builders_for::<dyn MessageConsumerT<TMessage>>();

    let mut consumers = Vec::new();
    for b in builders {
        let consumer = b.get(catalog).unwrap();
        consumers.push(consumer);
    }

    consumers
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn organize_consumers_mediators(
    mediators: &[Arc<dyn MessageConsumersMediator>],
) -> HashMap<String, Arc<dyn MessageConsumersMediator>> {
    let mut mediator_by_message_type = HashMap::new();
    for mediator in mediators {
        for message_type in mediator.get_supported_message_types() {
            assert!(
                !mediator_by_message_type.contains_key(*message_type),
                "Duplicate consumers mediator for message type '{message_type}'"
            );
            mediator_by_message_type.insert((*message_type).to_string(), mediator.clone());
        }
    }

    mediator_by_message_type
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn enumerate_all_messaging_routes(
    mediators: &[Arc<dyn MessageConsumersMediator>],
) -> Vec<MessageConsumerMetaInfoRow> {
    let mut res = Vec::new();

    for mediator in mediators {
        for meta_info in mediator.get_consumers_meta_info() {
            for record in meta_info.records {
                res.push(MessageConsumerMetaInfoRow {
                    consumer_name: meta_info.consumer_name,
                    producer_name: record.producer_name,
                    message_type: record.message_type,
                });
            }
        }
    }

    res
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn group_consumers_by_producers(
    meta_info_rows: &[MessageConsumerMetaInfoRow],
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

pub(crate) fn group_messages_by_consumer(
    meta_info_rows: &[MessageConsumerMetaInfoRow],
) -> HashMap<String, HashSet<String>> {
    let mut res: HashMap<String, HashSet<String>> = HashMap::new();

    for row in meta_info_rows {
        res.entry(row.consumer_name.to_string())
            .and_modify(|v| {
                v.insert(row.message_type.to_string());
            })
            .or_insert_with(|| HashSet::from([row.message_type.to_string()]));
    }

    res
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
