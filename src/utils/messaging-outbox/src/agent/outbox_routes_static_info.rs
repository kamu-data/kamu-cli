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

use crate::{
    InitialConsumerBoundary,
    MessageConsumerMeta,
    MessageConsumptionMode,
    MessageDispatcher,
    MessageSubscription,
    OutboxMessageBoundary,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct OutboxRoutesStaticInfo {
    pub(crate) message_dispatchers_by_producers: HashMap<String, Arc<dyn MessageDispatcher>>,
    pub(crate) consumer_metadata_by_consumer_name: HashMap<String, MessageConsumerMeta>,
    pub(crate) all_durable_messaging_routes: Vec<MessageSubscription>,
    pub(crate) consumers_by_producers: HashMap<String, Vec<String>>,
}

impl OutboxRoutesStaticInfo {
    pub(crate) fn new(
        message_dispatchers_by_producers: HashMap<String, Arc<dyn MessageDispatcher>>,
        consumer_metadata_by_consumer_name: HashMap<String, MessageConsumerMeta>,
        all_durable_messaging_routes: Vec<MessageSubscription>,
        consumers_by_producers: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            message_dispatchers_by_producers,
            consumer_metadata_by_consumer_name,
            all_durable_messaging_routes,
            consumers_by_producers,
        }
    }

    pub(crate) fn consumer_metadata(&self, consumer_name: &str) -> &MessageConsumerMeta {
        self.consumer_metadata_by_consumer_name
            .get(consumer_name)
            .unwrap_or_else(|| panic!("No consumer metadata for consumer '{consumer_name}'"))
    }

    pub(crate) fn consumer_consumption_mode(&self, consumer_name: &str) -> MessageConsumptionMode {
        self.consumer_metadata(consumer_name).consumption_mode
    }

    pub(crate) fn initial_consumption_boundary(
        &self,
        consumer_name: &str,
        latest_produced_message_boundary_maybe: Option<OutboxMessageBoundary>,
    ) -> OutboxMessageBoundary {
        if self
            .consumer_metadata(consumer_name)
            .initial_consumer_boundary
            == InitialConsumerBoundary::Latest
            && let Some(latest_produced_message_boundary) = latest_produced_message_boundary_maybe
        {
            latest_produced_message_boundary
        } else {
            OutboxMessageBoundary::default()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
