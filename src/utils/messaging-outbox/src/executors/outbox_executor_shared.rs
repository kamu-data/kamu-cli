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

use crate::{MessageDispatcher, MessageSubscription, OutboxMessage, OutboxMessageID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct OutboxRoutesStaticInfo {
    pub(crate) message_dispatchers_by_producers: HashMap<String, Arc<dyn MessageDispatcher>>,
    pub(crate) all_durable_messaging_routes: Vec<MessageSubscription>,
    pub(crate) consumers_by_producers: HashMap<String, Vec<String>>,
}

impl OutboxRoutesStaticInfo {
    pub(crate) fn new(
        message_dispatchers_by_producers: HashMap<String, Arc<dyn MessageDispatcher>>,
        all_durable_messaging_routes: Vec<MessageSubscription>,
        consumers_by_producers: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            message_dispatchers_by_producers,
            all_durable_messaging_routes,
            consumers_by_producers,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct UnconsumedProducerState {
    /// The earliest last processed message between this producer's consumers
    pub(crate) processed_boundary_id: OutboxMessageID,

    /// Last processed message IDs by each consumer
    pub(crate) consumption_boundaries_by_consumer: HashMap<String, OutboxMessageID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ProducerConsumptionTask {
    /// Messages to consume
    pub(crate) unconsumed_messages: Vec<OutboxMessage>,

    /// Last processed message IDs by each consumer
    pub(crate) consumption_boundaries_by_consumer: HashMap<String, OutboxMessageID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
