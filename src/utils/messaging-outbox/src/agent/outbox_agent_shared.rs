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

use crate::{OutboxMessage, OutboxMessageBoundary};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct UnconsumedProducerState {
    /// The earliest last processed message between this producer's consumers
    pub(crate) processed_boundary: OutboxMessageBoundary,

    /// Last processed message boundaries by each consumer
    pub(crate) consumption_boundaries_by_consumer: HashMap<String, OutboxMessageBoundary>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct ProducerConsumptionTask {
    /// Messages to consume
    pub(crate) unconsumed_messages: Vec<Arc<OutboxMessage>>,

    /// Last processed message boundaries by each consumer
    pub(crate) consumption_boundaries_by_consumer: HashMap<String, OutboxMessageBoundary>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
