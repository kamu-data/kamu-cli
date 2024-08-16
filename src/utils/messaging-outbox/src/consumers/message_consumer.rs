// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use internal_error::InternalError;

use crate::Message;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MessageConsumer: Send + Sync {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MessageConsumerT<TMessage: 'static + Message>: MessageConsumer {
    async fn consume_message(
        &self,
        target_catalog: &Catalog,
        message: &TMessage,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct MessageConsumerMeta {
    pub consumer_name: &'static str,
    pub feeding_producers: &'static [&'static str],
    pub durability: MessageConsumptionDurability,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum MessageConsumptionDurability {
    Durable,
    BestEffort,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
