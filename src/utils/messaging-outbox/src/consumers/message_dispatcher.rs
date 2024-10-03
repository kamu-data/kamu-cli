// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use dill::Catalog;
use internal_error::InternalError;

use super::consume_deserialized_message;
use crate::Message;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum ConsumerFilter<'a> {
    AllConsumers,
    ImmediateConsumers,
    SelectedConsumer(&'a str),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MessageDispatcher: Send + Sync {
    fn get_producer_name(&self) -> &'static str;

    async fn dispatch_message<'a>(
        &self,
        catalog: &Catalog,
        consumer_filter: ConsumerFilter<'a>,
        content_json: &str,
        version: u32,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_message_dispatcher<TMessage: Message + 'static>(
    catalog_builder: &mut dill::CatalogBuilder,
    producer_name: &'static str,
) {
    let dispatcher = MessageDispatcherT::<TMessage>::new(producer_name);
    catalog_builder.add_value(dispatcher);
    catalog_builder.bind::<dyn MessageDispatcher, MessageDispatcherT<TMessage>>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MessageDispatcherT<TMessage: Message + 'static> {
    producer_name: &'static str,
    _phantom: PhantomData<TMessage>,
}

impl<TMessage: Message + 'static> MessageDispatcherT<TMessage> {
    pub fn new(producer_name: &'static str) -> Self {
        Self {
            producer_name,
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<TMessage: Message + 'static> MessageDispatcher for MessageDispatcherT<TMessage> {
    fn get_producer_name(&self) -> &'static str {
        self.producer_name
    }

    async fn dispatch_message<'a>(
        &self,
        catalog: &Catalog,
        consumer_filter: ConsumerFilter<'a>,
        content_json: &str,
        version: u32,
    ) -> Result<(), InternalError> {
        consume_deserialized_message::<TMessage>(catalog, consumer_filter, content_json, version)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
