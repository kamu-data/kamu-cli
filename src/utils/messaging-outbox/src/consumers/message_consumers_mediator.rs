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

use crate::MessageConsumerMetaInfo;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MessageConsumersMediator: Send + Sync {
    fn get_supported_message_types(&self) -> &[&'static str];

    fn get_consumers_meta_info(&self) -> Vec<MessageConsumerMetaInfo>;

    async fn consume_message<'a>(
        &self,
        catalog: &Catalog,
        consumer_filter: ConsumerFilter<'a>,
        message_type: &str,
        content_json: serde_json::Value,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ConsumerFilter<'a> {
    AllConsumers,
    SelectedConsumer(&'a str),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
