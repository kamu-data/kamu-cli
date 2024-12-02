// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, meta, Catalog};
use internal_error::InternalError;
use kamu_core::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE};
use kamu_datasets::*;
use messaging_outbox::{
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
};

use crate::MESSAGE_CONSUMER_KAMU_DATASET_KEY_BLOCKS_SERVICE;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetKeyBlocksServiceImpl {
    _dataset_key_blocks_repo: Arc<dyn DatasetKeyBlocksRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetKeyBlocksService)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_KEY_BLOCKS_SERVICE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
})]
impl DatasetKeyBlocksServiceImpl {
    pub fn new(dataset_key_blocks_repo: Arc<dyn DatasetKeyBlocksRepository>) -> Self {
        Self {
            _dataset_key_blocks_repo: dataset_key_blocks_repo,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetKeyBlocksService for DatasetKeyBlocksServiceImpl {
    // TODO
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetKeyBlocksServiceImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for DatasetKeyBlocksServiceImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetKeyBlocksServiceImpl[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        // TODO
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
