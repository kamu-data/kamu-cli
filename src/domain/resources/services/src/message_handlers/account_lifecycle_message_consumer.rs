// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Catalog, component, interface, meta};
use internal_error::InternalError;
use kamu_accounts::{AccountLifecycleMessage, MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE};
use kamu_resources::{
    DeleteAccountResourcesUseCase,
    MESSAGE_CONSUMER_KAMU_RESOURCE_ACCOUNT_LIFECYCLE_HANDLER,
};
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<AccountLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_RESOURCE_ACCOUNT_LIFECYCLE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct AccountLifecycleMessageConsumer {
    delete_account_resources_use_case: Arc<dyn DeleteAccountResourcesUseCase>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for AccountLifecycleMessageConsumer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<AccountLifecycleMessage> for AccountLifecycleMessageConsumer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "AccountLifecycleMessageConsumer[AccountLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _target_catalog: &Catalog,
        message: &AccountLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received account lifecycle message");

        match message {
            AccountLifecycleMessage::Deleted(message) => {
                self.delete_account_resources_use_case
                    .execute(message.account_id.clone())
                    .await
            }
            AccountLifecycleMessage::Created(_)
            | AccountLifecycleMessage::Updated(_)
            | AccountLifecycleMessage::PasswordChanged(_) => Ok(()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
