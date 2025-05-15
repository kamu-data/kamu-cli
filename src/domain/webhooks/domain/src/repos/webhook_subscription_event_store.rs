// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::EventStore;
use internal_error::InternalError;
use thiserror::Error;

use crate::{
    WebhookEventType,
    WebhookSubscriptionID,
    WebhookSubscriptionLabel,
    WebhookSubscriptionState,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait WebhookSubscriptionEventStore: EventStore<WebhookSubscriptionState> {
    async fn count_subscriptions_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<usize, CountWebhookSubscriptionsError>;

    async fn list_subscription_ids_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<WebhookSubscriptionID>, ListWebhookSubscriptionsError>;

    async fn find_subscription_id_by_dataset_and_label(
        &self,
        dataset_id: &odf::DatasetID,
        label: &WebhookSubscriptionLabel,
    ) -> Result<Option<WebhookSubscriptionID>, FindWebhookSubscriptionError>;

    async fn list_enabled_subscription_ids_by_dataset_and_event_type(
        &self,
        dataset_id: &odf::DatasetID,
        event_type: &WebhookEventType,
    ) -> Result<Vec<WebhookSubscriptionID>, ListWebhookSubscriptionsError>;

    // TODO: dataset removal reaction?
    // TODO: system subscriptions?
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CountWebhookSubscriptionsError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ListWebhookSubscriptionsError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FindWebhookSubscriptionError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
