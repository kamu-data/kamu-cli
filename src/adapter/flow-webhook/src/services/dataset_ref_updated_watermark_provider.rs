// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_datasets::{DatasetReferenceService, GetDatasetReferenceError};
use kamu_webhooks::{
    WebhookEventType,
    WebhookEventTypeCatalog,
    WebhookEventWatermarkProvider,
    WebhookSubscription,
    WebhookSubscriptionID,
    WebhookWatermark,
    WebhookWatermarkRepository,
};

use crate::DatasetRefUpdatedWatermark;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Tracks the dataset head that the receivers of dataset reference update
/// events already know
#[dill::component]
#[dill::interface(dyn WebhookEventWatermarkProvider)]
pub struct DatasetRefUpdatedWatermarkProvider {
    dataset_reference_service: Arc<dyn DatasetReferenceService>,
    watermark_repo: Arc<dyn WebhookWatermarkRepository>,
}

impl DatasetRefUpdatedWatermarkProvider {
    /// Head the receiver of the subscription already knows, when there is one
    pub async fn find_delivered_head(
        &self,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<Option<odf::Multihash>, InternalError> {
        let Some(watermark) = self
            .watermark_repo
            .find_watermark(
                subscription_id,
                &WebhookEventTypeCatalog::dataset_ref_updated(),
            )
            .await?
        else {
            return Ok(None);
        };

        let watermark = DatasetRefUpdatedWatermark::try_from(watermark)?;
        Ok(Some(watermark.head))
    }
}

#[async_trait::async_trait]
impl WebhookEventWatermarkProvider for DatasetRefUpdatedWatermarkProvider {
    fn event_type(&self) -> WebhookEventType {
        WebhookEventTypeCatalog::dataset_ref_updated()
    }

    async fn initial_watermark(
        &self,
        subscription: &WebhookSubscription,
    ) -> Result<Option<WebhookWatermark>, InternalError> {
        let Some(dataset_id) = subscription.dataset_id() else {
            return Ok(None);
        };

        match self
            .dataset_reference_service
            .get_reference(dataset_id, &odf::BlockRef::Head)
            .await
        {
            Ok(head) => Ok(Some(DatasetRefUpdatedWatermark { head }.into())),
            Err(GetDatasetReferenceError::NotFound(_)) => Ok(None),
            Err(GetDatasetReferenceError::Internal(e)) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
