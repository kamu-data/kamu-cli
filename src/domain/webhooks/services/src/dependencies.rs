// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<WebhookDeliveryScheduler>();
    catalog_builder.add::<WebhookEventBuilderImpl>();
    catalog_builder.add::<WebhookDeliveryWorkerImpl>();
    catalog_builder.add::<WebhookSignerImpl>();
    catalog_builder.add::<WebhookSenderImpl>();
    catalog_builder.add::<WebhookSecretGeneratorImpl>();
    catalog_builder.add::<WebhookDatasetRemovalHandler>();

    catalog_builder.add::<CreateWebhookSubscriptionUseCaseImpl>();
    catalog_builder.add::<PauseWebhookSubscriptionUseCaseImpl>();
    catalog_builder.add::<RemoveWebhookSubscriptionUseCaseImpl>();
    catalog_builder.add::<ResumeWebhookSubscriptionUseCaseImpl>();
    catalog_builder.add::<UpdateWebhookSubscriptionUseCaseImpl>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
