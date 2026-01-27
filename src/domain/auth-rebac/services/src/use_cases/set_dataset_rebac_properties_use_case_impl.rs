// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_auth_rebac::*;
use messaging_outbox::{Outbox, OutboxExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn SetDatasetRebacPropertiesUseCase)]
pub struct SetDatasetRebacPropertiesUseCaseImpl {
    rebac_service: Arc<dyn RebacService>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SetDatasetRebacPropertiesUseCase for SetDatasetRebacPropertiesUseCaseImpl {
    async fn execute(
        &self,
        dataset_id: &odf::DatasetID,
        properties: DatasetProperties,
    ) -> Result<(), SetDatasetRebacPropertiesError> {
        for (name, value) in [
            DatasetPropertyName::allows_public_read(properties.allows_public_read),
            DatasetPropertyName::allows_anonymous_read(properties.allows_anonymous_read),
        ] {
            self.rebac_service
                .set_dataset_property(dataset_id, name, &value)
                .await
                .int_err()?;
        }

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_REBAC_DATASET_PROPERTIES_SERVICE,
                RebacDatasetPropertiesMessage::modified(
                    dataset_id.clone(),
                    self.rebac_service
                        .get_dataset_properties(dataset_id)
                        .await
                        .int_err()?,
                ),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
