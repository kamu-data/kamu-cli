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
#[dill::interface(dyn DeleteDatasetRebacPropertiesUseCase)]
pub struct DeleteDatasetRebacPropertiesUseCaseImpl {
    rebac_service: Arc<dyn RebacService>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DeleteDatasetRebacPropertiesUseCase for DeleteDatasetRebacPropertiesUseCaseImpl {
    async fn execute(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), DeleteDatasetRebacPropertiesError> {
        self.rebac_service
            .delete_dataset_properties(dataset_id)
            .await
            .int_err()?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_REBAC_DATASET_PROPERTIES_SERVICE,
                RebacDatasetPropertiesMessage::deleted(dataset_id.clone()),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
