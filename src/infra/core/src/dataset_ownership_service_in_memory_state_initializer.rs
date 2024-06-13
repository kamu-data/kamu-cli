// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use internal_error::InternalError;
use kamu_accounts::AuthenticationService;
use kamu_core::DatasetOwnershipService;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetOwnershipServiceInMemoryStateInitializer {
    authentication_service: Arc<dyn AuthenticationService>,
    dataset_ownership_service: Arc<dyn DatasetOwnershipService>,
}

#[component(pub)]
impl DatasetOwnershipServiceInMemoryStateInitializer {
    pub fn new(
        authentication_service: Arc<dyn AuthenticationService>,
        dataset_ownership_service: Arc<dyn DatasetOwnershipService>,
    ) -> Self {
        Self {
            authentication_service,
            dataset_ownership_service,
        }
    }

    pub async fn eager_initialization(&self) -> Result<(), InternalError> {
        self.dataset_ownership_service
            .eager_initialization(&self.authentication_service)
            .await?;

        Ok(())
    }
}
/////////////////////////////////////////////////////////////////////////////////////////
