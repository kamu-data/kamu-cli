// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common::PaginationOpts;
use dill::*;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::AccountRepository;
use kamu_auth_rebac::RebacService;
use kamu_auth_rebac_services::JOB_KAMU_REBAC_INDEXER;
use kamu_datasets::DatasetEntryRepository;

use crate::{
    DatasetResource,
    OsoResourceServiceInMem,
    UserActor,
    JOB_KAMU_AUTH_OSO_OSO_RESOURCE_SERVICE_INITIALIZATOR,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OsoResourceServiceInitializator {
    dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
    account_repository: Arc<dyn AccountRepository>,
    rebac_service: Arc<dyn RebacService>,
    oso_resource_service: Arc<OsoResourceServiceInMem>,
}

// TODO: Private Datasets: Add ReBAC-specific messages to update properties at
//       runtime
#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_AUTH_OSO_OSO_RESOURCE_SERVICE_INITIALIZATOR,
    depends_on: &[JOB_KAMU_REBAC_INDEXER],
    requires_transaction: true,
})]
impl OsoResourceServiceInitializator {
    pub fn new(
        dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
        account_repository: Arc<dyn AccountRepository>,
        rebac_service: Arc<dyn RebacService>,
        oso_resource_service: Arc<OsoResourceServiceInMem>,
    ) -> Self {
        Self {
            dataset_entry_repository,
            account_repository,
            rebac_service,
            oso_resource_service,
        }
    }
}

#[async_trait::async_trait]
impl InitOnStartup for OsoResourceServiceInitializator {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "OsoResourceServiceInitializator::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        use futures::TryStreamExt;

        // TODO: Private Datasets: use DatasetEntryService
        //       (also it removes futures dep)
        let dataset_entries = self
            .dataset_entry_repository
            .get_dataset_entries(PaginationOpts::all())
            .await
            .try_collect::<Vec<_>>()
            .await?;
        let accounts = self.account_repository.get_accounts().await.int_err()?;
        let accounts_map = accounts
            .into_iter()
            .fold(HashMap::new(), |mut acc, account| {
                acc.insert(account.id.clone(), account);
                acc
            });

        let mut dataset_resources = Vec::with_capacity(dataset_entries.len());

        for dataset_entry in dataset_entries {
            let properties = self
                .rebac_service
                .get_dataset_properties(&dataset_entry.id)
                .await
                .int_err()?;

            // TODO: Private Datasets: temporary: will be replaced by id
            let owner = accounts_map.get(&dataset_entry.owner_id).unwrap();
            let dataset_resource =
                DatasetResource::new(&owner.account_name, properties.allows_public_read);

            dataset_resources.push((dataset_entry.id.to_string(), dataset_resource));
        }

        let mut user_actors = Vec::with_capacity(accounts_map.len());

        for account in accounts_map.into_values() {
            let properties = self
                .rebac_service
                .get_account_properties(&account.id)
                .await
                .int_err()?;

            let user_actor =
                UserActor::new(account.account_name.as_str(), false, properties.is_admin);

            user_actors.push((account.id.to_string(), user_actor));
        }

        self.oso_resource_service
            .initialize(dataset_resources, user_actors)
            .await;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
