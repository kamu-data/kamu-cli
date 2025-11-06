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

use async_graphql::dataloader::Loader;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{Account, AccountService};
use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_core::auth;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// https://async-graphql.github.io/async-graphql/en/dataloader.html

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountEntityLoader
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountEntityLoader {
    account_service: Arc<dyn AccountService>,
}

impl AccountEntityLoader {
    pub fn new(account_service: Arc<dyn AccountService>) -> Self {
        Self { account_service }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountEntityLoader: odf::AccountID -> Account
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Loader<odf::AccountID> for AccountEntityLoader {
    type Value = Account;
    type Error = Arc<InternalError>;

    #[tracing::instrument(level = "debug", name = "Gql::AccountEntityLoader::byAccountID::load", skip_all, fields(num_keys = keys.len()))]
    async fn load(
        &self,
        keys: &[odf::AccountID],
    ) -> Result<HashMap<odf::AccountID, Self::Value>, Self::Error> {
        let key_refs: Vec<&odf::AccountID> = keys.iter().collect();

        let lookup = self.account_service.get_accounts_by_ids(&key_refs).await?;

        let mut result = HashMap::with_capacity(lookup.found.len());
        for account in lookup.found {
            result.insert(account.id.clone(), account);
        }

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountEntityLoader: odf::AccountName -> Account
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Loader<odf::AccountName> for AccountEntityLoader {
    type Value = Account;
    type Error = Arc<InternalError>;

    #[tracing::instrument(level = "debug", name = "Gql::AccountEntityLoader::byAccountName::load", skip_all, fields(num_keys = keys.len()))]
    async fn load(
        &self,
        keys: &[odf::AccountName],
    ) -> Result<HashMap<odf::AccountName, Self::Value>, Self::Error> {
        let key_refs: Vec<&odf::AccountName> = keys.iter().collect();

        let lookup = self
            .account_service
            .get_accounts_by_names(&key_refs)
            .await?;

        let mut result = HashMap::with_capacity(lookup.found.len());
        for account in lookup.found {
            result.insert(account.account_name.clone(), account);
        }

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetHandleLoader
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetHandleLoader {
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

impl DatasetHandleLoader {
    pub fn new(rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>) -> Self {
        Self {
            rebac_dataset_registry_facade,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetHandleLoader: odf::DatasetRef (Read) -> odf::DatasetHandle
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Loader<odf::DatasetRef> for DatasetHandleLoader {
    type Value = odf::DatasetHandle;
    type Error = Arc<InternalError>;

    #[tracing::instrument(level = "debug", name = "Gql::DatasetHandleLoader::byDatasetRef::load", skip_all, fields(num_keys = keys.len()))]
    async fn load(
        &self,
        keys: &[odf::DatasetRef],
    ) -> Result<HashMap<odf::DatasetRef, Self::Value>, Self::Error> {
        let dataset_refs: Vec<&odf::DatasetRef> = keys.iter().collect();

        let resolution = self
            .rebac_dataset_registry_facade
            .classify_dataset_refs_by_allowance(&dataset_refs, auth::DatasetAction::Read)
            .await
            .int_err()?;

        let mut result = HashMap::with_capacity(resolution.accessible_resolved_refs.len());
        for (dataset_ref, dataset_handle) in resolution.accessible_resolved_refs {
            result.insert(dataset_ref, dataset_handle);
        }

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
