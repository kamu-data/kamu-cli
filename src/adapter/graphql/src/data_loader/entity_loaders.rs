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
use kamu_datasets::{DatasetAction, DatasetRegistry, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// https://async-graphql.github.io/async-graphql/en/dataloader.html

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountEntityLoader
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Holds a *weak* reference to the catalog so that `DataLoader`'s internally
/// spawned tokio batch tasks do not keep a strong `Arc` count on the
/// transaction's `CatalogImpl` (and thus on `TransactionRefT`).  Without
/// this, a spawned batch task that outlives `schema.execute()` would
/// prevent `Arc::try_unwrap` from succeeding during `commit_transaction`.
pub struct AccountEntityLoader {
    catalog: dill::CatalogWeakRef,
}

impl AccountEntityLoader {
    pub fn new(catalog: dill::CatalogWeakRef) -> Self {
        Self { catalog }
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
        let catalog = self.catalog.upgrade();
        let account_service = catalog.get_one::<dyn AccountService>().unwrap();
        let key_refs: Vec<&odf::AccountID> = keys.iter().collect();

        let lookup = account_service
            .get_accounts_by_ids(&key_refs)
            .await
            .map_err(Arc::new)?;

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
        let catalog = self.catalog.upgrade();
        let account_service = catalog.get_one::<dyn AccountService>().unwrap();
        let key_refs: Vec<&odf::AccountName> = keys.iter().collect();

        let lookup = account_service
            .get_accounts_by_names(&key_refs)
            .await
            .map_err(Arc::new)?;

        Ok(lookup
            .found
            .into_iter()
            .map(|account| (account.account_name.clone(), account))
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetHandleLoader
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// See [`AccountEntityLoader`] for why this holds a weak catalog reference.
pub struct DatasetHandleLoader {
    catalog: dill::CatalogWeakRef,
}

impl DatasetHandleLoader {
    pub fn new(catalog: dill::CatalogWeakRef) -> Self {
        Self { catalog }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetHandleLoader: odf::DatasetRef (Read) -> odf::DatasetHandle
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Loader<odf::DatasetRef> for DatasetHandleLoader {
    type Value = odf::DatasetHandle;
    type Error = Arc<InternalError>;

    #[tracing::instrument(level = "debug", name = "Gql::DatasetHandleLoader::<odf::DatasetRef, odf::DatasetHandle>::load", skip_all, fields(num_keys = keys.len()))]
    async fn load(
        &self,
        keys: &[odf::DatasetRef],
    ) -> Result<HashMap<odf::DatasetRef, Self::Value>, Self::Error> {
        let catalog = self.catalog.upgrade();
        let rebac_facade = catalog.get_one::<dyn RebacDatasetRegistryFacade>().unwrap();
        let dataset_refs: Vec<&odf::DatasetRef> = keys.iter().collect();

        let resolution = rebac_facade
            .classify_dataset_refs_by_allowance(&dataset_refs, DatasetAction::Read)
            .await
            .int_err()
            .map_err(Arc::new)?;

        Ok(resolution.accessible_resolved_refs.into_iter().collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetHandleLoader: AccessCheckedDatasetRef(odf::DatasetRef)
//                      -> ResolvedDataset
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype::nutype(derive(AsRef, Clone, Debug, Eq, Hash, Into, PartialEq))]
pub struct AccessCheckedDatasetRef(odf::DatasetRef);

impl Loader<AccessCheckedDatasetRef> for DatasetHandleLoader {
    type Value = ResolvedDataset;
    type Error = Arc<InternalError>;

    #[tracing::instrument(level = "debug", name = "Gql::DatasetHandleLoader::byDatasetRef::load", skip_all, fields(num_keys = keys.len()))]
    async fn load(
        &self,
        keys: &[AccessCheckedDatasetRef],
    ) -> Result<HashMap<AccessCheckedDatasetRef, Self::Value>, Self::Error> {
        let catalog = self.catalog.upgrade();
        let dataset_registry = catalog.get_one::<dyn DatasetRegistry>().unwrap();
        let dataset_refs: Vec<&odf::DatasetRef> = keys.iter().map(AsRef::as_ref).collect();

        let resolution = dataset_registry
            .resolve_dataset_handles_by_refs(&dataset_refs)
            .await
            .int_err()
            .map_err(Arc::new)?;

        let futures_iter = resolution
            .resolved_handles
            .into_iter()
            .map(|(dataset_ref, dataset_handle)| {
                let dr = Arc::clone(&dataset_registry);
                async move {
                    let resolved_dataset = dr.get_dataset_by_handle(&dataset_handle).await;
                    (AccessCheckedDatasetRef::new(dataset_ref), resolved_dataset)
                }
            })
            .collect::<Vec<_>>();
        let resolved_dataset_pairs = futures::future::join_all(futures_iter).await;

        Ok(resolved_dataset_pairs.into_iter().collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
