// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::dataloader::Loader;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{Account, AccountService};
use kamu_datasets::{DatasetEntry, DatasetEntryService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// https://async-graphql.github.io/async-graphql/en/dataloader.html
pub struct EntityLoader {
    account_service: Arc<dyn AccountService>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
}

impl EntityLoader {
    pub fn new(
        account_service: Arc<dyn AccountService>,
        dataset_entry_service: Arc<dyn DatasetEntryService>,
    ) -> Self {
        Self {
            account_service,
            dataset_entry_service,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// odf::AccountID
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Loader<odf::AccountID> for EntityLoader {
    type Value = Account;
    type Error = Arc<InternalError>;

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
// odf::AccountName
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Loader<odf::AccountName> for EntityLoader {
    type Value = Account;
    type Error = Arc<InternalError>;

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
// odf::DatasetID
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Loader<odf::DatasetID> for EntityLoader {
    type Value = DatasetEntry;
    type Error = Arc<InternalError>;

    async fn load(
        &self,
        keys: &[odf::DatasetID],
    ) -> Result<HashMap<odf::DatasetID, Self::Value>, Self::Error> {
        let key_cows: Vec<Cow<odf::DatasetID>> = keys.iter().map(Cow::Borrowed).collect();

        let resolution = self
            .dataset_entry_service
            .get_multiple_entries(&key_cows)
            .await
            .int_err()?;

        let mut result = HashMap::with_capacity(resolution.resolved_entries.len());
        for entry in resolution.resolved_entries {
            result.insert(entry.id.clone(), entry);
        }

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
