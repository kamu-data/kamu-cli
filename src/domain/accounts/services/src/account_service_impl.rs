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

use database_common::{EntityPageListing, EntityPageStreamer};
use dill::*;
use internal_error::ResultIntoInternal;
use kamu_accounts::{
    Account,
    AccountPageStream,
    AccountRepository,
    AccountService,
    GetAccountByIdError,
    GetAccountMapError,
};
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountServiceImpl {
    account_repo: Arc<dyn AccountRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AccountService)]
impl AccountServiceImpl {
    pub fn new(account_repo: Arc<dyn AccountRepository>) -> Self {
        Self { account_repo }
    }
}

#[async_trait::async_trait]
impl AccountService for AccountServiceImpl {
    fn all_accounts(&self) -> AccountPageStream {
        EntityPageStreamer::default().into_stream(
            || async { Ok(()) },
            move |_, pagination| async move {
                use futures::TryStreamExt;

                let total_count = self.account_repo.accounts_count().await.int_err()?;
                let entries = self
                    .account_repo
                    .get_accounts(pagination)
                    .await
                    .try_collect()
                    .await?;

                Ok(EntityPageListing {
                    list: entries,
                    total_count,
                })
            },
        )
    }

    async fn get_account_map(
        &self,
        account_ids: Vec<odf::AccountID>,
    ) -> Result<HashMap<odf::AccountID, Account>, GetAccountMapError> {
        let account_map = match self.account_repo.get_accounts_by_ids(account_ids).await {
            Ok(accounts) => {
                let map = accounts
                    .into_iter()
                    .fold(HashMap::new(), |mut acc, account| {
                        acc.insert(account.id.clone(), account);
                        acc
                    });
                Ok(map)
            }
            Err(err) => match err {
                GetAccountByIdError::NotFound(_) => Ok(HashMap::new()),
                e => Err(e),
            },
        }
        .int_err()?;

        Ok(account_map)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
