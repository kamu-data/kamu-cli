// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{EntityListing, EntityStreamer, PaginationOpts};
use dill::*;
use internal_error::ResultIntoInternal;
use kamu_accounts::{Account, AccountRepository, AccountService, AccountStream, ListAccountError};

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
    fn all_accounts(&self) -> AccountStream {
        EntityStreamer::default().into_stream(
            || async { Ok(()) },
            |_, pagination| {
                let list_fut = self.list_all_accounts(pagination);
                async { list_fut.await.int_err() }
            },
        )
    }

    async fn list_all_accounts(
        &self,
        pagination: PaginationOpts,
    ) -> Result<EntityListing<Account>, ListAccountError> {
        use futures::TryStreamExt;

        let total_count = self.account_repo.accounts_count().await.int_err()?;
        let entries = self
            .account_repo
            .get_accounts(pagination)
            .await
            .try_collect()
            .await?;

        Ok(EntityListing {
            list: entries,
            total_count,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
