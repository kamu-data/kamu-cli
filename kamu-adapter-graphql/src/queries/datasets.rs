// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::queries::*;
use crate::scalars::*;

use async_graphql::*;
use kamu::domain;

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct Datasets;

#[Object]
impl Datasets {
    /// Returns dataset by its ID
    async fn by_id(&self, ctx: &Context<'_>, dataset_id: DatasetID) -> Result<Option<Dataset>> {
        let cat = ctx.data::<dill::Catalog>().unwrap();
        let dataset_reg = cat.get_one::<dyn domain::DatasetRegistry>().unwrap();
        match dataset_reg.resolve_dataset_ref(&dataset_id.as_local_ref()) {
            Ok(hdl) => Ok(Some(Dataset::new(Account::mock(), hdl))),
            Err(domain::DomainError::DoesNotExist { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // TODO: Multitenancy
    /// Returns dataset by its owner and name
    #[allow(unused_variables)]
    async fn by_owner_and_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName,
        dataset_name: DatasetName,
    ) -> Result<Option<Dataset>> {
        let account = Account::mock();
        let cat = ctx.data::<dill::Catalog>().unwrap();
        let dataset_reg = cat.get_one::<dyn domain::DatasetRegistry>().unwrap();
        match dataset_reg.resolve_dataset_ref(&dataset_name.as_local_ref()) {
            Ok(hdl) => Ok(Some(Dataset::new(Account::mock(), hdl))),
            Err(domain::DomainError::DoesNotExist { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // TODO: Multitenancy
    #[graphql(skip)]
    async fn by_account_impl(
        &self,
        ctx: &Context<'_>,
        account: Account,
        page: Option<usize>,
        per_page: usize,
    ) -> Result<DatasetConnection> {
        let cat = ctx.data::<dill::Catalog>().unwrap();
        let dataset_reg = cat.get_one::<dyn domain::DatasetRegistry>().unwrap();

        let page = page.unwrap_or(0);

        let nodes: Vec<_> = dataset_reg
            .get_all_datasets()
            .skip(page * per_page)
            .take(per_page)
            .map(|hdl| Dataset::new(account.clone(), hdl))
            .collect();

        // TODO: Slow but temporary
        let total_count = dataset_reg.get_all_datasets().count();

        Ok(DatasetConnection::new(
            nodes,
            page,
            per_page,
            Some(total_count),
        ))
    }

    /// Returns datasets belonging to the specified account
    #[allow(unused_variables)]
    async fn by_account_id(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID,
        page: Option<usize>,
        #[graphql(default = 15)] per_page: usize,
    ) -> Result<DatasetConnection> {
        let account = Account::mock();
        self.by_account_impl(ctx, account, page, per_page).await
    }

    /// Returns datasets belonging to the specified account
    #[allow(unused_variables)]
    async fn by_account_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName,
        page: Option<usize>,
        #[graphql(default = 15)] per_page: usize,
    ) -> Result<DatasetConnection> {
        let account = Account::mock();
        self.by_account_impl(ctx, account, page, per_page).await
    }
}

///////////////////////////////////////////////////////////////////////////////

page_based_connection!(Dataset, DatasetConnection, DatasetEdge);
