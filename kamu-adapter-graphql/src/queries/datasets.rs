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
use crate::utils::*;

use async_graphql::*;
use futures::TryStreamExt;
use kamu::domain;
use kamu::domain::LocalDatasetRepositoryExt;

///////////////////////////////////////////////////////////////////////////////

pub struct Datasets;

#[Object]
impl Datasets {
    const DEFAULT_PER_PAGE: usize = 15;

    /// Returns dataset by its ID
    async fn by_id(&self, ctx: &Context<'_>, dataset_id: DatasetID) -> Result<Option<Dataset>> {
        let local_repo = from_catalog::<dyn domain::LocalDatasetRepository>(ctx).unwrap();
        let hdl = local_repo
            .try_resolve_dataset_ref(&dataset_id.as_local_ref())
            .await?;
        Ok(hdl.map(|h| Dataset::new(Account::mock(), h)))
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
        let local_repo = from_catalog::<dyn domain::LocalDatasetRepository>(ctx).unwrap();
        let hdl = local_repo
            .try_resolve_dataset_ref(&dataset_name.as_local_ref())
            .await?;
        Ok(hdl.map(|h| Dataset::new(Account::mock(), h)))
    }

    // TODO: Multitenancy
    #[graphql(skip)]
    async fn by_account_impl(
        &self,
        ctx: &Context<'_>,
        account: Account,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<DatasetConnection> {
        let local_repo = from_catalog::<dyn domain::LocalDatasetRepository>(ctx).unwrap();

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let mut all_datasets: Vec<_> = local_repo.get_all_datasets().try_collect().await?;
        let total_count = all_datasets.len();
        all_datasets.sort_by(|a, b| a.name.cmp(&b.name));

        let nodes = all_datasets
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .map(|hdl| Dataset::new(account.clone(), hdl))
            .collect();

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
        per_page: Option<usize>,
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
        per_page: Option<usize>,
    ) -> Result<DatasetConnection> {
        let account = Account::mock();
        self.by_account_impl(ctx, account, page, per_page).await
    }
}

///////////////////////////////////////////////////////////////////////////////

page_based_connection!(Dataset, DatasetConnection, DatasetEdge);
