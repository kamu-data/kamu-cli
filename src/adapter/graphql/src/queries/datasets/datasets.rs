// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::auth::{self, DatasetActionAuthorizer, DatasetActionAuthorizerExt};

use crate::prelude::*;
use crate::queries::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Datasets;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Datasets {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    async fn by_dataset_ref(
        &self,
        ctx: &Context<'_>,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<Dataset>> {
        let rebac_dataset_registry_facade = from_catalog_n!(ctx, dyn RebacDatasetRegistryFacade);

        let resolve_res = rebac_dataset_registry_facade
            .resolve_dataset_handle_by_ref(dataset_ref, auth::DatasetAction::Read)
            .await;
        let handle = match resolve_res {
            Ok(handle) => Ok(handle),
            Err(e) => {
                use RebacDatasetRefUnresolvedError as E;

                match e {
                    E::NotFound(_) | E::Access(_) => return Ok(None),
                    e @ E::Internal(_) => Err(e.int_err()),
                }
            }
        }?;
        let account = Account::from_dataset_alias(ctx, &handle.alias)
            .await?
            .expect("Account must exist");

        Ok(Some(Dataset::new_access_checked(account, handle)))
    }

    /// Returns dataset by its ID
    #[tracing::instrument(level = "info", name = Datasets_by_id, skip_all, fields(%dataset_id))]
    async fn by_id(&self, ctx: &Context<'_>, dataset_id: DatasetID<'_>) -> Result<Option<Dataset>> {
        let dataset_id: odf::DatasetID = dataset_id.into();

        self.by_dataset_ref(ctx, &dataset_id.into_local_ref()).await
    }

    /// Returns dataset by its owner and name
    #[tracing::instrument(level = "info", name = Datasets_by_owner_and_name, skip_all, fields(%account_name, %dataset_name))]
    async fn by_owner_and_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName<'_>,
        dataset_name: DatasetName<'_>,
    ) -> Result<Option<Dataset>> {
        let dataset_alias = odf::DatasetAlias::new(Some(account_name.into()), dataset_name.into());

        self.by_dataset_ref(ctx, &dataset_alias.into_local_ref())
            .await
    }

    #[graphql(skip)]
    async fn by_account_impl(
        &self,
        ctx: &Context<'_>,
        account_ref: Account,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<DatasetConnection> {
        let (dataset_registry, dataset_action_authorizer) = from_catalog_n!(
            ctx,
            dyn kamu_core::DatasetRegistry,
            dyn DatasetActionAuthorizer
        );

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        use futures::TryStreamExt;

        let account_owned_datasets_stream =
            dataset_registry.all_dataset_handles_by_owner_id(account_ref.account_id_internal());
        let readable_dataset_handles_stream = dataset_action_authorizer
            .filtered_datasets_stream(account_owned_datasets_stream, auth::DatasetAction::Read);
        let mut accessible_datasets_handles = readable_dataset_handles_stream
            .try_collect::<Vec<_>>()
            .await?;

        let total_count = accessible_datasets_handles.len();

        accessible_datasets_handles.sort_by(|a, b| a.alias.cmp(&b.alias));

        let nodes = accessible_datasets_handles
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .map(|handle| Dataset::new_access_checked(account_ref.clone(), handle))
            .collect();

        Ok(DatasetConnection::new(nodes, page, per_page, total_count))
    }

    /// Returns datasets belonging to the specified account
    #[tracing::instrument(level = "info", name = Datasets_by_account_id, skip_all, fields(%account_id, ?page, ?per_page))]
    async fn by_account_id(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<DatasetConnection> {
        let account_service = from_catalog_n!(ctx, dyn kamu_accounts::AccountService);

        let account_id: odf::AccountID = account_id.into();
        let maybe_account_name = account_service.find_account_name_by_id(&account_id).await?;

        if let Some(account_name) = maybe_account_name {
            self.by_account_impl(
                ctx,
                Account::new(account_id.into(), account_name.into()),
                page,
                per_page,
            )
            .await
        } else {
            let page = page.unwrap_or(0);
            let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

            Ok(DatasetConnection::new(vec![], page, per_page, 0))
        }
    }

    /// Returns datasets belonging to the specified account
    #[tracing::instrument(level = "info", name = Datasets_by_account_name, skip_all, fields(%account_name, ?page, ?per_page))]
    async fn by_account_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<DatasetConnection> {
        let maybe_account = Account::from_account_name(ctx, account_name.into()).await?;

        if let Some(account) = maybe_account {
            self.by_account_impl(ctx, account, page, per_page).await
        } else {
            let page = page.unwrap_or(0);
            let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

            Ok(DatasetConnection::new(vec![], page, per_page, 0))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(Dataset, DatasetConnection, DatasetEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
