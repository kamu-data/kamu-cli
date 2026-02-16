// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{DatasetAction, DatasetActionAuthorizerExt};

use crate::prelude::*;
use crate::queries::*;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Datasets;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Datasets {
    const DEFAULT_PER_PAGE: usize = 15;

    pub async fn by_dataset_ref(
        ctx: &Context<'_>,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<Dataset>> {
        let dataset_handle_data_loader = utils::get_dataset_handle_data_loader(ctx);

        let tenancy_config = from_catalog_n!(ctx, kamu_core::TenancyConfig);
        let canonical_dataset_ref = tenancy_config.canonical_ref(dataset_ref);

        let maybe_handle = dataset_handle_data_loader
            .load_one(canonical_dataset_ref)
            .await
            .map_err(data_loader_error_mapper)?;

        if let Some(handle) = maybe_handle {
            let account = Account::from_dataset_alias(ctx, &handle.alias)
                .await?
                .expect("Account must exist");

            Ok(Some(Dataset::new_access_checked(account, handle)))
        } else {
            Ok(None)
        }
    }

    async fn by_dataset_refs(
        &self,
        ctx: &Context<'_>,
        dataset_refs: Vec<odf::DatasetRef>,
        skip_missing: bool,
    ) -> Result<Vec<Dataset>> {
        // TODO: PERF: GQL: DataLoader?

        let (rebac_dataset_registry_facade, account_service, current_account_subject) = from_catalog_n!(
            ctx,
            dyn kamu_auth_rebac::RebacDatasetRegistryFacade,
            dyn kamu_accounts::AccountService,
            kamu_accounts::CurrentAccountSubject
        );

        let dataset_refs_refs = dataset_refs.iter().collect::<Vec<_>>();
        let resolution = rebac_dataset_registry_facade
            .classify_dataset_refs_by_allowance(&dataset_refs_refs, DatasetAction::Read)
            .await?;

        if !skip_missing && !resolution.inaccessible_refs.is_empty() {
            return Err(GqlError::gql(format!(
                "Inaccessible datasets: {}",
                itertools::join(resolution.inaccessible_refs.iter().map(|(r, _)| r), ",")
            )));
        }

        // Reorder resolved handles to match the order of inputs
        let accessible_dataset_handles: Vec<odf::DatasetHandle> = {
            let resolved_handles_map: std::collections::BTreeMap<
                odf::DatasetRef,
                odf::DatasetHandle,
            > = resolution.accessible_resolved_refs.into_iter().collect();

            dataset_refs
                .iter()
                .filter_map(|r| resolved_handles_map.get(r).cloned())
                .collect()
        };

        let owner_names = accessible_dataset_handles
            .iter()
            .map(|h| current_account_subject.resolve_account_name_by_dataset_alias(&h.alias))
            .collect::<Vec<_>>();
        let owner_names_refs = owner_names.iter().collect::<Vec<_>>();
        let owner_lookup = account_service
            .get_accounts_by_names(&owner_names_refs)
            .await?;

        if !owner_lookup.not_found.is_empty() {
            return Err(GqlError::gql(format!(
                "Unresolved accounts: {}",
                itertools::join(owner_lookup.not_found.iter().map(|(n, _)| n), ",")
            )));
        }

        let owners_map: std::collections::BTreeMap<odf::AccountName, kamu_accounts::Account> =
            owner_lookup
                .found
                .into_iter()
                .map(|a| (a.account_name.clone(), a))
                .collect();

        let datasets = accessible_dataset_handles
            .into_iter()
            .map(|dataset_handle| {
                let owner_name = current_account_subject
                    .resolve_account_name_by_dataset_alias(&dataset_handle.alias);

                let owner = owners_map
                    .get(&owner_name)
                    .unwrap_or_else(|| unreachable!("{owner_name} not found in {owners_map:?}"))
                    .clone();

                Dataset::new_access_checked(Account::from_account(owner), dataset_handle)
            })
            .collect();

        Ok(datasets)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Datasets {
    /// Returns a dataset by its ID, if found
    #[tracing::instrument(level = "info", name = Datasets_by_id, skip_all, fields(%dataset_id))]
    pub async fn by_id(
        &self,
        ctx: &Context<'_>,
        dataset_id: DatasetID<'_>,
    ) -> Result<Option<Dataset>> {
        let dataset_id: odf::DatasetID = dataset_id.into();

        Self::by_dataset_ref(ctx, &dataset_id.into_local_ref()).await
    }

    /// Returns multiple datasets by their IDs.
    ///
    /// Order of results is guaranteed to match the inputs. Duplicate inputs
    /// will results in duplicate results.
    #[tracing::instrument(level = "info", name = Datasets_by_ids, skip_all, fields(?dataset_ids, ?skip_missing))]
    async fn by_ids(
        &self,
        ctx: &Context<'_>,
        dataset_ids: Vec<DatasetID<'_>>,
        #[graphql(
            desc = "Whether to skip unresolved datasets or return an error if one or more are \
                    missing"
        )]
        skip_missing: bool,
    ) -> Result<Vec<Dataset>> {
        let dataset_refs: Vec<odf::DatasetRef> =
            dataset_ids.iter().map(|id| id.as_local_ref()).collect();

        self.by_dataset_refs(ctx, dataset_refs, skip_missing).await
    }

    /// Returns a dataset by an ID or alias, if found
    #[tracing::instrument(level = "info", name = Datasets_by_ref, skip_all, fields(%dataset_ref))]
    async fn by_ref(
        &self,
        ctx: &Context<'_>,
        dataset_ref: DatasetRef<'_>,
    ) -> Result<Option<Dataset>> {
        Self::by_dataset_ref(ctx, &dataset_ref).await
    }

    /// Returns multiple datasets by their IDs or aliases. Order of results is
    /// guaranteed to match the inputs.
    #[tracing::instrument(level = "info", name = Datasets_by_refs, skip_all, fields(?dataset_refs, ?skip_missing))]
    async fn by_refs(
        &self,
        ctx: &Context<'_>,
        dataset_refs: Vec<DatasetRef<'_>>,
        #[graphql(
            desc = "Whether to skip unresolved datasets or return an error if one or more are \
                    missing"
        )]
        skip_missing: bool,
    ) -> Result<Vec<Dataset>> {
        let dataset_refs: Vec<odf::DatasetRef> = dataset_refs.into_iter().map(Into::into).collect();

        self.by_dataset_refs(ctx, dataset_refs, skip_missing).await
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

        Self::by_dataset_ref(ctx, &dataset_alias.into_local_ref()).await
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
            dyn kamu_datasets::DatasetRegistry,
            dyn kamu_datasets::DatasetActionAuthorizer
        );

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        use futures::TryStreamExt;

        let account_owned_datasets_stream =
            dataset_registry.all_dataset_handles_by_owner_id(account_ref.account_id_internal());
        let readable_dataset_handles_stream = dataset_action_authorizer
            .filtered_datasets_stream(account_owned_datasets_stream, DatasetAction::Read);
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
        let account_entity_data_loader = utils::get_account_entity_data_loader(ctx);

        let account_id: odf::AccountID = account_id.into();
        let maybe_account = account_entity_data_loader
            .load_one(account_id)
            .await
            .map_err(data_loader_error_mapper)?;

        if let Some(account) = maybe_account {
            self.by_account_impl(ctx, Account::from_account(account), page, per_page)
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
