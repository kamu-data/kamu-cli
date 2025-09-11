// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use kamu_accounts::AccountService;
use kamu_auth_rebac::RebacService;
use kamu_datasets::{DatasetEntryService, DatasetResolution};

use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Auth;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Auth {
    const DEFAULT_ENTRIES_PER_PAGE: usize = 100;

    #[allow(clippy::unused_async)]
    #[tracing::instrument(level = "info", name = Auth_enabled_providers, skip_all)]
    async fn enabled_providers(&self, ctx: &Context<'_>) -> Result<Vec<AccountProvider>> {
        use std::str::FromStr;

        let authentication_service = from_catalog_n!(ctx, dyn kamu_accounts::AuthenticationService);

        let providers = authentication_service
            .supported_login_methods()
            .into_iter()
            .map(kamu_accounts::AccountProvider::from_str)
            .map(|res| res.map(Into::into))
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        Ok(providers)
    }

    #[allow(clippy::unused_async)]
    #[tracing::instrument(level = "info", name = Auth_relations, skip_all)]
    #[graphql(guard = "AdminGuard::new()")]
    async fn relations(
        &self,
        ctx: &Context<'_>,
        account_ids: Vec<AccountID<'_>>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<AuthRelationConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_ENTRIES_PER_PAGE);

        let (rebac_service, account_service, dataset_entry_service) = from_catalog_n!(
            ctx,
            dyn RebacService,
            dyn AccountService,
            dyn DatasetEntryService
        );

        let account_ids = account_ids.iter().map(AsRef::as_ref).collect::<Vec<_>>();
        let accounts_map = account_service
            .get_account_map(&account_ids)
            .await
            .int_err()?;
        let authorized_datasets_map = rebac_service
            .get_authorized_datasets_by_account_ids(&account_ids[..])
            .await
            .int_err()?;
        let dataset_entries_map = {
            let dataset_ids = authorized_datasets_map
                .values()
                .flatten()
                .map(|e| Cow::Borrowed(&e.dataset_id))
                .collect::<Vec<_>>();
            let dataset_entries_resolution = dataset_entry_service
                .get_multiple_entries(&dataset_ids)
                .await
                .int_err()?;
            dataset_entries_resolution.into_resolution_map()
        };

        let mut auth_relations = Vec::new();

        for (account_id, authorized_datasets) in authorized_datasets_map {
            auth_relations.reserve_exact(authorized_datasets.len());

            for authorized_dataset in authorized_datasets {
                let account = accounts_map.get(&account_id).ok_or(GqlError::gql_extended(
                    "Account not found in accounts map",
                    |eev| {
                        eev.set("account_id", account_id.to_string());
                    },
                ))?;
                let gql_account = Account::from_account(account.clone());

                let dataset_id = &authorized_dataset.dataset_id;
                let dataset = dataset_entries_map
                    .get(dataset_id)
                    .ok_or(GqlError::gql_extended(
                        "Dataset not found in dataset entries map",
                        |eev| {
                            eev.set("dataset_id", dataset_id.to_string());
                        },
                    ))
                    .and_then(|dataset_resolution| match dataset_resolution {
                        DatasetResolution::Resolved(dataset_entry) => Ok(dataset_entry),
                        DatasetResolution::Unresolved => {
                            Err(GqlError::gql_extended("Dataset not resolved", |eev| {
                                eev.set("dataset_id", dataset_id.to_string());
                            }))
                        }
                    })?;
                let gql_dataset_owner = Account::new(
                    dataset.owner_id.clone().into(),
                    dataset.owner_name.clone().into(),
                );
                // As admin, we should have access to everything
                let gql_dataset = Dataset::new_access_checked(gql_dataset_owner, dataset.handle());

                let auth_relation = AuthRelation {
                    account: gql_account,
                    role: authorized_dataset.role.into(),
                    dataset: gql_dataset,
                };

                auth_relations.push(auth_relation);
            }
        }

        let total_count = auth_relations.len();
        let nodes = auth_relations
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .collect();

        Ok(AuthRelationConnection::new(
            nodes,
            page,
            per_page,
            total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(AuthRelation, AuthRelationConnection, AuthRelationEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
