// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_rebac::RebacService;

use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Auth;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Auth {
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
    ) -> Result<Vec<AuthRelation>> {
        let rebac_service = from_catalog_n!(ctx, dyn RebacService);

        let account_ids = account_ids.into_iter().map(Into::into).collect::<Vec<_>>();
        let authorized_datasets_map = rebac_service
            .get_authorized_datasets_by_account_ids(&account_ids)
            .await
            .int_err()?;

        let mut auth_relations = Vec::new();

        for (account_id, authorized_datasets) in authorized_datasets_map {
            auth_relations.reserve_exact(authorized_datasets.len());

            for authorized_dataset in authorized_datasets {
                let gql_account = Account::from_account_id(ctx, account_id.clone()).await?;
                let gql_dataset = match Dataset::try_from_ref(
                    ctx,
                    &authorized_dataset.dataset_id.into(),
                )
                .await?
                {
                    TransformInputDataset::Accessible(ti) => ti.dataset,
                    TransformInputDataset::NotAccessible(_) => {
                        // As admin, we should have access to everything
                        unreachable!()
                    }
                };

                let auth_relation = AuthRelation {
                    account: gql_account,
                    role: authorized_dataset.role.into(),
                    dataset: gql_dataset,
                };

                auth_relations.push(auth_relation);
            }
        }

        Ok(auth_relations)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
