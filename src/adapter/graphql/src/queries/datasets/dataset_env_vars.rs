// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use kamu_datasets::{DatasetEnvVarService, GetDatasetEnvVarError};

use super::ViewDatasetEnvVar;
use crate::prelude::*;
use crate::queries::DatasetRequestState;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVars<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> DatasetEnvVars<'a> {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(ctx: &Context<'_>, dataset_request_state: &'a DatasetRequestState) -> Result<Self> {
        utils::ensure_dataset_env_vars_enabled(ctx)?;

        Ok(Self {
            dataset_request_state,
        })
    }

    #[tracing::instrument(level = "info", name = DatasetEnvVars_exposed_value, skip_all)]
    async fn exposed_value(
        &self,
        ctx: &Context<'_>,
        dataset_env_var_id: DatasetEnvVarID<'_>,
    ) -> Result<String> {
        let dataset_env_var_service = from_catalog_n!(ctx, dyn DatasetEnvVarService);
        let dataset_env_var = dataset_env_var_service
            .get_dataset_env_var_by_id(&dataset_env_var_id)
            .await
            .map_err(|err| match err {
                GetDatasetEnvVarError::NotFound(err) => GqlError::Gql(err.into()),
                GetDatasetEnvVarError::Internal(err) => GqlError::Internal(err),
            })?;

        Ok(dataset_env_var_service
            .get_exposed_value(&dataset_env_var)
            .await?)
    }

    #[tracing::instrument(level = "info", name = DatasetEnvVars_list_env_variables, skip_all, fields(?page, ?per_page))]
    async fn list_env_variables(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ViewDatasetEnvVarConnection> {
        let dataset_env_var_service = from_catalog_n!(ctx, dyn DatasetEnvVarService);

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let dataset_env_var_listing = dataset_env_var_service
            .get_all_dataset_env_vars_by_dataset_id(
                &self.dataset_request_state.dataset_handle().id,
                Some(PaginationOpts::from_page(page, per_page)),
            )
            .await
            .int_err()?;

        let dataset_env_vars: Vec<_> = dataset_env_var_listing
            .list
            .into_iter()
            .map(ViewDatasetEnvVar::new)
            .collect();

        Ok(ViewDatasetEnvVarConnection::new(
            dataset_env_vars,
            page,
            per_page,
            dataset_env_var_listing.total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    ViewDatasetEnvVar,
    ViewDatasetEnvVarConnection,
    ViewDatasetEnvVarEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
