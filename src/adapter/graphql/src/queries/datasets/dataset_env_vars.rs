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
use opendatafabric as odf;

use super::ViewDatasetEnvVar;
use crate::prelude::*;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVars {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetEnvVars {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    async fn exposed_value(
        &self,
        ctx: &Context<'_>,
        dataset_env_var_id: DatasetEnvVarID,
    ) -> Result<String> {
        utils::check_dataset_read_access(ctx, &self.dataset_handle).await?;

        let dataset_env_var_service = from_catalog::<dyn DatasetEnvVarService>(ctx).unwrap();
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

    async fn list_env_variables(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ViewDatasetEnvVarConnection> {
        utils::check_dataset_read_access(ctx, &self.dataset_handle).await?;

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let dataset_env_var_service = from_catalog::<dyn DatasetEnvVarService>(ctx).unwrap();
        let dataset_env_var_listing = dataset_env_var_service
            .get_all_dataset_env_vars_by_dataset_id(
                &self.dataset_handle.id,
                Some(PaginationOpts {
                    offset: (page * per_page),
                    limit: per_page,
                }),
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
