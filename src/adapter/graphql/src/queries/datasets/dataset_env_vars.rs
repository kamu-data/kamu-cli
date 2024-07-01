// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_dataset_env_vars::{DatasetEnvVarPaginationOpts, DatasetEnvVarService};
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
    const DEFAULT_PER_PAGE: i64 = 15;

    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    async fn list_env_variables(
        &self,
        ctx: &Context<'_>,
        page: Option<i64>,
        per_page: Option<i64>,
    ) -> Result<ViewDatasetEnvVarConnection> {
        utils::check_dataset_read_access(ctx, &self.dataset_handle).await?;

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let dataset_env_var_service = from_catalog::<dyn DatasetEnvVarService>(ctx).unwrap();
        let dataset_env_vars: Vec<_> = dataset_env_var_service
            .get_all_dataset_env_vars_by_dataset_id(
                &self.dataset_handle.id,
                &DatasetEnvVarPaginationOpts {
                    offset: (page * per_page),
                    limit: per_page,
                },
            )
            .await
            .int_err()?
            .into_iter()
            .map(ViewDatasetEnvVar::new)
            .collect();
        let total_count = dataset_env_vars.len();

        Ok(ViewDatasetEnvVarConnection::new(
            dataset_env_vars,
            usize::try_from(page).unwrap(),
            usize::try_from(per_page).unwrap(),
            total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    ViewDatasetEnvVar,
    ViewDatasetEnvVarConnection,
    ViewDatasetEnvVarEdge
);
