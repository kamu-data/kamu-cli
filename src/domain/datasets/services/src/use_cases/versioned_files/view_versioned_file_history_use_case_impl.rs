// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::ResultIntoInternal;
use kamu_core::{GetDataOptions, QueryService};
use kamu_datasets::{
    FileVersion,
    ReadCheckedDataset,
    VersionedFileEntry,
    VersionedFileHistoryPage,
    ViewVersionedFileHistoryError,
    ViewVersionedFileHistoryUseCase,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ViewVersionedFileHistoryUseCase)]
pub struct ViewVersionedFileHistoryUseCaseImpl {
    query_svc: Arc<dyn QueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl ViewVersionedFileHistoryUseCase for ViewVersionedFileHistoryUseCaseImpl {
    #[tracing::instrument(level = "debug", name = ViewVersionedFileHistoryUseCaseImpl_execute, skip_all)]
    async fn execute(
        &self,
        file_dataset: ReadCheckedDataset<'_>,
        max_version: Option<FileVersion>,
        pagination: Option<PaginationOpts>,
    ) -> Result<VersionedFileHistoryPage, ViewVersionedFileHistoryError> {
        use datafusion::logical_expr::{col, lit};

        let query_res = self
            .query_svc
            .get_data(file_dataset.into_inner(), GetDataOptions::default())
            .await
            .int_err()?;

        let Some(df) = query_res.df else {
            return Ok(VersionedFileHistoryPage::default());
        };

        let df = if let Some(max_version) = max_version {
            df.filter(col("version").lt_eq(lit(max_version)))
                .int_err()?
        } else {
            df
        };

        let total_count = df.clone().count().await.int_err()?;

        let df = df.sort(vec![col("version").sort(false, false)]).int_err()?;

        let df = if let Some(pagination) = pagination {
            df.limit(pagination.offset, Some(pagination.limit))
                .int_err()?
        } else {
            df
        };

        let records = df.collect_json_aos().await.int_err()?;

        let entries = records
            .into_iter()
            .map(VersionedFileEntry::from_json)
            .collect::<Result<_, _>>()?;

        Ok(VersionedFileHistoryPage {
            list: entries,
            total_count,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
