// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_core::{GetDataOptions, GetDataResponse, QueryService};
use kamu_datasets::{
    FileVersion,
    FindVersionedFileVersionError,
    FindVersionedFileVersionUseCase,
    ReadCheckedDataset,
    VersionedFileEntry,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn FindVersionedFileVersionUseCase)]
pub struct FindVersionedFileVersionUseCaseImpl {
    query_svc: Arc<dyn QueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl FindVersionedFileVersionUseCase for FindVersionedFileVersionUseCaseImpl {
    #[tracing::instrument(level = "debug", name = FindVersionedFileVersionUseCaseImpl_execute, skip_all)]
    async fn execute(
        &self,
        file_dataset: ReadCheckedDataset<'_>,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<odf::Multihash>,
    ) -> Result<Option<VersionedFileEntry>, FindVersionedFileVersionError> {
        let query_res = if let Some(block_hash) = as_of_block_hash {
            self.query_svc
                .tail(
                    file_dataset.into_inner(),
                    0,
                    1,
                    GetDataOptions {
                        block_hash: Some(block_hash),
                    },
                )
                .await
        } else if let Some(version) = as_of_version {
            use datafusion::logical_expr::{col, lit};

            self.query_svc
                .get_data(file_dataset.into_inner(), GetDataOptions::default())
                .await
                .map(|res| GetDataResponse {
                    df: res
                        .df
                        .map(|df| df.filter(col("version").eq(lit(version))).unwrap()),
                    source: res.source,
                    block_hash: res.block_hash,
                })
        } else {
            self.query_svc
                .tail(file_dataset.into_inner(), 0, 1, GetDataOptions::default())
                .await
        }
        .int_err()?;

        let Some(df) = query_res.df else {
            return Ok(None);
        };

        let records = df.collect_json_aos().await.int_err()?;
        if records.is_empty() {
            return Ok(None);
        }

        assert_eq!(records.len(), 1);

        let entry = VersionedFileEntry::from_json(records.into_iter().next().unwrap())?;

        Ok(Some(entry))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
